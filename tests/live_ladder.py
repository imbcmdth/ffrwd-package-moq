"""The rendition ladder, end to end, on this machine - run it by hand.

    python tests/live_ladder.py [--rungs N] [--keep]

The claim under test: ONE decode, N encodes, ONE broadcast. Compiles this
package's `publish-ladder` recipe with the real compiler and runs it - ONE
ffmpeg splits the decoded source and encodes a rung apiece at its own
bitrate, one sidecar hosts one publish.wasm instance reading all of them,
and a local moq-relay carries the single broadcast. Then N sub-recv guests
subscribe, each reading `catalog.json` to find the tracks and taking a
different one, reassembling its fmp4 to a file ffprobe must accept at that
rung's own size.

What would pass with N independent publishers is exactly what this refuses
to accept: every subscriber reads ONE catalog naming every rendition, and
the module's own rows name the track each group left on.

Where a subscriber STARTS is not asserted, and cannot be: a MoQ
subscription begins at the latest group, and a reader that is still
choosing a rendition when the first fragment goes out joins at the group it
arrives in, as a reader of any live broadcast does. What is asserted is
that every rendition published every group, and that each reader got a
CONTIGUOUS run of its own rung's groups through to the last - which is what
a player switching rungs actually needs.

Never collected by any suite: it needs moq-relay, wasmtime, ffmpeg, ffprobe,
cargo with the wasm32-wasip2 target and a wasi-sdk clang - which CI lacks -
and it opens UDP sockets. The process machinery it shares with the loops
beside it is in common.py.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from common import (
    CLI,
    PACKAGE,
    SIDECAR,
    build_guests,
    ffprobe_frames,
    free_udp_port,
    kill_tree,
    run,
    start_relay,
    start_subscriber,
    wait_subscriber,
)

SECONDS = 8
RATE = 30
GOP = 30
BROADCAST = "live/ladder"
# One rung per entry, widest first; the heights follow from -2 scaling.
WIDTHS = (854, 640, 426)
BITRATES = ("2000k", "1000k", "400k")

BUILD_DEADLINE = 600
COMPILE_DEADLINE = 180
RUN_DEADLINE = 240
SUBSCRIBER_DEADLINE = 180
TOOL_DEADLINE = 60


def make_source(path: Path) -> None:
    done = run([
        "ffmpeg", "-v", "error", "-y",
        "-f", "lavfi", "-i", f"testsrc2=size=1280x720:rate={RATE}",
        "-t", str(SECONDS), "-c:v", "libx264", "-preset", "ultrafast",
        "-pix_fmt", "yuv420p", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"building the source failed:\n{done.stderr[-800:]}")


def recipe_args(source: Path, port: int, cert_hex: str, rungs: int) -> list[str]:
    return [
        "-f", str(PACKAGE / "recipes" / "publish-ladder.sql"),
        "-v", f"source={source}",
        "-v", f"relay=moqt://127.0.0.1:{port}",
        "-v", f"broadcast={BROADCAST}",
        "-v", f"rungs={rungs}",
        "-v", "widths=" + ",".join(str(w) for w in WIDTHS[:rungs]),
        "-v", "bitrates=" + ",".join(BITRATES[:rungs]),
        "-v", f"cert={cert_hex}",
    ]


def run_publisher(
    source: Path, port: int, cert_hex: str, rungs: int
) -> tuple[list[dict], str]:
    """Compiles and runs the ladder recipe through the real compiler."""
    env = dict(os.environ)
    env["FFRWD_WASM"] = str(SIDECAR)
    args = recipe_args(source, port, cert_hex, rungs)
    shown = run(
        ["uv", "run", "--project", CLI, "ffrwd", "compile", *args],
        COMPILE_DEADLINE, capture_output=True, text=True, env=env,
    )
    if shown.returncode != 0:
        sys.exit(f"the recipe does not compile:\n{shown.stderr[-1200:]}")
    print("--- compiled plan ---")
    print(shown.stdout.strip())
    if shown.stdout.count("ffrwd-wasm") != 1:
        sys.exit("the ladder must reach ONE sidecar process, and this plan has more")
    # One decode: the rungs leave one ffmpeg through a split, not one
    # ffmpeg apiece each opening the source again.
    if shown.stdout.count("ffmpeg -i") != 1:
        sys.exit("the ladder must decode its source ONCE, and this plan opens it more")
    if f"split={rungs}" not in shown.stdout:
        sys.exit(f"the one decode must split {rungs} ways, and this plan does not")

    started = time.monotonic()
    done = run(
        ["uv", "run", "--project", CLI, "ffrwd", "run", *args, "-q"],
        RUN_DEADLINE, capture_output=True, text=True, env=env,
    )
    wall = time.monotonic() - started
    print(f"--- ffrwd run: exit {done.returncode}, {wall:.1f}s wall ---")
    print(done.stdout)
    if done.returncode != 0:
        print(done.stderr[-2000:])
        sys.exit("ffrwd run failed")
    rows = []
    for line in done.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows, done.stdout


def catalog_of(transcript: str) -> dict:
    """The catalog document one subscriber printed."""
    for line in transcript.splitlines():
        if line.startswith("sub: catalog {"):
            return json.loads(line[len("sub: catalog "):])
    sys.exit("no subscriber printed a catalog")


def one_run(sub_wasm: Path, certgen: Path, rungs: int, keep: bool) -> None:
    dir = Path(tempfile.mkdtemp(prefix="moq-ladder-"))
    relay: subprocess.Popen | None = None
    subs: list[subprocess.Popen] = []
    try:
        made = run([certgen, dir], TOOL_DEADLINE, capture_output=True, text=True)
        if made.returncode != 0:
            sys.exit(f"certgen failed:\n{made.stderr[-400:]}")
        cert_hex = made.stdout.strip()
        source = dir / "source.mp4"
        make_source(source)
        out_dir = dir / "out"
        out_dir.mkdir()

        port = free_udp_port()
        relay = start_relay(port, dir / "cert.pem", dir / "key.pem", dir / "relay.log")
        # One subscriber per rung, each choosing its own track OFF THE
        # CATALOG rather than being told a name.
        subs = [
            start_subscriber(
                sub_wasm, port, cert_hex, out_dir, BROADCAST,
                rendition=rung, output=f"recv{rung}.mp4",
            )
            for rung in range(rungs)
        ]
        rows, _ = run_publisher(source, port, cert_hex, rungs)
        transcripts = [wait_subscriber(sub, SUBSCRIBER_DEADLINE) for sub in subs]

        group_rows = [r for r in rows if "group" in r]
        summaries = [r for r in rows if "groups" in r]
        assert group_rows, "the module emitted no group rows"
        assert len(summaries) == 1, f"expected one summary row, got {len(summaries)}"
        summary = summaries[0]
        assert summary["tracks"] == rungs, (
            f"one broadcast must carry {rungs} tracks, and the summary says "
            f"{summary['tracks']}"
        )
        published = sorted({row["track"] for row in group_rows})
        assert len(published) == rungs, (
            f"{rungs} rungs must leave on {rungs} distinct tracks, got {published}"
        )
        expected_frames = SECONDS * RATE
        assert summary["packets"] == expected_frames * rungs, (
            f"{summary['packets']} packets for {rungs} rungs of {expected_frames}"
        )

        catalog = catalog_of(transcripts[0])
        print("--- catalog ---")
        print(json.dumps(catalog, indent=2))
        assert len(catalog["tracks"]) == rungs, "the catalog names every rendition"
        assert [t["name"] for t in catalog["tracks"]] == sorted(
            published, key=lambda name: [t["name"] for t in catalog["tracks"]].index(name)
        ), "the catalog's tracks are the ones the rows named"
        for entry, width in zip(catalog["tracks"], WIDTHS[:rungs]):
            assert entry["width"] == width, (
                f"catalog track {entry['name']} says {entry['width']}, rung is {width}"
            )
            assert entry["codec"].startswith("avc1."), entry["codec"]
            assert entry["init"] == f"{entry['name']}.init", entry

        wanted_groups = expected_frames // GOP
        for track in published:
            groups = [row for row in group_rows if row["track"] == track]
            assert abs(len(groups) - wanted_groups) <= 1, (
                f"{track}: {len(groups)} groups for a {GOP}-frame keyframe interval"
            )

        # Every subscriber must have reassembled ITS rung, at its own size,
        # from a contiguous run of that rung's groups ending at the last.
        from_the_start = 0
        for rung, transcript in enumerate(transcripts):
            name = catalog["tracks"][rung]["name"]
            assert f"sub: subscribing to {name}" in transcript, (
                f"subscriber {rung} read the wrong track"
            )
            received = groups_received(transcript)
            published_here = [r["group"] for r in group_rows if r["track"] == name]
            assert received, f"rung {rung} received nothing"
            assert received == list(range(received[0], received[-1] + 1)), (
                f"rung {rung} received {received}, which has a hole in it"
            )
            assert received[-1] == max(published_here), (
                f"rung {rung} stopped at group {received[-1]}, and "
                f"{name} published through {max(published_here)}"
            )
            if received[0] == 0:
                from_the_start += 1
            frames = ffprobe_frames(out_dir / f"recv{rung}.mp4", TOOL_DEADLINE)
            assert frames == len(received) * GOP, (
                f"rung {rung}: {frames} frames over {len(received)} groups"
            )
            size = probe_size(out_dir / f"recv{rung}.mp4")
            assert size[0] == WIDTHS[rung], (
                f"rung {rung} reassembled at {size[0]}x{size[1]}, not {WIDTHS[rung]} wide"
            )
            print(
                f"PASS rung {rung}: {name} at {size[0]}x{size[1]}, {frames} frames "
                f"from group {received[0]}"
            )
        assert from_the_start, (
            "no reader captured from group 0, so nothing proves the first group "
            "is reachable at all"
        )
        print(
            f"PASS: one broadcast, {summary['tracks']} tracks, "
            f"{summary['groups']} groups, {summary['bytes']} bytes end to end"
        )
    finally:
        # Whatever went wrong above, nothing outlives the run.
        for sub in subs:
            kill_tree(sub)
        if relay is not None:
            kill_tree(relay)
        if keep:
            print(f"kept: {dir}")
        else:
            shutil.rmtree(dir, ignore_errors=True)


def groups_received(transcript: str) -> list[int]:
    """The group numbers one subscriber wrote, in arrival order."""
    return [
        int(line.split()[4])
        for line in transcript.splitlines()
        if line.startswith("sub: fragment ")
    ]


def probe_size(path: Path) -> tuple[int, int]:
    done = run([
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height", "-of", "csv=p=0", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe rejected {path}:\n{done.stderr[-800:]}")
    width, height = done.stdout.strip().split(",")[:2]
    return int(width), int(height)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rungs", type=int, default=3, choices=(2, 3))
    parser.add_argument("--keep", action="store_true", help="keep the work directory")
    args = parser.parse_args()

    if not SIDECAR.exists():
        sys.exit(f"no sidecar at {SIDECAR}; build it first (cargo build --release)")
    _, sub_wasm, certgen = build_guests(BUILD_DEADLINE)
    started = time.monotonic()
    one_run(sub_wasm, certgen, args.rungs, args.keep)
    print(f"=== the ladder took {time.monotonic() - started:.1f}s ===")


if __name__ == "__main__":
    main()
