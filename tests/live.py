"""The live loop, end to end, on this machine - run it by hand.

    python tests/live.py [--runs N] [--keep]

Compiles this package's `publish` recipe with the real compiler and
runs it: the feeder ffmpeg encodes, the sidecar hosts publish.wasm
under its network grant, a local moq-relay carries the broadcast, and
the sub-recv guest (wasmtime) subscribes from group 0, reassembles the
fmp4 to a file, and ffprobe must count every frame of it. The module's
own NDJSON rows and the subscriber's transcript must agree group for
group and byte for byte.

Never collected by any suite: it needs moq-relay, wasmtime, ffmpeg,
ffprobe, cargo with the wasm32-wasip2 target and a wasi-sdk clang -
which CI lacks - and it opens UDP sockets. Toolchain overrides:
WASMTIME, MOQ_RELAY, WASI_SDK_PATH (or CC_wasm32_wasip2 directly).

The process machinery it shares with the loops beside it - deadlines
that kill the whole tree, the relay, the subscriber - is in common.py.
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
BROADCAST = "live/demo"
TRACK = "video"

# Hard deadlines, seconds. Generous: an expiry means something is stuck,
# not slow.
BUILD_DEADLINE = 600
COMPILE_DEADLINE = 180
RUN_DEADLINE = 180
SUBSCRIBER_DEADLINE = 120
TOOL_DEADLINE = 60


def make_source(path: Path) -> None:
    done = run([
        "ffmpeg", "-v", "error", "-y",
        "-f", "lavfi", "-i", f"testsrc2=size=640x360:rate={RATE}",
        "-t", str(SECONDS), "-c:v", "libx264", "-preset", "ultrafast",
        "-pix_fmt", "yuv420p", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"building the source failed:\n{done.stderr[-800:]}")


def run_publisher(source: Path, port: int, cert_hex: str) -> tuple[list[dict], str]:
    """Compiles and runs the publish recipe through the real compiler."""
    recipe = PACKAGE / "recipes" / "publish.sql"
    env = dict(os.environ)
    env["FFRWD_WASM"] = str(SIDECAR)
    shown = run(
        ["uv", "run", "--project", CLI, "ffrwd", "compile", "-f", recipe,
         "-v", f"source={source}",
         "-v", f"relay=moqt://127.0.0.1:{port}",
         "-v", f"broadcast={BROADCAST}",
         "-v", f"cert={cert_hex}"],
        COMPILE_DEADLINE, capture_output=True, text=True, env=env,
    )
    if shown.returncode != 0:
        sys.exit(f"the recipe does not compile:\n{shown.stderr[-1200:]}")
    print("--- compiled command ---")
    print(shown.stdout.strip())

    started = time.monotonic()
    done = run(
        ["uv", "run", "--project", CLI, "ffrwd", "run", "-f", recipe,
         "-v", f"source={source}",
         "-v", f"relay=moqt://127.0.0.1:{port}",
         "-v", f"broadcast={BROADCAST}",
         "-v", f"cert={cert_hex}",
         "-q"],
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


def one_run(publish_wasm: Path, sub_wasm: Path, certgen: Path, keep: bool) -> None:
    dir = Path(tempfile.mkdtemp(prefix="moq-live-"))
    relay: subprocess.Popen | None = None
    sub: subprocess.Popen | None = None
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
        sub = start_subscriber(sub_wasm, port, cert_hex, out_dir, BROADCAST, TRACK)
        rows, _ = run_publisher(source, port, cert_hex)
        transcript = wait_subscriber(sub, SUBSCRIBER_DEADLINE)

        group_rows = [r for r in rows if "group" in r]
        summaries = [r for r in rows if "groups" in r]
        assert group_rows, "the module emitted no group rows"
        assert len(summaries) == 1, f"expected one summary row, got {len(summaries)}"
        summary = summaries[0]
        assert summary["groups"] == len(group_rows), (
            f"summary says {summary['groups']} groups, {len(group_rows)} rows arrived"
        )
        assert summary["bytes"] == sum(r["bytes"] for r in group_rows)
        assert summary["packets"] == sum(r["packets"] for r in group_rows)
        expected_frames = SECONDS * RATE
        assert summary["packets"] == expected_frames, (
            f"{summary['packets']} packets for {expected_frames} frames"
        )

        # The subscriber's summary line must agree with the module's rows.
        line = next(
            spoken for spoken in transcript.splitlines()
            if spoken.startswith("sub: reassembled")
        )
        words = line.split()
        sub_fragments, sub_groups, sub_bytes = int(words[2]), int(words[5]), int(words[7])
        # One fragment per sample: the reader's fragment count is the
        # packet count, its group count the group rows.
        assert sub_fragments == summary["packets"], "fragment counts diverge"
        assert sub_groups == len(group_rows), "group counts diverge"
        assert sub_bytes == summary["bytes"] + summary["init_bytes"], "byte counts diverge"

        frames = ffprobe_frames(out_dir / "recv.mp4", TOOL_DEADLINE)
        assert frames == expected_frames, f"{frames} frames, expected {expected_frames}"
        wanted_groups = expected_frames // GOP
        assert abs(len(group_rows) - wanted_groups) <= 1, (
            f"{len(group_rows)} groups for a {GOP}-frame keyframe interval"
        )
        print(
            f"PASS: {frames} frames reassembled in {sub_groups} groups, "
            f"{sub_bytes} bytes end to end"
        )
    finally:
        # Whatever went wrong above, nothing outlives the run.
        if sub is not None:
            kill_tree(sub)
        if relay is not None:
            kill_tree(relay)
        if keep:
            print(f"kept: {dir}")
        else:
            shutil.rmtree(dir, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--keep", action="store_true", help="keep the work directory")
    args = parser.parse_args()

    if not SIDECAR.exists():
        sys.exit(f"no sidecar at {SIDECAR}; build it first (cargo build --release)")
    publish_wasm, sub_wasm, certgen = build_guests(BUILD_DEADLINE)
    for attempt in range(1, args.runs + 1):
        print(f"=== run {attempt} of {args.runs} ===")
        started = time.monotonic()
        one_run(publish_wasm, sub_wasm, certgen, args.keep)
        print(f"=== run {attempt} took {time.monotonic() - started:.1f}s ===")


if __name__ == "__main__":
    main()
