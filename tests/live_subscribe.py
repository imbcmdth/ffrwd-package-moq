"""The roundtrip, end to end, on this machine - run it by hand.

    python tests/live_subscribe.py [--rungs N] [--keep]

Publish a ladder to a local relay, then read it back: subscribe.wasm
opens the broadcast, reads catalog.json, subscribes to every rendition
and demuxes each fmp4 fragment to the packets it was built from, one
`-f nut` output per catalog track. Every rung must come back at its own
geometry, the audio at its own rate, and each track's packet count must
agree with what sub-recv reassembled off the same broadcast.

Then the chain: the widest rung that came back is published again under
a NEW broadcast name, and subscribed to a second time. A rendition that
survives publish -> subscribe -> publish -> subscribe is one nothing in
either direction invented.

The subscribe module is driven by the sidecar directly rather than by
`ffrwd run`, because `ffrwd compile` cannot probe it: the compiler's
probe of a packet source passes the sidecar no `-net`, so a source that
reads a network refuses at compile time. `recipes/subscribe.sql` and
its two neighbours are red for exactly that reason, and go green with
no change here when the probe is granted what a run already grants.

Never collected by any suite: it needs moq-relay, wasmtime, ffmpeg,
ffprobe, cargo with the wasm32-wasip2 target and a wasi-sdk clang -
which CI lacks - and it opens UDP sockets. The process machinery it
shares with the loops beside it is in common.py.
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
    free_udp_port,
    guest,
    kill_tree,
    run,
    spawn,
    start_relay,
    start_subscriber,
    wait_subscriber,
)

SECONDS = 20
RATE = 30
GOP = 30
BROADCAST = "live/roundtrip"
COPY = "live/roundtrip-copy"
WIDTHS = (1280, 640, 426)
BITRATES = ("2000k", "600k", "300k")
SAMPLE_RATE = 48000
CHANNELS = 2

BUILD_DEADLINE = 600
RUN_DEADLINE = 300
READER_DEADLINE = 300
TOOL_DEADLINE = 120


def make_source(path: Path) -> None:
    done = run([
        "ffmpeg", "-v", "error", "-y",
        "-f", "lavfi", "-i", f"testsrc2=size=1280x720:rate={RATE}",
        "-f", "lavfi", "-i", f"sine=frequency=440:sample_rate={SAMPLE_RATE}",
        "-ac", str(CHANNELS), "-c:a", "aac",
        "-t", str(SECONDS), "-c:v", "libx264", "-preset", "ultrafast",
        "-pix_fmt", "yuv420p", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"building the source failed:\n{done.stderr[-800:]}")


def start_source(
    module: Path, port: int, cert_hex: str, broadcast: str, outputs: list[Path]
) -> subprocess.Popen:
    """subscribe.wasm under the sidecar: one nut output per catalog track.

    The grants are the ones the compiler's own run path writes; only its
    probe leaves them off.
    """
    params = json.dumps({
        "relay": f"moqt://127.0.0.1:{port}",
        "broadcast": broadcast,
        "cert": cert_hex,
        "token": "",
    })
    argv = [str(SIDECAR), "-net", str(module), "-http", str(module),
            "-m", str(module), "-params", params]
    for path in outputs:
        argv += ["-f", "nut", str(path)]
    print("+", " ".join(argv[:6]), "...", flush=True)
    return spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def wait_source(reader: subprocess.Popen, deadline: int) -> None:
    try:
        stdout, stderr = reader.communicate(timeout=deadline)
    except subprocess.TimeoutExpired:
        kill_tree(reader)
        stdout, stderr = reader.communicate()
        sys.exit(f"the subscribe module did not finish:\n{stdout}\n{stderr[-1200:]}")
    if stderr.strip():
        print("--- subscribe stderr ---")
        print(stderr[-1200:])
    if reader.returncode != 0:
        sys.exit(f"the subscribe module failed: {stderr[-800:]}")


def start_publish(recipe: str, port: int, cert_hex: str, broadcast: str,
                  extra: list[str]) -> subprocess.Popen:
    """One publish recipe, running while a reader subscribes to it.

    A subscriber opens an EXISTING broadcast, so the publisher goes
    first and the reader follows it; the module publishing holds its
    first media for a reader either way.
    """
    env = dict(os.environ)
    env["FFRWD_WASM"] = str(SIDECAR)
    argv = ["uv", "run", "--project", str(CLI), "ffrwd", "run",
            "-f", str(PACKAGE / "recipes" / recipe),
            "-v", f"relay=moqt://127.0.0.1:{port}",
            "-v", f"broadcast={broadcast}",
            "-v", f"cert={cert_hex}", *extra, "-q"]
    print("+", " ".join(argv[:8]), "...", flush=True)
    return spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)


def wait_publish(publisher: subprocess.Popen, deadline: int) -> list[dict]:
    try:
        stdout, stderr = publisher.communicate(timeout=deadline)
    except subprocess.TimeoutExpired:
        kill_tree(publisher)
        stdout, stderr = publisher.communicate()
        sys.exit(f"the publisher did not finish:\n{stderr[-1200:]}")
    if publisher.returncode != 0:
        print(stdout[-2000:])
        print(stderr[-2000:])
        sys.exit("the publisher failed")
    return rows_of(stdout)


def rows_of(text: str) -> list[dict]:
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows


def catalog_of(transcript: str) -> dict:
    for line in transcript.splitlines():
        if line.startswith("sub: catalog {"):
            return json.loads(line[len("sub: catalog "):])
    sys.exit("no subscriber printed a catalog")


def fragments_received(transcript: str) -> int:
    """How many fragments sub-recv reassembled off its own track."""
    for line in transcript.splitlines():
        if line.startswith("sub: reassembled"):
            return int(line.split()[2])
    sys.exit("no subscriber said what it reassembled")


def probe(path: Path, stream: str, fields: str) -> dict[str, str]:
    done = run([
        "ffprobe", "-v", "error", "-select_streams", stream,
        "-show_entries", f"stream={fields}", "-of", "default=nw=1", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe rejected {path}:\n{done.stderr[-800:]}")
    return dict(
        line.split("=", 1) for line in done.stdout.splitlines() if "=" in line
    )


def packet_count(path: Path, stream: str) -> int:
    done = run([
        "ffprobe", "-v", "error", "-select_streams", stream,
        "-show_entries", "packet=pts", "-of", "csv=p=0", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe rejected {path}:\n{done.stderr[-800:]}")
    return len([line for line in done.stdout.splitlines() if line.strip()])


def remux(nut: Path, mp4: Path) -> None:
    """The packets a rung came back as, into the container a recipe writes."""
    done = run(["ffmpeg", "-v", "error", "-y", "-i", str(nut), "-c", "copy", str(mp4)],
               TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"remuxing {nut} failed:\n{done.stderr[-800:]}")


def duration(path: Path) -> float:
    done = run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "csv=p=0", str(path)], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe rejected {path}:\n{done.stderr[-800:]}")
    return float(done.stdout.strip())


def one_run(rungs: int, keep: bool) -> None:
    dir = Path(tempfile.mkdtemp(prefix="moq-roundtrip-"))
    relay: subprocess.Popen | None = None
    readers: list[subprocess.Popen] = []
    try:
        certgen = build_guests(BUILD_DEADLINE)[2]
        made = run([certgen, dir], TOOL_DEADLINE, capture_output=True, text=True)
        if made.returncode != 0:
            sys.exit(f"certgen failed:\n{made.stderr[-400:]}")
        cert_hex = made.stdout.strip()
        source = dir / "source.mp4"
        make_source(source)
        out = dir / "out"
        out.mkdir()

        port = free_udp_port()
        relay = start_relay(port, dir / "cert.pem", dir / "key.pem", dir / "relay.log")

        # One track per rung plus the file's audio, so one nut apiece.
        tracks = rungs + 1
        nuts = [out / f"track{index}.nut" for index in range(tracks)]
        # sub-recv reads the same broadcast beside the module: its
        # fragment counts are what the module's packet counts are judged
        # against, and its transcript carries the catalog.
        readers = [
            start_subscriber(guest("sub-recv"), port, cert_hex, out, BROADCAST,
                             rendition=index, output=f"recv{index}.mp4")
            for index in range(tracks)
        ]
        # Every reader goes up before the publisher, waiting for the
        # broadcast to be announced: the module publishing holds its
        # first media until something has asked for a rendition, and a
        # reader that arrives after that starts wherever it lands.
        module = start_source(guest("subscribe"), port, cert_hex, BROADCAST, nuts)
        readers.append(module)
        publisher = start_publish(
            "publish-ladder.sql", port, cert_hex, BROADCAST,
            ["-v", f"source={source}", "-v", f"rungs={rungs}",
             "-v", "widths=" + ",".join(str(w) for w in WIDTHS[:rungs]),
             "-v", "bitrates=" + ",".join(BITRATES[:rungs])],
        )
        readers.append(publisher)
        rows = wait_publish(publisher, RUN_DEADLINE)
        readers.remove(publisher)
        readers.remove(module)
        wait_source(module, READER_DEADLINE)
        transcripts = [wait_subscriber(reader, READER_DEADLINE) for reader in readers]
        readers = []

        summaries = [row for row in rows if "groups" in row]
        assert len(summaries) == 1, f"expected one summary row, got {len(summaries)}"
        assert summaries[0]["tracks"] == tracks, (
            f"the ladder published {summaries[0]['tracks']} tracks, and {tracks} "
            "were subscribed to"
        )

        catalog = catalog_of(transcripts[0])
        video = catalog["video"]["renditions"]
        audio = catalog["audio"]["renditions"]
        # The order both readers count renditions in: the document's own.
        ordered = [*sorted(video), *sorted(audio)]
        assert len(ordered) == tracks, f"the catalog names {ordered}"
        print("--- catalog ---")
        print(json.dumps({"video": sorted(video), "audio": sorted(audio)}))

        widest: tuple[int, Path] | None = None
        for index, name in enumerate(ordered):
            nut = nuts[index]
            assert nut.exists(), f"the module wrote no output for {name}"
            reassembled = fragments_received(transcripts[index])
            if name in audio:
                read = probe(nut, "a:0", "codec_name,sample_rate,channels")
                assert read["codec_name"] == "aac", f"{name} came back as {read}"
                assert int(read["sample_rate"]) == SAMPLE_RATE, read
                assert int(read["channels"]) == CHANNELS, read
                packets = packet_count(nut, "a:0")
            else:
                read = probe(nut, "v:0", "codec_name,width,height")
                assert read["codec_name"] == "h264", f"{name} came back as {read}"
                assert int(read["width"]) == video[name]["codedWidth"], (
                    f"{name} came back {read['width']} wide, and the catalog says "
                    f"{video[name]['codedWidth']}"
                )
                assert int(read["height"]) == video[name]["codedHeight"], read
                packets = packet_count(nut, "v:0")
                if widest is None or int(read["width"]) > widest[0]:
                    widest = (int(read["width"]), nut)
            # A subscription joins at a group boundary, and which group
            # that is depends on when each reader arrived - so the two
            # counts agree up to whole groups, not exactly.
            assert packets > 0, f"{name} came back with no packets"
            assert abs(packets - reassembled) <= 2 * GOP, (
                f"{name}: the module read {packets} packets and sub-recv "
                f"reassembled {reassembled} fragments"
            )
            mp4 = out / f"{index}.mp4"
            remux(nut, mp4)
            seconds = duration(mp4)
            assert seconds > SECONDS / 2, (
                f"{name} came back {seconds:.1f}s of a {SECONDS}s broadcast"
            )
            print(f"PASS {name}: {read}, {packets} packets, {seconds:.1f}s")

        assert widest is not None, "the ladder published no video"
        chain(widest[1], port, cert_hex, out)
        print(f"PASS: {tracks} renditions read back, and one of them relayed")
    finally:
        for reader in readers:
            kill_tree(reader)
        if relay is not None:
            kill_tree(relay)
        if keep:
            print(f"kept: {dir}")
        else:
            shutil.rmtree(dir, ignore_errors=True)


def chain(rung: Path, port: int, cert_hex: str, out: Path) -> None:
    """The relay chain: what came back, published again under a new name.

    `recipes/relay.sql` is this in one query, and cannot compile until a
    packet source may be probed over the network; the two halves it is
    made of are run one after the other here instead.
    """
    copy = out / "copy.nut"
    reader = start_source(guest("subscribe"), port, cert_hex, COPY, [copy])
    publisher = start_publish("publish.sql", port, cert_hex, COPY,
                              ["-v", f"source={rung}"])
    rows = wait_publish(publisher, RUN_DEADLINE)
    wait_source(reader, READER_DEADLINE)
    assert [row for row in rows if "groups" in row], "the copy published no summary"
    assert copy.exists(), "the copy came back with no track"
    read = probe(copy, "v:0", "codec_name,width,height")
    original = probe(rung, "v:0", "codec_name,width,height")
    assert read["codec_name"] == "h264", read
    assert (read["width"], read["height"]) == (original["width"], original["height"]), (
        f"the copy came back {read['width']}x{read['height']}, and it went out "
        f"{original['width']}x{original['height']}"
    )
    print(f"PASS chain: {COPY} carries {read['width']}x{read['height']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rungs", type=int, default=2, choices=(2, 3))
    parser.add_argument("--keep", action="store_true", help="keep the work directory")
    args = parser.parse_args()

    if not SIDECAR.exists():
        sys.exit(f"no sidecar at {SIDECAR}; build it first (cargo build --release)")
    started = time.monotonic()
    one_run(args.rungs, args.keep)
    print(f"=== the roundtrip took {time.monotonic() - started:.1f}s ===")


if __name__ == "__main__":
    main()
