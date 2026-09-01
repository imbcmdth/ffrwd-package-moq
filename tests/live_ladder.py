"""The rendition ladder, end to end, on this machine - run it by hand.

    python tests/live_ladder.py [--rungs N] [--audio] [--keep]

The claim under test: ONE decode, N encodes, ONE broadcast. Compiles this
package's `publish-ladder` recipe with the real compiler and runs it - ONE
ffmpeg splits the decoded source and encodes a rung apiece at its own
bitrate, one sidecar hosts one publish.wasm instance reading all of them,
and a local moq-relay carries the single broadcast. Then N sub-recv guests
subscribe, each reading `catalog.json` to find the tracks and taking a
different one, reassembling its fmp4 to a file ffprobe must accept at that
rung's own size.

With `--audio` the source carries a tone as well and the run goes through
`publish-ladder-audio` and publish_av.wasm: the SAME broadcast then carries
one more track, the encoded audio as `mp4a` fragments, and one more
subscriber takes it off the same catalog and must get a stream ffprobe
decodes as AAC at the rate and channel count the catalog stated. The audio
group is a duration, not a keyframe - every AAC frame is a sync sample - so
what is asserted about it is the frame count and the group count that
duration implies, not that its boundaries fall where the video's do.

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
    build_hang_recv,
    ffprobe_frames,
    free_udp_port,
    kill_tree,
    run,
    start_hang_recv,
    start_relay,
    start_subscriber,
    wait_hang_recv,
    wait_subscriber,
)

SECONDS = 8
RATE = 30
GOP = 30
BROADCAST = "live/ladder"
# One rung per entry, widest first; the heights follow from -2 scaling.
WIDTHS = (854, 640, 426)
BITRATES = ("2000k", "1000k", "400k")

# The tone --audio adds, and what an AAC-LC encode of it comes to: a
# frame is 1024 samples whatever the rate, and the muxer closes an audio
# group once it spans a second - so the first frame a whole second past
# the group's start is the one that closes it.
SAMPLE_RATE = 48000
CHANNELS = 2
AAC_FRAME = 1024
AUDIO_PACKETS = SECONDS * SAMPLE_RATE // AAC_FRAME
AUDIO_GROUP_FRAMES = -(-SAMPLE_RATE // AAC_FRAME)
AUDIO_GROUPS = -(-AUDIO_PACKETS // AUDIO_GROUP_FRAMES)

BUILD_DEADLINE = 600
COMPILE_DEADLINE = 180
RUN_DEADLINE = 240
SUBSCRIBER_DEADLINE = 180
TOOL_DEADLINE = 60


def make_source(path: Path, audio: bool) -> None:
    argv = [
        "ffmpeg", "-v", "error", "-y",
        "-f", "lavfi", "-i", f"testsrc2=size=1280x720:rate={RATE}",
    ]
    if audio:
        argv += [
            "-f", "lavfi", "-i", f"sine=frequency=440:sample_rate={SAMPLE_RATE}",
            "-ac", str(CHANNELS), "-c:a", "aac",
        ]
    argv += [
        "-t", str(SECONDS), "-c:v", "libx264", "-preset", "ultrafast",
        "-pix_fmt", "yuv420p", str(path),
    ]
    done = run(argv, TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"building the source failed:\n{done.stderr[-800:]}")


def recipe_args(
    source: Path, port: int, cert_hex: str, rungs: int, audio: bool
) -> list[str]:
    recipe = "publish-ladder-audio.sql" if audio else "publish-ladder.sql"
    return [
        "-f", str(PACKAGE / "recipes" / recipe),
        "-v", f"source={source}",
        "-v", f"relay=moqt://127.0.0.1:{port}",
        "-v", f"broadcast={BROADCAST}",
        "-v", f"rungs={rungs}",
        "-v", "widths=" + ",".join(str(w) for w in WIDTHS[:rungs]),
        "-v", "bitrates=" + ",".join(BITRATES[:rungs]),
        "-v", f"cert={cert_hex}",
    ]


def run_publisher(
    source: Path, port: int, cert_hex: str, rungs: int, audio: bool
) -> tuple[list[dict], str]:
    """Compiles and runs the ladder recipe through the real compiler."""
    env = dict(os.environ)
    env["FFRWD_WASM"] = str(SIDECAR)
    args = recipe_args(source, port, cert_hex, rungs, audio)
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
    # One decode: every leg - the rungs through a split, the audio mapped
    # bare - leaves ONE ffmpeg, not one apiece each opening the source again.
    if f"split={rungs}" not in shown.stdout:
        sys.exit(f"the one decode must split {rungs} ways, and this plan does not")
    if shown.stdout.count("ffmpeg -i") != 1:
        sys.exit("the ladder must decode its source ONCE, and this plan opens it more")
    if audio and shown.stdout.count("-c:0 aac") != 1:
        # The audio reaches the sink on a pad of its own, encoded on the way
        # in rather than as the pcm every other audio edge carries.
        sys.exit("the audio must reach the sink as ONE encoded pad")

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


def one_run(
    sub_wasm: Path, hang_recv: Path, certgen: Path, rungs: int, audio: bool, keep: bool
) -> None:
    dir = Path(tempfile.mkdtemp(prefix="moq-ladder-"))
    relay: subprocess.Popen | None = None
    subs: list[subprocess.Popen] = []
    try:
        made = run([certgen, dir], TOOL_DEADLINE, capture_output=True, text=True)
        if made.returncode != 0:
            sys.exit(f"certgen failed:\n{made.stderr[-400:]}")
        cert_hex = made.stdout.strip()
        source = dir / "source.mp4"
        make_source(source, audio)
        out_dir = dir / "out"
        out_dir.mkdir()

        port = free_udp_port()
        relay = start_relay(port, dir / "cert.pem", dir / "key.pem", dir / "relay.log")
        # One subscriber per rung, each choosing its own track OFF THE
        # CATALOG rather than being told a name.
        # The audio track is the last the catalog names, so one more reader
        # at index `rungs` takes it - off the same catalog, chosen the same
        # way the rungs are.
        readers = rungs + 1 if audio else rungs
        subs = [
            start_subscriber(
                sub_wasm, port, cert_hex, out_dir, BROADCAST,
                rendition=index, output=f"recv{index}.mp4",
            )
            for index in range(readers)
        ]
        # Beside ours, the THIRD-PARTY subscriber: hang's stack reading the
        # widest rung (and the audio, when there is one) off the same
        # broadcast through its own catalog parser and CMAF depacketizer.
        widest = f"video.{round(WIDTHS[0] * 720 / 1280 / 2) * 2}p" if rungs > 1 else "video"
        hang = start_hang_recv(
            hang_recv, port, dir / "cert.pem", out_dir / "hang.mp4", BROADCAST,
            video=widest, audio="audio" if audio else None,
        )
        subs.append(hang)
        rows, _ = run_publisher(source, port, cert_hex, rungs, audio)
        hang_transcript = wait_hang_recv(subs.pop(), SUBSCRIBER_DEADLINE)
        transcripts = [wait_subscriber(sub, SUBSCRIBER_DEADLINE) for sub in subs]

        group_rows = [r for r in rows if "group" in r]
        summaries = [r for r in rows if "groups" in r]
        assert group_rows, "the module emitted no group rows"
        assert len(summaries) == 1, f"expected one summary row, got {len(summaries)}"
        summary = summaries[0]
        assert summary["tracks"] == readers, (
            f"one broadcast must carry {readers} tracks, and the summary says "
            f"{summary['tracks']}"
        )
        published = sorted({row["track"] for row in group_rows})
        assert len(published) == readers, (
            f"{readers} tracks must leave on {readers} distinct names, "
            f"got {published}"
        )
        expected_frames = SECONDS * RATE

        catalog = catalog_of(transcripts[0])
        print("--- catalog ---")
        print(json.dumps(catalog, indent=2))
        # hang's shape: renditions are name-keyed maps under video/audio,
        # camelCase fields, the init segment base64 inside a cmaf container.
        video_entries = catalog["video"]["renditions"]
        audio_entries = catalog["audio"]["renditions"]
        # The heights follow from -2 scaling of the 1280x720 source:
        # proportional, rounded to the nearest even.
        sizes = {
            f"video.{round(width * 720 / 1280 / 2) * 2}p": width
            for width in WIDTHS[:rungs]
        }
        assert sorted(video_entries) == sorted(sizes), (
            f"the catalog names {sorted(video_entries)}, the rungs are {sorted(sizes)}"
        )
        for name, entry in video_entries.items():
            assert entry["codedWidth"] == sizes[name], (
                f"catalog {name} says {entry['codedWidth']}, rung is {sizes[name]}"
            )
            assert entry["codec"].startswith("avc1."), entry["codec"]
            assert entry["container"]["kind"] == "cmaf", entry
            assert entry["container"]["init"], f"{name} carries no init segment"
        if audio:
            entry = audio_entries["audio"]
            assert entry["codec"].startswith("mp4a.40."), entry["codec"]
            assert entry["sampleRate"] == SAMPLE_RATE, entry
            assert entry["numberOfChannels"] == CHANNELS, entry
            assert entry["container"]["kind"] == "cmaf", entry
            assert entry["container"]["init"], "audio carries no init segment"
        else:
            assert not audio_entries, f"no audio published, yet {audio_entries}"
        assert sorted(published) == sorted([*video_entries, *audio_entries]), (
            "the catalog's tracks are the ones the rows named"
        )
        # The order a subscriber counts renditions in: the document's own -
        # video sorted by name, then audio.
        ordered = [*sorted(video_entries), *sorted(audio_entries)]

        video_groups = expected_frames // GOP
        for name in ordered:
            kind = "audio" if name in audio_entries else "video"
            groups = [row for row in group_rows if row["track"] == name]
            wanted = AUDIO_GROUPS if kind == "audio" else video_groups
            assert abs(len(groups) - wanted) <= 1, (
                f"{name}: {len(groups)} groups, and {wanted} is what "
                f"a {SECONDS}s {kind} stream comes to"
            )
            packets = sum(row["packets"] for row in groups)
            if kind == "audio":
                # ffmpeg's aac encoder may add a priming frame or two.
                assert abs(packets - AUDIO_PACKETS) <= 3, (
                    f"{name}: {packets} AAC frames, and {SECONDS}s at "
                    f"{SAMPLE_RATE} Hz is {AUDIO_PACKETS} of {AAC_FRAME} samples"
                )
            else:
                assert packets == expected_frames, (
                    f"{name}: {packets} packets for {expected_frames} frames"
                )

        # Every subscriber must have reassembled ITS OWN track - a rung at
        # its own size, the audio at its own rate - from a contiguous run of
        # that track's groups ending at the last.
        from_the_start = 0
        for index, transcript in enumerate(transcripts):
            name = ordered[index]
            kind = "audio" if name in audio_entries else "video"
            assert f"sub: subscribing to {name}" in transcript, (
                f"subscriber {index} read the wrong track"
            )
            # Arrival order may scramble over a backlog - the relay serves
            # groups on parallel streams - and the reader writes them back
            # in group order, so that is the order judged here.
            received = sorted(groups_received(transcript))
            published_here = [r["group"] for r in group_rows if r["track"] == name]
            assert received, f"{name} received nothing"
            assert received == list(range(received[0], received[-1] + 1)), (
                f"{name} received {received}, which has a hole in it"
            )
            assert received[-1] == max(published_here), (
                f"{name} stopped at group {received[-1]}, and it published "
                f"through {max(published_here)}"
            )
            if received[0] == 0:
                from_the_start += 1
            path = out_dir / f"recv{index}.mp4"
            if kind == "audio":
                codec, rate, channels, frames = probe_audio(path)
                assert codec == "aac", f"{name} reassembled as {codec}"
                assert (rate, channels) == (SAMPLE_RATE, CHANNELS), (
                    f"{name} reassembled at {rate} Hz {channels}ch, and the "
                    f"catalog said {SAMPLE_RATE} Hz {CHANNELS}ch"
                )
                # Whole groups, and a group holds the frames its duration does.
                wanted = len(received) * AUDIO_GROUP_FRAMES
                assert abs(frames - wanted) <= AUDIO_GROUP_FRAMES, (
                    f"{name}: {frames} AAC frames over {len(received)} groups "
                    f"of {AUDIO_GROUP_FRAMES}"
                )
                print(
                    f"PASS audio: {name} at {rate} Hz {channels}ch, {frames} "
                    f"frames from group {received[0]}"
                )
                continue
            frames = ffprobe_frames(path, TOOL_DEADLINE)
            assert frames == len(received) * GOP, (
                f"{name}: {frames} frames over {len(received)} groups"
            )
            size = probe_size(path)
            assert size[0] == sizes[name], (
                f"{name} reassembled at {size[0]}x{size[1]}, not {sizes[name]} wide"
            )
            print(
                f"PASS rung {index}: {name} at {size[0]}x{size[1]}, {frames} frames "
                f"from group {received[0]}"
            )
        assert from_the_start, (
            "no reader captured from group 0, so nothing proves the first group "
            "is reachable at all"
        )

        # What hang's own pipeline wrote must decode at the geometry it chose
        # off our catalog - their parser and container code, our broadcast.
        assert f"hang: selecting video {widest}" in hang_transcript
        size = probe_size(out_dir / "hang.mp4")
        assert size[0] == sizes[widest], (
            f"hang reassembled at {size[0]}x{size[1]}, not {sizes[widest]} wide"
        )
        if audio:
            codec, rate, channels, _ = probe_audio(out_dir / "hang.mp4")
            assert codec == "aac", f"hang reassembled audio as {codec}"
            assert (rate, channels) == (SAMPLE_RATE, CHANNELS), (
                f"hang reassembled audio at {rate} Hz {channels}ch"
            )
        print(
            f"PASS hang: their stack decoded {widest} at {size[0]}x{size[1]}"
            + (", audio included" if audio else "")
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


def probe_audio(path: Path) -> tuple[str, int, int, int]:
    """One reassembled audio file as ffprobe reads it: codec, rate,
    channels and the frames it decoded."""
    done = run([
        "ffprobe", "-v", "error", "-count_frames", "-select_streams", "a:0",
        "-show_entries", "stream=codec_name,sample_rate,channels,nb_read_frames",
        "-of", "default=nw=1", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe rejected {path}:\n{done.stderr[-800:]}")
    print("--- ffprobe (audio) ---")
    print(done.stdout.strip())
    read = dict(
        line.split("=", 1) for line in done.stdout.splitlines() if "=" in line
    )
    return (
        read["codec_name"],
        int(read["sample_rate"]),
        int(read["channels"]),
        int(read["nb_read_frames"]),
    )


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
    parser.add_argument(
        "--audio", action="store_true", help="publish the source's audio too"
    )
    parser.add_argument("--keep", action="store_true", help="keep the work directory")
    args = parser.parse_args()

    if not SIDECAR.exists():
        sys.exit(f"no sidecar at {SIDECAR}; build it first (cargo build --release)")
    _, sub_wasm, certgen = build_guests(BUILD_DEADLINE)
    hang_recv = build_hang_recv(BUILD_DEADLINE)
    started = time.monotonic()
    one_run(sub_wasm, hang_recv, certgen, args.rungs, args.audio, args.keep)
    print(f"=== the ladder took {time.monotonic() - started:.1f}s ===")


if __name__ == "__main__":
    main()
