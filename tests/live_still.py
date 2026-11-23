"""The still oracle: a reader that holds a still keeps its destination fed.

    python tests/live_still.py [--warmup N] [--kill-gap N] [--end-gap N]
                               [--restart-s N] [--work DIR] [--no-build] [--keep]

An egress piece copies a program's broadcast on to an RTMP push. A service
that hears nothing on a push for long enough closes it, and when the program
comes back its media starts a new broadcast there. With `still => 1` the
reader goes on handing media on while the publisher is away: the picture's
last keyframe again every 500 ms and silent AAC frames, so the push never
goes without.

The shape of a run is `tests/live_restart.py`'s: a local moq-relay, PACED
publishers of one rung with its audio, a data track and a text track, one
after another, and a reader in the egress shape (`subscribe` stream-copied to
an FLV under `ffrwd run`), here with `still => 1` and `restart_s` set. The
first publisher is killed outright after `--warmup` seconds of media, every
process of it at once, so nothing tells the reader it went: the stills start
once no media has come for a second. A second publisher starts `--kill-gap`
seconds later and runs its file to the end, which finishes its tracks, and
the reader holds a still while it waits for the next. A third starts
`--end-gap` seconds after that and runs to its end, after which the reader
holds a still for `restart_s` and ends.

Every source carries a white frame at each whole second of its own time and a
click on the same second of its sound, as `live_restart.py`'s do, so how sound
and picture line up can be read off the FLV on either side of each gap. Its
keyframes fall half a second off the white frames, and a small mark on every
frame says which publisher made it, so no two of the publishers' keyframes
are the same bytes and a still is told by being the last keyframe again.

What it owes:

  - the reader runs until its `restart_s` after the last publisher is spent,
    ends after the last publisher, and exits 0;
  - in the FLV, decode times go forward on both tracks, no two video packets
    are more than 600 ms apart, and no two audio packets are more than two
    AAC frames apart, which is a hole of under one frame: from the first
    packet to the last nothing is missing;
  - three stretches of stills, one each time the publisher was away: video
    packets each byte for byte the last keyframe before the stretch, each
    flagged a keyframe, 500 ms apart; and audio packets that are the silent
    AAC frame, which decode to samples of zero. Each stretch has as many of
    each as the reader's stderr says it handed on;
  - each stretch covers as much media time as has passed since the
    publisher's last packet by the reader's own account (how long before
    the first still went out, and how long it held them): the last within
    an interval, held for `restart_s` and a little more; one that a
    publisher ends, less the moment its first packets are held while the
    reader reads their shift, and more by the lead its first keyframe
    takes past the last still;
  - the first video packet after each gap is a keyframe, and the clicks sit
    against the white frames after each gap where they sat before it,
    within one audio frame;
  - none of the publishers' media is dropped for landing behind a still:
    `behind` is 0 in each track's last row;
  - the reader says each restart in a row, `replaced` and then `finished`,
    each with how long it held a still and how many stills it handed on.

Never collected by any suite: it needs moq-relay, ffmpeg, ffprobe, cargo with
the wasm32-wasip2 target and a wasi-sdk clang, and the sidecar. It starts a
relay, three publishers and a reader of its own and stops those alone.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from common import (
    PACKAGE,
    SIDECAR,
    build_guests,
    free_udp_port,
    guest,
    kill_tree,
    node_shape,
    run,
    start_relay,
)
from live_restart import (
    BROADCAST,
    Publisher,
    clicks,
    cut_cues,
    cut_messages,
    flashes,
    kill_outright,
    media_clock,
    offsets,
    start_reader,
    wait_until,
)

RATE = 30
SAMPLE_RATE = 48000
AUDIO_FRAME = 1024 / SAMPLE_RATE
WARMUP = 12.0
KILL_GAP = 6.0
END_GAP = 5.0
SECONDS = (120, 30, 20)
RESTART_S = 8
BUILD_DEADLINE = 1800
TOOL_DEADLINE = 300
INTERVAL = 0.5
# The longest two video packets may be apart: an interval, and the frame a
# picture's still is long.
VIDEO_GAP_MAX = 0.6
# FLV counts in milliseconds.
ROUNDING = 0.0015
# How much more media time a stretch of stills ending at a successor may
# cover than the time since the publisher's last packet: the successor's
# first keyframe lands past the last still, up to a GOP and an interval on.
LEAD = 1.6
# And how much less: the reader holds the successor's first packets for up
# to two seconds while it reads the shift off them, and the stills stop
# when the first of them arrives (see moq_core::restart::SETTLE).
SETTLE = 2.0
# The silent frame for stereo AAC-LC, which is what the sources carry; see
# moq_core::still::silent_aac.
SILENT = bytes([0x21, 0x10, 0x05, 0x00, 0xA0, 0x1C])

QUERY = """
COPY (
  SELECT s.*
  FROM ffrwd.moq.subscribe(:'subscribe_relay', :'subscribe_broadcast', :'subscribe_cert',
                           restart_s => :restart_s, still => 1) s
) TO :'destination'
"""


def make_source(path: Path, seconds: int, index: int) -> None:
    """A test pattern with a white frame at each whole second and a click on
    each whole second of the sound, as `live_restart.make_source`'s, with a
    mark that says which publisher it is and keyframes half a second off the
    white frames."""
    graph = (
        "[0:v]drawbox=x=0:y=0:w=iw:h=ih:color=white:t=fill:"
        "enable='lt(mod(t\\,1)\\,0.02)',"
        f"drawbox=x={8 + 24 * index}:y=8:w=16:h=16:color=black:t=fill[v]"
    )
    done = run([
        "ffmpeg", "-v", "error", "-y",
        "-f", "lavfi", "-i", f"testsrc2=size=640x360:rate={RATE}",
        "-f", "lavfi", "-i",
        f"aevalsrc=exprs='0.6*sin(2*PI*1000*t)*lt(mod(t\\,1)\\,0.03)':s={SAMPLE_RATE}:c=stereo",
        "-filter_complex", graph, "-map", "[v]", "-map", "1:a",
        "-t", str(seconds), "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
        "-b:v", "800k", "-g", str(RATE), "-sc_threshold", "0",
        "-force_key_frames", "expr:gte(t,n_forced+0.5)",
        "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "128k",
        "-f", "nut", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"building the source failed:\n{done.stderr[-800:]}")


def hashed(path: Path, stream: str) -> list[dict]:
    """A track's packets in the FLV: times, keyframe flag and an MD5 of the
    bytes."""
    done = run(["ffprobe", "-v", "error", "-select_streams", stream, "-show_data_hash", "md5",
                "-show_entries", "packet=pts_time,dts_time,duration_time,flags,data_hash",
                "-of", "json", str(path)], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe {path.name}:\n{done.stderr[-600:]}")
    out = []
    for packet in json.loads(done.stdout).get("packets", []):
        out.append({
            "dts": float(packet["dts_time"]),
            "pts": float(packet["pts_time"]),
            "key": "K" in packet.get("flags", ""),
            "hash": packet["data_hash"].split(":", 1)[1],
        })
    return out


def runs(found: list[dict], still) -> list[list[int]]:
    """The indices of a track's stills, in runs of consecutive packets."""
    out: list[list[int]] = []
    for index, packet in enumerate(found):
        if not still(index, packet):
            continue
        if out and out[-1][-1] == index - 1:
            out[-1].append(index)
        else:
            out.append([index])
    return out


def pcm(path: Path) -> memoryview:
    """The FLV's sound as mono samples, each at its own time from 0: a hole
    would be filled with silence, which the gap check rules out."""
    done = subprocess.run(["ffmpeg", "-v", "error", "-copyts", "-i", str(path), "-map", "0:a:0",
                           "-af", "aresample=async=1:min_hard_comp=0.001:first_pts=0",
                           "-ac", "1", "-ar", str(SAMPLE_RATE), "-f", "s16le", "-"],
                          capture_output=True, timeout=TOOL_DEADLINE)
    return memoryview(done.stdout).cast("h")


def said(lines: list[str]) -> list[tuple[str, float, int, int]]:
    """Each stretch of stills as the reader's stderr ends it: what ended it,
    how long it was held, and how many pictures and silent frames."""
    out = []
    for line in lines:
        found = re.search(r"subscribe: (.*) after ([\d.]+)s of stills: (\d+) pictures, "
                          r"(\d+) silent frames", line)
        if found:
            out.append((found[1], float(found[2]), int(found[3]), int(found[4])))
    return out


def judge(out: Path, reader, ended: dict, restart_s: int) -> list[str]:
    problems: list[str] = []
    stderr = reader.module_stderr().split("\n")
    started = [line for line in stderr if "holding a still until the publisher is back" in line]
    stopped = said(stderr)
    for line in started:
        print("reader:", line.strip(), flush=True)
    for ending, held, pictures, frames in stopped:
        print(f"reader: {ending} after {held:.1f}s: {pictures} pictures, {frames} silent frames",
              flush=True)
    endings = [ending for ending, _, _, _ in stopped]
    if len(started) != 3 or endings != ["the publisher's media goes on"] * 2 + ["the run ends"]:
        problems.append(f"the reader started {len(started)} stretches of stills and ended "
                        f"{endings}, not three ended by two publishers and the run")
    rows = [row for row in reader.counters() if row.get("kind") == "restart"]
    for row in rows:
        print("reader row:", json.dumps(row), flush=True)
    if [row.get("seen") for row in rows] != ["replaced", "finished"]:
        problems.append(f"the reader's restart rows say {[row.get('seen') for row in rows]}")
    for row in rows:
        if not row.get("still_ms") or not row.get("stills"):
            problems.append(f"a restart row says no still: {row}")
    # A publisher's media that landed behind a still would have been dropped,
    # and counted in its track's `behind`.
    finals = [row for row in reader.counters() if row.get("kind") == "track" and row.get("final")]
    behind = {row.get("track"): row.get("behind") for row in finals}
    print(f"  media dropped behind a still, by track: {behind}", flush=True)
    if not finals or any(behind.values()):
        problems.append(f"the tracks' last rows say {behind} packets were dropped behind")
    if problems:
        return problems

    video, audio = hashed(out, "v:0"), hashed(out, "a:0")
    for name, found in (("video", video), ("audio", audio)):
        back = [(a["dts"], b["dts"]) for a, b in zip(found, found[1:]) if b["dts"] <= a["dts"]]
        if back:
            problems.append(f"{name}: {len(back)} decode times that do not go forward, "
                            f"the first {back[0]}")
    video_gap = max(b["dts"] - a["dts"] for a, b in zip(video, video[1:]))
    audio_gap = max(b["dts"] - a["dts"] for a, b in zip(audio, audio[1:])) - AUDIO_FRAME
    print(f"  video: {video[0]['dts']:.3f}s to {video[-1]['dts']:.3f}s, {len(video)} packets, "
          f"the most apart {video_gap * 1000:.1f} ms", flush=True)
    print(f"  audio: {audio[0]['dts']:.3f}s to {audio[-1]['dts']:.3f}s, {len(audio)} packets, "
          f"the largest hole {max(audio_gap, 0) * 1000:.1f} ms", flush=True)
    if video_gap > VIDEO_GAP_MAX + ROUNDING:
        problems.append(f"video: two packets {video_gap:.3f}s apart")
    if audio_gap > AUDIO_FRAME + ROUNDING:
        problems.append(f"audio: a hole of {audio_gap * 1000:.1f} ms, over one frame")

    # A still is a keyframe the same bytes as the publisher's last keyframe
    # before it: the publishers' own keyframes are never the same twice. A
    # still that was not would split its stretch, or go uncounted.
    stills, last_key = set(), None
    for index, packet in enumerate(video):
        if packet["key"] and packet["hash"] == last_key:
            stills.add(index)
        elif packet["key"]:
            last_key = packet["hash"]
    pictures = runs(video, lambda index, _: index in stills)
    silent = hashlib.md5(SILENT).hexdigest()
    frames = runs(audio, lambda _, packet: packet["hash"] == silent)
    print(f"  stills: {[len(run) for run in pictures]} pictures and "
          f"{[len(run) for run in frames]} silent frames, by stretch", flush=True)
    if len(pictures) != 3 or len(frames) != 3:
        problems.append(f"{len(pictures)} stretches of still pictures and {len(frames)} of "
                        f"silent sound, not three")
        return problems
    samples = pcm(out)
    for stretch, (picture, sound) in enumerate(zip(pictures, frames)):
        apart = [video[b]["dts"] - video[a]["dts"] for a, b in zip(picture, picture[1:])]
        if any(abs(step - INTERVAL) > ROUNDING for step in apart):
            problems.append(f"stretch {stretch}: stills {min(apart):.3f}s to {max(apart):.3f}s "
                            f"apart, not {INTERVAL}s")
        ending, held, owed_pictures, owed_frames = stopped[stretch]
        if (len(picture), len(sound)) != (owed_pictures, owed_frames):
            problems.append(f"stretch {stretch}: {len(picture)} pictures and {len(sound)} silent "
                            f"frames, where the reader says {owed_pictures} and {owed_frames}")
        # From the end of the publisher's sound to its next packet, or for
        # the last stretch to the end of the stills.
        start = audio[sound[0]]["dts"]
        if sound[-1] + 1 < len(audio):
            end = audio[sound[-1] + 1]["dts"]
            after = video[picture[-1] + 1] if picture[-1] + 1 < len(video) else None
            if after is None or not after["key"]:
                problems.append(f"stretch {stretch}: the first video packet after it is not a "
                                f"keyframe")
            else:
                print(f"  stretch {stretch}: the publisher's picture follows the last still by "
                      f"{(after['dts'] - video[picture[-1]]['dts']) * 1000:.1f} ms and its sound "
                      f"the last silent frame by "
                      f"{(end - audio[sound[-1]]['dts'] - AUDIO_FRAME) * 1000:.1f} ms",
                      flush=True)
        else:
            end = audio[sound[-1]]["dts"] + AUDIO_FRAME
        # The stills start where the publisher's media ended, and run at the
        # pace of the wall clock: the media they cover is as long as the
        # time since the publisher's last packet, that before the first
        # still went out and the time the reader held them.
        span = end - start
        quiet = re.search(r"no media for ([\d.]+)s", started[stretch])
        since = (float(quiet[1]) if quiet else 0.0) + held
        print(f"  stretch {stretch}: stills from {start:.3f}s to {end:.3f}s, {span:.3f}s of "
              f"media, {since:.1f}s after the publisher's last packet", flush=True)
        if ending == "the run ends":
            if not restart_s - INTERVAL <= held <= restart_s + 2:
                problems.append(f"the last stretch of stills was held {held:.1f}s, where "
                                f"restart_s is {restart_s}s")
            if abs(span - since) > INTERVAL:
                problems.append(f"the last stretch of stills is {span:.3f}s of media over "
                                f"{since:.1f}s")
        elif not -SETTLE <= span - since <= LEAD:
            problems.append(f"stretch {stretch}: {span:.3f}s of media for stills over "
                            f"{since:.1f}s")
        # The first frames after the publisher's sound carry its tail.
        lo, hi = int((start + 2 * AUDIO_FRAME) * SAMPLE_RATE), int(end * SAMPLE_RATE)
        loud = max((abs(value) for value in samples[lo:hi]), default=None)
        print(f"  stretch {stretch}: the silence decodes to {hi - lo} samples, the loudest "
              f"{loud}", flush=True)
        if loud != 0:
            problems.append(f"stretch {stretch}: the silent frames decode to samples as loud "
                            f"as {loud}")
    if problems:
        return problems

    # Where the clicks sit against the white frames in each stretch of the
    # publishers' own media, between the stills.
    flash, click = flashes(out), clicks(out)
    bounds = [audio[0]["dts"]]
    for sound in frames:
        bounds += [audio[sound[0]]["dts"], audio[min(sound[-1] + 1, len(audio) - 1)]["dts"]]
    lined = []
    for index in range(3):
        start, end = bounds[2 * index], bounds[2 * index + 1]
        found = offsets(flash, click, start, end)
        if len(found) < 3:
            problems.append(f"only {len(found)} clicks found against white frames from "
                            f"{start:.3f}s to {end:.3f}s")
            return problems
        lined.append(statistics.median(found))
    print("  click less white frame, by stretch of the publishers' media: "
          + ", ".join(f"{value * 1000:.1f} ms" for value in lined), flush=True)
    for gap in (1, 2):
        moved = lined[gap] - lined[gap - 1]
        row = rows[gap - 1]
        print(f"  gap {gap} ({row['seen']}): away {row['away_ms']} ms, shift "
              f"{row['shift_us'] / 1e6:+.3f}s, still held {row['still_ms']} ms, {row['stills']} "
              f"stills; sound against picture moved {moved * 1000:+.1f} ms", flush=True)
        if abs(moved) > AUDIO_FRAME:
            problems.append(f"gap {gap}: the clicks moved {moved * 1000:+.1f} ms against the "
                            f"white frames, over one audio frame")
    if ended["reader"] < ended["last publisher"]:
        problems.append("the reader ended before the last publisher did")
    return problems


def one_run(args: argparse.Namespace) -> None:
    work = Path(tempfile.mkdtemp(prefix="moq-still-", dir=args.work))
    relay = None
    publishers: list[Publisher] = []
    reader = None
    try:
        if args.build:
            certgen = build_guests(BUILD_DEADLINE)[2]
        else:
            certgen = (PACKAGE / "harness" / "certgen" / "target" / "release"
                       / ("certgen.exe" if os.name == "nt" else "certgen"))
        # moq-relay reloads its certificate whenever the directory it sits in
        # changes, so it sits apart from what the run writes.
        certs = work / "certs"
        certs.mkdir()
        made = run([certgen, certs], TOOL_DEADLINE, capture_output=True, text=True)
        if made.returncode != 0:
            sys.exit(f"certgen failed:\n{made.stderr[-400:]}")
        cert_hex = made.stdout.strip()
        sources, messages, cues = [], [], []
        for index, seconds in enumerate(SECONDS):
            sources.append(work / f"source{index}.nut")
            make_source(sources[-1], seconds, index)
            messages.append(work / f"messages{index}.nut")
            cut_messages(messages[-1], seconds)
            cues.append(work / f"cues{index}.nut")
            cut_cues(cues[-1], seconds)

        def publisher(index: int) -> Publisher:
            return Publisher(work, index, sources[index], messages[index], cues[index], port,
                             cert_hex)

        query = work / "subscribe.sql"
        query.write_text(QUERY)
        port = free_udp_port()
        relay = start_relay(port, certs / "cert.pem", certs / "key.pem", work / "relay.log")
        print(f"moq-relay pid {relay.pid} on 127.0.0.1:{port}", flush=True)

        publishers.append(publisher(0))
        # The reader compiles against the catalog, so the broadcast is up
        # first.
        params = {"relay": f"moqt://127.0.0.1:{port}", "broadcast": BROADCAST,
                  "cert": cert_hex}
        until = time.monotonic() + 120
        while node_shape(guest("subscribe"), params, strict=False) is None:
            if publishers[0].tail.child.poll() is not None or time.monotonic() > until:
                sys.exit(f"the broadcast never came up:\n{publishers[0].tail.text()[1][-1500:]}")
        out = work / "out.flv"
        reader = start_reader(query, port, cert_hex, out, args.restart_s)

        wait_until(lambda: media_clock(publishers[0].tail) >= args.warmup, 120 + args.warmup,
                   "warming up", publishers[0].tail, reader)
        print(f"killing the first publisher outright at media "
              f"{media_clock(publishers[0].tail):.1f}s", flush=True)
        publishers[0].kill_outright()
        time.sleep(args.kill_gap)
        publishers.append(publisher(1))
        done = publishers[1].tail.finish(SECONDS[1] + 120)
        print(f"the second publisher ended, exit {done}", flush=True)
        if reader.child.poll() is not None:
            sys.exit(f"the reader ended with the second publisher:\n"
                     f"{reader.module_stderr()[-3000:]}")
        time.sleep(args.end_gap)
        publishers.append(publisher(2))
        done = publishers[2].tail.finish(SECONDS[2] + 120)
        ended = {"last publisher": time.monotonic()}
        print(f"the third publisher ended, exit {done}", flush=True)
        if reader.child.poll() is not None:
            sys.exit(f"the reader ended with the third publisher:\n"
                     f"{reader.module_stderr()[-3000:]}")
        read = reader.finish(args.restart_s + 120)
        ended["reader"] = time.monotonic()
        print(f"the reader ended {ended['reader'] - ended['last publisher']:.1f}s after the "
              f"last publisher, exit {read}", flush=True)
        print("--- reader stderr (tail) ---")
        print(reader.module_stderr()[-2500:])
        problems = []
        if read != 0:
            problems.append(f"the reader exited {read}")
        for index, one in enumerate(publishers[1:], start=1):
            if one.tail.child.returncode != 0:
                problems.append(f"publisher {index} exited {one.tail.child.returncode}")
        problems += judge(out, reader, ended, args.restart_s)
        if problems:
            print("=== FAIL ===")
            for problem in problems:
                print(" -", problem)
            sys.exit(1)
        print("=== PASS: the reader held a still while each publisher was away, and the media "
              "went on after it ===")
    finally:
        if reader is not None and reader.child.poll() is None:
            kill_outright(reader)
        for one in publishers:
            one.kill_outright()
        if relay is not None:
            kill_tree(relay)
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print(f"work kept at {work}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warmup", type=float, default=WARMUP)
    parser.add_argument("--kill-gap", type=float, default=KILL_GAP)
    parser.add_argument("--end-gap", type=float, default=END_GAP)
    parser.add_argument("--restart-s", type=int, default=RESTART_S)
    parser.add_argument("--work", default=None)
    parser.add_argument("--no-build", dest="build", action="store_false")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    if not SIDECAR.exists():
        sys.exit(f"no sidecar at {SIDECAR}; build it first (cargo build --release)")
    one_run(args)


if __name__ == "__main__":
    main()
