"""The reconnect oracle: a relay that goes away and comes back loses a stretch, not the run.

    python tests/live_reconnect.py [--seconds N] [--warmup N] [--down N]
                                   [--work DIR] [--no-build] [--keep]

A public relay resets sessions now and then, every one at once: two leaves
of a demo tree on the same root dropped at the same instant after 45
minutes, their one resubscribe on the dead session failed, and both runs
ended. Since 0.6.6 both halves of this package open a new session instead,
for as long as `reconnect_s` allows (60s by default).

The shape of a run: a local moq-relay, a PACED publisher (one rung, its
audio and a text track of the cue rows in `tests/data/cue-track.nut`, a row
per published group), and the subscribe module under the sidecar reading
every track on one session into one NUT, its picture and sound split into a
.nut file each afterwards. The publisher is publish.wasm under the sidecar
fed as `ffrwd run` feeds a node sink (see `common.start_publish_node`),
since a query cannot bind a column to its text input yet. After `--warmup`
seconds of media the relay is killed - a process gone, which says nothing
to either side, so each finds out from the QUIC idle timeout - and after
`--down` seconds it is started again on the same port. Then the run goes on
to the end of the file.

What it owes:

  - the publisher says it reconnected (an `event: reconnect` row) and runs
    to the end of its file;
  - the reader says it reconnected (a `kind: reconnect` row) and runs to
    the end of the broadcast, exit 0;
  - both .nut files carry media from after the relay came back, their
    timestamps never going backwards across the gap, and the first video
    packet after the gap is a keyframe;
  - the text track's rows never go back, and come from both sides of the
    gap: every cue of the fixture the reader was there for on either side
    comes back as it went in, and a cue on screen when the reader lost the
    session or took it up again comes back as a piece of itself, cut where
    the gap is.

Never collected by any suite: it needs moq-relay, ffmpeg, ffprobe, cargo
with the wasm32-wasip2 target and a wasi-sdk clang, and the sidecar. It
starts a relay, a publisher and a reader of its own and stops those alone.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

from common import (
    PACKAGE,
    SIDECAR,
    Tail,
    build_guests,
    free_udp_port,
    guest,
    kill_tree,
    nut_rows,
    run,
    split_streams,
    start_publish_node,
    start_relay,
    start_subscribe_node,
)

BROADCAST = "live/reconnect"
RATE = 30
GOP = 30
SAMPLE_RATE = 48000
SECONDS = 90
WARMUP = 12.0
DOWN = 4.0
BUILD_DEADLINE = 1800
TOOL_DEADLINE = 120
# The idle timeout (30s) plus the reconnect backoff plus the relay's own
# start, with room.
MEND_DEADLINE = 90.0

CUES = PACKAGE / "tests" / "data" / "cue-track.nut"
CUE_SOURCE = PACKAGE / "tests" / "data" / "cue-track.txt"
# How far behind its picture the publisher holds the text track, which is
# how far behind the reader's text runs.
TEXT_HOLD = 3.0


def make_program(path: Path, seconds: int) -> None:
    """The program's picture and sound as an encoder hands them to publish:
    640x360 H.264 at a keyframe a second, AAC beside it."""
    done = run([
        "ffmpeg", "-v", "error", "-y",
        "-f", "lavfi", "-i", f"testsrc2=size=640x360:rate={RATE}",
        "-f", "lavfi", "-i", f"sine=frequency=440:sample_rate={SAMPLE_RATE}",
        "-t", str(seconds), "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
        "-b:v", "800k", "-g", str(GOP), "-pix_fmt", "yuv420p",
        "-ac", "2", "-c:a", "aac", "-b:a", "128k", "-f", "nut", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"building the source failed:\n{done.stderr[-800:]}")


def cut_cues(path: Path, seconds: int) -> None:
    """The fixture's cue rows up to two seconds short of `seconds`, so they
    end with the picture."""
    done = run(["ffmpeg", "-v", "error", "-y", "-i", str(CUES), "-map", "0", "-c", "copy",
                "-t", str(seconds - 2), str(path)], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"cutting the cues failed:\n{done.stderr[-800:]}")


def fixture_cues(seconds: int) -> list[dict]:
    """The fixture's cues a publisher of `seconds` writes."""
    out = []
    for line in CUE_SOURCE.read_text().split("\n"):
        if line.strip():
            pts, row = line.split("\t", 1)
            if int(pts) / 1e6 < seconds - 2:
                out.append(json.loads(row))
    return out


def start_publisher(work: Path, source: Path, cues: Path, port: int,
                    cert_hex: str) -> tuple[Tail, list]:
    params = {"relay": f"moqt://127.0.0.1:{port}", "broadcast": BROADCAST, "cert": cert_hex,
              "rows": "groups", "hold_s": 0, "text_hold_ms": int(TEXT_HOLD * 1000)}
    rung = {"row": 0, "rendition": {"name": "360p"}}
    feeds = [
        {"port": "video", "source": source, "stream": "0:v:0",
         "rate": {"num": RATE, "den": 1}, "pad": rung},
        {"port": "audio", "source": source, "stream": "0:a:0",
         "rate": {"num": SAMPLE_RATE, "den": 1}, "pad": rung},
        {"port": "text", "source": cues, "stream": "0:d:0", "rate": None, "pad": None},
    ]
    return start_publish_node(guest("publish"), params, feeds, work, "publisher")


def start_reader(port: int, cert_hex: str, nut: Path) -> Tail:
    """The subscribe module under the sidecar, every track of the catalog
    into one NUT; see `common.start_subscribe_node`."""
    params = {
        "relay": f"moqt://127.0.0.1:{port}",
        "broadcast": BROADCAST,
        "cert": cert_hex,
        "token": "",
        "start": "live",
        "reconnect_s": 60,
    }
    reader, _ = start_subscribe_node(guest("subscribe"), params, nut, "reconnect")
    return reader


def media_clock(publisher: Tail) -> float:
    # A rung's own audio is named after the rung (`360p.audio`), which the
    # shared helper's first-letter rule does not take for audio.
    with publisher.lock:
        rows = [row for row in publisher.rows
                if "group" in row and "audio" in row.get("track", "")]
    return max((float(row.get("pts_end", 0.0)) for row in rows), default=0.0)


def wait_until(check, deadline: float, what: str, *children: Tail) -> float:
    started = time.monotonic()
    while time.monotonic() - started < deadline:
        if check():
            return time.monotonic() - started
        for child in children:
            if child.child.poll() is not None:
                out, err = child.text()
                sys.exit(f"{what}: a child stopped early (exit {child.child.returncode}):\n"
                         f"{child.module_stderr()[-3000:]}\n{out[-1000:]}")
        time.sleep(0.2)
    sys.exit(f"{what}: not within {deadline:.0f}s")


def packets(path: Path) -> list[tuple[float, bool]]:
    done = run(["ffprobe", "-v", "error", "-select_streams", "0",
                "-show_entries", "packet=pts_time,flags", "-of", "csv=p=0", str(path)],
               TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe {path.name}:\n{done.stderr[-600:]}")
    out = []
    for line in done.stdout.splitlines():
        parts = line.split(",")
        if len(parts) >= 2 and parts[0] not in ("", "N/A"):
            out.append((float(parts[0]), "K" in parts[1]))
    return out


def judge(name: str, found: list[tuple[float, bool]], video: bool,
          killed_at: float) -> list[str]:
    """What one output owes: media from both sides of the gap, a clock that
    never goes back, and a picture that starts again at a keyframe."""
    problems = []
    if not found:
        return [f"{name}: no packets"]
    times = [pts for pts, _ in found]
    # Decode order for video may reorder pts a frame or two; the gap is
    # what matters, so compare against the running maximum.
    widest, at = 0.0, None
    running = times[0]
    for index in range(1, len(times)):
        step = times[index] - running
        if step > widest:
            widest, at = step, index
        running = max(running, times[index])
    print(f"  {name}: {len(times)} packets, {times[0]:.3f}s to {max(times):.3f}s, "
          f"widest gap {widest:.3f}s", flush=True)
    if at is None or widest < 1.0:
        problems.append(f"{name}: no gap where the relay was down (widest {widest:.3f}s)")
        return problems
    before, after = running_max(times, at), times[at]
    print(f"  {name}: gap from {before:.3f}s to {after:.3f}s", flush=True)
    if after < killed_at:
        problems.append(f"{name}: the gap closes at {after:.3f}s, before the relay was "
                        f"killed at media {killed_at:.3f}s")
    if max(times) < after + 5.0:
        problems.append(f"{name}: under 5s of media after the gap")
    if video and not found[at][1]:
        problems.append(f"{name}: the first packet after the gap is not a keyframe")
    return problems


def running_max(times: list[float], upto: int) -> float:
    return max(times[:upto])


def judge_text(found: list[dict], fixture: list[dict]) -> list[str]:
    """What the text track owes across the gap: rows that never go back, and
    every cue the reader was there for, whole or cut where the gap is."""
    problems = []
    if not found:
        return ["text: no rows"]
    times = [row["time"] for row in found]
    if any(b < a for a, b in zip(times, times[1:])):
        problems.append("text: a row went back")
    jumps = [(b - a, index) for index, (a, b) in enumerate(zip(times, times[1:]), start=1)]
    widest, at = max(jumps)
    lost, back = times[at - 1], times[at]
    print(f"  text: {len(found)} rows, {times[0]:.3f}s to {times[-1]:.3f}s, gap from "
          f"{lost:.3f}s to {back:.3f}s", flush=True)
    if widest < 10.0:
        problems.append(f"text: no gap where the relay was down (widest {widest:.3f}s)")
        return problems
    by_text = {cue["text"]: cue for cue in fixture}
    finals = [row["row"] for row in found if row["row"].get("final")]
    whole, pieces = [], []
    for row in finals:
        cue = by_text.get(row["text"])
        if cue is None or row.get("voice") != cue.get("voice"):
            problems.append(f"text: {row} is none of the fixture's cues")
            continue
        exact = (abs(row["start_t"] - cue["start_t"]) < 0.002
                 and abs(row["end_t"] - cue["end_t"]) < 0.002)
        inside = (cue["start_t"] - 0.002 <= row["start_t"]
                  and row["end_t"] <= cue["end_t"] + 0.002)
        if exact:
            whole.append(cue["text"])
        elif inside:
            pieces.append(row)
        else:
            problems.append(f"text: {row} runs outside its cue {cue}")
    # Every cue the reader was there for on either side of the gap, whole.
    owed = [cue["text"] for cue in fixture
            if (times[0] <= cue["start_t"] and cue["end_t"] <= lost)
            or back <= cue["start_t"]]
    missing = [text for text in owed if text not in whole]
    print(f"  text: {len(whole)} cues whole, {len(pieces)} cut at the gap; "
          f"{len(missing)} of {len(owed)} owed missing", flush=True)
    for row in pieces:
        print(f"  text: cut at the gap: {json.dumps(row)}", flush=True)
    if missing:
        problems.append(f"text: the cues {missing[:4]} did not come back whole")
    if len([text for text in whole if by_text[text]["start_t"] >= back]) < 5:
        problems.append("text: under five cues after the gap")
    return problems


def one_run(args: argparse.Namespace) -> None:
    work = Path(tempfile.mkdtemp(prefix="moq-reconnect-", dir=args.work))
    relay = None
    publisher = reader = None
    feeders: list = []
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

        source = work / "source.nut"
        make_program(source, args.seconds)
        cues = work / "cues.nut"
        cut_cues(cues, args.seconds)

        port = free_udp_port()
        relay = start_relay(port, certs / "cert.pem", certs / "key.pem", work / "relay.log")
        print(f"moq-relay pid {relay.pid} on 127.0.0.1:{port}", flush=True)

        publisher, feeders = start_publisher(work, source, cues, port, cert_hex)
        outputs = [work / "video.nut", work / "audio.nut"]
        reader = start_reader(port, cert_hex, work / "read.nut")

        wait_until(lambda: media_clock(publisher) >= args.warmup, args.seconds + 60,
                   "warming up", publisher, reader)
        killed_at = media_clock(publisher)
        print(f"killing the relay at media {killed_at:.1f}s", flush=True)
        kill_tree(relay)
        time.sleep(args.down)
        relay = start_relay(port, certs / "cert.pem", certs / "key.pem", work / "relay2.log")
        print(f"moq-relay back, pid {relay.pid}, after {args.down:.1f}s", flush=True)

        def publisher_mended() -> bool:
            with publisher.lock:
                return any(row.get("event") == "reconnect" for row in publisher.rows)

        def reader_mended() -> bool:
            return any(row.get("kind") == "reconnect" for row in reader.counters())

        took = wait_until(publisher_mended, MEND_DEADLINE, "the publisher reconnecting",
                          publisher, reader)
        print(f"publisher reconnected {took:.1f}s after the relay came back", flush=True)
        took = wait_until(reader_mended, MEND_DEADLINE, "the reader reconnecting",
                          publisher, reader)
        print(f"reader reconnected {took:.1f}s after that", flush=True)

        published = publisher.finish(args.seconds + 120)
        read = reader.finish(120)
        out, err = publisher.text()
        print("--- publisher stderr (tail) ---")
        print(err[-1500:])
        print("--- reader stderr (tail) ---")
        print(reader.module_stderr()[-2500:])
        for row in publisher.rows:
            if row.get("event") == "reconnect" or "tracks" in row:
                print("publisher row:", json.dumps(row))
        for row in reader.counters():
            if row.get("kind") == "reconnect":
                print("reader row:", json.dumps(row))

        split_streams(work / "read.nut", outputs)
        problems = []
        if published != 0:
            problems.append(f"the publisher exited {published}")
        if read != 0:
            problems.append(f"the reader exited {read}")
        problems += judge("video", packets(outputs[0]), True, killed_at)
        problems += judge("audio", packets(outputs[1]), False, killed_at)
        problems += judge_text(nut_rows(work / "read.nut", "d:0"), fixture_cues(args.seconds))
        if problems:
            print("=== FAIL ===")
            for problem in problems:
                print(" -", problem)
            sys.exit(1)
        print("=== PASS: both halves reconnected and the media and the text went on ===")
    finally:
        for child in [*feeders, *(c.child for c in (reader, publisher) if c is not None)]:
            kill_tree(child)
        if relay is not None:
            kill_tree(relay)
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print(f"work kept at {work}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=SECONDS)
    parser.add_argument("--warmup", type=float, default=WARMUP)
    parser.add_argument("--down", type=float, default=DOWN)
    parser.add_argument("--work", default=None)
    parser.add_argument("--no-build", dest="build", action="store_false")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    if not SIDECAR.exists():
        sys.exit(f"no sidecar at {SIDECAR}; build it first (cargo build --release)")
    one_run(args)


if __name__ == "__main__":
    main()
