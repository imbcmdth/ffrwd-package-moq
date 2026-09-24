"""The reconnect oracle: a relay that goes away and comes back loses a stretch, not the run.

    python tests/live_reconnect.py [--seconds N] [--warmup N] [--down N]
                                   [--work DIR] [--no-build] [--keep]

A public relay resets sessions now and then, every one at once: two leaves
of a demo tree on the same root dropped at the same instant after 45
minutes, their one resubscribe on the dead session failed, and both runs
ended. Since 0.6.6 both halves of this package open a new session instead,
for as long as `reconnect_s` allows (60s by default).

The shape of a run: a local moq-relay, a PACED publisher (one rung and its
audio, a row per published group), and the subscribe module under the
sidecar reading both tracks to .nut files on one session. After `--warmup`
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
    packet after the gap is a keyframe.

Never collected by any suite: it needs moq-relay, ffmpeg, ffprobe, cargo
with the wasm32-wasip2 target and a wasi-sdk clang, and the sidecar. It
starts a relay and a reader of its own and stops those alone.
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
    PACKAGE,
    SIDECAR,
    Tail,
    build_guests,
    ffrwd_argv,
    free_udp_port,
    guest,
    kill_tree,
    run,
    spawn,
    start_relay,
)

BROADCAST = "live/reconnect"
RATE = 30
GOP = 30
SAMPLE_RATE = 48000
SECONDS = 70
WARMUP = 12.0
DOWN = 4.0
BUILD_DEADLINE = 1800
TOOL_DEADLINE = 120
# The idle timeout (10s) plus the reconnect backoff plus the relay's own
# start, with room.
MEND_DEADLINE = 60.0

PUBLISH_QUERY = """
COPY (
  SELECT scale(f.video[1], 640, -2), f.audio[1]
  FROM input(:'source', realtime => true) f
) TO ffrwd.moq.publish(:'relay', :'broadcast', COALESCE(:'cert', ''), '', 200, 'groups')
  WITH (video_bitrate '800k', gop 30, preset 'veryfast', tune 'zerolatency',
        audio_bitrate '128k')
"""


def make_source(path: Path, seconds: int) -> None:
    done = run([
        "ffmpeg", "-v", "error", "-y",
        "-f", "lavfi", "-i", f"testsrc2=size=640x360:rate={RATE}",
        "-f", "lavfi", "-i", f"sine=frequency=440:sample_rate={SAMPLE_RATE}",
        "-ac", "2", "-c:a", "aac",
        "-t", str(seconds), "-c:v", "libx264", "-preset", "ultrafast",
        "-g", str(GOP), "-pix_fmt", "yuv420p", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"building the source failed:\n{done.stderr[-800:]}")


def start_publisher(query: Path, source: Path, port: int, cert_hex: str) -> Tail:
    env = dict(os.environ)
    env.setdefault("FFRWD_WASM", str(SIDECAR))
    argv = ffrwd_argv("run", "-f", str(query),
                      "-v", f"source={source}",
                      "-v", f"relay=moqt://127.0.0.1:{port}",
                      "-v", f"broadcast={BROADCAST}",
                      "-v", f"cert={cert_hex}", "-q")
    print("+ publisher:", " ".join(argv[:6]), "...", flush=True)
    return Tail(spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                      text=True, env=env, cwd=PACKAGE))


def start_reader(port: int, cert_hex: str, outputs: list[Path]) -> Tail:
    module = guest("subscribe")
    params = json.dumps({
        "relay": f"moqt://127.0.0.1:{port}",
        "broadcast": BROADCAST,
        "cert": cert_hex,
        "token": "",
        "start": "live",
        "reconnect_s": 60,
    })
    argv = [str(SIDECAR), "-udp", str(module), "-http", str(module),
            "-m", str(module), "-params", params]
    for track, path in enumerate(outputs):
        argv += ["-track", str(track), "-f", "nut", str(path)]
    print("+ reader:", " ".join(argv[:6]), "...", flush=True)
    return Tail(spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))


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


def one_run(args: argparse.Namespace) -> None:
    work = Path(tempfile.mkdtemp(prefix="moq-reconnect-", dir=args.work))
    relay = None
    publisher = reader = None
    query = PACKAGE / ".live-reconnect-publish.sql"
    try:
        if args.build:
            certgen = build_guests(BUILD_DEADLINE)[2]
        else:
            certgen = (PACKAGE / "harness" / "certgen" / "target" / "release"
                       / ("certgen.exe" if os.name == "nt" else "certgen"))
        made = run([certgen, work], TOOL_DEADLINE, capture_output=True, text=True)
        if made.returncode != 0:
            sys.exit(f"certgen failed:\n{made.stderr[-400:]}")
        cert_hex = made.stdout.strip()

        source = work / "source.mp4"
        make_source(source, args.seconds)
        query.write_text(PUBLISH_QUERY)

        port = free_udp_port()
        relay = start_relay(port, work / "cert.pem", work / "key.pem", work / "relay.log")
        print(f"moq-relay pid {relay.pid} on 127.0.0.1:{port}", flush=True)

        publisher = start_publisher(query, source, port, cert_hex)
        outputs = [work / "video.nut", work / "audio.nut"]
        reader = start_reader(port, cert_hex, outputs)

        wait_until(lambda: media_clock(publisher) >= args.warmup, args.seconds + 60,
                   "warming up", publisher, reader)
        killed_at = media_clock(publisher)
        print(f"killing the relay at media {killed_at:.1f}s", flush=True)
        kill_tree(relay)
        time.sleep(args.down)
        relay = start_relay(port, work / "cert.pem", work / "key.pem", work / "relay2.log")
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
            if row.get("event") == "reconnect":
                print("publisher row:", json.dumps(row))
        for row in reader.counters():
            if row.get("kind") == "reconnect":
                print("reader row:", json.dumps(row))

        problems = []
        if published != 0:
            problems.append(f"the publisher exited {published}")
        if read != 0:
            problems.append(f"the reader exited {read}")
        problems += judge("video", packets(outputs[0]), True, killed_at)
        problems += judge("audio", packets(outputs[1]), False, killed_at)
        if problems:
            print("=== FAIL ===")
            for problem in problems:
                print(" -", problem)
            sys.exit(1)
        print("=== PASS: both halves reconnected and the media went on ===")
    finally:
        for child in (reader, publisher):
            if child is not None:
                kill_tree(child.child)
        if relay is not None:
            kill_tree(relay)
        query.unlink(missing_ok=True)
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
