"""The pacing oracle: what a call costs, and whether any track falls behind real time.

    python tests/live_pacing.py [--seconds N] [--rtt MS] [--audio-group-ms N]
                                [--fixture F] [--shape S] [--drift-max S]
                                [--work DIR] [--no-build] [--keep]

A relay built on moq-transport's `serve` model (Cloudflare's) keeps only a
track's newest group, so a group overtaken by the next one on its track
before the relay forwarded it is gone. The publisher answers that by pacing
each track: a group goes out only once the relay has acknowledged the one
before it. What this loop measures is what that costs the run, on a link
with a round trip in it.

The shape of a run: a local moq-relay, a UDP forwarder in this process that
holds every datagram between the publisher and the relay for half of
`--rtt` each way (the loopback has none, and an acknowledgement that comes
back at once costs nothing to wait for), a PACED publisher (one rung, its
audio at `--audio-group-ms`, and the data column of `--fixture`, pairs of
messages at one pts by default), and the subscribe module under the
sidecar reading every track straight off the relay, each packet timed as it
leaves the reader.

What it reports:

  - per summary window (5s), the longest host call into publish
    (`call_max_ms`) and the longest the session lay undriven between two
    (`gap_max_ms`), and for each track what it held back (`queue_max`,
    `wait_max_ms`, `unpaced`, when the publisher reports them);
  - per track, how late each packet came out of the reader against its own
    pts, over the first and the last ten seconds: a track that falls behind
    real time comes out later and later, so the lateness at the end less
    that at the start is its drift. The least late packet of each stretch
    is the one compared, since a video packet waits for its group at the
    reader and the rest of a group rides that sawtooth.

`--shape leaf` measures a LEAF instead: the paced publisher goes straight
to the relay as a head, and a second query subscribes to it and publishes
what it reads, re-encoded, through the round trip. That is what a node of
a tree does, and its packets arrive the way the head's groups do, whole:
a GOP of picture at a time and the sound beside it, lumps about a second
apart. The measured publisher is the leaf's, and the reader reads the
leaf's broadcast.

The first two windows hold the wait for a first reader and the join, so
the call and queue figures are read over the windows after them. A package from
before 0.7.2 (a checkout at 0.7.1, say, run with this file copied in)
reads the same, without the queue fields: that is the comparison the
README's tables are.

It passes when every message arrived and no track drifted by more than
`--drift-max` seconds. moq-relay keeps every group, so a loss here is not
the loss this is about; the loop is for the cost.

Never collected by any suite: it needs moq-relay, ffmpeg, ffprobe, cargo with
the wasm32-wasip2 target and a wasi-sdk clang, and the sidecar. It starts a
relay and a reader of its own and stops those alone.
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from common import (
    PACKAGE,
    SIDECAR,
    Tail,
    build_guests,
    ffrwd_argv,
    free_udp_port,
    kill_tree,
    run,
    spawn,
    start_relay,
)
from live_data import (
    BROADCAST,
    DATA,
    TRACKS,
    TimedPipe,
    expected_messages,
    make_source,
    packets,
    start_reader,
)

# As long as the pairs fixture, whose last message is at 98.5s: a source
# that ended first would leave its last group open until the data did.
SECONDS = 100
RTT_MS = 40.0
DRIFT_MAX = 0.5
BUILD_DEADLINE = 1800
TOOL_DEADLINE = 120
# How much of each end of the run the lateness is read over.
STRETCH = 10.0

PUBLISH_QUERY = """
COPY (
  SELECT scale(f.video[1], 640, -2), f.audio[1], d.data[1]
  FROM input(:'source', realtime => true) f, input(:'messages', realtime => true) d
) TO ffrwd.moq.publish(:'relay', :'broadcast', COALESCE(:'cert', ''), '', :group_ms, 'summary')
  WITH (video_bitrate '800k', gop 30, preset 'veryfast', tune 'zerolatency',
        audio_bitrate '128k')
"""

# The leaf of `--shape leaf`: the head's broadcast read at the live edge and
# published again, as a node of a tree does.
LEAF_QUERY = """
COPY (
  SELECT s.video[1], s.audio[1], s.data[1]
  FROM ffrwd.moq.subscribe(:'from_relay', :'from', COALESCE(:'cert', ''), '', 'live', 1000) s
) TO ffrwd.moq.publish(:'relay', :'broadcast', COALESCE(:'cert', ''), '', :group_ms, 'summary')
  WITH (video_bitrate '800k', gop 30, preset 'veryfast', tune 'zerolatency',
        audio_bitrate '128k')
"""

# The head's broadcast under `--shape leaf`; the leaf publishes BROADCAST,
# which is what the reader reads.
HEAD_BROADCAST = "live/head"
# How long the head has before the leaf compiles against its catalog.
HEAD_START = 4.0


class Delay:
    """A UDP forwarder that holds each datagram `one_way` seconds.

    One client (the publisher) on the front socket, the relay behind the
    back one. Each direction is a receiving thread and a sending one, the
    sender sleeping until each datagram's time: the delay is the same for
    every datagram, so a FIFO is the whole schedule, and nothing is dropped
    or reordered.
    """

    def __init__(self, relay_port: int, one_way: float) -> None:
        self.one_way = one_way
        self.relay = ("127.0.0.1", relay_port)
        self.front = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.front.bind(("127.0.0.1", 0))
        self.back = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.back.bind(("127.0.0.1", 0))
        for sock in (self.front, self.back):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 << 20)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8 << 20)
        self.port = self.front.getsockname()[1]
        self.client: tuple[str, int] | None = None
        self.stopped = False
        self.carried = [0, 0]
        up: collections.deque = collections.deque()
        down: collections.deque = collections.deque()
        ready = [threading.Condition(), threading.Condition()]
        self.threads = [
            threading.Thread(target=self._take, args=(self.front, up, ready[0], True), daemon=True),
            threading.Thread(target=self._give, args=(self.back, up, ready[0], 0), daemon=True),
            threading.Thread(target=self._take, args=(self.back, down, ready[1], False), daemon=True),
            threading.Thread(target=self._give, args=(self.front, down, ready[1], 1), daemon=True),
        ]
        for thread in self.threads:
            thread.start()

    def _take(self, sock: socket.socket, queue: collections.deque,
              ready: threading.Condition, from_client: bool) -> None:
        while not self.stopped:
            try:
                data, peer = sock.recvfrom(1 << 16)
            except ConnectionResetError:
                # Windows reports an ICMP port unreachable on the next read.
                continue
            except OSError:
                return
            if from_client:
                self.client = peer
            with ready:
                queue.append((time.perf_counter() + self.one_way, data))
                ready.notify()

    def _give(self, sock: socket.socket, queue: collections.deque,
              ready: threading.Condition, direction: int) -> None:
        while not self.stopped:
            with ready:
                while not queue and not self.stopped:
                    ready.wait(0.5)
                if self.stopped:
                    return
                due, data = queue.popleft()
            wait = due - time.perf_counter()
            if wait > 0:
                time.sleep(wait)
            to = self.relay if direction == 0 else self.client
            if to is None:
                continue
            try:
                sock.sendto(data, to)
                self.carried[direction] += 1
            except OSError:
                pass

    def stop(self) -> None:
        self.stopped = True
        self.front.close()
        self.back.close()


def start_publisher(query: Path, port: int, cert_hex: str, group_ms: int,
                    broadcast: str, **values: object) -> Tail:
    env = dict(os.environ)
    env.setdefault("FFRWD_WASM", str(SIDECAR))
    argv = ffrwd_argv("run", "-f", str(query),
                      "-v", f"relay=moqt://127.0.0.1:{port}",
                      "-v", f"broadcast={broadcast}",
                      "-v", f"cert={cert_hex}",
                      "-v", f"group_ms={group_ms}",
                      *[arg for name, value in values.items()
                        for arg in ("-v", f"{name}={value}")], "-q")
    print("+ publisher:", " ".join(argv[:6]), "...", flush=True)
    return Tail(spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                      text=True, env=env, cwd=PACKAGE))


def windows(rows: list[dict]) -> list[list[dict]]:
    """The publisher's track rows, a list per summary window: a window's
    rows go out together, one per track, so a track seen again starts the
    next window."""
    out: list[list[dict]] = []
    seen: set[str] = set()
    for row in rows:
        if "groups" not in row or "track" not in row:
            continue
        if not out or row["track"] in seen:
            out.append([])
            seen = set()
        out[-1].append(row)
        seen.add(row["track"])
    return out


def lateness(path: Path, pipe: TimedPipe, started: float) -> list[tuple[float, float]]:
    """(pts, seconds it came out after the run started less its pts), per
    packet, in the order they arrived."""
    out = []
    for packet in packets(path, False):
        if packet["size"] <= 1:
            continue
        at = pipe.arrival(packet["pos"] + packet["size"])
        if at is not None:
            out.append((packet["pts_time"], at - started - packet["pts_time"]))
    return out


def drift(timed: list[tuple[float, float]]) -> dict | None:
    """The least late packet in the first and the last STRETCH seconds of
    media, and the difference, which is what falling behind looks like."""
    if not timed:
        return None
    first_pts = min(pts for pts, _ in timed)
    last_pts = max(pts for pts, _ in timed)
    head = [late for pts, late in timed if pts <= first_pts + STRETCH]
    tail = [late for pts, late in timed if pts >= last_pts - STRETCH]
    # The track's last group closes only when the run ends, so the last
    # seconds say nothing about the worst of a live run.
    live = [(late, pts) for pts, late in timed if pts < last_pts - 3.0]
    base = min(head)
    worst = max(live, default=(base, 0.0))
    return {
        "packets": len(timed),
        "start": min(head) - base,
        "end": min(tail) - base,
        "drift": min(tail) - min(head),
        "worst": worst[0] - base,
        "worst_pts": worst[1],
        "median": statistics.median([late for late, _ in live] or [base]) - base,
    }


def one_run(args: argparse.Namespace) -> None:
    work = Path(tempfile.mkdtemp(prefix="moq-pacing-", dir=args.work))
    relay = publisher = reader = head = None
    delay = None
    query = PACKAGE / ".live-pacing-publish.sql"
    leaf_query = PACKAGE / ".live-pacing-leaf.sql"
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
        delay = Delay(port, args.rtt / 2000.0)
        print(f"publisher's path: 127.0.0.1:{delay.port}, {args.rtt:.0f} ms round trip",
              flush=True)

        messages = DATA / f"{args.fixture}.nut"
        if args.shape == "leaf":
            head = start_publisher(query, port, cert_hex, args.audio_group_ms,
                                   HEAD_BROADCAST, source=source, messages=messages)
            time.sleep(HEAD_START)
            leaf_query.write_text(LEAF_QUERY)
            publisher = start_publisher(leaf_query, delay.port, cert_hex,
                                        args.audio_group_ms, BROADCAST,
                                        from_relay=f"moqt://127.0.0.1:{port}",
                                        **{"from": HEAD_BROADCAST})
        else:
            publisher = start_publisher(query, delay.port, cert_hex, args.audio_group_ms,
                                        BROADCAST, source=source, messages=messages)
        started = time.monotonic()
        pipes = {name: TimedPipe(f"pacing-{name}", work / f"{name}.nut")
                 for name in TRACKS}
        reader = start_reader(port, cert_hex,
                              [(TRACKS[name], pipes[name].spelling) for name in TRACKS])

        if head is not None:
            headed = head.finish(args.seconds + 240)
            (work / "head.err").write_text(head.text()[1], encoding="utf-8")
            if headed != 0:
                print(f"the head exited {headed}:\n{head.text()[1][-1500:]}")
        published = publisher.finish(args.seconds + 240)
        read = reader.finish(120)
        for pipe in pipes.values():
            pipe.finish(30)
        _, err = publisher.text()
        (work / "reader.err").write_text(reader.module_stderr(), encoding="utf-8")
        (work / "publisher.err").write_text(err, encoding="utf-8")
        print(f"forwarded {delay.carried[0]} datagrams up, {delay.carried[1]} down")
        problems: list[str] = []
        if published != 0:
            problems.append(f"the publisher exited {published}:\n{err[-1500:]}")
        if read != 0:
            problems.append(f"the reader exited {read}")

        with publisher.lock:
            rows = list(publisher.rows)
        print("--- the publisher, per summary window ---")
        calls = []
        for index, window in enumerate(windows(rows)):
            call = window[0].get("call_max_ms")
            calls.append(call)
            parts = [f"call_max {call:4} ms", f"gap_max {window[0].get('gap_max_ms'):4} ms"]
            for row in window:
                queue = ""
                if "queue_max" in row:
                    queue = (f" q{row['queue_max']}/{row['wait_max_ms']}ms"
                             f"/u{row['unpaced']}")
                parts.append(f"{row['track']} {row['media']:6.1f}s{queue}")
            print(f"  {index:3}  " + "  ".join(parts))
        steady = calls[2:]
        if steady:
            print(f"call_max_ms over windows 2 to {len(calls) - 1}: min {min(steady)}, "
                  f"median {statistics.median(steady)}, max {max(steady)}")
        final = {}
        for window in windows(rows):
            for row in window:
                final[row["track"]] = row
        gaps = [window[0].get("gap_max_ms") for window in windows(rows)][2:]
        if gaps:
            print(f"gap_max_ms over windows 2 to {len(calls) - 1}: min {min(gaps)}, "
                  f"median {statistics.median(gaps)}, max {max(gaps)}")
        for name, row in final.items():
            if "queue_max" in row:
                every = [r for w in windows(rows) for r in w if r["track"] == name]
                after = [r for w in windows(rows)[2:] for r in w if r["track"] == name]
                print(f"  {name}: groups {row['groups']}, on the wire {row['appended']} "
                      f"appended {row['closed']} closed")
                for label, seen in (("whole run", every), ("after the join", after)):
                    if not seen:
                        continue
                    waits = [r.get("wait_max_ms", 0) for r in seen]
                    print(f"    {label:14}: queue_max {max(r.get('queue_max', 0) for r in seen)}, "
                          f"wait_max_ms max {max(waits)} median {statistics.median(waits)}, "
                          f"unpaced {sum(r.get('unpaced', 0) for r in seen)}")
        trailing = [row for row in rows if "tracks" in row]
        if trailing:
            print("trailing:", json.dumps(trailing[-1]))

        print("--- the reader: lateness against pts, least late of each end ---")
        for name in TRACKS:
            if name == "data":
                continue
            timed = lateness(work / f"{name}.nut", pipes[name], started)
            (work / f"{name}.late.txt").write_text(
                "".join(f"{pts:.6f} {late:.6f}\n" for pts, late in timed))
            seen = drift(timed)
            if seen is None:
                problems.append(f"{name}: nothing arrived")
                continue
            behind = seen["drift"] > args.drift_max
            print(f"  {name:5}: {seen['packets']} packets, end less start "
                  f"{seen['drift'] * 1000:7.1f} ms, median {seen['median'] * 1000:7.1f} ms, "
                  f"worst {seen['worst'] * 1000:7.1f} ms at {seen['worst_pts']:.2f}s"
                  + ("  FELL BEHIND" if behind else "  kept up"))
            if behind:
                problems.append(f"{name} fell behind real time by {seen['drift']:.3f}s")

        got = packets(work / "data.nut", True)
        expected = expected_messages(args.fixture)
        wanted = [message for _, message in expected]
        # A heartbeat is a one-byte packet, and no message.
        arrived = [packet["bytes"] for packet in got if packet["size"] > 1]
        # The live join can land after the first message, so the fixture
        # is matched from the first one that arrived.
        start = wanted.index(arrived[0]) if arrived and arrived[0] in wanted else 0
        print(f"data: {len(arrived)} of {len(wanted) - start} messages from the join")
        if arrived != wanted[start:start + len(arrived)] or len(arrived) != len(wanted) - start:
            problems.append("the data track lost or reordered messages")

        if problems:
            print("=== FAIL ===")
            for problem in problems:
                print(" -", problem)
            sys.exit(1)
        print("=== PASS: every message arrived and no track fell behind ===")
    finally:
        for child in (reader, publisher, head):
            if child is not None:
                kill_tree(child.child)
        if relay is not None:
            kill_tree(relay)
        if delay is not None:
            delay.stop()
        query.unlink(missing_ok=True)
        leaf_query.unlink(missing_ok=True)
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print(f"work kept at {work}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=SECONDS)
    parser.add_argument("--rtt", type=float, default=RTT_MS,
                        help="the round trip the forwarder adds between publisher and relay, ms")
    parser.add_argument("--audio-group-ms", type=int, default=200)
    parser.add_argument("--fixture", default="pairs", choices=("messages", "pairs"))
    parser.add_argument("--shape", default="head", choices=("head", "leaf"),
                        help="measure the paced publisher itself, or a leaf republishing it")
    parser.add_argument("--drift-max", type=float, default=DRIFT_MAX,
                        help="seconds a track may come out later at the end than at the start")
    parser.add_argument("--work", default=str(PACKAGE / "target"),
                        help="where the run's files go (a directory under it is made)")
    parser.add_argument("--no-build", dest="build", action="store_false")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    if not SIDECAR.exists():
        sys.exit(f"no sidecar at {SIDECAR}; build it first (cargo build --release)")
    one_run(args)


if __name__ == "__main__":
    main()
