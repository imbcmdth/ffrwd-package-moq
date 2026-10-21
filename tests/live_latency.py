"""The latency oracle: subscribe hands each frame on as it arrives, not in batches.

    python tests/live_latency.py [--fps N] [--seconds N] [--long] [--size WxH]
                                 [--spread-max F] [--work DIR] [--no-build] [--keep]

The shape of a run: a local moq-relay, a PACED publisher of one video track
(a 1080p test pattern at `--fps`, a keyframe a second, no B-frames, read
`realtime`), and `ffrwd.moq.subscribe` under `ffrwd run` reading that track
at the live edge into a pipe this loop reads as it arrives. Every packet's
arrival is timed, and its lateness is the time it arrived less its decode
time (both counted from the run's start, so the lateness carries a constant
the loop does not know: the publisher's encoder, the pipe, the reader's
join).

A reader that hands packets on as they arrive gives every packet about the
same lateness, give or take the wire's jitter. One that holds them and
lets them go together gives a sawtooth: each packet of a batch arrives with
the last one, so its lateness falls by a frame interval per packet and
jumps back up at the next batch. What the loop reports:

  - the lateness of each packet above the least late packet within
    1.5 seconds of it (more than a GOP either side), its median, p90, p99
    and maximum, in milliseconds;
  - the spacing between arrivals, and the batches: packets that arrived
    within a quarter of a frame interval of the one before. A batch of n
    is a sawtooth n - 1 frame intervals high, and each packet is counted
    in the batch it arrived in;
  - the shape of the sawtooth, the lateness of two seconds of packets.

The first `--settle` seconds (the join) are left out.

It fails when one packet in ten or more arrived in a batch larger than
`--spread-max` + 1 packets (a sawtooth over two frame intervals, by
default), or when the median packet sits more than a frame interval above
the least late one near it. Before 0.9.2 a video packet waited for its
whole group, a GOP, at the reader: every packet rode a batch of a GOP, and
the median packet was half a GOP late.

What is left on Windows is the publisher's: its sidecar's timers tick at
15.6 ms, so its packets reach the relay on that grid, and a native reader
sees the same sawtooth, 15.6 ms high.

Never collected by any suite: it needs moq-relay, ffmpeg, ffprobe, cargo with
the wasm32-wasip2 target and a wasi-sdk clang, and the sidecar. It starts a
relay and a reader of its own and stops those alone.
"""

from __future__ import annotations

import argparse
import json
import os
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
    Tail,
    build_guests,
    ffrwd_argv,
    free_udp_port,
    guest,
    kill_tree,
    node_shape,
    run,
    spawn,
    start_relay,
)
from live_data import TimedPipe

BROADCAST = "live/latency"
FPS = 60
# 25 seconds of frames by default, which is enough to fail on batching;
# --long is the minute the README's numbers were measured over.
SECONDS = 25
LONG_SECONDS = 60
SETTLE = 5.0
SPREAD_MAX = 2.0
# Seconds either side of a packet its floor is read over: more than a GOP.
FLOOR = 1.5
BUILD_DEADLINE = 1800
TOOL_DEADLINE = 300

PUBLISH_QUERY = """
COPY (
  SELECT f.video[1]
  FROM input(:'source', realtime => true) f
) TO ffrwd.moq.publish(:'relay', :'broadcast', COALESCE(:'cert', ''), '', 200, 'summary', 60, 0)
  WITH (video_bitrate '6M', gop :gop, preset 'ultrafast', tune 'zerolatency')
"""


# The reader: the one video track at the live edge, stream-copied into a
# NUT this loop times as it arrives. ffmpeg flushes a pipe, which cannot
# seek, after every packet.
SUBSCRIBE_QUERY = """
COPY (
  SELECT s.video[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', '')) s
) TO :'out' WITH (format 'nut')
"""


def make_source(path: Path, seconds: int, fps: int, size: str) -> None:
    done = run([
        "ffmpeg", "-v", "error", "-y",
        "-f", "lavfi", "-i", f"testsrc2=size={size}:rate={fps}",
        "-t", str(seconds), "-c:v", "libx264", "-preset", "ultrafast",
        "-tune", "zerolatency", "-g", str(fps), "-pix_fmt", "yuv420p", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"building the source failed:\n{done.stderr[-800:]}")


def video_packets(path: Path) -> list[dict]:
    argv = ["ffprobe", "-v", "error", "-select_streams", "0",
            "-show_entries", "packet=dts_time,pos,size,flags", "-of", "json", str(path)]
    done = run(argv, TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe {path.name}:\n{done.stderr[-600:]}")
    out = []
    for packet in json.loads(done.stdout).get("packets", []):
        if packet.get("dts_time") in (None, "N/A"):
            continue
        out.append({"dts": float(packet["dts_time"]), "pos": int(packet["pos"]),
                    "size": int(packet["size"]), "key": "K" in packet.get("flags", "")})
    return out


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def measure(path: Path, pipe: TimedPipe, started: float, fps: int, settle: float) -> dict:
    """The lateness and arrival spacing of every packet past the join."""
    interval = 1.0 / fps
    timed = []
    for packet in video_packets(path):
        at = pipe.arrival(packet["pos"] + packet["size"])
        if at is not None:
            timed.append((packet["dts"], at - started, packet["key"]))
    if len(timed) < fps * 5:
        sys.exit(f"only {len(timed)} packets arrived")
    first = timed[0][1]
    steady = [t for t in timed if t[1] - first >= settle]
    late = [at - dts for dts, at, _ in steady]
    # Each packet against the least late one within FLOOR seconds either
    # side of it, which is the floor the wire allows there. The floor of
    # the whole run would fold in any slow drift between the publisher's
    # clock and this loop's, which is not what is measured here.
    above = []
    for index, (dts, _, _) in enumerate(steady):
        low = index
        while low > 0 and dts - steady[low - 1][0] <= FLOOR:
            low -= 1
        high = index
        while high + 1 < len(steady) and steady[high + 1][0] - dts <= FLOOR:
            high += 1
        above.append(late[index] - min(late[low:high + 1]))
    gaps = [b[1] - a[1] for a, b in zip(steady, steady[1:])]
    batches = [1]
    for gap in gaps:
        if gap < interval / 4:
            batches[-1] += 1
        else:
            batches.append(1)
    # Each packet's batch, by packet: a batch of n is n packets that each
    # rode it, and a sawtooth n - 1 frame intervals high.
    riding = [float(size) for size in batches for _ in range(size)]
    ms = 1000.0
    return {
        "packets": len(steady),
        "fps": fps,
        "interval_ms": round(interval * ms, 2),
        "above_least_ms": {
            "median": round(statistics.median(above) * ms, 1),
            "p90": round(percentile(above, 0.90) * ms, 1),
            "p99": round(percentile(above, 0.99) * ms, 1),
            "max": round(max(above) * ms, 1),
            "mean": round(statistics.fmean(above) * ms, 1),
        },
        "above_least_intervals_p90": round(percentile(above, 0.90) / interval, 2),
        "spacing_ms": {
            "median": round(statistics.median(gaps) * ms, 1),
            "p90": round(percentile(gaps, 0.90) * ms, 1),
            "max": round(max(gaps) * ms, 1),
        },
        "batch": {
            "count": len(batches),
            "max": max(batches),
            # By packet: the size of the batch the median packet, the 90th
            # and the 99th percentile one arrived in.
            "packet_median": statistics.median(riding),
            "packet_p90": percentile(riding, 0.90),
            "packet_p99": percentile(riding, 0.99),
        },
        # A stretch of the sawtooth, a few seconds in: the lateness of each
        # packet above the least late one, in milliseconds.
        "shape_ms": [round(value * ms) for value in above[fps * 2:fps * 2 + fps * 2]],
        # Every packet past the join: its dts and when it arrived, seconds.
        "timed": [(round(dts, 6), round(at, 6)) for dts, at, _ in steady],
    }


def one_run(args: argparse.Namespace) -> dict:
    work = Path(tempfile.mkdtemp(prefix="moq-latency-", dir=args.work))
    relay = publisher = reader = None
    query = work / "publish.sql"
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
        make_source(source, args.seconds + 10, args.fps, args.size)
        query.write_text(PUBLISH_QUERY)

        port = free_udp_port()
        relay = start_relay(port, work / "cert.pem", work / "key.pem", work / "relay.log")
        print(f"moq-relay pid {relay.pid} on 127.0.0.1:{port}", flush=True)

        env = dict(os.environ)
        env.setdefault("FFRWD_WASM", str(SIDECAR))
        argv = ffrwd_argv("run", "-f", str(query), "-q",
                          "-v", f"relay=moqt://127.0.0.1:{port}",
                          "-v", f"broadcast={BROADCAST}",
                          "-v", f"cert={cert_hex}",
                          "-v", f"gop={args.fps}",
                          "-v", f"source={source}")
        publisher = Tail(spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, env=env, cwd=PACKAGE))
        # The reader goes up once the broadcast is on the relay: a reader
        # that waits for it inside the run is a stage the host sees idle,
        # and the host stops a stage idle for 30 s.
        params = {"relay": f"moqt://127.0.0.1:{port}", "broadcast": BROADCAST,
                  "cert": cert_hex}
        until = time.monotonic() + 120
        while node_shape(guest("subscribe"), params, strict=False) is None:
            if publisher.child.poll() is not None or time.monotonic() > until:
                sys.exit(f"the broadcast never came up:\n{publisher.text()[1][-1500:]}")

        pipe = TimedPipe(f"latency-{args.fps}", work / "video.nut")
        read_query = work / "subscribe.sql"
        read_query.write_text(SUBSCRIBE_QUERY)
        started = time.monotonic()
        reader = Tail(spawn(ffrwd_argv("run", "-f", str(read_query), "-y",
                                       "-v", f"relay=moqt://127.0.0.1:{port}",
                                       "-v", f"broadcast={BROADCAST}",
                                       "-v", f"cert={cert_hex}",
                                       "-v", f"out={pipe.spelling}"),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                            env=env, cwd=PACKAGE))
        published = publisher.finish(args.seconds + 120)
        read = reader.finish(60)
        pipe.finish(30)
        (work / "publisher.err").write_text(publisher.text()[1], encoding="utf-8")
        (work / "reader.err").write_text(reader.module_stderr(), encoding="utf-8")
        if published != 0:
            sys.exit(f"the publisher exited {published}:\n{publisher.text()[1][-1500:]}")
        if read != 0:
            sys.exit(f"the reader exited {read}:\n{reader.module_stderr()[-1500:]}")
        result = measure(work / "video.nut", pipe, started, args.fps, args.settle)
        result["reconnects"] = sum(1 for row in reader.counters() if row.get("kind") == "reconnect")
        return result
    finally:
        for child in (reader, publisher):
            if child is not None:
                kill_tree(child.child)
        if relay is not None:
            kill_tree(relay)
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print(f"work kept at {work}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fps", type=int, default=FPS)
    parser.add_argument("--seconds", type=int, default=None)
    parser.add_argument("--long", action="store_true",
                        help=f"{LONG_SECONDS} s rather than {SECONDS} s")
    parser.add_argument("--size", default="1920x1080")
    parser.add_argument("--settle", type=float, default=SETTLE)
    parser.add_argument("--spread-max", type=float, default=SPREAD_MAX,
                        help="frame intervals the p90 lateness may sit above the least late")
    parser.add_argument("--work", default=str(PACKAGE / "target"))
    parser.add_argument("--json", help="also write the result here")
    parser.add_argument("--no-build", dest="build", action="store_false")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    if args.seconds is None:
        args.seconds = LONG_SECONDS if args.long else SECONDS
    if not SIDECAR.exists():
        sys.exit(f"no sidecar at {SIDECAR}; build it first (cargo build --release)")
    result = one_run(args)
    shape = result.pop("shape_ms")
    timed = result.pop("timed")
    print(json.dumps(result, indent=1))
    print("shape (ms above the least late, two seconds of packets):", shape)
    if args.json:
        Path(args.json).write_text(json.dumps({**result, "shape_ms": shape, "timed": timed}))
    problems = []
    # A batch of three is a sawtooth two frame intervals high.
    if result["batch"]["packet_p90"] > args.spread_max:
        problems.append(f"one packet in ten or more arrived in a batch of "
                        f"{result['batch']['packet_p90']:.0f} or more: a sawtooth over "
                        f"{args.spread_max:.0f} frame intervals")
    if result["above_least_ms"]["median"] > result["interval_ms"]:
        problems.append(f"the median packet arrived {result['above_least_ms']['median']} ms "
                        f"above the least late one near it, over a frame interval")
    if result["reconnects"]:
        problems.append(f"{result['reconnects']} reconnect(s)")
    if problems:
        print("=== FAIL ===")
        for problem in problems:
            print(" -", problem)
        sys.exit(1)
    print("=== PASS: packets left subscribe one at a time ===")


if __name__ == "__main__":
    main()
