"""The burst loop: every group survives a call that holds many of them.

    python tests/live_burst.py [--seconds N] [--keep]

A `process` call hands the sink whatever arrived since the last one.
Usually that is a packet or two, but anything upstream that stalls and
catches up - a windowed module buffering, a hiccup on a live input -
hands over a lump, and a lump spans many group boundaries. Every one of
those groups has to reach the relay: a publisher that quietly keeps the
last of them loses seconds of sound.

So the loop makes lumps on purpose. The audio goes through
`ffrwd.switch.audio`, which on ffrwd 0.18.0 hands the sink audio in
seconds-wide lumps, and the run is paced (`realtime => true`) like the
live stream it stands for. The module's own rows say which groups it
published; a subscriber shaped like a player - a default subscription,
whose latency window is zero, so a group that is no longer the latest
when it is reached is skipped rather than served - says which arrived.
Every group must.

Three cases: audio at the default grouping, audio at a group per frame
(the shape that loses everything without the per-group drive), and an
all-intra video stream where every frame is its own group.

Never collected by any suite: it needs moq-relay, wasmtime, ffmpeg,
cargo with the wasm32-wasip2 target and a wasi-sdk clang, and the
`ffrwd/switch` package installed to make the lumps. The process
machinery is in common.py; nothing it did not start is touched.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from common import (
    PACKAGE,
    SIDECAR,
    build_guests,
    ffrwd_argv,
    free_udp_port,
    guest,
    kill_tree,
    run,
    start_relay,
    start_subscriber,
)

RATE = 30
SAMPLE_RATE = 48000
BROADCAST = "live/burst"
BUILD_DEADLINE = 600
TOOL_DEADLINE = 120

# The query the loop publishes, with the audio led through a module so
# the sink is handed lumps rather than packets.
QUERY = """
COPY (
  WITH vid AS (
    SELECT f.video[1] AS v, 1 AS rung FROM input(:'source', realtime => true) f
  ),
  aud AS (
    SELECT ffrwd.switch.audio(a) AS t, 2 AS rung
    FROM input(:'source', realtime => true) g, unnest(g.audio) a
  )
  SELECT vid.v, aud.t
  FROM vid FULL JOIN aud ON vid.rung = aud.rung
) TO ffrwd.moq.publish(:'relay', :'broadcast', COALESCE(:'cert', ''), '',
                       :audio_group_ms, 'groups')
  WITH (gop :gop, preset 'veryfast', tune 'zerolatency', audio_bitrate '128k')
"""


def switch_installed() -> bool:
    done = run(ffrwd_argv("path", "-g", "ffrwd/switch"), 60,
               capture_output=True, text=True)
    return done.returncode == 0


def make_source(path: Path, seconds: int) -> None:
    done = run([
        "ffmpeg", "-v", "error", "-y",
        "-f", "lavfi", "-i", f"testsrc2=size=1280x720:rate={RATE}",
        "-f", "lavfi", "-i", f"sine=frequency=440:sample_rate={SAMPLE_RATE}",
        "-ac", "2", "-c:a", "aac", "-t", str(seconds),
        "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
        str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"building the source failed:\n{done.stderr[-800:]}")


def one_case(
    source: Path, dir: Path, name: str, *, group_ms: int, gop: int, watch: str,
    seconds: int,
) -> None:
    """One publish, and the groups it published against the ones that arrived."""
    out_dir = dir / f"out-{name}"
    out_dir.mkdir()
    query = PACKAGE / ".live-burst.sql"
    query.write_text(QUERY.replace(":gop", str(gop)))
    made = run([PACKAGE / "harness" / "certgen" / "target" / "release" / "certgen.exe", dir],
               TOOL_DEADLINE, capture_output=True, text=True)
    cert_hex = made.stdout.strip()
    relay = sub = None
    try:
        port = free_udp_port()
        relay = start_relay(port, dir / "cert.pem", dir / "key.pem", dir / f"relay-{name}.log")
        # The catalog names video first, so rendition 0 is the picture and
        # rendition 1 the sound.
        env = dict(os.environ)
        env["PLAYER"] = "1"
        os.environ["PLAYER"] = "1"
        sub = start_subscriber(
            guest("sub-recv"), port, cert_hex, out_dir, BROADCAST,
            rendition=0 if watch == "video" else 1, output=f"{name}.mp4",
        )
        env["FFRWD_WASM"] = str(SIDECAR)
        done = run(
            ffrwd_argv("run", "-f", query,
                       "-v", f"source={source}",
                       "-v", f"relay=moqt://127.0.0.1:{port}",
                       "-v", f"broadcast={BROADCAST}",
                       "-v", f"cert={cert_hex}",
                       "-v", f"audio_group_ms={group_ms}", "-q"),
            seconds * 6 + 180, capture_output=True, text=True, env=env, cwd=PACKAGE,
        )
        if done.returncode != 0:
            print(done.stdout[-1200:])
            sys.exit(f"the burst run failed:\n{done.stderr[-1500:]}")
        rows = []
        for line in done.stdout.splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        published = sorted(
            r["group"] for r in rows
            if "group" in r
            and (r["track"].startswith("a") if watch == "audio" else not r["track"].startswith("a"))
        )
        try:
            sub.communicate(timeout=seconds * 6 + 120)
        except subprocess.TimeoutExpired:
            kill_tree(sub)
        arrived = {
            int(m.group(1))
            for m in re.finditer(r"sub: group (\d+) complete", Path(sub.transcript).read_text())
        }
        # The reader is killed with the run, so the tail it never saw is
        # not loss: what is judged is every group up to the last arrival.
        last = max(arrived, default=-1)
        missing = [g for g in published if g <= last and g not in arrived]
        print(
            f"--- {name}: {len(published)} {watch} groups published, "
            f"{len(arrived)} arrived, {len(missing)} lost ---"
        )
        assert published, f"{name}: the module published no {watch} groups"
        assert len(arrived) > 10, f"{name}: only {len(arrived)} groups arrived"
        assert not missing, (
            f"{name}: {len(missing)} groups never reached the subscriber: "
            f"{missing[:20]}"
        )
        print(f"PASS {name}: every {watch} group through to {last}")
    finally:
        for child in (sub, relay):
            if child is not None:
                kill_tree(child)
        query.unlink(missing_ok=True)
        os.environ.pop("PLAYER", None)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=15)
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()

    if not SIDECAR.exists():
        sys.exit(f"no sidecar at {SIDECAR}; build it first (cargo build --release)")
    if not switch_installed():
        sys.exit(
            "this loop needs the ffrwd/switch package installed (`ffrwd install -g "
            "ffrwd/switch`): leading the audio through a module is what makes the "
            "lumps it is about"
        )
    build_guests(BUILD_DEADLINE)
    dir = Path(tempfile.mkdtemp(prefix="moq-burst-"))
    try:
        source = dir / "source.mp4"
        make_source(source, args.seconds)
        started = time.monotonic()
        one_case(source, dir, "audio-default", group_ms=100, gop=30,
                 watch="audio", seconds=args.seconds)
        one_case(source, dir, "audio-per-frame", group_ms=0, gop=30,
                 watch="audio", seconds=args.seconds)
        one_case(source, dir, "video-all-intra", group_ms=100, gop=1,
                 watch="video", seconds=args.seconds)
        print(f"PASS: every group survived the lumps ({time.monotonic() - started:.1f}s)")
    finally:
        if args.keep:
            print(f"kept: {dir}")
        else:
            shutil.rmtree(dir, ignore_errors=True)


if __name__ == "__main__":
    main()
