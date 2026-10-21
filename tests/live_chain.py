"""The chain oracle: a broadcast read, re-encoded and published again, timed by the clock in its picture.

    python tests/live_chain.py [--long] [--fps N] [--seconds N] [--drop-at S] [--down S]
                               [--start live|backlog] [--vcodec C] [--preset P]
                               [--bframes N] [--gop N] [--bitrate B]
                               [--package DIR] [--json PATH] [--work DIR] [--no-build] [--keep]

What a node of a relay tree does, end to end: a source publishing a live
1080p H.264 and AAC broadcast to relay A, a query that reads it with
`ffrwd.moq.subscribe`, re-encodes the picture and publishes the result with
`ffrwd.moq.publish` to relay B under a new name (recipes/relay.sql's shape,
with an encoder in it), and readers timing every frame at three places:

  - `native`, hang's own stack (harness/hang-recv) reading the source on A:
    what the source, its encoder and the relay cost, and the reference the
    other two are held to;
  - `subscribe`, `ffrwd.moq.subscribe` reading the source on A into a NUT
    pipe, stream-copied: what subscribe adds;
  - `chain`, hang-recv reading the republished broadcast on B: the whole
    node, subscribe, decode, encode, publish and the second relay.

Each frame carries the wall clock. ffmpeg draws the test pattern and stamps
its top 64 rows with 48 bits: the frame's own presentation time, which is
milliseconds since an epoch this loop chooses and hands it, read in
real time (`-re`), so a frame is stamped with the moment it was due. Each
reader decodes what it gets with one ffmpeg thread, reads the bits back
and compares them with the clock. Both ends read the same clock, so a
reading is the whole path from the frame's creation to its decoded
picture, and every reader pays the same decoder.

`--drop-at` kills relay A that many seconds in and starts it again
`--down` seconds later on the same port: the source and the chain's
subscribe have to come back, and the native reader is started again. The
window around the drop is left out of the distributions.

What it reports, per reader: frames, and the latency median, p90, p99 and
max, and the batches (frames decoded within a quarter of a frame interval
of the one before); and of the republished broadcast, read whole by
hang-recv after the run: its streams, whether it decodes without an
error, where its keyframes fall, and how its audio and video line up.

It passes when every reader saw frames after the drop as well as before,
the bands step a frame at a time, the republished broadcast decodes clean
with its keyframes a GOP apart and its audio beside its video, and
subscribe's median sits within `--subscribe-max` ms of the native reader's.

Never collected by any suite: it needs moq-relay, ffmpeg, ffprobe, cargo
with the wasm32-wasip2 target and a wasi-sdk clang, the sidecar, and
harness/hang-recv built (HANG_RECV names another). It starts its relays and
readers itself and stops those alone.
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
import threading
import time
from pathlib import Path

from common import (
    EXE,
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

FPS = 60
# The short form, the default: 30 seconds, one join, no drop, which is
# enough to fail on a broken chain or a batching reader. --long is the
# matrix's run: 150 seconds with relay A dropped 50 seconds in.
SECONDS = 30
DROP_AT = 0.0
LONG_SECONDS = 150
LONG_DROP_AT = 50.0
DOWN = 4.0
SETTLE = 8.0
# The window after the relay comes back that the distributions leave out:
# the readers' own reconnects, and the chain catching up.
MEND = 45.0
WIDTH, HEIGHT = 1920, 1080
BITS = 48
BAND = 64
BUILD_DEADLINE = 1800
TOOL_DEADLINE = 300
SOURCE = "live/source"
COPY = "live/copy"
SUBSCRIBE_MAX = 40.0
# How far the copy's sound may move against its picture, in ms: a frame.
AV_SHIFT_MAX = 17.0

# The source: ffmpeg's live stream over UDP, published as it arrives.
SOURCE_QUERY = """
COPY (
  SELECT f.video[1], f.audio[1]
  FROM input(:'udp') f
) TO ffrwd.moq.publish(:'relay', :'broadcast', COALESCE(:'cert', ''), '', 200, 'summary', 60, 0)
  {encode}
"""

# The node: the source read at `start`, re-encoded, and published again.
CHAIN_QUERY = """
COPY (
  SELECT s.video[1], s.audio[1]
  FROM ffrwd.moq.subscribe(:'from_relay', :'from', COALESCE(:'cert', ''), '', :'start') s
) TO ffrwd.moq.publish(:'to_relay', :'into', COALESCE(:'cert', ''), '', 200, 'summary', 60, 0)
  {encode}
"""

# A broadcast's picture and sound into a file, for how they line up.
WHOLE_QUERY = """
COPY (
  SELECT s.video[1], s.audio[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', '')) s
) TO :'out'
"""

# subscribe's own output, the picture stream-copied into a NUT.
TAP_QUERY = """
COPY (
  SELECT s.video[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''), '', :'start') s
) TO :'out' WITH (format 'nut')
"""


def encode_options(args: argparse.Namespace) -> str:
    """The WITH options of an encoding publish, as the run asks for them;
    `copy` asks for none."""
    if args.vcodec == "copy":
        return ""
    options = [f"video_codec '{args.vcodec}'", f"gop {args.gop}",
               f"video_bitrate '{args.bitrate}'", "audio_bitrate '128k'"]
    if args.preset:
        options.append(f"preset '{args.preset}'")
    if args.vcodec == "libx264":
        options.append("tune 'zerolatency'" if args.bframes == 0 else "tune 'film'")
        options.append(f"codec_params 'bframes={args.bframes}:b-adapt=0'")
    if args.vcodec.endswith(("_nvenc", "_d3d12va")):
        options.append("pix_fmt 'nv12'")
    return "WITH (" + ", ".join(options) + ")"


# How the source's picture is carried to its publisher, and how readers
# decode: on the CPU by default, and on the GPU where a machine has more
# of that than of cores (`--gpu`).
CARRY_CPU = ["-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency", "-crf", "16"]
CARRY_GPU = ["-c:v", "h264_nvenc", "-preset", "p1", "-tune", "ull", "-rc", "constqp", "-qp", "16",
             "-bf", "0"]


def source_encoder(args: argparse.Namespace) -> list[str]:
    """The source's own encoder, for `--source-copy`: the run's codec,
    GOP, bitrate and B-frames, as a camera's encoder would be set."""
    rate = ["-b:v", args.bitrate, "-maxrate", args.bitrate, "-bufsize", args.bitrate]
    if args.vcodec == "h264_nvenc":
        return ["-c:v", "h264_nvenc", "-preset", args.preset or "p1", "-g", str(args.gop),
                "-bf", str(args.bframes), *rate]
    return ["-c:v", "libx264", "-preset", "veryfast", "-g", str(args.gop),
            "-x264-params", f"bframes={args.bframes}:b-adapt=0", *rate]


def source_argv(epoch_us: int, fps: int, seconds: int, port: int,
                gpu: bool = False, pattern: str = "testsrc2",
                encoder: list[str] | None = None) -> list[str]:
    """ffmpeg drawing the picture with its clock in it, and a tone, live over
    UDP, as a camera's encoder would send them.

    The pattern is read in real time (`-re`), and each frame's top
    band spells the moment it left the pacing, in milliseconds since
    `epoch_us`, in 48 blocks. Its pts is then put back on a constant grid
    from the start of the run, at the frame rate, as an encoder's is: a
    coarse timer releases frames unevenly, and an uneven pts is not what a
    live source sends. The tone is paced and stamped the same way, from
    the same epoch, with a click on every whole second of its own time, so
    how sound and picture line up is fixed here, and read back off the
    source and off the copy alike."""
    start = f"(RTCSTART-{epoch_us})/(TB*1000000)"
    graph = (
        f"[0:v]settb=1/1000,setpts=(RTCTIME-{epoch_us})/(TB*1000000),split[a][b];"
        f"[b]format=gray,crop={BITS}:2:0:0,"
        f"geq=lum='16+219*mod(floor(T*1000/pow(2\\,{BITS - 1}-X))\\,2)',"
        f"scale={WIDTH}:{BAND}:flags=neighbor,format=yuv420p[band];"
        f"[a][band]overlay=0:0:format=yuv420,setpts=N/({fps}*TB)+{start}[v];"
        f"[1:a]asetpts=N/SR/TB+{start}[s]"
    )
    return [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-re", "-f", "lavfi", "-i", f"{pattern}=size={WIDTH}x{HEIGHT}:rate={fps}",
        "-re", "-f", "lavfi", "-i",
        "aevalsrc=exprs='0.6*sin(2*PI*1000*t)*lt(mod(t\\,1)\\,0.03)':s=48000:c=stereo",
        "-t", str(seconds), "-filter_complex", graph, "-map", "[v]", "-map", "[s]",
        "-fps_mode", "passthrough",
        # What carries the picture to the source's encoder: quick, and
        # clean enough that the band survives two encodes.
        *(encoder or [*(CARRY_GPU if gpu else CARRY_CPU), "-g", str(fps)]),
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-ac", "2", "-b:a", "192k",
        # The muxer's default 0.7 s of delay holds the picture back for the
        # sound and lets it go in lumps; a live encoder sends as it goes.
        "-muxdelay", "0", "-muxpreload", "0",
        "-f", "mpegts", "-mpegts_flags", "+resend_headers",
        f"udp://127.0.0.1:{port}?pkt_size=1316",
    ]


class Tap:
    """One reader's pictures, decoded, with the clock in each read back.

    `feed` is what hands the decoder its stream: a process whose stdout is
    the stream (a fresh one each time `restart` is called), or a pipe this
    loop relays. Every picture is a reading: (its clock, when it was read),
    both in milliseconds since the epoch.
    """

    def __init__(self, name: str, fps: int, demuxer: str, gpu: bool = False) -> None:
        self.gpu = gpu
        self.name = name
        self.fps = fps
        self.demuxer = demuxer
        self.readings: list[tuple[int, float]] = []
        self.lock = threading.Lock()
        self.decoders: list[subprocess.Popen] = []
        self.feeders: list[subprocess.Popen] = []
        self.errors: list[str] = []

    def decoder(self) -> subprocess.Popen:
        argv = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-fflags", "nobuffer",
                "-flags", "low_delay", "-threads", "1", "-probesize", "32768",
                *(["-hwaccel", "cuda"] if self.gpu else []),
                "-f", self.demuxer, "-i", "pipe:0", "-map", "0:v:0",
                "-vf", f"crop={WIDTH}:{BAND}:0:0,scale={BITS}:1:flags=area,format=gray",
                # Every decoded picture once, as it comes: a constant output
                # rate would repeat pictures across a gap.
                "-fps_mode", "passthrough", "-f", "rawvideo", "pipe:1"]
        child = spawn(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                      stderr=subprocess.PIPE)
        self.decoders.append(child)
        threading.Thread(target=self._read, args=(child,), daemon=True).start()
        threading.Thread(target=self._errors, args=(child,), daemon=True).start()
        return child

    def _read(self, child: subprocess.Popen) -> None:
        weights = [1 << (BITS - 1 - index) for index in range(BITS)]
        while True:
            band = child.stdout.read(BITS)
            if len(band) < BITS:
                return
            now = time.time() * 1000.0
            code = sum(weight for value, weight in zip(band, weights) if value > 125)
            with self.lock:
                self.readings.append((code, now))

    def _errors(self, child: subprocess.Popen) -> None:
        for line in child.stderr:
            self.errors.append(line.decode(errors="replace").rstrip())

    def feed_from(self, argv: list[str], env: dict | None = None) -> subprocess.Popen:
        """A process whose stdout is the stream, into a decoder of its own."""
        decoder = self.decoder()
        feeder = spawn(argv, stdout=decoder.stdin, stderr=subprocess.DEVNULL, env=env)
        decoder.stdin.close()
        self.feeders.append(feeder)
        return feeder

    def stop(self) -> None:
        for child in [*self.feeders, *self.decoders]:
            kill_tree(child)


class PipeFeed:
    """A pipe the sidecar writes one output into, relayed to a decoder's
    stdin as it arrives: a named pipe on Windows, a fifo elsewhere."""

    def __init__(self, name: str, work: Path, sink) -> None:
        self.sink = sink
        if os.name == "nt":
            import _winapi

            self.spelling = rf"\\.\pipe\moq-chain-{os.getpid()}-{name}"
            self.handle = _winapi.CreateNamedPipe(
                self.spelling, _winapi.PIPE_ACCESS_INBOUND, _winapi.PIPE_WAIT,
                1, 1 << 16, 1 << 16, 0, _winapi.NULL)
        else:
            self.spelling = str(work / f"{name}.fifo")
            os.mkfifo(self.spelling)
        threading.Thread(target=self._relay, daemon=True).start()

    def _relay(self) -> None:
        try:
            if os.name == "nt":
                import _winapi

                _winapi.ConnectNamedPipe(self.handle, False)
                while True:
                    try:
                        chunk, _ = _winapi.ReadFile(self.handle, 1 << 16, False)
                    except OSError:
                        break
                    if not chunk:
                        break
                    self.sink.write(chunk)
                    self.sink.flush()
            else:
                with open(self.spelling, "rb", buffering=0) as pipe:
                    while chunk := pipe.read(1 << 16):
                        self.sink.write(chunk)
                        self.sink.flush()
        except (BrokenPipeError, OSError):
            pass
        finally:
            try:
                self.sink.close()
            except OSError:
                pass


def hang_argv(hang: Path) -> list[str]:
    return [str(hang)]


def hang_env(port: int, cert_pem: Path, broadcast: str, output: str,
             video: str | None, audio: str | None = None, frames: bool = False) -> dict:
    """hang-recv's environment: a reader of `broadcast` on the relay at
    `port`. `frames` reads the video's frames as they arrive rather than
    through hang's export, which hands a group over once it is whole."""
    env = dict(os.environ)
    env.update({"RELAY_PORT": str(port), "RELAY_CERT_PEM": str(cert_pem),
                "BROADCAST": broadcast, "OUTPUT": output, "FRAMES": "1" if frames else "0"})
    for name, value in (("VIDEO", video), ("AUDIO", audio)):
        env.pop(name, None)
        if value is not None:
            env[name] = value
    return env


def wait_shape(relay: str, broadcast: str, cert_hex: str, maker: Tail,
               deadline: float = 180.0) -> dict:
    """The catalog of `broadcast`, as subscribe's shape reads it, once its
    publisher `maker` has it on the relay: a query reading a live input
    takes a while to probe it and start."""
    until = time.monotonic() + deadline
    while time.monotonic() < until:
        shape = node_shape(guest("subscribe"), {"relay": relay, "broadcast": broadcast,
                                                "cert": cert_hex}, strict=False)
        if shape is not None:
            return shape
        if maker.child.poll() is not None:
            break
    _, err = maker.text()
    sys.exit(f"'{broadcast}' was never announced; its publisher says:\n{err[-3000:]}")


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def summarise(tap: Tap, epoch_ms: float, started_ms: float, fps: int,
              gap: tuple[float, float] | None) -> dict:
    """A reader's latency distribution, leaving out the join and the drop."""
    interval = 1000.0 / fps
    with tap.lock:
        readings = list(tap.readings)
    kept, before, after, steps_bad = [], 0, 0, 0
    previous = None
    for code, now in readings:
        at = now - started_ms
        if previous is not None:
            step = code - previous
            # A frame is stamped when it leaves the pattern's pacing, which
            # on a coarse timer releases two at once now and then; what is
            # wrong is a clock that goes back, or jumps frames.
            if not (0 < step <= 4 * interval) and not (
                    gap and gap[0] - 2 <= at / 1000 <= gap[1] + MEND):
                steps_bad += 1
        previous = code
        if at < SETTLE * 1000:
            continue
        if gap and gap[0] * 1000 <= at <= (gap[1] + MEND) * 1000:
            continue
        early = bool(gap) and at < gap[0] * 1000
        if early:
            before += 1
        else:
            after += 1
        kept.append((now - (epoch_ms + code), now, early))
    if not kept:
        return {"frames": len(readings), "error": "no frames past the join"}
    latency = [value for value, _, _ in kept]
    arrivals = [now for _, now, _ in kept]
    phases = {}
    if gap:
        for label, wanted in (("before_drop", True), ("after_drop", False)):
            values = [value for value, _, early in kept if early == wanted]
            if values:
                phases[label] = {"median_ms": round(statistics.median(values), 1),
                                 "p90_ms": round(percentile(values, 0.90), 1),
                                 "max_ms": round(max(values), 1)}
    batches = [1]
    for a, b in zip(arrivals, arrivals[1:]):
        if b - a < interval / 4:
            batches[-1] += 1
        else:
            batches.append(1)
    riding = [float(size) for size in batches for _ in range(size)]
    return {
        "frames": len(readings),
        "measured": len(kept),
        "before_drop": before,
        "after_drop": after,
        "steps_not_one_frame": steps_bad,
        "median_ms": round(statistics.median(latency), 1),
        "p90_ms": round(percentile(latency, 0.90), 1),
        "p99_ms": round(percentile(latency, 0.99), 1),
        "max_ms": round(max(latency), 1),
        "min_ms": round(min(latency), 1),
        "batch_packet_p90": percentile(riding, 0.90),
        "batch_max": max(batches),
        "decoder_errors": len(tap.errors),
        "phases": phases,
    }


def clock_offsets(path: Path) -> list[float]:
    """Each picture's presentation time less the clock in its band, in
    milliseconds, in presentation order."""
    times = run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                 "frame=pts_time", "-of", "csv=p=0", str(path)],
                TOOL_DEADLINE, capture_output=True, text=True)
    pts = [float(line.strip(",")) for line in times.stdout.split()
           if line.strip(",") not in ("", "N/A")]
    bands = subprocess.run(
        ["ffmpeg", "-v", "error", "-i", str(path), "-map", "0:v:0", "-vf",
         f"crop={WIDTH}:{BAND}:0:0,scale={BITS}:1:flags=area,format=gray",
         "-f", "rawvideo", "-"], capture_output=True, timeout=TOOL_DEADLINE).stdout
    weights = [1 << (BITS - 1 - index) for index in range(BITS)]
    codes = [sum(w for value, w in zip(bands[i:i + BITS], weights) if value > 125)
             for i in range(0, len(bands) - BITS + 1, BITS)]
    return [p * 1000.0 - code for p, code in zip(pts, codes)]


def sync(path: Path) -> dict:
    """How the sound lines up with the picture in one file: the time of
    each click (one every second of the tone's own time) less the time of
    the nearest whole second of the clock drawn into the pictures, both in
    the file's own timestamps. A chain that keeps the two together keeps
    this the same."""
    if not path.exists():
        return {"error": f"{path.name} was not written"}
    times = run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                 "frame=pts_time", "-of", "csv=p=0", str(path)],
                TOOL_DEADLINE, capture_output=True, text=True)
    pts = [float(line.strip(",")) * 1000.0 for line in times.stdout.split()
           if line.strip(",") not in ("", "N/A")]
    offsets = clock_offsets(path)
    codes = [p - offset for p, offset in zip(pts, offsets)]
    seconds = []
    for (p0, c0), (p1, c1) in zip(zip(pts, codes), zip(pts[1:], codes[1:])):
        if c1 // 1000 > c0 // 1000 and 0 < c1 - c0 < 200:
            seconds.append(p1 - (c1 - (c1 // 1000) * 1000))
    first = run(["ffprobe", "-v", "error", "-select_streams", "a:0", "-show_entries",
                 "packet=pts_time", "-of", "csv=p=0", "-read_intervals", "%+#1", str(path)],
                TOOL_DEADLINE, capture_output=True, text=True)
    starts = [float(line.strip(",")) for line in first.stdout.split()
              if line.strip(",") not in ("", "N/A")]
    if not starts or not seconds:
        return {"error": "no sound or no clock", "seconds": len(seconds)}
    # Gaps in the sound (a reconnect leaves one) are filled with silence, so
    # a sample's place says its time.
    pcm = subprocess.run(["ffmpeg", "-v", "error", "-i", str(path), "-map", "0:a:0",
                          "-af", "aresample=async=1:min_hard_comp=0.001:first_pts=0",
                          "-ac", "1", "-ar", "48000", "-f", "s16le", "-"],
                         capture_output=True, timeout=TOOL_DEADLINE).stdout
    samples = memoryview(pcm).cast("h")
    clicks, quiet = [], 0
    for index in range(len(samples)):
        loud = abs(samples[index]) > 6000
        if loud and quiet > 24000:
            # The resampler starts its output at time zero, filling up to
            # the first sample with silence, so a sample's index is its time.
            clicks.append(index / 48.0)
        quiet = 0 if loud else quiet + 1
    lags = []
    for click in clicks:
        nearest = min(seconds, key=lambda second: abs(second - click))
        if abs(click - nearest) < 500:
            lags.append((click, click - nearest))
    if not lags:
        return {"error": "no click near a second of the clock", "clicks": len(clicks)}
    # The longest pause in the picture, if there is one: the drop.
    pause = max(zip(pts, pts[1:]), key=lambda pair: pair[1] - pair[0], default=None)
    report = {"clicks": len(clicks), "matched": len(lags)}

    def spread(values: list[float]) -> dict:
        return {"median": round(statistics.median(values), 1),
                "p10": round(percentile(values, 0.1), 1),
                "p90": round(percentile(values, 0.9), 1), "clicks": len(values)}

    report["click_less_second_ms"] = spread([lag for _, lag in lags])
    if pause and pause[1] - pause[0] > 2000:
        early = [lag for at, lag in lags if at < pause[0]]
        late = [lag for at, lag in lags if at > pause[1]]
        if early:
            report["before_pause"] = spread(early)
        if late:
            report["after_pause"] = spread(late)
    return report


def judge_copy(path: Path, gop: int, fps: int) -> tuple[dict, list[str]]:
    """The republished broadcast as hang-recv wrote it whole: its streams,
    a clean decode, keyframes a GOP apart, and its sound beside its picture."""
    problems: list[str] = []
    done = run(["ffprobe", "-v", "error", "-show_entries",
                "stream=index,codec_type,codec_name,width,height,has_b_frames",
                "-of", "json", str(path)], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        return {}, [f"the copy does not probe: {done.stderr[-400:]}"]
    streams = json.loads(done.stdout).get("streams", [])
    kinds = sorted(stream.get("codec_type") for stream in streams)
    if "video" not in kinds:
        problems.append(f"hang's export of the copy carries {kinds}, and no picture")
    decoded = run(["ffmpeg", "-v", "error", "-i", str(path), "-map", "0:v:0", "-f", "null", "-"],
                  TOOL_DEADLINE, capture_output=True, text=True)
    errors = [line for line in decoded.stderr.splitlines()
              if line.startswith(("[h264", "[aac")) or "rror while decoding" in line]
    if decoded.returncode != 0 or errors:
        problems.append(f"the copy decodes with {len(errors)} error line(s): "
                        f"{' | '.join(errors[:3])}")
    timing = {}
    for kind in ("v", "a"):
        got = run(["ffprobe", "-v", "error", "-select_streams", f"{kind}:0",
                   "-show_entries", "packet=pts_time,dts_time,flags", "-of", "csv=p=0",
                   str(path)], TOOL_DEADLINE, capture_output=True, text=True)
        rows = [line.split(",") for line in got.stdout.splitlines() if line.strip()]
        times = [float(row[0]) for row in rows if row[0] not in ("", "N/A")]
        keys = [float(row[0]) for row in rows if len(row) > 2 and "K" in row[2]
                and row[0] not in ("", "N/A")]
        timing[kind] = {"packets": len(times), "first": min(times, default=None),
                        "last": max(times, default=None), "keys": keys}
    keys = timing["v"]["keys"]
    spacings = [round((b - a) * fps) for a, b in zip(keys, keys[1:])]
    report = {
        "streams": [{key: stream.get(key) for key in
                     ("codec_type", "codec_name", "width", "height", "has_b_frames")}
                    for stream in streams],
        "video_packets": timing["v"]["packets"],
        "audio_packets": timing["a"]["packets"],
        "keyframes": len(keys),
        "keyframe_spacing_frames": sorted(set(spacings)),
        "decode_errors": len(errors),
    }
    # Both tracks were read until the same moment, so where each ends says
    # whether the sound runs on the picture's clock.
    if timing["v"]["last"] is not None and timing["a"]["last"] is not None:
        report["audio_less_video_last_s"] = round(timing["a"]["last"] - timing["v"]["last"], 3)
        if abs(report["audio_less_video_last_s"]) > 0.5:
            problems.append(f"the sound ends {report['audio_less_video_last_s']}s from the "
                            "picture")
    # Each picture's pts against the clock drawn into it: a constant, the
    # same in the source and the copy, when nothing moved the timestamps.
    offsets = clock_offsets(path)
    if offsets:
        report["pts_less_clock_ms"] = {"median": round(statistics.median(offsets), 1),
                                       "spread": round(max(offsets) - min(offsets), 1)}
    # A keyframe every GOP; the reconnect may restart one early.
    regular = [spacing for spacing in spacings if spacing != gop]
    if len(regular) > 3:
        problems.append(f"the copy's keyframes are not a GOP apart: {sorted(set(spacings))}")
    return report, problems


def one_run(args: argparse.Namespace) -> dict:
    work = Path(tempfile.mkdtemp(prefix="moq-chain-", dir=args.work))
    package = Path(args.package)
    relays: dict[str, subprocess.Popen] = {}
    children: list[subprocess.Popen] = []
    tails: list[Tail] = []
    taps: list[Tap] = []
    hang = Path(os.environ.get(
        "HANG_RECV",
        PACKAGE / "harness" / "hang-recv" / "target" / "release" / f"hang-recv{EXE}"))
    if not hang.exists():
        sys.exit(f"no hang-recv at {hang}; build harness/hang-recv first")
    try:
        if args.build:
            certgen = build_guests(BUILD_DEADLINE)[2]
        else:
            certgen = Path(os.environ.get(
                "CERTGEN",
                PACKAGE / "harness" / "certgen" / "target" / "release" / f"certgen{EXE}"))
        made = run([certgen, work], TOOL_DEADLINE, capture_output=True, text=True)
        if made.returncode != 0:
            sys.exit(f"certgen failed:\n{made.stderr[-400:]}")
        cert_hex = made.stdout.strip()
        cert_pem, key_pem = work / "cert.pem", work / "key.pem"

        port_a, port_b, port_udp = free_udp_port(), free_udp_port(), free_udp_port()
        relays["a"] = start_relay(port_a, cert_pem, key_pem, work / "relay-a.log")
        relays["b"] = start_relay(port_b, cert_pem, key_pem, work / "relay-b.log")
        relay_a, relay_b = f"moqt://127.0.0.1:{port_a}", f"moqt://127.0.0.1:{port_b}"
        print(f"relay A on {port_a}, relay B on {port_b}", flush=True)

        env = dict(os.environ)
        env.setdefault("FFRWD_WASM", str(SIDECAR))
        encode = encode_options(args)
        # A chain that copies still needs a source that was encoded.
        source_encode = encode if args.vcodec != "copy" else encode_options(
            argparse.Namespace(**{**vars(args), "vcodec": "libx264", "preset": "veryfast"}))
        source_query = work / "source.sql"
        # With `--source-copy` the source's own encoder is this ffmpeg's, at
        # the run's settings, and its publisher copies what it sends.
        # Its sound still goes through an encoder: AAC over MPEG-TS carries
        # no AudioSpecificConfig, which publish needs for the track's init.
        source_query.write_text(SOURCE_QUERY.format(
            encode="WITH (audio_bitrate '128k')" if args.source_copy else source_encode))
        chain_query = work / "chain.sql"
        chain_query.write_text(CHAIN_QUERY.format(encode=encode))
        tap_query = work / "tap.sql"
        tap_query.write_text(TAP_QUERY)

        # The publisher of the source goes up first and waits on its UDP.
        source_pub = Tail(spawn(ffrwd_argv(
            "run", "-f", source_query, "-q",
            "-v", f"udp=udp://127.0.0.1:{port_udp}?fifo_size=1000000&overrun_nonfatal=1",
            "-v", f"relay={relay_a}", "-v", f"broadcast={SOURCE}", "-v", f"cert={cert_hex}"),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, cwd=package))
        tails.append(source_pub)
        epoch_us = int(time.time() * 1e6) - 2_000_000
        epoch_ms = epoch_us / 1000.0
        started_ms = time.time() * 1000.0
        source = spawn(source_argv(epoch_us, args.fps, args.seconds + 30, port_udp, args.gpu,
                                   args.pattern, source_encoder(args) if args.source_copy else None),
                       stderr=open(work / "source.err", "w"))
        children.append(source)
        print("source up", flush=True)

        # The source's catalog, once it is on the relay: the rendition names
        # the native readers ask for.
        shape = wait_shape(relay_a, SOURCE, cert_hex, source_pub)
        video_name = json.loads(shape["relation"][0])["name"]
        print(f"the source's video rendition is '{video_name}'", flush=True)

        chain = Tail(spawn(ffrwd_argv(
            "run", "-f", chain_query, "-q",
            "-v", f"from_relay={relay_a}", "-v", f"from={SOURCE}",
            "-v", f"to_relay={relay_b}", "-v", f"into={COPY}",
            "-v", f"cert={cert_hex}", "-v", f"start={args.chain_start or args.start}"),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, cwd=package))
        tails.append(chain)

        native = Tap("native", args.fps, "mp4", args.gpu)
        taps.append(native)
        native.feed_from(hang_argv(hang), hang_env(port_a, cert_pem, SOURCE, "-", video_name, frames=True))

        through = Tap("subscribe", args.fps, "nut", args.gpu)
        taps.append(through)
        decoder = through.decoder()
        pipe = PipeFeed("subscribe", work, decoder.stdin)
        tapped = Tail(spawn(ffrwd_argv(
            "run", "-f", tap_query, "-y", "-q",
            "-v", f"relay={relay_a}", "-v", f"broadcast={SOURCE}",
            "-v", f"cert={cert_hex}", "-v", f"start={args.start}", "-v", f"out={pipe.spelling}"),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, cwd=package))
        tails.append(tapped)

        # The copy has to be announced before its reader can ask for it.
        shape_b = wait_shape(relay_b, COPY, cert_hex, chain)
        copy_name = json.loads(shape_b["relation"][0])["name"]
        audio_ports = [o for o in shape_b["outputs"]
                       if (o.get("format") or {}).get("codec") == "aac"]
        copied = Tap("chain", args.fps, "mp4", args.gpu)
        taps.append(copied)
        copied.feed_from(hang_argv(hang), hang_env(port_b, cert_pem, COPY, "-", copy_name, frames=True))
        # The source's picture and sound, and the copy's, each into a file:
        # how sound and picture line up in each. And hang's own export of the
        # copy's picture: what a hang player makes of its catalog, its init
        # segment and its groups.
        whole_query = work / "whole.sql"
        whole_query.write_text(WHOLE_QUERY)
        wholes = []
        readings = [(relay_a, SOURCE, "source.mkv"), (relay_b, COPY, "copy.mkv")]
        if args.twice:
            # The source read a second time, joining later: whether two
            # readers of one broadcast line its sound up alike.
            readings.append((relay_a, SOURCE, "source2.mkv"))
        for relay, broadcast, out in readings:
            if out == "source2.mkv":
                time.sleep(7.0)
            reader = Tail(spawn(ffrwd_argv(
                "run", "-f", whole_query, "-y", "-q",
                "-v", f"relay={relay}", "-v", f"broadcast={broadcast}",
                "-v", f"cert={cert_hex}", "-v", f"out={work / out}"),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, cwd=package))
            tails.append(reader)
            wholes.append(reader)
        whole = spawn(hang_argv(hang), env=hang_env(
            port_b, cert_pem, COPY, str(work / "copy.mp4"), copy_name),
            stdout=open(work / "copy.hang.log", "w"), stderr=subprocess.STDOUT)
        children.append(whole)

        gap = None
        deadline = time.monotonic() + args.seconds
        dropped = False
        while time.monotonic() < deadline:
            elapsed = time.time() - started_ms / 1000.0
            if args.drop_at and not dropped and elapsed >= args.drop_at:
                print(f"dropping relay A at {elapsed:.1f}s", flush=True)
                kill_tree(relays["a"])
                time.sleep(args.down)
                relays["a"] = start_relay(port_a, cert_pem, key_pem, work / "relay-a2.log")
                gap = (elapsed, time.time() - started_ms / 1000.0)
                print(f"relay A back at {gap[1]:.1f}s", flush=True)
                dropped = True
            # The native reader ends with its session; it is started again.
            if native.feeders[-1].poll() is not None and dropped:
                native.feed_from(hang_argv(hang),
                                 hang_env(port_a, cert_pem, SOURCE, "-", video_name, frames=True))
                time.sleep(1.0)
            for tail, name in ((source_pub, "the source's publisher"), (chain, "the chain"),
                               (tapped, "the subscribe reader")):
                if tail.child.poll() is not None:
                    _, err = tail.text()
                    sys.exit(f"{name} exited {tail.child.returncode}:\n{err[-2000:]}")
            time.sleep(0.25)

        print("stopping", flush=True)
        kill_tree(whole)
        for tap in taps:
            tap.stop()
        for tail in tails:
            kill_tree(tail.child)
            tail.finish(30)
        (work / "chain.err").write_text(chain.text()[1], encoding="utf-8")
        (work / "source.pub.err").write_text(source_pub.text()[1], encoding="utf-8")
        result: dict = {
            "fps": args.fps, "seconds": args.seconds, "start": args.start,
            "encode": encode, "package": str(package),
            "drop": gap and [round(gap[0], 1), round(gap[1], 1)],
            "readers": {tap.name: summarise(tap, epoch_ms, started_ms, args.fps, gap)
                        for tap in taps},
        }
        for tap in taps:
            (work / f"{tap.name}.readings.json").write_text(json.dumps(tap.readings))
        result["reconnects"] = {
            "source": sum(1 for row in source_pub.rows if row.get("event") == "reconnect"),
            "chain_publish": sum(1 for row in chain.rows if row.get("event") == "reconnect"),
            "chain_subscribe": sum(1 for row in chain.rows if row.get("kind") == "reconnect"),
            "subscribe_reader": sum(1 for row in tapped.rows if row.get("kind") == "reconnect"),
        }
        gaps = [row.get("gap_max_ms") for row in tapped.rows if row.get("kind") == "track"]
        result["subscribe_gap_max_ms"] = max((g for g in gaps if g is not None), default=None)
        copy_report, problems = judge_copy(work / "copy.mp4", args.gop, args.fps)
        result["copy"] = copy_report
        # Sound against picture in the copy, against the same in the source:
        # the chain re-encodes both and must keep how they line up.
        lined = {name: sync(work / f"{name}.mkv")
                 for name in ("source", "copy", *(("source2",) if args.twice else ()))}
        result["sync"] = lined
        if all("click_less_second_ms" in seen for seen in lined.values()):
            shift = (lined["copy"]["click_less_second_ms"]["median"]
                     - lined["source"]["click_less_second_ms"]["median"])
            result["copy_av_shift_ms"] = round(shift, 1)
            if abs(shift) > AV_SHIFT_MAX:
                problems.append(f"the copy's sound moved {shift:+.1f} ms against its picture")
        else:
            problems.append(f"the sound could not be lined up with the picture: {lined}")
        readers = result["readers"]
        for name, seen in readers.items():
            if "error" in seen:
                problems.append(f"{name}: {seen['error']}")
                continue
            if gap and (seen["before_drop"] == 0 or seen["after_drop"] == 0):
                problems.append(f"{name}: frames before the drop {seen['before_drop']}, "
                                f"after {seen['after_drop']}")
            if seen["steps_not_one_frame"] > args.fps:
                problems.append(f"{name}: {seen['steps_not_one_frame']} frames did not "
                                "follow the one before by a frame interval")
        if gap and not result["reconnects"]["chain_subscribe"]:
            problems.append("the chain's subscribe never said it reconnected")
        if "error" not in readers.get("subscribe", {"error": 1}) and \
                "error" not in readers.get("native", {"error": 1}):
            over = readers["subscribe"]["median_ms"] - readers["native"]["median_ms"]
            result["subscribe_less_native_median_ms"] = round(over, 1)
            if over > args.subscribe_max:
                problems.append(f"subscribe's median is {over:.1f} ms behind the native "
                                f"reader's, over {args.subscribe_max}")
        result["problems"] = problems
        return result
    finally:
        for tap in taps:
            tap.stop()
        for tail in tails:
            kill_tree(tail.child)
        for child in children:
            kill_tree(child)
        for relay in relays.values():
            kill_tree(relay)
        if args.keep:
            print(f"work kept at {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fps", type=int, default=FPS)
    parser.add_argument("--seconds", type=int, default=None)
    parser.add_argument("--long", action="store_true",
                        help=f"{LONG_SECONDS} s with relay A dropped {LONG_DROP_AT:.0f} s in")
    parser.add_argument("--drop-at", type=float, default=None,
                        help="seconds in to drop relay A; 0 drops nothing")
    parser.add_argument("--down", type=float, default=DOWN)
    parser.add_argument("--start", default="live", choices=("live", "backlog"),
                        help="where the subscribe reader and the chain join the source")
    parser.add_argument("--chain-start", default=None, choices=("live", "backlog"),
                        help="where the chain joins, when not where --start says")
    parser.add_argument("--vcodec", default="libx264")
    parser.add_argument("--preset", default=None)
    parser.add_argument("--bframes", type=int, default=0,
                        help="B-frames, for libx264; another encoder keeps its own")
    parser.add_argument("--gop", type=int, default=120)
    parser.add_argument("--bitrate", default="8M")
    parser.add_argument("--pattern", default="testsrc2",
                        help="the lavfi pattern drawn; testsrc costs a slow machine less")
    parser.add_argument("--source-copy", action="store_true",
                        help="encode the source in the pattern's own ffmpeg and publish it as it comes")
    parser.add_argument("--twice", action="store_true",
                        help="read the source's sound and picture twice, the second reader joining later")
    parser.add_argument("--gpu", action="store_true",
                        help="carry the source to its publisher with h264_nvenc and decode on CUDA")
    parser.add_argument("--subscribe-max", type=float, default=SUBSCRIBE_MAX,
                        help="ms subscribe's median may sit behind the native reader's")
    parser.add_argument("--package", default=str(PACKAGE),
                        help="the package the queries run against: this one, or a checkout of another version")
    parser.add_argument("--work", default=str(PACKAGE / "target"))
    parser.add_argument("--json", help="also write the result here")
    parser.add_argument("--no-build", dest="build", action="store_false")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    if args.seconds is None:
        args.seconds = LONG_SECONDS if args.long else SECONDS
    if args.drop_at is None:
        args.drop_at = LONG_DROP_AT if args.long else DROP_AT
    if args.preset is None:
        args.preset = {"libx264": "veryfast", "h264_nvenc": "p1"}.get(args.vcodec)
    result = one_run(args)
    print(json.dumps(result, indent=1))
    if args.json:
        Path(args.json).write_text(json.dumps(result, indent=1))
    if result["problems"]:
        print("=== FAIL ===")
        for problem in result["problems"]:
            print(" -", problem)
        sys.exit(1)
    print("=== PASS: the chain read, re-encoded and published the broadcast ===")


if __name__ == "__main__":
    main()
