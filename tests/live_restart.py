"""The restart oracle: a publisher replaced under a reader is a gap, not the end.

    python tests/live_restart.py [--warmup N] [--kill-gap N] [--end-gap N]
                                 [--restart-s N] [--work DIR] [--no-build] [--keep]

A program publishing a broadcast is sometimes replaced. Its pod is lost and
it comes back on another machine, or its run ends and the next run of the
same query starts. Each time a new publisher process publishes the same
broadcast name with the same query and settings, its timestamps and group
numbers starting over. An egress piece reading the broadcast copies it on to
an RTMP push, an HLS output or a file, and its run has to carry on across
the restart with a gap, rather than end.

The shape of a run: a local moq-relay, a PACED publisher of one rung, its
audio and a data track of messages (`tests/data/messages.nut`, cut to the
length of the publisher's file), a reader in the egress shape (`subscribe`
stream-copied to an FLV under `ffrwd run`, with `restart_s` set), and beside it
a second reader of the picture, the sound and the messages into a NUT, which
the egress shape does not read. The first publisher is killed
outright after `--warmup` seconds of media, every process of it at once, the
way a lost pod goes: nothing closes its session, so the relay hears nothing
until its idle timeout. A second is started `--kill-gap` seconds later and
runs its file to the end, which finishes its tracks. A third is started
`--end-gap` seconds after that and runs to its end too.

Every source carries a white frame at each whole second of its own time and a
click on the same second of its sound, so how sound and picture line up can be
read off the FLV on either side of each gap.

What it owes:

  - the reader is still running after both restarts, and ends of itself only
    after the last publisher has ended, exit 0;
  - the FLV holds media from all three publishers: three stretches of video
    and of audio, each several seconds long;
  - its timestamps never go back, on either track;
  - each gap is about as long as the stream was away: from the end of the
    last media before it to the moment the first media after it was
    produced, both read off the publishers' own rows against the wall clock;
  - the first video packet after each gap is a keyframe;
  - the clicks sit against the white frames after each gap where they sat
    before it, within one audio frame;
  - the reader says each restart in a row (`kind: restart`), the first
    `replaced` and the second `finished`, and the shift each row states is
    the one the white frames show;
  - the second reader follows both restarts too, its picture and sound in
    three stretches that never go back, a keyframe after each gap, and its
    messages never going back either: each one after a gap is a message of
    the fixture under the shift its reader's row states, and every message a
    publisher that started again sent after its first media went out came
    through.

Never collected by any suite: it needs moq-relay, ffmpeg, ffprobe, cargo with
the wasm32-wasip2 target and a wasi-sdk clang, and the sidecar. It starts a
relay, three publishers and a reader of its own and stops those alone.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
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

BROADCAST = "jobs/restart/out"
RATE = 30
GOP = 30
SAMPLE_RATE = 48000
AUDIO_FRAME = 1024 / SAMPLE_RATE
WARMUP = 15.0
KILL_GAP = 3.0
END_GAP = 4.0
SECONDS = (120, 40, 25)
RESTART_S = 20
BUILD_DEADLINE = 1800
TOOL_DEADLINE = 300
# A jump in decode time this long between two packets of a track is a gap.
GAP_MIN = 1.5
# How far a gap may be from the time the stream was away. The reader reads
# each publisher's clock off the packets that reached it, and this loop reads
# it off the rows the publisher writes as each group closes; the two agreed
# within 20 ms on an idle machine.
AWAY_SLACK = 0.25

MESSAGES = PACKAGE / "tests" / "data" / "messages.nut"
MESSAGE_PTS = PACKAGE / "tests" / "data" / "messages.txt"

PUBLISH_QUERY = """
COPY (
  SELECT scale(f.video[1], 640, -2), f.audio[1], d.data[1]
  FROM input(:'source', realtime => true) f, input(:'messages', realtime => true) d
) TO ffrwd.moq.publish(:'relay', :'broadcast', COALESCE(:'cert', ''), '', 200, 'groups', 60, 0)
  WITH (video_bitrate '800k', gop 30, preset 'veryfast', tune 'zerolatency',
        audio_bitrate '128k')
"""

# The egress shape: the program's broadcast stream-copied to a destination,
# here a file.
SUBSCRIBE_QUERY = """
COPY (
  SELECT s.*
  FROM ffrwd.moq.subscribe(:'subscribe_relay', :'subscribe_broadcast', :'subscribe_cert',
                           restart_s => :restart_s) s
) TO :'destination'
"""


# Every track, the messages beside the picture and the sound, into a NUT.
ALL_QUERY = """
COPY (
  SELECT s.video[1], s.audio[1], s.data[1]
  FROM ffrwd.moq.subscribe(:'subscribe_relay', :'subscribe_broadcast', :'subscribe_cert',
                           restart_s => :restart_s) s
) TO :'destination'
"""


class TimedTail(Tail):
    """A child's rows with the moment each one arrived, monotonic seconds."""

    def __init__(self, child: subprocess.Popen, dump: Path | None = None) -> None:
        self.arrived: list[tuple[float, dict]] = []
        super().__init__(child, dump)

    def _drain(self, pipe, into: list[str], parse: bool) -> None:
        if pipe is None:
            return
        for line in pipe:
            at = time.monotonic()
            with self.lock:
                into.append(line)
                if parse and line.lstrip().startswith("{"):
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    self.rows.append(row)
                    self.arrived.append((at, row))


def make_source(path: Path, seconds: int) -> None:
    """A test pattern with a white frame at each whole second, and a click on
    each whole second of the sound."""
    graph = (
        "[0:v]drawbox=x=0:y=0:w=iw:h=ih:color=white:t=fill:"
        "enable='lt(mod(t\\,1)\\,0.02)'[v]"
    )
    done = run([
        "ffmpeg", "-v", "error", "-y",
        "-f", "lavfi", "-i", f"testsrc2=size=640x360:rate={RATE}",
        "-f", "lavfi", "-i",
        f"aevalsrc=exprs='0.6*sin(2*PI*1000*t)*lt(mod(t\\,1)\\,0.03)':s={SAMPLE_RATE}:c=stereo",
        "-filter_complex", graph, "-map", "[v]", "-map", "1:a",
        "-t", str(seconds), "-c:v", "libx264", "-preset", "ultrafast",
        "-g", str(GOP), "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "192k", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"building the source failed:\n{done.stderr[-800:]}")


def cut_messages(path: Path, seconds: int) -> None:
    """The fixture's messages up to two seconds short of `seconds`, so a
    publisher's run ends with its file."""
    done = run(["ffmpeg", "-v", "error", "-y", "-i", str(MESSAGES), "-map", "0", "-c", "copy",
                "-t", str(seconds - 2), str(path)], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"cutting the messages failed:\n{done.stderr[-800:]}")


def fixture_pts() -> list[float]:
    """Every message's pts in the fixture, seconds."""
    return [int(line.split("\t", 1)[0]) / 1e6
            for line in MESSAGE_PTS.read_text().split("\n") if line.strip()]


def start_publisher(query: Path, source: Path, messages: Path, port: int,
                    cert_hex: str) -> TimedTail:
    argv = ffrwd_argv("run", "-f", str(query),
                      "-v", f"source={source}",
                      "-v", f"messages={messages}",
                      "-v", f"relay=moqt://127.0.0.1:{port}",
                      "-v", f"broadcast={BROADCAST}",
                      "-v", f"cert={cert_hex}", "-q")
    print("+ publisher:", " ".join(argv[:6]), "...", flush=True)
    return TimedTail(spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           text=True, env=dict(os.environ), cwd=PACKAGE))


def start_reader(query: Path, port: int, cert_hex: str, out: Path,
                 restart_s: int) -> Tail:
    env = dict(os.environ)
    dump = out.parent / f"{out.stem}-stderr"
    env["FFRWD_DUMP_STDERR"] = str(dump)
    argv = ffrwd_argv("run", "-f", str(query), "-y",
                      "-v", f"subscribe_relay=moqt://127.0.0.1:{port}",
                      "-v", f"subscribe_broadcast={BROADCAST}",
                      "-v", f"subscribe_cert={cert_hex}",
                      "-v", f"restart_s={restart_s}",
                      "-v", f"destination={out}", "-q")
    print("+ reader:", " ".join(argv[:6]), "...", flush=True)
    return Tail(spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                      text=True, env=env, cwd=PACKAGE), dump)


def descendants(pid: int) -> list[int]:
    """Every process under `pid`, whatever session or group it was put in."""
    table = subprocess.run(["ps", "-eo", "pid=,ppid="], capture_output=True,
                           text=True).stdout
    children: dict[int, list[int]] = {}
    for line in table.split("\n"):
        if line.strip():
            child, parent = (int(field) for field in line.split())
            children.setdefault(parent, []).append(child)
    found, todo = [], [pid]
    while todo:
        for child in children.get(todo.pop(), []):
            found.append(child)
            todo.append(child)
    return found


def kill_outright(tail: Tail) -> None:
    """Every process of a publisher at once, the way a lost pod goes.

    `ffrwd run` puts ffmpeg and the sidecar in sessions of their own, so a
    kill of its group alone leaves the sidecar to see its input end and finish
    its tracks: a run that ends, not a publisher that is lost."""
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(tail.child.pid)],
                       capture_output=True, timeout=30)
    else:
        for pid in [*reversed(descendants(tail.child.pid)), tail.child.pid]:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    tail.child.wait(timeout=30)


def media_clock(publisher: TimedTail) -> float:
    with publisher.lock:
        rows = [row for row in publisher.rows if "group" in row]
    return max((float(row.get("pts_end", 0.0)) for row in rows), default=0.0)


def wall_origin(publisher: TimedTail) -> float:
    """When the publisher's media time 0 went by, on this loop's clock: the
    least of each group row's arrival less the media time the group ended
    at. A row goes out as its group closes, so the least is the one that
    waited least."""
    with publisher.lock:
        timed = [(at, row) for at, row in publisher.arrived if "pts_end" in row]
    if not timed:
        sys.exit("a publisher wrote no group rows")
    return min(at - float(row["pts_end"]) for at, row in timed)


def wait_until(check, deadline: float, what: str, *children: Tail) -> None:
    started = time.monotonic()
    while time.monotonic() - started < deadline:
        if check():
            return
        for child in children:
            if child.child.poll() is not None:
                out, err = child.text()
                sys.exit(f"{what}: a child stopped early (exit {child.child.returncode}):\n"
                         f"{child.module_stderr()[-3000:]}\n{out[-1500:]}")
        time.sleep(0.2)
    sys.exit(f"{what}: not within {deadline:.0f}s")


def packets(path: Path, stream: str) -> list[dict]:
    done = run(["ffprobe", "-v", "error", "-select_streams", stream,
                "-show_entries", "packet=pts_time,dts_time,duration_time,flags",
                "-of", "json", str(path)], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe {path.name}:\n{done.stderr[-600:]}")
    out = []
    for packet in json.loads(done.stdout).get("packets", []):
        # A message carries a pts alone, which is its decode time too.
        times = [packet.get(key) for key in ("dts_time", "pts_time")]
        times = [float(value) for value in times if value not in (None, "N/A")]
        if not times:
            continue
        duration = packet.get("duration_time")
        out.append({
            "dts": times[0],
            "pts": times[-1],
            "duration": float(duration) if duration not in (None, "N/A") else 0.0,
            "key": "K" in packet.get("flags", ""),
        })
    return out


def stretches(found: list[dict]) -> list[list[dict]]:
    """A track's packets cut at every gap."""
    out: list[list[dict]] = []
    for packet in found:
        if out and packet["dts"] - out[-1][-1]["dts"] < GAP_MIN:
            out[-1].append(packet)
        else:
            out.append([packet])
    return out


def flashes(path: Path) -> list[float]:
    """When each white frame is presented, in the file's own time."""
    done = run(["ffmpeg", "-v", "error", "-copyts", "-i", str(path), "-map", "0:v:0",
                "-vf", "signalstats,metadata=print:key=lavfi.signalstats.YAVG:file=-",
                "-f", "null", "-"], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"decoding the picture:\n{done.stderr[-800:]}")
    out, at = [], None
    for line in done.stdout.split("\n"):
        if line.startswith("frame:"):
            at = float(line.split("pts_time:")[1].split()[0])
        elif line.startswith("lavfi.signalstats.YAVG=") and at is not None:
            if float(line.split("=")[1]) > 200:
                out.append(at)
    return out


def clicks(path: Path) -> list[float]:
    """When each click starts, in the file's own time. A gap in the sound is
    filled with silence on the way out of the decoder, so a sample's place
    is its time."""
    pcm = subprocess.run(["ffmpeg", "-v", "error", "-copyts", "-i", str(path),
                          "-map", "0:a:0",
                          "-af", "aresample=async=1:min_hard_comp=0.001:first_pts=0",
                          "-ac", "1", "-ar", str(SAMPLE_RATE), "-f", "s16le", "-"],
                         capture_output=True, timeout=TOOL_DEADLINE).stdout
    samples = memoryview(pcm).cast("h")
    out, quiet = [], 0
    for index in range(len(samples)):
        loud = abs(samples[index]) > 6000
        if loud and quiet > SAMPLE_RATE // 2:
            out.append(index / SAMPLE_RATE)
        quiet = 0 if loud else quiet + 1
    return out


def offsets(flash: list[float], click: list[float], start: float, end: float) -> list[float]:
    """Each click in [start, end) less the white frame nearest it."""
    inside = [at for at in flash if start - 0.5 <= at < end + 0.5]
    out = []
    for at in click:
        if start <= at < end and inside:
            nearest = min(inside, key=lambda frame: abs(frame - at))
            if abs(at - nearest) < 0.5:
                out.append(at - nearest)
    return out


def phase(values: list[float]) -> float:
    """Where in the second a set of times a second apart sits: the first of
    them, moved by the median of how far each of the others is off a whole
    second from it. Only its place within the second means anything."""
    first = values[0]
    return first + statistics.median((value - first) - round(value - first)
                                     for value in values)


def judge(out: Path, publishers: list[TimedTail], reader: Tail, ended: dict) -> list[str]:
    problems: list[str] = []
    rows = [row for row in reader.counters() if row.get("kind") == "restart"]
    for row in rows:
        print("reader row:", json.dumps(row), flush=True)
    seen = [row.get("seen") for row in rows]
    if seen != ["replaced", "finished"]:
        problems.append(f"the reader's restart rows say {seen}, not ['replaced', 'finished']")
    said = [line for line in reader.module_stderr().split("\n")
            if "the publisher started again" in line]
    if len(said) != 2:
        problems.append(f"the reader said {len(said)} restart(s) on stderr, not 2")

    video, audio = packets(out, "v:0"), packets(out, "a:0")
    for name, found in (("video", video), ("audio", audio)):
        back = [(a["dts"], b["dts"]) for a, b in zip(found, found[1:]) if b["dts"] <= a["dts"]]
        if back:
            problems.append(f"{name}: {len(back)} decode times that do not go forward, "
                            f"the first {back[0]}")
    parts = {"video": stretches(video), "audio": stretches(audio)}
    for name, part in parts.items():
        print(f"  {name}: " + ", ".join(
            f"{p[0]['dts']:.3f}s to {p[-1]['dts']:.3f}s ({len(p)} packets)" for p in part),
            flush=True)
        if len(part) != 3:
            problems.append(f"{name}: {len(part)} stretches where three publishers were read")
        for index, stretch in enumerate(part):
            if stretch[-1]["dts"] - stretch[0]["dts"] < 5.0:
                problems.append(f"{name}: stretch {index} is under 5s long")
    if problems:
        return problems

    origins = [wall_origin(publisher) for publisher in publishers]
    shifts = [0.0] + [row["shift_us"] / 1e6 for row in rows]
    flash, click = flashes(out), clicks(out)
    # Where each stretch's white frames sit in the second: the publishers'
    # own whole seconds, moved by the stretch's shift.
    phases = []
    for stretch in parts["video"]:
        inside = [at for at in flash if stretch[0]["pts"] - 0.1 <= at <= stretch[-1]["pts"] + 0.1]
        if len(inside) < 3:
            problems.append(f"only {len(inside)} white frames in the video from "
                            f"{stretch[0]['pts']:.3f}s")
            return problems
        phases.append(phase(inside))
    lined = []
    for index in range(len(parts["video"])):
        start = parts["audio"][index][0]["pts"]
        end = parts["audio"][index][-1]["pts"]
        found = offsets(flash, click, start, end)
        if len(found) < 3:
            problems.append(f"only {len(found)} clicks found against white frames in stretch "
                            f"{index}")
            return problems
        lined.append(statistics.median(found))
    print(f"  click less white frame, by stretch: "
          + ", ".join(f"{value * 1000:.1f} ms" for value in lined), flush=True)

    for gap in (1, 2):
        row = rows[gap - 1]
        print(f"restart {gap} ({row['seen']}): the reader says away {row['away_ms']} ms, "
              f"shift {row['shift_us'] / 1e6:+.6f}s", flush=True)
        # The shift the row states against the one the white frames show,
        # which is only known within the second.
        stated = shifts[gap] - shifts[gap - 1]
        shown = phases[gap] - phases[gap - 1]
        drift = (stated - shown) - round(stated - shown)
        print(f"  within the second, the white frames moved {(shown - round(shown)) * 1000:+.1f} "
              f"ms and the stated shift {(stated - round(stated)) * 1000:+.1f} ms: off by "
              f"{drift * 1000:+.1f} ms", flush=True)
        if abs(drift) > 1.5 / RATE:
            problems.append(f"restart {gap}: the white frames moved "
                            f"{(shown - round(shown)) * 1000:+.1f} ms within the second, "
                            f"where the row's shift says {stated:+.6f}s")
        for name in ("video", "audio"):
            before, after = parts[name][gap - 1][-1], parts[name][gap][0]
            measured = after["dts"] - (before["dts"] + before["duration"])
            # The publishers' own media times of the two packets, and when
            # each was produced on this loop's clock.
            made_after = origins[gap] + after["dts"] - shifts[gap]
            made_before = origins[gap - 1] + before["dts"] + before["duration"] - shifts[gap - 1]
            away = made_after - made_before
            print(f"  {name}: gap {measured:.3f}s from {before['dts']:.3f}s to "
                  f"{after['dts']:.3f}s; the stream was away {away:.3f}s; first packet "
                  f"after {'a keyframe' if after['key'] else 'NOT a keyframe'}", flush=True)
            if abs(measured - away) > AWAY_SLACK:
                problems.append(f"restart {gap}, {name}: a gap of {measured:.3f}s where the "
                                f"stream was away {away:.3f}s")
            if name == "video" and not after["key"]:
                problems.append(f"restart {gap}: the first video packet after the gap is "
                                f"not a keyframe")
        moved = lined[gap] - lined[gap - 1]
        print(f"  sound against picture moved {moved * 1000:+.1f} ms", flush=True)
        if abs(moved) > AUDIO_FRAME:
            problems.append(f"restart {gap}: the clicks moved {moved * 1000:+.1f} ms against "
                            f"the white frames, over one audio frame")
    if ended["reader"] < ended["last publisher"]:
        problems.append("the reader ended before the last publisher did")
    return problems


def judge_all(nut: Path, reader: Tail, sources: list[int]) -> list[str]:
    """What the reader of every track owes: its media as the egress reader's,
    and its messages in order, each after a gap under its reader's shift."""
    problems: list[str] = []
    rows = [row for row in reader.counters() if row.get("kind") == "restart"]
    for row in rows:
        print("second reader row:", json.dumps(row), flush=True)
    if [row.get("seen") for row in rows] != ["replaced", "finished"]:
        problems.append(f"the second reader's restart rows say {[r.get('seen') for r in rows]}")
        return problems
    video, audio = packets(nut, "v:0"), packets(nut, "a:0")
    messages = packets(nut, "d:0")
    for name, found in (("video", video), ("audio", audio)):
        back = [(a["dts"], b["dts"]) for a, b in zip(found, found[1:]) if b["dts"] <= a["dts"]]
        if back:
            problems.append(f"second reader, {name}: decode times that do not go forward, "
                            f"the first {back[0]}")
    parts = stretches(video)
    print("  second reader, video: " + ", ".join(
        f"{p[0]['dts']:.3f}s to {p[-1]['dts']:.3f}s" for p in parts), flush=True)
    if len(parts) != 3 or len(stretches(audio)) != 3:
        problems.append(f"second reader: {len(parts)} stretches of video and "
                        f"{len(stretches(audio))} of audio, not three")
        return problems
    for gap in (1, 2):
        if not parts[gap][0]["key"]:
            problems.append(f"second reader, restart {gap}: the first video packet after "
                            f"the gap is not a keyframe")
    times = [message["pts"] for message in messages]
    if any(b < a for a, b in zip(times, times[1:])):
        problems.append("second reader: a message went back")
    shifts = [0.0] + [row["shift_us"] / 1e6 for row in rows]
    fixture = fixture_pts()
    for index, stretch in enumerate(parts):
        start = stretch[0]["dts"] - 0.5
        end = parts[index + 1][0]["dts"] - 0.5 if index + 1 < len(parts) else float("inf")
        inside = [at for at in times if start <= at < end]
        if index == 0:
            continue
        # Each message after a gap is one of the fixture's, at its own pts
        # under the shift the reader's row states.
        own = [at - shifts[index] for at in inside]
        strays = [at for at in own if min(abs(at - pts) for pts in fixture) > 0.002]
        # Every message the publisher sent once its first media had gone
        # out: the fixture's, past the stretch's first media in the
        # publisher's own time, up to where its file was cut.
        first_media = stretch[0]["dts"] - shifts[index]
        owed = [pts for pts in fixture if first_media < pts < sources[index] - 2]
        missing = [pts for pts in owed if min((abs(at - pts) for at in own), default=1.0) > 0.002]
        print(f"  second reader, stretch {index}: {len(inside)} messages, the first at "
              f"{own[0] if own else float('nan'):.3f}s of its publisher's own time; "
              f"{len(strays)} not the fixture's, {len(missing)} of {len(owed)} owed missing",
              flush=True)
        if strays:
            problems.append(f"second reader, stretch {index}: messages at {strays[:3]} of their "
                            f"publisher's time are none of the fixture's")
        if missing:
            problems.append(f"second reader, stretch {index}: the messages at {missing[:3]} did "
                            f"not come through")
    return problems


def one_run(args: argparse.Namespace) -> None:
    work = Path(tempfile.mkdtemp(prefix="moq-restart-", dir=args.work))
    relay = None
    publishers: list[TimedTail] = []
    reader = everything = None
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
        sources, messages = [], []
        for index, seconds in enumerate(SECONDS):
            sources.append(work / f"source{index}.mp4")
            make_source(sources[-1], seconds)
            messages.append(work / f"messages{index}.nut")
            cut_messages(messages[-1], seconds)
        publish = work / "publish.sql"
        publish.write_text(PUBLISH_QUERY)
        subscribe = work / "subscribe.sql"
        subscribe.write_text(SUBSCRIBE_QUERY)
        every_track = work / "every-track.sql"
        every_track.write_text(ALL_QUERY)

        port = free_udp_port()
        relay = start_relay(port, work / "cert.pem", work / "key.pem", work / "relay.log")
        print(f"moq-relay pid {relay.pid} on 127.0.0.1:{port}", flush=True)

        publishers.append(start_publisher(publish, sources[0], messages[0], port, cert_hex))
        # The reader compiles against the catalog, so the broadcast is up
        # first.
        params = {"relay": f"moqt://127.0.0.1:{port}", "broadcast": BROADCAST,
                  "cert": cert_hex}
        until = time.monotonic() + 120
        while node_shape(guest("subscribe"), params, strict=False) is None:
            if publishers[0].child.poll() is not None or time.monotonic() > until:
                sys.exit(f"the broadcast never came up:\n{publishers[0].text()[1][-1500:]}")
        out = work / "out.flv"
        reader = start_reader(subscribe, port, cert_hex, out, args.restart_s)
        everything = start_reader(every_track, port, cert_hex, work / "every-track.nut",
                                  args.restart_s)

        wait_until(lambda: media_clock(publishers[0]) >= args.warmup, 120 + args.warmup,
                   "warming up", publishers[0], reader, everything)
        print(f"killing the first publisher outright at media "
              f"{media_clock(publishers[0]):.1f}s", flush=True)
        kill_outright(publishers[0])
        time.sleep(args.kill_gap)
        publishers.append(start_publisher(publish, sources[1], messages[1], port, cert_hex))
        done = publishers[1].finish(SECONDS[1] + 120)
        print(f"the second publisher ended, exit {done}", flush=True)
        for child in (reader, everything):
            if child.child.poll() is not None:
                sys.exit(f"a reader ended with the second publisher:\n"
                         f"{child.module_stderr()[-3000:]}")
        time.sleep(args.end_gap)
        publishers.append(start_publisher(publish, sources[2], messages[2], port, cert_hex))
        done = publishers[2].finish(SECONDS[2] + 120)
        ended = {"last publisher": time.monotonic()}
        print(f"the third publisher ended, exit {done}", flush=True)
        for child in (reader, everything):
            if child.child.poll() is not None:
                sys.exit(f"a reader ended with the third publisher:\n"
                         f"{child.module_stderr()[-3000:]}")
        read = reader.finish(args.restart_s + 120)
        read_all = everything.finish(60)
        ended["reader"] = time.monotonic()
        print(f"the reader ended {ended['reader'] - ended['last publisher']:.1f}s after the "
              f"last publisher, exit {read}", flush=True)
        print("--- reader stderr (tail) ---")
        print(reader.module_stderr()[-2500:])
        problems = []
        if read != 0:
            problems.append(f"the reader exited {read}")
        if read_all != 0:
            problems.append(f"the second reader exited {read_all}:\n"
                            f"{everything.module_stderr()[-1500:]}")
        for index, publisher in enumerate(publishers[1:], start=1):
            if publisher.child.returncode != 0:
                problems.append(f"publisher {index} exited {publisher.child.returncode}")
        problems += judge(out, publishers, reader, ended)
        problems += judge_all(work / "every-track.nut", everything, list(SECONDS))
        if problems:
            print("=== FAIL ===")
            for problem in problems:
                print(" -", problem)
            sys.exit(1)
        print("=== PASS: the reader followed both restarts, and the media went on ===")
    finally:
        # Every process of a run still going, its ffmpeg and sidecar too,
        # which `ffrwd run` starts in sessions of their own.
        for child in (reader, everything, *publishers):
            if child is not None and child.child.poll() is None:
                kill_outright(child)
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
