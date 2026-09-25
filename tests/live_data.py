"""The data-track oracle: every message arrives whole, on time, and ahead of its picture.

    python tests/live_data.py [--seconds N] [--relay-version V] [--reconnect]
                              [--fixture F] [--warmup N] [--down N] [--work DIR]
                              [--no-build] [--keep]

A data stream is a sequence of messages, each one JSON object at the pts it
was emitted at (a break's own cue is a `start_pts` field inside it, seconds
ahead). publish carries a data column as a track of its own, one message per
MoQ group, and subscribe hands each one back as a packet. A message is an
announcement, so what matters beside getting it there is WHEN: it must not
wait behind the media it announces.

The shape of a run: a local moq-relay, a PACED publisher (one rung, its audio,
and a data column read from `tests/data/messages.nut`: 43 messages over 98.5
seconds, two sharing a pts, one off the millisecond grid and outside ASCII,
one whose bytes are not their JSON's canonical spelling; `messages.txt` is
its source, which ffrwd-nut's `json_nut` example turns into the .nut), and
the subscribe module under the sidecar reading the video, the audio and the
data track to .nut files on one session. The video and the data leave the
sidecar through pipes this loop reads as they arrive, so each packet's
arrival is timed.

What it owes:

  - the catalog lists the data track: kind `data`, codec `json`, time base
    1/1000000, on the first row beside the media, and a query naming
    `s.data[1]` compiles against it;
  - every message is published, and every one arrives, byte for byte, in
    order, at the pts the publisher published it at (its `groups` rows say
    which), with no hole, late group or skip on the reader's data track;
  - every message leaves the subscriber no later than the first video
    packet whose pts is at or past the message's own.

`--fixture pairs` publishes `tests/data/pairs.nut` instead (its source is
`pairs.txt`): 81 messages of about 1.2 KB, in pairs at one pts every 2.5
seconds and one triple, which is what a node forwarding an upstream award
beside its own writes. About half of the pairs reach publish in one call,
which then has two groups of one track to write at once. A relay that keeps
only a track's latest group (Cloudflare's) lost the first of such a pair
more often than not; moq-relay keeps every group and loses nothing either
way, so here the case holds the rest: every message of a pair arrives, in
order, at its pts, with no hole.

`--relay-version moq-transport-16` holds the relay to that one IETF draft,
which sends no timescale: a subscriber there stamps every frame with the
time it arrived, so a message whose pts still matches the publisher's to
the microsecond has had it read out of the frame itself.

`--reconnect` kills the relay after `--warmup` seconds of media and starts it
again `--down` seconds later, as tests/live_reconnect.py does. Then both
halves have to say they reconnected, the messages lost are only those
published while the reader had no session, and messages go on arriving,
byte for byte and at their pts, after it.

Never collected by any suite: it needs moq-relay, ffmpeg, ffprobe, cargo with
the wasm32-wasip2 target and a wasi-sdk clang, and the sidecar. It starts a
relay and a reader of its own and stops those alone.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
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
    guest,
    kill_tree,
    run,
    spawn,
    start_relay,
)

BROADCAST = "live/data"
RATE = 30
GOP = 30
SAMPLE_RATE = 48000
SECONDS = 105
WARMUP = 20.0
DOWN = 4.0
# The idle timeout (30s) plus the reconnect backoff plus the relay's own
# start, with room.
MEND_DEADLINE = 90.0
BUILD_DEADLINE = 1800
TOOL_DEADLINE = 120
DATA = PACKAGE / "tests" / "data"

PUBLISH_QUERY = """
COPY (
  SELECT scale(f.video[1], 640, -2), f.audio[1], d.data[1]
  FROM input(:'source', realtime => true) f, input(:'messages', realtime => true) d
) TO ffrwd.moq.publish(:'relay', :'broadcast', COALESCE(:'cert', ''), '', 200, 'groups')
  WITH (video_bitrate '800k', gop 30, preset 'veryfast', tune 'zerolatency',
        audio_bitrate '128k')
"""

# What a query reading the broadcast back looks like; compiled, not run.
SUBSCRIBE_QUERY = """
COPY (
  SELECT s.video[1], s.audio[1], s.data[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', '')) s
) TO :'out'
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


def expected_messages(fixture: str) -> list[tuple[int, bytes]]:
    """The fixture's messages, in order: pts in microseconds, and bytes."""
    out = []
    for line in (DATA / f"{fixture}.txt").read_text(encoding="utf-8").splitlines():
        if line.strip():
            pts, message = line.split("\t", 1)
            out.append((int(pts), message.encode("utf-8")))
    return out


class TimedPipe:
    """A pipe the sidecar writes one output into, read as it arrives.

    Every read is logged as (monotonic time, bytes so far), and the bytes go
    to `path` as well, so a packet's offset in the file says when it came
    out of the reader. On Windows it is a named pipe, which the sidecar
    opens for writing rather than creating; elsewhere a fifo.
    """

    def __init__(self, name: str, path: Path) -> None:
        self.path = path
        self.log: list[tuple[float, int]] = []
        self.error: str | None = None
        if os.name == "nt":
            import _winapi

            self.spelling = rf"\\.\pipe\moq-live-data-{os.getpid()}-{name}"
            self.handle = _winapi.CreateNamedPipe(
                self.spelling,
                _winapi.PIPE_ACCESS_INBOUND,
                # Byte mode, which is 0 and has no name in _winapi.
                _winapi.PIPE_WAIT,
                1, 1 << 16, 1 << 16, 0, _winapi.NULL,
            )
        else:
            self.spelling = str(path.with_suffix(".fifo"))
            os.mkfifo(self.spelling)
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self) -> None:
        total = 0
        try:
            with open(self.path, "wb") as out:
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
                        total += len(chunk)
                        self.log.append((time.monotonic(), total))
                        out.write(chunk)
                    _winapi.CloseHandle(self.handle)
                else:
                    with open(self.spelling, "rb", buffering=0) as pipe:
                        while chunk := pipe.read(1 << 16):
                            total += len(chunk)
                            self.log.append((time.monotonic(), total))
                            out.write(chunk)
        except Exception as err:  # noqa: BLE001 - said below, not swallowed
            self.error = repr(err)

    def arrival(self, end: int) -> float | None:
        """When the byte at offset `end - 1` had come out."""
        for at, total in self.log:
            if total >= end:
                return at
        return None

    def finish(self, deadline: float) -> None:
        self.thread.join(timeout=deadline)
        if self.error is not None:
            sys.exit(f"reading {self.path.name}: {self.error}")


class TimedTail(Tail):
    """A Tail that also notes when each row reached this loop, which is
    as close as the loop gets to when the publisher closed a group: the
    run's rows cross the sidecar and the CLI on their way here."""

    def __init__(self, child: subprocess.Popen) -> None:
        self.stamped: list[tuple[float, dict]] = []
        super().__init__(child)

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
                    self.stamped.append((at, row))


def start_publisher(query: Path, source: Path, port: int, cert_hex: str,
                    fixture: str) -> Tail:
    env = dict(os.environ)
    env.setdefault("FFRWD_WASM", str(SIDECAR))
    argv = ffrwd_argv("run", "-f", str(query),
                      "-v", f"source={source}",
                      "-v", f"messages={DATA / f'{fixture}.nut'}",
                      "-v", f"relay=moqt://127.0.0.1:{port}",
                      "-v", f"broadcast={BROADCAST}",
                      "-v", f"cert={cert_hex}", "-q")
    print("+ publisher:", " ".join(argv[:6]), "...", flush=True)
    return TimedTail(spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           text=True, env=env, cwd=PACKAGE))


def reader_params(port: int, cert_hex: str) -> str:
    return json.dumps({
        "relay": f"moqt://127.0.0.1:{port}",
        "broadcast": BROADCAST,
        "cert": cert_hex,
        "token": "",
        "start": "live",
    })


# The catalog's own order: the video section, the audio one, the data one.
TRACKS = {"video": 0, "audio": 1, "data": 2}


def probe(port: int, cert_hex: str) -> dict:
    """The catalog as the sidecar reports it at compile time."""
    module = guest("subscribe")
    done = run([SIDECAR, "--probe", module, "-udp", module, "-http", module,
                "-params", reader_params(port, cert_hex)],
               TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"the probe failed:\n{done.stderr[-1500:]}")
    return json.loads(done.stdout.strip().splitlines()[-1])


def compile_subscriber(port: int, cert_hex: str, out: Path, query: Path) -> str:
    query.write_text(SUBSCRIBE_QUERY)
    env = dict(os.environ)
    env.setdefault("FFRWD_WASM", str(SIDECAR))
    done = run(ffrwd_argv("compile", "-f", str(query),
                          "-v", f"relay=moqt://127.0.0.1:{port}",
                          "-v", f"broadcast={BROADCAST}",
                          "-v", f"cert={cert_hex}",
                          "-v", f"out={out}"),
               TOOL_DEADLINE, capture_output=True, text=True, env=env, cwd=PACKAGE)
    if done.returncode != 0:
        sys.exit(f"a query naming s.data[1] did not compile:\n{done.stdout[-1500:]}"
                 f"\n{done.stderr[-1500:]}")
    return done.stdout


def start_reader(port: int, cert_hex: str, outputs: list[tuple[int, str]]) -> Tail:
    module = guest("subscribe")
    argv = [str(SIDECAR), "-udp", str(module), "-http", str(module),
            "-m", str(module), "-params", reader_params(port, cert_hex)]
    for track, spelling in outputs:
        argv += ["-track", str(track), "-f", "nut", spelling]
    print("+ reader:", " ".join(argv[:6]), "...", flush=True)
    return Tail(spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))


def hexdump_bytes(dump: str) -> bytes:
    """ffprobe's `-show_data` hexdump, back to the bytes: eight groups of
    four hex digits after the offset, the ASCII column after that."""
    out = bytearray()
    for line in dump.splitlines():
        if ":" not in line:
            continue
        digits = line.split(":", 1)[1][:41].replace(" ", "")
        out += bytes.fromhex(digits)
    return bytes(out)


def packets(path: Path, data: bool) -> list[dict]:
    entries = "packet=pts,pts_time,pos,size,flags" + (",data" if data else "")
    argv = ["ffprobe", "-v", "error", "-select_streams", "0",
            "-show_entries", entries, "-of", "json"]
    if data:
        argv.append("-show_data")
    done = run([*argv, str(path)], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe {path.name}:\n{done.stderr[-600:]}")
    found = json.loads(done.stdout).get("packets", [])
    for packet in found:
        packet["pts"] = int(packet["pts"])
        packet["pos"] = int(packet["pos"])
        packet["size"] = int(packet["size"])
        packet["pts_time"] = float(packet["pts_time"])
        if data:
            packet["bytes"] = hexdump_bytes(packet.get("data", ""))
    return found


def wait_for_publishing(publisher: Tail, reader: Tail, deadline: float = 120.0) -> None:
    """Until the publisher has published a group, which is past its hold
    for a first reader."""
    began = time.monotonic()
    while time.monotonic() - began < deadline:
        with publisher.lock:
            if any("group" in row for row in publisher.rows):
                return
        for child, name in ((publisher, "publisher"), (reader, "reader")):
            if child.child.poll() is not None:
                out, _ = child.text()
                sys.exit(f"the {name} stopped early (exit {child.child.returncode}):\n"
                         f"{child.module_stderr()[-2500:]}\n{out[-800:]}")
        time.sleep(0.2)
    sys.exit(f"nothing was published within {deadline:.0f}s")


def relay_args(args: argparse.Namespace) -> list[str]:
    """What the relay is told about the drafts it speaks: all of them, or
    the one `--relay-version` names."""
    return ["--server-version", args.relay_version] if args.relay_version else []


def media_clock(publisher: Tail) -> float:
    """How far into the media the publisher has got, off its audio groups."""
    with publisher.lock:
        rows = [row for row in publisher.rows
                if "group" in row and "audio" in row.get("track", "")]
    return max((float(row.get("pts_end", 0.0)) for row in rows), default=0.0)


def wait_until(check, deadline: float, what: str, *children: Tail) -> float:
    began = time.monotonic()
    while time.monotonic() - began < deadline:
        if check():
            return time.monotonic() - began
        for child in children:
            if child.child.poll() is not None:
                out, _ = child.text()
                sys.exit(f"{what}: a child stopped early (exit {child.child.returncode}):\n"
                         f"{child.module_stderr()[-3000:]}\n{out[-1000:]}")
        time.sleep(0.2)
    sys.exit(f"{what}: not within {deadline:.0f}s")


def take_the_relay_away(args: argparse.Namespace, publisher: Tail, reader: Tail,
                        relays: list[subprocess.Popen], port: int, work: Path) -> dict:
    """Kills the relay after the warmup and starts it again on the same
    port, then waits for both halves to say they are back. The new relay
    joins `relays`, which the run stops at its end. Answers the media
    clock at the kill and when the reader was back."""
    wait_until(lambda: media_clock(publisher) >= args.warmup, args.seconds + 60,
               "warming up", publisher, reader)
    killed_at = media_clock(publisher)
    print(f"killing the relay at media {killed_at:.1f}s", flush=True)
    kill_tree(relays[-1])
    time.sleep(args.down)
    relays.append(start_relay(port, work / "cert.pem", work / "key.pem", work / "relay2.log",
                              relay_args(args)))
    print(f"moq-relay back, pid {relays[-1].pid}, after {args.down:.1f}s", flush=True)

    def publisher_back() -> bool:
        with publisher.lock:
            return any(row.get("event") == "reconnect" for row in publisher.rows)

    def reader_back() -> bool:
        return any(row.get("kind") == "reconnect" for row in reader.counters())

    took = wait_until(publisher_back, MEND_DEADLINE, "the publisher reconnecting",
                      publisher, reader)
    print(f"publisher reconnected {took:.1f}s after the relay came back", flush=True)
    wait_until(reader_back, MEND_DEADLINE, "the reader reconnecting", publisher, reader)
    back_at = media_clock(publisher)
    print(f"reader reconnected, at media {back_at:.1f}s", flush=True)
    rows = [row for row in reader.counters() if row.get("kind") == "reconnect"]
    for row in rows:
        print("reader row:", json.dumps(row))
    return {"killed_at": killed_at, "back_at": back_at, "reader_row": rows}


def one_run(args: argparse.Namespace) -> None:
    work = Path(tempfile.mkdtemp(prefix="moq-data-", dir=args.work))
    relays: list[subprocess.Popen] = []
    publisher = reader = None
    query = PACKAGE / ".live-data-publish.sql"
    read_query = PACKAGE / ".live-data-subscribe.sql"
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
        relays.append(start_relay(port, work / "cert.pem", work / "key.pem",
                                  work / "relay.log", relay_args(args)))
        print(f"moq-relay pid {relays[0].pid} on 127.0.0.1:{port}", flush=True)

        publisher = start_publisher(query, source, port, cert_hex, args.fixture)
        started = time.monotonic()
        # The reader goes up beside the publisher, as a node of a tree
        # does: it waits for the broadcast, and it has to be subscribed
        # before the first message goes out, since a live join starts at
        # the newest group.
        video = TimedPipe("video", work / "video.nut")
        data = TimedPipe("data", work / "data.nut")
        reader = start_reader(port, cert_hex, [
            (TRACKS["video"], video.spelling),
            (TRACKS["audio"], str(work / "audio.nut")),
            (TRACKS["data"], data.spelling),
        ])

        # While the broadcast runs: the catalog as a query sees it, and a
        # query naming the data column compiled against it.
        wait_for_publishing(publisher, reader)
        catalog = probe(port, cert_hex)
        tracks = catalog.get("tracks", [])
        print("catalog:", json.dumps([
            {key: track.get(key) for key in ("kind", "codec", "time_base", "row")}
            for track in tracks]), flush=True)
        kinds = [track.get("kind") for track in tracks]
        problems: list[str] = []
        if kinds != list(TRACKS):
            sys.exit(f"the catalog is not one rung, its audio and one data track: {kinds}")
        data_track = tracks[TRACKS["data"]]
        if data_track.get("codec") != "json":
            problems.append(f"the data track's codec is {data_track.get('codec')}")
        if data_track.get("time_base") != [1, 1_000_000]:
            problems.append(f"the data track's time base is {data_track.get('time_base')}")
        if data_track.get("row") != tracks[TRACKS["video"]].get("row"):
            problems.append("the data track is not on the video's row")
        listing = compile_subscriber(port, cert_hex, work / "readback.nut", read_query)
        print("--- a query reading s.data[1] compiles ---")
        print("\n".join(line for line in listing.splitlines() if "-track" in line)[:1500])

        gap = None
        if args.reconnect:
            gap = take_the_relay_away(args, publisher, reader, relays, port, work)

        published = publisher.finish(args.seconds + 180)
        read = reader.finish(120)
        video.finish(30)
        data.finish(30)
        out, err = publisher.text()
        print("--- publisher stderr (tail) ---")
        print(err[-1500:])
        print("--- reader stderr (tail) ---")
        print(reader.module_stderr()[-2500:])
        (work / "reader.err").write_text(reader.module_stderr(), encoding="utf-8")
        (work / "publisher.err").write_text(err, encoding="utf-8")
        if published != 0:
            problems.append(f"the publisher exited {published}")
        notes: list[str] = []
        if read != 0:
            # Over an IETF draft the end of the broadcast does not reach the
            # reader as the end of its tracks: moq-net 0.2.15 fails every
            # track's stream (`short buffer`, then `dropped`), media and data
            # alike, and the reader goes looking for a new session until
            # `reconnect_s` runs out. That is the transport's, not a data
            # track's, and this mode is here for the pts.
            (notes if args.relay_version else problems).append(f"the reader exited {read}")

        # What the publisher published on the data track, in order.
        with publisher.lock:
            rows = list(publisher.rows)
        sent = [row for row in rows if row.get("track") == "data" and "group" in row]
        expected = expected_messages(args.fixture)
        got = packets(work / "data.nut", True)
        print(f"data: {len(expected)} messages in the fixture, {len(sent)} published, "
              f"{len(got)} received", flush=True)
        if len(sent) != len(expected):
            problems.append(f"{len(sent)} of {len(expected)} messages were published")
        # Each message received is matched to the fixture by its bytes, in
        # order: what is skipped over is what was lost. The pipeline may
        # rebase a file's clock, the same for every message; what the
        # reader owes is the pts the publisher had.
        shift = None
        cursor = 0
        missing: list[int] = []
        for position, packet in enumerate(got):
            match = next((index for index in range(cursor, len(expected))
                          if expected[index][1] == packet["bytes"]), None)
            if match is None:
                problems.append(f"received message {position} ({packet['bytes']!r}) is no "
                                "message of the fixture's, or arrived out of order")
                continue
            missing += range(cursor, match)
            cursor = match + 1
            pts = expected[match][0]
            if shift is None:
                shift = packet["pts"] - pts
            elif packet["pts"] - pts != shift:
                problems.append(f"message {match}: pts {packet['pts']} is not the "
                                f"fixture's {pts} shifted by {shift}")
            if match < len(sent) and abs(sent[match]["pts_start"] - packet["pts"] / 1e6) > 1e-6:
                problems.append(f"message {match}: published at "
                                f"{sent[match]['pts_start']:.6f}s, received at "
                                f"{packet['pts'] / 1e6:.6f}s")
            if "K" not in packet["flags"]:
                problems.append(f"message {match} is not a keyframe")
        missing += range(cursor, len(expected))
        print(f"data: the fixture's pts arrive shifted by {shift} us", flush=True)
        if gap is None:
            if missing:
                problems.append(f"messages {missing} did not arrive")
        else:
            # Lost: only what went out while the reader had no session,
            # from shortly before the relay went (in flight) to shortly
            # after the reader was back (the join lands on a whole group).
            lost_from, lost_to = gap["killed_at"] - 1.0, gap["back_at"] + 3.0
            print(f"data: lost {[expected[i][0] / 1e6 for i in missing]} with the relay "
                  f"away from media {gap['killed_at']:.1f}s and the reader back at "
                  f"{gap['back_at']:.1f}s", flush=True)
            for index in missing:
                if not lost_from <= expected[index][0] / 1e6 <= lost_to:
                    problems.append(f"message {index} at {expected[index][0] / 1e6:.3f}s was "
                                    "lost outside the gap")
            after = [packet for packet in got if packet["pts_time"] > gap["back_at"]]
            if len(after) < 3:
                problems.append(f"{len(after)} messages arrived after the reader was back")
            if not gap["reader_row"]:
                problems.append("the reader wrote no reconnect row")

        # The reader's own word on the track: nothing lost or skipped. Its
        # last row per track, which is the final one on a clean end.
        final = {row.get("track"): row for row in reader.counters()
                 if row.get("kind") == "track"}
        for name, row in final.items():
            print(f"  {name}: received {row.get('received')} delivered {row.get('delivered')} "
                  f"holes {row.get('holes_opened')} late {row.get('dropped_late')} "
                  f"skipped {row.get('skipped_join')} held at most "
                  f"{row.get('hold_max_groups')} reordered by {row.get('reorder_max')}",
                  flush=True)
        data_row = final.get("data")
        if data_row is None:
            problems.append("the reader said nothing about the data track")
        else:
            # A reconnect gives up the hole between the two sessions, and
            # says so; nothing else may be lost, late or skipped.
            keys = ("skipped_join", "holes_abandoned_gone", "holes_abandoned_budget",
                    "holes_abandoned_end")
            if gap is None:
                keys += ("holes_opened",)
            for key in keys:
                if data_row.get(key):
                    problems.append(f"data track: {key} {data_row.get(key)}")
            # A group that came too late to use. Over an IETF draft the end
            # of the broadcast fails the tracks' streams and the reader takes
            # each up again at the live edge, which hands the LAST group over
            # a second time; that one is expected there, and nothing else.
            late = [row for row in reader.counters()
                    if row.get("kind") == "late" and row.get("track") == "data"]
            for row in late:
                if not (args.relay_version and row.get("group") == data_row.get("last")):
                    problems.append(f"data track: group {row.get('group')} arrived late, "
                                    f"the cursor at {row.get('cursor')}")

        # When each message came out against the picture of the same pts,
        # and beside that when the publisher said it had closed each: its
        # data group, and the video group holding that picture.
        pictures = packets(work / "video.nut", False)
        with publisher.lock:
            stamped = list(publisher.stamped)
        closed_data = [at for at, row in stamped
                       if row.get("track") == "data" and "group" in row]
        closed_video = [(at, row) for at, row in stamped
                        if "group" in row and row.get("track") not in ("data",)
                        and "audio" not in row.get("track", "")]
        print("--- arrival at the reader: message against the first picture at its pts ---")
        leads = []
        for position, packet in enumerate(got):
            at = data.arrival(packet["pos"] + packet["size"])
            beside = next((p for p in pictures if p["pts_time"] >= packet["pts_time"]), None)
            group = next((stamp for stamp, row in closed_video
                          if row["pts_end"] >= packet["pts_time"] - 1e-6), None)
            if position < len(closed_data) and group is not None:
                print(f"  message {position} published {closed_data[position] - started:7.3f}s, "
                      f"its picture's group {group - started:7.3f}s")
            if at is None:
                problems.append(f"message {position}: no arrival logged")
                continue
            if beside is None:
                print(f"  message {position} at {packet['pts_time']:.3f}s: no picture at or "
                      "past it")
                continue
            then = video.arrival(beside["pos"] + beside["size"])
            if then is None:
                problems.append(f"message {position}: the picture beside it has no arrival")
                continue
            lead = then - at
            leads.append(lead)
            print(f"  message {position} pts {packet['pts_time']:9.6f}s  picture pts "
                  f"{beside['pts_time']:9.6f}s  message out {at - started:7.3f}s  picture "
                  f"out {then - started:7.3f}s  lead {lead * 1000:7.1f} ms", flush=True)
            if lead < 0:
                problems.append(f"message {position} at {packet['pts_time']:.6f}s came out "
                                f"{-lead * 1000:.1f} ms after the picture at "
                                f"{beside['pts_time']:.6f}s")
        if leads:
            print(f"lead over the picture: min {min(leads) * 1000:.1f} ms, "
                  f"max {max(leads) * 1000:.1f} ms, over {len(leads)} messages")
        else:
            problems.append("no message had a picture to be timed against")

        for note in notes:
            print(f"NOTE: {note}")
        if problems:
            print("=== FAIL ===")
            for problem in problems:
                print(" -", problem)
            sys.exit(1)
        print("=== PASS: every message arrived whole, at its pts, ahead of its picture ===")
    finally:
        for child in (reader, publisher):
            if child is not None:
                kill_tree(child.child)
        for relay in relays:
            kill_tree(relay)
        query.unlink(missing_ok=True)
        read_query.unlink(missing_ok=True)
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print(f"work kept at {work}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=SECONDS)
    parser.add_argument("--reconnect", action="store_true",
                        help="take the relay away mid-run and bring it back")
    parser.add_argument("--relay-version", default=None,
                        help="the one MoQ draft the relay speaks, e.g. moq-transport-16, "
                        "where no frame timestamp crosses and a message's pts has to "
                        "come out of its frame")
    parser.add_argument("--fixture", default="messages", choices=("messages", "pairs"),
                        help="the messages published: tests/data/<fixture>.nut, whose "
                        "source is <fixture>.txt")
    parser.add_argument("--warmup", type=float, default=WARMUP)
    parser.add_argument("--down", type=float, default=DOWN)
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
