"""The live-edge oracle: a reader joins where the broadcast is, not where it was.

    python tests/live_edge.py [--warmup N] [--seconds N] [--work DIR]
                              [--report-only] [--no-build] [--keep]

Every subscribe before 0.6.5 asked for the whole backlog, so a reader
attaching to a broadcast that had been running for a minute started a
retention window behind live and read the whole cache before it forwarded
anything. `start => 'live'` is the default now, and what it promises is
one GROUP of delay: the publisher serves from its newest group, and the
reader starts at the first group a decoder can begin at - the first group
whose first frame is a keyframe for video, the first whole group for
audio.

The shape of a run: one publisher writes a two rung ladder and its audio
to a local relay, PACED (`realtime => true`), so the relay holds a real
live broadcast rather than a file poured into it. One reader - `watch` -
is up from the first second and stays for the whole run, because a relay
caches a track only while something is subscribed to it: nobody watching
means no cache, and then there is no backlog for anything to join at the
wrong end of. After `--warmup` seconds of media, four more readers
attach:

  edge     the subscribe module under the sidecar, ONE session pulling
           every catalog track at once, each to its own .nut. This is
           where the join is measured group for group, because the
           module says on its own stderr when it is about to subscribe
           and the publisher's newest group can be read at that instant.
           It is also the only reader that can say whether the tracks of
           one session join together.
  live     `ffrwd run` over the real query path, a rung and the audio
           stream-copied into one .mkv, `start => 'live'` spelled out.
  default  the same query with no start argument at all, which is what
           proves the default.
  back     the same query asking for `start => 'backlog'`, which is what
           every reader used to do.

The query path compiles before it runs, so the wall clock between
spawning one of those and its subscription being accepted is seconds. A
join is therefore judged in MEDIA time instead: where the group a reader
started at sits against the media clock the publisher had reached when
that reader was spawned. A live join starts at or after it, a backlog
join tens of seconds before it.

From the join onward every reader owes the same thing whichever end it
joined at: every group delivered in sequence, nothing abandoned, nothing
arriving below the cursor. While they run, the machine is loaded for a
few seconds on purpose - a reader that lags must catch up, not skip.

Never collected by any suite: it needs moq-relay, ffmpeg, ffprobe, cargo
with the wasm32-wasip2 target and a wasi-sdk clang, and the sidecar for
the `edge` reader. It starts a relay and readers of its own and stops
those alone. The process machinery it shares with the loops beside it is
in common.py.
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

SECONDS = 110
WARMUP = 60.0
RATE = 30
GOP = 30
WIDTHS = (320, 160)
BITRATES = ("400k", "150k")
HEIGHT = 180
SAMPLE_RATE = 48000
CHANNELS = 2
AUDIO_GROUP_MS = 200
BROADCAST = "live/edge"

BUILD_DEADLINE = 900
TOOL_DEADLINE = 300
RUN_DEADLINE = 600
ATTACH_DEADLINE = 120

# How far from the publisher's newest group a live join may land, in
# groups. One is the promise; the measurement is looser than the promise
# at both ends. A group row goes out when the group CLOSES, so the newest
# row is up to one group behind the group the relay would actually serve,
# which puts a perfectly punctual join one group AHEAD of the row. Add
# the drain of the publisher's own row pipe and whatever the publisher
# closes between the module saying it is about to subscribe and the relay
# accepting, and three is the bound worth failing on.
LIVE_SLACK = 3

# How far apart in MEDIA seconds the tracks of one session may join. A
# track can only join at one of its own group boundaries, and those are
# a whole GOP apart for video against a fifth of a second for audio, so
# one group of slack is a second here.
COHERENT_SPREAD = 1.5

# In media seconds, how far a live join may sit before the clock the
# publisher had reached when the reader was spawned, and how far behind
# it a backlog join must sit to have read the cache at all. A live join
# lands at or after that clock; the slack below it is the drain of the
# publisher's own row pipe. The relay holds what this package's retention
# window asks for, which is 30 seconds.
LIVE_BEHIND_MAX = 2.0
BACKLOG_BEHIND_MIN = 15.0

# The deliberate load: a few busy processes for a few seconds, once the
# readers have settled. The point is not to wedge the machine, it is to
# make the host calls late and prove that late is not lost.
LOAD_AFTER = 8.0
LOAD_SECONDS = 4.0
LOAD_WORKERS = 2

# The publisher: a paced ladder and the file's audio, one row per
# rendition, a row per published group on stdout so a reader's join can
# be read back against what was live at the time.
PUBLISH_QUERY = """
COPY (
  WITH vid AS (
    SELECT scale(f.video[1], :widths[i.i], -2) AS v, i.i AS rung
    FROM input(:'source', realtime => true) f, generate_series(1, :rungs) i
  ),
  aud AS (
    SELECT a AS t, :rungs + a.index AS rung
    FROM input(:'source', realtime => true) g, unnest(g.audio) a
  )
  SELECT vid.v, aud.t
  FROM vid FULL JOIN aud ON vid.rung = aud.rung
) TO ffrwd.moq.publish(:'relay', :'broadcast', COALESCE(:'cert', ''), '',
                       :audio_group_ms, 'groups')
  WITH (video_bitrate :'bitrates'[vid.rung], gop 30, preset 'veryfast',
        tune 'zerolatency', audio_bitrate '128k')
"""

# The reader over the real query path: a rung picked by height and the
# audio row beside it, stream-copied into one file.
SUBSCRIBE_QUERY = """
COPY (
  SELECT v.video[1], a.audio[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''), '',
                           :'start') v,
       ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''), '',
                           :'start') a
  WHERE v.height = :height AND a.height IS NULL
) TO :'dest'
"""

# The same with no start argument at all, which must read as 'live'.
DEFAULT_QUERY = """
COPY (
  SELECT v.video[1], a.audio[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''), '') v,
       ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''), '') a
  WHERE v.height = :height AND a.height IS NULL
) TO :'dest'
"""

# What the module prints as it opens, which is the last thing it does
# before it subscribes.
ATTACHING = "subscribe: pulling"


def make_source(path: Path, seconds: int) -> None:
    done = run([
        "ffmpeg", "-v", "error", "-y",
        "-f", "lavfi", "-i", f"testsrc2=size=640x360:rate={RATE}",
        "-f", "lavfi", "-i", f"sine=frequency=440:sample_rate={SAMPLE_RATE}",
        "-ac", str(CHANNELS), "-c:a", "aac",
        "-t", str(seconds), "-c:v", "libx264", "-preset", "ultrafast",
        "-g", str(GOP), "-pix_fmt", "yuv420p", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"building the source failed:\n{done.stderr[-800:]}")


def start_publisher(query: Path, source: Path, port: int, cert_hex: str,
                    rungs: int) -> Tail:
    argv = ffrwd_argv("run", "-f", str(query),
                      "-v", f"source={source}",
                      "-v", f"relay=moqt://127.0.0.1:{port}",
                      "-v", f"broadcast={BROADCAST}",
                      "-v", f"cert={cert_hex}",
                      "-v", f"rungs={rungs}",
                      "-v", "widths=" + ",".join(str(w) for w in WIDTHS[:rungs]),
                      "-v", "bitrates=" + ",".join(BITRATES[:rungs]),
                      "-v", f"audio_group_ms={AUDIO_GROUP_MS}", "-q")
    print("+ publisher:", " ".join(argv[:6]), "...", flush=True)
    return Tail(spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                      text=True, env=dict(os.environ), cwd=PACKAGE))


def start_reader(query: Path, port: int, cert_hex: str, dest: Path, name: str,
                 start: str | None) -> Tail:
    """One `ffrwd run` reader against the broadcast, its rows kept."""
    env = dict(os.environ)
    dump = dest.parent / f"{name}-stderr"
    env["FFRWD_DUMP_STDERR"] = str(dump)
    argv = ffrwd_argv("run", "-f", str(query),
                      "-v", f"relay=moqt://127.0.0.1:{port}",
                      "-v", f"broadcast={BROADCAST}",
                      "-v", f"cert={cert_hex}",
                      "-v", f"height={HEIGHT}",
                      "-v", f"dest={dest}",
                      *(("-v", f"start={start}") if start is not None else ()),
                      "-q")
    print(f"+ reader {name}:", " ".join(argv[:6]), "...", flush=True)
    return Tail(spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                      text=True, env=env, cwd=PACKAGE), dump)


def start_session_reader(module: Path, port: int, cert_hex: str,
                         outputs: list[Path], label: str) -> Tail:
    """The subscribe module under the sidecar: ONE session, every track.

    The query path opens a session per subscribe() the query names, so it
    cannot say whether several tracks of one session join together. This
    can: one module instance, one output per catalog track, one set of
    counters naming each track by its catalog name, and its stderr on a
    pipe rather than through the CLI, so the moment it subscribes is
    visible from here.
    """
    params = json.dumps({
        "relay": f"moqt://127.0.0.1:{port}",
        "broadcast": BROADCAST,
        "cert": cert_hex,
        "token": "",
        "start": "live",
    })
    argv = [str(SIDECAR), "-udp", str(module), "-http", str(module),
            "-m", str(module), "-params", params]
    for track, path in enumerate(outputs):
        argv += ["-track", str(track), "-f", "nut", str(path)]
    print(f"+ reader {label}:", " ".join(argv[:6]), "...", flush=True)
    return Tail(spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                      text=True))


def newest_groups(publisher: Tail) -> dict[str, int]:
    """The highest group sequence closed on each track so far."""
    with publisher.lock:
        rows = list(publisher.rows)
    newest: dict[str, int] = {}
    for row in rows:
        if "group" in row and "track" in row:
            newest[row["track"]] = max(newest.get(row["track"], -1), int(row["group"]))
    return newest


def media_clock(publisher: Tail) -> float:
    """How far into the media the publisher has published, in seconds."""
    rows = publisher.group_rows("audio")
    return max((float(row.get("pts_end", 0.0)) for row in rows), default=0.0)


def published(publisher: Tail) -> dict[tuple[str, int], dict]:
    """Every group row the publisher wrote, by track and sequence."""
    with publisher.lock:
        rows = list(publisher.rows)
    return {
        (row["track"], int(row["group"])): row
        for row in rows if "group" in row and "track" in row
    }


def wait_for_media(publisher: Tail, seconds: float, deadline: float) -> float:
    """Blocks until the publisher has published `seconds` of media."""
    started = time.monotonic()
    while time.monotonic() - started < deadline:
        if media_clock(publisher) >= seconds:
            return time.monotonic() - started
        if publisher.child.poll() is not None:
            out, err = publisher.text()
            print(out[-2000:])
            sys.exit(f"the publisher stopped early:\n{err[-2000:]}")
        time.sleep(0.2)
    sys.exit(f"no {seconds}s of media published within {deadline}s")


def wait_for_attach(reader: Tail, deadline: float, name: str) -> None:
    """Blocks until the module says it is about to subscribe."""
    until = time.monotonic() + deadline
    while time.monotonic() < until:
        if ATTACHING in reader.text()[1]:
            return
        if reader.child.poll() is not None:
            out, err = reader.text()
            print(out[-1000:])
            sys.exit(f"the {name} reader stopped before it subscribed:\n{err[-2000:]}")
        time.sleep(0.02)
    sys.exit(f"the {name} reader did not subscribe within {deadline}s")


def load_burst(after: float, seconds: float, workers: int) -> None:
    """A few busy processes, a while from now, in a thread of this one.

    Not a stall the module can be told to take - there is no such hook,
    and a shipped module should not grow one for a test - but what it
    stands in for is the real condition: the QUIC session runs only while
    a host call is on the executor, so a machine with nothing to spare
    makes those calls late. Late must not mean lost.
    """
    if seconds <= 0 or workers <= 0:
        return

    def body() -> None:
        time.sleep(after)
        print(f"+ load: {workers} busy processes for {seconds:.0f}s", flush=True)
        spin = ("import time\n"
                f"end = time.monotonic() + {seconds}\n"
                "while time.monotonic() < end: pow(7, 20000, 10**9 + 7)\n")
        busy = [spawn([sys.executable, "-c", spin]) for _ in range(workers)]
        for child in busy:
            try:
                child.wait(timeout=seconds + 30)
            except subprocess.TimeoutExpired:
                kill_tree(child)
        print("+ load: done", flush=True)

    threading.Thread(target=body, daemon=True).start()


def first_video_packet(path: Path) -> tuple[bool, str]:
    done = run([
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "packet=pts_time,flags", "-of", "csv=p=0",
        "-read_intervals", "%+#4", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe rejected {path}:\n{done.stderr[-800:]}")
    lines = [line for line in done.stdout.splitlines() if line.strip()]
    if not lines:
        return False, "no video packets at all"
    return "K" in lines[0].split(",")[-1], lines[0]


def packet_count(path: Path, stream: str) -> int:
    done = run([
        "ffprobe", "-v", "error", "-select_streams", stream,
        "-show_entries", "packet=pts", "-of", "csv=p=0", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe rejected {path}:\n{done.stderr[-800:]}")
    return len([line for line in done.stdout.splitlines() if line.strip()])


def summaries(reader: Tail) -> dict[str, dict]:
    """Each track's last counter row, by track name."""
    out: dict[str, dict] = {}
    for row in reader.counters():
        if row.get("kind") == "track":
            out[row["track"]] = row
    return out


def holes(reader: Tail, track: str) -> list[dict]:
    """The rows naming every hole one track gave up on."""
    return [row for row in reader.counters()
            if row.get("kind") == "hole" and row.get("track") == track]


def in_sequence(name: str, track: str, row: dict, holes: list[dict],
                whole: bool) -> list[str]:
    """What a reader owes for the groups it was handed.

    Every group received has to be accounted for, and none may arrive
    below the cursor. A reader at the live edge owes more than that: the
    relay is serving it a group at a time as the publisher writes them,
    so the sequence from the join onward has to be unbroken.

    `whole` is False for a reader replaying a backlog off a LIVE relay,
    where the cache is a retention window and the oldest of it can age
    out from under the replay. Losing a group there is the relay's
    answer, not a defect - but it still has to be named in a row of its
    own rather than dropped in silence. `tests/live_backlog.py` is where
    a relay holding the whole backlog is measured.
    """
    delivered, first, last = int(row["delivered"]), row["first"], row["last"]
    if first is None or last is None:
        return [f"{name}/{track}: nothing was delivered"]
    failures = []
    abandoned = sum(int(row[key]) for key in (
        "holes_abandoned_gone", "holes_abandoned_budget",
        "holes_abandoned_restart", "holes_abandoned_end",
    ))
    if row["dropped_late"]:
        failures.append(
            f"{name}/{track}: {row['dropped_late']} groups arrived below the cursor"
        )
    accounted = (delivered + int(row["dropped_late"]) + int(row["repeated"])
                 + int(row.get("skipped_join", 0)))
    if accounted != int(row["received"]):
        failures.append(
            f"{name}/{track}: {row['received']} received and {accounted} accounted for"
        )
    if whole:
        if abandoned:
            failures.append(f"{name}/{track}: {abandoned} holes given up on")
        if delivered != int(last) - int(first) + 1:
            failures.append(
                f"{name}/{track}: {delivered} groups delivered over {first}..{last}, "
                "which is not all of it"
            )
    elif abandoned:
        reasons = ", ".join(sorted({hole["reason"] for hole in holes}))
        print(f"{name}/{track}: {abandoned} holes given up on ({reasons}), "
              f"{len(holes)} named, over {first}..{last}")
        if len(holes) != abandoned:
            failures.append(
                f"{name}/{track}: {abandoned} holes given up on and {len(holes)} named"
            )
    return failures


def judge(name: str, reader: Tail, rows: dict[tuple[str, int], dict],
          spawned_at: float, newest: dict[str, int] | None,
          live: bool) -> list[str]:
    """One reader's join, and what it did from there.

    `spawned_at` is the publisher's media clock when this reader was
    started; `newest` is the publisher's newest group per track at the
    instant it subscribed, for the one reader whose subscribing is
    visible from here.
    """
    tracks = summaries(reader)
    if not tracks:
        return [f"{name}: the module reported no counters"]
    failures = []
    joined_at: dict[str, float] = {}
    for track, row in sorted(tracks.items()):
        if row["first"] is None:
            failures.append(f"{name}/{track}: nothing was delivered")
            continue
        first = int(row["first"])
        published_row = rows.get((track, first))
        if published_row is None:
            failures.append(
                f"{name}/{track}: group {first} is not one the publisher wrote"
            )
            continue
        at = float(published_row["pts_start"])
        joined_at[track] = at
        lag = spawned_at - at
        note = ""
        if newest is not None and track in newest:
            distance = newest[track] - first
            note = (f", the publisher's newest was {newest[track]} "
                    f"({distance:+d} groups)")
            if live and abs(distance) > LIVE_SLACK:
                failures.append(
                    f"{name}/{track}: joined {distance} groups from live, and a "
                    f"live join is within {LIVE_SLACK}"
                )
        print(f"{name}/{track}: joined at group {first} (media {at:.2f}s, "
              f"{lag:+.2f}s behind the clock at spawn){note}; "
              f"{row['delivered']} groups delivered, "
              f"{row.get('skipped_join', 0)} stepped over at the join")
        failures += in_sequence(name, track, row, holes(reader, track), live)
        if live and lag > LIVE_BEHIND_MAX:
            failures.append(
                f"{name}/{track}: a live join started {lag:.2f}s of media behind "
                f"the clock at spawn, and a live join is within {LIVE_BEHIND_MAX}s"
            )
        if not live and lag < BACKLOG_BEHIND_MIN:
            failures.append(
                f"{name}/{track}: a backlog join started only {lag:.2f}s of media "
                f"back, and the relay held more than {BACKLOG_BEHIND_MIN}s"
            )
    # Every track of ONE session joins the same edge: a ladder whose rungs
    # began seconds apart would be a ladder nothing can cut between.
    if newest is not None and len(joined_at) > 1:
        spread = max(joined_at.values()) - min(joined_at.values())
        print(f"{name}: {len(joined_at)} tracks joined within {spread:.2f}s of "
              "media of each other")
        if live and spread > COHERENT_SPREAD:
            failures.append(
                f"{name}: the tracks of one session joined {spread:.2f}s apart, "
                f"and one session joins them within {COHERENT_SPREAD}s"
            )
    return failures


def one_run(args: argparse.Namespace) -> None:
    work = Path(tempfile.mkdtemp(prefix="moq-edge-", dir=args.work))
    relay = publisher = None
    readers: list[Tail] = []
    queries = [PACKAGE / f".live-edge-{name}.sql"
               for name in ("publish", "subscribe", "default")]
    publish_query, subscribe_query, default_query = queries
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
        publish_query.write_text(PUBLISH_QUERY)
        subscribe_query.write_text(SUBSCRIBE_QUERY)
        default_query.write_text(DEFAULT_QUERY)

        port = free_udp_port()
        # The relay is left alone: it keeps what each track's own retention
        # window asks for, which is this package's 30 seconds. That is the
        # point of the loop - a paced publisher a minute in has a cache, and
        # a backlog reader takes all of it where a live reader takes none.
        relay = start_relay(port, work / "cert.pem", work / "key.pem",
                            work / "relay.log")
        print(f"moq-relay pid {relay.pid} on 127.0.0.1:{port}", flush=True)

        started = time.monotonic()
        rungs = len(WIDTHS)
        publisher = start_publisher(publish_query, source, port, cert_hex, rungs)
        print(f"publisher pid {publisher.child.pid}", flush=True)

        # Somebody watching from the first second. A relay caches a track
        # only while it is subscribed to one - and this package's publisher
        # holds its first media until a subscriber arrives - so without
        # this the whole run would sit at the live edge and there would be
        # no backlog for `back` to join at the wrong end of. It reads every
        # track, which is what makes every track cached.
        watching = [work / f"watch{index}.nut" for index in range(rungs + 1)]
        watch = start_session_reader(guest("subscribe"), port, cert_hex,
                                     watching, "watch")
        readers.append(watch)
        wait_for_attach(watch, ATTACH_DEADLINE, "watch")
        print(f"watch subscribed at {time.monotonic() - started:.1f}s", flush=True)

        # A while into a paced broadcast, which is when a node joining a
        # live stream actually joins one.
        wait_for_media(publisher, args.warmup, args.seconds + 60)
        print(f"attaching at {time.monotonic() - started:.1f}s, media clock "
              f"{media_clock(publisher):.1f}s", flush=True)

        spawned: dict[str, float] = {}
        nuts = [work / f"edge{index}.nut" for index in range(rungs + 1)]
        spawned["edge"] = media_clock(publisher)
        edge = start_session_reader(guest("subscribe"), port, cert_hex, nuts,
                                    "edge")
        readers.append(edge)
        # The one reader whose subscribing is visible from here: what the
        # publisher had just closed at that instant is what its join is
        # measured against, group for group.
        wait_for_attach(edge, ATTACH_DEADLINE, "edge")
        newest = newest_groups(publisher)
        print(f"edge subscribed at {time.monotonic() - started:.1f}s, the "
              f"publisher's newest groups: {json.dumps(newest, sort_keys=True)}",
              flush=True)

        others = (("live", subscribe_query, "live"),
                  ("default", default_query, None),
                  ("back", subscribe_query, "backlog"))
        running: dict[str, Tail] = {"watch": watch, "edge": edge}
        for name, query, start in others:
            spawned[name] = media_clock(publisher)
            reader = start_reader(query, port, cert_hex, work / f"{name}.mkv",
                                  name, start)
            readers.append(reader)
            running[name] = reader
            print(f"{name} reader pid {reader.child.pid}", flush=True)

        load_burst(args.load_after, args.load_seconds, LOAD_WORKERS)

        code = publisher.finish(RUN_DEADLINE)
        if code != 0:
            out, err = publisher.text()
            print(out[-2000:])
            sys.exit(f"the publisher failed ({code}):\n{err[-2000:]}")
        rows = published(publisher)
        print(f"the publisher ended at {time.monotonic() - started:.1f}s with "
              f"{len(rows)} groups", flush=True)

        for name, reader in running.items():
            code = reader.finish(RUN_DEADLINE)
            if code != 0:
                out, _ = reader.text()
                print(out[-1500:])
                print(reader.module_stderr()[-3000:])
                sys.exit(f"the {name} reader failed ({code})")
        readers = []

        print()
        failures = []
        # The watcher joined before the broadcast had a backlog to join,
        # so there is no distance to measure - but it read the whole run,
        # so what it owes is the sequence.
        for track, row in sorted(summaries(watch).items()):
            failures += in_sequence("watch", track, row, holes(watch, track), True)
        failures += judge("edge", edge, rows, spawned["edge"], newest, live=True)
        for name in ("live", "default", "back"):
            failures += judge(name, running[name], rows, spawned[name], None,
                              live=name != "back")

        # And what a live join is FOR: the first picture out of it has to
        # be one a decoder can start at.
        for name in ("live", "default"):
            dest = work / f"{name}.mkv"
            if not dest.exists():
                failures.append(f"{name}: the reader wrote nothing")
                continue
            keyframe, packet = first_video_packet(dest)
            video, audio = packet_count(dest, "v:0"), packet_count(dest, "a:0")
            print(f"{name}: first video packet {packet}, {video} video and "
                  f"{audio} audio packets")
            if not keyframe:
                failures.append(
                    f"{name}: the first video packet out is not a keyframe ({packet})"
                )
            if video < RATE * args.watch_at_least:
                failures.append(
                    f"{name}: {video} video packets is under the {args.watch_at_least}s "
                    "it watched for"
                )
        for index, nut in enumerate(nuts):
            if not nut.exists():
                failures.append(f"edge: no output for catalog track {index}")

        for failure in failures:
            print(f"FAIL {failure}")
        if failures and not args.report_only:
            sys.exit("the live edge was not where a reader joined")
        print(f"PASS: every reader joined where it asked to "
              f"({time.monotonic() - started:.1f}s)")
    finally:
        for reader in readers:
            kill_tree(reader.child)
        if publisher is not None:
            kill_tree(publisher.child)
        if relay is not None:
            kill_tree(relay)
        for query in queries:
            query.unlink(missing_ok=True)
        if args.keep:
            print(f"kept: {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=SECONDS,
                        help="seconds of media the publisher paces out")
    parser.add_argument("--warmup", type=float, default=WARMUP,
                        help="seconds of media in the relay before the readers attach")
    parser.add_argument("--watch-at-least", type=float, default=20.0,
                        help="seconds a live reader must come back with")
    parser.add_argument("--load-after", type=float, default=LOAD_AFTER,
                        help="seconds after the readers attach before the load burst")
    parser.add_argument("--load-seconds", type=float, default=LOAD_SECONDS,
                        help="how long the load burst lasts; 0 leaves it out")
    parser.add_argument("--work", default=None, help="where the run's files go")
    parser.add_argument("--no-build", dest="build", action="store_false",
                        help="use the guests already built")
    parser.add_argument("--report-only", action="store_true",
                        help="print the numbers without failing on them")
    parser.add_argument("--keep", action="store_true", help="keep the work directory")
    args = parser.parse_args()

    if not SIDECAR.exists():
        sys.exit(f"no sidecar at {SIDECAR}; build it first (cargo build --release)")
    started = time.monotonic()
    one_run(args)
    print(f"=== the live edge loop took {time.monotonic() - started:.1f}s ===")


if __name__ == "__main__":
    main()
