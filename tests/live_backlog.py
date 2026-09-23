"""The backlog oracle: every group the relay kept reaches the subscriber.

    python tests/live_backlog.py [--seconds N] [--keepalive N] [--work DIR]
                                 [--report-only] [--keep]

A subscriber that asks for the backlog - the `start` argument, since
0.6.5; before it every subscribe did - is served by the relay in the order
the groups arrived, newest one first. So the groups land wildly out of
sequence: two minutes of audio is six hundred of them, and they come back
in something close to reverse order. What this loop proves is that none of
them is thrown away.

The shape of a run: one publisher writes two renditions to a local relay.
The media rendition is the audio of a `--seconds` file read UNPACED, so
the whole of it is in the relay's cache within a few seconds. Beside it a
small video rendition is read in real time, which is what keeps the
session, and therefore the cached backlog, alive after the media is in.

Two subscribers read the media rendition back, each through `ffrwd run`
and the real query path, stream-copying to an .mkv:

  mid   attaches while the media is still being published
  after attaches once every media group is in the relay

Each is judged against what the publisher said it published: the group
sequences must be complete and in order, the packet count must agree, and
the module's own counters must say nothing was abandoned and nothing
arrived too late to be used. A module with no counters (0.6.3 and before)
is judged on the packets alone, which is the number the defect moved.

Never collected by any suite: it needs moq-relay, ffmpeg, ffprobe, cargo
with the wasm32-wasip2 target and a wasi-sdk clang. It starts a relay and
two readers of its own and stops those alone. The process machinery it
shares with the loops beside it is in common.py.
"""

from __future__ import annotations

import argparse
import bisect
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
    Tail,
    build_guests,
    ffrwd_argv,
    free_udp_port,
    kill_tree,
    run,
    spawn,
    start_relay,
)

SECONDS = 120
KEEPALIVE = 60
SAMPLE_RATE = 48000
CHANNELS = 2
AUDIO_GROUP_MS = 200
BROADCAST = "live/backlog"

BUILD_DEADLINE = 900
TOOL_DEADLINE = 300
RUN_DEADLINE = 600

# How long the media burst is waited for, and how long with no new group
# row says it is over.
BURST_DEADLINE = 180
BURST_QUIET = 4.0
# How much media has to be published before the mid-backlog reader joins.
MID_JOIN_AFTER = 10.0

# The publisher: the media as audio read as fast as it can be read, and a
# small picture beside it in real time. The two rungs never match, so the
# join hands the sink two rows - one carrying only video, one only audio -
# which is the demuxed pair `publish` reads as two renditions. The picture
# is what holds the session open after the media is in: a relay keeps a
# broadcast only while its publisher is connected.
PUBLISH_QUERY = """
COPY (
  WITH media AS (
    SELECT f.audio[1] AS a, 1 AS rung FROM input(:'source') f
  ),
  keep AS (
    SELECT g.video[1] AS v, 2 AS rung FROM input(:'keepalive', realtime => true) g
  )
  SELECT keep.v, media.a
  FROM keep FULL JOIN media ON keep.rung = media.rung
) TO ffrwd.moq.publish(:'relay', :'broadcast', COALESCE(:'cert', ''), '',
                       :audio_group_ms, 'groups')
  WITH (gop 30, preset 'veryfast', tune 'zerolatency', audio_bitrate '128k')
"""

# The subscriber: the rendition with no geometry is the audio one, which
# is the media. Stream-copied to the destination, packet for packet. The
# hold is spelled out so a run can starve it on purpose, and so is the
# BACKLOG: a subscribe joins at the live edge unless it is asked not to,
# and what this loop is about is the other end.
SUBSCRIBE_QUERY = """
COPY (
  SELECT s.audio[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''), '',
                           'backlog',
                           COALESCE(:hold_ms, 30000), COALESCE(:hold_mib, 64),
                           COALESCE(:join_ms, 2000)) s
  WHERE s.height IS NULL
) TO :'dest'
"""

# The same against a package whose subscribe takes neither a hold nor a
# start: what 0.6.4 and before are measured with, for a before and after
# over one loop. Those versions always asked for the backlog.
PLAIN_QUERY = """
COPY (
  SELECT s.audio[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''), '') s
  WHERE s.height IS NULL
) TO :'dest'
"""


def make_sources(source: Path, keepalive: Path, seconds: int, keep_seconds: int) -> None:
    done = run([
        "ffmpeg", "-v", "error", "-y",
        "-f", "lavfi", "-i", f"sine=frequency=440:sample_rate={SAMPLE_RATE}",
        "-ac", str(CHANNELS), "-c:a", "aac", "-t", str(seconds), str(source),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"building the media source failed:\n{done.stderr[-800:]}")
    done = run([
        "ffmpeg", "-v", "error", "-y",
        "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=10",
        "-t", str(keep_seconds), "-c:v", "libx264", "-preset", "ultrafast",
        "-pix_fmt", "yuv420p", str(keepalive),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"building the keepalive source failed:\n{done.stderr[-800:]}")


def start_publisher(query: Path, source: Path, keepalive: Path, port: int,
                    cert_hex: str, group_ms: int) -> Tail:
    env = dict(os.environ)
    argv = ffrwd_argv("run", "-f", str(query),
                      "-v", f"source={source}",
                      "-v", f"keepalive={keepalive}",
                      "-v", f"relay=moqt://127.0.0.1:{port}",
                      "-v", f"broadcast={BROADCAST}",
                      "-v", f"cert={cert_hex}",
                      "-v", f"audio_group_ms={group_ms}", "-q")
    print("+ publisher:", " ".join(argv[:6]), "...", flush=True)
    return Tail(spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                      text=True, env=env, cwd=PACKAGE))


def start_reader(query: Path, port: int, cert_hex: str, dest: Path, name: str,
                 hold: dict[str, int] | None = None) -> Tail:
    env = dict(os.environ)
    # The module's rows ride its own stderr, which the CLI keeps to
    # itself for a run that succeeds; this is where it writes it anyway.
    dump = dest.parent / f"{name}-stderr"
    env["FFRWD_DUMP_STDERR"] = str(dump)
    argv = ffrwd_argv("run", "-f", str(query),
                      "-v", f"relay=moqt://127.0.0.1:{port}",
                      "-v", f"broadcast={BROADCAST}",
                      "-v", f"cert={cert_hex}",
                      "-v", f"dest={dest}",
                      *[arg for name_, value in (hold or {}).items()
                        for arg in ("-v", f"{name_}={value}")], "-q")
    print(f"+ reader {name}:", " ".join(argv[:6]), "...", flush=True)
    return Tail(spawn(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                      text=True, env=env, cwd=PACKAGE), dump)


def wait_for_media(publisher: Tail, seconds: float, deadline: float) -> float:
    """Blocks until the publisher has published `seconds` of media."""
    started = time.monotonic()
    while time.monotonic() - started < deadline:
        rows = publisher.group_rows("audio")
        if rows and max(row.get("pts_end", 0.0) for row in rows) >= seconds:
            return time.monotonic() - started
        if publisher.child.poll() is not None:
            out, err = publisher.text()
            print(out[-2000:])
            sys.exit(f"the publisher stopped early:\n{err[-2000:]}")
        time.sleep(0.25)
    sys.exit(f"no {seconds}s of media published within {deadline}s")


def wait_for_burst(publisher: Tail, deadline: float, quiet: float) -> list[dict]:
    """Blocks until the unpaced media has stopped arriving, and says what it was."""
    started = time.monotonic()
    last_seen = time.monotonic()
    count = 0
    while time.monotonic() - started < deadline:
        rows = publisher.group_rows("audio")
        if len(rows) != count:
            count = len(rows)
            last_seen = time.monotonic()
        elif count > 0 and time.monotonic() - last_seen >= quiet:
            return rows
        if publisher.child.poll() is not None:
            out, err = publisher.text()
            print(out[-2000:])
            sys.exit(f"the publisher stopped early:\n{err[-2000:]}")
        time.sleep(0.25)
    sys.exit(f"the media was still arriving after {deadline}s")


def packet_times(path: Path) -> list[float]:
    """Every packet time on the file's first audio stream, in order."""
    done = run([
        "ffprobe", "-v", "error", "-select_streams", "a:0",
        "-show_entries", "packet=pts_time", "-of", "csv=p=0", str(path),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe rejected {path}:\n{done.stderr[-800:]}")
    return [float(line) for line in done.stdout.splitlines() if line.strip()]


def groups_missing(published: dict[int, dict], times: list[float]) -> list[int]:
    """Which published groups left no packet at all in the copy.

    A group is a span of media time, and matroska keeps its timestamps to
    the millisecond, so the span is matched with a millisecond of slack at
    each end rather than exactly.
    """
    ordered = sorted(times)
    missing = []
    for sequence, row in sorted(published.items()):
        start, end = float(row["pts_start"]) - 0.002, float(row["pts_end"]) + 0.002
        index = bisect.bisect_left(ordered, start)
        if index >= len(ordered) or ordered[index] > end:
            missing.append(sequence)
    return missing


def judge(name: str, published: list[dict], reader: Tail, dest: Path,
          report_only: bool) -> bool:
    """One reader's haul against what the publisher said it published."""
    by_group = {int(row["group"]): row for row in published}
    first_published, last_published = min(by_group), max(by_group)
    counters = [row for row in reader.counters() if row.get("kind") == "track"]
    final = [row for row in counters if row.get("final")]
    summary = final[-1] if final else (counters[-1] if counters else None)

    times = packet_times(dest)
    packets = len(times)
    missing = groups_missing(by_group, times)
    whole = sum(int(row["packets"]) for row in by_group.values())
    print(f"--- {name} ---")
    print(f"published: {len(by_group)} groups {first_published}..{last_published}, "
          f"{whole} packets")
    if summary is None:
        print("received:  the module reported no counters (0.6.3 and before)")
    else:
        print("received:  " + json.dumps(summary, sort_keys=True))
    print(f"copied:    {packets} packets, pts "
          f"{times[0] if times else 0.0:.3f}..{times[-1] if times else 0.0:.3f}")
    if missing:
        print(f"missing:   {len(missing)} whole groups: {missing[:24]}"
              f"{' ...' if len(missing) > 24 else ''}")

    if summary is None:
        # Nothing but the packets to go on: what came out against what was
        # published over the whole broadcast.
        lost = whole - packets
        share = 100.0 * lost / whole if whole else 0.0
        print(f"MISSING {name}: {lost} of {whole} packets ({share:.2f}%), "
              f"{len(missing)} of {len(by_group)} groups")
        return report_only

    failures = []
    delivered = int(summary["delivered"])
    received = int(summary["received"])
    first, last = int(summary["first"]), int(summary["last"])
    abandoned = sum(
        int(summary[key]) for key in (
            "holes_abandoned_gone", "holes_abandoned_budget",
            "holes_abandoned_restart", "holes_abandoned_end",
        )
    )
    if abandoned:
        failures.append(
            f"{abandoned} holes abandoned ("
            + ", ".join(
                f"{summary[key]} {key.rsplit('_', 1)[1]}" for key in (
                    "holes_abandoned_gone", "holes_abandoned_budget",
                    "holes_abandoned_restart", "holes_abandoned_end",
                ) if summary[key]
            )
            + ")"
        )
    if summary["dropped_late"]:
        failures.append(f"{summary['dropped_late']} groups arrived below the cursor")
    # A backlog join starts at the lowest sequence it is given, so it
    # steps over nothing: `skipped_join` is a live join's business.
    if summary.get("skipped_join"):
        failures.append(f"{summary['skipped_join']} groups stepped over at the join")
    accounted = (delivered + int(summary["dropped_late"]) + int(summary["repeated"])
                 + int(summary.get("skipped_join", 0)))
    if accounted != received:
        failures.append(
            f"{received} groups received, and {accounted} delivered, late, "
            "repeated or stepped over does not account for them"
        )
    if delivered != last - first + 1:
        failures.append(
            f"{delivered} groups delivered over the range {first}..{last}, "
            "which is not all of it"
        )
    if first != first_published or last != last_published:
        failures.append(
            f"delivered {first}..{last} of the published {first_published}..{last_published}"
        )
    wanted = sum(int(by_group[g]["packets"]) for g in range(first, last + 1) if g in by_group)
    if packets != wanted:
        failures.append(f"{packets} packets copied out, and {wanted} were published")
    if missing:
        failures.append(f"{len(missing)} published groups left no packet: {missing[:24]}")

    if failures:
        for failure in failures:
            print(f"FAIL {name}: {failure}")
        return report_only
    print(f"PASS {name}: {delivered} groups, {packets} packets, none dropped")
    return True


def judge_starved(published: list[dict], reader: Tail, dest: Path) -> bool:
    """The reader whose hold was too small on purpose.

    It is MEANT to lose groups: a megabyte cannot hold two minutes of
    backlog. What it must not do is lose them quietly. Every hole given
    up on and every group that came too late has to be named in a row of
    its own, the counters have to account for every group received, and
    what it did write has to still be in order.
    """
    rows = reader.counters()
    tracks = [row for row in rows if row.get("kind") == "track" and row.get("final")]
    if not tracks:
        print("FAIL starved: the module reported no counters")
        return False
    summary = tracks[-1]
    holes = [row for row in rows if row.get("kind") == "hole"]
    late = [row for row in rows if row.get("kind") == "late"]
    times = packet_times(dest)
    print("--- starved ---")
    print("received:  " + json.dumps(summary, sort_keys=True))
    print(f"named:     {len(holes)} holes, {len(late)} late groups")
    print(f"copied:    {len(times)} packets of "
          f"{sum(int(row['packets']) for row in published)} published")

    abandoned = sum(
        int(summary[key]) for key in (
            "holes_abandoned_gone", "holes_abandoned_budget",
            "holes_abandoned_restart", "holes_abandoned_end",
        )
    )
    failures = []
    if not abandoned:
        failures.append("a one megabyte hold gave nothing up, so it was not starved")
    if not summary["holes_abandoned_budget"]:
        failures.append("nothing was given up for the budget")
    if len(holes) != abandoned:
        failures.append(f"{abandoned} holes abandoned and {len(holes)} named")
    if len(late) != int(summary["dropped_late"]):
        failures.append(
            f"{summary['dropped_late']} groups came too late and {len(late)} named"
        )
    accounted = (int(summary["delivered"]) + int(summary["dropped_late"])
                 + int(summary["repeated"]) + int(summary.get("skipped_join", 0)))
    if accounted != int(summary["received"]):
        failures.append(
            f"{summary['received']} received and {accounted} accounted for"
        )
    if times != sorted(times):
        failures.append("the packets it did write are not in order")
    for failure in failures:
        print(f"FAIL starved: {failure}")
    if failures:
        return False
    print(f"PASS starved: {abandoned} holes and {summary['dropped_late']} late "
          "groups, every one of them named")
    return True


def one_run(args: argparse.Namespace) -> None:
    work = Path(tempfile.mkdtemp(prefix="moq-backlog-", dir=args.work))
    relay = publisher = None
    readers: list[Tail] = []
    publish_query = PACKAGE / ".live-backlog-publish.sql"
    subscribe_query = PACKAGE / ".live-backlog-subscribe.sql"
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

        source = work / "media.mp4"
        keepalive = work / "keepalive.mp4"
        make_sources(source, keepalive, args.seconds, args.keepalive)
        publish_query.write_text(PUBLISH_QUERY)
        subscribe_query.write_text(PLAIN_QUERY if args.plain else SUBSCRIBE_QUERY)

        port = free_udp_port()
        # The relay holds the whole backlog on purpose: what bounds it in a
        # run like this is the publisher's own retention window, not the
        # relay's cache.
        relay = start_relay(port, work / "cert.pem", work / "key.pem",
                            work / "relay.log", ["--cache-duration", "300s"])
        print(f"moq-relay pid {relay.pid} on 127.0.0.1:{port}", flush=True)

        started = time.monotonic()
        publisher = start_publisher(publish_query, source, keepalive, port,
                                    cert_hex, args.audio_group_ms)
        print(f"publisher pid {publisher.child.pid}", flush=True)

        # The mid-backlog reader: it joins with the media still arriving, so
        # it is reading a backlog that is still growing behind it.
        hold = {} if args.plain else {
            "hold_ms": args.hold_ms, "hold_mib": args.hold_mib,
            "join_ms": args.join_ms,
        }
        wait_for_media(publisher, MID_JOIN_AFTER, BURST_DEADLINE)
        mid = start_reader(subscribe_query, port, cert_hex, work / "mid.mkv", "mid",
                           hold)
        readers.append(mid)
        print(f"mid reader pid {mid.child.pid} at {time.monotonic() - started:.1f}s",
              flush=True)

        # The after reader: it joins once the whole of the media is in the
        # relay and nothing more is being written to that track.
        published = wait_for_burst(publisher, BURST_DEADLINE, BURST_QUIET)
        print(f"the media was all in at {time.monotonic() - started:.1f}s: "
              f"{len(published)} groups", flush=True)
        after = start_reader(subscribe_query, port, cert_hex, work / "after.mkv",
                             "after", hold)
        readers.append(after)
        print(f"after reader pid {after.child.pid}", flush=True)

        # And one whose hold is far too small for the backlog: what it
        # loses it has to name.
        starved = None
        if args.starve and not args.plain:
            starved = start_reader(subscribe_query, port, cert_hex,
                                   work / "starved.mkv", "starved",
                                   {**hold, "hold_mib": 1})
            readers.append(starved)
            print(f"starved reader pid {starved.child.pid}", flush=True)

        code = publisher.finish(RUN_DEADLINE)
        if code != 0:
            out, err = publisher.text()
            print(out[-2000:])
            sys.exit(f"the publisher failed ({code}):\n{err[-2000:]}")
        published = publisher.group_rows("audio")
        print(f"the publisher ended at {time.monotonic() - started:.1f}s with "
              f"{len(published)} audio groups", flush=True)

        passed = True
        for name, reader, dest in (("mid", mid, work / "mid.mkv"),
                                   ("after", after, work / "after.mkv")):
            code = reader.finish(RUN_DEADLINE)
            out, _ = reader.text()
            err = reader.module_stderr()
            if code != 0:
                print(out[-1500:])
                print(err[-3000:])
                sys.exit(f"the {name} reader failed ({code})")
            if not dest.exists():
                sys.exit(f"the {name} reader wrote nothing")
            if args.show:
                print(f"--- {name} stderr ---")
                print(err[-8000:])
            for line in err.splitlines():
                if "subscribe: row" in line and '"kind":"track"' not in line:
                    print(f"{name}: {line.strip()}")
            passed &= judge(name, published, reader, dest, args.report_only)

        if starved is not None:
            code = starved.finish(RUN_DEADLINE)
            if code != 0:
                print(starved.module_stderr()[-3000:])
                sys.exit(f"the starved reader failed ({code})")
            passed &= judge_starved(published, starved, work / "starved.mkv")
        readers = []

        if not passed:
            sys.exit("the backlog was not delivered whole")
        print(f"PASS: both readers took the backlog whole "
              f"({time.monotonic() - started:.1f}s)")
    finally:
        for reader in readers:
            kill_tree(reader.child)
        if publisher is not None:
            kill_tree(publisher.child)
        if relay is not None:
            kill_tree(relay)
        publish_query.unlink(missing_ok=True)
        subscribe_query.unlink(missing_ok=True)
        if args.keep:
            print(f"kept: {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=SECONDS,
                        help="seconds of media published unpaced")
    parser.add_argument("--keepalive", type=int, default=KEEPALIVE,
                        help="seconds the session is held open after the media")
    parser.add_argument("--audio-group-ms", type=int, default=AUDIO_GROUP_MS)
    parser.add_argument("--hold-ms", type=int, default=30000,
                        help="how long a hole waits for the group that fills it")
    parser.add_argument("--hold-mib", type=int, default=64,
                        help="how much one track holds meanwhile")
    parser.add_argument("--join-ms", type=int, default=2000,
                        help="how long a joining reader waits for a lower sequence")
    parser.add_argument("--work", default=None, help="where the run's files go")
    parser.add_argument("--no-build", dest="build", action="store_false",
                        help="use the guests already built")
    parser.add_argument("--report-only", action="store_true",
                        help="print the numbers without failing on them")
    parser.add_argument("--show", action="store_true",
                        help="print what each reader said on its stderr")
    parser.add_argument("--no-starve", dest="starve", action="store_false",
                        help="leave out the reader whose hold is too small")
    parser.add_argument("--plain", action="store_true",
                        help="a subscribe that takes neither hold nor start "
                             "arguments, which is how a package older than "
                             "0.6.5 is measured")
    parser.add_argument("--keep", action="store_true", help="keep the work directory")
    args = parser.parse_args()
    one_run(args)


if __name__ == "__main__":
    main()
