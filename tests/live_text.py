"""Cue rows in at publish, a text rendition through a local moq-relay, and
cue rows out of subscribe.

    python tests/live_text.py [--hold-ms N] [--prefixes P] [--keep]
                              [--hang-recv EXE --shaka DIR]

The publisher is publish.wasm under the sidecar, fed as `ffrwd run` feeds a
node sink (see `common.start_publish_node`): an H.264 picture bound to
`video`, and the rows of `tests/data/cues.nut` bound to `text`. A query
cannot bind a column to `text` yet, so the sidecar is started by hand here.
It publishes at once (`hold_s` 0), so neither feed waits on a reader and
both keep the wall clock from the same start.

`cues.txt` is the fixture's source, which ffrwd-nut's `json_nut` example
turns into the .nut: each row at the time a recognizer would write it, the
cue's own times inside it. One row is an utterance still growing, one is a
whole cue with no `final`, one carries markup, one has an empty line and a
`-->` in it, one ends before the text track has got that far, one starts
before it, and a long one runs through the second half.

Three readers take the text rendition:

- subscribe under the sidecar from the start, its text output into a NUT:
  every finished cue comes back as the row that went in, its text made safe
  for a `.vtt`, the late one dropped and the early one starting where the
  track had got;
- the same, joining 22 s in, inside the long cue: its first rows are the
  cues on screen, from the sample it joined at, and the long one ends where
  it does;
- `COPY (SELECT s.data[1] FROM ffrwd.moq.subscribe(...) s) TO 'query.nut'`
  under `ffrwd run`, which reads the same rows.

Each sample is a group of its own: the publisher's text track rows count as
many groups as samples.

With `--hang-recv` (harness/hang-recv, built) and `--shaka` (an installed
`shaka-player` package), a fourth reader takes the text track's init segment
and fragments off the wire with hang's own stack, and Shaka Player's
WebVTT-in-MP4 parser reads its cues (tests/shaka_cues.js): every cue the
capture was there for, at its own times and with its text, put back
together from the samples it spans.

`--prefixes` sets `FFRWD_MODULE_MOQ_PREFIXES` for every module; the
broadcast is `jobs/m5/out`.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    EXE, PACKAGE, Tail, ffrwd_argv, free_udp_port, guest, kill_tree, nut_rows, run, spawn,
    start_node_reader, start_publish_node, start_relay,
)

BROADCAST = "jobs/m5/out"
SECONDS = 40
CUES = PACKAGE / "tests" / "data" / "cues.nut"
CUE_SOURCE = PACKAGE / "tests" / "data" / "cues.txt"

# When the second reader joins, in seconds after the feeds start: the
# program is 22 s in and its text track two to three seconds behind that,
# inside the long cue.
LATE_JOIN = 22.0


def rows_in() -> list[dict]:
    """The fixture's rows, in the order they are written."""
    return [json.loads(line.split("\t", 1)[1])
            for line in CUE_SOURCE.read_text().splitlines() if line.strip()]


def safe(row: dict) -> dict:
    """A row as the track carries its text: empty lines gone, `-->` written
    `--&gt;`."""
    lines = [line for line in re.split(r"\r\n|\r|\n", row["text"]) if line.strip()]
    return {**row, "text": "\n".join(lines).replace("-->", "--&gt;")}


def unexpected(rows: list[dict], finals: list[dict]) -> str | None:
    """What is wrong with the final rows a reader there from the start got
    back, if anything. They are each finished cue as it went in, its text
    made safe, the whole cue with `final` added, and the one that started
    before the track had got that far from where it had got, which is later
    than its own start and no later than when it was written; the one that
    ended before the track had got that far is gone."""
    wanted = [rows[1], rows[2], {**rows[3], "final": True}, rows[4], safe(rows[5]), rows[7],
              rows[8], rows[9]]
    if len(finals) != len(wanted):
        return f"{len(finals)} final rows where {len(wanted)} were wanted: {finals}"
    for got, want in zip(finals, wanted):
        if want is rows[7]:
            moved = {**got, "start_t": want["start_t"]}
            if moved != want or not want["start_t"] < got["start_t"] < 17.0:
                return f"the cue written after its start came back as {got}"
        elif got != want:
            return f"{got} came back for {want}"
    return None


def say(text: str) -> None:
    print(f"== {text}", flush=True)


def picture(work: Path) -> Path:
    """The program's picture, as the NUT its feed reads."""
    video = work / "video.nut"
    done = run(["ffmpeg", "-v", "error", "-y", "-f", "lavfi",
                "-i", f"testsrc2=size=320x180:rate=30:duration={SECONDS}",
                "-c:v", "libx264", "-preset", "ultrafast", "-g", "30", "-bf", "0",
                "-pix_fmt", "yuv420p", "-f", "nut", str(video)],
               120, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(done.stderr)
    return video


def publisher(work: Path, port: int, cert_hex: str, hold_ms: int, video: Path):
    params = {
        "relay": f"moqt://127.0.0.1:{port}", "broadcast": BROADCAST, "cert": cert_hex,
        "rows": "summary", "reconnect_s": 0, "hold_s": 0, "text_hold_ms": hold_ms,
    }
    feeds = [
        {"port": "video", "source": video, "stream": "0:v:0", "rate": {"num": 30, "den": 1},
         "pad": None},
        {"port": "text", "source": CUES, "stream": "0:d:0", "rate": None, "pad": None},
    ]
    return start_publish_node(guest("publish"), params, feeds, work, "publisher")


def show(label: str, out: list[dict]) -> None:
    say(f"{len(out)} rows came out of {label}")
    for packet in out:
        print(f"  pts {packet['pts']:>6}  {json.dumps(packet['row'], separators=(',', ':'))}")


def shaka_cues(shaka: Path, track: Path) -> list[dict]:
    """Shaka's cues over a captured track, one per sample, put back together:
    a cue shown in samples one after another, the same text in each, is one
    cue from the first one's start to the last one's end."""
    done = run(["node", str(PACKAGE / "tests" / "shaka_cues.js"), str(shaka), str(track)], 120,
               capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"Shaka refused the captured track:\n{done.stderr[-1500:]}")
    pieces = [json.loads(line) for line in done.stdout.splitlines() if line.startswith("{")]
    cues: list[dict] = []
    for piece in pieces:
        open_cue = next((cue for cue in cues if cue["text"] == piece["text"]
                         and abs(cue["end"] - piece["start"]) < 0.0005), None)
        if open_cue is not None:
            open_cue["end"] = piece["end"]
        else:
            cues.append(dict(piece))
    return cues


def check_shaka(shaka: Path, track: Path, first_rows: list[dict]) -> list[str]:
    """Shaka's read of the captured track against the rows the first reader
    got back: each final row the capture was there for, at the same times,
    with the text it shows, which is the row's with its tags taken off and
    its character references read."""
    cues = shaka_cues(shaka, track)
    say(f"Shaka read {len(cues)} cues off the captured track")
    for cue in cues:
        print(f"  {json.dumps(cue)}")
    if not cues:
        return ["Shaka read no cues off the captured track"]
    begins = min(cue["start"] for cue in cues)
    problems = []
    owed = [row for row in first_rows if row.get("final") and row["start_t"] >= begins]
    for row in owed:
        shown = html.unescape(re.sub(r"<[^>]*>", "", row["text"]))
        match = [cue for cue in cues if cue["text"] == shown
                 and abs(cue["start"] - row["start_t"]) < 0.0015
                 and abs(cue["end"] - row["end_t"]) < 0.0015]
        if not match:
            problems.append(f"Shaka has no cue for {row}")
        elif ("<i>" in row["text"]) != match[0]["italic"]:
            problems.append(f"Shaka's cue for {row} is "
                            f"{'italic' if match[0]['italic'] else 'not italic'}")
    say(f"Shaka agrees with {len(owed) - len(problems)} of the {len(owed)} final rows the "
        f"capture was there for")
    return problems


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hold-ms", type=int, default=3000)
    parser.add_argument("--prefixes", help="FFRWD_MODULE_MOQ_PREFIXES for every module")
    parser.add_argument("--hang-recv", type=Path, help="harness/hang-recv, built")
    parser.add_argument("--shaka", type=Path, help="an installed shaka-player package")
    parser.add_argument("--keep", action="store_true", help="leave the work directory")
    args = parser.parse_args()
    if args.prefixes is not None:
        os.environ["FFRWD_MODULE_MOQ_PREFIXES"] = args.prefixes
    work = Path(tempfile.mkdtemp(prefix="moq-text-", dir=os.environ.get("WORK")))
    say(f"work in {work}")
    certgen = PACKAGE / "harness" / "certgen" / "target" / "release" / f"certgen{EXE}"
    # moq-relay reloads its certificate whenever the directory it sits in
    # changes, so it sits apart from what the run writes.
    certs = work / "certs"
    certs.mkdir()
    cert_hex = run([certgen, certs], 60, capture_output=True, text=True).stdout.strip()
    video = picture(work)
    port = free_udp_port()
    relay = start_relay(port, certs / "cert.pem", certs / "key.pem", work / "relay.log")
    children = []
    failures = []
    try:
        pub, feeders = publisher(work, port, cert_hex, args.hold_ms, video)
        children += [pub.child, *feeders]
        fed = time.monotonic()
        captured = capture = None
        if args.hang_recv and args.shaka:
            captured = work / "text-track.mp4"
            env = {**os.environ, "RELAY_PORT": str(port), "RELAY_CERT_PEM": str(certs / "cert.pem"),
                   "BROADCAST": BROADCAST, "OUTPUT": str(captured), "FRAMES": "1",
                   "TEXT": "text"}
            capture = Tail(spawn([str(args.hang_recv)], env=env, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True))
            children.append(capture.child)
        time.sleep(0.5)
        params = {"relay": f"moqt://127.0.0.1:{port}", "broadcast": BROADCAST,
                  "cert": cert_hex, "start": "live"}
        first_nut, late_nut, query_nut = work / "text.nut", work / "late.nut", work / "query.nut"
        reader = start_node_reader(guest("subscribe"), params, ["text"], first_nut, "first")
        children.append(reader.child)
        query = work / "read.sql"
        query.write_text("COPY (SELECT s.data[1] FROM ffrwd.moq.subscribe(:'relay', "
                         ":'broadcast', :'cert') s) TO :'out'\n")
        asked = Tail(spawn(ffrwd_argv("run", "-f", query, "-y", "-q",
                                      "-v", f"relay=moqt://127.0.0.1:{port}",
                                      "-v", f"broadcast={BROADCAST}", "-v", f"cert={cert_hex}",
                                      "-v", f"out={query_nut}"),
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=work))
        children.append(asked.child)
        time.sleep(max(0.0, LATE_JOIN - (time.monotonic() - fed)))
        late = start_node_reader(guest("subscribe"), params, ["text"], late_nut, "late")
        children.append(late.child)
        waits = [("first", reader, SECONDS + 90), ("late", late, 60), ("query", asked, 60)]
        if capture is not None:
            waits.append(("hang-recv", capture, 60))
        for name, child, deadline in waits:
            code = child.finish(deadline)
            if code != 0:
                failures.append(f"the {name} reader exited {code}: {child.text()[1][-800:]}")
        pub.finish(30)
        trailing = [row for row in pub.rows if "tracks" in row]
        say("the publisher's trailing row: " + json.dumps(trailing[-1] if trailing else None))
        counted = [(row["groups"], row["packets"]) for row in pub.rows
                   if row.get("track") == "text"]
        say(f"the text track's groups and samples, row by row: {counted}")
        if not counted or any(groups != samples for groups, samples in counted):
            failures.append(f"the text track's groups and samples differ: {counted}")

        rows = rows_in()
        first = nut_rows(first_nut, "d:0")
        show("the first reader", first)
        finals = [packet["row"] for packet in first if packet["row"].get("final")]
        wrong = unexpected(rows, finals)
        if wrong is not None:
            failures.append(f"the first reader's final rows: {wrong}")
        if [p["pts"] for p in first] != sorted(p["pts"] for p in first):
            failures.append("the first reader's rows go back in time")
        for packet in first:
            row = packet["row"]
            if row.get("final") and packet["pts"] != round(row["end_t"] * 1000):
                failures.append(f"a final row not at its cue's end: {packet}")
        if trailing and (trailing[-1].get("late"), trailing[-1].get("clipped"),
                         trailing[-1].get("growing"), trailing[-1].get("empty")) != (1, 1, 1, 0):
            failures.append("the publisher did not count one late, one clipped, one growing "
                            "and no empty row")

        joined = nut_rows(late_nut, "d:0")
        show(f"the reader that joined {LATE_JOIN:.0f}s in", joined)
        # Its first sample's rows are every cue on screen when it joined, the
        # long one among them, each from where the reader joined.
        joined_at = joined[0]["pts"] if joined else None
        opening = [p["row"] for p in joined if p["pts"] == joined_at]
        long_one = [row for row in opening if row.get("text") == "a long one"]
        if not long_one or long_one[0].get("final") or long_one[0]["start_t"] * 1000 != joined_at:
            failures.append("the late reader's first rows are not the cues on screen when it "
                            f"joined: {opening}")
        ended = [p["row"] for p in joined
                 if p["row"].get("text") == "a long one" and p["row"].get("final")]
        if not (long_one and len(ended) == 1 and ended[0]["end_t"] == 38.0
                and ended[0]["start_t"] == long_one[0]["start_t"]):
            failures.append("the late reader did not end the cue it joined in where it ends")

        asked_rows = nut_rows(query_nut, "d:0") if query_nut.exists() else []
        asked_finals = [p["row"] for p in asked_rows if p["row"].get("final")]
        say(f"the query's final rows: {json.dumps(asked_finals, separators=(',', ':'))}")
        # It joins a moment after the first reader, so its first cue may start
        # where it joined; every one after that is the first reader's.
        if len(asked_finals) < 2 or asked_finals[1:] != finals[len(finals) - len(asked_finals) + 1:]:
            failures.append("the query did not read the rows the first reader did")

        if captured is not None:
            failures += check_shaka(args.shaka, captured, [p["row"] for p in first])
    finally:
        for child in children:
            kill_tree(child)
        kill_tree(relay)
        for fifo in work.glob("*.fifo"):
            fifo.unlink(missing_ok=True)
        if not args.keep and not failures:
            for path in sorted(work.rglob("*"), reverse=True):
                path.rmdir() if path.is_dir() else path.unlink()
            work.rmdir()
    if failures:
        for failure in failures:
            print("FAIL:", failure)
        sys.exit(1)
    say("PASS: the finished cue rows came back out of subscribe as they went in, a reader "
        "that joined late had the cue on screen from its first row, and a query read the same")


if __name__ == "__main__":
    main()
