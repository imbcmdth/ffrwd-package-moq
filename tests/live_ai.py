"""A live source, an AI stage and a MoQ broadcast in one query - run it by hand.

    python tests/live_ai.py [--runs N] [--keep] [--clip PATH]
                            [--offset S] [--seconds S]

The composed target: ffmpeg reads a real clip AT ITS OWN FRAME RATE
(`realtime => true` puts `-re` on the input), ffrwd.yolo26 segments the
people in it, ffrwd.mask_tools mosaics what the model matted, and the
whole thing leaves through ffrwd.moq.publish in ONE `COPY ... TO` - no
intermediate file anywhere. A local moq-relay carries the broadcast and
the sub-recv guest (wasmtime) reassembles it to an mp4 that ffprobe must
count every frame of.

That the AI actually ran is proved against a CONTROL: the same source,
the same encoder options, the same relay, published WITHOUT the yolo26
stage. The two reassembled streams are compared frame by frame inside
the matte the model produced and outside it. The mosaic makes every
pixel of a block its block's mean, so a block lying wholly inside the
matte comes back FLAT from the AI stream while the control's keeps
whatever detail the picture had; outside the matte the two pipelines
carry the same pixels through the same libx264, so a visibly moved
pixel is close to absent there. A run where the two regions change
alike proves nothing and fails.

The two packages meet through a scratch PROJECT: `ffrwd init` writes its
manifest and lockfile, `ffrwd link` points the lockfile at each package's
working directory, and the compiler resolves ffrwd.moq, ffrwd.yolo26 and
ffrwd.mask_tools out of that lockfile. Nothing is installed and nothing
is published.

Never collected by any suite: it needs moq-relay, wasmtime, ffmpeg,
ffprobe, cargo, a wasi-sdk clang, a machine that can run the yolo26
models, and a clip with people in it. Toolchain overrides: WASMTIME,
MOQ_RELAY, WASI_SDK_PATH (or CC_wasm32_wasip2 directly). The clip is
FFRWD_DEMO_CLIP or --clip.

Outputs are left under the work directory, whose path every run prints;
--keep stops it being removed so one can be watched.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from common import (
    CLI,
    SIDECAR,
    build_guests,
    ffprobe_frames,
    free_udp_port,
    kill_tree,
    run,
    start_relay,
    start_subscriber,
    wait_subscriber,
)

PACKAGES = Path(__file__).resolve().parent.parent.parent
YOLO26 = PACKAGES / "yolo26"
MASK_TOOLS = PACKAGES / "mask_tools"
MOQ = PACKAGES / "moq"

DEFAULT_CLIP = Path(r"D:\projects\angel-one.mp4")
OFFSET = 30
SECONDS = 10
GOP = 30
MOSAIC = 24
TRACK = "video"

# How many frames the pixel comparison reads out of each reassembly, and
# how far in from either end it stays: the first and last group of a live
# publish are the ones the pipeline is still filling and draining.
SAMPLES = 6
MARGIN = 0.1

# What the transformation itself says the comparison must find.
#
# The mosaic makes every pixel of a block its block's mean, so a block that
# lies wholly inside the matte is FLAT in the AI stream and whatever the
# picture is in the control: that collapse is the mosaic, and it does not
# depend on how textured the people happen to be. A pixel-difference of more
# than MOVED levels is a visible one; inside the matte a mosaic produces
# them in quantity, and outside it the two pipelines carry the same pixels
# through the same encoder, so only the yuv420p/gbrp round trip separates
# them and a visible move is close to absent.
MOVED = 16
FLATTEST_MOSAIC = 3.0
LEAST_DETAIL = 3.0
LEAST_FLATTENING = 4.0
LEAST_LOCALITY = 20.0
MOST_MOVED_OUTSIDE = 0.01
LEAST_COVERAGE = 0.01

# What "kept up with live pace" means. `-re` paces the reader at the source's
# own frame rate, so the media duration is the floor and everything above it
# is startup plus whatever the graph could not absorb. The AI run is allowed
# a quarter of the media duration over the control's wall - the control is
# the same pipeline with the model taken out, so the gap IS the model.
PACE_MARGIN = 0.25
STARTUP_ALLOWANCE = 8.0

# Hard deadlines, seconds. Generous: an expiry means something is stuck,
# not slow.
BUILD_DEADLINE = 600
PROJECT_DEADLINE = 180
COMPILE_DEADLINE = 180
PUBLISH_DEADLINE = 600
INFERENCE_DEADLINE = 900
SUBSCRIBER_DEADLINE = 180
TOOL_DEADLINE = 180

# The composed query. One COPY: a live-paced input, the model, the mosaic
# the model's matte selects, and the relay.
#
# The stream is subscripted rather than unnested, unlike the yolo26 recipes:
# `unnest` needs the probe, and an input carrying options is probed with those
# options, which ffprobe does not take. The plan the compiler makes of this
# opens the source TWICE - the model's copy and the picture's - so a source
# that can only be opened once needs the recipe split around a fan-out.
MOSAIC_PUBLISH = f"""\
COPY (
  SELECT ffrwd.mask_tools.mosaic_where(
           f.video[1], ffrwd.yolo26.segment_mask(f.video[1], 'person'), {MOSAIC})
  FROM input(:'source', realtime => true) f
) TO ffrwd.moq.publish(:'relay', :'broadcast', 'video', COALESCE(:'cert', ''))
  WITH (gop {GOP}, preset 'veryfast', tune 'zerolatency')
"""

# The control: the same source at the same pace to the same relay with the
# same encoder options, and no model.
PLAIN_PUBLISH = f"""\
COPY (
  SELECT f.video[1]
  FROM input(:'source', realtime => true) f
) TO ffrwd.moq.publish(:'relay', :'broadcast', 'video', COALESCE(:'cert', ''))
  WITH (gop {GOP}, preset 'veryfast', tune 'zerolatency')
"""

# The matte the pipeline mosaics under, written out on its own so the
# comparison knows which pixels are the people. Lossless, so a threshold
# reads the model's own answer and not an encoder's.
MATTE = """\
COPY (
  SELECT ffrwd.yolo26.segment_mask(f.video[1], 'person')
  FROM input(:'source') f
) TO :'dest' WITH (video_codec 'ffv1')
"""

QUERIES = {
    "mosaic-publish.sql": MOSAIC_PUBLISH,
    "plain-publish.sql": PLAIN_PUBLISH,
    "matte.sql": MATTE,
}


@dataclass
class Published:
    """One broadcast that went out and came back."""

    rows: list[dict]
    transcript: str
    output: Path
    frames: int
    wall: float


def ffrwd(argv: list[str], deadline: int, project: Path) -> subprocess.CompletedProcess:
    """The compiler, run out of the scratch project so its lockfile is the one found."""
    env = dict(os.environ)
    env["FFRWD_WASM"] = str(SIDECAR)
    return run(
        ["uv", "run", "--project", CLI, "ffrwd", *argv],
        deadline, capture_output=True, text=True, env=env, cwd=project,
    )


def make_project(
    clip: Path, offset: int, seconds: int
) -> tuple[Path, Path, float, int, int, int]:
    """The scratch project, the source cut from `clip`, and what ffprobe says it is."""
    project = Path(tempfile.mkdtemp(prefix="moq-live-ai-"))
    done = ffrwd(["init", "--name", "demo/live_ai"], PROJECT_DEADLINE, project)
    if done.returncode != 0:
        sys.exit(f"the scratch project could not be started:\n{done.stderr[-1200:]}")
    for package in (MOQ, YOLO26, MASK_TOOLS):
        done = ffrwd(["link", str(package)], PROJECT_DEADLINE, project)
        if done.returncode != 0:
            sys.exit(f"linking {package} failed:\n{done.stderr[-1200:]}")
        print(done.stdout.strip())
    for name, text in QUERIES.items():
        (project / name).write_text(text, encoding="utf-8", newline="\n")

    source = project / "source.mp4"
    done = run([
        "ffmpeg", "-v", "error", "-y", "-ss", str(offset), "-i", str(clip),
        "-t", str(seconds), "-an", "-c:v", "libx264", "-preset", "veryfast",
        "-g", str(GOP), "-pix_fmt", "yuv420p", str(source),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"cutting the source out of {clip} failed:\n{done.stderr[-800:]}")

    rate, width, height, frames = probe_source(source)
    print(
        f"source: {source} - {frames} frames, {width}x{height}, {rate:g} fps, "
        f"{frames / rate:.2f}s of media"
    )
    return project, source, rate, width, height, frames


def probe_source(source: Path) -> tuple[float, int, int, int]:
    done = run([
        "ffprobe", "-v", "error", "-count_frames", "-select_streams", "v:0",
        "-show_entries", "stream=nb_read_frames,avg_frame_rate,width,height",
        "-of", "default=nw=1", str(source),
    ], TOOL_DEADLINE, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe rejected the source:\n{done.stderr[-800:]}")
    found: dict[str, str] = {}
    for line in done.stdout.splitlines():
        key, _, value = line.partition("=")
        found[key] = value
    top, _, bottom = found["avg_frame_rate"].partition("/")
    rate = float(top) / float(bottom or 1)
    return rate, int(found["width"]), int(found["height"]), int(found["nb_read_frames"])


def compile_query(query: Path, source: Path, project: Path) -> str:
    """The command the compiler makes of `query`, printed as it prints it."""
    done = ffrwd(
        ["compile", "-f", str(query),
         "-v", f"source={source}",
         "-v", "relay=moqt://127.0.0.1:4443",
         "-v", "broadcast=live/demo",
         "-v", "cert=00"],
        COMPILE_DEADLINE, project,
    )
    if done.returncode != 0:
        sys.exit(f"{query.name} does not compile:\n{done.stderr[-1600:]}")
    print(f"--- {query.name} ---")
    print(done.stdout.strip())
    return done.stdout


def rows_of(stdout: str) -> list[dict]:
    rows = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows


def publish(
    query: Path, source: Path, project: Path, sub_wasm: Path, certgen: Path,
    work: Path, broadcast: str,
) -> Published:
    """One query out to its own relay and back through the subscriber."""
    out_dir = work / "out"
    out_dir.mkdir(parents=True)
    made = run([certgen, work], TOOL_DEADLINE, capture_output=True, text=True)
    if made.returncode != 0:
        sys.exit(f"certgen failed:\n{made.stderr[-400:]}")
    cert_hex = made.stdout.strip()

    port = free_udp_port()
    relay = None
    sub = None
    try:
        relay = start_relay(port, work / "cert.pem", work / "key.pem", work / "relay.log")
        sub = start_subscriber(sub_wasm, port, cert_hex, out_dir, broadcast, TRACK)
        started = time.monotonic()
        done = ffrwd(
            ["run", "-f", str(query),
             "-v", f"source={source}",
             "-v", f"relay=moqt://127.0.0.1:{port}",
             "-v", f"broadcast={broadcast}",
             "-v", f"cert={cert_hex}",
             "-q"],
            PUBLISH_DEADLINE, project,
        )
        wall = time.monotonic() - started
        print(f"--- {query.name}: exit {done.returncode}, {wall:.1f}s wall ---")
        print(done.stdout)
        if done.returncode != 0:
            print(done.stderr[-2000:])
            sys.exit(f"{query.name} did not run")
        transcript = wait_subscriber(sub, SUBSCRIBER_DEADLINE)
        sub = None
    finally:
        if sub is not None:
            kill_tree(sub)
        if relay is not None:
            kill_tree(relay)

    output = out_dir / "recv.mp4"
    frames = ffprobe_frames(output, TOOL_DEADLINE)
    return Published(rows_of(done.stdout), transcript, output, frames, wall)


def check_broadcast(published: Published, expected_frames: int, named: str) -> None:
    """The module's own rows, the subscriber's transcript and ffprobe must agree."""
    group_rows = [r for r in published.rows if "group" in r]
    summaries = [r for r in published.rows if "groups" in r]
    assert group_rows, f"{named}: the module emitted no group rows"
    assert len(summaries) == 1, f"{named}: expected one summary row, got {len(summaries)}"
    summary = summaries[0]
    assert summary["groups"] == len(group_rows), (
        f"{named}: summary says {summary['groups']} groups, {len(group_rows)} rows arrived"
    )
    assert summary["bytes"] == sum(r["bytes"] for r in group_rows), f"{named}: bytes disagree"
    assert summary["packets"] == sum(r["packets"] for r in group_rows), (
        f"{named}: packet counts disagree"
    )
    assert summary["packets"] == expected_frames, (
        f"{named}: {summary['packets']} packets published for {expected_frames} source frames"
    )

    line = next(
        spoken for spoken in published.transcript.splitlines()
        if spoken.startswith("sub: reassembled")
    )
    words = line.split()
    fragments, groups, received = int(words[2]), int(words[5]), int(words[7])
    assert fragments == len(group_rows), f"{named}: fragment counts diverge"
    assert groups == len(group_rows), f"{named}: group counts diverge"
    assert received == summary["bytes"] + summary["init_bytes"], (
        f"{named}: byte counts diverge"
    )
    assert published.frames == expected_frames, (
        f"{named}: {published.frames} frames reassembled, {expected_frames} published"
    )
    # The encoder starts a group at least every GOP frames; a scene cut may
    # start one sooner, never later.
    least = math.ceil(expected_frames / GOP)
    assert groups >= least, f"{named}: {groups} groups for a {GOP}-frame keyframe interval"


def gray_frames(path: Path, picks: list[int], width: int, height: int) -> list[bytes]:
    """The named frames of `path`, one gray8 plane each."""
    chosen = "+".join(rf"eq(n\,{n})" for n in picks)
    done = run([
        "ffmpeg", "-v", "error", "-i", str(path),
        "-vf", f"select='{chosen}'", "-fps_mode", "passthrough",
        "-pix_fmt", "gray", "-f", "rawvideo", "-",
    ], TOOL_DEADLINE, capture_output=True)
    if done.returncode != 0:
        sys.exit(f"reading frames of {path} failed:\n{done.stderr[-800:].decode(errors='replace')}")
    size = width * height
    raw = done.stdout
    if len(raw) != size * len(picks):
        sys.exit(
            f"{path}: asked for {len(picks)} frames of {size} bytes, got {len(raw)} bytes"
        )
    return [raw[at * size:(at + 1) * size] for at in range(len(picks))]


@dataclass
class Divergence:
    """How the two reassemblies differ, inside the model's matte and outside it.

    `moved_inside`/`moved_outside` are the share of pixels a visible step
    apart, `mean_inside`/`mean_outside` the average step. `flat`/`detailed`
    are the mean within-block spread over the blocks that lie wholly inside
    the matte - the AI stream's and the control's.
    """

    coverage: float
    mean_inside: float
    mean_outside: float
    moved_inside: float
    moved_outside: float
    flat: float
    detailed: float
    blocks: int


def _pixels(
    treated: bytes, control: bytes, matte: bytes
) -> tuple[int, int, int, int, int, int]:
    """Difference total, moved count and pixel count, inside the matte and outside."""
    inside = moved_in = pixels_in = outside = moved_out = pixels_out = 0
    for mask, one, other in zip(matte, treated, control):
        gap = one - other
        if gap < 0:
            gap = -gap
        if mask > 128:
            inside += gap
            moved_in += gap > MOVED
            pixels_in += 1
        else:
            outside += gap
            moved_out += gap > MOVED
            pixels_out += 1
    return inside, moved_in, pixels_in, outside, moved_out, pixels_out


def _matted_blocks(matte: bytes, width: int, height: int) -> list[int]:
    """Where the mosaic's blocks lie wholly inside the matte, as pixel offsets.

    The blocks are the ones `pixelize` cuts: a grid from the frame's origin.
    A block straddling the matte's edge is part mosaic and part picture, so
    it says nothing and is left out.
    """
    found = []
    for top in range(0, height - MOSAIC + 1, MOSAIC):
        for left in range(0, width - MOSAIC + 1, MOSAIC):
            at = top * width + left
            if all(
                min(matte[at + row * width:at + row * width + MOSAIC]) > 128
                for row in range(MOSAIC)
            ):
                found.append(at)
    return found


def _spread(frame: bytes, at: int, width: int) -> float:
    """One block's standard deviation - what the mosaic drives to nothing."""
    total = square = 0
    for row in range(MOSAIC):
        start = at + row * width
        for value in frame[start:start + MOSAIC]:
            total += value
            square += value * value
    count = MOSAIC * MOSAIC
    mean = total / count
    return max(square / count - mean * mean, 0.0) ** 0.5


def prove_ai(
    treated: Path, control: Path, matte: Path, frames: int, width: int, height: int
) -> Divergence:
    """How far the two reassemblies diverge inside the model's matte, and outside it."""
    first = int(frames * MARGIN)
    last = frames - 1 - int(frames * MARGIN)
    step = (last - first) / max(SAMPLES - 1, 1)
    picks = sorted({first + round(step * n) for n in range(SAMPLES)})
    print(f"--- comparing frames {picks} ---")

    treated_frames = gray_frames(treated, picks, width, height)
    control_frames = gray_frames(control, picks, width, height)
    matte_frames = gray_frames(matte, picks, width, height)

    inside = moved_in = pixels_in = outside = moved_out = pixels_out = 0
    flat = detailed = 0.0
    blocks = 0
    for one, other, mask in zip(treated_frames, control_frames, matte_frames):
        a, b, c, d, e, f = _pixels(one, other, mask)
        inside += a
        moved_in += b
        pixels_in += c
        outside += d
        moved_out += e
        pixels_out += f
        for at in _matted_blocks(mask, width, height):
            flat += _spread(one, at, width)
            detailed += _spread(other, at, width)
            blocks += 1
    total = pixels_in + pixels_out
    return Divergence(
        coverage=pixels_in / total if total else 0.0,
        mean_inside=inside / pixels_in if pixels_in else 0.0,
        mean_outside=outside / pixels_out if pixels_out else 0.0,
        moved_inside=moved_in / pixels_in if pixels_in else 0.0,
        moved_outside=moved_out / pixels_out if pixels_out else 0.0,
        flat=flat / blocks if blocks else 0.0,
        detailed=detailed / blocks if blocks else 0.0,
        blocks=blocks,
    )


def detections(source: Path, project: Path, dest: Path) -> Counter[str]:
    """What yolo26 says is in the source, by class - the model's own rows."""
    done = ffrwd(
        ["run", "detections", "-v", f"source={source}", "-v", f"dest={dest}", "-q"],
        INFERENCE_DEADLINE, project,
    )
    if done.returncode != 0:
        sys.exit(f"the detections pass failed:\n{done.stderr[-1600:]}")
    found: Counter[str] = Counter()
    for line in dest.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        found[json.loads(line)["class"]] += 1
    return found


def one_run(
    number: int, project: Path, source: Path, matte: Path, rate: float,
    width: int, height: int, frames: int, sub_wasm: Path, certgen: Path,
) -> None:
    work = project / f"run{number}"
    treated = publish(
        project / "mosaic-publish.sql", source, project, sub_wasm, certgen,
        work / "ai", "live/ai",
    )
    control = publish(
        project / "plain-publish.sql", source, project, sub_wasm, certgen,
        work / "control", "live/control",
    )
    check_broadcast(treated, frames, "ai")
    check_broadcast(control, frames, "control")

    media = frames / rate
    print(
        f"--- pace: {media:.2f}s of media; ai {treated.wall:.1f}s wall "
        f"({frames / treated.wall:.1f} fps), control {control.wall:.1f}s wall "
        f"({frames / control.wall:.1f} fps) ---"
    )
    assert treated.wall <= control.wall + media * PACE_MARGIN, (
        f"the model cost {treated.wall - control.wall:.1f}s over the control's "
        f"{control.wall:.1f}s, more than {media * PACE_MARGIN:.1f}s of the "
        f"{media:.2f}s it had"
    )
    assert treated.wall <= media + STARTUP_ALLOWANCE, (
        f"{treated.wall:.1f}s wall for {media:.2f}s of live-paced media"
    )

    seen = prove_ai(treated.output, control.output, matte, frames, width, height)
    print(
        f"--- ai proof: the matte covers {seen.coverage * 100:.1f}% of the sampled "
        f"pixels\n"
        f"    mean |ai - control|: {seen.mean_inside:.2f} inside the matte, "
        f"{seen.mean_outside:.2f} outside it\n"
        f"    pixels more than {MOVED} levels apart: "
        f"{seen.moved_inside * 100:.2f}% inside, {seen.moved_outside * 100:.2f}% outside\n"
        f"    within-block spread over {seen.blocks} blocks wholly inside the matte: "
        f"ai {seen.flat:.2f}, control {seen.detailed:.2f} ---"
    )
    assert seen.coverage >= LEAST_COVERAGE, (
        f"the matte covers {seen.coverage * 100:.2f}% of the frame; nothing to prove"
    )
    assert seen.blocks > 0, "no mosaic block lies wholly inside the matte"
    assert seen.detailed >= LEAST_DETAIL, (
        f"the control's matted blocks spread only {seen.detailed:.2f}; there is no "
        f"detail there for a mosaic to destroy"
    )
    assert seen.flat <= FLATTEST_MOSAIC, (
        f"the ai stream's matted blocks spread {seen.flat:.2f}, and a "
        f"{MOSAIC}-pixel mosaic leaves a block flat"
    )
    assert seen.detailed >= seen.flat * LEAST_FLATTENING, (
        f"{seen.flat:.2f} against the control's {seen.detailed:.2f}: the matted "
        f"blocks were not flattened"
    )
    assert seen.moved_outside <= MOST_MOVED_OUTSIDE, (
        f"{seen.moved_outside * 100:.2f}% of the pixels outside the matte moved more "
        f"than {MOVED} levels, where both pipelines carry the same picture through "
        f"the same encoder"
    )
    assert seen.moved_inside >= seen.moved_outside * LEAST_LOCALITY, (
        f"{seen.moved_inside * 100:.2f}% moved inside the matte against "
        f"{seen.moved_outside * 100:.2f}% outside it: the change is not localized "
        f"to the people"
    )
    print(f"PASS: run {number} - {treated.frames} AI-processed frames end to end")
    print(f"  watch: {treated.output}")
    print(f"  control: {control.output}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--keep", action="store_true", help="keep the work directory")
    parser.add_argument(
        "--clip", type=Path,
        default=Path(os.environ.get("FFRWD_DEMO_CLIP", DEFAULT_CLIP)),
        help="the clip the live source is cut from; it needs people in it",
    )
    parser.add_argument("--offset", type=int, default=OFFSET, help="seconds into the clip")
    parser.add_argument("--seconds", type=int, default=SECONDS, help="seconds of live source")
    args = parser.parse_args()

    if not SIDECAR.exists():
        sys.exit(f"no sidecar at {SIDECAR}; build it first (cargo build --release)")
    if not args.clip.is_file():
        sys.exit(f"no clip at {args.clip}; pass --clip or set FFRWD_DEMO_CLIP")
    models = YOLO26 / "target" / "wasm32-wasip2" / "release"
    for needed in ("segment_mask.wasm", "segment_mask.onnx", "detect.wasm", "detect.onnx"):
        if not (models / needed).is_file():
            sys.exit(
                f"no {needed} in {models}; build the yolo26 modules and install the "
                f"package so its models land beside them"
            )
    _, sub_wasm, certgen = build_guests(BUILD_DEADLINE)

    project, source, rate, width, height, frames = make_project(
        args.clip, args.offset, args.seconds
    )
    try:
        for name in ("mosaic-publish.sql", "plain-publish.sql"):
            compile_query(project / name, source, project)

        # The same model over the same frames with no `-re`: what it manages
        # when nothing paces it, which is the headroom the live runs spend.
        matte = project / "matte.mkv"
        started = time.monotonic()
        done = ffrwd(
            ["run", "-f", str(project / "matte.sql"),
             "-v", f"source={source}", "-v", f"dest={matte}", "-q"],
            INFERENCE_DEADLINE, project,
        )
        unpaced = time.monotonic() - started
        if done.returncode != 0:
            sys.exit(f"the matte pass failed:\n{done.stderr[-1600:]}")
        print(
            f"--- unpaced: {frames} frames through the model in {unpaced:.1f}s, "
            f"{frames / unpaced:.1f} fps against the source's {rate:g} ---"
        )
        found = detections(source, project, project / "detections.ndjson")
        print(f"--- yolo26 rows over the source: {dict(found.most_common())} ---")
        assert found["person"] > 0, (
            f"yolo26 found no people in {args.seconds}s from {args.offset}s into "
            f"{args.clip}; there is no AI stage to prove"
        )

        for attempt in range(1, args.runs + 1):
            print(f"=== run {attempt} of {args.runs} ===")
            started = time.monotonic()
            one_run(
                attempt, project, source, matte, rate, width, height, frames,
                sub_wasm, certgen,
            )
            print(f"=== run {attempt} took {time.monotonic() - started:.1f}s ===")
    finally:
        if args.keep:
            print(f"kept: {project}")
        else:
            shutil.rmtree(project, ignore_errors=True)


if __name__ == "__main__":
    main()
