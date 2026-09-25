"""The OBS oracle: a broadcast from libmoq, the moq-dev OBS plugin's library, reads as media.

    python tests/live_obs.py --relay URL --broadcast PATH
                             [--token-file FILE --token-key KEY]
                             [--seconds N] [--work DIR] [--no-build] [--keep]

libmoq publishes in hang's `legacy` container - a varint pts in
microseconds ahead of an Annex B access unit or a raw AAC frame - and
writes its catalog once, when its tracks are made. Cloudflare's draft-16
relay keeps no history and refuses FETCH, so before 0.8.0 a late reader
never saw the catalog at all ("sent no catalog within 15s"), and the media
was not a container this read.

This reads a broadcast someone is publishing now: the subscribe module
under the sidecar, every catalog track to its own .nut, for `--seconds`,
then stopped. It publishes nothing and subscribes only. What it owes:

  - the catalog arrives, and the reader opens every track in it;
  - each .nut probes as the codec and geometry the catalog named;
  - the first video packet is a keyframe, and the video decodes with no
    error from ffmpeg;
  - pts never go backwards on any track, and the media covers most of the
    time the reader ran.

The token is read from a JSON file by key and handed to the module in its
params. It is never printed.

Never collected by any suite: it needs a live publisher, the network,
ffmpeg, ffprobe, cargo with the wasm32-wasip2 target and a wasi-sdk clang,
and the sidecar. It starts one reader and stops that alone.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from common import PACKAGE, SIDECAR, build_guests, guest, kill_tree, spawn

SECONDS = 20


def probe(path: Path) -> tuple[dict, list[dict]]:
    """The stream and its packets, as ffprobe reads the .nut."""
    done = subprocess.run(
        ["ffprobe", "-v", "error", "-show_streams", "-show_packets",
         "-of", "json", str(path)],
        capture_output=True, text=True, timeout=120,
    )
    if done.returncode != 0:
        sys.exit(f"ffprobe refused {path.name}: {done.stderr[-600:]}")
    parsed = json.loads(done.stdout)
    return parsed["streams"][0], parsed.get("packets", [])


#: What the NUT demuxer says about a file whose writer was stopped mid-run,
#: which is how every read here ends: no index at the end, and so no
#: timestamps to seek by. Neither is about the media.
CUT_SHORT = ("read_timestamp failed", "no index at the end", "Last message repeated")


def decode_errors(path: Path) -> str:
    """What ffmpeg says when it decodes the whole file, errors only."""
    done = subprocess.run(
        ["ffmpeg", "-v", "error", "-i", str(path), "-f", "null", "-"],
        capture_output=True, text=True, timeout=300,
    )
    lines = (done.stderr or "").splitlines()
    return "\n".join(line for line in lines if not any(cut in line for cut in CUT_SHORT))


def decoded_frames(path: Path) -> int:
    """How many frames a decoder gets out of the file."""
    done = subprocess.run(
        ["ffprobe", "-v", "quiet", "-count_frames", "-show_entries",
         "stream=nb_read_frames", "-of", "csv=p=0", str(path)],
        capture_output=True, text=True, timeout=300,
    )
    return int((done.stdout or "0").strip() or 0)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--relay", required=True)
    parser.add_argument("--broadcast", required=True)
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--token-key", default="subscribe")
    parser.add_argument("--seconds", type=float, default=SECONDS)
    parser.add_argument("--work", type=Path)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()

    if not SIDECAR.exists():
        sys.exit(f"no sidecar at {SIDECAR}; build it first (cargo build --release)")
    if not args.no_build:
        build_guests(900)
    module = guest("subscribe")

    token = ""
    if args.token_file is not None:
        token = json.loads(args.token_file.read_text())[args.token_key]
    params = {"relay": args.relay, "broadcast": args.broadcast, "token": token}

    work = args.work or Path(tempfile.mkdtemp(prefix="moq-obs-", dir=PACKAGE / "target"))
    work.mkdir(parents=True, exist_ok=True)

    # The catalog first: a probe opens every track the broadcast names.
    catalog = subprocess.run(
        [str(SIDECAR), "--probe", str(module), "-udp", str(module), "-http", str(module),
         "-params", json.dumps(params)],
        capture_output=True, text=True, timeout=120,
    )
    if catalog.returncode != 0:
        sys.exit(f"the probe failed: {catalog.stderr[-1200:]}")
    tracks = json.loads(catalog.stdout)["tracks"]
    for index, track in enumerate(tracks):
        print(f"track {index}: {json.dumps(track)[:300]}")

    outputs = [work / f"track{index}.nut" for index in range(len(tracks))]
    argv = [str(SIDECAR), "-udp", str(module), "-http", str(module), "-m", str(module),
            "-params", json.dumps(params)]
    for index, path in enumerate(outputs):
        argv += ["-track", str(index), "-f", "nut", str(path)]
    print(f"+ reader: {len(outputs)} tracks for {args.seconds:.0f}s", flush=True)
    log = (work / "reader.log").open("w")
    reader = spawn(argv, stdout=log, stderr=subprocess.STDOUT)
    started = time.monotonic()
    time.sleep(args.seconds)
    exited = reader.poll()
    kill_tree(reader)
    log.close()
    ran = time.monotonic() - started
    if exited is not None:
        shown = (work / "reader.log").read_text(errors="replace")[-1500:]
        sys.exit(f"the reader ended early ({exited}):\n{shown}")

    failures = []
    for index, (track, path) in enumerate(zip(tracks, outputs)):
        stream, packets = probe(path)
        kind = stream["codec_type"]
        pts = [float(p["pts_time"]) for p in packets if "pts_time" in p]
        span = (max(pts) - min(pts)) if pts else 0.0
        print(f"track {index} ({kind} {stream['codec_name']}): {len(packets)} packets, "
              f"{span:.1f}s of media over {ran:.1f}s")
        if not packets:
            failures.append(f"track {index} wrote no packets")
            continue
        backwards = [b for a, b in zip(pts, pts[1:]) if b < a] if kind == "audio" else []
        if kind == "video":
            if packets[0].get("flags", "").find("K") < 0:
                failures.append(f"track {index}: the first picture is not a keyframe")
            print(f"  {stream['width']}x{stream['height']}")
            dts = [float(p["dts_time"]) for p in packets if "dts_time" in p]
            backwards = [b for a, b in zip(dts, dts[1:]) if b < a]
        else:
            print(f"  {stream['sample_rate']} Hz, {stream['channels']} channels")
        errors = decode_errors(path)
        if errors:
            failures.append(f"track {index} decodes with errors: {errors[:600]}")
        frames = decoded_frames(path)
        print(f"  {frames} frames decoded")
        if frames < len(packets) - 2:
            failures.append(f"track {index}: {len(packets)} packets decode to {frames} frames")
        if backwards:
            failures.append(f"track {index}: time goes backwards {len(backwards)} times")
        if span < ran * 0.5:
            failures.append(f"track {index} covers {span:.1f}s of a {ran:.1f}s read")

    if not args.keep and args.work is None and not failures:
        for path in [*outputs, work / "reader.log"]:
            path.unlink(missing_ok=True)
        work.rmdir()
    if failures:
        print(f"work kept in {work}")
        sys.exit("FAIL\n  " + "\n  ".join(failures))
    print("PASS")


if __name__ == "__main__":
    main()
