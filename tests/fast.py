"""The fast tier: what to run after a change, in about five minutes.

    python tests/fast.py [--no-build]

The unit tests (`cargo test`, the hold's among them), then the short forms
of the two live loops that fail on what subscribe's latency depends on:
`live_latency.py` (25 s at 60 fps, fails when packets leave in batches)
and `live_chain.py` (30 s of a subscribe, encode and publish chain, fails
when the chain breaks or subscribe falls behind a native reader). Each
loop's `--long` form, and the other loops, are for before a push.

It stops at the first failure and says which.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time

from common import PACKAGE


def step(name: str, argv: list[str], cwd) -> None:
    began = time.monotonic()
    print(f"== {name}", flush=True)
    done = subprocess.run(argv, cwd=cwd)
    took = time.monotonic() - began
    if done.returncode != 0:
        sys.exit(f"FAIL {name} ({took:.0f}s)")
    print(f"ok {name} ({took:.0f}s)", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-build", dest="build", action="store_false")
    args = parser.parse_args()
    step("cargo test", ["cargo", "test", "--workspace", "-q"], PACKAGE)
    build = [] if args.build else ["--no-build"]
    tests = PACKAGE / "tests"
    step("live_latency", [sys.executable, "live_latency.py", *build], tests)
    step("live_chain", [sys.executable, "live_chain.py", "--no-build"], tests)
    print("=== PASS: the fast tier ===")


if __name__ == "__main__":
    main()
