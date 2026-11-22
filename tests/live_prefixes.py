"""The prefixes a host holds a run to, against a real relay - run it by hand.

    python tests/live_prefixes.py [--no-build] [--keep]

A host running other people's queries on relays that check nothing sets
FFRWD_MODULE_MOQ_PREFIXES in every module's environment. The prefix-probe
guest, under wasmtime with that variable in its environment, makes the
same `moq_core::prefixes` calls `publish` and `subscribe` make, against a
local moq-relay logging what it announces to each session:

- a publish inside the prefixes goes out, with the prefix spelled in the
  broadcast, in the relay URL's path or in the token;
- a publish and a subscribe outside them are refused, and nothing is
  dialed: the relay URL's path and the token count toward the name, a
  prefix matches whole segments, and a `..` segment is refused;
- a subscriber held to prefixes asks the relay to announce only what is
  under them, relative to the root its session opens at, and the relay's
  log shows it announced nothing else to that session. A subscriber held
  to nothing is told of every broadcast, which is what the prefixes stop.

The modules see the variable only under a host that passes FFRWD_MODULE_*
variables into their environment. The guest makes the same calls with
the variable set by wasmtime, so this loop needs no such host.

Never collected by any suite: it needs moq-relay, wasmtime, cargo with
the wasm32-wasip2 target and a wasi-sdk clang, and it opens UDP sockets.
Toolchain overrides: WASMTIME, MOQ_RELAY, WASI_SDK_PATH (or
CC_wasm32_wasip2 directly), and CARGO_TARGET_DIR is where the builds are
looked for.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from common import (
    EXE,
    PACKAGE,
    free_udp_port,
    kill_tree,
    run,
    spawn,
    start_relay,
    tool,
    wasi_cc_env,
)

VARIABLE = "FFRWD_MODULE_MOQ_PREFIXES"
HELD = "jobs/abc"

# Every broadcast on the relay while the subscribers look: the path the
# relay sees, then how it is published (the URL's path, the token, the
# broadcast argument) and the prefixes its publisher is held to.
PUBLISHED = {
    "jobs/abc/out": ("", "", "jobs/abc/out", HELD),
    "jobs/abc/url": ("/jobs", "", "abc/url", HELD),
    "jobs/abc/token": ("", "jobs/abc", "token", HELD),
    "jobs/xyz/out": ("", "", "jobs/xyz/out", "jobs/xyz"),
    "jobs/abcd/out": ("", "", "jobs/abcd/out", "jobs/abcd"),
}
UNDER_HELD = {path for path in PUBLISHED if path.startswith(HELD + "/")}

# Long enough for every refusal and every subscriber to run while the
# publishers are up; they are stopped once the subscribers are done.
PUBLISH_SECONDS = 120
LISTEN_SECONDS = 3

BUILD_DEADLINE = 600
PROBE_DEADLINE = 60
READY_DEADLINE = 30

ANNOUNCED = re.compile(r"conn\{id=(\d+)\}: moq_net::lite::publisher: announce broadcast=(\S+)")
ACCEPTED = re.compile(r"conn\{id=(\d+)\}: moq_relay::connection: session accepted .*role=(\S+) .*root=(\S*)")
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def target_dir() -> Path:
    named = os.environ.get("CARGO_TARGET_DIR")
    return Path(named) if named else PACKAGE / "target"


def build() -> tuple[Path, Path]:
    """The probe guest and the certificate helper, built release."""
    env = wasi_cc_env()
    done = run(["cargo", "build", "--target", "wasm32-wasip2", "--release", "-p", "prefix-probe"],
               BUILD_DEADLINE, cwd=PACKAGE, env=env)
    if done.returncode != 0:
        sys.exit("the probe's wasm build failed")
    done = run(["cargo", "build", "--release", "--manifest-path",
                PACKAGE / "harness" / "certgen" / "Cargo.toml"], BUILD_DEADLINE, cwd=PACKAGE)
    if done.returncode != 0:
        sys.exit("the certgen build failed")
    return built()


def built() -> tuple[Path, Path]:
    probe = target_dir() / "wasm32-wasip2" / "release" / "prefix-probe.wasm"
    certgen = (target_dir() if os.environ.get("CARGO_TARGET_DIR")
               else PACKAGE / "harness" / "certgen" / "target") / "release" / f"certgen{EXE}"
    for path in (probe, certgen):
        if not path.exists():
            sys.exit(f"no {path}; build it first, or leave out --no-build")
    return probe, certgen


class Relay:
    """The relay under test, and its log read by line."""

    def __init__(self, work: Path, cert_hex: str) -> None:
        self.port = free_udp_port()
        self.cert_hex = cert_hex
        self.log = work / "relay.log"
        self.child = start_relay(self.port, work / "cert.pem", work / "key.pem", self.log,
                                 ["--log-level", "debug"])

    def url(self, path: str = "") -> str:
        return f"moqt://127.0.0.1:{self.port}{path}"

    def lines(self) -> list[str]:
        return ANSI.sub("", self.log.read_text(errors="replace")).splitlines()


def probe_argv(probe: Path, relay: Relay, mode: str, *, path: str = "", token: str = "",
               broadcast: str, prefixes: str | None, seconds: float) -> list[str]:
    argv = [tool("WASMTIME", "wasmtime"), "run", "-S", "inherit-network", "-S", "http",
            "--env", f"MODE={mode}",
            "--env", f"RELAY={relay.url(path)}",
            "--env", f"RELAY_CERT_HEX={relay.cert_hex}",
            "--env", f"BROADCAST={broadcast}",
            "--env", f"SECONDS={seconds}"]
    if token:
        argv += ["--env", f"TOKEN={token}"]
    if prefixes is not None:
        argv += ["--env", f"{VARIABLE}={prefixes}"]
    return [*argv, str(probe)]


def probe_once(probe: Path, relay: Relay, mode: str, **how) -> tuple[int, str]:
    done = run(probe_argv(probe, relay, mode, seconds=LISTEN_SECONDS, **how),
               PROBE_DEADLINE, capture_output=True, text=True)
    return done.returncode, done.stdout + done.stderr


def start_publishers(probe: Path, relay: Relay, work: Path) -> list[tuple[subprocess.Popen, Path]]:
    started = []
    for name, (path, token, broadcast, prefixes) in PUBLISHED.items():
        out = work / f"publish-{name.replace('/', '_')}.log"
        argv = probe_argv(probe, relay, "publish", path=path, token=token, broadcast=broadcast,
                          prefixes=prefixes, seconds=PUBLISH_SECONDS)
        print(f"+ publisher {name}: {broadcast} under {relay.url(path)}"
              f"{' token ' + token if token else ''}, held to {prefixes}", flush=True)
        child = spawn(argv, stdout=open(out, "w"), stderr=subprocess.STDOUT)
        started.append((child, out))
    deadline = time.monotonic() + READY_DEADLINE
    for child, out in started:
        while "probe: publishing" not in out.read_text(errors="replace"):
            if child.poll() is not None or time.monotonic() > deadline:
                sys.exit(f"a publisher inside its prefixes did not publish:\n{out.read_text()}")
            time.sleep(0.2)
    # Every publish session in, and each broadcast announced to the relay.
    time.sleep(1.0)
    return started


def check_refusals(probe: Path, relay: Relay) -> None:
    """Each one refused before it dials: no session reaches the relay."""
    cases = [
        ("publish", dict(broadcast="jobs/xyz/out"), "'jobs/xyz/out'"),
        ("publish", dict(path="/jobs/xyz", broadcast="jobs/abc/out"), "'jobs/xyz/jobs/abc/out'"),
        ("publish", dict(token="jobs/xyz", broadcast="out"), "'<token 8 characters>/out'"),
        ("publish", dict(broadcast="jobs/abcd/out"), "'jobs/abcd/out'"),
        ("publish", dict(broadcast="jobs/abc/../xyz/out"), "'..'"),
        ("subscribe", dict(broadcast="jobs/xyz/out"), "'jobs/xyz/out'"),
        ("subscribe", dict(path="/jobs/xyz", broadcast="out"), "'jobs/xyz/out'"),
        ("subscribe", dict(path="/jobs", broadcast="abcd/out"), "'jobs/abcd/out'"),
    ]
    before = len(relay.lines())
    for mode, how, named in cases:
        code, said = probe_once(probe, relay, mode, prefixes=HELD, **how)
        print(f"--- {mode} {how}: exit {code}\n{said.strip()}")
        assert code != 0 and "probe: refused:" in said, f"{mode} {how} was not refused"
        assert named in said, f"the refusal does not name {named}"
        assert f"'{HELD}'" in said and VARIABLE in said, "the refusal does not say what is allowed"
    code, said = probe_once(probe, relay, "publish", broadcast="jobs/abc/out", prefixes="")
    print(f"--- publish under an empty list: exit {code}\n{said.strip()}")
    assert code != 0 and "it was given none" in said, "an empty list did not refuse everything"
    time.sleep(0.5)
    dialed = [line for line in relay.lines()[before:] if ACCEPTED.search(line)]
    assert not dialed, f"a refused probe reached the relay:\n{dialed}"
    print("PASS: every name outside the prefixes was refused before a session opened")


def check_announces(probe: Path, relay: Relay, label: str, *, path: str = "", token: str = "",
                    broadcast: str, prefixes: str | None, asks: list[str], told: set[str],
                    root: str) -> None:
    """One subscriber: what it asks the relay for, what it is told, and
    what the relay's log says it announced to that session, in full paths."""
    before = len(relay.lines())
    code, said = probe_once(probe, relay, "subscribe", path=path, token=token,
                            broadcast=broadcast, prefixes=prefixes)
    print(f"--- {label}: exit {code}\n{said.strip()}")
    assert code == 0 and "probe: done" in said, f"{label}: the subscriber failed"
    asked = re.findall(r"probe: asks for '([^']*)'", said)
    assert sorted(asked) == sorted(asks), f"{label}: asked for {asked}, not {asks}"
    heard = set(re.findall(r"probe: announced '([^']*)'", said))
    relative = {name.removeprefix(root + "/") if root else name for name in told}
    assert heard == relative, f"{label}: told of {sorted(heard)}, not {sorted(relative)}"
    time.sleep(0.5)
    window = relay.lines()[before:]
    sessions = [ACCEPTED.search(line).groups() for line in window if ACCEPTED.search(line)]
    assert len(sessions) == 1, f"{label}: {len(sessions)} sessions in the window"
    conn, role, opened_at = sessions[0]
    assert opened_at == root, f"{label}: the relay rooted the session at '{opened_at}', not '{root}'"
    sent = {name for (on, name) in (ANNOUNCED.search(line).groups()
                                    for line in window if ANNOUNCED.search(line)) if on == conn}
    assert sent == told, f"{label}: the relay announced {sorted(sent)}, not {sorted(told)}"
    print(f"PASS: {label}: the relay announced {sorted(sent)} to session {conn} and nothing else")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-build", dest="build", action="store_false")
    parser.add_argument("--keep", action="store_true", help="keep the work directory")
    args = parser.parse_args()
    probe, certgen = build() if args.build else built()

    work = Path(tempfile.mkdtemp(prefix="moq-prefixes-"))
    relay: Relay | None = None
    publishers: list[tuple[subprocess.Popen, Path]] = []
    try:
        made = run([certgen, work], 60, capture_output=True, text=True)
        if made.returncode != 0:
            sys.exit(f"certgen failed:\n{made.stderr[-400:]}")
        relay = Relay(work, made.stdout.strip())
        publishers = start_publishers(probe, relay, work)
        print("PASS: a publish inside the prefixes went out, spelled in the broadcast, "
              "the URL's path and the token")

        check_refusals(probe, relay)

        everything = set(PUBLISHED)
        check_announces(probe, relay, "held to nothing", broadcast="jobs/abc/out",
                        prefixes=None, asks=[""], told=everything, root="")
        check_announces(probe, relay, "held at the relay's root", broadcast="jobs/abc/out",
                        prefixes=HELD, asks=[HELD], told=UNDER_HELD, root="")
        check_announces(probe, relay, "rooted above the prefix by the URL", path="/jobs",
                        broadcast="abc/out", prefixes=HELD, asks=["abc"], told=UNDER_HELD,
                        root="jobs")
        check_announces(probe, relay, "rooted above the prefix by the token", token="jobs",
                        broadcast="abc/out", prefixes=HELD, asks=["abc"], told=UNDER_HELD,
                        root="jobs")
        check_announces(probe, relay, "rooted at the prefix", path="/jobs/abc",
                        broadcast="out", prefixes=f"{HELD}/", asks=[""], told=UNDER_HELD,
                        root="jobs/abc")
        check_announces(probe, relay, "held to two prefixes", broadcast="jobs/xyz/out",
                        prefixes=f"{HELD}, jobs/xyz", asks=[HELD, "jobs/xyz"],
                        told=UNDER_HELD | {"jobs/xyz/out"}, root="")

        for child, out in publishers:
            if child.poll() is not None:
                sys.exit(f"a publisher ended early:\n{out.read_text()}")
        print("=== PASS: the prefixes held ===")
    finally:
        for child, _ in publishers:
            kill_tree(child)
        if relay is not None:
            kill_tree(relay.child)
        if args.keep:
            print(f"kept: {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
