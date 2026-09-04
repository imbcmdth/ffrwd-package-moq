"""What the loops beside this file share: the toolchain, and processes that die.

Every subprocess runs under a hard deadline, and an expired deadline kills the
whole process tree - `taskkill /T` on Windows, the process group elsewhere -
because a killed direct child (uv, say) leaves its ffmpeg and sidecar holding
the output pipes, and a harness that then waits for pipe EOF waits forever.

Toolchain overrides: WASMTIME, MOQ_RELAY, WASI_SDK_PATH (or CC_wasm32_wasip2
directly), FFRWD_WASM for the sidecar binary, FFRWD_REPO for the checkout
holding the compiler and the sidecar.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

PACKAGE = Path(__file__).resolve().parent.parent


def _compiler_repo() -> Path:
    """The checkout holding the compiler and the sidecar.

    FFRWD_REPO names it outright. Otherwise: the repo this package sits
    inside, for a checkout that vendors it under packages/, and the sibling
    `ffrwd-cli` clone for one that does not.
    """
    named = os.environ.get("FFRWD_REPO")
    if named is not None:
        return Path(named)
    vendored = PACKAGE.parent.parent.parent
    if (vendored / "cli").is_dir():
        return vendored
    return PACKAGE.parent / "ffrwd-cli"


REPO = _compiler_repo()
CLI = REPO / "cli"
SIDECAR = Path(
    os.environ.get(
        "FFRWD_WASM",
        REPO / "sidecar" / "target" / "release"
        / ("ffrwd-wasm.exe" if os.name == "nt" else "ffrwd-wasm"),
    )
)

EXE = ".exe" if os.name == "nt" else ""


def tool(env_name: str, default: str) -> str:
    return os.environ.get(env_name, default)


def wasi_cc_env() -> dict[str, str]:
    """CC/AR for ring's C, derived from WASI_SDK_PATH unless already set."""
    env = dict(os.environ)
    if "CC_wasm32_wasip2" in env:
        return env
    sdk = env.get("WASI_SDK_PATH")
    if sdk is None:
        sys.exit("set WASI_SDK_PATH (or CC_wasm32_wasip2) so ring's C can build for wasm")
    env["CC_wasm32_wasip2"] = str(Path(sdk) / "bin" / f"clang{EXE}")
    env["AR_wasm32_wasip2"] = str(Path(sdk) / "bin" / f"ar{EXE}")
    return env


def spawn(argv: list[str], **kwargs) -> subprocess.Popen:
    """Starts a child in its own process group, so a kill reaches the tree."""
    if os.name != "nt":
        kwargs.setdefault("start_new_session", True)
    return subprocess.Popen([str(a) for a in argv], **kwargs)


def kill_tree(child: subprocess.Popen) -> None:
    """Kills the child and every descendant still holding its pipes."""
    if child.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(child.pid)],
            capture_output=True, timeout=30,
        )
    else:
        import signal

        try:
            os.killpg(os.getpgid(child.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
    try:
        child.wait(timeout=30)
    except subprocess.TimeoutExpired:
        child.kill()


def run(argv: list[str], deadline: int, **kwargs) -> subprocess.CompletedProcess:
    """subprocess.run with a deadline that kills the whole tree."""
    print("+", " ".join(str(a) for a in argv), flush=True)
    if kwargs.pop("capture_output", False):
        kwargs["stdout"] = subprocess.PIPE
        kwargs["stderr"] = subprocess.PIPE
    child = spawn(argv, **kwargs)
    try:
        stdout, stderr = child.communicate(timeout=deadline)
    except subprocess.TimeoutExpired:
        kill_tree(child)
        stdout, stderr = child.communicate()
        shown = stderr or stdout or ""
        if isinstance(shown, bytes):
            shown = shown.decode(errors="replace")
        sys.exit(f"deadline ({deadline}s) expired: {argv[0]}\n{shown[-1200:]}")
    return subprocess.CompletedProcess(argv, child.returncode, stdout, stderr)


def free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as held:
        held.bind(("127.0.0.1", 0))
        return held.getsockname()[1]


def start_relay(port: int, cert: Path, key: Path, log: Path) -> subprocess.Popen:
    relay = tool("MOQ_RELAY", "moq-relay")
    child = spawn(
        [relay, "--server-bind", f"127.0.0.1:{port}",
         "--tls-cert", str(cert), "--tls-key", str(key),
         "--auth-public", ""],
        stdout=subprocess.DEVNULL, stderr=open(log, "w"),
    )
    time.sleep(1.5)
    if child.poll() is not None:
        sys.exit(f"moq-relay exited early; its log:\n{log.read_text()[-800:]}")
    return child


def start_subscriber(
    wasm: Path,
    port: int,
    cert_hex: str,
    out_dir: Path,
    broadcast: str,
    track: str | None = None,
    *,
    rendition: int = 0,
    output: str = "recv.mp4",
) -> subprocess.Popen:
    """One sub-recv guest against `broadcast`.

    `track` names the media track outright; left None the subscriber reads
    the catalog and takes its `rendition`-th track, which is what a player
    choosing off a ladder does.
    """
    wasmtime = tool("WASMTIME", "wasmtime")
    argv = [wasmtime, "run", "-S", "inherit-network",
            "--dir", f"{out_dir}::/out",
            "--env", f"RELAY_PORT={port}",
            "--env", f"RELAY_CERT_HEX={cert_hex}",
            "--env", f"BROADCAST={broadcast}",
            "--env", f"RENDITION={rendition}",
            "--env", f"OUTPUT=/out/{output}"]
    if track is not None:
        argv += ["--env", f"TRACK={track}"]
    return spawn(
        [*argv, str(wasm)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )


def wait_subscriber(sub: subprocess.Popen, deadline: int) -> str:
    try:
        stdout, stderr = sub.communicate(timeout=deadline)
    except subprocess.TimeoutExpired:
        kill_tree(sub)
        stdout, stderr = sub.communicate()
        sys.exit(f"sub-recv did not finish:\n{stdout}\n{stderr[-800:]}")
    print("--- sub-recv transcript ---")
    print(stdout)
    if stderr.strip():
        print("--- sub-recv stderr ---")
        print(stderr[-1200:])
    if sub.returncode != 0 or "sub: PASS" not in stdout:
        sys.exit("sub-recv did not pass")
    return stdout


def build_guests(deadline: int) -> tuple[Path, Path, Path]:
    """The two wasm guests and the certificate helper, built release."""
    env = wasi_cc_env()
    done = run(
        ["cargo", "build", "--target", "wasm32-wasip2", "--release"],
        deadline, cwd=PACKAGE, env=env,
    )
    if done.returncode != 0:
        sys.exit("the wasm build failed")
    done = run(
        ["cargo", "build", "--release", "--manifest-path",
         PACKAGE / "harness" / "certgen" / "Cargo.toml"],
        deadline, cwd=PACKAGE,
    )
    if done.returncode != 0:
        sys.exit("the certgen build failed")
    release = PACKAGE / "target" / "wasm32-wasip2" / "release"
    certgen = PACKAGE / "harness" / "certgen" / "target" / "release" / f"certgen{EXE}"
    return release / "publish.wasm", release / "sub-recv.wasm", certgen


def guest(name: str) -> Path:
    """One wasm guest by name, where `build_guests` leaves it."""
    return PACKAGE / "target" / "wasm32-wasip2" / "release" / f"{name}.wasm"


def build_hang_recv(deadline: int) -> Path:
    """The third-party subscriber, built from the pinned hang stack."""
    done = run(
        ["cargo", "build", "--release", "--manifest-path",
         PACKAGE / "harness" / "hang-recv" / "Cargo.toml"],
        deadline, cwd=PACKAGE,
    )
    if done.returncode != 0:
        sys.exit("the hang-recv build failed")
    return PACKAGE / "harness" / "hang-recv" / "target" / "release" / f"hang-recv{EXE}"


def start_hang_recv(
    exe: Path,
    port: int,
    cert_pem: Path,
    out_path: Path,
    broadcast: str,
    *,
    video: str | None = None,
    audio: str | None = None,
) -> subprocess.Popen:
    """hang's own stack against the broadcast: their catalog parser, their
    CMAF depacketizer, their fmp4 writer. See harness/hang-recv."""
    env = dict(os.environ)
    env["RELAY_PORT"] = str(port)
    env["RELAY_CERT_PEM"] = str(cert_pem)
    env["BROADCAST"] = broadcast
    env["OUTPUT"] = str(out_path)
    for name in ("VIDEO", "AUDIO"):
        env.pop(name, None)
    if video is not None:
        env["VIDEO"] = video
    if audio is not None:
        env["AUDIO"] = audio
    return spawn(
        [str(exe)], env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )


def wait_hang_recv(sub: subprocess.Popen, deadline: int) -> str:
    try:
        stdout, stderr = sub.communicate(timeout=deadline)
    except subprocess.TimeoutExpired:
        kill_tree(sub)
        stdout, stderr = sub.communicate()
        sys.exit(f"hang-recv did not finish:\n{stdout}\n{stderr[-800:]}")
    print("--- hang-recv transcript ---")
    print(stdout)
    if stderr.strip():
        print("--- hang-recv stderr ---")
        print(stderr[-1200:])
    if sub.returncode != 0 or "hang: PASS" not in stdout:
        sys.exit("hang-recv did not pass")
    return stdout


def ffprobe_frames(path: Path, deadline: int) -> int:
    done = run([
        "ffprobe", "-v", "error", "-count_frames", "-select_streams", "v:0",
        "-show_entries", "stream=nb_read_frames,avg_frame_rate",
        "-of", "default=nw=1", str(path),
    ], deadline, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"ffprobe rejected the reassembly:\n{done.stderr[-800:]}")
    print("--- ffprobe ---")
    print(done.stdout.strip())
    for line in done.stdout.splitlines():
        if line.startswith("nb_read_frames="):
            return int(line.split("=", 1)[1])
    sys.exit("ffprobe counted no frames")
