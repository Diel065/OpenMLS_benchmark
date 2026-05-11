from __future__ import annotations

import json
import re
import shlex
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class DeviceConfig:
    id: str
    enabled: bool
    kind: str

    connection: dict
    transport: dict
    target: dict
    worker: dict
    metadata: dict = field(default_factory=dict)


@dataclass
class WorkerLaunch:
    worker_id: str
    binary_path: str
    kr_url: str
    relay_url: str
    listen_addr: str
    run_id: str
    scenario: str
    profile_path_template: str
    remote_results_root: str
    remote_tmp: str
    node_name: str = ""


# ---------------------------------------------------------------------------
# Run ID validation (mirrors the Rust validate_run_id)
# ---------------------------------------------------------------------------

RUN_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def validate_run_id(run_id: str) -> None:
    if not run_id:
        raise ValueError("Run ID must not be empty")
    if run_id in ("/", ".", ".."):
        raise ValueError(f"Run ID must not be '{run_id}'")
    if "/" in run_id:
        raise ValueError("Run ID must not contain '/'")
    if not RUN_ID_RE.match(run_id):
        raise ValueError(
            f"Run ID must only contain [A-Za-z0-9._-], got '{run_id}'"
        )


# ---------------------------------------------------------------------------
# YAML loader
# ---------------------------------------------------------------------------


def load_devices_config(path: Path) -> list[DeviceConfig]:
    try:
        import yaml
    except ImportError:
        raise RuntimeError(
            "PyYAML is required to parse device config. "
            "Install it with: pip install pyyaml"
        )

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not raw or "devices" not in raw:
        raise ValueError(f"No 'devices' key found in {path}")

    devices: list[DeviceConfig] = []
    for entry in raw["devices"]:
        devices.append(DeviceConfig(
            id=entry["id"],
            enabled=bool(entry.get("enabled", True)),
            kind=entry.get("kind", ""),
            connection=entry.get("connection", {}),
            transport=entry.get("transport", {}),
            target=entry.get("target", {}),
            worker=entry.get("worker", {}),
            metadata=entry.get("metadata", {}),
        ))
    return devices


# ---------------------------------------------------------------------------
# Abstract backend
# ---------------------------------------------------------------------------


class DeviceBackend:
    def check_reachable(self) -> None:
        raise NotImplementedError

    def shell(self, command: str, check: bool = True) -> subprocess.CompletedProcess:
        raise NotImplementedError

    def push(self, local: Path, remote: str) -> None:
        raise NotImplementedError

    def pull(self, remote: str, local: Path) -> None:
        raise NotImplementedError

    def wipe_for_run(self, run_id: str) -> None:
        validate_run_id(run_id)
        self._do_wipe(run_id)

    def _do_wipe(self, safe_run_id: str) -> None:
        raise NotImplementedError

    def install_worker(self, local_binary: Path, remote_binary: str) -> None:
        raise NotImplementedError

    def start_worker(self, launch: WorkerLaunch) -> None:
        raise NotImplementedError

    def stop_worker(self) -> None:
        raise NotImplementedError

    def wait_health(self, url: str, timeout_s: float = 30.0) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    body = resp.read().decode("utf-8", errors="replace").strip()
                    if 200 <= resp.status < 300 and body == "ok":
                        return
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                pass
            time.sleep(0.5)
        raise RuntimeError(f"Timed out waiting for health endpoint: {url}")

    def read_epoch_seconds(self) -> int:
        result = self.shell("date -u +%s", check=True)
        value = result.stdout.strip().splitlines()[-1].strip()
        try:
            return int(value)
        except ValueError as exc:
            raise RuntimeError(f"Could not parse device UTC epoch from: {value!r}") from exc

    def set_epoch_seconds(self, epoch_seconds: int) -> None:
        self.shell(f"date -u -s @{int(epoch_seconds)}", check=True)

    def ensure_clock_synchronized(self, max_skew_seconds: int = 300) -> None:
        host_epoch = int(time.time())
        before = self.read_epoch_seconds()
        skew = abs(host_epoch - before)
        if skew <= max_skew_seconds:
            print(f"[device] clock skew ok ({skew}s)", flush=True)
            return

        print(
            f"[device] clock skew {skew}s exceeds {max_skew_seconds}s; syncing device clock",
            flush=True,
        )
        self.set_epoch_seconds(host_epoch)
        after = self.read_epoch_seconds()
        remaining_skew = abs(int(time.time()) - after)
        if remaining_skew > max_skew_seconds:
            raise RuntimeError(
                "Device clock remains out of sync after setting UTC time "
                f"(skew={remaining_skew}s, max={max_skew_seconds}s)."
            )
        print(f"[device] clock synchronized (skew {remaining_skew}s)", flush=True)


# ---------------------------------------------------------------------------
# ADB backend
# ---------------------------------------------------------------------------


class AdbDeviceBackend(DeviceBackend):
    def __init__(self, config: DeviceConfig, serial: str | None = None):
        self.config = config
        self.serial = serial or config.connection.get("serial", "")
        self._adb_base = ["adb"]
        if self.serial:
            self._adb_base += ["-s", self.serial]

    def _adb(self, args: list[str], check: bool = True, timeout: int = 60) -> subprocess.CompletedProcess:
        cmd = self._adb_base + args
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=check,
            timeout=timeout,
        )

    def check_reachable(self) -> None:
        result = self._adb(["devices", "-l"], check=False)
        if self.serial and self.serial not in result.stdout:
            raise RuntimeError(
                f"ADB device '{self.serial}' not found in 'adb devices -l' output"
            )
        info = self._adb(["shell", "hostname; whoami; uname -a; uname -m"])
        print(f"[adb] device info:\n{info.stdout}")

    def shell(self, command: str, check: bool = True) -> subprocess.CompletedProcess:
        return self._adb(["shell", command], check=check, timeout=120)

    def push(self, local: Path, remote: str) -> None:
        self._adb(["push", str(local), remote])

    def pull(self, remote: str, local: Path) -> None:
        local.parent.mkdir(parents=True, exist_ok=True)
        self._adb(["pull", remote, str(local)])

    def _do_wipe(self, safe_run_id: str) -> None:
        cmds = (
            f"set -e; "
            f"killall worker 2>/dev/null || pkill worker 2>/dev/null || true; "
            f"rm -rf /results/signal/{shlex.quote(safe_run_id)}; "
            f"mkdir -p /results/signal/{shlex.quote(safe_run_id)}; "
            f"rm -rf /tmp/signal-benchmark; "
            f"mkdir -p /tmp/signal-benchmark"
        )
        self._adb(["shell", cmds])

    def install_worker(self, local_binary: Path, remote_binary: str) -> None:
        self.push(local_binary, remote_binary)
        self._adb(["shell", f"chmod +x {shlex.quote(remote_binary)}"])

    def start_worker(self, launch: WorkerLaunch) -> None:
        profile_flag = ""
        env_args = ""
        if launch.profile_path_template:
            profile_path = launch.profile_path_template.replace("{participant_id}", launch.worker_id)
            env = {
                "SIGNAL_PROFILE_ENABLED": "true",
                "SIGNAL_PROFILE_PARTICIPANT_IDS": launch.worker_id,
                "SIGNAL_PROFILE_PATH": profile_path,
                "SIGNAL_PROFILE_PATH_TEMPLATE": launch.profile_path_template,
                "SIGNAL_PROFILE_RUN_ID": launch.run_id,
                "SIGNAL_PROFILE_SCENARIO": launch.scenario,
            }
            if launch.node_name:
                env["SIGNAL_PROFILE_NODE"] = launch.node_name
            env_args = " ".join(
                f"{key}={shlex.quote(value)}" for key, value in env.items()
            )
            profile_flag = (
                f" --profile-path-template {shlex.quote(launch.profile_path_template)}"
                f" --profile-enabled-participant-ids {shlex.quote(launch.worker_id)}"
            )

        worker_cmd = (
            f"{shlex.quote(launch.binary_path)}"
            f" --name {shlex.quote(launch.worker_id)}"
            f" --kr-url {shlex.quote(launch.kr_url)}"
            f" --relay-url {shlex.quote(launch.relay_url)}"
            f" --listen-addr {shlex.quote(launch.listen_addr)}"
            f"{profile_flag}"
        )
        if env_args:
            worker_cmd = f"env {env_args} {worker_cmd}"

        launcher = (
            f"exec {worker_cmd} "
            f"> {shlex.quote(launch.remote_tmp)}/worker.log 2>&1"
        )

        cmd = (
            f"mkdir -p {shlex.quote(launch.remote_results_root)}/{shlex.quote(launch.run_id)} "
            f"{shlex.quote(launch.remote_tmp)}; "
            f"rm -f {shlex.quote(launch.remote_tmp)}/worker.pid; "
            f"start-stop-daemon -S -b -m "
            f"-p {shlex.quote(launch.remote_tmp)}/worker.pid "
            f"-x /bin/sh -- -c {shlex.quote(launcher)}"
        )
        self._adb(["shell", cmd])

    def stop_worker(self) -> None:
        remote_tmp = self.config.worker.get("remote_tmp", "/tmp/signal-benchmark")
        cmd = (
            f"start-stop-daemon -K -p {shlex.quote(remote_tmp)}/worker.pid 2>/dev/null || "
            f"killall worker 2>/dev/null || pkill worker 2>/dev/null || true"
        )
        self._adb(["shell", cmd], check=False)


# ---------------------------------------------------------------------------
# SSH backend (stub for future Raspberry Pi support)
# ---------------------------------------------------------------------------


class SshDeviceBackend(DeviceBackend):
    def __init__(self, config: DeviceConfig):
        self.config = config
        self.host = config.connection.get("host", "")
        self.user = config.connection.get("user", "root")
        self.identity_file = config.connection.get("identity_file", "")

    def _ssh_base(self) -> list[str]:
        cmd = ["ssh"]
        if self.identity_file:
            cmd += ["-i", self.identity_file]
        cmd += ["-o", "StrictHostKeyChecking=no",
                "-o", "ConnectTimeout=10",
                f"{self.user}@{self.host}"]
        return cmd

    def _ssh(self, command: str, check: bool = True, timeout: int = 60) -> subprocess.CompletedProcess:
        cmd = self._ssh_base() + [command]
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=check,
            timeout=timeout,
        )

    def check_reachable(self) -> None:
        result = self._ssh("hostname; whoami; uname -a; uname -m", check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f"SSH device '{self.host}' not reachable: {result.stderr}"
            )
        print(f"[ssh] device info:\n{result.stdout}")

    def shell(self, command: str, check: bool = True) -> subprocess.CompletedProcess:
        return self._ssh(command, check=check, timeout=120)

    def _scp_cmd(self, src: str, dst: str, *, recursive: bool = False) -> list[str]:
        cmd = ["scp"]
        if recursive:
            cmd.append("-r")
        if self.identity_file:
            cmd += ["-i", self.identity_file]
        cmd += ["-o", "StrictHostKeyChecking=no",
                "-o", "ConnectTimeout=10"]
        cmd += [src, dst]
        return cmd

    def push(self, local: Path, remote: str) -> None:
        remote_scp = f"{self.user}@{self.host}:{remote}"
        subprocess.run(
            self._scp_cmd(str(local), remote_scp),
            check=True,
            timeout=120,
        )

    def pull(self, remote: str, local: Path) -> None:
        local.parent.mkdir(parents=True, exist_ok=True)
        remote_scp = f"{self.user}@{self.host}:{remote}"
        subprocess.run(
            self._scp_cmd(remote_scp, str(local), recursive=True),
            check=True,
            timeout=120,
        )

    def _do_wipe(self, safe_run_id: str) -> None:
        cmds = (
            f"set -e; "
            f"killall worker 2>/dev/null || pkill worker 2>/dev/null || true; "
            f"rm -rf /results/signal/{shlex.quote(safe_run_id)}; "
            f"mkdir -p /results/signal/{shlex.quote(safe_run_id)}; "
            f"rm -rf /tmp/signal-benchmark; "
            f"mkdir -p /tmp/signal-benchmark"
        )
        self._ssh(cmds)

    def install_worker(self, local_binary: Path, remote_binary: str) -> None:
        self.push(local_binary, remote_binary)
        self._ssh(f"chmod +x {shlex.quote(remote_binary)}")

    def start_worker(self, launch: WorkerLaunch) -> None:
        profile_flag = ""
        env_prefix = ""
        if launch.profile_path_template:
            profile_path = launch.profile_path_template.replace("{participant_id}", launch.worker_id)
            env = {
                "SIGNAL_PROFILE_ENABLED": "true",
                "SIGNAL_PROFILE_PARTICIPANT_IDS": launch.worker_id,
                "SIGNAL_PROFILE_PATH": profile_path,
                "SIGNAL_PROFILE_PATH_TEMPLATE": launch.profile_path_template,
                "SIGNAL_PROFILE_RUN_ID": launch.run_id,
                "SIGNAL_PROFILE_SCENARIO": launch.scenario,
            }
            if launch.node_name:
                env["SIGNAL_PROFILE_NODE"] = launch.node_name
            env_prefix = " ".join(
                f"{key}={shlex.quote(value)}" for key, value in env.items()
            ) + " "
            profile_flag = (
                f" --profile-path-template {shlex.quote(launch.profile_path_template)}"
                f" --profile-enabled-participant-ids {shlex.quote(launch.worker_id)}"
            )

        cmd = (
            f"mkdir -p {shlex.quote(launch.remote_results_root)}/{shlex.quote(launch.run_id)} "
            f"{shlex.quote(launch.remote_tmp)}; "
            f"rm -f {shlex.quote(launch.remote_tmp)}/worker.pid; "
            f"{env_prefix}nohup {shlex.quote(launch.binary_path)}"
            f" --name {shlex.quote(launch.worker_id)}"
            f" --kr-url {shlex.quote(launch.kr_url)}"
            f" --relay-url {shlex.quote(launch.relay_url)}"
            f" --listen-addr {shlex.quote(launch.listen_addr)}"
            f"{profile_flag}"
            f" > {shlex.quote(launch.remote_tmp)}/worker.log 2>&1 &"
            f" echo $! > {shlex.quote(launch.remote_tmp)}/worker.pid"
        )
        self._ssh(cmd)

    def stop_worker(self) -> None:
        self._ssh("killall worker 2>/dev/null || pkill worker 2>/dev/null || true", check=False)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_backend(config: DeviceConfig) -> DeviceBackend:
    conn_type = config.connection.get("type", "adb")

    if conn_type == "adb":
        return AdbDeviceBackend(config)
    elif conn_type == "ssh":
        return SshDeviceBackend(config)
    else:
        raise ValueError(f"Unsupported connection type: {conn_type}")


# ---------------------------------------------------------------------------
# Build worker layout entries for external devices
# ---------------------------------------------------------------------------


def build_external_device_layout_entry(
    config: DeviceConfig,
    transport_ip: str,
    host_ip: str,
    worker_port: int,
    kr_port: int | None = None,
    relay_port: int = 4000,
    run_id: str = "",
    ds_port: int | None = None,
) -> tuple[dict, dict, str]:
    """
    Returns (layout_client_entry, layout_physical_worker_entry, worker_file_line).

    The `worker_file_line` is in ID=URL format for the workers file.
    """
    if kr_port is None:
        kr_port = ds_port
    if kr_port is None:
        raise ValueError("kr_port is required")

    worker_id = config.worker.get("id", config.id)
    device_ip = config.transport.get("device_ip", transport_ip)
    url = f"http://{device_ip}:{worker_port}"

    worker_file_line = f"{worker_id}={url}"

    meta = config.metadata

    layout_client = {
        "client_id": worker_id,
        "physical_worker_id": config.id,
        "container_mode": "singleton",
        "profile_enabled": True,
        "command_url": url,
        "health_url": f"{url}/health",
        "execution_backend": meta.get("execution_backend", "real_device"),
        "device_kind": meta.get("device_kind", config.kind),
        "transport": meta.get("transport", config.transport.get("type", "usb_rndis")),
        "access_backend": meta.get("access_backend", config.connection.get("type", "adb")),
        "arch": config.target.get("arch", "armv7l"),
        "rust_target": config.target.get("rust_target", ""),
    }

    layout_physical_worker = {
        "physical_worker_id": config.id,
        "container_mode": "singleton",
        "client_ids": [worker_id],
        "base_url": url,
        "profile_enabled_client_ids": [worker_id],
        "execution_backend": meta.get("execution_backend", "real_device"),
        "device_kind": meta.get("device_kind", config.kind),
        "transport": meta.get("transport", config.transport.get("type", "usb_rndis")),
        "access_backend": meta.get("access_backend", config.connection.get("type", "adb")),
        "arch": config.target.get("arch", "armv7l"),
        "rust_target": config.target.get("rust_target", ""),
    }

    return layout_client, layout_physical_worker, worker_file_line
