"""SSH transport for the content-addressed Cyclone analysis worker."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import selectors
import shlex
import shutil
import subprocess
import tempfile
import threading
from pathlib import Path, PurePosixPath
from typing import Any

import config


class RemoteExecutionError(RuntimeError):
    """The remote worker rejected an operation or returned malformed output."""


class RemoteConnectionError(RemoteExecutionError):
    """Cyclone could not be reached without establishing a workflow outcome."""


WORKER_FILES = (
    "remote_worker.py",
    "analysis_runner.py",
    "standard_report.py",
    "adaptive_routing.py",
    "adaptive_screening.py",
    "pipeline_spec.py",
    "resource_calibration.py",
    "workflow_adapters.py",
    "workflow_catalog.py",
)
DEPLOY_PYTHON = "/usr/bin/python3"

RUNNER_ENV_DEFAULTS = {
    "BIOAGENT_SNAKEMAKE": "/home/liufangzhou/.pixi/bin/snakemake",
    "BIOAGENT_SEQKIT": "/home/liufangzhou/.pixi/bin/seqkit",
    "BIOAGENT_WORKFLOW1_SOURCE": "/data/user/guojindan/Projects/Workflows/workflow1_upstream",
    "BIOAGENT_WORKFLOW2_SOURCE": "/data/user/guojindan/Projects/Workflows/workflow2_virusid_2026db",
    "BIOAGENT_WORKFLOW3_SOURCE": "/data/user/guojindan/Projects/Workflows/workflow3_hostid",
    "BIOAGENT_WORKFLOW4_SOURCE": "/data/user/guojindan/Projects/Workflows/workflow4_kraken_bracken",
    "BIOAGENT_WORKFLOW5_SOURCE": "/data/user/guojindan/Projects/Workflows/workflow5_virus_quantification",
    "BIOAGENT_WORKFLOW6_SOURCE": "/data/user/guojindan/Projects/Workflows/workflow6_virus_annotation",
    "BIOAGENT_SNAKEMAKE_CORES": "8",
    "BIOAGENT_REPORT_PYTHON": "/home/liufangzhou/.pixi/bin/bioagent-report-python",
    "BIOAGENT_RSCRIPT": "/home/liufangzhou/.pixi/bin/bioagent-rscript",
    "BIOAGENT_WEASYPRINT": "/home/liufangzhou/.pixi/bin/bioagent-weasyprint",
}


class RemoteWorkerClient:
    def __init__(self) -> None:
        self.host = config.SSH_HOST
        self.python = config.REMOTE_PYTHON
        self.control_python = config.REMOTE_CONTROL_PYTHON
        self.tmux = config.REMOTE_TMUX
        self.worker_root = PurePosixPath(config.REMOTE_WORKER_ROOT)
        self.data_root = str(config.ANALYSIS_DATA_ROOT)
        self.run_root = str(config.ANALYSIS_RUN_ROOT)
        self.allowed_roots = list(config.REMOTE_DATA_ALLOWED_ROOTS)
        self.project_root = config.PROJECT_ROOT
        self.runner_environment = {
            key: os.getenv(key, default) for key, default in RUNNER_ENV_DEFAULTS.items()
        }
        self._deployment_lock = threading.Lock()
        self._rpc_lock = threading.Lock()
        self._deployed_version: str | None = None
        self._master_ready = False
        self._multiplex_usable = True
        self._rpc_process: subprocess.Popen[bytes] | None = None
        self._rpc_version: str | None = None
        control_digest = hashlib.sha256(self.host.encode()).hexdigest()[:12]
        self.control_path = f"/tmp/bioagent-ssh-{os.getuid()}-{control_digest}"

    @property
    def enabled(self) -> bool:
        return bool(self.host)

    def _ssh(self, *, multiplex: bool = True) -> list[str]:
        command = [
            "ssh",
            "-T",
            "-o",
            "ClearAllForwardings=yes",
            "-o",
            "ControlMaster=no",
            "-o",
            "ConnectTimeout=8",
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=3",
        ]
        command.extend(
            ["-o", f"ControlPath={self.control_path}"]
            if multiplex
            else ["-o", "ControlPath=none"]
        )
        return [*command, self.host]

    @staticmethod
    def _remote_command(arguments: list[str]) -> str:
        return " ".join(shlex.quote(value) for value in arguments)

    def _source_files(self) -> list[tuple[Path, str]]:
        files = [(self.project_root / name, name) for name in WORKER_FILES]
        files.append((self.project_root / "extract_io.py", "scripts/extract_io.py"))
        evidence = (
            self.project_root.parent
            / "open-webui"
            / "backend"
            / "open_webui"
            / "utils"
            / "report_evidence.py"
        )
        files.append((evidence, "report_evidence.py"))
        return files

    def _ensure_master(self) -> None:
        if self._master_ready:
            return
        check = subprocess.run(
            [
                "ssh",
                "-o",
                "ClearAllForwardings=yes",
                "-o",
                f"ControlPath={self.control_path}",
                "-O",
                "check",
                self.host,
            ],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=5,
            check=False,
        )
        if check.returncode == 0:
            self._master_ready = True
            return
        Path(self.control_path).unlink(missing_ok=True)
        command = [
            "ssh",
            "-o",
            "ClearAllForwardings=yes",
            "-o",
            "ControlMaster=yes",
            "-o",
            "ControlPersist=600",
            "-o",
            f"ControlPath={self.control_path}",
            "-o",
            "ConnectTimeout=8",
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=3",
            "-MNf",
            self.host,
        ]
        self._run(command, timeout=15)
        self._master_ready = True

    def _discard_master(self) -> None:
        try:
            subprocess.run(
                [
                    "ssh",
                    "-o",
                    "ClearAllForwardings=yes",
                    "-o",
                    f"ControlPath={self.control_path}",
                    "-O",
                    "exit",
                    self.host,
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=3,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            pass
        Path(self.control_path).unlink(missing_ok=True)
        self._master_ready = False

    def _run_remote(
        self,
        remote_command: str,
        *,
        timeout: float,
        input: bytes | None = None,
        multiplex_timeout: float | None = None,
    ) -> subprocess.CompletedProcess:
        try:
            self._ensure_master()
            return self._run(
                [*self._ssh(), remote_command],
                timeout=multiplex_timeout or timeout,
                input=input,
            )
        except RemoteConnectionError:
            self._discard_master()
            return self._run(
                [*self._ssh(multiplex=False), remote_command],
                timeout=timeout,
                input=input,
            )

    def _version(self) -> str:
        digest = hashlib.sha256()
        for path, name in self._source_files():
            digest.update(name.encode())
            digest.update(b"\0")
            digest.update(path.read_bytes())
        return digest.hexdigest()

    def _rsync_command(
        self, source: Path, staging: PurePosixPath, *, multiplex: bool
    ) -> list[str]:
        control_path = self.control_path if multiplex else "none"
        transport = (
            "ssh -T -o ClearAllForwardings=yes -o ControlMaster=no "
            f"-o ControlPath={shlex.quote(control_path)} -o ConnectTimeout=8"
        )
        return [
            "rsync",
            "-a",
            "--delete",
            "-e",
            transport,
            f"{source}/",
            f"{self.host}:{staging}/",
        ]

    def _sync_sources(self, staging: PurePosixPath) -> None:
        prepare_code = "import pathlib,sys; pathlib.Path(sys.argv[1]).mkdir(parents=True,exist_ok=True)"
        self._run_remote(
            self._remote_command(
                [DEPLOY_PYTHON, "-S", "-c", prepare_code, str(staging)]
            ),
            timeout=30,
            multiplex_timeout=5,
        )
        with tempfile.TemporaryDirectory(prefix="bioagent-worker-") as temporary:
            source_root = Path(temporary)
            for source, name in self._source_files():
                target = source_root / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
            try:
                self._ensure_master()
                self._run(
                    self._rsync_command(source_root, staging, multiplex=True),
                    timeout=60,
                )
            except RemoteConnectionError:
                self._discard_master()
                self._run(
                    self._rsync_command(source_root, staging, multiplex=False),
                    timeout=60,
                )

    @staticmethod
    def _wait_for_line(process: subprocess.Popen[bytes], timeout: float) -> bytes:
        if process.stdout is None:
            raise RemoteConnectionError("Cyclone worker output is unavailable")
        selector = selectors.DefaultSelector()
        try:
            selector.register(process.stdout, selectors.EVENT_READ)
            if not selector.select(timeout):
                raise RemoteConnectionError("Cyclone worker response timed out")
        finally:
            selector.close()
        line = process.stdout.readline()
        if not line:
            raise RemoteConnectionError("Cyclone worker connection closed")
        return line

    def _stop_rpc(self) -> None:
        process = self._rpc_process
        self._rpc_process = None
        self._rpc_version = None
        if process is None:
            return
        if process.stdin is not None:
            try:
                process.stdin.close()
            except OSError:
                pass
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)

    def _start_rpc_process(
        self, release: PurePosixPath, version: str, *, multiplex: bool
    ) -> None:
        if multiplex:
            self._ensure_master()
        try:
            process = subprocess.Popen(
                [
                    *self._ssh(multiplex=multiplex),
                    self._remote_command(
                        [
                            self.control_python,
                            "-S",
                            str(release / "remote_worker.py"),
                            "serve",
                        ]
                    ),
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
        except OSError as exc:
            raise RemoteConnectionError("无法启动 Cyclone worker 连接") from exc
        self._rpc_process = process
        self._rpc_version = version
        try:
            ready = json.loads(self._wait_for_line(process, 45 if multiplex else 120))
            if not isinstance(ready, dict) or ready.get("ready") is not True:
                raise RemoteConnectionError("Cyclone worker handshake is invalid")
        except (UnicodeError, json.JSONDecodeError, RemoteConnectionError):
            self._stop_rpc()
            raise

    def _ensure_rpc_process(
        self, release: PurePosixPath, version: str
    ) -> subprocess.Popen[bytes]:
        process = self._rpc_process
        if (
            process is not None
            and process.poll() is None
            and self._rpc_version == version
        ):
            return process
        self._stop_rpc()
        if self._multiplex_usable:
            try:
                self._start_rpc_process(release, version, multiplex=True)
            except RemoteConnectionError:
                self._discard_master()
                self._multiplex_usable = False
        if self._rpc_process is None:
            self._start_rpc_process(release, version, multiplex=False)
        assert self._rpc_process is not None
        return self._rpc_process

    def close(self) -> None:
        with self._rpc_lock:
            self._stop_rpc()

    @staticmethod
    def _run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
        try:
            stdin = kwargs.pop("stdin", None)
            if stdin is None and "input" not in kwargs:
                stdin = subprocess.DEVNULL
            return subprocess.run(
                command,
                check=True,
                stdin=stdin,
                capture_output=True,
                timeout=kwargs.pop("timeout", 30),
                **kwargs,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            detail = ""
            if isinstance(exc, subprocess.CalledProcessError):
                detail = exc.stderr.decode("utf-8", errors="replace").strip()
            raise RemoteConnectionError(
                f"无法连接 Cyclone{f'：{detail}' if detail else ''}"
            ) from exc

    def ensure_deployed(self) -> tuple[str, PurePosixPath]:
        if not self.enabled:
            raise RemoteExecutionError("Cyclone SSH backend is not configured")
        version = self._version()
        release = self.worker_root / "releases" / version
        if self._deployed_version == version:
            return version, release
        with self._deployment_lock:
            if self._deployed_version == version:
                return version, release
            self._ensure_master()
            required_names = [name for _, name in self._source_files()]
            check_code = (
                "import pathlib,sys; p=pathlib.Path(sys.argv[1]); "
                "print('ready' if p.is_dir() and all((p/name).is_file() "
                "for name in sys.argv[2:]) else 'missing')"
            )
            existing = self._run_remote(
                self._remote_command(
                    [
                        DEPLOY_PYTHON,
                        "-S",
                        "-c",
                        check_code,
                        str(release),
                        *required_names,
                    ]
                ),
                timeout=60,
                multiplex_timeout=10,
            )
            if existing.stdout.strip() == b"ready":
                self._deployed_version = version
                return version, release
            staging = self.worker_root / "staging" / f"{version}-{os.getpid()}"
            self._sync_sources(staging)
            publish_code = (
                "import os,pathlib,shutil,sys; "
                "src=pathlib.Path(sys.argv[1]); dst=pathlib.Path(sys.argv[2]); "
                "dst.parent.mkdir(parents=True,exist_ok=True); "
                "shutil.rmtree(src) if dst.exists() else os.replace(src,dst)"
            )
            self._run_remote(
                self._remote_command(
                    [
                        DEPLOY_PYTHON,
                        "-S",
                        "-c",
                        publish_code,
                        str(staging),
                        str(release),
                    ]
                ),
                timeout=60,
                multiplex_timeout=10,
            )
            self._deployed_version = version
        return version, release

    def _request(
        self, operation: str, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        version, release = self.ensure_deployed()
        request = {
            "operation": operation,
            "worker_version": version,
            "settings": {
                "python": self.python,
                "tmux": self.tmux,
                "tmux_socket": "bioagent-worker",
                "data_root": self.data_root,
                "run_root": self.run_root,
                "allowed_roots": self.allowed_roots,
                "runner_environment": self.runner_environment,
            },
            **(payload or {}),
        }
        timeout = (
            3600
            if (
                operation == "dry_run"
                or (operation == "preflight" and request.get("full") is True)
            )
            else 120
        )
        with self._rpc_lock:
            process = self._ensure_rpc_process(release, version)
            if process.stdin is None:
                raise RemoteConnectionError("Cyclone worker input is unavailable")
            try:
                process.stdin.write(
                    json.dumps(request, ensure_ascii=False).encode() + b"\n"
                )
                process.stdin.flush()
                output = self._wait_for_line(process, timeout)
            except (OSError, RemoteConnectionError) as exc:
                self._stop_rpc()
                if isinstance(exc, RemoteConnectionError):
                    raise
                raise RemoteConnectionError("Cyclone worker connection failed") from exc
        try:
            value = json.loads(output.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise RemoteExecutionError("Cyclone worker returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise RemoteExecutionError("Cyclone worker returned an invalid response")
        if value.get("ok") is not True:
            raise RemoteExecutionError(
                str(value.get("error") or "Cyclone worker rejected the request")
            )
        result = value.get("result")
        if not isinstance(result, dict):
            raise RemoteExecutionError("Cyclone worker result is missing")
        return result

    def invoke_sync(self, operation: str, **payload: Any) -> dict[str, Any]:
        return self._request(operation, payload)

    async def invoke(self, operation: str, **payload: Any) -> dict[str, Any]:
        return await asyncio.to_thread(self._request, operation, payload)


class RemoteResourceCalibrator:
    def __init__(self, client: RemoteWorkerClient):
        self.client = client

    def estimate(self, execution_spec: dict[str, Any]) -> dict[str, Any]:
        return self.client.invoke_sync("estimate", execution_spec=execution_spec)
