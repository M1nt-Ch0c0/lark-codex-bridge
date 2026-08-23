#!/usr/bin/env python3
"""Deterministic Codex app-server schema maintenance.

This tool is deliberately outside Cargo's build graph.  `sync` is the only
command that executes Codex; `verify`, `diff`, and `contract` operate entirely
on committed artifacts and are safe in an offline build/test environment.
"""

from __future__ import annotations

import argparse
import contextvars
import functools
import hashlib
import json
import math
import os
import queue
import re
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from fractions import Fraction
from pathlib import Path
from typing import Any, Iterable, Iterator, NoReturn
from urllib.parse import unquote, urldefrag, urljoin


GENERATOR_NAME = "lark-codex-bridge/codex-schema"
GENERATOR_VERSION = "1.2.0"
LEGACY_GENERATOR_VERSIONS = {"0.146.0": "1.1.0"}
LEGACY_TEMPLATE_SHA256 = {
    "0.146.0": "4d07c5c97841b0c9fb14aa525e611026543e785347c293ff70498c36051de19e"
}
MANIFEST_FORMAT_VERSION = 1
SCHEMA_BUNDLE_FORMAT_VERSION = 1
CONTRACT_FORMAT_VERSION = 1
AUDIT_FORMAT_VERSION = 1
HISTORY_FORMAT_VERSION = 1
ESTABLISHED_BASELINE_VERSION = "0.146.0"
ESTABLISHED_BASELINE_SCHEMA_SHA256 = "8f949f41d0de731f26d264db686a90469a817837f83050c47487045745a3b3a6"
MAX_CAPTURE_BYTES = 64 * 1024
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_AGGREGATE_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_JSON_NODES_PER_ARTIFACT = 500_000
MAX_AGGREGATE_JSON_NODES = 2_000_000
MAX_JSON_DEPTH = 128
MAX_JSON_NUMBER_CHARACTERS = 4_096
MAX_WORK_UNITS = 5_000_000
MAX_CLASSIFIED_CHANGES = 10_000
MAX_DIRECTORY_ENTRIES = 1_024
MAX_GENERATOR_ARGUMENTS = 64
MAX_SELECTED_ROOTS = 128
MAX_TRACKED_VERSIONS = 256
MAX_OPERATION_SECONDS = 180
MAX_REGEX_SECONDS = 1.0
MAX_REGEX_PATTERN_CHARACTERS = 64 * 1024
MAX_REGEX_TEXT_CHARACTERS = MAX_ARTIFACT_BYTES
REQUIRED_COMPATIBILITY_REVIEW_EVIDENCE = [
    "exact_schema_sync_reproduces",
    "shared_contract_matrix_covers_selected_roots",
    "incoming_open_values_are_preserved_or_rejected",
    "outgoing_adapter_rejects_unrepresentable_values",
]
READ_CHUNK_BYTES = 64 * 1024
SCHEMA_VALIDATION_RECURSION_LIMIT = MAX_JSON_DEPTH
VERSION_TIMEOUT_SECONDS = 10
GENERATION_TIMEOUT_SECONDS = 120
VERSION_RE = re.compile(rb"codex-cli (0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:\r?\n)?\Z")

REPO_ROOT = Path(__file__).resolve().parent.parent
PROTOCOL_ROOT = REPO_ROOT / "protocol" / "codex"
SELECTION_PATH = PROTOCOL_ROOT / "selection.json"
POLICY_PATH = PROTOCOL_ROOT / "support-policy.json"
SCHEMAS_ROOT = PROTOCOL_ROOT / "schemas"
CONTRACTS_ROOT = PROTOCOL_ROOT / "contracts"
REPORTS_ROOT = PROTOCOL_ROOT / "reports"
WIRE_ROOT = REPO_ROOT / "src" / "codex" / "wire"
WIRE_TEMPLATE_PATH = REPO_ROOT / "tools" / "codex-wire-template.rs"
SHARED_WIRE_TEMPLATE_PATH = REPO_ROOT / "tools" / "codex-shared-wire-template.rs"
HISTORY_PATH = PROTOCOL_ROOT / "support-history.json"
COMPATIBILITY_REVIEWS_ROOT = PROTOCOL_ROOT / "compatibility-reviews"


class SchemaToolError(Exception):
    """A sanitized maintenance failure; it never contains wire payloads."""


@dataclass
class OperationBudget:
    """One bounded maintenance operation shared by all nested helpers."""

    deadline: float
    maximum_aggregate_bytes: int = MAX_AGGREGATE_ARTIFACT_BYTES
    maximum_file_json_nodes: int = MAX_JSON_NODES_PER_ARTIFACT
    maximum_json_nodes: int = MAX_AGGREGATE_JSON_NODES
    maximum_json_depth: int = MAX_JSON_DEPTH
    maximum_work: int = MAX_WORK_UNITS
    maximum_changes: int = MAX_CLASSIFIED_CHANGES
    artifact_bytes: int = 0
    json_nodes: int = 0
    work: int = 0
    changes: int = 0
    regex_worker: BoundedRegexWorker | None = field(default=None, init=False, repr=False)
    reference_indexes: dict[int, tuple[Any, Any]] = field(
        default_factory=dict, init=False, repr=False
    )
    reference_fingerprints: dict[
        tuple[int, int], tuple[tuple[str, str], ...]
    ] = field(default_factory=dict, init=False, repr=False)

    def checkpoint(self, units: int = 1) -> None:
        if units < 0:
            fail("maintenance work accounting is invalid")
        self.work += units
        if self.work > self.maximum_work:
            fail("maintenance operation exceeded the bounded work limit")
        if time.monotonic() > self.deadline:
            fail("maintenance operation exceeded its bounded deadline")

    def consume_bytes(self, count: int) -> None:
        self.checkpoint()
        self.artifact_bytes += count
        if self.artifact_bytes > self.maximum_aggregate_bytes:
            fail("maintenance operation exceeded the aggregate artifact-byte limit")

    def consume_json_nodes(self, count: int) -> None:
        self.checkpoint()
        self.json_nodes += count
        if self.json_nodes > self.maximum_json_nodes:
            fail("maintenance operation exceeded the aggregate JSON-node limit")

    def consume_change(self) -> None:
        self.checkpoint()
        self.changes += 1
        if self.changes > self.maximum_changes:
            fail("schema comparison exceeded the classified-change limit")


_ACTIVE_BUDGET: contextvars.ContextVar[OperationBudget | None] = contextvars.ContextVar(
    "codex_schema_operation_budget", default=None
)


@contextmanager
def operation_budget(
    *,
    timeout: float = MAX_OPERATION_SECONDS,
    maximum_aggregate_bytes: int = MAX_AGGREGATE_ARTIFACT_BYTES,
    maximum_file_json_nodes: int = MAX_JSON_NODES_PER_ARTIFACT,
    maximum_json_nodes: int = MAX_AGGREGATE_JSON_NODES,
    maximum_json_depth: int = MAX_JSON_DEPTH,
    maximum_work: int = MAX_WORK_UNITS,
    maximum_changes: int = MAX_CLASSIFIED_CHANGES,
) -> Iterator[OperationBudget]:
    """Install one aggregate resource budget unless a caller already owns it."""
    existing = _ACTIVE_BUDGET.get()
    if existing is not None:
        existing.checkpoint()
        yield existing
        return
    if (
        timeout <= 0
        or maximum_aggregate_bytes <= 0
        or maximum_file_json_nodes <= 0
        or maximum_json_nodes <= 0
        or maximum_json_depth <= 0
        or maximum_work <= 0
        or maximum_changes <= 0
    ):
        fail("maintenance operation has an invalid resource budget")
    budget = OperationBudget(
        deadline=time.monotonic() + timeout,
        maximum_aggregate_bytes=maximum_aggregate_bytes,
        maximum_file_json_nodes=maximum_file_json_nodes,
        maximum_json_nodes=maximum_json_nodes,
        maximum_json_depth=maximum_json_depth,
        maximum_work=maximum_work,
        maximum_changes=maximum_changes,
    )
    token = _ACTIVE_BUDGET.set(budget)
    try:
        yield budget
    finally:
        if budget.regex_worker is not None:
            budget.regex_worker.close()
            budget.regex_worker = None
        _ACTIVE_BUDGET.reset(token)


def budgeted(function: Any) -> Any:
    """Give directly-invoked helpers the same limits as CLI operations."""

    @functools.wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with operation_budget():
            return function(*args, **kwargs)

    return wrapper


def active_budget() -> OperationBudget:
    budget = _ACTIVE_BUDGET.get()
    if budget is None:
        fail("maintenance resource budget is unavailable")
    budget.checkpoint()
    return budget


@dataclass(frozen=True)
class SelectionRoot:
    name: str
    path: str
    since_version: str | None = None


@dataclass(frozen=True)
class Selection:
    protocol_family: str
    generator_arguments: tuple[str, ...]
    roots: tuple[SelectionRoot, ...]
    notification_catalog: str


@dataclass(frozen=True)
class BoundedProcessResult:
    returncode: int
    stdout: bytes
    stderr: bytes
    overflowed: bool
    timed_out: bool


@dataclass(frozen=True)
class CodexExecutionContext:
    cwd: Path
    codex_home: Path
    environment: dict[str, str]


class WindowsJob:
    """Kill-on-close Windows Job Object assigned before the child can execute."""

    def __init__(self) -> None:
        if os.name != "nt":
            fail("Windows process-tree ownership is unavailable on this platform")
        import ctypes
        from ctypes import wintypes

        class BasicLimitInformation(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BasicLimitInformation),
                ("IoInfo", IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        ]
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel32.TerminateJobObject.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.CreateJobObjectW(None, None)
        if not handle:
            fail("Windows process-tree owner could not be created")
        information = ExtendedLimitInformation()
        # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE in the extended-limit class (9).
        information.BasicLimitInformation.LimitFlags = 0x00002000
        if not kernel32.SetInformationJobObject(
            handle, 9, ctypes.byref(information), ctypes.sizeof(information)
        ):
            kernel32.CloseHandle(handle)
            fail("Windows process-tree owner could not be configured")
        self._ctypes = ctypes
        self._wintypes = wintypes
        self._kernel32 = kernel32
        self._handle = handle

    def assign_and_resume(self, process: subprocess.Popen[bytes]) -> None:
        ctypes = self._ctypes
        wintypes = self._wintypes
        kernel32 = self._kernel32
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        # PROCESS_SET_QUOTA | PROCESS_TERMINATE, required by assignment.
        process_handle = kernel32.OpenProcess(0x00000101, False, process.pid)
        if not process_handle:
            fail("Windows child process handle is unavailable")
        try:
            if not kernel32.AssignProcessToJobObject(self._handle, process_handle):
                fail("Windows child could not enter the owned process tree")
        finally:
            kernel32.CloseHandle(process_handle)

        class ThreadEntry32(ctypes.Structure):
            _fields_ = [
                ("dwSize", wintypes.DWORD),
                ("cntUsage", wintypes.DWORD),
                ("th32ThreadID", wintypes.DWORD),
                ("th32OwnerProcessID", wintypes.DWORD),
                ("tpBasePri", wintypes.LONG),
                ("tpDeltaPri", wintypes.LONG),
                ("dwFlags", wintypes.DWORD),
            ]

        kernel32.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
        kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
        kernel32.Thread32First.argtypes = [wintypes.HANDLE, ctypes.POINTER(ThreadEntry32)]
        kernel32.Thread32First.restype = wintypes.BOOL
        kernel32.Thread32Next.argtypes = [wintypes.HANDLE, ctypes.POINTER(ThreadEntry32)]
        kernel32.Thread32Next.restype = wintypes.BOOL
        kernel32.OpenThread.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenThread.restype = wintypes.HANDLE
        kernel32.ResumeThread.argtypes = [wintypes.HANDLE]
        kernel32.ResumeThread.restype = wintypes.DWORD
        snapshot = kernel32.CreateToolhelp32Snapshot(0x00000004, 0)  # TH32CS_SNAPTHREAD
        invalid_handle = ctypes.c_void_p(-1).value
        snapshot_value = ctypes.cast(snapshot, ctypes.c_void_p).value
        if not snapshot or snapshot_value == invalid_handle:
            fail("Windows child main thread could not be enumerated")
        thread_handle = None
        try:
            entry = ThreadEntry32()
            entry.dwSize = ctypes.sizeof(entry)
            present = kernel32.Thread32First(snapshot, ctypes.byref(entry))
            while present:
                if entry.th32OwnerProcessID == process.pid:
                    # THREAD_SUSPEND_RESUME is the only requested thread right.
                    thread_handle = kernel32.OpenThread(0x0002, False, entry.th32ThreadID)
                    break
                present = kernel32.Thread32Next(snapshot, ctypes.byref(entry))
        finally:
            kernel32.CloseHandle(snapshot)
        if not thread_handle:
            fail("Windows child main thread could not be opened")
        try:
            if kernel32.ResumeThread(thread_handle) == 0xFFFFFFFF:
                fail("Windows child main thread could not be resumed")
        finally:
            kernel32.CloseHandle(thread_handle)

    def terminate(self) -> None:
        if self._handle:
            self._kernel32.TerminateJobObject(self._handle, 1)

    def close(self) -> None:
        if self._handle:
            self._kernel32.CloseHandle(self._handle)
            self._handle = None


def _signal_process_group(
    process: subprocess.Popen[bytes],
    signal_number: int,
    windows_job: WindowsJob | None = None,
) -> None:
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal_number)
        else:
            if windows_job is not None:
                windows_job.terminate()
            elif signal_number == signal.SIGTERM:
                process.terminate()
            else:
                process.kill()
    except OSError:
        pass


def _stop_process(
    process: subprocess.Popen[bytes], windows_job: WindowsJob | None = None
) -> None:
    _signal_process_group(process, signal.SIGTERM, windows_job)
    if process.poll() is None:
        try:
            process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            _signal_process_group(process, signal.SIGKILL, windows_job)
    elif windows_job is not None:
        windows_job.terminate()
    if process.poll() is None:
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass


REGEX_WORKER_SOURCE = r"""
import json
import re
import sys

for line in sys.stdin.buffer:
    try:
        request = json.loads(line)
        if (
            not isinstance(request, list)
            or len(request) != 2
            or not all(isinstance(value, str) for value in request)
        ):
            raise ValueError
        pattern, text = request
        matched = re.search(pattern, text) is not None
    except re.error:
        response = b"E\n"
    except Exception:
        response = b"X\n"
    else:
        response = b"1\n" if matched else b"0\n"
    sys.stdout.buffer.write(response)
    sys.stdout.buffer.flush()
"""


class BoundedRegexWorker:
    """Evaluate untrusted schema regexes beyond a hard-kill process boundary."""

    def __init__(self) -> None:
        self._windows_job = WindowsJob() if os.name == "nt" else None
        self._closed = False
        self._responses: queue.Queue[bytes] = queue.Queue()
        self._writers: list[threading.Thread] = []
        try:
            self._process = subprocess.Popen(
                [sys.executable, "-I", "-u", "-c", REGEX_WORKER_SOURCE],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                start_new_session=os.name == "posix",
                creationflags=0x00000004 if os.name == "nt" else 0,
            )
        except (OSError, ValueError) as error:
            if self._windows_job is not None:
                self._windows_job.close()
            raise SchemaToolError("schema regex isolation could not start") from error
        if self._windows_job is not None:
            try:
                self._windows_job.assign_and_resume(self._process)
            except Exception as error:
                try:
                    self._process.kill()
                    self._process.wait(timeout=1)
                except (OSError, subprocess.TimeoutExpired):
                    pass
                self._windows_job.close()
                if isinstance(error, SchemaToolError):
                    raise
                raise SchemaToolError("schema regex isolation ownership failed") from error
        assert self._process.stdin is not None and self._process.stdout is not None
        self._reader = threading.Thread(target=self._read_responses, daemon=True)
        try:
            self._reader.start()
        except RuntimeError as error:
            self.close()
            raise SchemaToolError("schema regex isolation reader could not start") from error

    @property
    def is_closed(self) -> bool:
        return (
            self._closed
            and self._process.poll() is not None
            and not self._reader.is_alive()
            and not any(writer.is_alive() for writer in self._writers)
        )

    def _read_responses(self) -> None:
        assert self._process.stdout is not None
        try:
            while True:
                line = self._process.stdout.readline()
                self._responses.put(line)
                if not line:
                    return
        except (OSError, ValueError):
            self._responses.put(b"")

    def search(self, pattern: str, text: str, timeout: float) -> bool:
        if self._closed or self._process.poll() is not None:
            raise SchemaToolError("schema regex isolation stopped unexpectedly")
        deadline = time.monotonic() + timeout
        try:
            request = (json.dumps([pattern, text], ensure_ascii=False) + "\n").encode("utf-8")
        except (TypeError, UnicodeError, ValueError) as error:
            raise ValidationFailure("schema pattern is invalid") from error
        write_errors: list[BaseException] = []

        def write_request() -> None:
            try:
                assert self._process.stdin is not None
                self._process.stdin.write(request)
                self._process.stdin.flush()
            except (BrokenPipeError, OSError, ValueError) as error:
                write_errors.append(error)

        writer = threading.Thread(target=write_request, daemon=True)
        try:
            writer.start()
        except RuntimeError as error:
            self.close()
            raise SchemaToolError("schema regex isolation writer could not start") from error
        self._writers.append(writer)
        try:
            response = self._responses.get(timeout=max(0.0, deadline - time.monotonic()))
        except queue.Empty:
            self.close()
            writer.join(timeout=1)
            raise SchemaToolError("schema regex evaluation exceeded its bounded deadline") from None
        writer.join(timeout=1)
        if writer.is_alive():
            self.close()
            raise SchemaToolError("schema regex isolation write did not complete")
        if write_errors or response in {b"", b"X\n"}:
            self.close()
            raise SchemaToolError("schema regex isolation failed safely")
        if response == b"E\n":
            raise ValidationFailure("schema pattern is invalid")
        if response not in {b"0\n", b"1\n"}:
            self.close()
            raise SchemaToolError("schema regex isolation returned an invalid response")
        return response == b"1\n"

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._process.poll() is None:
            _stop_process(self._process, self._windows_job)
        elif self._windows_job is not None:
            self._windows_job.terminate()
        if os.name == "posix":
            _signal_process_group(self._process, signal.SIGKILL, self._windows_job)
        if self._windows_job is not None:
            self._windows_job.close()
        for pipe in (self._process.stdin, self._process.stdout):
            if pipe is not None:
                try:
                    pipe.close()
                except OSError:
                    pass
        for writer in self._writers:
            if writer.ident is not None:
                writer.join(timeout=1)
        if hasattr(self, "_reader") and self._reader.ident is not None:
            self._reader.join(timeout=1)


def bounded_regex_search(pattern: str, text: str) -> bool:
    """Match with a per-regex cap no later than the operation deadline."""
    budget = active_budget()
    if (
        len(pattern) > MAX_REGEX_PATTERN_CHARACTERS
        or len(text) > MAX_REGEX_TEXT_CHARACTERS
    ):
        fail("schema regex input exceeds the bounded character limit")
    budget.checkpoint(max(1, (len(pattern) + len(text)) // 4_096))
    if budget.regex_worker is None:
        budget.regex_worker = BoundedRegexWorker()
    remaining = min(MAX_REGEX_SECONDS, budget.deadline - time.monotonic())
    if remaining <= 0:
        fail("maintenance operation exceeded its bounded deadline")
    matched = budget.regex_worker.search(pattern, text, remaining)
    budget.checkpoint()
    return matched


def run_bounded(
    command: list[str],
    *,
    timeout: float,
    cwd: Path | None = None,
    environment: dict[str, str] | None = None,
) -> BoundedProcessResult:
    """Run a command while concurrently draining and bounding both output pipes."""
    windows_job = WindowsJob() if os.name == "nt" else None
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=os.name == "posix",
            cwd=cwd,
            env=environment,
            # CREATE_SUSPENDED closes the child-spawn race before Job assignment.
            creationflags=0x00000004 if os.name == "nt" else 0,
        )
    except (OSError, ValueError) as error:
        if windows_job is not None:
            windows_job.close()
        raise SchemaToolError("external command could not start") from error
    if windows_job is not None:
        try:
            windows_job.assign_and_resume(process)
        except Exception as error:
            try:
                process.kill()
                process.wait(timeout=1)
            except (OSError, subprocess.TimeoutExpired):
                pass
            windows_job.close()
            if isinstance(error, SchemaToolError):
                raise
            raise SchemaToolError("Windows process-tree ownership failed") from error
    assert process.stdout is not None and process.stderr is not None
    overflow = threading.Event()
    reader_failed = threading.Event()
    captures = [bytearray(), bytearray()]

    def drain(pipe: Any, capture: bytearray) -> None:
        try:
            while chunk := pipe.read(8192):
                remaining = MAX_CAPTURE_BYTES - len(capture)
                if remaining > 0:
                    capture.extend(chunk[:remaining])
                if len(chunk) > remaining:
                    overflow.set()
        except (OSError, ValueError):
            reader_failed.set()
        finally:
            pipe.close()

    readers = [
        threading.Thread(target=drain, args=(process.stdout, captures[0]), daemon=True),
        threading.Thread(target=drain, args=(process.stderr, captures[1]), daemon=True),
    ]
    started_readers: list[threading.Thread] = []
    try:
        for reader in readers:
            reader.start()
            started_readers.append(reader)
    except RuntimeError as error:
        _stop_process(process, windows_job)
        if windows_job is not None:
            windows_job.close()
        for reader in started_readers:
            reader.join(timeout=1)
        process.stdout.close()
        process.stderr.close()
        raise SchemaToolError("external command pipe readers could not start") from error
    deadline = time.monotonic() + timeout
    timed_out = False
    inherited_pipe = False
    try:
        while process.poll() is None:
            if _ACTIVE_BUDGET.get() is not None:
                active_budget().checkpoint()
            if overflow.is_set():
                _stop_process(process, windows_job)
                break
            if reader_failed.is_set():
                _stop_process(process, windows_job)
                break
            if time.monotonic() >= deadline:
                timed_out = True
                _stop_process(process, windows_job)
                break
            time.sleep(0.01)
    finally:
        if process.poll() is None:
            _stop_process(process, windows_job)
        if windows_job is not None:
            # KILL_ON_JOB_CLOSE settles descendants even after the direct child
            # exits normally and before inherited output pipes are joined.
            windows_job.close()
        for reader in readers:
            reader.join(timeout=0.25)
        if any(reader.is_alive() for reader in readers):
            # A descendant inherited stdout/stderr after the direct child
            # exited. Kill the isolated group so EOF and cleanup are bounded.
            inherited_pipe = True
            _signal_process_group(process, signal.SIGKILL, windows_job)
            for reader in readers:
                reader.join(timeout=1)
        if os.name == "posix":
            # Also settle descendants that deliberately closed inherited pipes
            # before the direct child exited.
            _signal_process_group(process, signal.SIGKILL, windows_job)
        if any(reader.is_alive() for reader in readers):
            raise SchemaToolError("external command pipe cleanup did not complete")
    if reader_failed.is_set():
        raise SchemaToolError("external command pipe read failed")
    if inherited_pipe and os.name != "nt" and not (timed_out or overflow.is_set()):
        raise SchemaToolError("external command descendants retained output pipes")
    return BoundedProcessResult(
        process.returncode if process.returncode is not None else -1,
        bytes(captures[0]),
        bytes(captures[1]),
        overflow.is_set(),
        timed_out,
    )


def fail(message: str) -> NoReturn:
    raise SchemaToolError(message)


def inspect_json_shape(value: Any, *, count_toward_aggregate: bool) -> None:
    """Bound JSON depth/nodes iteratively before recursive consumers see it."""
    budget = active_budget()
    stack: list[tuple[Any, int]] = [(value, 1)]
    nodes = 0
    while stack:
        current, depth = stack.pop()
        nodes += 1
        budget.checkpoint()
        if nodes > budget.maximum_file_json_nodes:
            fail("JSON artifact exceeds the per-file node limit")
        if depth > budget.maximum_json_depth:
            fail("JSON artifact exceeds the nesting-depth limit")
        if isinstance(current, dict):
            for key, child in current.items():
                if not isinstance(key, str):
                    fail("JSON object contains a non-string key")
                stack.append((child, depth + 1))
        elif isinstance(current, list):
            for child in current:
                stack.append((child, depth + 1))
        elif not (
            current is None
            or isinstance(current, (str, bool, int))
            or (isinstance(current, float) and math.isfinite(current))
        ):
            fail("JSON artifact contains an unsupported value")
    if count_toward_aggregate:
        budget.consume_json_nodes(nodes)


def inspect_json_text_depth(text: str) -> None:
    """Reject excessive container nesting before the JSON decoder recurses."""
    budget = active_budget()
    depth = 0
    in_string = False
    escaped = False
    for index, character in enumerate(text):
        if index % 4_096 == 0:
            budget.checkpoint()
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character in "[{":
            depth += 1
            if depth > budget.maximum_json_depth:
                fail("JSON artifact exceeds the nesting-depth limit")
        elif character in "]}":
            depth -= 1


@budgeted
def read_bounded_bytes(path: Path, *, maximum: int = MAX_ARTIFACT_BYTES) -> bytes:
    """Read one file without a stat/read race and charge the aggregate budget."""
    if maximum <= 0 or maximum > MAX_ARTIFACT_BYTES:
        fail("artifact read has an invalid byte limit")
    content = bytearray()
    try:
        if path.is_symlink():
            fail(f"artifact symbolic links are not permitted: {safe_relative(path)}")
        flags = os.O_RDONLY
        for flag_name in ("O_BINARY", "O_CLOEXEC", "O_NOINHERIT", "O_NONBLOCK", "O_NOFOLLOW"):
            flags |= getattr(os, flag_name, 0)
        descriptor = os.open(path, flags)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                fail(f"artifact is not a regular file: {safe_relative(path)}")
            with os.fdopen(descriptor, "rb") as handle:
                descriptor = -1
                while True:
                    active_budget().checkpoint()
                    remaining = maximum + 1 - len(content)
                    if remaining <= 0:
                        fail(
                            f"artifact exceeds the {maximum}-byte limit: {safe_relative(path)}"
                        )
                    chunk = handle.read(min(READ_CHUNK_BYTES, remaining))
                    if not chunk:
                        break
                    content.extend(chunk)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
    except SchemaToolError:
        raise
    except OSError as error:
        raise SchemaToolError(
            f"required artifact is unavailable: {safe_relative(path)}"
        ) from error
    if len(content) > maximum:
        fail(f"artifact exceeds the {maximum}-byte limit: {safe_relative(path)}")
    active_budget().consume_bytes(len(content))
    return bytes(content)


@budgeted
def load_json(path: Path, *, maximum: int = MAX_ARTIFACT_BYTES) -> Any:
    try:
        text = read_bounded_bytes(path, maximum=maximum).decode("utf-8")
        inspect_json_text_depth(text)
        value = json.loads(
            text,
            parse_int=parse_bounded_integer,
            parse_float=parse_bounded_float,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
            object_pairs_hook=unique_json_object,
        )
        inspect_json_shape(value, count_toward_aggregate=True)
        return value
    except SchemaToolError:
        raise
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise SchemaToolError(f"invalid JSON artifact: {safe_relative(path)}") from error


def parse_bounded_integer(value: str) -> int:
    active_budget().checkpoint(max(1, len(value) // 64))
    digits = len(value) - int(value.startswith("-"))
    if digits > MAX_JSON_NUMBER_CHARACTERS:
        fail("JSON integer exceeds the per-number character limit")
    return int(value)


def parse_bounded_float(value: str) -> float:
    active_budget().checkpoint(max(1, len(value) // 64))
    if len(value) > MAX_JSON_NUMBER_CHARACTERS:
        fail("JSON number exceeds the per-number character limit")
    return float(value)


def unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def safe_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except (OSError, ValueError):
        return path.name


@budgeted
def canonical_bytes(value: Any) -> bytes:
    inspect_json_shape(value, count_toward_aggregate=False)
    try:
        encoded = json.dumps(value, allow_nan=False, ensure_ascii=False, indent=2, sort_keys=True)
        content = (encoded + "\n").encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError) as error:
        raise SchemaToolError("value cannot be encoded as canonical JSON") from error
    if len(content) > MAX_ARTIFACT_BYTES:
        fail("generated artifact exceeds the per-file byte limit")
    return content


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


@budgeted
def atomic_write(path: Path, content: bytes) -> None:
    active_budget().checkpoint()
    if len(content) > MAX_ARTIFACT_BYTES:
        fail("generated artifact exceeds the per-file byte limit")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


@budgeted
def read_selection() -> Selection:
    raw = load_json(SELECTION_PATH)
    if not isinstance(raw, dict) or raw.get("formatVersion") not in {1, 2}:
        fail("unsupported Codex schema selection format")
    protocol = raw.get("protocolFamily")
    arguments = raw.get("generatorArguments")
    roots = raw.get("roots")
    catalog = raw.get("notificationCatalog")
    if (
        not isinstance(protocol, str)
        or re.fullmatch(r"[A-Za-z0-9+./_-]{1,128}", protocol) is None
    ):
        fail("schema selection has no protocol family")
    if (
        not isinstance(arguments, list)
        or len(arguments) > MAX_GENERATOR_ARGUMENTS
        or not all(
            isinstance(value, str)
            and re.fullmatch(r"[A-Za-z0-9_./<>-]{1,256}", value) is not None
            for value in arguments
        )
    ):
        fail("schema selection has invalid generator arguments")
    if arguments.count("<temporary-directory>") != 1:
        fail("schema selection must contain one temporary output placeholder")
    if not isinstance(roots, list) or not roots or len(roots) > MAX_SELECTED_ROOTS:
        fail("schema selection has no roots")
    selected: list[SelectionRoot] = []
    names: set[str] = set()
    paths: set[str] = set()
    for raw_root in roots:
        active_budget().checkpoint()
        if not isinstance(raw_root, dict):
            fail("schema selection contains an invalid root")
        name = raw_root.get("name")
        relative = raw_root.get("path")
        since_version = raw_root.get("sinceVersion")
        if (
            not isinstance(name, str)
            or re.fullmatch(r"[a-z][a-z0-9_.]{0,127}", name) is None
            or not isinstance(relative, str)
            or re.fullmatch(r"[A-Za-z0-9_./-]{1,512}", relative) is None
            or (since_version is not None and not is_version(since_version))
        ):
            fail("schema selection contains an invalid root")
        candidate = Path(relative)
        if candidate.is_absolute() or ".." in candidate.parts or candidate.suffix != ".json":
            fail("schema selection contains an unsafe root path")
        if name in names or relative in paths:
            fail("schema selection contains a duplicate root")
        names.add(name)
        paths.add(relative)
        selected.append(SelectionRoot(name, relative, since_version))
    if (
        not isinstance(catalog, str)
        or re.fullmatch(r"[A-Za-z0-9_.-]{1,255}", catalog) is None
        or Path(catalog).name != catalog
    ):
        fail("schema selection has an invalid notification catalog")
    return Selection(protocol, tuple(arguments), tuple(selected), catalog)


def selected_roots_for_version(selection: Selection, version: str) -> tuple[SelectionRoot, ...]:
    if not is_version(version):
        fail("Codex version is not a stable X.Y.Z value")
    current = version_key(version)
    return tuple(
        root
        for root in selection.roots
        if root.since_version is None or version_key(root.since_version) <= current
    )


@budgeted
def read_policy() -> dict[str, Any]:
    raw = load_json(POLICY_PATH)
    if not isinstance(raw, dict) or raw.get("formatVersion") != 1:
        fail("unsupported Codex support policy format")
    required = (
        "protocolFamily",
        "selectedWireVersion",
        "supportedVersions",
        "candidateVersions",
    )
    if any(key not in raw for key in required):
        fail("Codex support policy is incomplete")
    for key in ("supportedVersions", "candidateVersions"):
        versions = raw[key]
        if (
            not isinstance(versions, list)
            or len(versions) > MAX_TRACKED_VERSIONS
            or not all(is_version(value) for value in versions)
        ):
            fail(f"Codex support policy has invalid {key}")
        if versions != sorted(set(versions), key=version_key):
            fail(f"Codex support policy {key} must be sorted and unique")
    if len(raw["supportedVersions"]) + len(raw["candidateVersions"]) > MAX_TRACKED_VERSIONS:
        fail("Codex support policy tracks too many versions")
    if raw["selectedWireVersion"] not in raw["supportedVersions"]:
        fail("selected wire version is not supported")
    if set(raw["supportedVersions"]) & set(raw["candidateVersions"]):
        fail("supported and candidate Codex versions overlap")
    return raw


@budgeted
def read_history(path: Path = HISTORY_PATH) -> dict[str, Any]:
    raw = load_json(path)
    if not isinstance(raw, dict) or raw.get("formatVersion") != HISTORY_FORMAT_VERSION:
        fail("unsupported Codex support history format")
    if raw.get("establishedBaselineVersion") != ESTABLISHED_BASELINE_VERSION:
        fail("Codex support history changed the established baseline")
    releases = raw.get("releases")
    if not isinstance(releases, list) or not releases or len(releases) > MAX_TRACKED_VERSIONS:
        fail("Codex support history has no releases")
    seen: set[str] = set()
    for release in releases:
        active_budget().checkpoint()
        if not isinstance(release, dict) or release.get("decision") != "supported":
            fail("Codex support history contains an invalid decision")
        version = release.get("version")
        if not is_version(version) or version in seen:
            fail("Codex support history contains an invalid version")
        seen.add(version)
        for key in ("schemaSha256", "contractSha256", "rustWireSha256"):
            if not isinstance(release.get(key), str) or re.fullmatch(r"[0-9a-f]{64}", release[key]) is None:
                fail(f"Codex support history has an invalid {key}")
        review_sha = release.get("compatibilityReviewSha256")
        if review_sha is not None and (
            not isinstance(review_sha, str)
            or re.fullmatch(r"[0-9a-f]{64}", review_sha) is None
        ):
            fail("Codex support history has an invalid compatibilityReviewSha256")
    baseline = next(
        (release for release in releases if release["version"] == ESTABLISHED_BASELINE_VERSION),
        None,
    )
    if baseline is None or baseline["schemaSha256"] != ESTABLISHED_BASELINE_SCHEMA_SHA256:
        fail("Codex support history no longer contains the pinned baseline schema")
    return raw


@budgeted
def verify_history_append_only(previous_path: Path) -> None:
    previous = read_history(previous_path)
    current = read_history()
    if previous.get("protocolFamily") != current.get("protocolFamily"):
        fail("Codex support history protocol family changed")
    old_releases = previous["releases"]
    if current["releases"][: len(old_releases)] != old_releases:
        fail("Codex support history is not append-only")


def is_version(value: Any) -> bool:
    if not isinstance(value, str) or len(value) > 64:
        return False
    match = re.fullmatch(
        r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)",
        value,
    )
    return match is not None and all(int(part) <= 2**64 - 1 for part in match.groups())


def version_key(value: str) -> tuple[int, int, int]:
    return tuple(int(part) for part in value.split("."))  # type: ignore[return-value]


def rust_version_module(version: str) -> str:
    if not is_version(version):
        fail("Codex version is not a stable X.Y.Z value")
    return "v" + version.replace(".", "_")


def private_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=False)
        if os.name == "posix":
            path.chmod(0o700)
            if path.stat().st_mode & 0o077:
                fail("isolated Codex directory permissions are not private")
    except SchemaToolError:
        raise
    except OSError as error:
        raise SchemaToolError("isolated Codex directory could not be created") from error


@budgeted
def isolated_codex_context(root: Path) -> CodexExecutionContext:
    """Create the only profile, temp roots, and cwd visible to Codex export."""
    active_budget().checkpoint()
    try:
        if os.name == "posix":
            root.chmod(0o700)
            if root.stat().st_mode & 0o077:
                fail("isolated Codex root permissions are not private")
    except SchemaToolError:
        raise
    except OSError as error:
        raise SchemaToolError("isolated Codex root could not be secured") from error
    cwd = root / "work"
    codex_home = root / "codex-home"
    home = root / "home"
    temporary = root / "tmp"
    config = root / "config"
    data = root / "data"
    cache = root / "cache"
    runtime = root / "runtime"
    app_data = root / "app-data"
    local_app_data = root / "local-app-data"
    for path in (
        cwd,
        codex_home,
        home,
        temporary,
        config,
        data,
        cache,
        runtime,
        app_data,
        local_app_data,
    ):
        private_directory(path)

    environment: dict[str, str] = {}
    # PATH is required by the npm launcher (`#!/usr/bin/env node`). The Windows
    # loader also relies on these static platform variables. Everything else,
    # including proxies, auth, Python/Node options, and the real profile, is
    # deliberately absent.
    for key in ("PATH", "SystemRoot", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT"):
        value = os.environ.get(key)
        if value:
            environment[key] = value
    environment.update(
        {
            "CODEX_HOME": os.fspath(codex_home),
            "HOME": os.fspath(home),
            "USERPROFILE": os.fspath(home),
            "XDG_CONFIG_HOME": os.fspath(config),
            "XDG_DATA_HOME": os.fspath(data),
            "XDG_CACHE_HOME": os.fspath(cache),
            "XDG_RUNTIME_DIR": os.fspath(runtime),
            "APPDATA": os.fspath(app_data),
            "LOCALAPPDATA": os.fspath(local_app_data),
            "TMPDIR": os.fspath(temporary),
            "TMP": os.fspath(temporary),
            "TEMP": os.fspath(temporary),
            "NO_COLOR": "1",
        }
    )
    if os.name == "posix":
        environment.update({"LANG": "C", "LC_ALL": "C"})
    return CodexExecutionContext(cwd=cwd, codex_home=codex_home, environment=environment)


@budgeted
def probe_version_in_context(binary: Path, context: CodexExecutionContext) -> str:
    try:
        result = run_bounded(
            [os.fspath(binary), "--version"],
            timeout=VERSION_TIMEOUT_SECONDS,
            cwd=context.cwd,
            environment=context.environment,
        )
    except SchemaToolError as error:
        raise SchemaToolError("Codex version probe could not complete") from error
    if result.timed_out:
        fail("Codex version probe timed out")
    if result.overflowed:
        fail("Codex version output exceeded the bounded capture limit")
    if result.returncode != 0:
        fail(f"Codex version probe exited unsuccessfully (code {result.returncode})")
    if result.stderr:
        fail("Codex version probe wrote unexpected stderr output")
    match = VERSION_RE.fullmatch(result.stdout)
    if match is None:
        fail("Codex version output must exactly match `codex-cli X.Y.Z`")
    version = ".".join(part.decode("ascii") for part in match.groups())
    if not is_version(version):
        fail("Codex version output must exactly match a bounded `codex-cli X.Y.Z`")
    return version


@budgeted
def probe_version(binary: Path) -> str:
    try:
        with tempfile.TemporaryDirectory(prefix="lark-codex-version-") as directory:
            context = isolated_codex_context(Path(directory))
            return probe_version_in_context(binary.resolve(), context)
    except OSError as error:
        raise SchemaToolError("Codex version probe could not prepare isolation") from error


@budgeted
def generate_schema_directory(
    binary: Path, selection: Selection
) -> tuple[str, Path, tempfile.TemporaryDirectory[str]]:
    temporary = tempfile.TemporaryDirectory(prefix="lark-codex-schema-")
    try:
        context = isolated_codex_context(Path(temporary.name))
        resolved_binary = binary.resolve(strict=True)
        version = probe_version_in_context(resolved_binary, context)
        output = Path(temporary.name) / "export"
        arguments = [
            str(output) if value == "<temporary-directory>" else value
            for value in selection.generator_arguments
        ]
        result = run_bounded(
            [os.fspath(resolved_binary), *arguments],
            timeout=GENERATION_TIMEOUT_SECONDS,
            cwd=context.cwd,
            environment=context.environment,
        )
    except (OSError, SchemaToolError) as error:
        temporary.cleanup()
        raise SchemaToolError("Codex schema export could not complete") from error
    if result.timed_out:
        temporary.cleanup()
        fail(f"Codex {version} schema export timed out")
    if result.overflowed:
        temporary.cleanup()
        fail(f"Codex {version} schema export exceeded the bounded diagnostic limit")
    if result.returncode != 0:
        temporary.cleanup()
        fail(f"Codex {version} schema export failed (code {result.returncode})")
    try:
        expected_output = Path(temporary.name).resolve(strict=True) / "export"
        output_is_isolated = output.is_dir() and output.resolve(strict=True) == expected_output
    except OSError:
        output_is_isolated = False
    if not output_is_isolated:
        temporary.cleanup()
        fail(f"Codex {version} schema export produced no isolated output directory")
    return version, output, temporary


@budgeted
def normalize_schema(value: Any, parent_key: str | None = None) -> Any:
    if isinstance(value, dict):
        return {key: normalize_schema(value[key], key) for key in sorted(value)}
    if isinstance(value, list):
        normalized = [normalize_schema(item) for item in value]
        if parent_key in {"required", "enum", "type"}:
            normalized.sort(key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))
        return normalized
    return value


@budgeted
def notification_methods(schema: Any) -> list[str]:
    if not isinstance(schema, dict):
        fail("notification catalog is not a JSON Schema object")
    methods: set[str] = set()
    variants = schema.get("oneOf", [])
    if not isinstance(variants, list):
        fail("notification catalog has no oneOf variants")
    for variant in variants:
        active_budget().checkpoint()
        try:
            values = variant["properties"]["method"]["enum"]
        except (KeyError, TypeError):
            continue
        if isinstance(values, list):
            methods.update(value for value in values if isinstance(value, str))
    if not methods:
        fail("notification catalog contains no methods")
    return sorted(methods)


@budgeted
def make_bundle(export: Path, selection: Selection, version: str) -> dict[str, Any]:
    roots: dict[str, Any] = {}
    for root in selected_roots_for_version(selection, version):
        active_budget().checkpoint()
        source = isolated_export_file(export, root.path)
        roots[root.name] = normalize_schema(load_json(source))
    catalog_path = isolated_export_file(export, selection.notification_catalog)
    catalog = normalize_schema(load_json(catalog_path))
    bundle = {
        "formatVersion": SCHEMA_BUNDLE_FORMAT_VERSION,
        "notificationMethods": notification_methods(catalog),
        "roots": roots,
    }
    validate_bundle_schema_shapes(roots)
    return bundle


@budgeted
def isolated_export_file(export: Path, relative: str) -> Path:
    """Resolve one regular export file without allowing link-based escape."""
    candidate = export / relative
    try:
        export_root = export.resolve(strict=True)
        resolved = candidate.resolve(strict=True)
    except OSError as error:
        raise SchemaToolError("Codex schema export omitted a selected artifact") from error
    expected = export_root.joinpath(*Path(relative).parts)
    if resolved != expected or not candidate.is_file() or candidate.is_symlink():
        fail("Codex schema export contains an unsafe selected artifact")
    return candidate


def camel_to_snake(value: str) -> str:
    first = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", first).replace("-", "_").lower()


@budgeted
def generated_value_fields(properties: dict[str, Any], required: set[str], excluded: set[str]) -> str:
    lines: list[str] = []
    for wire_name in sorted(set(properties) - excluded):
        active_budget().checkpoint()
        rust_name = camel_to_snake(wire_name)
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", rust_name):
            fail("selected schema contains a field the Rust generator cannot name safely")
        if wire_name in required:
            lines.extend(
                [
                    f'    #[serde(rename = "{wire_name}")]',
                    f"    pub {rust_name}: Value,",
                ]
            )
        else:
            lines.extend(
                [
                    f'    #[serde(default, rename = "{wire_name}", skip_serializing_if = "Option::is_none")]',
                    f"    pub {rust_name}: Option<Value>,",
                ]
            )
    return "\n".join(lines)


@budgeted
def render_wire(version: str, protocol_family: str, schema_sha: str, bundle: dict[str, Any]) -> bytes:
    try:
        template = read_bounded_bytes(WIRE_TEMPLATE_PATH).decode("utf-8")
    except UnicodeError as error:
        raise SchemaToolError("wire template is unavailable") from error
    roots = bundle["roots"]
    thread_response = roots["thread.start.response"]
    thread_schema = thread_response["definitions"]["Thread"]
    thread_properties = thread_schema.get("properties", {})
    thread_required = set(thread_schema.get("required", []))
    if not isinstance(thread_properties, dict):
        fail("selected Thread schema has no object properties")
    base_thread_fields = {
        "id",
        "sessionId",
        "preview",
        "modelProvider",
        "createdAt",
        "updatedAt",
        "status",
        "ephemeral",
        "turns",
        "source",
        "cliVersion",
        "cwd",
        "name",
        "path",
        "forkedFromId",
        "parentThreadId",
    }
    thread_fields = generated_value_fields(thread_properties, thread_required, base_thread_fields)

    list_schema = roots["thread.list.params"]
    list_properties = list_schema.get("properties", {})
    if not isinstance(list_properties, dict):
        fail("selected ThreadListParams schema has no object properties")
    base_list_fields = {
        "cursor",
        "limit",
        "sortKey",
        "sortDirection",
        "modelProviders",
        "sourceKinds",
        "cwd",
        "archived",
        "searchTerm",
        "useStateDbOnly",
    }
    list_fields = generated_value_fields(list_properties, set(list_schema.get("required", [])), base_list_fields)

    shared_wire = ""
    if "thread.unsubscribe.params" in roots:
        try:
            shared_wire = read_bounded_bytes(SHARED_WIRE_TEMPLATE_PATH).decode("utf-8")
        except UnicodeError as error:
            raise SchemaToolError("shared wire template is unavailable") from error

    replacements = {
        "@GENERATOR_VERSION@": LEGACY_GENERATOR_VERSIONS.get(version, GENERATOR_VERSION),
        "@CODEX_VERSION@": version,
        "@PROTOCOL_FAMILY@": protocol_family,
        "@SCHEMA_SHA256@": schema_sha,
        "@THREAD_VERSION_FIELDS@": thread_fields,
        "@THREAD_LIST_VERSION_FIELDS@": list_fields,
        "@SHARED_WIRE_TYPES@": shared_wire,
    }
    rendered = template
    for marker, value in replacements.items():
        rendered = rendered.replace(marker, value)
    if re.search(r"@[A-Z][A-Z0-9_]+@", rendered):
        fail("wire template contains an unresolved generator marker")
    content = rendered.encode("utf-8")
    if len(content) > MAX_ARTIFACT_BYTES:
        fail("generated Rust wire artifact exceeds the per-file byte limit")
    return content


@budgeted
def template_sha256(version: str) -> str:
    if version in LEGACY_TEMPLATE_SHA256:
        return LEGACY_TEMPLATE_SHA256[version]
    return sha256_bytes(
        read_bounded_bytes(WIRE_TEMPLATE_PATH)
        + b"\0shared-wire-template\0"
        + read_bounded_bytes(SHARED_WIRE_TEMPLATE_PATH)
    )


@budgeted
def manifest_for(
    version: str,
    selection: Selection,
    schema_bytes: bytes,
    wire_bytes: bytes,
    audit_bytes: bytes,
    policy: dict[str, Any],
) -> dict[str, Any]:
    module = rust_version_module(version)
    if version in policy["supportedVersions"]:
        lifecycle = "supported"
    elif version in policy["candidateVersions"]:
        lifecycle = "candidate"
    else:
        lifecycle = "unclassified"
    return {
        "formatVersion": MANIFEST_FORMAT_VERSION,
        "codexVersion": version,
        "protocolFamily": selection.protocol_family,
        "schemaSha256": sha256_bytes(schema_bytes),
        "generator": {
            "name": GENERATOR_NAME,
            "version": LEGACY_GENERATOR_VERSIONS.get(version, GENERATOR_VERSION),
            "templateSha256": template_sha256(version),
        },
        "generationArguments": list(selection.generator_arguments),
        "lifecycle": lifecycle,
        "selectedRoots": [
            root.name for root in selected_roots_for_version(selection, version)
        ],
        "artifacts": {
            "incomingAudit": f"protocol/codex/schemas/{version}/incoming-audit.json",
            "normalizedSchema": f"protocol/codex/schemas/{version}/selected.schema.json",
            "rustWire": f"src/codex/wire/{module}.rs",
        },
        "artifactSha256": {
            "incomingAudit": sha256_bytes(audit_bytes),
            "normalizedSchema": sha256_bytes(schema_bytes),
            "rustWire": sha256_bytes(wire_bytes),
        },
    }


def is_incoming_root(name: str) -> bool:
    return (
        name.endswith(".response")
        or name.startswith("notification.")
        or (name.startswith("server_request.") and name.endswith(".params"))
    )


@budgeted
def incoming_audit(version: str, bundle: dict[str, Any]) -> dict[str, Any]:
    entries: dict[tuple[str, str], dict[str, Any]] = {}

    def walk(value: Any, path: str) -> None:
        active_budget().checkpoint()
        if isinstance(value, dict):
            logical_path = path.split("/definitions/", 1)[-1]
            if logical_path != path:
                logical_path = "definitions/" + logical_path
            for kind in ("enum", "oneOf", "anyOf"):
                if isinstance(value.get(kind), list):
                    open_name = next(
                        (
                            name
                            for name in ("TurnStatus", "MessagePhase")
                            if f"definitions/{name}" in logical_path
                        ),
                        None,
                    )
                    thread_item = logical_path == "definitions/ThreadItem" and kind in {
                        "oneOf",
                        "anyOf",
                    }
                    if open_name is not None:
                        handling = "open-string-fallback"
                        evidence = "unknown_generated_enum_values_fail_soft_at_the_stable_boundary"
                    elif thread_item:
                        handling = "open-tagged-fallback"
                        evidence = "unknown_thread_items_preserve_the_complete_raw_payload"
                    else:
                        handling = "promotion-blocking"
                        evidence = "incoming_closed_union_additions_are_breaking"
                    key = (logical_path, kind)
                    entries[key] = {
                        "schemaPath": logical_path,
                        "construct": kind,
                        "handling": handling,
                        "evidence": evidence,
                    }
            for key, child in value.items():
                if key not in {"description", "title", "default", "examples"}:
                    walk(child, f"{path}/{key}")
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, f"{path}/{index}")

    for name, schema in bundle["roots"].items():
        if is_incoming_root(name):
            walk(schema, f"roots/{name}")
    ordered = sorted(entries.values(), key=lambda item: (item["schemaPath"], item["construct"]))
    return {
        "formatVersion": AUDIT_FORMAT_VERSION,
        "codexVersion": version,
        "incomingRoots": sorted(name for name in bundle["roots"] if is_incoming_root(name)),
        "constructs": ordered,
    }


@budgeted
def render_wire_mod(policy: dict[str, Any], versions: Iterable[str]) -> bytes:
    module_set: set[str] = set()
    for index, version in enumerate(versions, start=1):
        active_budget().checkpoint()
        if index > MAX_TRACKED_VERSIONS or not is_version(version):
            fail("generated wire registry has an invalid version set")
        module_set.add(version)
    modules = sorted(module_set, key=version_key)
    if len(modules) > MAX_TRACKED_VERSIONS:
        fail("generated wire registry tracks too many versions")
    lines = [
        "// @generated by tools/codex_schema.py; DO NOT EDIT.",
        "//! Versioned Codex app-server wire DTOs. Stable domain types live in `types`.",
        "",
    ]
    for version in modules:
        active_budget().checkpoint()
        lines.extend(["#[rustfmt::skip]", f"pub mod {rust_version_module(version)};"])
    lines.extend(
        [
            "",
            "/// Exact versions whose schema and contracts have passed review.",
        ]
    )
    quoted_versions = ", ".join(f'"{version}"' for version in policy["supportedVersions"])
    version_tuples = ", ".join(
        f"({major}, {minor}, {patch})"
        for major, minor, patch in (version_key(version) for version in policy["supportedVersions"])
    )
    lines.extend(
        [
            f"pub const SUPPORTED_CODEX_VERSIONS: &[&str] = &[{quoted_versions}];",
            f"const SUPPORTED_CODEX_VERSION_TRIPLES: &[(u64, u64, u64)] = &[{version_tuples}];",
            "",
            "/// Returns true only for an exact, reviewed schema/contract version.",
            "#[must_use]",
            "pub fn is_supported_codex_version(version: &semver::Version) -> bool {",
            "    SUPPORTED_CODEX_VERSION_TRIPLES.contains(&(version.major, version.minor, version.patch))",
            "        && version.pre.is_empty()",
            "        && version.build.is_empty()",
            "}",
            "",
        ]
    )
    content = "\n".join(lines).encode("utf-8")
    if len(content) > MAX_ARTIFACT_BYTES:
        fail("generated Rust module registry exceeds the per-file byte limit")
    return content


@budgeted
def existing_wire_versions(extra: str | None = None) -> list[str]:
    versions: set[str] = set()
    if SCHEMAS_ROOT.is_dir():
        for index, child in enumerate(SCHEMAS_ROOT.iterdir(), start=1):
            active_budget().checkpoint()
            if index > MAX_DIRECTORY_ENTRIES:
                fail("schema artifact directory contains too many entries")
            if child.is_dir() and is_version(child.name) and (child / "manifest.json").is_file():
                versions.add(child.name)
    if extra is not None:
        versions.add(extra)
    if len(versions) > MAX_TRACKED_VERSIONS:
        fail("schema artifact directory tracks too many versions")
    return sorted(versions, key=version_key)


JSON_SCHEMA_TYPES = frozenset(
    {"array", "boolean", "integer", "null", "number", "object", "string"}
)
SCHEMA_MAP_KEYWORDS = frozenset(
    {
        "$defs",
        "definitions",
        "dependentSchemas",
        "patternProperties",
        "properties",
    }
)
SCHEMA_VALUE_KEYWORDS = frozenset(
    {
        "additionalItems",
        "additionalProperties",
        "contains",
        "contentSchema",
        "else",
        "if",
        "not",
        "propertyNames",
        "then",
        "unevaluatedItems",
        "unevaluatedProperties",
    }
)
SCHEMA_ARRAY_KEYWORDS = frozenset({"allOf", "anyOf", "oneOf"})
STRING_KEYWORDS = frozenset(
    {
        "$anchor",
        "$comment",
        "$dynamicAnchor",
        "$dynamicRef",
        "$id",
        "$recursiveRef",
        "$ref",
        "$schema",
        "contentEncoding",
        "contentMediaType",
        "description",
        "format",
        "id",
        "pattern",
        "title",
    }
)
BOOLEAN_KEYWORDS = frozenset(
    {
        "$recursiveAnchor",
        "deprecated",
        "readOnly",
        "uniqueItems",
        "writeOnly",
    }
)
NUMBER_KEYWORDS = frozenset(
    {"exclusiveMaximum", "exclusiveMinimum", "maximum", "minimum"}
)
NONNEGATIVE_INTEGER_KEYWORDS = frozenset(
    {
        "maxContains",
        "maxItems",
        "maxLength",
        "maxProperties",
        "minContains",
        "minItems",
        "minLength",
        "minProperties",
    }
)


def invalid_schema_keyword_shape() -> NoReturn:
    fail("normalized schema contains an invalid JSON Schema keyword shape")


def is_json_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and (not isinstance(value, float) or math.isfinite(value))
    )


def is_nonnegative_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def unique_strings(value: Any, *, require_nonempty: bool) -> bool:
    if not isinstance(value, list) or (require_nonempty and not value):
        return False
    seen: set[str] = set()
    for item in value:
        active_budget().checkpoint()
        if not isinstance(item, str) or item in seen:
            return False
        seen.add(item)
    return True


@budgeted
def validate_schema_keyword_shapes(root: Any) -> None:
    """Validate every recognized schema keyword before any semantic consumer."""
    stack: list[tuple[Any, int]] = [(root, 1)]
    while stack:
        schema, depth = stack.pop()
        active_budget().checkpoint()
        if depth > SCHEMA_VALIDATION_RECURSION_LIMIT:
            invalid_schema_keyword_shape()
        if isinstance(schema, bool):
            continue
        if not isinstance(schema, dict) or not all(isinstance(key, str) for key in schema):
            invalid_schema_keyword_shape()

        def push_schema(value: Any) -> None:
            if not isinstance(value, (dict, bool)):
                invalid_schema_keyword_shape()
            stack.append((value, depth + 1))

        for keyword in sorted(STRING_KEYWORDS & schema.keys()):
            value = schema[keyword]
            if not isinstance(value, str):
                invalid_schema_keyword_shape()
            if keyword == "pattern":
                try:
                    bounded_regex_search(value, "")
                except ValidationFailure:
                    invalid_schema_keyword_shape()

        for keyword in sorted(BOOLEAN_KEYWORDS & schema.keys()):
            if not isinstance(schema[keyword], bool):
                invalid_schema_keyword_shape()

        for keyword in sorted(NUMBER_KEYWORDS & schema.keys()):
            if not is_json_number(schema[keyword]):
                invalid_schema_keyword_shape()

        for keyword in sorted(NONNEGATIVE_INTEGER_KEYWORDS & schema.keys()):
            if not is_nonnegative_integer(schema[keyword]):
                invalid_schema_keyword_shape()

        if "multipleOf" in schema:
            multiple = schema["multipleOf"]
            if not is_json_number(multiple) or multiple <= 0:
                invalid_schema_keyword_shape()

        if "type" in schema:
            raw_type = schema["type"]
            if isinstance(raw_type, str):
                valid_type = raw_type in JSON_SCHEMA_TYPES
            else:
                valid_type = (
                    unique_strings(raw_type, require_nonempty=True)
                    and all(value in JSON_SCHEMA_TYPES for value in raw_type)
                )
            if not valid_type:
                invalid_schema_keyword_shape()

        if "enum" in schema:
            values = schema["enum"]
            if not isinstance(values, list) or not values:
                invalid_schema_keyword_shape()
            semantic_values = [semantic_json_key(value) for value in values]
            if len(semantic_values) != len(set(semantic_values)):
                invalid_schema_keyword_shape()

        if "required" in schema and not unique_strings(
            schema["required"], require_nonempty=False
        ):
            invalid_schema_keyword_shape()

        if "examples" in schema and not isinstance(schema["examples"], list):
            invalid_schema_keyword_shape()

        vocabulary = schema.get("$vocabulary")
        if "$vocabulary" in schema and (
            not isinstance(vocabulary, dict)
            or not all(
                isinstance(uri, str) and isinstance(required, bool)
                for uri, required in vocabulary.items()
            )
        ):
            invalid_schema_keyword_shape()

        for keyword in sorted(SCHEMA_MAP_KEYWORDS & schema.keys()):
            mapping = schema[keyword]
            if not isinstance(mapping, dict) or not all(
                isinstance(name, str) for name in mapping
            ):
                invalid_schema_keyword_shape()
            for name in sorted(mapping):
                active_budget().checkpoint()
                if keyword == "patternProperties":
                    try:
                        bounded_regex_search(name, "")
                    except ValidationFailure:
                        invalid_schema_keyword_shape()
                push_schema(mapping[name])

        dependencies = schema.get("dependencies")
        if "dependencies" in schema:
            if not isinstance(dependencies, dict) or not all(
                isinstance(name, str) for name in dependencies
            ):
                invalid_schema_keyword_shape()
            for name in sorted(dependencies):
                active_budget().checkpoint()
                dependency = dependencies[name]
                if isinstance(dependency, list):
                    if not unique_strings(dependency, require_nonempty=True):
                        invalid_schema_keyword_shape()
                else:
                    push_schema(dependency)

        dependent_required = schema.get("dependentRequired")
        if "dependentRequired" in schema:
            if not isinstance(dependent_required, dict) or not all(
                isinstance(name, str) for name in dependent_required
            ):
                invalid_schema_keyword_shape()
            for name in sorted(dependent_required):
                active_budget().checkpoint()
                if not unique_strings(
                    dependent_required[name], require_nonempty=False
                ):
                    invalid_schema_keyword_shape()

        for keyword in sorted(SCHEMA_VALUE_KEYWORDS & schema.keys()):
            push_schema(schema[keyword])

        items = schema.get("items")
        if "items" in schema:
            if isinstance(items, list):
                if not items:
                    invalid_schema_keyword_shape()
                for child in items:
                    push_schema(child)
            else:
                push_schema(items)

        prefix_items = schema.get("prefixItems")
        if "prefixItems" in schema:
            if not isinstance(prefix_items, list):
                invalid_schema_keyword_shape()
            for child in prefix_items:
                push_schema(child)

        for keyword in sorted(SCHEMA_ARRAY_KEYWORDS & schema.keys()):
            variants = schema[keyword]
            if not isinstance(variants, list) or not variants:
                invalid_schema_keyword_shape()
            for child in variants:
                push_schema(child)


def validate_bundle_schema_shapes(roots: dict[str, Any]) -> None:
    for name in sorted(roots):
        active_budget().checkpoint()
        validate_schema_keyword_shapes(roots[name])


@budgeted
def sync(binary: Path, *, check: bool) -> str:
    selection = read_selection()
    policy = read_policy()
    version, export, temporary = generate_schema_directory(binary, selection)
    try:
        bundle = make_bundle(export, selection, version)
        schema_bytes = canonical_bytes(bundle)
        schema_sha = sha256_bytes(schema_bytes)
        wire_bytes = render_wire(version, selection.protocol_family, schema_sha, bundle)
        audit_bytes = canonical_bytes(incoming_audit(version, bundle))
        manifest = manifest_for(version, selection, schema_bytes, wire_bytes, audit_bytes, policy)
        manifest_bytes = canonical_bytes(manifest)
        module = rust_version_module(version)
        targets = {
            SCHEMAS_ROOT / version / "incoming-audit.json": audit_bytes,
            SCHEMAS_ROOT / version / "selected.schema.json": schema_bytes,
            SCHEMAS_ROOT / version / "manifest.json": manifest_bytes,
            WIRE_ROOT / f"{module}.rs": wire_bytes,
            WIRE_ROOT / "mod.rs": render_wire_mod(policy, existing_wire_versions(version)),
        }
        if check:
            mismatches = []
            for path, expected in targets.items():
                active_budget().checkpoint()
                actual = read_bounded_bytes(path) if path.is_file() else b""
                if actual != expected:
                    mismatches.append(safe_relative(path))
            if mismatches:
                fail("schema sync is not reproducible for: " + ", ".join(mismatches))
        else:
            for path, content in targets.items():
                atomic_write(path, content)
        return version
    finally:
        temporary.cleanup()


@budgeted
def load_bundle(version: str) -> dict[str, Any]:
    if not is_version(version):
        fail("Codex version is not a stable X.Y.Z value")
    bundle = load_json(SCHEMAS_ROOT / version / "selected.schema.json")
    if not isinstance(bundle, dict) or bundle.get("formatVersion") != SCHEMA_BUNDLE_FORMAT_VERSION:
        fail(f"Codex {version} has an unsupported normalized schema format")
    if not isinstance(bundle.get("roots"), dict) or not isinstance(bundle.get("notificationMethods"), list):
        fail(f"Codex {version} has an incomplete normalized schema")
    if len(bundle["roots"]) > MAX_SELECTED_ROOTS:
        fail(f"Codex {version} normalized schema selects too many roots")
    if len(bundle["notificationMethods"]) > MAX_CLASSIFIED_CHANGES:
        fail(f"Codex {version} normalized schema contains too many notifications")
    if any(
        re.fullmatch(r"[a-z][a-z0-9_.]{0,127}", name) is None
        or not isinstance(schema, (dict, bool))
        for name, schema in bundle["roots"].items()
    ):
        fail(f"Codex {version} normalized schema contains an invalid root")
    notifications = bundle["notificationMethods"]
    if (
        not all(
            isinstance(method, str)
            and re.fullmatch(r"[A-Za-z0-9_./-]{1,256}", method) is not None
            for method in notifications
        )
        or notifications != sorted(set(notifications))
    ):
        fail(f"Codex {version} normalized schema contains an invalid notification catalog")
    validate_bundle_schema_shapes(bundle["roots"])
    return bundle


@budgeted
def change(classification: str, kind: str, path: str, **details: Any) -> dict[str, Any]:
    active_budget().consume_change()
    result = {"classification": classification, "kind": kind, "path": path}
    result.update(details)
    return result


def schema_types(schema: dict[str, Any]) -> set[str] | None:
    raw = schema.get("type")
    if isinstance(raw, str):
        return {raw}
    if isinstance(raw, list) and all(isinstance(value, str) for value in raw):
        return set(raw)
    return None


def type_atoms(types: set[str]) -> set[str]:
    atoms: set[str] = set()
    for value in types:
        if value == "number":
            atoms.update(("integer", "non_integer_number"))
        else:
            atoms.add(value)
    return atoms


@budgeted
def semantic_json_key(value: Any) -> str:
    active_budget().checkpoint()
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return f"{type(value).__name__}:{json.dumps(value, sort_keys=True)}"
    if isinstance(value, int):
        return f"number:{signed_hex(value)}/1"
    if isinstance(value, float):
        if not math.isfinite(value):
            fail("JSON number is not finite")
        numerator, denominator = value.as_integer_ratio()
        return f"number:{signed_hex(numerator)}/{denominator:x}"
    if isinstance(value, list):
        return "list:[" + ",".join(semantic_json_key(item) for item in value) + "]"
    if isinstance(value, dict):
        return "object:{" + ",".join(
            f"{json.dumps(key)}:{semantic_json_key(value[key])}" for key in sorted(value)
        ) + "}"
    return canonical_bytes(value).decode("utf-8")


def signed_hex(value: int) -> str:
    """Encode an arbitrary-size integer without decimal or float conversion."""
    sign = "-" if value < 0 else ""
    return f"{sign}{abs(value):x}"


def exact_number_fraction(value: int | float) -> Fraction:
    """Represent an accepted JSON number without coercing integers to floats."""
    if isinstance(value, int):
        return Fraction(value, 1)
    if not math.isfinite(value):
        fail("JSON number is not finite")
    numerator, denominator = value.as_integer_ratio()
    return Fraction(numerator, denominator)


def finite_values(schema: dict[str, Any]) -> list[Any] | None:
    values = schema.get("enum")
    if "const" not in schema:
        return values if isinstance(values, list) else None
    constant = schema["const"]
    if not isinstance(values, list):
        return [constant]
    constant_key = semantic_json_key(constant)
    return (
        [constant]
        if any(semantic_json_key(value) == constant_key for value in values)
        else []
    )


def classify_bound(
    before: dict[str, Any],
    after: dict[str, Any],
    path: str,
    changes: list[dict[str, Any]],
    key: str,
    *,
    minimum: bool,
) -> None:
    old = before.get(key)
    new = after.get(key)
    old_number = isinstance(old, (int, float)) and not isinstance(old, bool)
    new_number = isinstance(new, (int, float)) and not isinstance(new, bool)
    if not old_number and not new_number:
        return
    if not old_number:
        changes.append(change("breaking", f"{camel_to_snake(key)}_added", path, value=new))
    elif not new_number:
        changes.append(change("additive", f"{camel_to_snake(key)}_removed", path, value=old))
    elif old != new:
        narrows = new > old if minimum else new < old
        changes.append(
            change(
                "breaking" if narrows else "additive",
                f"{camel_to_snake(key)}_{'narrowed' if narrows else 'widened'}",
                path,
                before=old,
                after=new,
            )
        )


@budgeted
def branch_key(schema: Any) -> tuple[str, str] | None:
    if not isinstance(schema, dict):
        return None
    if isinstance(schema.get("$ref"), str):
        return ("ref", schema["$ref"])
    properties = schema.get("properties")
    required = schema.get("required", [])
    if isinstance(properties, dict) and isinstance(required, list):
        for name in sorted(properties):
            active_budget().checkpoint()
            child = properties[name]
            if (
                name not in required
                or not isinstance(child, dict)
                or "$ref" in child
            ):
                continue
            values = finite_values(child)
            if values is not None and len(values) == 1:
                return (f"tag:{name}", semantic_json_key(values[0]))
    types = schema_types(schema)
    if types is not None and len(type_atoms(types)) == 1:
        return ("type", next(iter(type_atoms(types))))
    return None


@budgeted
def branches_provably_disjoint(left: Any, right: Any) -> bool:
    if not isinstance(left, dict) or not isinstance(right, dict):
        return False
    if "$ref" in left or "$ref" in right:
        return False
    left_types = schema_types(left)
    right_types = schema_types(right)
    if left_types is not None and right_types is not None:
        if type_atoms(left_types).isdisjoint(type_atoms(right_types)):
            return True
    left_key = branch_key(left)
    right_key = branch_key(right)
    return (
        left_key is not None
        and right_key is not None
        and left_types is not None
        and right_types is not None
        and type_atoms(left_types) == {"object"}
        and type_atoms(right_types) == {"object"}
        and left_key[0].startswith("tag:")
        and left_key[0] == right_key[0]
        and left_key[1] != right_key[1]
    )


REFERENCE_KEYWORDS = ("$ref", "$dynamicRef", "$recursiveRef")
SYNTHETIC_ROOT_URI = "https://lark-codex.invalid/__root__.json"


def json_pointer_token(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def schema_child_locations(
    schema: dict[str, Any]
) -> Iterator[tuple[tuple[str, ...], Any]]:
    """Yield recognized child schemas and their JSON Pointer path components."""
    for keyword in sorted(SCHEMA_MAP_KEYWORDS & schema.keys()):
        mapping = schema[keyword]
        if isinstance(mapping, dict):
            for name in sorted(mapping):
                active_budget().checkpoint()
                yield (keyword, name), mapping[name]
    dependencies = schema.get("dependencies")
    if isinstance(dependencies, dict):
        for name in sorted(dependencies):
            active_budget().checkpoint()
            child = dependencies[name]
            if isinstance(child, (dict, bool)):
                yield ("dependencies", name), child
    for keyword in sorted(SCHEMA_VALUE_KEYWORDS & schema.keys()):
        yield (keyword,), schema[keyword]
    items = schema.get("items")
    if isinstance(items, list):
        for index, child in enumerate(items):
            active_budget().checkpoint()
            yield ("items", str(index)), child
    elif isinstance(items, (dict, bool)):
        yield ("items",), items
    prefix_items = schema.get("prefixItems")
    if isinstance(prefix_items, list):
        for index, child in enumerate(prefix_items):
            active_budget().checkpoint()
            yield ("prefixItems", str(index)), child
    for keyword in sorted(SCHEMA_ARRAY_KEYWORDS & schema.keys()):
        variants = schema[keyword]
        if isinstance(variants, list):
            for index, child in enumerate(variants):
                active_budget().checkpoint()
                yield (keyword, str(index)), child


@dataclass
class SchemaReferenceIndex:
    root: Any
    root_digest: str
    node_bases: dict[int, str | None]
    resources: dict[str, Any]
    ambiguous_resources: set[str]
    anchors: dict[str, Any]
    ambiguous_anchors: set[str]


def register_reference_target(
    targets: dict[str, Any], ambiguous: set[str], uri: str, schema: Any
) -> None:
    if uri in ambiguous:
        return
    existing = targets.get(uri)
    if existing is None:
        targets[uri] = schema
    elif existing is not schema:
        targets.pop(uri, None)
        ambiguous.add(uri)


@budgeted
def build_reference_index(root: Any) -> SchemaReferenceIndex:
    """Index bounded Draft-07 identifier scopes without fetching external data."""
    digest = sha256_bytes(canonical_bytes(root))
    node_bases: dict[int, str | None] = {}
    resources: dict[str, Any] = {}
    ambiguous_resources: set[str] = set()
    anchors: dict[str, Any] = {}
    ambiguous_anchors: set[str] = set()
    register_reference_target(
        resources, ambiguous_resources, SYNTHETIC_ROOT_URI, root
    )
    pending: list[tuple[Any, str, str, tuple[str, ...]]] = [
        (root, SYNTHETIC_ROOT_URI, SYNTHETIC_ROOT_URI, ())
    ]
    visited: set[tuple[int, str, str, tuple[str, ...]]] = set()
    while pending:
        active_budget().checkpoint()
        current, inherited_base, resource_uri, pointer = pending.pop()
        if isinstance(current, bool) or not isinstance(current, dict):
            continue
        visit = (id(current), inherited_base, resource_uri, pointer)
        if visit in visited:
            continue
        visited.add(visit)

        base_uri = inherited_base
        identifier = current.get("$id")
        if isinstance(identifier, str):
            base_uri = urljoin(inherited_base, identifier)
            identifier_resource, identifier_fragment = urldefrag(base_uri)
            if identifier_resource and identifier_resource != resource_uri:
                resource_uri = identifier_resource
                pointer = ()
                register_reference_target(
                    resources, ambiguous_resources, resource_uri, current
                )
            if identifier_fragment:
                register_reference_target(
                    anchors, ambiguous_anchors, base_uri, current
                )
            elif identifier_resource:
                register_reference_target(
                    resources, ambiguous_resources, identifier_resource, current
                )

        identity = id(current)
        existing_base = node_bases.get(identity, base_uri)
        node_bases[identity] = base_uri if existing_base == base_uri else None

        for keyword in ("$anchor", "$dynamicAnchor"):
            anchor = current.get(keyword)
            if isinstance(anchor, str):
                document_uri, _ = urldefrag(base_uri)
                register_reference_target(
                    anchors,
                    ambiguous_anchors,
                    f"{document_uri}#{anchor}",
                    current,
                )

        for child_tokens, child in schema_child_locations(current):
            if not isinstance(child, (dict, bool)):
                continue
            child_pointer = pointer + tuple(
                json_pointer_token(token) for token in child_tokens
            )
            pending.append((child, base_uri, resource_uri, child_pointer))
    return SchemaReferenceIndex(
        root=root,
        root_digest=digest,
        node_bases=node_bases,
        resources=resources,
        ambiguous_resources=ambiguous_resources,
        anchors=anchors,
        ambiguous_anchors=ambiguous_anchors,
    )


def reference_index(root: Any) -> SchemaReferenceIndex:
    budget = active_budget()
    key = id(root)
    cached = budget.reference_indexes.get(key)
    if cached is not None and cached[0] is root:
        return cached[1]
    index = build_reference_index(root)
    budget.reference_indexes[key] = (root, index)
    return index


def resolve_indexed_reference(
    index: SchemaReferenceIndex, source: Any, reference: str
) -> tuple[str, Any]:
    base_uri = index.node_bases.get(id(source))
    if base_uri is None:
        raise ValidationFailure("schema reference base is ambiguous")
    absolute = urljoin(base_uri, reference)
    resource_uri, fragment = urldefrag(absolute)
    if resource_uri in index.ambiguous_resources:
        raise ValidationFailure("schema reference resource is ambiguous")
    resource = index.resources.get(resource_uri)
    if not fragment:
        if not isinstance(resource, (dict, bool)):
            raise ValidationFailure("schema reference resource is unresolved")
        return absolute, resource
    if fragment.startswith("/"):
        if not isinstance(resource, (dict, bool)):
            raise ValidationFailure("schema reference resource is unresolved")
        return absolute, resolve_pointer(resource, f"#{unquote(fragment)}")
    if absolute in index.ambiguous_anchors:
        raise ValidationFailure("schema reference anchor is ambiguous")
    target = index.anchors.get(absolute)
    if not isinstance(target, (dict, bool)):
        raise ValidationFailure("schema reference anchor is unresolved")
    return absolute, target


@budgeted
def schema_reference_dependencies(schema: Any, root: Any) -> tuple[tuple[str, str], ...]:
    """Return compact fingerprints for the transitive reference closure.

    Static Draft-07 URI references are resolved within the selected root. Dynamic,
    recursive, ambiguous, and external references are deliberately tied to a
    compact whole-root digest so any potentially retargeting edit fails closed.
    """
    budget = active_budget()
    cache_key = (id(schema), id(root))
    cached = budget.reference_fingerprints.get(cache_key)
    if cached is not None:
        return cached
    index = reference_index(root)
    pending = [schema]
    visited_objects: set[int] = set()
    visited_references: set[tuple[int, str, str]] = set()
    dependencies: dict[str, str] = {}
    while pending:
        active_budget().checkpoint()
        current = pending.pop()
        if isinstance(current, list):
            pending.extend(current)
            continue
        if isinstance(current, bool) or not isinstance(current, dict):
            continue
        identity = id(current)
        if identity in visited_objects:
            continue
        visited_objects.add(identity)

        for keyword in REFERENCE_KEYWORDS:
            reference = current.get(keyword)
            reference_key = (identity, keyword, reference)
            if not isinstance(reference, str) or reference_key in visited_references:
                continue
            visited_references.add(reference_key)
            try:
                absolute, target = resolve_indexed_reference(index, current, reference)
            except ValidationFailure:
                absolute = urljoin(
                    index.node_bases.get(identity) or SYNTHETIC_ROOT_URI,
                    reference,
                )
                dependencies[f"{keyword}:{absolute}"] = index.root_digest
                continue
            dependency_key = f"{keyword}:{absolute}"
            if keyword in {"$dynamicRef", "$recursiveRef"}:
                dependencies[dependency_key] = index.root_digest
            else:
                dependencies[dependency_key] = sha256_bytes(canonical_bytes(target))
            pending.append(target)

        for _, child in schema_child_locations(current):
            if isinstance(child, (dict, bool)):
                pending.append(child)
    result = tuple(sorted(dependencies.items()))
    budget.reference_fingerprints[cache_key] = result
    return result


def open_incoming_fallback(path: str, *, tagged: bool = False) -> str | None:
    if tagged and path.endswith("/definitions/ThreadItem"):
        return "unknown_thread_items_preserve_the_complete_raw_payload"
    if any(f"/definitions/{name}" in path for name in ("TurnStatus", "MessagePhase")):
        return "unknown_generated_enum_values_fail_soft_at_the_stable_boundary"
    return None


@budgeted
def optional_property_addition_is_additive(
    before: dict[str, Any], name: str, after_child: Any
) -> bool:
    """Prove that declaring an optional property cannot reject an old instance."""
    if after_child is True or after_child == {}:
        return True
    patterns = before.get("patternProperties", {})
    if isinstance(patterns, dict):
        for pattern in sorted(patterns):
            active_budget().checkpoint()
            try:
                if bounded_regex_search(pattern, name):
                    return False
            except (TypeError, ValidationFailure):
                return False
    old_additional = before.get("additionalProperties", True)
    if old_additional is False:
        # A non-pattern-matched property was impossible before and is merely
        # admitted (subject to its new schema) afterward.
        return True
    if isinstance(old_additional, dict) and isinstance(after_child, dict):
        # Replacing the applicable additionalProperties schema with the exact
        # same property schema is semantically neutral.
        return canonical_bytes(old_additional) == canonical_bytes(after_child)
    return False


@budgeted
def compare_combinator(
    before: dict[str, Any],
    after: dict[str, Any],
    path: str,
    changes: list[dict[str, Any]],
    combinator: str,
    *,
    incoming: bool,
    before_root: Any,
    after_root: Any,
) -> None:
    first_change = len(changes)
    old = before.get(combinator)
    new = after.get(combinator)
    if not isinstance(old, list) and not isinstance(new, list):
        return
    snake = camel_to_snake(combinator)
    if not isinstance(old, list):
        changes.append(change("breaking", f"{snake}_constraint_added", path))
        return
    if not isinstance(new, list):
        changes.append(change("additive", f"{snake}_constraint_removed", path))
        return

    old_canonical = [canonical_bytes(variant) for variant in old]
    new_canonical = [canonical_bytes(variant) for variant in new]
    old_counts: dict[bytes, int] = {}
    new_counts: dict[bytes, int] = {}
    for fingerprint in old_canonical:
        old_counts[fingerprint] = old_counts.get(fingerprint, 0) + 1
    for fingerprint in new_canonical:
        new_counts[fingerprint] = new_counts.get(fingerprint, 0) + 1
    pure_addition = len(new) > len(old) and all(
        new_counts.get(fingerprint, 0) >= count
        for fingerprint, count in old_counts.items()
    )
    reference_dependencies_changed = (
        combinator == "oneOf"
        and schema_reference_dependencies(old, before_root)
        != schema_reference_dependencies(new, after_root)
    )

    old_unmatched = set(range(len(old)))
    new_unmatched = set(range(len(new)))
    pairs: list[tuple[int, int]] = []
    new_fingerprints: dict[bytes, list[int]] = {}
    for index, fingerprint in enumerate(new_canonical):
        active_budget().checkpoint()
        new_fingerprints.setdefault(fingerprint, []).append(index)
    for old_index, fingerprint in enumerate(old_canonical):
        active_budget().checkpoint()
        candidates = new_fingerprints.get(fingerprint, [])
        candidate = next((index for index in candidates if index in new_unmatched), None)
        if candidate is not None:
            old_unmatched.remove(old_index)
            new_unmatched.remove(candidate)

    for old_index in list(old_unmatched):
        active_budget().checkpoint()
        key = branch_key(old[old_index])
        candidates = []
        for index in new_unmatched:
            active_budget().checkpoint()
            if branch_key(new[index]) == key:
                candidates.append(index)
        if key is not None and len(candidates) == 1:
            new_index = candidates[0]
            old_unmatched.remove(old_index)
            new_unmatched.remove(new_index)
            pairs.append((old_index, new_index))
    # Preserve a modified branch at the same position. This is what prevents an
    # optional field edit inside oneOf from being double-counted as remove+add.
    for index in sorted(old_unmatched & new_unmatched):
        active_budget().checkpoint()
        old_unmatched.remove(index)
        new_unmatched.remove(index)
        pairs.append((index, index))
    for old_index, new_index in pairs:
        active_budget().checkpoint()
        if isinstance(old[old_index], (dict, bool)) and isinstance(
            new[new_index], (dict, bool)
        ):
            compare_named_schemas(
                old[old_index],
                new[new_index],
                f"{path}/{combinator}/{new_index}",
                changes,
                incoming=incoming,
                before_root=before_root,
                after_root=after_root,
            )
        elif old[old_index] != new[new_index]:
            changes.append(
                change(
                    "breaking",
                    f"{snake}_invalid_variant_changed",
                    f"{path}/{combinator}/{new_index}",
                )
            )

    if old_unmatched:
        classification = "additive" if combinator == "allOf" else "breaking"
        changes.append(
            change(classification, f"{snake}_variants_removed", path, count=len(old_unmatched))
        )
    if new_unmatched:
        fallback: str | None = None
        if combinator == "allOf":
            classification = "breaking"
            kind = "all_of_variants_added"
        elif combinator == "anyOf":
            fallback = open_incoming_fallback(path, tagged=True) if incoming else None
            classification = "breaking" if incoming and fallback is None else "additive"
            kind = "incoming_closed_union_variants_added" if classification == "breaking" else "any_of_variants_added"
        else:
            addition_indices = sorted(new_unmatched)
            additions_disjoint = all(
                all(branches_provably_disjoint(new[index], existing) for existing in old)
                and all(
                    branches_provably_disjoint(new[index], new[prior])
                    for prior in addition_indices
                    if prior < index
                )
                for index in addition_indices
            )
            fallback = open_incoming_fallback(path, tagged=True) if incoming else None
            classification = "additive" if additions_disjoint and (not incoming or fallback) else "breaking"
            kind = (
                "one_of_variants_added"
                if classification == "additive"
                else "one_of_variant_added_unproven_or_closed"
            )
        details: dict[str, Any] = {"count": len(new_unmatched)}
        if classification == "additive" and incoming and fallback is not None:
            details["fallbackEvidence"] = fallback
        changes.append(change(classification, kind, path, **details))

    if (
        combinator == "oneOf"
        and (
            (old_counts != new_counts and not pure_addition)
            or reference_dependencies_changed
        )
        and not any(
            item["classification"] == "breaking" for item in changes[first_change:]
        )
    ):
        changes.append(
            change(
                "breaking",
                "one_of_global_exclusivity_changed_unproven",
                path,
            )
        )


@budgeted
def compare_named_schemas(
    before: Any,
    after: Any,
    path: str,
    changes: list[dict[str, Any]],
    *,
    incoming: bool = False,
    before_root: Any | None = None,
    after_root: Any | None = None,
) -> None:
    if before_root is None:
        before_root = before
    if after_root is None:
        after_root = after
    first_change = len(changes)
    active_budget().checkpoint()
    if isinstance(before, bool) or isinstance(after, bool):
        if before == after:
            return
        if before is True or after is False:
            classification = "breaking"
            kind = "boolean_schema_narrowed"
        elif before is False or after is True:
            classification = "additive"
            kind = "boolean_schema_widened"
        else:
            classification = "breaking"
            kind = "boolean_schema_changed"
        changes.append(change(classification, kind, path))
        if incoming and classification == "additive":
            changes[-1]["classification"] = "breaking"
            changes[-1]["incomingDirection"] = "conservative_consumer_boundary"
        return
    if not isinstance(before, dict) or not isinstance(after, dict):
        if before != after:
            changes.append(change("breaking", "schema_shape_changed", path))
        return
    before_types = schema_types(before)
    after_types = schema_types(after)
    if before_types is not None and after_types is not None:
        before_atoms = type_atoms(before_types)
        after_atoms = type_atoms(after_types)
        removed_types = sorted(before_atoms - after_atoms)
        added_types = sorted(after_atoms - before_atoms)
        if removed_types:
            changes.append(
                change(
                    "breaking",
                    "type_narrowed_or_changed",
                    path,
                    removedTypes=removed_types,
                    addedTypes=added_types,
                )
            )
        elif added_types:
            changes.append(change("additive", "type_widened", path, addedTypes=added_types))
    elif before_types is None and after_types is not None:
        changes.append(change("breaking", "type_narrowed", path, addedConstraint=sorted(after_types)))
    elif before_types is not None and after_types is None:
        changes.append(change("additive", "type_constraint_removed", path))

    before_values_raw = finite_values(before)
    after_values_raw = finite_values(after)
    if before_values_raw is not None and after_values_raw is not None:
        before_values = {semantic_json_key(value): value for value in before_values_raw}
        after_values = {semantic_json_key(value): value for value in after_values_raw}
        removed = [before_values[key] for key in sorted(before_values.keys() - after_values.keys())]
        added = [after_values[key] for key in sorted(after_values.keys() - before_values.keys())]
        if removed:
            changes.append(change("breaking", "finite_values_removed", path, values=removed))
        if added:
            fallback = open_incoming_fallback(path) if incoming else None
            classification = "breaking" if incoming and fallback is None else "additive"
            details = {"values": added}
            if fallback is not None:
                details["fallbackEvidence"] = fallback
            kind = (
                "incoming_closed_values_added"
                if classification == "breaking"
                else "finite_values_added"
            )
            changes.append(change(classification, kind, path, **details))
    elif before_values_raw is None and after_values_raw is not None:
        changes.append(change("breaking", "finite_constraint_added", path))
    elif before_values_raw is not None and after_values_raw is None:
        changes.append(change("additive", "finite_constraint_removed", path))

    before_ref = before.get("$ref")
    after_ref = after.get("$ref")
    if before_ref != after_ref or (("$ref" in before) != ("$ref" in after)):
        if isinstance(before_ref, str) and isinstance(after_ref, str):
            changes.append(change("breaking", "reference_changed", path))
        elif "$ref" not in before and isinstance(after_ref, str):
            changes.append(change("breaking", "reference_added", path))
        elif isinstance(before_ref, str) and "$ref" not in after:
            # Draft-07 ignores siblings next to $ref. Removing it activates those
            # siblings, so widening is not proven even when the target disappears.
            changes.append(change("breaking", "reference_removed", path))
        else:
            changes.append(change("breaking", "reference_invalid_or_changed", path))

    before_identifier = before.get("$id")
    after_identifier = after.get("$id")
    if before_identifier != after_identifier or (
        ("$id" in before) != ("$id" in after)
    ):
        if "$id" not in before:
            kind = "schema_identifier_added"
        elif "$id" not in after:
            kind = "schema_identifier_removed"
        else:
            kind = "schema_identifier_changed"
        changes.append(change("breaking", kind, path))

    before_draft = before.get("$schema")
    after_draft = after.get("$schema")
    if before_draft != after_draft or (("$schema" in before) != ("$schema" in after)):
        if "$schema" not in before:
            kind = "schema_draft_added"
        elif "$schema" not in after:
            kind = "schema_draft_removed"
        else:
            kind = "schema_draft_changed"
        changes.append(change("breaking", kind, path))

    old_additional = before.get("additionalProperties", True)
    new_additional = after.get("additionalProperties", True)
    if old_additional != new_additional:
        if old_additional is True:
            changes.append(change("breaking", "additional_properties_narrowed", path))
        elif new_additional is True:
            changes.append(change("additive", "additional_properties_widened", path))
        elif old_additional is False:
            changes.append(change("additive", "additional_properties_widened", path))
        elif new_additional is False:
            changes.append(change("breaking", "additional_properties_narrowed", path))
        elif isinstance(old_additional, dict) and isinstance(new_additional, dict):
            compare_named_schemas(
                old_additional,
                new_additional,
                f"{path}/additionalProperties",
                changes,
                incoming=incoming,
                before_root=before_root,
                after_root=after_root,
            )

    for key in ("minimum", "exclusiveMinimum", "minLength", "minItems", "minProperties"):
        classify_bound(before, after, path, changes, key, minimum=True)
    for key in ("maximum", "exclusiveMaximum", "maxLength", "maxItems", "maxProperties"):
        classify_bound(before, after, path, changes, key, minimum=False)
    for key in ("pattern", "format"):
        old = before.get(key)
        new = after.get(key)
        if old != new:
            if old is None:
                changes.append(change("breaking", f"{key}_constraint_added", path))
            elif new is None:
                changes.append(change("additive", f"{key}_constraint_removed", path))
            else:
                changes.append(change("breaking", f"{key}_constraint_changed", path))
    old_multiple = before.get("multipleOf")
    new_multiple = after.get("multipleOf")
    if old_multiple != new_multiple:
        if old_multiple is None:
            changes.append(change("breaking", "multiple_of_added", path))
        elif new_multiple is None:
            changes.append(change("additive", "multiple_of_removed", path))
        elif (
            isinstance(old_multiple, (int, float))
            and not isinstance(old_multiple, bool)
            and isinstance(new_multiple, (int, float))
            and not isinstance(new_multiple, bool)
        ):
            old_fraction = exact_number_fraction(old_multiple)
            new_fraction = exact_number_fraction(new_multiple)
            classification = "breaking"
            if old_fraction > 0 and new_fraction > 0:
                ratio = new_fraction / old_fraction
                inverse = old_fraction / new_fraction
                widened = inverse.denominator == 1
                narrowed = ratio.denominator == 1
                if widened and not narrowed:
                    classification = "additive"
            changes.append(change(classification, "multiple_of_changed", path))
        else:
            changes.append(change("breaking", "multiple_of_changed", path))
    old_unique = before.get("uniqueItems", False) is True
    new_unique = after.get("uniqueItems", False) is True
    if old_unique != new_unique:
        changes.append(
            change(
                "breaking" if new_unique else "additive",
                "unique_items_enabled" if new_unique else "unique_items_disabled",
                path,
            )
        )

    before_props = before.get("properties", {})
    after_props = after.get("properties", {})
    if isinstance(before_props, dict) and isinstance(after_props, dict):
        before_required = set(before.get("required", []))
        after_required = set(after.get("required", []))
        for name in sorted(before_props.keys() - after_props.keys()):
            changes.append(change("breaking", "property_removed", f"{path}/properties/{name}"))
        for name in sorted(after_props.keys() - before_props.keys()):
            required = name in after_required
            classification = (
                "breaking"
                if required
                or not optional_property_addition_is_additive(
                    before, name, after_props[name]
                )
                else "additive"
            )
            kind = (
                "required_property_added" if required else "optional_property_added"
            )
            changes.append(change(classification, kind, f"{path}/properties/{name}"))
        for name in sorted(before_props.keys() & after_props.keys()):
            before_child = before_props[name]
            after_child = after_props[name]
            if isinstance(before_child, (dict, bool)) and isinstance(
                after_child, (dict, bool)
            ):
                compare_named_schemas(
                    before_child,
                    after_child,
                    f"{path}/properties/{name}",
                    changes,
                    incoming=incoming,
                    before_root=before_root,
                    after_root=after_root,
                )
            elif before_child != after_child:
                changes.append(
                    change("breaking", "property_schema_shape_changed", f"{path}/properties/{name}")
                )
        newly_declared = after_props.keys() - before_props.keys()
        for name in sorted(after_required - before_required):
            if name not in newly_declared:
                changes.append(change("breaking", "property_became_required", f"{path}/properties/{name}"))
        for name in sorted(before_required - after_required):
            changes.append(change("additive", "property_became_optional", f"{path}/properties/{name}"))

    before_defs = before.get("definitions", {})
    after_defs = after.get("definitions", {})
    if isinstance(before_defs, dict) and isinstance(after_defs, dict):
        for name in sorted(before_defs.keys() - after_defs.keys()):
            changes.append(change("breaking", "definition_removed", f"{path}/definitions/{name}"))
        for name in sorted(after_defs.keys() - before_defs.keys()):
            changes.append(change("additive", "definition_added", f"{path}/definitions/{name}"))
        for name in sorted(before_defs.keys() & after_defs.keys()):
            before_child = before_defs[name]
            after_child = after_defs[name]
            if isinstance(before_child, (dict, bool)) and isinstance(
                after_child, (dict, bool)
            ):
                compare_named_schemas(
                    before_child,
                    after_child,
                    f"{path}/definitions/{name}",
                    changes,
                    incoming=incoming,
                    before_root=before_root,
                    after_root=after_root,
                )
            elif before_child != after_child:
                changes.append(
                    change("breaking", "definition_schema_shape_changed", f"{path}/definitions/{name}")
                )

    before_items = before.get("items")
    after_items = after.get("items")
    if isinstance(before_items, (dict, bool)) and isinstance(after_items, (dict, bool)):
        compare_named_schemas(
            before_items,
            after_items,
            f"{path}/items",
            changes,
            incoming=incoming,
            before_root=before_root,
            after_root=after_root,
        )
    elif isinstance(before_items, list) and isinstance(after_items, list):
        shared_items = min(len(before_items), len(after_items))
        for index in range(shared_items):
            active_budget().checkpoint()
            before_item = before_items[index]
            after_item = after_items[index]
            if isinstance(before_item, (dict, bool)) and isinstance(
                after_item, (dict, bool)
            ):
                compare_named_schemas(
                    before_item,
                    after_item,
                    f"{path}/items/{index}",
                    changes,
                    incoming=incoming,
                    before_root=before_root,
                    after_root=after_root,
                )
            elif before_item != after_item:
                changes.append(
                    change("breaking", "tuple_item_schema_shape_changed", f"{path}/items/{index}")
                )
        if len(before_items) > shared_items:
            after_additional_items = after.get("additionalItems", True)
            unrestricted_after_items = (
                after_additional_items is True or after_additional_items == {}
            )
            changes.append(
                change(
                    "additive" if unrestricted_after_items else "breaking",
                    (
                        "tuple_item_constraints_removed"
                        if unrestricted_after_items
                        else "tuple_shortened_with_restricted_additional_items"
                    ),
                    f"{path}/items",
                    count=len(before_items) - shared_items,
                )
            )
        if len(after_items) > shared_items:
            changes.append(
                change(
                    "breaking",
                    "tuple_item_constraints_added",
                    f"{path}/items",
                    count=len(after_items) - shared_items,
                )
            )
    elif before_items is None and after_items is not None:
        changes.append(change("breaking", "items_constraint_added", f"{path}/items"))
    elif before_items is not None and after_items is None:
        changes.append(change("additive", "items_constraint_removed", f"{path}/items"))
    elif before_items != after_items:
        changes.append(change("breaking", "items_schema_shape_changed", f"{path}/items"))

    for combinator in ("anyOf", "oneOf", "allOf"):
        compare_combinator(
            before,
            after,
            path,
            changes,
            combinator,
            incoming=incoming,
            before_root=before_root,
            after_root=after_root,
        )

    for key in ("not", "if", "then", "else"):
        if before.get(key) != after.get(key):
            classification = "additive" if key == "not" and key in before and key not in after else "breaking"
            changes.append(change(classification, f"{key}_constraint_changed", path))
        elif isinstance(before.get(key), (dict, bool)) and isinstance(
            after.get(key), (dict, bool)
        ):
            before_dependencies = schema_reference_dependencies(
                before[key], before_root
            )
            after_dependencies = schema_reference_dependencies(
                after[key], after_root
            )
            if before_dependencies != after_dependencies:
                changes.append(
                    change(
                        "breaking",
                        f"{key}_reference_dependency_changed_unproven",
                        path,
                    )
                )

    annotations = {
        "title",
        "description",
        "default",
        "examples",
        "deprecated",
        "readOnly",
        "writeOnly",
    }
    handled = annotations | {
        "$id", "$ref", "$schema", "type", "enum", "const", "properties", "required", "definitions",
        "additionalProperties", "items", "anyOf", "oneOf", "allOf", "not", "if", "then", "else",
        "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf",
        "minLength", "maxLength", "pattern", "format", "minItems", "maxItems", "uniqueItems",
        "minProperties", "maxProperties",
    }
    for key in sorted((set(before) | set(after)) - handled):
        if before.get(key) != after.get(key):
            changes.append(change("breaking", "unknown_constraint_changed", f"{path}/{key}", keyword=key))

    if incoming:
        for item in changes[first_change:]:
            if item["classification"] != "additive":
                continue
            evidenced_fallback = isinstance(item.get("fallbackEvidence"), str)
            if item["kind"] in {"optional_property_added", "definition_added"} or evidenced_fallback:
                continue
            item["classification"] = "breaking"
            item["incomingDirection"] = "conservative_consumer_boundary"


@budgeted
def compatibility_report(baseline: str, candidate: str) -> dict[str, Any]:
    before = load_bundle(baseline)
    after = load_bundle(candidate)
    changes: list[dict[str, Any]] = []
    before_roots = before["roots"]
    after_roots = after["roots"]
    for name in sorted(before_roots.keys() - after_roots.keys()):
        active_budget().checkpoint()
        changes.append(change("breaking", "selected_root_removed", f"roots/{name}"))
    for name in sorted(after_roots.keys() - before_roots.keys()):
        active_budget().checkpoint()
        changes.append(change("additive", "selected_root_added", f"roots/{name}"))
    for name in sorted(before_roots.keys() & after_roots.keys()):
        active_budget().checkpoint()
        compare_named_schemas(
            before_roots[name],
            after_roots[name],
            f"roots/{name}",
            changes,
            incoming=is_incoming_root(name),
        )

    before_notifications = set(before["notificationMethods"])
    after_notifications = set(after["notificationMethods"])
    for method in sorted(before_notifications - after_notifications):
        active_budget().checkpoint()
        changes.append(change("breaking", "notification_removed", f"notifications/{method}"))
    for method in sorted(after_notifications - before_notifications):
        active_budget().checkpoint()
        changes.append(change("additive", "notification_added", f"notifications/{method}"))

    before_audit = incoming_audit(baseline, before)
    after_audit = incoming_audit(candidate, after)
    before_constructs = {
        (entry["schemaPath"], entry["construct"]) for entry in before_audit["constructs"]
    }
    for entry in after_audit["constructs"]:
        active_budget().checkpoint()
        key = (entry["schemaPath"], entry["construct"])
        if key in before_constructs:
            continue
        classification = "breaking" if entry["handling"] == "promotion-blocking" else "additive"
        changes.append(
            change(
                classification,
                "incoming_construct_added",
                entry["schemaPath"],
                construct=entry["construct"],
                handling=entry["handling"],
                fallbackEvidence=entry["evidence"],
            )
        )

    unique: dict[bytes, dict[str, Any]] = {}
    for item in changes:
        active_budget().checkpoint()
        dedupe = dict(item)
        if "/definitions/" in dedupe["path"]:
            dedupe["path"] = "definitions/" + dedupe["path"].split("/definitions/", 1)[1]
        unique.setdefault(canonical_bytes(dedupe), item)
    ordered = sorted(
        unique.values(),
        key=lambda item: (
            item["classification"],
            item["kind"],
            item["path"],
            canonical_bytes(item),
        ),
    )
    breaking = sum(item["classification"] == "breaking" for item in ordered)
    additive = len(ordered) - breaking
    return {
        "formatVersion": 1,
        "baselineVersion": baseline,
        "candidateVersion": candidate,
        "protocolFamily": read_selection().protocol_family,
        "compatible": breaking == 0,
        "summary": {"additive": additive, "breaking": breaking, "total": len(ordered)},
        "changes": ordered,
    }


@budgeted
def report_markdown(report: dict[str, Any]) -> bytes:
    state = "PASS (additive only)" if report["compatible"] else "BLOCKED (breaking changes found)"
    lines = [
        f"# Codex schema upgrade: {report['baselineVersion']} → {report['candidateVersion']}",
        "",
        f"Gate: **{state}**",
        "",
        f"- Additive changes: {report['summary']['additive']}",
        f"- Breaking changes: {report['summary']['breaking']}",
        f"- Total classified changes: {report['summary']['total']}",
        "",
        "| Classification | Kind | Schema path |",
        "| --- | --- | --- |",
    ]
    limit = 250
    for item in report["changes"][:limit]:
        active_budget().checkpoint()
        path = str(item["path"]).replace("|", "\\|")
        lines.append(f"| {item['classification']} | `{item['kind']}` | `{path}` |")
    omitted = len(report["changes"]) - limit
    if omitted > 0:
        lines.extend(["", f"The table omits {omitted} additional machine-readable entries; see the JSON report."])
    lines.extend(
        [
            "",
            "Incoming enum/union additions are additive only when the machine-readable "
            "audit names tested fallback evidence; closed constructs block promotion.",
            "Promotion still requires contract fixtures, append-only support history, and explicit adapter review.",
            "",
        ]
    )
    content = "\n".join(lines).encode("utf-8")
    if len(content) > MAX_ARTIFACT_BYTES:
        fail("generated compatibility report exceeds the per-file byte limit")
    return content


@budgeted
def write_report(baseline: str, candidate: str, json_path: Path | None, markdown_path: Path | None) -> dict[str, Any]:
    report = compatibility_report(baseline, candidate)
    if json_path is not None:
        atomic_write(json_path, canonical_bytes(report))
    if markdown_path is not None:
        atomic_write(markdown_path, report_markdown(report))
    return report


class ValidationFailure(Exception):
    pass


def instance_type_matches(instance: Any, expected: str) -> bool:
    if expected == "null":
        return instance is None
    if expected == "boolean":
        return isinstance(instance, bool)
    if expected == "integer":
        return isinstance(instance, int) and not isinstance(instance, bool)
    if expected == "number":
        return isinstance(instance, (int, float)) and not isinstance(instance, bool)
    if expected == "string":
        return isinstance(instance, str)
    if expected == "array":
        return isinstance(instance, list)
    if expected == "object":
        return isinstance(instance, dict)
    return False


@budgeted
def resolve_pointer(root: Any, reference: str) -> Any:
    if reference == "#":
        if not isinstance(root, (dict, bool)):
            raise ValidationFailure("schema reference is not a schema")
        return root
    if not reference.startswith("#/"):
        raise ValidationFailure("external references are not permitted")
    current: Any = root
    for encoded in reference[2:].split("/"):
        active_budget().checkpoint()
        key = encoded.replace("~1", "/").replace("~0", "~")
        if not isinstance(current, dict) or key not in current:
            raise ValidationFailure("schema reference is unresolved")
        current = current[key]
    if not isinstance(current, (dict, bool)):
        raise ValidationFailure("schema reference is not a schema")
    return current


@budgeted
def validate_instance(
    instance: Any, schema: Any, root: Any, *, depth: int = 0
) -> None:
    active_budget().checkpoint()
    if depth > SCHEMA_VALIDATION_RECURSION_LIMIT:
        raise ValidationFailure("schema validation nesting limit exceeded")
    if schema is True:
        return
    if schema is False or not isinstance(schema, dict):
        raise ValidationFailure("boolean schema constraint failed")
    for reference_keyword in REFERENCE_KEYWORDS:
        reference = schema.get(reference_keyword)
        if isinstance(reference, str):
            _, target = resolve_indexed_reference(
                reference_index(root), schema, reference
            )
            validate_instance(instance, target, root, depth=depth + 1)
            return
    if "const" in schema and semantic_json_key(instance) != semantic_json_key(schema["const"]):
        raise ValidationFailure("constant constraint failed")
    if isinstance(schema.get("enum"), list) and semantic_json_key(instance) not in {
        semantic_json_key(value) for value in schema["enum"]
    }:
        raise ValidationFailure("enum constraint failed")
    raw_type = schema.get("type")
    allowed_types = [raw_type] if isinstance(raw_type, str) else raw_type
    if isinstance(allowed_types, list) and allowed_types:
        if not any(isinstance(value, str) and instance_type_matches(instance, value) for value in allowed_types):
            raise ValidationFailure("type constraint failed")

    for combinator in ("allOf",):
        variants = schema.get(combinator)
        if isinstance(variants, list):
            for variant in variants:
                active_budget().checkpoint()
                if isinstance(variant, (dict, bool)):
                    validate_instance(instance, variant, root, depth=depth + 1)
    for combinator, exact in (("anyOf", False), ("oneOf", True)):
        variants = schema.get(combinator)
        if isinstance(variants, list):
            matches = 0
            for variant in variants:
                active_budget().checkpoint()
                if not isinstance(variant, (dict, bool)):
                    continue
                try:
                    validate_instance(instance, variant, root, depth=depth + 1)
                    matches += 1
                except ValidationFailure:
                    pass
            if matches == 0 or (exact and matches != 1):
                raise ValidationFailure(f"{combinator} constraint failed")
    negative = schema.get("not")
    if isinstance(negative, (dict, bool)):
        try:
            validate_instance(instance, negative, root, depth=depth + 1)
        except ValidationFailure:
            pass
        else:
            raise ValidationFailure("not constraint failed")
    condition = schema.get("if")
    if isinstance(condition, (dict, bool)):
        try:
            validate_instance(instance, condition, root, depth=depth + 1)
            branch = schema.get("then")
        except ValidationFailure:
            branch = schema.get("else")
        if isinstance(branch, (dict, bool)):
            validate_instance(instance, branch, root, depth=depth + 1)

    if isinstance(instance, dict):
        if isinstance(schema.get("minProperties"), int) and len(instance) < schema["minProperties"]:
            raise ValidationFailure("minimum object property count failed")
        if isinstance(schema.get("maxProperties"), int) and len(instance) > schema["maxProperties"]:
            raise ValidationFailure("maximum object property count failed")
        required = schema.get("required", [])
        if isinstance(required, list):
            for key in required:
                active_budget().checkpoint()
                if isinstance(key, str) and key not in instance:
                    raise ValidationFailure("required property is absent")
        properties = schema.get("properties", {})
        if isinstance(properties, dict):
            for key, value in instance.items():
                active_budget().checkpoint()
                child = properties.get(key)
                declared_property = isinstance(child, (dict, bool))
                if isinstance(child, (dict, bool)):
                    validate_instance(value, child, root, depth=depth + 1)
                matched_pattern = False
                patterns = schema.get("patternProperties", {})
                if isinstance(patterns, dict):
                    for pattern, pattern_schema in patterns.items():
                        active_budget().checkpoint()
                        try:
                            matches_pattern = bounded_regex_search(pattern, key)
                        except (TypeError, ValidationFailure) as error:
                            raise ValidationFailure("schema pattern is invalid") from error
                        if matches_pattern and isinstance(pattern_schema, (dict, bool)):
                            matched_pattern = True
                            validate_instance(value, pattern_schema, root, depth=depth + 1)
                if declared_property or matched_pattern:
                    continue
                additional = schema.get("additionalProperties", True)
                if additional is False:
                    raise ValidationFailure("additional property is forbidden")
                if isinstance(additional, dict):
                    validate_instance(value, additional, root, depth=depth + 1)
        dependencies = schema.get("dependencies", {})
        if isinstance(dependencies, dict):
            for key, dependency in dependencies.items():
                active_budget().checkpoint()
                if key not in instance:
                    continue
                if isinstance(dependency, list) and any(item not in instance for item in dependency):
                    raise ValidationFailure("property dependency failed")
                if isinstance(dependency, (dict, bool)):
                    validate_instance(instance, dependency, root, depth=depth + 1)
    if isinstance(instance, list):
        if isinstance(schema.get("minItems"), int) and len(instance) < schema["minItems"]:
            raise ValidationFailure("minimum array length failed")
        if isinstance(schema.get("maxItems"), int) and len(instance) > schema["maxItems"]:
            raise ValidationFailure("maximum array length failed")
        if schema.get("uniqueItems") is True:
            keys = [semantic_json_key(item) for item in instance]
            if len(keys) != len(set(keys)):
                raise ValidationFailure("unique array item constraint failed")
        items = schema.get("items")
        if isinstance(items, (dict, bool)):
            for item in instance:
                active_budget().checkpoint()
                validate_instance(item, items, root, depth=depth + 1)
        elif isinstance(items, list):
            for index, item in enumerate(instance[: len(items)]):
                active_budget().checkpoint()
                validate_instance(item, items[index], root, depth=depth + 1)
            if len(instance) > len(items):
                additional_items = schema.get("additionalItems", True)
                if additional_items is False:
                    raise ValidationFailure("additional array item is forbidden")
                if isinstance(additional_items, dict):
                    for item in instance[len(items) :]:
                        active_budget().checkpoint()
                        validate_instance(item, additional_items, root, depth=depth + 1)
        contains = schema.get("contains")
        if isinstance(contains, (dict, bool)):
            if not any(instance_valid(item, contains, root, depth + 1) for item in instance):
                raise ValidationFailure("array contains constraint failed")
    if isinstance(instance, str):
        if isinstance(schema.get("minLength"), int) and len(instance) < schema["minLength"]:
            raise ValidationFailure("minimum string length failed")
        if isinstance(schema.get("maxLength"), int) and len(instance) > schema["maxLength"]:
            raise ValidationFailure("maximum string length failed")
        pattern = schema.get("pattern")
        if isinstance(pattern, str):
            try:
                matched = bounded_regex_search(pattern, instance)
            except ValidationFailure as error:
                raise ValidationFailure("schema pattern is invalid") from error
            if not matched:
                raise ValidationFailure("string pattern constraint failed")
    if isinstance(instance, (int, float)) and not isinstance(instance, bool):
        if isinstance(schema.get("minimum"), (int, float)) and instance < schema["minimum"]:
            raise ValidationFailure("minimum numeric constraint failed")
        if isinstance(schema.get("maximum"), (int, float)) and instance > schema["maximum"]:
            raise ValidationFailure("maximum numeric constraint failed")
        if isinstance(schema.get("exclusiveMinimum"), (int, float)) and instance <= schema["exclusiveMinimum"]:
            raise ValidationFailure("exclusive minimum numeric constraint failed")
        if isinstance(schema.get("exclusiveMaximum"), (int, float)) and instance >= schema["exclusiveMaximum"]:
            raise ValidationFailure("exclusive maximum numeric constraint failed")
        multiple = schema.get("multipleOf")
        if (
            isinstance(multiple, (int, float))
            and not isinstance(multiple, bool)
            and multiple > 0
        ):
            if isinstance(instance, int) and isinstance(multiple, int):
                is_multiple = instance % multiple == 0
            else:
                quotient = instance / multiple
                is_multiple = abs(quotient - round(quotient)) <= 1e-12
            if not is_multiple:
                raise ValidationFailure("multiple-of numeric constraint failed")
        numeric_ranges = {
            "int32": (-(2**31), 2**31 - 1),
            "uint16": (0, 2**16 - 1),
            "uint32": (0, 2**32 - 1),
            "int64": (-(2**63), 2**63 - 1),
            "uint64": (0, 2**64 - 1),
            "uint": (0, 2**64 - 1),
        }
        bounds = numeric_ranges.get(schema.get("format"))
        if bounds is not None and not bounds[0] <= instance <= bounds[1]:
            raise ValidationFailure("formatted integer range failed")


def instance_valid(instance: Any, schema: Any, root: dict[str, Any], depth: int) -> bool:
    try:
        validate_instance(instance, schema, root, depth=depth)
        return True
    except ValidationFailure:
        return False


METHOD_ROOTS = {
    "initialize": ("initialize.params", "initialize.response"),
    "thread/start": ("thread.start.params", "thread.start.response"),
    "thread/list": ("thread.list.params", "thread.list.response"),
    "thread/read": ("thread.read.params", "thread.read.response"),
    "thread/resume": ("thread.resume.params", "thread.resume.response"),
    "turn/start": ("turn.start.params", "turn.start.response"),
    "turn/interrupt": ("turn.interrupt.params", "turn.interrupt.response"),
}

SHARED_METHOD_ROOTS = {
    "thread/unsubscribe": ("thread.unsubscribe.params", "thread.unsubscribe.response"),
    "thread/turns/list": ("thread.turns.list.params", "thread.turns.list.response"),
    "thread/items/list": ("thread.items.list.params", "thread.items.list.response"),
    "thread/queue/add": ("thread.queue.add.params", "thread.queue.add.response"),
    "thread/queue/list": ("thread.queue.list.params", "thread.queue.list.response"),
    "thread/queue/start": ("thread.queue.start.params", "thread.queue.start.response"),
    "turn/steer": ("turn.steer.params", "turn.steer.response"),
}

NOTIFICATION_ROOTS = {
    "thread/started": "notification.thread.started",
    "turn/started": "notification.turn.started",
    "item/started": "notification.item.started",
    "item/agentMessage/delta": "notification.item.agent_message.delta",
    "item/commandExecution/outputDelta": "notification.item.command_execution.output_delta",
    "item/completed": "notification.item.completed",
    "thread/tokenUsage/updated": "notification.thread.token_usage.updated",
    "error": "notification.error",
    "turn/completed": "notification.turn.completed",
}

SHARED_NOTIFICATION_ROOTS = {
    "thread/status/changed": "notification.thread.status.changed",
    "thread/queue/changed": "notification.thread.queue.changed",
    "serverRequest/resolved": "notification.server_request.resolved",
}

REVERSE_REQUEST_ROOTS = {
    "item/tool/call": ("server_request.dynamic_tool_call.params", "server_request.dynamic_tool_call.response")
}

SHARED_REVERSE_REQUEST_ROOTS = {
    "item/commandExecution/requestApproval": (
        "server_request.command_execution.request_approval.params",
        "server_request.command_execution.request_approval.response",
    ),
    "item/fileChange/requestApproval": (
        "server_request.file_change.request_approval.params",
        "server_request.file_change.request_approval.response",
    ),
    "item/permissions/requestApproval": (
        "server_request.permissions.request_approval.params",
        "server_request.permissions.request_approval.response",
    ),
}

REQUIRED_UNKNOWN_CASES = {
    ("thread/unsubscribe.status", "preserved_unknown"),
    ("thread/status/changed.status", "preserved_unknown"),
    ("item/commandExecution/requestApproval.result", "rejected_unknown"),
    ("item/fileChange/requestApproval.result", "rejected_unknown"),
    ("item/permissions/requestApproval.result", "rejected_unknown"),
    ("required.notification.method", "rejected_unknown"),
}

MUTATION_FAILURE_METHODS = {
    "turn/steer",
    "thread/queue/add",
    "thread/queue/start",
}

MUTATION_FAILURE_EXPECTATIONS = {
    "local_validation": "definitely_not_applied",
    "server_rejection": "definitely_rejected",
    "timeout": "uncertain",
    "connection_lost": "uncertain",
    "malformed_success": "uncertain",
    "stale_epoch_response": "rejected_stale",
}


def contract_roots(
    roots: dict[str, Any],
    base: dict[str, Any],
    shared: dict[str, Any],
) -> dict[str, Any]:
    selected = dict(base)
    for method, root_names in shared.items():
        names = (root_names,) if isinstance(root_names, str) else root_names
        present = [name in roots for name in names]
        if any(present) and not all(present):
            fail(f"selected shared Codex method {method} has incomplete roots")
        if all(present):
            selected[method] = root_names
    return selected


def verify_required_field_rejections(
    version: str, contract_name: str, instance: Any, schema: Any
) -> None:
    if not isinstance(instance, dict) or not isinstance(schema, dict):
        fail(f"Codex {version} contract {contract_name} is not an object fixture")
    required = schema.get("required", [])
    if not isinstance(required, list):
        fail(f"Codex {version} contract {contract_name} has invalid required fields")
    for field in required:
        active_budget().checkpoint()
        if not isinstance(field, str) or field not in instance:
            fail(f"Codex {version} contract {contract_name} omits a required fixture field")
        invalid = dict(instance)
        invalid.pop(field)
        try:
            validate_instance(invalid, schema, schema)
        except ValidationFailure:
            continue
        fail(
            f"Codex {version} contract {contract_name} does not reject a missing required field"
        )

@budgeted
def validate_contract(version: str) -> None:
    bundle = load_bundle(version)
    roots = bundle["roots"]
    path = CONTRACTS_ROOT / f"{version}.json"
    contract = load_json(path)
    if not isinstance(contract, dict) or contract.get("formatVersion") != CONTRACT_FORMAT_VERSION:
        fail(f"Codex {version} contract has an unsupported format")
    if contract.get("codexVersion") != version:
        fail(f"Codex {version} contract records the wrong version")
    if contract.get("protocolFamily") != read_selection().protocol_family:
        fail(f"Codex {version} contract records the wrong protocol family")

    method_roots = contract_roots(roots, METHOD_ROOTS, SHARED_METHOD_ROOTS)
    notification_roots = contract_roots(
        roots, NOTIFICATION_ROOTS, SHARED_NOTIFICATION_ROOTS
    )
    reverse_request_roots = contract_roots(
        roots, REVERSE_REQUEST_ROOTS, SHARED_REVERSE_REQUEST_ROOTS
    )

    exchanges = contract.get("exchanges")
    if not isinstance(exchanges, list):
        fail(f"Codex {version} contract has no method exchanges")
    seen_methods: set[str] = set()
    for exchange in exchanges:
        active_budget().checkpoint()
        if not isinstance(exchange, dict) or not isinstance(exchange.get("method"), str):
            fail(f"Codex {version} contract contains an invalid exchange")
        method = exchange["method"]
        if method not in method_roots or method in seen_methods:
            fail(f"Codex {version} contract contains an unexpected or duplicate method")
        seen_methods.add(method)
        params_root, result_root = method_roots[method]
        try:
            validate_instance(exchange.get("params"), roots[params_root], roots[params_root])
            validate_instance(exchange.get("result"), roots[result_root], roots[result_root])
        except (KeyError, ValidationFailure) as error:
            raise SchemaToolError(f"Codex {version} contract violates the selected schema for {method}") from error
        verify_required_field_rejections(
            version, f"{method} params", exchange.get("params"), roots[params_root]
        )
        verify_required_field_rejections(
            version, f"{method} result", exchange.get("result"), roots[result_root]
        )
    if seen_methods != set(method_roots):
        fail(f"Codex {version} contract does not cover every selected method")

    notifications = contract.get("notifications")
    if not isinstance(notifications, list):
        fail(f"Codex {version} contract has no notifications")
    seen_notifications: set[str] = set()
    for notification in notifications:
        active_budget().checkpoint()
        if not isinstance(notification, dict) or not isinstance(notification.get("method"), str):
            fail(f"Codex {version} contract contains an invalid notification")
        method = notification["method"]
        if method not in notification_roots or method in seen_notifications:
            fail(f"Codex {version} contract contains an unexpected or duplicate notification")
        seen_notifications.add(method)
        params = notification.get("params")
        root_name = notification_roots[method]
        try:
            validate_instance(params, roots[root_name], roots[root_name])
        except (KeyError, ValidationFailure) as error:
            raise SchemaToolError(f"Codex {version} contract violates the selected schema for {method}") from error
        verify_required_field_rejections(
            version, f"{method} notification", params, roots[root_name]
        )
    if seen_notifications != set(notification_roots):
        fail(f"Codex {version} contract does not cover every consumed notification")
    normal_order = contract.get("normalNotificationOrder")
    if not isinstance(normal_order, list) or not all(isinstance(method, str) for method in normal_order):
        fail(f"Codex {version} contract has an invalid notification-order fixture")

    reverse_requests = contract.get("reverseRequests")
    if not isinstance(reverse_requests, list) or len(reverse_requests) != len(reverse_request_roots):
        fail(f"Codex {version} contract has invalid reverse-request coverage")
    seen_reverse: set[str] = set()
    for request in reverse_requests:
        active_budget().checkpoint()
        if not isinstance(request, dict) or request.get("method") not in reverse_request_roots:
            fail(f"Codex {version} contract has an unexpected reverse request")
        method = request["method"]
        if method in seen_reverse:
            fail(f"Codex {version} contract has a duplicate reverse request")
        seen_reverse.add(method)
        params_root, result_root = reverse_request_roots[method]
        try:
            validate_instance(request.get("params"), roots[params_root], roots[params_root])
            validate_instance(request.get("result"), roots[result_root], roots[result_root])
        except (KeyError, ValidationFailure) as error:
            raise SchemaToolError(f"Codex {version} contract violates the selected schema for {method}") from error
        verify_required_field_rejections(
            version, f"{method} params", request.get("params"), roots[params_root]
        )
        verify_required_field_rejections(
            version, f"{method} result", request.get("result"), roots[result_root]
        )

    failures = contract.get("failureCases")
    if not isinstance(failures, list) or not failures:
        fail(f"Codex {version} contract has no failure classification cases")
    observed_failure_sources: set[str] = set()
    for case in failures:
        active_budget().checkpoint()
        if not isinstance(case, dict):
            fail(f"Codex {version} contract contains an invalid failure case")
        source = case.get("source")
        expected = case.get("expected")
        if not isinstance(source, str) or not isinstance(expected, str) or source in observed_failure_sources:
            fail(f"Codex {version} contract contains an invalid failure classification")
        observed_failure_sources.add(source)

    if "thread/unsubscribe" in method_roots:
        unknown_cases = contract.get("unknownValueCases")
        if not isinstance(unknown_cases, list):
            fail(f"Codex {version} contract has no shared unknown-value cases")
        observed_unknown: set[tuple[str, str]] = set()
        for case in unknown_cases:
            active_budget().checkpoint()
            if not isinstance(case, dict):
                fail(f"Codex {version} contract contains an invalid unknown-value case")
            surface = case.get("surface")
            expected = case.get("expected")
            if not isinstance(surface, str) or not isinstance(expected, str):
                fail(f"Codex {version} contract contains an invalid unknown-value case")
            observed_unknown.add((surface, expected))
        if (
            len(unknown_cases) != len(REQUIRED_UNKNOWN_CASES)
            or observed_unknown != REQUIRED_UNKNOWN_CASES
        ):
            fail(f"Codex {version} contract has incomplete shared unknown-value coverage")

        mutation_failures = contract.get("mutationFailureCases")
        if not isinstance(mutation_failures, list):
            fail(f"Codex {version} contract has no shared mutation-failure cases")
        observed_mutation_failures: set[tuple[str, str, str]] = set()
        for case in mutation_failures:
            active_budget().checkpoint()
            if not isinstance(case, dict):
                fail(f"Codex {version} contract contains an invalid mutation-failure case")
            method = case.get("method")
            source = case.get("source")
            expected = case.get("expected")
            if (
                method not in MUTATION_FAILURE_METHODS
                or source not in MUTATION_FAILURE_EXPECTATIONS
                or expected != MUTATION_FAILURE_EXPECTATIONS[source]
            ):
                fail(f"Codex {version} contract contains an invalid mutation-failure case")
            observed_mutation_failures.add((method, source, expected))
        required_mutation_failures = {
            (method, source, expected)
            for method in MUTATION_FAILURE_METHODS
            for source, expected in MUTATION_FAILURE_EXPECTATIONS.items()
        }
        if (
            len(mutation_failures) != len(required_mutation_failures)
            or observed_mutation_failures != required_mutation_failures
        ):
            fail(f"Codex {version} contract has incomplete mutation-failure coverage")


@budgeted
def verify_manifest(version: str, selection: Selection, policy: dict[str, Any]) -> None:
    manifest_path = SCHEMAS_ROOT / version / "manifest.json"
    schema_path = SCHEMAS_ROOT / version / "selected.schema.json"
    wire_path = WIRE_ROOT / f"{rust_version_module(version)}.rs"
    audit_path = SCHEMAS_ROOT / version / "incoming-audit.json"
    manifest_bytes = read_bounded_bytes(manifest_path)
    schema_bytes = read_bounded_bytes(schema_path)
    wire_bytes = read_bounded_bytes(wire_path)
    audit_bytes = read_bounded_bytes(audit_path)
    bundle = load_bundle(version)
    expected_schema = canonical_bytes(bundle)
    if schema_bytes != expected_schema:
        fail(f"Codex {version} normalized schema is not canonical")
    if list(bundle["roots"].keys()) != sorted(
        root.name for root in selected_roots_for_version(selection, version)
    ):
        fail(f"Codex {version} selected roots are stale")
    expected_wire = render_wire(
        version, selection.protocol_family, sha256_bytes(expected_schema), bundle
    )
    if wire_bytes != expected_wire:
        fail(f"Codex {version} generated Rust does not match the normalized schema")
    expected_audit = canonical_bytes(incoming_audit(version, bundle))
    if audit_bytes != expected_audit:
        fail(f"Codex {version} incoming enum/union audit is stale")
    expected_manifest = canonical_bytes(
        manifest_for(version, selection, expected_schema, expected_wire, expected_audit, policy)
    )
    if manifest_bytes != expected_manifest:
        fail(f"Codex {version} manifest is stale")


@budgeted
def validate_compatibility_review(
    baseline: str, candidate: str, report: dict[str, Any]
) -> str:
    path = COMPATIBILITY_REVIEWS_ROOT / f"{baseline}-to-{candidate}.json"
    raw_bytes = read_bounded_bytes(path)
    review = load_json(path)
    if canonical_bytes(review) != raw_bytes:
        fail(f"Codex {candidate} compatibility review is not canonical")
    expected_keys = {
        "baselineVersion",
        "breakingChangeCount",
        "candidateVersion",
        "decision",
        "evidence",
        "formatVersion",
        "reportSha256",
    }
    if not isinstance(review, dict) or set(review) != expected_keys:
        fail(f"Codex {candidate} compatibility review has an invalid format")
    report_path = REPORTS_ROOT / f"{baseline}-to-{candidate}.json"
    report_bytes = read_bounded_bytes(report_path)
    if (
        review.get("formatVersion") != 1
        or review.get("baselineVersion") != baseline
        or review.get("candidateVersion") != candidate
        or review.get("decision") != "supported"
        or review.get("reportSha256") != sha256_bytes(report_bytes)
        or review.get("breakingChangeCount") != report.get("summary", {}).get("breaking")
        or review.get("evidence") != REQUIRED_COMPATIBILITY_REVIEW_EVIDENCE
    ):
        fail(f"Codex {candidate} compatibility review does not bind the exact report and evidence")
    return sha256_bytes(raw_bytes)


@budgeted
def verify_all() -> None:
    selection = read_selection()
    policy = read_policy()
    history = read_history()
    if policy["protocolFamily"] != selection.protocol_family:
        fail("support policy and schema selection protocol families differ")
    if history.get("protocolFamily") != selection.protocol_family:
        fail("support history and schema selection protocol families differ")
    historical_versions = [release["version"] for release in history["releases"]]
    if policy["supportedVersions"] != sorted(historical_versions, key=version_key):
        fail("support policy must retain every version in append-only support history")
    versions = sorted(
        set(policy["supportedVersions"] + policy["candidateVersions"] + historical_versions),
        key=version_key,
    )
    for version in versions:
        active_budget().checkpoint()
        verify_manifest(version, selection, policy)
        validate_contract(version)

    expected_mod = render_wire_mod(policy, versions)
    actual_mod = read_bounded_bytes(WIRE_ROOT / "mod.rs")
    if actual_mod != expected_mod:
        fail("generated wire module registry is stale")

    for release in history["releases"]:
        active_budget().checkpoint()
        version = release["version"]
        schema = read_bounded_bytes(SCHEMAS_ROOT / version / "selected.schema.json")
        contract_bytes = read_bounded_bytes(CONTRACTS_ROOT / f"{version}.json")
        wire = read_bounded_bytes(WIRE_ROOT / f"{rust_version_module(version)}.rs")
        if release["schemaSha256"] != sha256_bytes(schema):
            fail(f"Codex {version} support history schema hash is stale")
        if release["contractSha256"] != sha256_bytes(contract_bytes):
            fail(f"Codex {version} support history contract hash is stale")
        if release["rustWireSha256"] != sha256_bytes(wire):
            fail(f"Codex {version} support history Rust hash is stale")

    baseline = ESTABLISHED_BASELINE_VERSION
    for candidate in policy["candidateVersions"]:
        active_budget().checkpoint()
        for supported in historical_versions:
            active_budget().checkpoint()
            expected_report = compatibility_report(supported, candidate)
            json_path = REPORTS_ROOT / f"{supported}-to-{candidate}.json"
            markdown_path = REPORTS_ROOT / f"{supported}-to-{candidate}.md"
            actual_json = read_bounded_bytes(json_path)
            actual_markdown = read_bounded_bytes(markdown_path)
            if (
                actual_json != canonical_bytes(expected_report)
                or actual_markdown != report_markdown(expected_report)
            ):
                fail(f"Codex {candidate} compatibility report is stale")

    for supported in policy["supportedVersions"]:
        active_budget().checkpoint()
        if supported == baseline:
            continue
        expected_report = compatibility_report(baseline, supported)
        json_path = REPORTS_ROOT / f"{baseline}-to-{supported}.json"
        markdown_path = REPORTS_ROOT / f"{baseline}-to-{supported}.md"
        actual_json = read_bounded_bytes(json_path)
        actual_markdown = read_bounded_bytes(markdown_path)
        if (
            actual_json != canonical_bytes(expected_report)
            or actual_markdown != report_markdown(expected_report)
        ):
            fail(f"Codex {supported} compatibility report is stale")
        if not expected_report["compatible"]:
            review_sha = validate_compatibility_review(
                baseline, supported, expected_report
            )
            release = next(
                item for item in history["releases"] if item["version"] == supported
            )
            if release.get("compatibilityReviewSha256") != review_sha:
                fail(f"Codex {supported} support history compatibility review hash is stale")


def default_report_paths(baseline: str, candidate: str) -> tuple[Path, Path]:
    stem = f"{baseline}-to-{candidate}"
    return REPORTS_ROOT / f"{stem}.json", REPORTS_ROOT / f"{stem}.md"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser("sync", help="export and normalize an exact Codex binary schema")
    sync_parser.add_argument("--binary", required=True, type=Path)
    sync_parser.add_argument("--check", action="store_true", help="compare with committed output without writing")

    diff_parser = subparsers.add_parser("diff", help="classify a committed candidate schema")
    diff_parser.add_argument("--baseline", required=True)
    diff_parser.add_argument("--candidate", required=True)
    diff_parser.add_argument("--json", type=Path)
    diff_parser.add_argument("--markdown", type=Path)
    diff_parser.add_argument("--write-defaults", action="store_true")
    diff_parser.add_argument("--allow-breaking", action="store_true")

    contract_parser = subparsers.add_parser("contract", help="validate committed fixtures against selected schemas")
    contract_parser.add_argument("--version", action="append", dest="versions")

    history_parser = subparsers.add_parser(
        "verify-history", help="verify the support ledger only appends to a trusted prior copy"
    )
    history_parser.add_argument("--previous", required=True, type=Path)

    subparsers.add_parser("verify", help="offline verification of manifests, contracts, reports, and promotion gates")
    return parser.parse_args(argv)


@budgeted
def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.command == "sync":
        version = sync(args.binary, check=args.check)
        print(json.dumps({"codexVersion": version, "status": "verified" if args.check else "synced"}, sort_keys=True))
        return 0
    if args.command == "diff":
        json_path = args.json
        markdown_path = args.markdown
        if args.write_defaults:
            json_path, markdown_path = default_report_paths(args.baseline, args.candidate)
        report = write_report(args.baseline, args.candidate, json_path, markdown_path)
        print(json.dumps({"compatible": report["compatible"], **report["summary"]}, sort_keys=True))
        return 0 if report["compatible"] or args.allow_breaking else 2
    if args.command == "contract":
        policy = read_policy()
        versions = args.versions or policy["supportedVersions"] + policy["candidateVersions"]
        if len(versions) > MAX_TRACKED_VERSIONS:
            fail("contract validation requested too many versions")
        for version in versions:
            active_budget().checkpoint()
            validate_contract(version)
        print(json.dumps({"contracts": sorted(set(versions), key=version_key), "status": "valid"}, sort_keys=True))
        return 0
    if args.command == "verify-history":
        verify_history_append_only(args.previous)
        print(json.dumps({"history": "append-only", "status": "valid"}, sort_keys=True))
        return 0
    if args.command == "verify":
        verify_all()
        policy = read_policy()
        print(
            json.dumps(
                {
                    "candidates": policy["candidateVersions"],
                    "selectedWireVersion": policy["selectedWireVersion"],
                    "status": "valid",
                    "supportedVersions": policy["supportedVersions"],
                },
                sort_keys=True,
            )
        )
        return 0
    fail("unknown command")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SchemaToolError as error:
        print(f"codex-schema: {error}", file=sys.stderr)
        raise SystemExit(1) from None
    except Exception:
        print("codex-schema: maintenance operation failed safely", file=sys.stderr)
        raise SystemExit(1) from None
