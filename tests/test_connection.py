"""Deterministic tests for repository connection lifecycle semantics."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import logging
import threading
import time

import pytest

from essential_open_api.connection import (
    AuthenticationFailure,
    ConnectionManager,
    ErrorCategory,
    RepositoryUnavailable,
    UnknownWriteOutcome,
)


class RemoteException(RuntimeError):
    """Test double whose type name is classified as a Java RMI failure."""


class FakeProject:
    def __init__(self, identifier: int) -> None:
        self.identifier = identifier
        self.dead = False
        self.disposed = threading.Event()

    def dispose(self) -> None:
        self.disposed.set()


class Harness:
    def __init__(self) -> None:
        self.available = True
        self.authenticated = True
        self.connect_calls = 0
        self.probe_calls = 0
        self.projects: list[FakeProject] = []
        self._lock = threading.Lock()

    def connect(self):
        with self._lock:
            self.connect_calls += 1
            identifier = self.connect_calls
        if not self.authenticated:
            raise AuthenticationFailure("credential-value-must-not-leak")
        if not self.available:
            raise RemoteException("java.rmi.ConnectException: connection refused")
        project = FakeProject(identifier)
        self.projects.append(project)
        return project, {"project": identifier}

    def probe(self, project, _knowledge_base) -> None:
        with self._lock:
            self.probe_calls += 1
        if not self.available or project.dead:
            raise RemoteException("java.rmi.NoSuchObjectException: stale stub")


def manager_for(harness: Harness, **overrides) -> ConnectionManager:
    options = {
        "repository": "disposable-test",
        "initial_backoff_seconds": 0.01,
        "maximum_backoff_seconds": 0.02,
        "probe_interval_seconds": 0.0,
    }
    options.update(overrides)
    return ConnectionManager(
        harness.connect,
        harness.probe,
        disposer=lambda project: project.dispose(),
        **options,
    )


def wait_until(predicate, timeout: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def test_clean_connection_has_real_probe_and_safe_metadata():
    harness = Harness()
    manager = manager_for(harness)

    lease = manager.acquire(force_probe=True)
    status = manager.status()

    assert lease.generation == 1
    assert harness.connect_calls == 1
    assert harness.probe_calls >= 2  # candidate validation plus forced readiness
    assert status["status"] == "READY"
    assert status["repository"] == "disposable-test"
    assert status["connected_since"]
    assert status["last_successful_probe"]
    assert "password" not in str(status).lower()


def test_background_start_recovers_when_server_becomes_available():
    harness = Harness()
    harness.available = False
    manager = manager_for(harness)
    manager.start()
    try:
        assert wait_until(lambda: harness.connect_calls >= 1)
        assert manager.status()["status"] == "NOT_READY"

        harness.available = True
        assert wait_until(lambda: manager.status()["status"] == "READY")
        assert manager.status()["connection_generation"] == 1
    finally:
        manager.stop()


def test_stale_non_null_proxy_never_reports_ready():
    harness = Harness()
    manager = manager_for(harness)
    lease = manager.acquire()
    lease.project.dead = True

    assert manager.current_lease() is not None
    assert manager.readiness() is False
    assert manager.current_lease() is None
    assert manager.status()["state"] == "INVALID"


def test_invalidation_and_reconnect_increment_generation_and_counter():
    harness = Harness()
    manager = manager_for(harness)
    first = manager.acquire()

    assert manager.invalidate(ErrorCategory.CONNECTION, generation=first.generation)
    second = manager.acquire()

    assert second.generation == first.generation + 1
    assert second.project is not first.project
    assert manager.status()["reconnect_count"] == 1
    assert first.project.disposed.wait(0.5)


def test_old_generation_cannot_invalidate_new_connection():
    harness = Harness()
    manager = manager_for(harness)
    first = manager.acquire()
    manager.invalidate(ErrorCategory.CONNECTION, generation=first.generation)
    second = manager.acquire()

    assert not manager.invalidate(
        ErrorCategory.CONNECTION, generation=first.generation
    )
    assert manager.current_lease() == second


def test_concurrent_callers_create_only_one_connection():
    harness = Harness()
    entered = threading.Event()
    release = threading.Event()

    def slow_connect():
        with harness._lock:  # pylint: disable=protected-access
            harness.connect_calls += 1
            identifier = harness.connect_calls
        entered.set()
        assert release.wait(1.0)
        project = FakeProject(identifier)
        harness.projects.append(project)
        return project, {"project": identifier}

    manager = ConnectionManager(
        slow_connect,
        harness.probe,
        initial_backoff_seconds=0.01,
        probe_interval_seconds=60.0,
    )
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(manager.ensure_connection, 1.0) for _ in range(10)]
        assert entered.wait(0.5)
        release.set()
        generations = [future.result().generation for future in futures]

    assert generations == [1] * 10
    assert harness.connect_calls == 1


def test_outage_backoff_prevents_busy_loop_and_is_bounded():
    harness = Harness()
    harness.available = False
    manager = manager_for(harness)

    with pytest.raises(RepositoryUnavailable) as first:
        manager.ensure_connection()
    with pytest.raises(RepositoryUnavailable) as second:
        manager.ensure_connection()

    assert harness.connect_calls == 1
    assert 0 < first.value.retry_after_seconds <= 0.02
    assert second.value.retry_after_seconds <= first.value.retry_after_seconds
    time.sleep(0.025)
    with pytest.raises(RepositoryUnavailable) as third:
        manager.ensure_connection()
    assert harness.connect_calls == 2
    assert third.value.retry_after_seconds <= 0.021


def test_read_connection_failure_reconnects_and_retries_once():
    harness = Harness()
    manager = manager_for(harness)
    calls: list[int] = []

    def read(lease):
        calls.append(lease.generation)
        if len(calls) == 1:
            raise RemoteException("java.rmi.ConnectException")
        return lease.knowledge_base["project"]

    assert manager.execute_read(read) == 2
    assert calls == [1, 2]
    assert manager.status()["connection_generation"] == 2


def test_read_reconnect_failure_returns_clear_unavailable_error():
    harness = Harness()
    manager = manager_for(harness)

    def read(_lease):
        harness.available = False
        raise RemoteException("java.rmi.ConnectException")

    with pytest.raises(RepositoryUnavailable) as failure:
        manager.execute_read(read)

    assert failure.value.category is ErrorCategory.CONNECTION
    assert manager.status()["status"] == "NOT_READY"


def test_read_is_retried_only_once():
    harness = Harness()
    manager = manager_for(harness)
    attempts = 0

    def read(_lease):
        nonlocal attempts
        attempts += 1
        raise RemoteException("java.rmi.ConnectException")

    with pytest.raises(RepositoryUnavailable):
        manager.execute_read(read)
    assert attempts == 2


def test_non_connection_read_error_is_not_retried():
    harness = Harness()
    manager = manager_for(harness)
    attempts = 0

    def read(_lease):
        nonlocal attempts
        attempts += 1
        raise ValueError("invalid query")

    with pytest.raises(ValueError):
        manager.execute_read(read)
    assert attempts == 1


def test_write_transport_failure_is_not_replayed_and_is_unknown():
    harness = Harness()
    manager = manager_for(harness)
    attempts = 0

    def write(_lease):
        nonlocal attempts
        attempts += 1
        raise RemoteException("response lost after commit")

    with pytest.raises(UnknownWriteOutcome) as outcome:
        manager.execute_write(write)

    assert attempts == 1
    assert outcome.value.outcome == "UNKNOWN_OUTCOME"
    assert outcome.value.generation == 1
    assert manager.status()["status"] == "NOT_READY"


def test_invalid_credentials_fail_closed_and_are_not_logged(caplog):
    harness = Harness()
    harness.authenticated = False
    manager = manager_for(harness)

    with caplog.at_level(logging.WARNING):
        with pytest.raises(RepositoryUnavailable) as failure:
            manager.ensure_connection()

    assert failure.value.category is ErrorCategory.AUTHENTICATION
    assert manager.status()["last_error_category"] == "AUTHENTICATION"
    assert "credential-value-must-not-leak" not in caplog.text


def test_validate_performs_remote_probe_for_the_same_generation():
    harness = Harness()
    manager = manager_for(harness)
    lease = manager.acquire()
    before = harness.probe_calls

    manager.validate(lease)

    assert harness.probe_calls == before + 1
    assert manager.status()["connection_generation"] == lease.generation
