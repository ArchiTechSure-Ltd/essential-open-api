"""HTTP health and fail-closed operation response tests."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading

import essential_open_api
from essential_open_api import create_app
from essential_open_api.connection import (
    ConnectionManager,
    ErrorCategory,
    UnknownWriteOutcome,
)
from essential_open_api import routes


class RemoteException(RuntimeError):
    pass


class Project:
    def __init__(self) -> None:
        self.dead = False

    def dispose(self) -> None:
        return None


def make_manager():
    project = Project()
    probes = {"count": 0}

    def connect():
        return project, object()

    def probe(candidate, _kb):
        probes["count"] += 1
        if candidate.dead:
            raise RemoteException("java.rmi.NoSuchObjectException")

    manager = ConnectionManager(
        connect,
        probe,
        repository="disposable-test",
        probe_interval_seconds=60.0,
        initial_backoff_seconds=0.01,
    )
    return manager, project, probes


def test_liveness_remains_up_while_repository_is_unavailable():
    def unavailable():
        raise RemoteException("java.rmi.ConnectException")

    manager = ConnectionManager(
        unavailable,
        lambda _project, _kb: None,
        repository="disposable-test",
        initial_backoff_seconds=0.01,
    )
    app = create_app(manager, start_connections=False)
    client = app.test_client()

    assert client.get("/health/live").status_code == 200
    ready = client.get("/health/ready")
    assert ready.status_code == 503
    assert ready.get_json()["status"] == "NOT_READY"


def test_readiness_forces_real_probe_and_exposes_safe_metadata():
    manager, _project, probes = make_manager()
    app = create_app(manager, start_connections=False)
    client = app.test_client()

    response = client.get("/health/ready")

    assert response.status_code == 200
    body = response.get_json()
    assert body["status"] == "READY"
    assert body["connection_generation"] == 1
    assert body["repository"] == "disposable-test"
    assert probes["count"] >= 2
    assert "credential" not in str(body).lower()


def test_stale_proxy_transitions_readiness_to_not_ready():
    manager, project, _probes = make_manager()
    app = create_app(manager, start_connections=False)
    client = app.test_client()
    assert client.get("/health/ready").status_code == 200

    project.dead = True
    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.get_json()["state"] == "INVALID"


def test_busy_probe_returns_truthful_not_ready_payload(monkeypatch):
    manager, _project, _probes = make_manager()
    manager.acquire()
    entered = threading.Event()
    release = threading.Event()
    original_probe = manager._probe  # pylint: disable=protected-access

    def blocking_probe(project, knowledge_base):
        original_probe(project, knowledge_base)
        entered.set()
        assert release.wait(1.0)

    manager._probe = blocking_probe  # pylint: disable=protected-access
    monkeypatch.setattr(essential_open_api, "CONNECTION_WAIT_SECONDS", 0.01)
    app = create_app(manager, start_connections=False)
    client = app.test_client()

    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(manager.acquire, wait_timeout=1.0, force_probe=True)
        assert entered.wait(0.5)
        response = client.get("/health/ready")

        assert response.status_code == 503
        assert response.get_json()["state"] == "READY"
        assert response.get_json()["status"] == "NOT_READY"
        release.set()
        pending.result(timeout=0.5)


def test_legacy_health_does_not_claim_ready_for_stale_proxy():
    manager, project, _probes = make_manager()
    app = create_app(manager, start_connections=False)
    client = app.test_client()
    assert client.get("/health").get_json()["kb_loaded"] is True

    project.dead = True
    response = client.get("/health")

    assert response.status_code == 200
    assert response.get_json()["status"] == "NOT_READY"
    assert response.get_json()["kb_loaded"] is False


def test_write_route_serialises_unknown_outcome_without_secrets(monkeypatch):
    manager, _project, _probes = make_manager()
    app = create_app(manager, start_connections=False)
    client = app.test_client()

    def unknown(_operation):
        raise UnknownWriteOutcome(ErrorCategory.CONNECTION, 7)

    monkeypatch.setattr(routes, "execute_repository_write", unknown)
    response = client.post(
        "/api/instances",
        json={"password": "must-not-appear", "className": "Anything"},
    )

    assert response.status_code == 503
    body = response.get_json()
    assert body["outcome"] == "UNKNOWN_OUTCOME"
    assert body["retrySafe"] is False
    assert body["connectionGeneration"] == 7
    assert "must-not-appear" not in response.get_data(as_text=True)
