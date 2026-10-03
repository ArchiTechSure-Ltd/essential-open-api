"""Tests for the JPype/RMI adapter around the generic connection manager."""

from __future__ import annotations

import pytest

from essential_open_api.connection import AuthenticationFailure
from essential_open_api import jvm


class FakeProject:
    def __init__(self, server=None, session=None) -> None:
        self._server = server
        self._session = session
        self.kb = object()

    def getKnowledgeBase(self):
        return self.kb

    def getServer(self):
        return self._server

    def getSession(self):
        return self._session


class FakeRemoteServer:
    def __init__(self, *, authenticated=True) -> None:
        self.authenticated = authenticated
        self.open_session_args = None
        self.open_project_args = None
        self.available_calls = 0
        self.status_calls = 0

    def openSession(self, username, address, password):
        self.open_session_args = (username, address, password)
        return object() if self.authenticated else None

    def openProject(self, project, session):
        self.open_project_args = (project, session)
        return object()

    def getAvailableProjectNames(self, _session):
        self.available_calls += 1
        return ["disposable"]

    def getProjectStatus(self, _project):
        self.status_calls += 1
        return "READY"


def configure_fake_jpype(monkeypatch, remote_server):
    lookups = []

    class Naming:
        @staticmethod
        def lookup(url):
            lookups.append(url)
            return remote_server

    class Server:
        @staticmethod
        def getBoundName():
            return "ProtegeServer"

    class SystemUtilities:
        @staticmethod
        def getMachineIpAddress():
            return "127.0.0.1"

    class RemoteClientProject:
        @staticmethod
        def createProject(server, _server_project, session, poll_events):
            assert server is remote_server
            assert poll_events is True
            return FakeProject(server, session)

    classes = {
        "java.rmi.Naming": Naming,
        "edu.stanford.smi.protege.server.Server": Server,
        "edu.stanford.smi.protege.util.SystemUtilities": SystemUtilities,
        "edu.stanford.smi.protege.server.RemoteClientProject": RemoteClientProject,
    }
    monkeypatch.setattr(jvm, "start_jvm", lambda: None)
    monkeypatch.setattr(jvm.jpype, "JClass", classes.__getitem__)
    monkeypatch.setattr(jvm, "PROTEGE_MODE", "server")
    monkeypatch.setattr(jvm, "PROTEGE_SERVER", "rmi.example:5100")
    monkeypatch.setattr(jvm, "PROTEGE_PROJECT", "disposable")
    monkeypatch.setattr(jvm, "PROTEGE_USERNAME", "reader")
    monkeypatch.setattr(jvm, "PROTEGE_PASSWORD", "secret")
    monkeypatch.setattr(jvm, "PROTEGE_POLL_EVENTS", True)
    return lookups


def test_server_connector_performs_fresh_lookup_session_and_project(monkeypatch):
    remote_server = FakeRemoteServer()
    lookups = configure_fake_jpype(monkeypatch, remote_server)

    project, knowledge_base = jvm._connect_server()  # pylint: disable=protected-access

    assert lookups == ["//rmi.example:5100/ProtegeServer"]
    assert remote_server.open_session_args == ("reader", "127.0.0.1", "secret")
    assert remote_server.open_project_args[0] == "disposable"
    assert knowledge_base is project.kb


def test_server_connector_rejects_invalid_credentials(monkeypatch):
    remote_server = FakeRemoteServer(authenticated=False)
    configure_fake_jpype(monkeypatch, remote_server)

    with pytest.raises(AuthenticationFailure):
        jvm._connect_server()  # pylint: disable=protected-access


def test_server_probe_reaches_session_and_project(monkeypatch):
    remote_server = FakeRemoteServer()
    session = object()
    project = FakeProject(remote_server, session)
    monkeypatch.setattr(jvm, "PROTEGE_MODE", "server")
    monkeypatch.setattr(jvm, "PROTEGE_PROJECT", "disposable")

    jvm._probe(project, project.kb)  # pylint: disable=protected-access

    assert remote_server.available_calls == 1
    assert remote_server.status_calls == 1


def test_rmi_timeouts_are_opt_in_and_mapped_accurately(monkeypatch):
    names = (
        "RMI_IDLE_CONNECTION_TIMEOUT_MS",
        "RMI_INCOMING_READ_TIMEOUT_MS",
        "RMI_HANDSHAKE_TIMEOUT_MS",
        "RMI_RESPONSE_TIMEOUT_MS",
    )
    for name in names:
        monkeypatch.delenv(name, raising=False)
    assert jvm._configured_rmi_jvm_arguments() == []  # pylint: disable=protected-access

    monkeypatch.setenv("RMI_IDLE_CONNECTION_TIMEOUT_MS", "15000")
    monkeypatch.setenv("RMI_INCOMING_READ_TIMEOUT_MS", "7200000")
    monkeypatch.setenv("RMI_HANDSHAKE_TIMEOUT_MS", "60000")
    monkeypatch.setenv("RMI_RESPONSE_TIMEOUT_MS", "30000")

    assert set(jvm._configured_rmi_jvm_arguments()) == {  # pylint: disable=protected-access
        "-Dsun.rmi.transport.connectionTimeout=15000",
        "-Dsun.rmi.transport.tcp.readTimeout=7200000",
        "-Dsun.rmi.transport.tcp.handshakeTimeout=60000",
        "-Dsun.rmi.transport.tcp.responseTimeout=30000",
    }


def test_invalid_rmi_timeout_fails_before_jvm_start(monkeypatch):
    monkeypatch.setenv("RMI_RESPONSE_TIMEOUT_MS", "-1")
    with pytest.raises(RuntimeError):
        jvm._configured_rmi_jvm_arguments()  # pylint: disable=protected-access
