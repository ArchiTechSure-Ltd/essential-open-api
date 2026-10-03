"""JVM setup and resilient Protégé repository access."""

from __future__ import annotations

import logging
import os
from pathlib import Path
import threading
from typing import Callable, Optional, Tuple, TypeVar

import jpype

from .connection import (
    AuthenticationFailure,
    ConfigurationFailure,
    ConnectionLease,
    ConnectionManager,
    RepositoryUnavailable,
    UnknownWriteOutcome,
    classify_exception,
)


LOGGER = logging.getLogger(__name__)
T = TypeVar("T")


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


BASE_DIR = Path(__file__).resolve().parent.parent
JARS_DIR = Path(os.environ.get("JARS_DIR", BASE_DIR / "jars"))
PPRJ_DIR = Path(os.environ.get("PPRJ_DIR", BASE_DIR / "resources"))
PPRJ_FILE = Path(
    os.environ.get("PPRJ_FILE", PPRJ_DIR / "essential_baseline_6_20.pprj")
)

PROTEGE_MODE = os.environ.get("PROTEGE_MODE", "file").strip().lower()
PROTEGE_SERVER = os.environ.get("PROTEGE_SERVER", "")
PROTEGE_PROJECT = os.environ.get("PROTEGE_PROJECT", "")
PROTEGE_USERNAME = os.environ.get("PROTEGE_USERNAME", "")
PROTEGE_PASSWORD = os.environ.get("PROTEGE_PASSWORD", "")
PROTEGE_POLL_EVENTS = os.environ.get(
    "PROTEGE_POLL_EVENTS", "true"
).strip().lower() in ("1", "true", "yes", "on")

CONNECTION_INITIAL_BACKOFF_SECONDS = _float_env(
    "PROTEGE_RECONNECT_INITIAL_BACKOFF_SECONDS", 1.0
)
CONNECTION_MAXIMUM_BACKOFF_SECONDS = _float_env(
    "PROTEGE_RECONNECT_MAX_BACKOFF_SECONDS", 30.0
)
CONNECTION_PROBE_INTERVAL_SECONDS = _float_env(
    "PROTEGE_PROBE_INTERVAL_SECONDS", 5.0
)
CONNECTION_WAIT_SECONDS = _float_env("PROTEGE_CONNECTION_WAIT_SECONDS", 2.0)

JARS_DIR.mkdir(parents=True, exist_ok=True)
PPRJ_DIR.mkdir(parents=True, exist_ok=True)
CLASSPATH = [str(jar.resolve()) for jar in JARS_DIR.glob("*.jar")]

_JVM_LOCK = threading.Lock()
_MANAGER_LOCK = threading.Lock()
_CONNECTION_MANAGER: Optional[ConnectionManager] = None


def _configured_rmi_jvm_arguments() -> list[str]:
    """Return opt-in RMI transport properties configured before JVM startup.

    These deliberately have no implicit values. In particular, a global response
    timeout also limits legitimate long-running repository calls and must be chosen
    from measured workload evidence.
    """

    properties = {
        "RMI_IDLE_CONNECTION_TIMEOUT_MS": "sun.rmi.transport.connectionTimeout",
        "RMI_INCOMING_READ_TIMEOUT_MS": "sun.rmi.transport.tcp.readTimeout",
        "RMI_HANDSHAKE_TIMEOUT_MS": "sun.rmi.transport.tcp.handshakeTimeout",
        "RMI_RESPONSE_TIMEOUT_MS": "sun.rmi.transport.tcp.responseTimeout",
    }
    arguments: list[str] = []
    for env_name, property_name in properties.items():
        value = os.environ.get(env_name, "").strip()
        if not value:
            continue
        try:
            parsed = int(value)
        except ValueError as exc:
            raise RuntimeError(
                f"{env_name} must be an integer number of milliseconds."
            ) from exc
        if parsed < 0:
            raise RuntimeError(f"{env_name} must not be negative.")
        arguments.append(f"-D{property_name}={parsed}")
    return arguments


def start_jvm() -> None:
    """Ensure the JVM is running with the configured classpath and RMI options."""

    if jpype.isJVMStarted():
        return
    with _JVM_LOCK:
        if jpype.isJVMStarted():
            return
        if not CLASSPATH:
            raise RuntimeError(
                f"No .jar files found in {JARS_DIR}. "
                "Add the Protégé JARs before starting the JVM."
            )
        jpype.startJVM(*_configured_rmi_jvm_arguments(), classpath=CLASSPATH)
        LOGGER.info("JVM started successfully.")


def jvm_is_started() -> bool:
    """Return JVM process state without starting or probing the repository."""

    return bool(jpype.isJVMStarted())


def _validate_server_configuration() -> None:
    missing = [
        name
        for name, value in (
            ("PROTEGE_SERVER", PROTEGE_SERVER),
            ("PROTEGE_PROJECT", PROTEGE_PROJECT),
            ("PROTEGE_USERNAME", PROTEGE_USERNAME),
        )
        if not value
    ]
    if missing:
        raise ConfigurationFailure(
            f"{', '.join(missing)} must be set when PROTEGE_MODE=server."
        )


def _connect_server() -> tuple[object, object]:
    """Perform a fresh RMI lookup, login, project open, and KB acquisition."""

    _validate_server_configuration()
    start_jvm()

    naming = jpype.JClass("java.rmi.Naming")
    server_class = jpype.JClass("edu.stanford.smi.protege.server.Server")
    system_utilities = jpype.JClass(
        "edu.stanford.smi.protege.util.SystemUtilities"
    )
    remote_client_project = jpype.JClass(
        "edu.stanford.smi.protege.server.RemoteClientProject"
    )

    remote_server = naming.lookup(
        f"//{PROTEGE_SERVER}/{server_class.getBoundName()}"
    )
    session = remote_server.openSession(
        PROTEGE_USERNAME,
        system_utilities.getMachineIpAddress(),
        PROTEGE_PASSWORD,
    )
    if session is None:
        raise AuthenticationFailure("Protégé authentication failed.")

    remote_server_project = remote_server.openProject(PROTEGE_PROJECT, session)
    if remote_server_project is None:
        raise RuntimeError("Protégé Server returned no project.")

    project = remote_client_project.createProject(
        remote_server,
        remote_server_project,
        session,
        PROTEGE_POLL_EVENTS,
    )
    if project is None:
        raise RuntimeError("Protégé client could not create the project.")
    knowledge_base = project.getKnowledgeBase()
    if knowledge_base is None:
        raise RuntimeError("Protégé project returned no knowledge base.")
    return project, knowledge_base


def _connect_file() -> tuple[object, object]:
    start_jvm()
    if not PPRJ_FILE.exists():
        raise RuntimeError(f"The file {PPRJ_FILE} was not found.")
    project_class = jpype.JPackage("edu.stanford.smi.protege.model").Project
    project = project_class.loadProjectFromFile(str(PPRJ_FILE), [])
    if project is None:
        raise RuntimeError("Protégé could not load the local project.")
    knowledge_base = project.getKnowledgeBase()
    if knowledge_base is None:
        raise RuntimeError("Protégé project returned no knowledge base.")
    return project, knowledge_base


def _connect() -> tuple[object, object]:
    if PROTEGE_MODE == "server":
        return _connect_server()
    if PROTEGE_MODE == "file":
        return _connect_file()
    raise RuntimeError(
        f"Unsupported PROTEGE_MODE '{PROTEGE_MODE}'. Use 'file' or 'server'."
    )


def _probe(project: object, knowledge_base: object) -> None:
    """Force a real server/session operation; a non-null proxy is insufficient."""

    if PROTEGE_MODE == "server":
        remote_server = project.getServer()
        session = project.getSession()
        available = remote_server.getAvailableProjectNames(session)
        if available is None or PROTEGE_PROJECT not in {
            str(name) for name in available
        }:
            raise RuntimeError("Configured project is not available to this session.")
        if remote_server.getProjectStatus(PROTEGE_PROJECT) is None:
            raise RuntimeError("Configured project has no server status.")
        return

    root = getattr(knowledge_base, "getRootCls", lambda: None)()
    if root is None:
        root = knowledge_base.getCls(":THING")
    if root is None:
        raise RuntimeError("Local knowledge base root is unavailable.")


def _dispose(project: object) -> None:
    dispose = getattr(project, "dispose", None)
    if callable(dispose):
        dispose()


def get_connection_manager() -> ConnectionManager:
    """Return the process-wide manager, creating it exactly once."""

    global _CONNECTION_MANAGER  # noqa: PLW0603
    if _CONNECTION_MANAGER is not None:
        return _CONNECTION_MANAGER
    with _MANAGER_LOCK:
        if _CONNECTION_MANAGER is None:
            repository = (
                PROTEGE_PROJECT if PROTEGE_MODE == "server" else PPRJ_FILE.name
            )
            _CONNECTION_MANAGER = ConnectionManager(
                _connect,
                _probe,
                disposer=_dispose,
                repository=repository,
                mode=PROTEGE_MODE,
                initial_backoff_seconds=CONNECTION_INITIAL_BACKOFF_SECONDS,
                maximum_backoff_seconds=CONNECTION_MAXIMUM_BACKOFF_SECONDS,
                probe_interval_seconds=CONNECTION_PROBE_INTERVAL_SECONDS,
            )
    return _CONNECTION_MANAGER


def start_connection_manager() -> ConnectionManager:
    """Start asynchronous connection attempts without blocking Flask startup."""

    manager = get_connection_manager()
    manager.start()
    return manager


def load_pprj() -> Optional[Tuple[object, object]]:
    """Compatibility entry point that obtains the manager's current generation."""

    manager = start_connection_manager()
    try:
        lease = manager.ensure_connection(wait_timeout=CONNECTION_WAIT_SECONDS)
        return lease.project, lease.knowledge_base
    except RepositoryUnavailable:
        return None


def _lease_or_none() -> Optional[ConnectionLease]:
    manager = start_connection_manager()
    try:
        return manager.acquire(wait_timeout=CONNECTION_WAIT_SECONDS)
    except RepositoryUnavailable:
        return None


def get_knowledge_base() -> Optional[object]:
    """Return a KnowledgeBase only from a currently READY generation."""

    lease = _lease_or_none()
    return lease.knowledge_base if lease else None


def get_project() -> Optional[object]:
    """Return a Project only from a currently READY generation."""

    lease = _lease_or_none()
    return lease.project if lease else None


def connection_status() -> dict[str, object]:
    """Return non-sensitive connection metadata."""

    return get_connection_manager().status()


def repository_ready() -> bool:
    """Perform a real repository probe for readiness."""

    return get_connection_manager().readiness(
        wait_timeout=CONNECTION_WAIT_SECONDS
    )


def execute_repository_read(
    operation: Callable[[ConnectionLease], T],
) -> T:
    """Execute a generation-bound read with at most one safe retry."""

    return get_connection_manager().execute_read(
        operation, wait_timeout=CONNECTION_WAIT_SECONDS
    )


def execute_repository_write(
    operation: Callable[[ConnectionLease], T],
) -> T:
    """Execute a write once and expose uncertain transport outcomes."""

    return get_connection_manager().execute_write(
        operation, wait_timeout=CONNECTION_WAIT_SECONDS
    )


def call_publish_async(
    project: object,
    url: str,
    user: str,
    pwd: str,
) -> Optional[str]:
    """Start one publish job; never replay a failed/uncertain invocation."""

    del project  # the generation-bound project is supplied by the manager lease

    def start(lease: ConnectionLease) -> str:
        start_jvm()
        publish_service = jpype.JClass(
            "com.enterprise_architecture.essential_os.publish_service.PublishService"
        )
        job_id = publish_service.startPublishAsync(
            lease.project, url, user, pwd
        )
        if job_id is None:
            raise RuntimeError("PublishService returned no job identifier.")
        return str(job_id)

    try:
        return execute_repository_write(start)
    except (RepositoryUnavailable, UnknownWriteOutcome):
        raise
    except Exception as exc:
        LOGGER.error(
            "Publish job could not be started (category=%s).",
            classify_exception(exc).value,
        )
        return None


def get_publish_status(job_id: str) -> Optional[Tuple[str, str]]:
    """Retrieve the in-process PublishService status and logs for a known ID."""

    try:
        start_jvm()
        publish_service = jpype.JClass(
            "com.enterprise_architecture.essential_os.publish_service.PublishService"
        )
        status = publish_service.getPublishStatus(job_id)
        logs = publish_service.getPublishLogs(job_id)
        return str(status), str(logs)
    except Exception as exc:
        LOGGER.error(
            "Publish status could not be retrieved (category=%s).",
            classify_exception(exc).value,
        )
        return None


def save_project():
    """Persist local projects; server mode persistence remains server-owned."""

    project = get_project()
    if project is None:
        return False, ["Protégé project not loaded."]
    if PROTEGE_MODE == "server":
        return True, None

    try:
        start_jvm()
        array_list = jpype.JClass("java.util.ArrayList")
        errors = array_list()
        save_method = getattr(project, "save", None)
        if callable(save_method):
            save_method(errors)
        else:
            legacy_save = getattr(project, "saveProject", None)
            if not callable(legacy_save):
                return False, ["Project object has no save method."]
            legacy_save()
        if hasattr(errors, "isEmpty") and not errors.isEmpty():
            return False, [str(error) for error in errors]
        return True, None
    except Exception as exc:  # local mode only; caller needs the save failure
        return False, [f"Error while saving project: {exc}"]


def _replace_connection_manager_for_tests(
    manager: Optional[ConnectionManager],
) -> None:
    """Replace the singleton in deterministic unit tests."""

    global _CONNECTION_MANAGER  # noqa: PLW0603
    with _MANAGER_LOCK:
        previous = _CONNECTION_MANAGER
        _CONNECTION_MANAGER = manager
    if previous is not None and previous is not manager:
        previous.stop()


__all__ = [
    "RepositoryUnavailable",
    "UnknownWriteOutcome",
    "call_publish_async",
    "connection_status",
    "execute_repository_read",
    "execute_repository_write",
    "get_connection_manager",
    "get_knowledge_base",
    "get_project",
    "get_publish_status",
    "jvm_is_started",
    "load_pprj",
    "repository_ready",
    "save_project",
    "start_connection_manager",
]
