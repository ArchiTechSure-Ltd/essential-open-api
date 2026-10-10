"""Thread-safe lifecycle management for Protégé repository connections."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import logging
import threading
import time
from typing import Callable, Optional, TypeVar


LOGGER = logging.getLogger(__name__)
T = TypeVar("T")


class ConnectionState(str, Enum):
    """Externally visible repository connection states."""

    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    READY = "READY"
    INVALID = "INVALID"
    DEGRADED = "DEGRADED"
    RECONNECTING = "RECONNECTING"


class ErrorCategory(str, Enum):
    """Sanitised failure categories suitable for logs and health responses."""

    AUTHENTICATION = "AUTHENTICATION"
    CONFIGURATION = "CONFIGURATION"
    CONNECTION = "CONNECTION"
    SESSION = "SESSION"
    PROBE = "PROBE"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class ConnectionLease:
    """A generation-bound Project/KnowledgeBase pair."""

    project: object
    knowledge_base: object
    generation: int


class RepositoryUnavailable(RuntimeError):
    """The repository cannot currently provide a usable connection."""

    def __init__(
        self,
        category: ErrorCategory,
        *,
        retry_after_seconds: float = 0.0,
    ) -> None:
        super().__init__("Protégé repository is temporarily unavailable.")
        self.category = category
        self.retry_after_seconds = max(0.0, retry_after_seconds)


class UnknownWriteOutcome(RuntimeError):
    """A write may have reached the repository and must not be replayed."""

    outcome = "UNKNOWN_OUTCOME"

    def __init__(self, category: ErrorCategory, generation: int) -> None:
        super().__init__(
            "The repository write outcome is unknown; reconcile state before retrying."
        )
        self.category = category
        self.generation = generation


class AuthenticationFailure(RuntimeError):
    """Protégé rejected the configured credentials."""


class ConfigurationFailure(RuntimeError):
    """Required repository connection configuration is missing or invalid."""


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""

    return datetime.now(timezone.utc)


def _iso_or_none(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat().replace("+00:00", "Z") if value else None


def _exception_chain_text(exc: BaseException) -> str:
    """Collect type names and messages for classification only, never for output."""

    parts: list[str] = []
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending and len(seen) < 12:
        current = pending.pop(0)
        if id(current) in seen:
            continue
        seen.add(id(current))
        parts.extend(
            (
                f"{type(current).__module__}.{type(current).__qualname__}",
                str(current),
            )
        )
        for linked in (
            getattr(current, "__cause__", None),
            getattr(current, "__context__", None),
        ):
            if isinstance(linked, BaseException):
                pending.append(linked)
        try:
            java_class = current.getClass().getName()  # type: ignore[attr-defined]
            parts.append(str(java_class))
        except Exception:  # pragma: no cover - only available for JPype exceptions
            pass
        try:
            java_cause = current.getCause()  # type: ignore[attr-defined]
            if isinstance(java_cause, BaseException):
                pending.append(java_cause)
            elif java_cause is not None:
                parts.append(str(java_cause))
        except Exception:  # pragma: no cover - only available for JPype exceptions
            pass
    return " ".join(parts).lower()


def classify_exception(exc: BaseException) -> ErrorCategory:
    """Classify transport/session failures without retaining sensitive messages."""

    if isinstance(exc, RepositoryUnavailable):
        return exc.category
    if isinstance(exc, AuthenticationFailure):
        return ErrorCategory.AUTHENTICATION
    if isinstance(exc, ConfigurationFailure):
        return ErrorCategory.CONFIGURATION

    text = _exception_chain_text(exc)
    if any(
        marker in text
        for marker in (
            "serversessionlost",
            "session lost",
            "invalid session",
            "session is not valid",
            "nosuchobjectexception",
        )
    ):
        return ErrorCategory.SESSION
    if any(
        marker in text
        for marker in (
            "java.rmi",
            "remoteexception",
            "connectexception",
            "connectioexception",
            "marshalexception",
            "unmarshalexception",
            "socketexception",
            "sockettimeoutexception",
            "eofexception",
            "connection refused",
            "connection reset",
            "broken pipe",
            "no route to host",
            "timed out",
        )
    ):
        return ErrorCategory.CONNECTION
    return ErrorCategory.UNKNOWN


def is_connection_failure(exc: BaseException) -> bool:
    """Return whether an exception proves the current generation unusable."""

    return classify_exception(exc) in {
        ErrorCategory.CONNECTION,
        ErrorCategory.SESSION,
    }


class ConnectionManager:
    """Own and recover a single generation of repository client objects."""

    def __init__(
        self,
        connector: Callable[[], tuple[object, object]],
        probe: Callable[[object, object], None],
        *,
        disposer: Optional[Callable[[object], None]] = None,
        repository: str = "",
        mode: str = "server",
        initial_backoff_seconds: float = 1.0,
        maximum_backoff_seconds: float = 30.0,
        probe_interval_seconds: float = 5.0,
    ) -> None:
        self._connector = connector
        self._probe = probe
        self._disposer = disposer or self._default_disposer
        self._repository = repository
        self._mode = mode
        self._initial_backoff = max(0.01, initial_backoff_seconds)
        self._maximum_backoff = max(
            self._initial_backoff, maximum_backoff_seconds
        )
        self._probe_interval = max(0.0, probe_interval_seconds)

        self._condition = threading.Condition(threading.RLock())
        self._probe_lock = threading.Lock()
        self._state = ConnectionState.DISCONNECTED
        self._project: Optional[object] = None
        self._knowledge_base: Optional[object] = None
        self._generation = 0
        self._reconnect_count = 0
        self._ever_connected = False
        self._connected_since: Optional[datetime] = None
        self._last_successful_probe: Optional[datetime] = None
        self._last_failure_at: Optional[datetime] = None
        self._last_error_category: Optional[ErrorCategory] = None
        self._consecutive_failures = 0
        self._current_backoff_seconds = 0.0
        self._next_retry_monotonic = 0.0
        self._connecting = False

        self._wake_event = threading.Event()
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._worker_cycle_started_at: Optional[datetime] = None
        self._worker_last_progress_at: Optional[datetime] = None
        self._worker_last_outcome: Optional[str] = None

    @staticmethod
    def _default_disposer(project: object) -> None:
        dispose = getattr(project, "dispose", None)
        if callable(dispose):
            dispose()

    def start(self) -> None:
        """Start bounded background connection/reconnection attempts."""

        with self._condition:
            if self._worker_thread and self._worker_thread.is_alive():
                return
            self._stop_event.clear()
            self._worker_thread = threading.Thread(
                target=self._worker,
                name="protege-connection-manager",
                daemon=True,
            )
            self._worker_thread.start()
        self._wake_event.set()

    def stop(self, timeout: float = 1.0) -> None:
        """Stop the background worker and retire the active generation."""

        self._stop_event.set()
        self._wake_event.set()
        thread = self._worker_thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=max(0.0, timeout))
        self.invalidate(ErrorCategory.UNKNOWN)

    def _worker(self) -> None:
        while not self._stop_event.is_set():
            with self._condition:
                self._worker_cycle_started_at = utc_now()
            outcome = "READY"
            try:
                # Do not wait for user traffic to discover a dead session.  A
                # normal acquire probes the current generation whenever its
                # successful probe is older than the configured interval.
                self.acquire(wait_timeout=0.0)
            except RepositoryUnavailable:
                outcome = "UNAVAILABLE"
                with self._condition:
                    delay = max(
                        0.05, self._next_retry_monotonic - time.monotonic()
                    )
            except Exception:  # keep the recovery worker alive on internal faults
                outcome = "INTERNAL_ERROR"
                with self._condition:
                    if self._connecting:
                        self._record_failure_locked(ErrorCategory.UNKNOWN)
                    delay = max(
                        0.05,
                        self._initial_backoff,
                        self._next_retry_monotonic - time.monotonic(),
                    )
                LOGGER.error(
                    "Protégé connection monitor recovered from an internal error."
                )
            else:
                with self._condition:
                    if self._probe_interval <= 0:
                        delay = None
                    elif self._last_successful_probe is None:
                        delay = 0.05
                    else:
                        age = (
                            utc_now() - self._last_successful_probe
                        ).total_seconds()
                        delay = max(0.05, self._probe_interval - age)

            with self._condition:
                self._worker_cycle_started_at = None
                self._worker_last_progress_at = utc_now()
                self._worker_last_outcome = outcome

            self._wake_event.wait(delay)
            self._wake_event.clear()

    def _retry_after_locked(self) -> float:
        return max(0.0, self._next_retry_monotonic - time.monotonic())

    def _record_failure_locked(self, category: ErrorCategory) -> None:
        self._consecutive_failures += 1
        if self._current_backoff_seconds <= 0:
            delay = self._initial_backoff
        else:
            delay = min(
                self._maximum_backoff,
                self._current_backoff_seconds * 2,
            )
        self._current_backoff_seconds = delay
        self._next_retry_monotonic = time.monotonic() + delay
        self._state = ConnectionState.DEGRADED
        self._last_failure_at = utc_now()
        self._last_error_category = category
        self._connecting = False
        self._condition.notify_all()

    def _dispose_in_background(self, project: Optional[object]) -> None:
        if project is None:
            return

        def dispose() -> None:
            try:
                self._disposer(project)
            except Exception:  # best effort; stale RMI disposal may itself fail
                LOGGER.info("Old Protégé connection disposal failed safely.")

        threading.Thread(
            target=dispose,
            name="protege-connection-disposer",
            daemon=True,
        ).start()

    def ensure_connection(self, wait_timeout: float = 0.0) -> ConnectionLease:
        """Return a usable generation, allowing only one connector at a time."""

        deadline = time.monotonic() + max(0.0, wait_timeout)
        while True:
            with self._condition:
                if (
                    self._state is ConnectionState.READY
                    and self._project is not None
                    and self._knowledge_base is not None
                ):
                    return ConnectionLease(
                        self._project,
                        self._knowledge_base,
                        self._generation,
                    )

                now = time.monotonic()
                remaining = deadline - now
                retry_after = self._retry_after_locked()
                if retry_after > 0:
                    if remaining <= 0:
                        raise RepositoryUnavailable(
                            self._last_error_category or ErrorCategory.CONNECTION,
                            retry_after_seconds=retry_after,
                        )
                    self._condition.wait(timeout=min(retry_after, remaining))
                    continue

                if self._connecting:
                    if remaining <= 0:
                        raise RepositoryUnavailable(
                            self._last_error_category or ErrorCategory.CONNECTION
                        )
                    self._condition.wait(timeout=remaining)
                    continue

                self._connecting = True
                self._state = (
                    ConnectionState.RECONNECTING
                    if self._ever_connected
                    else ConnectionState.CONNECTING
                )
                break

        project: Optional[object] = None
        try:
            project, knowledge_base = self._connector()
            if project is None or knowledge_base is None:
                raise RuntimeError("Connector returned an incomplete connection.")
            self._probe(project, knowledge_base)
        except Exception as exc:
            category = classify_exception(exc)
            if category is ErrorCategory.UNKNOWN:
                category = ErrorCategory.CONNECTION
            self._dispose_in_background(project)
            with self._condition:
                self._record_failure_locked(category)
                retry_after = self._retry_after_locked()
            LOGGER.warning(
                "Protégé connection attempt failed (category=%s).",
                category.value,
            )
            raise RepositoryUnavailable(
                category, retry_after_seconds=retry_after
            ) from exc

        with self._condition:
            old_project = self._project
            if self._ever_connected:
                self._reconnect_count += 1
            self._ever_connected = True
            self._generation += 1
            self._project = project
            self._knowledge_base = knowledge_base
            self._state = ConnectionState.READY
            self._connected_since = utc_now()
            self._last_successful_probe = self._connected_since
            self._last_error_category = None
            self._consecutive_failures = 0
            self._current_backoff_seconds = 0.0
            self._next_retry_monotonic = 0.0
            self._connecting = False
            lease = ConnectionLease(project, knowledge_base, self._generation)
            self._condition.notify_all()

        if old_project is not None and old_project is not project:
            self._dispose_in_background(old_project)
        if self._reconnect_count:
            LOGGER.warning(
                "Protégé repository connection recovered "
                "(generation=%d, reconnect_count=%d).",
                lease.generation,
                self._reconnect_count,
            )
        else:
            LOGGER.info(
                "Protégé repository connection ready (generation=%d).",
                lease.generation,
            )
        return lease

    def current_lease(self) -> Optional[ConnectionLease]:
        """Return the current READY generation without performing I/O."""

        with self._condition:
            if (
                self._state is not ConnectionState.READY
                or self._project is None
                or self._knowledge_base is None
            ):
                return None
            return ConnectionLease(
                self._project, self._knowledge_base, self._generation
            )

    def acquire(
        self,
        *,
        wait_timeout: float = 0.0,
        force_probe: bool = False,
    ) -> ConnectionLease:
        """Get a connection and probe it when forced or when the probe is stale."""

        deadline = time.monotonic() + max(0.0, wait_timeout)
        lease = self.ensure_connection(wait_timeout=wait_timeout)
        with self._condition:
            probe_due = (
                force_probe
                or self._last_successful_probe is None
                or (
                    utc_now() - self._last_successful_probe
                ).total_seconds()
                >= self._probe_interval
            )
        if not probe_due:
            return lease

        remaining = max(0.0, deadline - time.monotonic())
        if not self._probe_lock.acquire(timeout=remaining):
            raise RepositoryUnavailable(ErrorCategory.PROBE)
        try:
            lease = self.ensure_connection(
                wait_timeout=max(0.0, deadline - time.monotonic())
            )
            with self._condition:
                probe_due = (
                    force_probe
                    or self._last_successful_probe is None
                    or (
                        utc_now() - self._last_successful_probe
                    ).total_seconds()
                    >= self._probe_interval
                )
            if not probe_due:
                return lease
            try:
                self._probe(lease.project, lease.knowledge_base)
            except Exception as exc:
                category = classify_exception(exc)
                if category is ErrorCategory.UNKNOWN:
                    category = ErrorCategory.PROBE
                self.invalidate(category, generation=lease.generation)
                raise RepositoryUnavailable(category) from exc
            with self._condition:
                if lease.generation == self._generation:
                    self._last_successful_probe = utc_now()
            return lease
        finally:
            self._probe_lock.release()

    def invalidate(
        self,
        category: ErrorCategory,
        *,
        generation: Optional[int] = None,
    ) -> bool:
        """Atomically retire a failed generation and request a fresh lookup."""

        with self._condition:
            if generation is not None and generation != self._generation:
                return False
            old_project = self._project
            had_connection = old_project is not None or self._knowledge_base is not None
            invalidated_generation = self._generation
            self._project = None
            self._knowledge_base = None
            self._state = ConnectionState.INVALID
            self._connected_since = None
            self._last_failure_at = utc_now()
            self._last_error_category = category
            self._next_retry_monotonic = 0.0
            if had_connection:
                self._consecutive_failures = 0
                self._current_backoff_seconds = 0.0
            self._condition.notify_all()
        self._dispose_in_background(old_project)
        self._wake_event.set()
        if had_connection:
            LOGGER.warning(
                "Protégé repository connection invalidated "
                "(generation=%d, category=%s).",
                invalidated_generation,
                category.value,
            )
        return had_connection

    def execute_read(
        self,
        operation: Callable[[ConnectionLease], T],
        *,
        wait_timeout: float = 0.0,
    ) -> T:
        """Run a read and retry once on a proven transport/session failure."""

        lease = self.acquire(wait_timeout=wait_timeout)
        try:
            return operation(lease)
        except Exception as exc:
            if not is_connection_failure(exc):
                raise
            category = classify_exception(exc)
            self.invalidate(category, generation=lease.generation)

        replacement = self.acquire(wait_timeout=wait_timeout, force_probe=True)
        try:
            return operation(replacement)
        except Exception as exc:
            if is_connection_failure(exc):
                category = classify_exception(exc)
                self.invalidate(
                    category, generation=replacement.generation
                )
                raise RepositoryUnavailable(category) from exc
            raise

    def validate(self, lease: ConnectionLease) -> None:
        """Probe a specific generation after an operation completes."""

        with self._condition:
            if (
                lease.generation != self._generation
                or self._state is not ConnectionState.READY
            ):
                raise RepositoryUnavailable(
                    self._last_error_category or ErrorCategory.CONNECTION
                )
        self._probe(lease.project, lease.knowledge_base)
        with self._condition:
            if lease.generation == self._generation:
                self._last_successful_probe = utc_now()

    def execute_write(
        self,
        operation: Callable[[ConnectionLease], T],
        *,
        wait_timeout: float = 0.0,
    ) -> T:
        """Run a write once; transport/session failure yields UNKNOWN_OUTCOME."""

        lease = self.acquire(wait_timeout=wait_timeout)
        try:
            return operation(lease)
        except Exception as exc:
            if not is_connection_failure(exc):
                raise
            category = classify_exception(exc)
            self.invalidate(category, generation=lease.generation)
            raise UnknownWriteOutcome(category, lease.generation) from exc

    def readiness(self, wait_timeout: float = 0.0) -> bool:
        """Perform a genuine remote probe and return current readiness."""

        try:
            self.acquire(wait_timeout=wait_timeout, force_probe=True)
            return True
        except RepositoryUnavailable:
            return False

    def status(self) -> dict[str, object]:
        """Return non-sensitive operational metadata."""

        with self._condition:
            return {
                "status": (
                    "READY"
                    if self._state is ConnectionState.READY
                    else "NOT_READY"
                ),
                "state": self._state.value,
                "mode": self._mode,
                "repository": self._repository,
                "connection_generation": self._generation,
                "reconnect_count": self._reconnect_count,
                "connected_since": _iso_or_none(self._connected_since),
                "last_successful_probe": _iso_or_none(
                    self._last_successful_probe
                ),
                "last_failure_at": _iso_or_none(self._last_failure_at),
                "last_error_category": (
                    self._last_error_category.value
                    if self._last_error_category
                    else None
                ),
                "retry_after_seconds": round(self._retry_after_locked(), 3),
                "background_monitor_alive": bool(
                    self._worker_thread and self._worker_thread.is_alive()
                ),
                "background_monitor_cycle_started_at": _iso_or_none(
                    self._worker_cycle_started_at
                ),
                "background_monitor_last_progress_at": _iso_or_none(
                    self._worker_last_progress_at
                ),
                "background_monitor_last_outcome": self._worker_last_outcome,
            }
