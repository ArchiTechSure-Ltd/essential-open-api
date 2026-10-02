# FR #29 — resilient Protégé/RMI connection management

Date: 2026-10-02

## Outcome

The API now owns remote `Project` and `KnowledgeBase` objects through a thread-safe
connection manager rather than treating module globals as immortal. The manager
performs a fresh RMI registry lookup, authenticates a new session, opens the configured
project, obtains a new knowledge base, and validates the candidate before publishing a
new connection generation.

The process can remain live while Protégé is unavailable and reconnect later. Reads
may be replayed once after a proven connection/session failure. Writes are never
replayed by the connection layer; an uncertain transport failure is represented as
`UNKNOWN_OUTCOME` for later state reconciliation.

## Evidence boundaries

### API source and bundled client JAR (observed)

The feature branch is based on API commit `ea0a5ea` (`v1.1.0`). The bundled files were
inspected locally:

| Artifact | Evidence |
|---|---|
| `jars/protege.jar` | SHA-256 `d831be2091f8785bf8667bc4682e3eb2bbbab6428abef29b981b3147b03b9c32`; manifest `Protégé 3.5 (663)`, built 2019-10-17 |
| `jars/publish-service-1.0.0.jar` | SHA-256 `a3c7d3beac0caa2d9fa2f146e9a78a19d5f3e7018527e5d3a08d8738e365bfd0` |

Bytecode inspection of this exact `protege.jar` shows
`ServerProperties.heartbeatDisabled()` as only `iconst_1; ireturn`: it always returns
`true`. The `server.disable.heartbeat` constant remains in the class, but the method
does not read it. No user, metaproject, project, or Java RMI property can enable that
disabled Protégé heartbeat path in this build. This is distinct from RMI transport
socket behaviour.

`RemoteProjectManager.getProject(...)` bytecode confirms that each invocation does a
fresh `java.rmi.Naming.lookup`, calls `openSession`, calls `openProject`, and constructs
a `RemoteClientProject`. It also catches `RemoteException` and returns `null`, losing
useful failure detail. The new connector performs those steps directly so it can
distinguish rejected credentials and transport failures.

### Tower deployment (observed through the fixed read-only gateway)

The permitted inspection on 2026-10-02 showed:

- container `platform-essential-protege-server`;
- image `architechsure/essential-protege-server:local`;
- container start `2026-10-02T10:19:21Z`, restart count zero;
- published TCP ports 5100 and 5200;
- writable `/opt/EssentialAM/server` and `/repositories` mounts;
- no Docker healthcheck;
- Compose build context `./protege/build`;
- no output in the permitted 30-minute Protégé log window.

The gateway does not permit container execution, image/JAR export, arbitrary file
reads, client API container inspection, scheduler inspection, or older logs.
Consequently, the exact deployed **server** JAR hash/version, server and JVM arguments,
deployed API client JAR hash, API healthcheck, and scheduled restart definition remain
unverified. The platform client template builds API tag `v1.1.0`, which is supporting
source evidence but not an independent observation of a running client container.

The additional production evidence needed is narrowly scoped read-only output for:

1. image ID/digest and immutable build provenance;
2. hashes/manifests for the server and API client JARs;
3. `java -version`, container command, Protégé arguments, and JVM `-D` arguments;
4. the client API container health configuration and recent disconnect logs; and
5. the existing restart scheduler definition/history.

No broader shell, write, lifecycle, repository-content, or secret access is required.

## Existing failure and root cause

The old `jvm.py` assigned remote proxies to `_PROTEGE_PROJECT` and `_KNOWLEDGE_BASE`
once. `load_pprj()` returned them forever whenever `_KNOWLEDGE_BASE` was non-null.
`/health` repeated the same non-null test. There was no remote probe, invalidation,
fresh lookup, generation, reconnect lock, or backoff. A stale Java proxy could
therefore remain cached indefinitely and still be reported healthy.

Deterministic tests reproduce the relevant failure paths with RMI-shaped
`java.rmi.ConnectException` and `java.rmi.NoSuchObjectException` failures. They show a
non-null stale generation becoming `INVALID`, readiness returning 503, a new generation
being created after availability returns, and reads resuming without an API restart.

An actual JPype exception/cause chain from a disposable server stop/restart was not
captured in this environment: Docker is not installed locally, no disposable
Protégé project/server fixture is present, and the production gateway correctly
forbids lifecycle operations. Production was not changed or restarted.

## State and concurrency model

The manager exposes:

`DISCONNECTED -> CONNECTING -> READY -> INVALID/DEGRADED -> RECONNECTING -> READY`

It owns:

- the current Project and KnowledgeBase;
- connection generation and state;
- connected timestamp and last successful real probe;
- last failure timestamp and sanitised category;
- successful reconnect count; and
- bounded exponential-backoff state.

Only one thread may connect at a time. Concurrent callers wait for that bounded result
or receive a clear 503. Candidate objects are validated before an atomic generation
swap. Invalidation is generation-aware, so a late failure from generation N cannot
invalidate replacement generation N+1. Disposal is best-effort on a daemon thread so
a dead RMI close cannot block recovery.

Initial backoff defaults to 1 second, maximum backoff to 30 seconds, probe interval to
5 seconds, and request wait to 2 seconds. They are configurable with:

- `PROTEGE_RECONNECT_INITIAL_BACKOFF_SECONDS`;
- `PROTEGE_RECONNECT_MAX_BACKOFF_SECONDS`;
- `PROTEGE_PROBE_INTERVAL_SECONDS`; and
- `PROTEGE_CONNECTION_WAIT_SECONDS`.

## Real readiness and health

`GET /health/live` reports Flask process liveness plus the current `jvm_started`
indicator and always remains independent from repository state.

`GET /health/ready` performs calls through the current `RemoteServer` using the current
`RemoteSession`: `getAvailableProjectNames(session)` and
`getProjectStatus(configuredProject)`. These cross the RMI boundary and prove that the
session is recognised and the configured project remains available. A non-null local
proxy alone is never sufficient. Not-ready responses use HTTP 503.

Health metadata contains state, mode, repository/project, generation, reconnect count,
connected/probe/failure timestamps, sanitised error category, and retry delay. It does
not include server credentials, session objects, exception text, or stack traces.
The legacy `/health` remains HTTP 200 for compatibility but now reports honest
`READY`/`NOT_READY` and sets `kb_loaded` from the real probe.

## Read and write semantics

Repository GET routes execute as a generation-bound read. On a classified RMI/session
failure they invalidate that generation, reconnect, and replay the complete read once.
A second failure returns 503; no unbounded loop exists.

Mutation routes execute once. A classified transport/session failure invalidates the
generation and returns:

```json
{
  "outcome": "UNKNOWN_OUTCOME",
  "retrySafe": false,
  "connectionGeneration": 1,
  "errorCategory": "CONNECTION"
}
```

FR #28 must reconnect and read the current post-state before classifying
`VERIFIED_SUCCESS`, `NOT_APPLIED`, `CONFLICT`, or remaining `UNKNOWN`. Only a separately
proved idempotent operation may then be retried. This layer does not claim that
multi-step writes are transactions.

## Publication

Bytecode inspection of the exact bundled PublishService shows that
`startPublishAsync`:

1. creates a random UUID;
2. inserts in-memory log and `RUNNING` status entries;
3. starts a daemon thread named with the UUID; and
4. returns the UUID.

The daemon catches failures (including remote project traversal failures), records an
error log, and changes the in-memory status to `ERROR`. `getPublishStatus(id)` returns
that map entry or `NOT_FOUND`. The API never automatically calls `startPublishAsync`
again.

This permits reconciliation when the caller received the job ID. It does **not** permit
reconciliation when the HTTP response carrying a newly generated ID was lost: there is
no caller-supplied idempotency key, durable job store, or lookup by request. Jobs/status
also disappear with the API process. FR #28 must treat a lost publish-start response as
unknown and must not start a second publication automatically.

## Event polling and session lifetime

`PROTEGE_POLL_EVENTS=true` remains the default. It enables the remote frame-store event
poll path used for client cache/event synchronisation and can surface
`ServerSessionLost`; it does not create a new registry stub, session, project, or
knowledge base. It is therefore useful detection/synchronisation machinery, not a
reconnect mechanism. Disabling it is not the FR #29 solution.

Because `heartbeatDisabled()` is unconditional in the bundled build, no evidence was
found for an active configurable idle session expiry. A server restart, lost exported
RMI object, network failure, or dead cached stub is more strongly evidenced than a
normal configurable user-session timeout. The deployed server JAR still needs the
read-only verification described above.

## Java RMI transport properties

Java 8's documented properties have different scopes:

| Property | Meaning | Java 8 default |
|---|---|---|
| `sun.rmi.transport.connectionTimeout` | How long an unused pooled outbound RMI connection may remain before it can be closed; not session expiry and not TCP connect establishment | 15,000 ms |
| `sun.rmi.transport.tcp.readTimeout` | Idle read timeout for accepted/incoming RMI connections; not an outbound invocation deadline | 7,200,000 ms |
| `sun.rmi.transport.tcp.handshakeTimeout` | Client read timeout while waiting for the initial JRMP protocol acknowledgement | 60,000 ms |
| `sun.rmi.transport.tcp.responseTimeout` | Client socket read deadline while waiting for a remote invocation response | 0, unlimited |
| `sun.rmi.transport.proxy.connectTimeout` | Connect timeout for the HTTP proxy fallback path only | implementation default |

The documented Java 8 list has no equivalent `sun.rmi.transport.tcp.connectTimeout`
for an ordinary direct TCP connect; the operating-system socket connect behaviour
applies. None of these properties expires a Protégé `RemoteSession`.

The implementation supports explicit, pre-JVM opt-in values through
`RMI_IDLE_CONNECTION_TIMEOUT_MS`, `RMI_INCOMING_READ_TIMEOUT_MS`,
`RMI_HANDSHAKE_TIMEOUT_MS`, and `RMI_RESPONSE_TIMEOUT_MS`. It deliberately changes no
default. A finite response timeout is attractive for black-holed network paths, but it
is global and becomes the maximum duration of every successful RMI invocation. It must
be selected only after timing worst-case Essential queries, writes, and publication
traversals; it must not disguise a missing reconnect path.

Primary references:

- [Oracle Java 8 `sun.rmi` properties](https://docs.oracle.com/javase/8/docs/technotes/guides/rmi/sunrmiproperties.html)
- [Protégé core 3.5.1 source](https://github.com/protegeproject/protege-core/tree/protege-3.5.1)

## Verification and remaining acceptance work

`pytest` currently covers clean connection, unavailable startup and later recovery,
real readiness probes, stale proxies, invalidation, reconnect/generation changes,
single-flight concurrency, bounded backoff, one-retry reads, reconnect failure, no
write replay, `UNKNOWN_OUTCOME`, fail-closed credentials, sanitised logs/responses, and
HTTP liveness/readiness behaviour.

The remaining integration acceptance test requires an isolated disposable Protégé
server/project plus lifecycle control. It must run the sequence: API ready and GET
works; stop server; API stays live and becomes not ready; start server; generation
changes; readiness and GET recover without API restart. It must also capture the real
Python/JPype/Java cause chain. The current environment cannot run it because Docker is
unavailable and the only reachable server is the protected shared Tower service.

Therefore the daily API restart is no longer part of the implemented recovery design
and deterministic tests prove it is not required by the connection state machine.
Removing the operational workaround should wait for the isolated restart acceptance
test and deployment-specific JAR/argument verification.

The connection contract is sufficient for the FR #28 governed-mutation **POC** to
integrate real readiness, generation, invalidation, automatic reconnect, safe read
retry, and `UNKNOWN_OUTCOME`. Production/client mutation remains gated by the
disposable restart test, deployed artifact verification, publication limitation above,
and the separate checkpoint/recovery controls.
