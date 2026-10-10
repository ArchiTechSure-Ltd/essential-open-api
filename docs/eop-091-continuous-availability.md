# EOP-091 — EA API continuous availability and RMI self-recovery

EOP-091 is a post-FR29 defect fix. It retains FR29's connection generations,
single-flight connector, real readiness, bounded backoff, safe read retry, and
`UNKNOWN_OUTCOME`/no-blind-write-replay rules.

## Gate A evidence

The following read-only observations were made on Tower on 10 October 2026. No API,
Protégé, client repository, container, network, credential, or permission state was
changed.

- Demo, Lambeth, Luton, and ATS API containers were running the pinned FR29 image
  `architechsure/essential-open-api:fr29-024911ee` (source `024911ee`).
- The API containers had started on 8 October; the shared Protégé server had started on
  9 October. The API processes therefore survived a later server restart.
- The Docker health check called `/health/live` only. It correctly proved process
  liveness but did not exercise repository readiness.
- Demo, Luton, and ATS returned HTTP 503 from the first `/health/ready` call after the
  idle interval. Each reported `SESSION`, an invalidated generation, and a last
  successful probe on 8 October.
- Without restarting those API containers, the existing wake/reconnect path restored
  READY on a fresh generation. Observed failure-to-ready intervals were approximately
  8.5 seconds (Demo), 10.0 seconds (Luton), and 9.0 seconds (ATS).
- Their event-polling logs had already recorded repeated
  `java.rmi.NoSuchObjectException: no such object in table`. The event poller detected
  the dead exported object but did not notify the Python connection manager.
- Lambeth additionally recorded `java.rmi.ConnectIOException` with a nested
  `SocketTimeoutException` during JRMP connection establishment and no longer accepted
  even bounded local `/health/live` connections. It was inspected only and was not
  restarted or otherwise changed.
- Lambeth Lab used a different image without the FR29 `/health/live` and
  `/health/ready` contract. It remains an operational rollout/qualification gap; this
  source change does not deploy it.

## Root cause

The server restart invalidated server-exported RMI objects. That is the initiating
server-side event, but the continuous-availability failure was client-side lifecycle
handling:

1. after a successful connection, the FR29 background worker waited indefinitely on a
   wake event and did not run its configured five-second probe while READY;
2. Protégé event polling logged the stale session but had no path to invalidate the
   manager's cached Project/KnowledgeBase generation;
3. the first later readiness/read request performed the overdue probe, invalidated the
   generation, and returned NOT_READY before the background reconnect completed;
4. callers waiting for another thread's probe used an unbounded Python lock wait even
   though the public connection-wait setting was bounded; and
5. exponential backoff calculated an ever-growing power before applying its maximum,
   so a sufficiently long outage could overflow and terminate the recovery worker.

The evidence supports a combination of expected server-side RMI invalidation and a
client-side monitoring/recovery gap. It does not support changing global JVM/DGC or
heartbeat settings.

## Gate B remediation

The smallest application-level correction is in the existing connection manager:

- use the existing probe interval as an active background READY monitor;
- invalidate the exact failed generation and wake the existing single-flight connector;
- recreate Project, KnowledgeBase, session, cache, and event-polling state through the
  existing fresh connector;
- bound waits for an in-flight probe by the caller's connection-wait budget;
- calculate capped backoff iteratively so an arbitrarily long outage cannot overflow;
- keep the monitor alive after an unexpected internal exception; and
- expose `background_monitor_alive` with the existing non-sensitive readiness metadata.

No read or write replay rules change. Writes still execute once and return
`UNKNOWN_OUTCOME` after a classified transport/session failure.

## Verification boundary

Deterministic tests cover idle invalidation, first read after automatic repair, temporary
network loss, fresh generation, event-dependent object recreation, concurrent
single-flight connection, bounded probe-lock waiting, prolonged capped backoff, honest
readiness, safe read retry, and no write replay.

The exact pushed commit `d5f2bd74b689499f23c7f12c7c1beb859d3f9b12` was also built on
Tower and run as a labelled disposable container against Demo's read-only repository
connection on 10 October 2026:

- initial readiness was HTTP 200 at generation 1 with the background monitor alive;
- after 35 seconds with no repository request, the first repository GET returned HTTP
  200 (250 response bytes) without a reconnect or API restart;
- disconnecting only the disposable container's Docker network caused the background
  monitor to invalidate generation 1 within approximately eight seconds;
- during the interruption, liveness remained HTTP 200 while readiness returned HTTP 503,
  state `DEGRADED`, category `CONNECTION`, and a bounded retry delay;
- after reconnecting that network, the same container returned to HTTP 200 readiness at
  generation 2 with reconnect count 1; the first repository GET again returned HTTP 200;
- network-restored-to-ready took approximately 65 seconds, consistent with the existing
  bounded JRMP handshake/failure path rather than an unbounded retry loop;
- Docker restart count remained zero and the container health was healthy; and
- the disposable container, image, source directory, and root-only temporary environment
  file were all removed after the test.

A shared Protégé restart was deliberately not performed because it would disrupt
Lambeth Live. Deployment, image replacement, Lambeth recovery, and Lambeth Lab/QA
rollout remain separate approval-bound operational work.
