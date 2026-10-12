# Publication review — 11 October 2026

**Conclusion:** the original wrapper was a useful prototype. The revised
transport is ready for local use and a standalone repository review. A public
release still needs CI results on the intended Python versions and operating
systems. The project is licensed under MIT, Copyright (c) 2026 Jim Lewis.

## Keep

- Live Core NATS; no JetStream, persistence, or replay.
- Synchronous construction, subscribe, publish, and close calls carrying bytes.
- The existing NATS subjects and Agent Messenger JSON envelope.
- Agent Messenger for existing named agents, request/reply, and discovery.
- Application-owned interpretation and processing of commands and JSON.

Agent Two and HINT contained byte-identical copies of the original wrapper.
HINT's Messenger was slightly older: Agent Two already recorded receipt times
and deducted listener time from outgoing request allowances. The coordinated
update gives HINT those same corrections. Both projects now contain identical
transport and Messenger files.

## Corrected behavior

| Original issue | Revised behavior |
| --- | --- |
| Imported an agent exception class | Standalone transport exception classes; Messenger translates transport failures at its boundary |
| Callbacks ran on the networking loop | Dedicated callback threads preserve per-subscription order and permit publish/flush/close re-entry |
| Async callbacks silently produced unawaited coroutines | Synchronous contract checked; returned awaitables are reported and returned coroutine objects closed |
| Connection errors were ignored | Logged errors and an optional separate synchronous error hook |
| Blocking waits had no deadline | Configurable connection, operation, and shutdown allowances; per-call publication budgets |
| DNS names bypassed the quick TCP check | DNS/TCP checks share the initial connection allowance |
| Shutdown during reconnection could fail with tasks still pending | Explicit close while disconnected, task cancellation, and bounded thread cleanup |
| Server URL credentials appeared in wrapper errors | Configured URL credentials removed from error text |
| Slow callbacks could obstruct network work | Separate subscription workers and bounded native inbound backlog, with overflow reported |
| No standalone publishing project | Package metadata, README, examples, live-server tests, and CI workflow |

Messenger forwards remaining request, response, and discovery time to the local
publication call. No envelope fields or subject names were added. No legacy
format switches or compatibility adapters are included. A custom Transport must
accept `publish(subject, bytes, *, timeout=None)` and honor an allowance when
supplied. Ordinary two-argument calls are still supported.

## Validation

On Linux with Python 3.12.14 and a real NATS server 2.15.1:

| Check | Result |
| --- | --- |
| Agent Two complete suite, nats-py 2.15.0 | 160 tests passed, including its original 131 tests |
| HINT transport/Messenger/remote tests, nats-py 2.15.0 | 30 tests passed; real remote `--list` worker and client, temporary empty signal data |
| Standalone transport, nats-py 2.16.0 | 22 tests passed |
| Callback-triggered close | 40 consecutive checks passed |

Tests cover byte delivery and ordering, subscription readiness, concurrent
subscriptions, reentrant publication, error handling, bounded backlog,
timeouts/cancellation, reconnect, shutdown, credentials, and TLS verification.
Messenger checks preserve its original envelope, match simultaneous replies,
exercise discovery, and bound publication by the request/discovery allowance.
The package builds into a wheel and source distribution. Its CLI example and
the generated Agent Two installation archive are also checked separately.

The CI matrix is provided but has not run on GitHub. Python 3.10/3.14 and other
operating systems have not been tested here. Agent Two's ordinary suite mocks
model behavior; no real LLM inference is part of this transport verification.
HINT checks do not run physical handlers, arm the RTC, or install services.

## Limits and release decisions

Publication and flushing do not confirm remote processing. Offline recipients
miss live messages, and a slow consumer may drop messages at its backlog limit.
The underlying client may hold outgoing messages in memory during reconnection;
the wrapper adds no durable delivery or retries. A timeout cannot retract traffic
already sent or undo completed application work.

Python callbacks cannot be forcibly killed; close reports a timeout if one
outlives the shutdown budget. The transport bounds its own inbound backlog, not
the queues created by Messenger or application handlers.

The actual Agent Zero checkout was not supplied. Its documentation and the
development demos were reviewed, but this update does not claim to update or
verify an unseen Agent Zero installation. Keep its Messenger layer and coordinate
the same source update there when its files are available. The experimental
chat/native-async-request demos are not part of this public transport package.

Confirm the repository/package name before public release.
The runtime is a single source file; a separate repository with its supporting
docs, tests, and examples is the recommended home. No existing agent repository
needs to be rewritten or split in place.
The standalone repository includes no updater, exporter, or consumer-sync scripts.
