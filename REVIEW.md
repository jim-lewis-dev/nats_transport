# Design and behavior notes

NATS Transport is a small synchronous byte transport over live Core NATS.
It can be used directly or underneath
[Agent Messenger](https://github.com/jim-lewis-dev/agent_messenger).
See the [README](README.md) for installation, examples, and the API.

## Responsibilities

| Layer | Owns |
| --- | --- |
| Application | Meaning of commands and JSON, processing, scheduling, permissions, and retry policy |
| Agent Messenger, when used | Peer names, one JSON envelope format, request IDs, reply matching, and discovery |
| NATS Transport | Byte publication and subscription, local wait limits, callback delivery, and transport errors |
| nats-py and the NATS server | NATS protocol, connections, reconnection, and live subject routing |

The transport has no dependency on an agent application and does not execute
received commands. It does not define a second request/reply envelope, provide
JetStream, or store messages for offline recipients.

## Implemented behavior

| Concern | Contract |
| --- | --- |
| Public calls | Synchronous construction, `subscribe(topic, callback)`, `publish(topic, data)`, `flush()`, and `close()` |
| Payload | Bytes; applications choose their own encoding |
| Errors | Standalone `NatsTransportError` and `NatsTransportTimeout` exceptions |
| Callback execution | Separate worker per subscription, preserving that subscription's arrival order |
| Reentrant callbacks | Callbacks can publish, flush, or subscribe without blocking the network event loop |
| Async callbacks | Rejected; callbacks and error hooks are synchronous |
| Wait limits | Configurable connection, operation, and shutdown allowances; optional shorter per-call publication allowance |
| Error reporting | Logging and an optional separate synchronous error hook |
| Backlog | Bounded inbound subscription queues; overflow is reported and may drop messages |
| Shutdown | Bounded drain, cancellation, and thread cleanup, including while reconnecting |
| Configured URL credentials | Removed from wrapper error messages |

The original two-argument `publish(topic, data)` call remains valid.
`publish(topic, data, *, timeout=None)` adds an optional keyword allowance.
Messenger passes its remaining request, reply, or discovery allowance through
that keyword. Custom transports used with Messenger must honor this contract.

## Limits

- Publication confirms local acceptance, not remote processing. Flushing confirms
  a server round trip, not an application acknowledgment.
- Offline recipients miss live traffic. The underlying client may buffer outgoing
  messages in memory during reconnection; this is not durable storage or replay.
- A timeout cannot retract traffic already sent or undo work already started.
  The wrapper does not add automatic retries or deduplication.
- Python callbacks cannot be forcibly killed. A callback outliving the shutdown
  budget produces a timeout; its daemon thread may finish later.
- Transport backlog limits do not bound queues created by Messenger or an
  application's own handlers.
- Peer authentication, TLS, and subject authorization are configured in the NATS
  client/server setup. Message-level command validation belongs to the application.

## Validation and upgrades

The repository's tests use isolated NATS servers and cover byte delivery,
ordering, subscription readiness, callback reentry, error reporting, backlog
overflow, deadlines, shutdown, reconnect, authentication, and certificate-verified
TLS. The CI workflow defines Python 3.10, 3.12, and 3.14 with nats-py 2.15 and 2.16;
consult [Actions](https://github.com/jim-lewis-dev/nats_transport/actions) for
actual run results.

Consuming applications need their own integration tests. A transport test cannot
establish that a desktop handler, scheduler, or agent application works correctly
on its target computer.

Consume this library as a package pinned to a reviewed commit or release.
Application installations are updated separately from Git pushes. Messenger and
other consumers should adopt new transport versions deliberately, with their
compatibility tests. This repository contains the transport, documentation,
examples, packaging, and tests; application deployment belongs to each consumer.

MIT licensed. Copyright (c) 2026 Jim Lewis.
