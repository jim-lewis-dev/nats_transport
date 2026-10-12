# NATS Transport

A small synchronous Python transport for live communication between computers.
The runtime is one file, `nats_transport.py`, plus the `nats-py` dependency.
It sends bytes over Core NATS and has no dependency on Agent Messenger, Agent
Zero, Agent Two, or HINT. There is no JetStream or durable message storage.

## Layers and scope

| Layer | Responsibility |
| --- | --- |
| Your application, Agent Two, or HINT | Interpret JSON or commands and perform the work |
| Agent Messenger, when used | Named peers, the existing JSON envelope, request IDs, replies, and discovery |
| `NatsTransport` | Publish bytes, subscribe, report errors, and bound local waits |
| `nats-py` | NATS protocol, connection, buffering, and reconnection |
| NATS server | Route live messages by subject |

Keep Agent Messenger above this transport for existing agents and HINT. The
transport does not add a second request/reply API or envelope format. A plain
application can use it directly and decide how to encode and process its bytes.
Receiving a command does not execute it automatically.

## Install and try it

Requires Python 3.10+ and a running NATS server. The server runs independently
and is not included in this Python project.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
nats-server -a 127.0.0.1 -p 4222
```

Leave the server running. In another terminal, activate the environment and
start the listener before sending:

```bash
python examples/pubsub.py listen computers.desktop.commands
```

In a third terminal:

```bash
python examples/pubsub.py send computers.desktop.commands '{"command":"status"}' --json
python examples/pubsub.py send computers.desktop.commands 'hello'
```

For two computers, run the server on one reachable computer and use its LAN
address in `--server nats://192.168.0.107:4222` on both clients. Bind the server
to its LAN interface and allow TCP port 4222 through the relevant firewall.
Use an authenticated server; use TLS or a trusted private network when traffic
can be observed. Only run command handlers for peers you authorize. The wrapper
handles transport; authorization and command validation belong to the server
configuration and your application.

## API

```python
import json
from nats_transport import NatsTransport, NatsTransportError, NatsTransportTimeout

transport = NatsTransport(
    "nats://192.168.0.107:4222",
    connect_timeout=5,
    operation_timeout=5,
    close_timeout=5,
)
try:
    transport.subscribe("computers.desktop.commands", lambda data: print(json.loads(data)))
    transport.publish("computers.laptop.commands", b'{"command":"status"}')
    transport.flush()
finally:
    transport.close()
```

| Call | Meaning |
| --- | --- |
| `NatsTransport(server, **options)` | Connect before returning; the default URL is `nats://127.0.0.1:4222` |
| `subscribe(topic, callback)` | Wait until a synchronous `callback(bytes)` subscription is registered at the server |
| `publish(topic, data, *, timeout=None)` | Queue a `bytes` message with an optional shorter local allowance |
| `flush(timeout=None)` | Confirm a server round trip covering earlier protocol traffic |
| `close()` | Drain and stop within the configured shutdown allowance; repeated calls are harmless |

`timeout` on `publish` is keyword-only. NATS wildcard subjects work for
subscriptions. The wrapper does not offer unsubscribe, queue groups, multiple
initial server URLs, native request/reply, or async callbacks.

The original positional and keyword names are retained: `subscribe(topic,
callback)` and `publish(topic, data)`. The optional publication keyword is an
addition; the original two-argument call remains valid. The callback still gets
bytes. Internally, callbacks now run outside the network event loop. Direct
transport failures now raise `NatsTransportError`; updated Agent Messenger
translates those into its own `AgentMessengerError`.

`publish()` returning means the client accepted the message locally.
`flush()` confirms server communication, not receipt or processing by another
computer. To obtain an application answer, use Agent Messenger's existing
`request(peer, text, timeout=10)` / `respond(request_id, text)` calls. Its
`send(peer, text)` is one-way; JSON can be encoded as the existing string payload.

## Deadlines

All three constructor timeouts default to five seconds and accept positive,
finite numbers of seconds:

- `connect_timeout` bounds initial DNS/TCP/NATS connection waiting.
- `operation_timeout` bounds each subscribe, publish, or flush wait.
- `close_timeout` budgets draining, forced cleanup, and thread joins together.

For example, `operation_timeout=5` with `publish(..., timeout=0.4)` allows at
most 0.4 seconds of local waiting. A larger per-call timeout cannot extend the
configured default. Expiry raises `NatsTransportTimeout`, a subclass of
`NatsTransportError`, and cancels the queued coroutine. Cancellation cannot undo
traffic already sent; this wrapper does not automatically retry timed-out calls.
Initial connection failure can add a brief cleanup interval.

Agent Messenger's request timeout is a separate allowance for publication and
waiting for the answer. The updated Agent Two and HINT integrations pass the
remaining allowance into publication. Their existing envelope still carries a
duration; each receiver uses its own monotonic clock. This does not require
synchronized clocks, and it is not a shared end-to-end timestamp. Transit time
is not subtracted from the receiver's duration.

Transport deadlines do not forcibly terminate your Python message handler.
Application code must enforce its own processing deadlines where needed.

## Callbacks, errors, and shutdown

Each subscription has one daemon callback thread. It processes messages in
order for that subscription; separate subscriptions can run concurrently.
Callbacks can publish, flush, or subscribe without blocking the networking
event loop. Async callbacks and callbacks returning awaitables are rejected.

Callback and connection errors are logged through Python's `nats_transport`
logger. Supply `on_error=lambda error: ...` for an additional synchronous error
hook running on its own thread. The hook can call transport operations. A
failing hook is logged; it is not recursively called. API failures raise
`NatsTransportError`. Credentials in the configured server URL are removed from
wrapper error messages; application exception text is otherwise preserved.

Inbound backlog limits default to 1,024 messages and 8 MiB per subscription;
set `pending_msgs_limit` / `pending_bytes_limit` to change them. A slow consumer
can drop messages once these limits are reached, and the client reports that
error. Keep handlers short or enqueue work into your own bounded work queue.
The transport cannot bound queues created by Agent Messenger or your application.

An external `close()` drains and waits for shutdown. Calling it inside a
transport callback starts shutdown and returns so that callback can finish; a
later external `close()` waits for completion. If a callback keeps running past
the shutdown budget, close reports a timeout and stops the network loop. Python
cannot forcibly kill that callback; its daemon thread may finish later.

## Reconnection, authentication, and TLS

Core NATS provides live, at-most-once delivery. An offline recipient misses
messages; there is no stored replay. `nats-py` automatically reconnects and may
buffer outgoing messages temporarily in memory. That buffer is not durable and
may send messages after reconnection. The wrapper does not add retries,
deduplication, delivery acknowledgments, or authentication logic.

Additional keyword options are passed to `nats-py` (except `servers` and
`error_cb`; use `server` and `on_error`). Examples:

```python
transport = NatsTransport(server, user="alice", password="secret",
                          reconnect_time_wait=1, max_reconnect_attempts=10)
```

```python
import ssl
context = ssl.create_default_context(cafile="server-ca.pem")
transport = NatsTransport("tls://server.example:4222", tls=context,
                          user_credentials="client.creds")
```

Certificate verification remains enabled. Configure the server's authentication,
TLS, and subject permissions separately. See the official
[NATS documentation](https://docs.nats.io/) and
[nats-py documentation](https://nats-io.github.io/nats.py/).

## Keep consumers current

The coordinated update supplies identical transport and Agent Messenger copies
to Agent Two and HINT. Agent Messenger remains a separate project and is not
bundled into this package.

For future transport-only updates, stop the consumers, review their Git changes,
copy the canonical module, and run their tests. For these example project paths:

```bash
cp ~/projects/nats_transport/nats_transport.py ~/projects/agent_two/nats_transport.py
cp ~/projects/nats_transport/nats_transport.py ~/projects/hint/nats_transport.py
```

If a future release changes the transport contract, update Messenger and other callers in the same
release. Prefer a pinned package dependency once the projects no longer carry
local copies; a local file shadows an installed package with the same import name.
This standalone repository contains no update, export, or consumer-sync scripts.

## Tests and publishing

```bash
python -m unittest -v
# If the executable is elsewhere:
NATS_SERVER_BIN=/path/to/nats-server REQUIRE_NATS_SERVER=1 python -m unittest -v
```

Tests use isolated real servers and cover bytes and ordering, readiness,
callback re-entry, error reporting, backlog overflow, connection and operation
timeouts, shutdown, reconnect, authentication, and verified TLS. Tests requiring
a server skip when none is installed; CI requires the server. The certificate
test also needs `openssl`. The package supports `nats-py>=2.15,<3`; the supplied
CI workflow tests 2.15 and 2.16 on Python 3.10, 3.12, and 3.14.

This directory can be the root of its own GitHub repository; it does not require
moving the agent repositories. The runtime remains one file, with docs,
examples, tests, and packaging around it. To retain history from an existing
repository, extract the relevant file's history in a separate clone rather
than rewriting the original project.

Before a public release, set the repository owner/URL and run CI on the target
platforms. The provisional
distribution name `nats-transport-wrapper` has not been checked for availability
on PyPI. No public repository or package has been published by this update.

The code is available under the MIT license. Copyright (c) 2026 Jim Lewis.
See `LICENSE` for the complete terms.
