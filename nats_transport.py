"""Synchronous live NATS pub/sub with ordered callbacks outside the network loop."""

from __future__ import annotations

import asyncio
from concurrent.futures import TimeoutError as FutureTimeout
import inspect
import logging
import math
import queue
import threading
import time
from urllib.parse import urlsplit, urlunsplit

__version__ = "0.1.0"


class NatsTransportError(Exception):
    """A connection, transport operation, or callback failed."""


class NatsTransportTimeout(NatsTransportError):
    """A transport operation exceeded its local waiting allowance."""


def _seconds(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise NatsTransportError(f"{name} must be a positive finite number")
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise NatsTransportError(f"{name} must be a positive finite number")
    return value


def _synchronous(callback):
    return callable(callback) and not (
        inspect.iscoroutinefunction(callback)
        or inspect.iscoroutinefunction(getattr(callback, "__call__", None))
    )


class _CallbackWorker:
    """One daemon worker per subscription; the client owns the bounded backlog."""

    def __init__(self, owner, callback, *, errors=False):
        self.owner = owner
        self.callback = callback
        self.errors = errors
        self.jobs = queue.Queue(maxsize=128 if errors else 1)
        self.stopped = threading.Event()
        self.thread = threading.Thread(
            target=self._run,
            name="nats-transport-errors" if errors else "nats-transport-callback",
            daemon=True,
        )
        self.thread.start()

    def put(self, data, future=None):
        if self.stopped.is_set():
            raise NatsTransportError("NATS callback worker is closed")
        self.jobs.put_nowait((data, future))

    def stop(self):
        self.stopped.set()
        try:
            self.jobs.put_nowait(None)
        except queue.Full:
            pass

    def _run(self):
        while not self.stopped.is_set():
            job = self.jobs.get()
            if job is None or self.stopped.is_set():
                return
            data, future = job
            error = None
            try:
                result = self.callback(data)
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result):
                        result.close()
                    raise NatsTransportError("callbacks must be synchronous")
            except BaseException as exc:
                error = self.owner._error("callback", exc)
            if future is None:
                if error is not None:
                    self.owner._logger.error("NATS error handler failed: %s", error)
            else:
                try:
                    self.owner._loop.call_soon_threadsafe(self._complete, future, error)
                except RuntimeError:
                    # A timed-out shutdown can outlive a running user callback.
                    pass

    @staticmethod
    def _complete(future, error):
        if not future.done():
            if error is None:
                future.set_result(None)
            else:
                future.set_exception(error)


class NatsTransport:
    """Carry bytes over Core NATS; application protocols stay with the caller.

    Callbacks run serially within each subscription on dedicated daemon threads.
    publish() queues locally; flush() confirms a server round trip. Neither
    confirms recipient processing. No messages are retried by this wrapper.
    """

    def __init__(
        self,
        server="nats://127.0.0.1:4222",
        *,
        connect_timeout=5.0,
        operation_timeout=5.0,
        close_timeout=5.0,
        pending_msgs_limit=1024,
        pending_bytes_limit=8 * 1024 * 1024,
        on_error=None,
        **nats_options,
    ):
        self.connect_timeout = _seconds(connect_timeout, "connect_timeout")
        self.operation_timeout = _seconds(operation_timeout, "operation_timeout")
        self.close_timeout = _seconds(close_timeout, "close_timeout")
        for name, value in (
            ("pending_msgs_limit", pending_msgs_limit),
            ("pending_bytes_limit", pending_bytes_limit),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise NatsTransportError(f"{name} must be a positive integer")
        if on_error is not None and not _synchronous(on_error):
            raise NatsTransportError("on_error must be a synchronous callable")
        if "error_cb" in nats_options:
            raise NatsTransportError("use on_error instead of error_cb")
        if "servers" in nats_options:
            raise NatsTransportError("supply the server URL using server")
        if not isinstance(server, str):
            raise NatsTransportError("server must be a NATS URL string")
        try:
            address = urlsplit(server)
            host, port = address.hostname, address.port or 4222
            if address.scheme not in {"nats", "tls"} or not host:
                raise ValueError
        except ValueError:
            raise NatsTransportError("server must be a valid nats:// or tls:// URL") from None
        try:
            import nats
        except ModuleNotFoundError as exc:
            raise NatsTransportError("install dependency: python3 -m pip install nats-py") from exc

        safe_host = f"[{host}]" if ":" in host else host
        self.server = server
        self._address = urlunsplit((address.scheme, f"{safe_host}:{port}", "", "", ""))
        self._host, self._port = host, port
        self._nats = nats
        self._options = nats_options
        self._pending_msgs_limit = pending_msgs_limit
        self._pending_bytes_limit = pending_bytes_limit
        self._logger = logging.getLogger(__name__)
        self._nc = None
        self._closed = False
        self._finished = threading.Event()
        self._close_error = None
        self._state_lock = threading.Lock()
        self._workers = []
        self._last_connect_error = None
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, name="nats-transport", daemon=True)
        self._thread.start()
        self._error_worker = None
        try:
            if on_error is not None:
                self._error_worker = self._worker(on_error, errors=True)
            self._wait(self._connect(), self.connect_timeout, "connect")
        except BaseException:
            try:
                self.close()
            except Exception:
                pass
            raise

    def subscribe(self, topic, callback):
        """Register a synchronous callback receiving bytes; wait for readiness."""
        if not _synchronous(callback):
            raise NatsTransportError("callback must be a synchronous callable")
        worker = self._worker(callback)
        try:
            self._wait(self._subscribe(topic, worker), self.operation_timeout, "subscribe")
        except BaseException:
            worker.stop()
            raise

    def publish(self, topic, data, *, timeout=None):
        """Queue bytes locally, with an optional shorter per-call deadline."""
        if not isinstance(data, bytes):
            raise NatsTransportError("data must be bytes")
        allowance = self._allowance(timeout)
        self._wait(self._nc.publish(topic, data), allowance, "publish")

    def flush(self, timeout=None):
        """Wait for a server round trip covering preceding protocol traffic."""
        allowance = self._allowance(timeout)
        self._wait(self._nc.flush(timeout=allowance), allowance, "flush")

    def close(self):
        """Drain and stop within close_timeout; repeated calls are harmless.

        From a callback this initiates shutdown and returns, allowing that
        callback to finish. A later external close() waits for completion.
        """
        with self._state_lock:
            if self._finished.is_set():
                return
            first = not self._closed
            self._closed = True
            in_callback = any(threading.current_thread() is worker.thread for worker in self._workers)
        if in_callback or threading.current_thread() is self._thread:
            if first:
                threading.Thread(target=self._finish_close, name="nats-transport-close", daemon=True).start()
            return
        if first:
            self._finish_close()
        elif not self._finished.wait(self.close_timeout):
            raise NatsTransportTimeout("NATS shutdown is still in progress")
        if self._close_error is not None:
            raise self._close_error

    def _allowance(self, timeout):
        return self.operation_timeout if timeout is None else min(
            self.operation_timeout, _seconds(timeout, "timeout")
        )

    def _worker(self, callback, *, errors=False):
        with self._state_lock:
            if self._closed:
                raise NatsTransportError("NATS transport is closed")
            worker = _CallbackWorker(self, callback, errors=errors)
            self._workers.append(worker)
        return worker

    async def _connect(self):
        # This TCP check includes DNS in the same overall connection allowance.
        try:
            _, writer = await asyncio.open_connection(self._host, self._port)
            writer.close()
            await writer.wait_closed()
        except OSError as exc:
            raise self._error("connect", exc) from None
        self._nc = self._nats.NATS()
        options = dict(self._options)
        options.setdefault("drain_timeout", self.close_timeout)
        await self._nc.connect(
            self.server,
            connect_timeout=self.connect_timeout,
            error_cb=self._report_error,
            **options,
        )

    async def _subscribe(self, topic, worker):
        async def receive(message):
            future = self._loop.create_future()
            worker.put(message.data, future)
            await future

        subscription = None
        try:
            subscription = await self._nc.subscribe(
                topic, cb=receive,
                pending_msgs_limit=self._pending_msgs_limit,
                pending_bytes_limit=self._pending_bytes_limit,
            )
            await self._nc.flush(timeout=self.operation_timeout)
        except BaseException:
            if subscription is not None and not self._nc.is_closed:
                await subscription.unsubscribe()
            raise

    def _wait(self, coroutine, timeout, operation, *, closing=False):
        with self._state_lock:
            if (self._closed and not closing) or not self._thread.is_alive():
                coroutine.close()
                raise NatsTransportError("NATS transport is closed")
            if threading.current_thread() is self._thread:
                coroutine.close()
                raise NatsTransportError("blocking transport calls cannot run on the NATS event loop")
            future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout=timeout)
        except FutureTimeout:
            future.cancel()
            detail = f"{operation} exceeded {timeout:g}s for {self._address}"
            if operation == "connect" and self._last_connect_error is not None:
                detail += f"; last error: {self._last_connect_error}"
            raise NatsTransportTimeout(detail) from None
        except NatsTransportError:
            raise
        except Exception as exc:
            raise self._error(operation, exc) from None
        except BaseException:
            future.cancel()
            raise

    def _error(self, operation, error):
        detail = " ".join(str(error).split())
        detail = f"{type(error).__name__}: {detail}" if detail else type(error).__name__
        detail = detail.replace(self.server, self._address)
        return NatsTransportError(f"NATS {operation} failed for {self._address}: {detail}")

    async def _report_error(self, error):
        error = error if isinstance(error, NatsTransportError) else self._error("connection", error)
        self._last_connect_error = error
        self._logger.error("%s", error)
        if self._error_worker is not None:
            try:
                self._error_worker.put(error)
            except (queue.Full, NatsTransportError):
                self._logger.error("NATS error handler backlog is full or closed")

    async def _drain(self, allowance):
        if self._nc is None or self._nc.is_closed:
            return
        if not self._nc.is_connected:
            await self._nc.close()
            return
        try:
            await asyncio.wait_for(self._nc.drain(), allowance)
        finally:
            if not self._nc.is_closed:
                await self._nc.close()

    def _finish_close(self):
        deadline = time.monotonic() + self.close_timeout
        # Reserve part of the allowance for forced cleanup and thread joins.
        reserve = min(0.5, self.close_timeout / 4)
        try:
            self._wait(self._drain(self.close_timeout - 2 * reserve),
                       self.close_timeout - reserve, "close", closing=True)
        except Exception as exc:
            self._close_error = exc
        finally:
            for worker in self._workers:
                worker.stop()
            if self._thread.is_alive():
                try:
                    self._loop.call_soon_threadsafe(self._loop.stop)
                except RuntimeError:
                    pass
                self._thread.join(max(0.0, deadline - time.monotonic()))
            for worker in self._workers:
                worker.thread.join(max(0.0, deadline - time.monotonic()))
            if self._thread.is_alive() or any(worker.thread.is_alive() for worker in self._workers):
                self._close_error = NatsTransportTimeout("NATS shutdown exceeded its allowance; a callback may still be running")
            if self._close_error is not None:
                self._logger.error("%s", self._close_error)
            self._finished.set()

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        finally:
            async def cancel_pending():
                tasks = asyncio.all_tasks(self._loop) - {asyncio.current_task()}
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.wait(tasks, timeout=min(0.1, self.close_timeout / 4))
            self._loop.run_until_complete(cancel_pending())
            self._loop.close()
