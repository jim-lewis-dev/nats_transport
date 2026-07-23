#!/usr/bin/env python3
"""NATS publish/subscribe adapter for AgentMessenger."""

import asyncio
import inspect
import ipaddress
import socket
import threading
from urllib.parse import urlparse

from agent_messenger import AgentMessengerError


DEFAULT_NATS_PORT = 4222
PREFLIGHT_TIMEOUT_SECONDS = 3


class NatsTransport:
    def __init__(self, server="nats://127.0.0.1:4222"):
        try:
            import nats
        except ModuleNotFoundError:
            raise AgentMessengerError(
                "install dependency: python3 -m pip install nats-py"
            ) from None

        self.server = server
        self._nats = nats
        self._nc = None
        self._closed = False
        self._state_lock = threading.Lock()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            name="nats-transport",
            daemon=True
        )
        self._thread.start()

        try:
            self._preflight()
            self._wait(self._connect())
        except BaseException:
            self._shutdown_loop()
            raise

    def subscribe(self, topic, callback):
        self._ensure_open()
        self._wait(self._subscribe(topic, callback))

    def publish(self, topic, data):
        self._ensure_open()
        self._wait(self._nc.publish(topic, data))

    def close(self):
        with self._state_lock:
            if self._closed:
                return
            self._closed = True

        try:
            if self._nc is not None and not self._nc.is_closed:
                self._wait(self._nc.drain())
        finally:
            self._shutdown_loop()

    def _ensure_open(self):
        with self._state_lock:
            if self._closed:
                raise AgentMessengerError("NATS transport is closed")

    def _preflight(self):
        parsed = urlparse(self.server)
        host = parsed.hostname
        port = parsed.port or DEFAULT_NATS_PORT
        if host is None:
            raise AgentMessengerError(
                f"invalid NATS server address: {self.server}"
            )

        try:
            ipaddress.ip_address(host)
        except ValueError:
            if host != "localhost":
                return

        try:
            connection = socket.create_connection(
                (host, port),
                timeout=PREFLIGHT_TIMEOUT_SECONDS
            )
        except ConnectionRefusedError:
            raise AgentMessengerError(
                f"cannot connect to NATS server {self.server}: "
                f"connection refused at {host}:{port}; the computer is reachable, "
                "but no service is accepting NATS connections there"
            ) from None
        except TimeoutError:
            raise AgentMessengerError(
                f"cannot connect to NATS server {self.server}: "
                f"timed out reaching {host}:{port}"
            ) from None
        except OSError as error:
            message = " ".join(str(error).split()) or error.__class__.__name__
            raise AgentMessengerError(
                f"cannot connect to NATS server {self.server}: {message}"
            ) from None
        else:
            connection.close()

    async def _connect(self):
        options = {}
        try:
            parameters = inspect.signature(self._nats.connect).parameters.values()
        except (TypeError, ValueError):
            parameters = ()
        if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters):
            options["error_cb"] = self._ignore_error
        self._nc = await self._nats.connect(self.server, **options)

    @staticmethod
    async def _ignore_error(_error):
        pass

    async def _subscribe(self, topic, callback):
        async def receive(message):
            callback(message.data)

        await self._nc.subscribe(topic, cb=receive)
        await self._nc.flush()

    def _wait(self, coroutine):
        if not self._thread.is_alive():
            coroutine.close()
            raise AgentMessengerError("NATS transport is closed")

        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result()

    def _shutdown_loop(self):
        if self._thread.is_alive():
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=2)

        if self._thread.is_alive():
            raise AgentMessengerError("NATS event loop thread did not stop")

        if not self._loop.is_closed():
            self._loop.close()
