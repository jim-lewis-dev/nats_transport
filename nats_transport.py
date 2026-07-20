#!/usr/bin/env python3
"""NATS publish/subscribe adapter for AgentMessenger."""

import asyncio
import threading

from agent_messenger import AgentMessengerError


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
            daemon=True,
        )
        self._thread.start()

        try:
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

    async def _connect(self):
        self._nc = await self._nats.connect(self.server)

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
