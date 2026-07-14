#!/usr/bin/env python3
"""NATS adapter for agent messenger.Transport. Uses publish/subscribe only."""

import asyncio
import threading

from agent_messenger import AgentMessengerError


class NatsTransport:
    def __init__(self, server="nats://127.0.0.1:4222"):
        try:
            import nats
        except ModuleNotFoundError:
            raise AgentMessengerError("install dependency: python3 -m pip install nats-py") from None
        self.server = server
        self._nats = nats
        self._nc = None
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._wait(self._connect())

    def subscribe(self, topic, callback):
        self._wait(self._subscribe(topic, callback))

    def publish(self, topic, data):
        self._wait(self._nc.publish(topic, data))

    def close(self):
        try:
            if self._nc is not None and not self._nc.is_closed:
                self._wait(self._nc.drain())
        finally:
            if self._thread.is_alive():
                self._loop.call_soon_threadsafe(self._loop.stop)
                self._thread.join(timeout=2)
            if not self._loop.is_closed():
                self._loop.close()

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
