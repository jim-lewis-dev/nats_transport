"""Behavior tests using a real, isolated Core NATS server.

Set NATS_SERVER_BIN to a server executable, or install nats-server on PATH.
"""

import asyncio
import contextlib
import os
from pathlib import Path
import queue
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from nats_transport import NatsTransport, NatsTransportError, NatsTransportTimeout


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class NatsServer:
    def __init__(self, *options):
        self.binary = os.environ.get("NATS_SERVER_BIN") or shutil.which("nats-server")
        if not self.binary:
            if os.environ.get("REQUIRE_NATS_SERVER"):
                raise RuntimeError("a real nats-server is required for this test run")
            raise unittest.SkipTest("install nats-server or set NATS_SERVER_BIN")
        self.options = options
        self.port = free_port()
        self.url = f"nats://127.0.0.1:{self.port}"
        self.process = None

    def start(self):
        self.process = subprocess.Popen(
            [self.binary, "-a", "127.0.0.1", "-p", str(self.port), *self.options],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("NATS server exited during startup")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.1):
                    return self
            except OSError:
                time.sleep(0.01)
        self.stop()
        raise RuntimeError("NATS server did not start")

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.stop()


class TransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = NatsServer().start()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def transport(self, **options):
        transport = NatsTransport(self.server.url, **options)
        self.addCleanup(transport.close)
        return transport

    def test_binary_empty_json_and_order(self):
        inbox = queue.Queue()
        receiver, sender = self.transport(), self.transport()
        receiver.subscribe("test.data", inbox.put)
        messages = [b"", b"\x00\xff", b'{"command":"status","value":42}']
        messages += [str(index).encode() for index in range(25)]
        for data in messages:
            sender.publish("test.data", data)
        sender.flush()
        self.assertEqual([inbox.get(timeout=2) for _ in messages], messages)

    def test_subscription_is_ready_when_subscribe_returns(self):
        inbox = queue.Queue()
        receiver, sender = self.transport(), self.transport()
        receiver.subscribe("test.*", inbox.put)
        sender.publish("test.ready", b"first")
        sender.flush()
        self.assertEqual(inbox.get(timeout=2), b"first")

    def test_callback_can_publish_and_flush_on_its_own_transport(self):
        receiver, sender = self.transport(), self.transport()
        inbox = queue.Queue()
        sender.subscribe("test.reply", inbox.put)

        def reply(data):
            receiver.publish("test.reply", data.upper())
            receiver.flush()

        receiver.subscribe("test.request", reply)
        sender.publish("test.request", b"hello")
        sender.flush()
        self.assertEqual(inbox.get(timeout=2), b"HELLO")

    def test_slow_callback_does_not_block_another_subscription(self):
        receiver, sender = self.transport(), self.transport()
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        inbox = queue.Queue()

        def slow(_data):
            entered.set()
            release.wait(2)

        receiver.subscribe("test.slow", slow)
        receiver.subscribe("test.fast", inbox.put)
        sender.publish("test.slow", b"slow")
        self.assertTrue(entered.wait(2))
        receiver.flush(timeout=0.2)
        sender.publish("test.fast", b"fast")
        self.assertEqual(inbox.get(timeout=0.5), b"fast")
        release.set()

    def test_callback_can_initiate_close(self):
        receiver, sender = self.transport(), self.transport()
        returned = threading.Event()

        def close(_data):
            receiver.close()
            returned.set()

        receiver.subscribe("test.close", close)
        sender.publish("test.close", b"close")
        sender.flush()
        self.assertTrue(returned.wait(2))
        receiver.close()
        self.assertFalse(receiver._thread.is_alive())

    def test_callback_failure_is_reported_and_next_message_runs(self):
        errors, inbox = queue.Queue(), queue.Queue()
        receiver, sender = self.transport(on_error=errors.put), self.transport()

        def callback(data):
            if data == b"fail":
                raise ValueError("application callback failed")
            inbox.put(data)

        receiver.subscribe("test.errors", callback)
        with self.assertLogs("nats_transport", level="ERROR"):
            sender.publish("test.errors", b"fail")
            error = errors.get(timeout=2)
        self.assertIsInstance(error, NatsTransportError)
        self.assertIn("application callback failed", str(error))
        sender.publish("test.errors", b"next")
        self.assertEqual(inbox.get(timeout=2), b"next")

    def test_async_callback_is_rejected_before_subscription(self):
        transport = self.transport()

        async def callback(_data):
            pass

        class AsyncCallback:
            async def __call__(self, _data):
                pass

        for function in (callback, AsyncCallback()):
            with self.assertRaisesRegex(NatsTransportError, "synchronous"):
                transport.subscribe("test.async", function)

    def test_callback_returning_coroutine_is_reported(self):
        errors = queue.Queue()
        receiver, sender = self.transport(on_error=errors.put), self.transport()

        async def result():
            pass

        receiver.subscribe("test.coroutine", lambda _data: result())
        with self.assertLogs("nats_transport", level="ERROR"):
            sender.publish("test.coroutine", b"go")
            self.assertIn("synchronous", str(errors.get(timeout=2)))

    def test_error_hook_can_publish_without_blocking_network_loop(self):
        inbox = queue.Queue()
        sender = self.transport()
        sender.subscribe("test.error.reply", inbox.put)
        receiver = self.transport(on_error=lambda _error: receiver.publish("test.error.reply", b"reported"))
        receiver.subscribe("test.error.hook", lambda _data: 1 / 0)
        with self.assertLogs("nats_transport", level="ERROR"):
            sender.publish("test.error.hook", b"fail")
            self.assertEqual(inbox.get(timeout=2), b"reported")

    def test_publish_timeout_cancels_wait_without_automatic_retry(self):
        transport = self.transport(operation_timeout=0.15)
        called, cancelled = threading.Event(), threading.Event()

        async def stalled_publish(*_args):
            called.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        with patch.object(transport._nc, "publish", stalled_publish):
            started = time.monotonic()
            with self.assertRaises(NatsTransportTimeout):
                transport.publish("test.deadline", b"data", timeout=0.05)
            self.assertLess(time.monotonic() - started, 0.25)
            self.assertTrue(called.is_set())
            self.assertTrue(cancelled.wait(1))

    def test_per_call_timeout_cannot_extend_default(self):
        transport = self.transport(operation_timeout=0.05)

        async def stalled_publish(*_args):
            await asyncio.Event().wait()

        with patch.object(transport._nc, "publish", stalled_publish):
            started = time.monotonic()
            with self.assertRaises(NatsTransportTimeout):
                transport.publish("test.deadline", b"data", timeout=10)
            self.assertLess(time.monotonic() - started, 0.25)

    def test_invalid_subject_is_reported_and_closed_calls_fail(self):
        transport = self.transport()
        with self.assertRaises(NatsTransportError):
            transport.subscribe("", lambda _data: None)
        with self.assertRaises(NatsTransportError):
            transport.publish("", b"data")
        with self.assertRaisesRegex(NatsTransportError, "bytes"):
            transport.publish("test.text", "text")
        transport.close()
        transport.close()
        for operation in (
            lambda: transport.publish("test.closed", b"data"),
            transport.flush,
            lambda: transport.subscribe("test.closed", lambda _data: None),
        ):
            with self.assertRaisesRegex(NatsTransportError, "closed"):
                operation()

    def test_close_and_publish_race_is_bounded(self):
        transport = self.transport()
        started, finished = threading.Event(), threading.Event()
        failures = []

        def publish():
            started.set()
            try:
                while True:
                    transport.publish("test.race", b"data")
            except NatsTransportError:
                pass
            except BaseException as error:
                failures.append(error)
            finally:
                finished.set()

        thread = threading.Thread(target=publish, daemon=True)
        thread.start()
        self.assertTrue(started.wait(1))
        transport.close()
        self.assertTrue(finished.wait(1))
        thread.join(1)
        self.assertEqual(failures, [])

    def test_slow_consumer_is_reported_with_bounded_pending_queue(self):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        errors = queue.Queue()
        receiver = self.transport(pending_msgs_limit=2, on_error=errors.put)
        sender = self.transport()

        def slow(_data):
            entered.set()
            release.wait(2)

        receiver.subscribe("test.backlog", slow)
        sender.publish("test.backlog", b"first")
        self.assertTrue(entered.wait(2))
        with self.assertLogs("nats_transport", level="ERROR"):
            for _ in range(20):
                sender.publish("test.backlog", b"next")
            error = errors.get(timeout=2)
            release.set()
            sender.flush()
            receiver.flush()
            # Await the receive loop so its remaining overflow reports are captured.
            time.sleep(0.05)
        self.assertIn("SlowConsumer", str(error))

    def test_unfinished_callback_does_not_make_close_wait_forever(self):
        receiver = NatsTransport(self.server.url, close_timeout=0.3)
        sender = self.transport()
        entered, release = threading.Event(), threading.Event()
        receiver.subscribe("test.blocked", lambda _data: (entered.set(), release.wait(5)))
        sender.publish("test.blocked", b"block")
        sender.flush()
        self.assertTrue(entered.wait(2))
        try:
            started = time.monotonic()
            with self.assertLogs("nats_transport", level="ERROR"):
                with self.assertRaises(NatsTransportTimeout):
                    receiver.close()
            self.assertLess(time.monotonic() - started, 0.6)
            self.assertFalse(receiver._thread.is_alive())
        finally:
            release.set()
            for worker in receiver._workers:
                worker.thread.join(1)
            receiver.close()


class ConnectionTests(unittest.TestCase):
    def test_refused_ip_and_hostname_clean_up_and_redact_credentials(self):
        for host in ("127.0.0.1", "localhost", socket.gethostname()):
            started = time.monotonic()
            with self.assertRaises(NatsTransportError) as caught:
                NatsTransport(f"nats://alice:private_password@{host}:{free_port()}",
                              connect_timeout=0.3, close_timeout=0.3)
            self.assertNotIn("private_password", str(caught.exception))
            self.assertNotIn("alice:", str(caught.exception))
            self.assertLess(time.monotonic() - started, 0.8)

    def test_connect_deadline_covers_a_listener_that_never_speaks_nats(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(8)
            port = listener.getsockname()[1]
            started = time.monotonic()
            with self.assertRaises(NatsTransportTimeout):
                NatsTransport(f"nats://127.0.0.1:{port}",
                              connect_timeout=0.15, close_timeout=0.3)
            self.assertLess(time.monotonic() - started, 0.7)
        self.assertFalse(any(t.name == "nats-transport" for t in threading.enumerate()))

    def test_failed_connection_with_error_hook_leaves_no_threads(self):
        with self.assertRaises(NatsTransportError):
            NatsTransport(f"nats://127.0.0.1:{free_port()}", on_error=lambda _error: None)
        self.assertFalse(any(t.name.startswith("nats-transport") for t in threading.enumerate()))

    def test_reconnect_restores_subscription_and_close_during_reconnect(self):
        with NatsServer() as server:
            errors, inbox = queue.Queue(), queue.Queue()
            receiver = NatsTransport(server.url, reconnect_time_wait=0.03, on_error=errors.put)
            self.addCleanup(receiver.close)
            receiver.subscribe("test.reconnect", inbox.put)
            with self.assertLogs("nats_transport", level="ERROR"):
                server.stop()
                errors.get(timeout=2)
                server.start()
                deadline = time.monotonic() + 3
                while not receiver._nc.is_connected and time.monotonic() < deadline:
                    time.sleep(0.01)
                receiver.flush(timeout=1)
            with contextlib.closing(NatsTransport(server.url)) as sender:
                sender.publish("test.reconnect", b"after restart")
                sender.flush()
                self.assertEqual(inbox.get(timeout=2), b"after restart")
            with self.assertLogs("nats_transport", level="ERROR"):
                server.stop()
                errors.get(timeout=2)
                receiver.close()
            self.assertFalse(receiver._thread.is_alive())

    def test_credentials_and_authentication_failure(self):
        with NatsServer("--user", "alice", "--pass", "correct_password") as server:
            transport = NatsTransport(server.url, user="alice", password="correct_password")
            transport.flush()
            transport.close()
            url = server.url.replace("nats://", "nats://alice:wrong_private_password@")
            with self.assertLogs("nats_transport", level="ERROR"):
                with self.assertRaises(NatsTransportError) as caught:
                    NatsTransport(url, connect_timeout=0.2, close_timeout=0.3,
                                  reconnect_time_wait=0.02)
            self.assertNotIn("wrong_private_password", str(caught.exception))

    @unittest.skipUnless(shutil.which("openssl"), "openssl is needed to generate the temporary test certificate")
    def test_tls_certificate_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cert, key = root / "cert.pem", root / "key.pem"
            subprocess.run([
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-keyout", str(key), "-out", str(cert), "-days", "1", "-subj", "/CN=localhost",
                "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            configuration = root / "server.conf"
            configuration.write_text(f'tls {{ cert_file: "{cert}"; key_file: "{key}" }}\n')
            with NatsServer("-c", str(configuration)) as server:
                context = ssl.create_default_context(cafile=str(cert))
                transport = NatsTransport(server.url.replace("nats://", "tls://"), tls=context)
                transport.flush()
                transport.close()


class ConfigurationTests(unittest.TestCase):
    def test_invalid_deadlines_limits_and_async_error_hook(self):
        for field in ("connect_timeout", "operation_timeout", "close_timeout"):
            for value in (0, -1, float("inf"), float("nan"), True, "5"):
                with self.subTest(field=field, value=value), self.assertRaises(NatsTransportError):
                    NatsTransport(**{field: value})
        for field in ("pending_msgs_limit", "pending_bytes_limit"):
            for value in (0, -1, True, 1.5):
                with self.subTest(field=field, value=value), self.assertRaises(NatsTransportError):
                    NatsTransport(**{field: value})

        async def error_hook(_error):
            pass

        with self.assertRaisesRegex(NatsTransportError, "synchronous"):
            NatsTransport(on_error=error_hook)


if __name__ == "__main__":
    unittest.main()
