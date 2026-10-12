#!/usr/bin/env python3
"""Send text or JSON, or listen for live messages on a subject."""

import argparse
import json
import logging
import queue
import sys

from nats_transport import NatsTransport, NatsTransportError


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="nats://127.0.0.1:4222")
    operations = parser.add_subparsers(dest="operation", required=True)
    send = operations.add_parser("send", help="send one live message")
    send.add_argument("subject")
    send.add_argument("text", nargs="?", help="read stdin if omitted")
    send.add_argument("--json", action="store_true", help="validate the supplied JSON")
    listen = operations.add_parser("listen", help="listen until Ctrl-C")
    listen.add_argument("subject", help="may contain NATS wildcards")
    listen.add_argument("--count", type=int, default=0, help="stop after N messages; 0 means unlimited")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
    transport = None
    try:
        if args.operation == "send":
            data = args.text if args.text is not None else sys.stdin.read()
            if args.json:
                json.loads(data)
            transport = NatsTransport(args.server)
            transport.publish(args.subject, data.encode("utf-8"))
            transport.flush()
        else:
            if args.count < 0:
                parser.error("--count must be nonnegative")
            inbox = queue.Queue()
            transport = NatsTransport(args.server)
            transport.subscribe(args.subject, inbox.put)
            print(f"Listening on {args.subject}; Ctrl-C to stop", file=sys.stderr, flush=True)
            count = 0
            while args.count == 0 or count < args.count:
                data = inbox.get()
                try:
                    text = data.decode("utf-8")
                except UnicodeDecodeError:
                    text = f"binary: {data.hex()}"
                print(text, flush=True)
                count += 1
        return 0
    except KeyboardInterrupt:
        return 130
    except (NatsTransportError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    finally:
        if transport is not None:
            try:
                transport.close()
            except NatsTransportError as error:
                print(f"ERROR during close: {error}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
