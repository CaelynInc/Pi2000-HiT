#!/usr/bin/env python3
import subprocess
import pika
import sys
import time
import json
import re
import uuid
import os
import signal
from datetime import datetime, timezone

# RabbitMQ settings
RABBITMQ_HOST = "vps.caelyn.nl"
RABBITMQ_USER = "p2000"
RABBITMQ_PASS = "Pi2000"
RABBITMQ_QUEUE = "p2000"

# SDR settings
FREQUENCY = "169.65M"

# Logging
LOG_DIR = "/var/log/p2000"
LOG_FILE = f"{LOG_DIR}/p2000.log"

# Graceful shutdown flag
running = True


def log(msg):
    timestamp = datetime.now(timezone.utc).isoformat()
    line = f"{timestamp} | {msg}"

    # Print to journald (stdout)
    print(line, flush=True)

    # Append to file
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def handle_signal(signum, frame):
    global running
    running = False
    log(f"Received signal {signum}, shutting down...")


for s in (signal.SIGINT, signal.SIGTERM):
    signal.signal(s, handle_signal)


def start_decoder():
    """Start rtl_fm + multimon-ng."""
    rtl_cmd = [
        "rtl_fm",
        "-f", FREQUENCY,
        "-M", "fm",
        "-s", "22050",
        "-g", "42"
    ]

    multi_cmd = [
        "multimon-ng",
        "-a", "FLEX",
        "-t", "raw",
        "-q",
        "-"
    ]

    log("Starting decoder pipeline...")

    rtl = subprocess.Popen(
        rtl_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )

    multi = subprocess.Popen(
        multi_cmd,
        stdin=rtl.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True
    )

    return rtl, multi


def connect_rabbit():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

    while running:
        try:
            log("Connecting to RabbitMQ...")
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    credentials=credentials,
                    heartbeat=30
                )
            )
            ch = conn.channel()
            ch.queue_declare(
                queue=RABBITMQ_QUEUE,
                durable=True,
                arguments={'x-message-ttl': 300000}
            )
            log("RabbitMQ connected.")
            return conn, ch
        except Exception as e:
            log(f"RabbitMQ connection failed: {e}")
            time.sleep(5)

    return None, None


def parse_flex_line(line):
    """Parse FLEX output into structured JSON."""
    parts = line.split('|')

    if len(parts) < 7:
        message_text = line
        capcodes = []
        timestamp_raw = None
        prio = None
        grip = None
    else:
        timestamp_raw = parts[1]
        message_text = '|'.join(parts[5:]).strip()
        capcodes = parts[4].split() if parts[4].strip() else []

        prio_match = re.search(r'\b(A[1-2]|B1|P[1-3]|PRIO\s?[1-5])\b', message_text, re.I)
        prio = prio_match.group(0) if prio_match else None

        grip_match = re.search(r'\bGRIP\s?([1-4])\b', message_text, re.I)
        grip = f"GRIP {grip_match.group(1)}" if grip_match else None

    now = datetime.now(timezone.utc)

    return {
        "id": str(uuid.uuid4()),
        "protocol": "FLEX",
        "timestamp_unix": int(now.timestamp()),
        "timestamp_iso": now.isoformat(),
        "raw_flex_timestamp": timestamp_raw,
        "raw": line,
        "data": {
            "message": message_text,
            "prio": prio,
            "grip": grip,
            "capcodes": capcodes
        }
    }


def main():
    # Ensure log directory exists
    os.makedirs(LOG_DIR, exist_ok=True)

    fail_counter = 0

    while running:
        conn, ch = connect_rabbit()
        if not conn:
            break

        rtl_proc, multi_proc = start_decoder()

        log("Decoder running. Waiting for messages...")

        try:
            while running:
                line = multi_proc.stdout.readline()

                if not line:
                    # No data — FLEX is bursty; wait and continue
                    time.sleep(0.1)
                    continue

                line = line.strip()
                if not line or line.startswith("Enabled demodulators:"):
                    continue

                msg = parse_flex_line(line)
                msg_json = json.dumps(msg)

                log(f"RX {msg_json}")

                try:
                    ch.basic_publish(
                        exchange='',
                        routing_key=RABBITMQ_QUEUE,
                        body=msg_json.encode("utf8"),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                except Exception as e:
                    log(f"Publish failed: {e}")
                    break  # reconnect

        except Exception as e:
            log(f"Decoder loop error: {e}")
        finally:
            fail_counter += 1

            if rtl_proc.poll() is None:
                rtl_proc.kill()

            if multi_proc.poll() is None:
                multi_proc.kill()

            if conn.is_open:
                conn.close()

            # Backoff if crashing repeatedly
            sleep_time = min(30, fail_counter * 3)
            log(f"Restarting decoder in {sleep_time} seconds...")
            time.sleep(sleep_time)

    log("Service stopped.")


if __name__ == "__main__":
    main()
