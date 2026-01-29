#!/usr/bin/env python3
import subprocess
import pika
import sys
import time
import json
import re

# -----------------------------
# RabbitMQ connection settings
# -----------------------------
RABBITMQ_HOST = "vps.caelyn.nl"
RABBITMQ_USER = "p2000"
RABBITMQ_PASS = "Pi2000"
RABBITMQ_QUEUE = "p2000"

# -----------------------------
# RTL-SDR / multimon-ng settings
# -----------------------------
FREQUENCY = "169.65M"


def start_pipeline():
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

    rtl_proc = subprocess.Popen(
        rtl_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )

    multi_proc = subprocess.Popen(
        multi_cmd,
        stdin=rtl_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True
    )

    return multi_proc


def connect_rabbit():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    credentials=credentials
                )
            )
            channel = connection.channel()
            channel.queue_declare(
                queue=RABBITMQ_QUEUE,
                durable=True,
                arguments={'x-message-ttl': 300000}  # 5 minutes
            )
            print("[+] Connected to RabbitMQ")
            return connection, channel
        except Exception as e:
            print(f"[-] RabbitMQ unavailable ({e}), retrying...")
            time.sleep(5)


def parse_p2000_line(line):
    parts = line.split('|')
    
    if len(parts) < 7:
        return {
            "raw": line,
            "time": None,
            "message": line,
            "prio": None,
            "grip": None,
            "capcode": []
        }

    # Message body: fields 5+
    message_text = '|'.join(parts[5:]).strip()

    # Capcodes: field 4, split by space
    capcode_field = parts[4].strip()
    capcodes = capcode_field.split() if capcode_field else []

    # Parse priority
    prio_match = re.search(r'\b(A[1-2]|B1|P[1-3]|PRIO\s?[1-5])\b', message_text, re.IGNORECASE)
    prio = prio_match.group(0) if prio_match else None

    # Parse GRIP
    grip_match = re.search(r'\bGRIP\s?([1-4])\b', message_text, re.IGNORECASE)
    grip = f"GRIP {grip_match.group(1)}" if grip_match else None

    return {
        "raw": line,
        "time": parts[1],
        "message": message_text,
        "prio": prio,
        "grip": grip,
        "capcode": capcodes
    }


def main():
    connection, channel = connect_rabbit()
    decoder = start_pipeline()

    print("[+] Listening for P2000 messages...")

    for line in decoder.stdout:
        line = line.strip()
        if not line or line.startswith("Enabled demodulators:"):
            continue

        msg_data = parse_p2000_line(line)
        msg_json = json.dumps(msg_data)

        print("[RX]", msg_json)

        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=msg_json.encode("utf8")
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping...")
        sys.exit(0)
