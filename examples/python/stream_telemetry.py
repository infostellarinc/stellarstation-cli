# Copyright 2026 Infostellar, Inc.
#
# Receives telemetry live while a pass executes. Start it before the pass
# begins and leave it running; payloads are written to one file per channel
# and framing under --dest as they arrive, and a running total is printed.
# Press Ctrl-C to stop.
#
# Live streaming carries the low rate channels. High rate channels are served
# from storage instead: retrieve them with download_telemetry.py, which also
# retrieves whatever a live session missed, for example messages sent before
# it connected.
#
# This example is a passive consumer: it does not publish acknowledgments and
# does not recover dropped connections. The stellar CLI implements the
# complete protocol.
#
# Usage:
#   python stream_telemetry.py --pass-id <id> [--dest ./telemetry]

import argparse
import os
import time

import streaming
import toolkit


def main():
    parser = argparse.ArgumentParser(description="Receive telemetry live while a pass executes.")
    parser.add_argument("--pass-id", required=True, help="Pass ID, from list_passes.py")
    parser.add_argument("--dest", default="./telemetry", help="Directory to write into (default ./telemetry)")
    args = parser.parse_args()

    grant = streaming.authorize(toolkit.Client(), args.pass_id, downlink=True)
    # Listen only where telemetry can arrive: not on uplink channels, and not
    # on high rate channels, which are served from storage.
    skip = {c["channelId"] for c in grant.get("channels") or []
            if c.get("rateClass") == "high_rate" or c.get("direction") == "uplink"}
    topics = [s["mqttTopic"] for s in grant["streams"].get("lowRate") or []
              if streaming.channel_of_topic(s["mqttTopic"]) not in skip]
    if not topics:
        toolkit.fail("Every downlink channel of this pass is high rate, so nothing streams live. "
                     "Retrieve the telemetry with download_telemetry.py instead.")

    os.makedirs(args.dest, exist_ok=True)
    totals = {}
    last_report = 0.0

    def on_message(client, userdata, message):
        payloads = streaming.unwrap_envelope(message.payload)
        if not payloads:
            return
        # Topics end in <channel>/downlink/<framing>.
        parts = message.topic.split("/")
        channel, framing = parts[-3], parts[-1]
        path = os.path.join(args.dest, f"{channel}-{framing}.bin")
        with open(path, "ab") as f:
            for payload in payloads:
                f.write(payload)
                totals[path] = totals.get(path, 0) + len(payload)

        nonlocal last_report
        if time.monotonic() - last_report >= 1:
            last_report = time.monotonic()
            print("Received: " + "  ".join(
                f"{os.path.basename(path)} {count} bytes" for path, count in sorted(totals.items())),
                flush=True)

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            toolkit.fail(f"The message broker refused the connection: {reason_code}")
        # Subscribing here rather than once at startup means the
        # subscriptions survive an automatic reconnection.
        for topic in topics:
            client.subscribe(topic, qos=streaming.MQTT_QOS)
        plural = "s" if len(topics) != 1 else ""
        print(f"Connected. Listening on {len(topics)} channel{plural}. Press Ctrl-C to stop.", flush=True)

    client = streaming.connect(grant)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()

    if totals:
        print("Received telemetry, one file per channel and framing:")
        for path, count in sorted(totals.items()):
            print(f"  {path}  {count} bytes")
    else:
        print("No telemetry arrived. Check the pass is executing, with list_passes.py.")


if __name__ == "__main__":
    main()
