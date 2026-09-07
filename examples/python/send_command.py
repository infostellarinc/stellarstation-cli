# Copyright 2026 Infostellar, Inc.
#
# Sends one command to your satellite during a pass and waits for the ground
# station to acknowledge it.
#
# Commands are only transmitted while the pass is booked, so run this between
# the booking start and stop times. One client at a time holds commanding
# authority for a channel; if another session already holds it, the request
# for access is refused.
#
# Usage:
#   python send_command.py --pass-id <id> --command 0A1B2C3D [--channel-id <id>]

import argparse
import queue
import threading

import streaming
import toolkit

CONNECT_TIMEOUT_SECONDS = 15
ACK_TIMEOUT_SECONDS = 30


def select_uplink(uplinks, channel_id):
    """Returns the uplink topics for the requested channel.

    A pass can have several channels that accept commands; each has its own
    publish topic and acknowledgment topic.
    """
    if channel_id:
        for u in uplinks:
            if streaming.channel_of_topic(u["publishTopic"]) == channel_id:
                return u
        toolkit.fail(f"Channel {channel_id} does not accept commands on this pass.")
    if len(uplinks) > 1:
        channels = ", ".join(streaming.channel_of_topic(u["publishTopic"]) for u in uplinks)
        toolkit.fail(f"This pass has several commanding channels: {channels}. "
                     "Name one with --channel-id.")
    return uplinks[0]


def main():
    parser = argparse.ArgumentParser(description="Send one command to a satellite during a pass.")
    parser.add_argument("--pass-id", required=True, help="Pass ID, from list_passes.py")
    parser.add_argument("--command", required=True, help="The command to transmit, as hexadecimal")
    parser.add_argument("--channel-id", help="Channel to command, when the pass has more than one")
    parser.add_argument("--override-commanding-lock", action="store_true",
                        help="Take commanding authority from the client that currently holds it")
    args = parser.parse_args()

    try:
        command = bytes.fromhex(args.command)
    except ValueError:
        toolkit.fail("--command must be hexadecimal, for example 0A1B2C3D.")

    api = toolkit.Client()
    grant = streaming.authorize(api, args.pass_id, uplink=True,
                                override_lock=args.override_commanding_lock)
    try:
        run(api, grant, args, command)
    finally:
        # The grant holds this pass's commanding authority; release it so the
        # next commanding client does not have to override it.
        streaming.close_stream(api, grant, args.pass_id)


def run(api, grant, args, command):
    uplinks = grant["streams"].get("uplink") or []
    if not uplinks:
        toolkit.fail("This pass has no channel that accepts commands.")
    uplink = select_uplink(uplinks, args.channel_id)

    # Acknowledgments for our commands arrive on the acknowledgment topic.
    # The command is only published once that subscription is confirmed, so
    # the acknowledgment cannot slip past before we listen.
    acks = queue.Queue()
    ready = threading.Event()

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            toolkit.fail(f"The message broker refused the connection: {reason_code}")
        client.subscribe(uplink["ackTopic"], qos=streaming.MQTT_QOS)

    def on_subscribe(client, userdata, mid, reason_codes, properties):
        ready.set()

    def on_message(client, userdata, message):
        acks.put(streaming.decode_ack(message.payload))

    client = streaming.connect(grant)
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.loop_start()
    if not ready.wait(CONNECT_TIMEOUT_SECONDS):
        toolkit.fail("Could not connect to the message broker. Check that your "
                     "network allows outbound TLS on port 8883.")

    message_id, encoded = streaming.encode_command_message(
        grant["streamId"], args.pass_id, index=1, commands=[command])
    client.publish(uplink["publishTopic"], encoded, qos=streaming.MQTT_QOS).wait_for_publish()
    print(f"Command sent ({len(command)} bytes). Waiting for the acknowledgment...")

    try:
        while True:
            ack = acks.get(timeout=ACK_TIMEOUT_SECONDS)
            if ack["acked_message_id"] == message_id:
                break
    except queue.Empty:
        toolkit.fail(f"No acknowledgment within {ACK_TIMEOUT_SECONDS} seconds. "
                     "Check the pass is inside its booking window, with list_passes.py.")
    finally:
        client.loop_stop()
        client.disconnect()

    if ack["status"] == "ACK":
        print("The ground station acknowledged the command.")
    else:
        reason = f": {ack['reason']}" if ack["reason"] else ""
        toolkit.fail(f"The ground station rejected the command{reason}")


if __name__ == "__main__":
    main()
