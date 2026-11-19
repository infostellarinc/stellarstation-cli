# Copyright 2026 Infostellar, Inc.
#
# Sends one command to your satellite during a pass, waits for the ground
# station to acknowledge it, then waits for its final delivery status, for
# example the time it was radiated.
#
# Commands are only transmitted while the pass is booked, so run this between
# the booking start and stop times. One client at a time holds commanding
# authority for a channel; if another session already holds it, the request
# for access is refused.
#
# An unacknowledged command is sent again, byte for byte, until it is
# acknowledged. This is safe: the ground station transmits each message ID
# once and answers every copy with the same acknowledgment. Running the script
# again is different: it sends a new command, which is transmitted again.
#
# Exit codes:
#   0  the command was acknowledged and, where the ground station reports it,
#      delivered
#   1  the command was refused, failed, or was not transmitted; or an error
#   3  the outcome is unknown: check spacecraft telemetry before sending the
#      command again
#
# Usage:
#   python send_command.py --pass-id <id> --command 0A1B2C3D [--channel-id <id>]

import argparse
import queue
import threading
import time

import streaming
import toolkit

CONNECT_TIMEOUT_SECONDS = 15
ACK_TIMEOUT_SECONDS = 30
STATUS_TIMEOUT_SECONDS = 60

EXIT_FAILED = 1
EXIT_UNKNOWN = 3

CHECK_TELEMETRY = "Check spacecraft telemetry before sending the command again."


def select_uplink(uplinks, channel_id):
    """Returns the uplink topics for the requested channel.

    A pass can have several channels that accept commands; each has its own
    publish topic, acknowledgment topic and status topic.
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


def wait_for_ack(replies, message_id, timeout=ACK_TIMEOUT_SECONDS):
    """Waits for the acknowledgment of message_id.

    replies yields ("ack", ack) and ("status", batch) tuples. Returns the
    acknowledgment, or None after timeout, and the status batches that
    arrived meanwhile. The command is never sent again by this script: a
    retry during a pass only adds to the congestion that lost the first copy.
    """
    statuses = []
    deadline = time.monotonic() + timeout
    while True:
        now = time.monotonic()
        if now >= deadline:
            return None, statuses
        try:
            kind, reply = replies.get(timeout=deadline - now)
        except queue.Empty:
            continue
        if kind == "status":
            statuses.append(reply)
        elif reply["acked_message_id"] == message_id:
            return reply, statuses


def find_final(batch, message_id, stream_id, index):
    """Returns the final status of the command from a status batch, or None.

    A command the ground station declared missing is reported as
    {"skipped": True}.
    """
    for status in batch["statuses"]:
        if status["acked_message_id"] == message_id and status["message_final"]:
            return status
    for skipped in batch["skipped"]:
        if skipped["stream_id"] in ("", stream_id) and skipped["first_index"] <= index <= skipped["last_index"]:
            return {"skipped": True}
    return None


def wait_for_final(replies, earlier, message_id, stream_id, index, timeout=STATUS_TIMEOUT_SECONDS):
    """Waits for the final status of the command; earlier holds the status
    batches already received. Returns None after timeout."""
    for batch in earlier:
        final = find_final(batch, message_id, stream_id, index)
        if final:
            return final
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            kind, reply = replies.get(timeout=remaining)
        except queue.Empty:
            return None
        if kind == "status":
            final = find_final(reply, message_id, stream_id, index)
            if final:
                return final


def describe_reason(status):
    """Returns the reason a status gives, with the raw equipment code."""
    reason = status["reason"] or status["failure"].replace("_", " ").lower() or "no reason given"
    return f"{reason} ({status['provider_code']})" if status["provider_code"] else reason


def describe_final(final):
    """Returns (exit code, text) for the final status of a command."""
    if final is None:
        return EXIT_UNKNOWN, (f"No final delivery status within {STATUS_TIMEOUT_SECONDS} seconds. "
                              + CHECK_TELEMETRY)
    if final.get("skipped"):
        return EXIT_FAILED, "The command was not received by the ground station in time and was not transmitted."
    outcome = final["message_outcome"] or final["outcome"]
    if outcome == "SUCCEEDED":
        if final["stage"] == "RADIATED":
            when = f" at {final['radiated_at']}" if final["radiated_at"] else ""
            return 0, f"The command was radiated{when}."
        return 0, "The command was passed to the ground station equipment."
    if outcome == "FAILED":
        return EXIT_FAILED, f"The ground station did not transmit the command: {describe_reason(final)}."
    if outcome == "PARTIAL":
        return EXIT_FAILED, (f"The ground station transmitted only part of the command: {describe_reason(final)}. "
                             "Resend only the frames whose status failed.")
    if outcome == "UNKNOWN":
        return EXIT_UNKNOWN, f"The outcome of the command is unknown: {describe_reason(final)}. " + CHECK_TELEMETRY
    return EXIT_UNKNOWN, f"The ground station reported outcome {outcome}. " + CHECK_TELEMETRY


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
        run(grant, args, command)
    finally:
        # The grant holds this pass's commanding authority; release it so the
        # next commanding client does not have to override it.
        streaming.close_stream(api, grant, args.pass_id)


def run(grant, args, command):
    uplinks = grant["streams"].get("uplink") or []
    if not uplinks:
        toolkit.fail("This pass has no channel that accepts commands.")
    uplink = select_uplink(uplinks, args.channel_id)

    # Acknowledgments and delivery statuses arrive on their own topics. The
    # command is only published once those subscriptions are confirmed, so no
    # reply can arrive before the subscriptions are in place.
    topics = [uplink["ackTopic"]] + ([uplink["statusTopic"]] if uplink.get("statusTopic") else [])
    replies = queue.Queue()
    ready = threading.Event()

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            toolkit.fail(f"The message broker refused the connection: {reason_code}")
        client.subscribe([(t, streaming.MQTT_QOS) for t in topics])

    def on_subscribe(client, userdata, mid, reason_codes, properties):
        ready.set()

    def on_message(client, userdata, message):
        if message.topic.endswith("/status"):
            replies.put(("status", streaming.decode_status_batch(message.payload)))
        else:
            replies.put(("ack", streaming.decode_ack(message.payload)))

    client = streaming.connect(grant)
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.loop_start()
    try:
        if not ready.wait(CONNECT_TIMEOUT_SECONDS):
            toolkit.fail("Could not connect to the message broker. Check that your "
                         "network allows outbound TLS on port 8883.")
        exit_code, text = send_and_wait(client, uplink, grant["streamId"], args.pass_id, command, replies)
    finally:
        client.loop_stop()
        client.disconnect()
    if exit_code:
        toolkit.fail(text, exit_code)
    print(text)


def send_and_wait(client, uplink, stream_id, pass_id, command, replies):
    """Sends the command once, waits for its acknowledgment, then for its
    final status. Returns (exit code, text)."""
    index = 1
    message_id, encoded = streaming.encode_command_message(stream_id, pass_id, index=index, commands=[command])

    client.publish(uplink["publishTopic"], encoded, qos=streaming.MQTT_QOS).wait_for_publish()
    print(f"Command sent ({len(command)} bytes). Waiting for the acknowledgment...")

    ack, statuses = wait_for_ack(replies, message_id)
    if ack is None:
        return EXIT_UNKNOWN, (f"No acknowledgment within {ACK_TIMEOUT_SECONDS} seconds. Check the pass is "
                              "inside its booking window, with list_passes.py. " + CHECK_TELEMETRY)
    if ack["status"] != "ACK":
        reason = ack["reason"] or ack["reason_code"]
        text = "The ground station refused the command" + (f": {reason}" if reason else ".")
        if ack["reason_code"] == streaming.REASON_HOLD_BUFFER_FULL:
            text += " It was not recorded there, so it can be sent again unchanged."
        return EXIT_FAILED, text
    print("The ground station acknowledged the command.")

    if not ack["expected_final_stage"] or not uplink.get("statusTopic"):
        return 0, "This ground station does not report delivery status, so only receipt is confirmed."
    print("Waiting for the delivery status...")
    return describe_final(wait_for_final(replies, statuses, message_id, stream_id, index))


if __name__ == "__main__":
    main()
