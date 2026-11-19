# Copyright 2026 Infostellar, Inc.
#
# Checks the command encoding, the acknowledgment and status decoding, and the
# acknowledgment and status waits of send_command.py, without connecting to anything.
#
# Usage:
#   python -m unittest test_commanding.py

import queue
import unittest

import send_command
import streaming
from streaming import _field_bytes, _field_varint


def timestamp(seconds):
    return _field_varint(streaming.TIMESTAMP_SECONDS, seconds)


def status(**values):
    """Encodes a CommandStatus from field values keyed by constant suffix."""
    numbers = {
        "acked_message_id": streaming.STATUS_ACKED_MESSAGE_ID, "stream_id": streaming.STATUS_STREAM_ID,
        "index": streaming.STATUS_INDEX, "frame_index": streaming.STATUS_FRAME_INDEX,
        "frame_count": streaming.STATUS_FRAME_COUNT, "stage": streaming.STATUS_STAGE, "outcome": streaming.STATUS_OUTCOME,
        "final": streaming.STATUS_FINAL, "message_final": streaming.STATUS_MESSAGE_FINAL,
        "radiated_at": streaming.STATUS_RADIATED_AT, "failure": streaming.STATUS_FAILURE,
        "provider_code": streaming.STATUS_PROVIDER_CODE, "reason": streaming.STATUS_REASON,
        "config_fields": streaming.STATUS_CONFIG_FIELDS, "message_outcome": streaming.STATUS_MESSAGE_OUTCOME,
    }
    out = b""
    for key, value in values.items():
        if isinstance(value, list):
            out += b"".join(_field_bytes(numbers[key], v) for v in value)
        elif isinstance(value, (str, bytes)):
            out += _field_bytes(numbers[key], value)
        else:
            out += _field_varint(numbers[key], int(value))
    return out


def batch(statuses=(), skipped=()):
    return (b"".join(_field_bytes(streaming.STATUS_BATCH_STATUSES, s) for s in statuses)
            + b"".join(_field_bytes(streaming.STATUS_BATCH_SKIPPED, s) for s in skipped))


def skipped_range(stream_id, first, last):
    return (_field_bytes(streaming.SKIPPED_STREAM_ID, stream_id)
            + _field_varint(streaming.SKIPPED_FIRST_INDEX, first)
            + _field_varint(streaming.SKIPPED_LAST_INDEX, last))


def ack(acked_message_id, status_value=0, reason="", reason_code="", expected_final_stage=0):
    return (_field_bytes(streaming.ACK_ACKED_MESSAGE_ID, acked_message_id)
            + _field_varint(streaming.ACK_STATUS, status_value)
            + (_field_bytes(streaming.ACK_REASON, reason) if reason else b"")
            + _field_varint(streaming.ACK_INDEX, 1)
            + _field_varint(streaming.ACK_EXPECTED_FINAL_STAGE, expected_final_stage)
            + (_field_bytes(streaming.ACK_REASON_CODE, reason_code) if reason_code else b""))


class EncodeTest(unittest.TestCase):
    def test_commands_are_carried_once(self):
        message_id, encoded = streaming.encode_command_message("stream-1", "pass-1", 3, [b"\x0a\x1b"])
        numbers = [number for number, _, _ in streaming.fields(encoded)]
        self.assertEqual(numbers, [1, 2, 3, 4, 7])
        self.assertNotIn(5, numbers, "the retired send_commands_message field must not be written")
        decoded = {number: value for number, _, value in streaming.fields(encoded)}
        self.assertEqual(decoded[streaming.TO_STARPASS_COMMAND], b"\x0a\x1b")
        self.assertEqual(decoded[streaming.TO_STARPASS_INDEX], 3)
        self.assertEqual(decoded[streaming.TO_STARPASS_MESSAGE_ID], message_id.encode())


class DecodeAckTest(unittest.TestCase):
    def test_new_fields(self):
        got = streaming.decode_ack(ack("m-1", 1, "arrived after later commands were sent", "arrived_late", 2))
        self.assertEqual(got, {
            "acked_message_id": "m-1", "status": "NACK", "reason": "arrived after later commands were sent",
            "index": 1, "expected_final_stage": "RADIATED", "reason_code": "arrived_late",
        })

    def test_defaults_and_unknown_values(self):
        got = streaming.decode_ack(_field_varint(streaming.ACK_STATUS, 7)
                                   + _field_varint(streaming.ACK_EXPECTED_FINAL_STAGE, 9))
        self.assertEqual(got["status"], "7")
        self.assertEqual(got["expected_final_stage"], "9")
        self.assertEqual(streaming.decode_ack(b"")["expected_final_stage"], "")

    def test_wrong_wire_type_is_skipped(self):
        got = streaming.decode_ack(_field_varint(streaming.ACK_ACKED_MESSAGE_ID, 5))
        self.assertEqual(got["acked_message_id"], "")


class DecodeStatusBatchTest(unittest.TestCase):
    def test_statuses_and_skipped(self):
        field = (_field_bytes(streaming.CONFIG_FIELD_FIELD, "transmitter.bitrate")
                 + _field_varint(streaming.CONFIG_FIELD_OUTCOME, 4)
                 + _field_varint(streaming.CONFIG_FIELD_FAILURE, 8))
        got = streaming.decode_status_batch(batch(
            statuses=[status(acked_message_id="m-1", index=2, frame_index=1, frame_count=3, stage=2, outcome=2, final=True,
                             message_final=True, message_outcome=2, radiated_at=timestamp(1790000000),
                             provider_code="cortex-tcu:0", config_fields=[field])],
            skipped=[skipped_range("stream-1", 3, 5)],
        ))
        s = got["statuses"][0]
        self.assertEqual(s["acked_message_id"], "m-1")
        self.assertEqual(s["index"], 2)
        self.assertEqual((s["frame_index"], s["frame_count"]), (1, 3))
        self.assertEqual((s["stage"], s["outcome"], s["message_outcome"]), ("RADIATED", "SUCCEEDED", "SUCCEEDED"))
        self.assertTrue(s["final"])
        self.assertTrue(s["message_final"])
        self.assertEqual(s["radiated_at"], "2026-09-21T14:13:20Z")
        self.assertEqual(s["provider_code"], "cortex-tcu:0")
        self.assertEqual(s["config_fields"], [{
            "field": "transmitter.bitrate", "outcome": "FAILED", "failure": "OUT_OF_RANGE",
            "provider_code": "", "reason": "",
        }])
        self.assertEqual(got["skipped"], [{
            "stream_id": "stream-1", "channel_id": "", "message_type": "", "first_index": 3, "last_index": 5,
        }])

    def test_unknown_values_stay_readable(self):
        got = streaming.decode_status_batch(batch(statuses=[status(stage=8, outcome=9, failure=42)]))
        s = got["statuses"][0]
        self.assertEqual((s["stage"], s["outcome"], s["failure"]), ("8", "9", "42"))


class WaitForAckTest(unittest.TestCase):
    def test_times_out_without_an_acknowledgment(self):
        got, statuses = send_command.wait_for_ack(queue.Queue(), "m-1", timeout=0.05)
        self.assertIsNone(got)
        self.assertEqual(statuses, [])

    def test_returns_the_matching_ack(self):
        replies = queue.Queue()
        early = streaming.decode_status_batch(batch(statuses=[status(acked_message_id="m-1")]))
        replies.put(("ack", streaming.decode_ack(ack("someone-else"))))
        replies.put(("status", early))
        replies.put(("ack", streaming.decode_ack(ack("m-1", expected_final_stage=2))))
        got, statuses = send_command.wait_for_ack(replies, "m-1", timeout=1)
        self.assertEqual(got["status"], "ACK")
        self.assertEqual(got["expected_final_stage"], "RADIATED")
        self.assertEqual(statuses, [early])

    def test_a_refusal_is_returned_as_it_is(self):
        replies = queue.Queue()
        replies.put(("ack", streaming.decode_ack(ack("m-1", 1, reason_code="hold_buffer_full"))))
        got, _ = send_command.wait_for_ack(replies, "m-1", timeout=1)
        self.assertEqual((got["status"], got["reason_code"]), ("NACK", "hold_buffer_full"))


class WaitForFinalTest(unittest.TestCase):
    def test_final_status_already_received(self):
        earlier = [streaming.decode_status_batch(batch(statuses=[status(acked_message_id="m-1", message_final=True,
                                                                       message_outcome=2)]))]
        got = send_command.wait_for_final(queue.Queue(), earlier, "m-1", "stream-1", 1, timeout=0.01)
        self.assertEqual(got["message_outcome"], "SUCCEEDED")

    def test_skipped_range(self):
        replies = queue.Queue()
        replies.put(("status", streaming.decode_status_batch(batch(statuses=[status(acked_message_id="m-1")]))))
        replies.put(("status", streaming.decode_status_batch(batch(skipped=[skipped_range("stream-2", 1, 1)]))))
        replies.put(("status", streaming.decode_status_batch(batch(skipped=[skipped_range("stream-1", 1, 3)]))))
        self.assertEqual(send_command.wait_for_final(replies, [], "m-1", "stream-1", 1, timeout=1),
                         {"skipped": True})

    def test_timeout(self):
        self.assertIsNone(send_command.wait_for_final(queue.Queue(), [], "m-1", "stream-1", 1, timeout=0.01))


class DescribeFinalTest(unittest.TestCase):
    def final(self, **values):
        return streaming.decode_status_batch(batch(statuses=[status(message_final=True, **values)]))["statuses"][0]

    def test_outcomes(self):
        cases = [
            (self.final(stage=2, message_outcome=2, radiated_at=timestamp(1790000000)),
             0, "The command was radiated at 2026-09-21T14:13:20Z."),
            (self.final(stage=1, outcome=2), 0, "The command was passed to the ground station equipment."),
            (self.final(message_outcome=4, failure=6, provider_code="cortex-tcu:12"), send_command.EXIT_FAILED,
             "The ground station did not transmit the command: deadline exceeded (cortex-tcu:12)."),
            (self.final(message_outcome=3, failure=1), send_command.EXIT_FAILED,
             "The ground station transmitted only part of the command: plan ended. "
             "Resend only the frames whose status failed."),
            (self.final(message_outcome=5, reason="connection lost"), send_command.EXIT_UNKNOWN,
             "The outcome of the command is unknown: connection lost. " + send_command.CHECK_TELEMETRY),
            (self.final(message_outcome=9), send_command.EXIT_UNKNOWN,
             "The ground station reported outcome 9. " + send_command.CHECK_TELEMETRY),
            ({"skipped": True}, send_command.EXIT_FAILED,
             "The command was not received by the ground station in time and was not transmitted."),
        ]
        for final, code, text in cases:
            with self.subTest(text=text):
                self.assertEqual(send_command.describe_final(final), (code, text))

    def test_no_final_status(self):
        code, text = send_command.describe_final(None)
        self.assertEqual(code, send_command.EXIT_UNKNOWN)
        self.assertIn(send_command.CHECK_TELEMETRY, text)


if __name__ == "__main__":
    unittest.main()
