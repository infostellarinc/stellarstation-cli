# Copyright 2026 Infostellar, Inc.
#
# Shared helpers for the streaming examples: requesting stream access,
# reading and writing the protobuf messages carried on the pass topics, and
# connecting to the message broker.
#
# Only a handful of fields of the streaming protobuf messages are needed
# here, so these helpers read and write the protobuf wire format directly
# rather than requiring generated protobuf code. Ask your StellarStation
# contact for the full protobuf definitions if you are building a client
# that covers more of the protocol.

import ssl
import tempfile
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

# Deliver each message at least once. Duplicates are possible but telemetry
# consumers can de-duplicate by index, and commands carry their own index.
MQTT_QOS = 1


def authorize(client, pass_id, downlink=False, uplink=False, override_lock=False):
    """Requests streaming access for a pass via POST /authorize.

    The response grants temporary storage credentials, a client certificate
    for the message broker, the topics of every requested stream, and
    metadata describing each channel. The grant is valid for one hour.

    Commanding authority is exclusive, so a request with uplink is refused
    while another stream holds it; override_lock takes the authority over.
    """
    return client.post("/authorize", {
        "passId": pass_id,
        "channelIds": [],
        "enableDownlink": downlink,
        "enableUplink": uplink,
        "overrideCommandingLock": override_lock,
        "source": "python-example",
    })


def close_stream(client, grant, pass_id):
    """Releases the grant's stream via POST /stream/close.

    A stream holding commanding authority keeps it until closed or until the
    pass ends, so a commanding client should always close its stream on exit.
    """
    client.post("/stream/close", {
        "passId": pass_id,
        "streamId": grant.get("streamId", ""),
        "source": "python-example",
    })


def high_rate_channels(grant):
    """Returns the IDs of the grant's high rate channels.

    A channel's rate class decides how to consume it: high rate channels are
    served from storage, low rate channels stream over the message broker.
    """
    return {c["channelId"] for c in grant.get("channels") or [] if c.get("rateClass") == "high_rate"}


def channel_of_topic(topic):
    """Reads the channel ID from a topic or storage prefix containing channel/<id>/."""
    parts = topic.split("/")
    return parts[parts.index("channel") + 1]


def connect(grant):
    """Connects to the message broker with the certificate from the grant.

    The connection uses mutual TLS on port 8883; the grant provisions a fresh
    certificate for each session. Set the on_message callback and subscribe
    after this returns, then run the client's network loop.
    """
    context = ssl.create_default_context()
    with tempfile.NamedTemporaryFile("w", suffix=".pem") as cert, \
            tempfile.NamedTemporaryFile("w", suffix=".pem") as key:
        cert.write(grant["iotCertificatePem"])
        cert.flush()
        key.write(grant["iotPrivateKeyPem"])
        key.flush()
        context.load_cert_chain(cert.name, key.name)

    # The broker authorizes client IDs of the form "<clientId>-<suffix>"; the
    # suffix also keeps concurrent sessions from evicting each other.
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"{grant['clientId']}-{uuid.uuid4().hex[:8]}",
    )
    client.tls_set_context(context)
    client.connect(grant["iotCertEndpoint"], 8883)
    return client


# ---- Protobuf wire format, reading -------------------------------------------

# FromStarPassMessage, the envelope on every downlink topic.
FROM_STARPASS_SEND_TELEMETRY = 8

# SendTelemetryMessage
SEND_TELEMETRY_TELEMETRY = 5

# Telemetry
TELEMETRY_DATA = 4

# Ack, published on a command topic with "/ack" appended.
ACK_ACKED_MESSAGE_ID = 1
ACK_STATUS = 2
ACK_REASON = 3
ACK_INDEX = 7
ACK_EXPECTED_FINAL_STAGE = 13
ACK_REASON_CODE = 14
ACK_STATUS_NAMES = {0: "ACK", 1: "NACK"}

# Ack.reason_code of a NACK the ground station does not record: send the
# same message again unchanged.
REASON_HOLD_BUFFER_FULL = "hold_buffer_full"

# CommandStatusBatch, published on a command topic with "/status" appended.
STATUS_BATCH_STATUSES = 1
STATUS_BATCH_SKIPPED = 2

# CommandStatus
STATUS_ACKED_MESSAGE_ID = 1
STATUS_STREAM_ID = 2
STATUS_INDEX = 4
STATUS_CHANNEL_ID = 5
STATUS_MESSAGE_TYPE = 6
STATUS_FRAME_COUNT = 9
STATUS_STAGE = 10
STATUS_OUTCOME = 11
STATUS_FINAL = 12
STATUS_MESSAGE_FINAL = 13
STATUS_RADIATED_AT = 15
STATUS_FAILURE = 16
STATUS_PROVIDER_CODE = 17
STATUS_REASON = 18
STATUS_CONFIG_FIELDS = 19
STATUS_MESSAGE_OUTCOME = 21
STATUS_FRAME_INDEX = 22

# ConfigFieldResult
CONFIG_FIELD_FIELD = 1
CONFIG_FIELD_OUTCOME = 2
CONFIG_FIELD_FAILURE = 3
CONFIG_FIELD_PROVIDER_CODE = 4
CONFIG_FIELD_REASON = 5

# SkippedRange
SKIPPED_STREAM_ID = 1
SKIPPED_CHANNEL_ID = 2
SKIPPED_MESSAGE_TYPE = 3
SKIPPED_FIRST_INDEX = 4
SKIPPED_LAST_INDEX = 5

# google.protobuf.Timestamp
TIMESTAMP_SECONDS = 1
TIMESTAMP_NANOS = 2

# Enum names. The unspecified value reads as "", and a value newer than this
# example reads as its number.
STAGE_NAMES = {0: "", 1: "DISPATCHED", 2: "RADIATED", 3: "APPLIED"}
OUTCOME_NAMES = {0: "", 1: "IN_PROGRESS", 2: "SUCCEEDED", 3: "PARTIAL", 4: "FAILED", 5: "UNKNOWN"}
FAILURE_NAMES = {
    0: "", 1: "PLAN_ENDED", 2: "SUPERSEDED", 3: "INVALID_COMMAND", 4: "ENCODER_LOCKED_OUT",
    5: "REJECTED_BY_DEVICE", 6: "DEADLINE_EXCEEDED", 7: "UNSUPPORTED", 8: "OUT_OF_RANGE",
    9: "NO_DEVICE_ACK", 10: "STATION_ERROR",
}


def _read_varint(data, pos):
    result = 0
    shift = 0
    while True:
        if pos >= len(data) or shift > 63:
            raise ValueError("invalid varint")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


def fields(data):
    """Yields (field_number, wire_type, value) for each field in a message."""
    pos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        number, wire_type = tag >> 3, tag & 7
        if wire_type == 0:  # varint
            value, pos = _read_varint(data, pos)
        elif wire_type in (1, 5):  # fixed 64 bit / fixed 32 bit
            size = 8 if wire_type == 1 else 4
            value, pos = data[pos:pos + size], pos + size
        elif wire_type == 2:  # length delimited
            size, pos = _read_varint(data, pos)
            value, pos = data[pos:pos + size], pos + size
        else:
            raise ValueError(f"unsupported wire type {wire_type}")
        if isinstance(value, (bytes, bytearray)) and pos > len(data):
            raise ValueError("truncated field")
        yield number, wire_type, value


def unwrap_envelope(message):
    """Extracts the telemetry payloads from one FromStarPassMessage envelope."""
    payloads = []
    for number, wire_type, submessage in fields(message):
        if number != FROM_STARPASS_SEND_TELEMETRY or wire_type != 2:
            continue
        for n, w, telemetry in fields(submessage):
            if n != SEND_TELEMETRY_TELEMETRY or w != 2:
                continue
            for n2, w2, data in fields(telemetry):
                if n2 == TELEMETRY_DATA and w2 == 2:
                    payloads.append(data)
    return payloads


def payloads_from_object(data):
    """Returns the telemetry payloads of one stored object.

    Stored telemetry is either a FromStarPassMessage envelope or raw
    telemetry bytes, depending on the producing ground station. Matching the
    stellar CLI, an object that does not parse as an envelope is treated as
    raw bytes.
    """
    try:
        return unwrap_envelope(data)
    except ValueError:
        return [data]


VARINT = 0
LENGTH_DELIMITED = 2


def _decode(message, schema, defaults):
    """Decodes the fields of message listed in schema.

    schema maps a field number to (key, wire type, convert). Fields that are
    not listed, or arrive with another wire type, are skipped. A repeated
    field collects into a list when its default is a list.
    """
    out = {k: (list(v) if isinstance(v, list) else v) for k, v in defaults.items()}
    for number, wire_type, value in fields(message):
        if number not in schema or schema[number][1] != wire_type:
            continue
        key, _, convert = schema[number]
        if isinstance(out[key], list):
            out[key].append(convert(value))
        else:
            out[key] = convert(value)
    return out


def _text(value):
    return value.decode()


def _enum(names):
    """Converts an enum value to its name, or its number when unknown."""
    return lambda value: names.get(value, str(value))


def _string(key):
    return key, LENGTH_DELIMITED, _text


def _uint(key):
    return key, VARINT, int


def _bool(key):
    return key, VARINT, bool


def _enum_field(key, names):
    return key, VARINT, _enum(names)


def _message(key, decode):
    return key, LENGTH_DELIMITED, decode


def _timestamp(message):
    """Formats a google.protobuf.Timestamp as UTC RFC3339."""
    parts = _decode(message, {TIMESTAMP_SECONDS: _uint("seconds"), TIMESTAMP_NANOS: _uint("nanos")},
                    {"seconds": 0, "nanos": 0})
    when = datetime.fromtimestamp(parts["seconds"] + parts["nanos"] / 1e9, timezone.utc)
    return when.strftime("%Y-%m-%dT%H:%M:%SZ")


def decode_ack(message):
    """Decodes an Ack message.

    expected_final_stage is the furthest stage the ground station reports on
    the status topic for this command; "" means it reports no status.
    reason_code is the stable form of reason, set on every NACK.
    """
    return _decode(message, {
        ACK_ACKED_MESSAGE_ID: _string("acked_message_id"),
        ACK_STATUS: _enum_field("status", ACK_STATUS_NAMES),
        ACK_REASON: _string("reason"),
        ACK_INDEX: _uint("index"),
        ACK_EXPECTED_FINAL_STAGE: _enum_field("expected_final_stage", STAGE_NAMES),
        ACK_REASON_CODE: _string("reason_code"),
    }, {"acked_message_id": "", "status": "ACK", "reason": "", "index": 0,
        "expected_final_stage": "", "reason_code": ""})


def _decode_config_field(message):
    return _decode(message, {
        CONFIG_FIELD_FIELD: _string("field"),
        CONFIG_FIELD_OUTCOME: _enum_field("outcome", OUTCOME_NAMES),
        CONFIG_FIELD_FAILURE: _enum_field("failure", FAILURE_NAMES),
        CONFIG_FIELD_PROVIDER_CODE: _string("provider_code"),
        CONFIG_FIELD_REASON: _string("reason"),
    }, {"field": "", "outcome": "", "failure": "", "provider_code": "", "reason": ""})


def _decode_status(message):
    return _decode(message, {
        STATUS_ACKED_MESSAGE_ID: _string("acked_message_id"),
        STATUS_STREAM_ID: _string("stream_id"),
        STATUS_INDEX: _uint("index"),
        STATUS_CHANNEL_ID: _string("channel_id"),
        STATUS_MESSAGE_TYPE: _string("message_type"),
        STATUS_FRAME_INDEX: _uint("frame_index"),
        STATUS_FRAME_COUNT: _uint("frame_count"),
        STATUS_STAGE: _enum_field("stage", STAGE_NAMES),
        STATUS_OUTCOME: _enum_field("outcome", OUTCOME_NAMES),
        STATUS_FINAL: _bool("final"),
        STATUS_MESSAGE_FINAL: _bool("message_final"),
        STATUS_RADIATED_AT: _message("radiated_at", _timestamp),
        STATUS_FAILURE: _enum_field("failure", FAILURE_NAMES),
        STATUS_PROVIDER_CODE: _string("provider_code"),
        STATUS_REASON: _string("reason"),
        STATUS_CONFIG_FIELDS: _message("config_fields", _decode_config_field),
        STATUS_MESSAGE_OUTCOME: _enum_field("message_outcome", OUTCOME_NAMES),
    }, {"acked_message_id": "", "stream_id": "", "index": 0, "channel_id": "", "message_type": "",
        "frame_index": 0, "frame_count": 0, "stage": "", "outcome": "", "final": False, "message_final": False, "radiated_at": "",
        "failure": "", "provider_code": "", "reason": "", "config_fields": [], "message_outcome": ""})


def _decode_skipped(message):
    return _decode(message, {
        SKIPPED_STREAM_ID: _string("stream_id"),
        SKIPPED_CHANNEL_ID: _string("channel_id"),
        SKIPPED_MESSAGE_TYPE: _string("message_type"),
        SKIPPED_FIRST_INDEX: _uint("first_index"),
        SKIPPED_LAST_INDEX: _uint("last_index"),
    }, {"stream_id": "", "channel_id": "", "message_type": "", "first_index": 0, "last_index": 0})


def decode_status_batch(message):
    """Decodes a CommandStatusBatch from a status topic.

    Each status reports how far one frame of a command got (frame_index of
    frame_count): stage DISPATCHED (handed to the ground station equipment),
    RADIATED (transmitted, at radiated_at) or APPLIED (configuration), with an
    outcome. message_final marks the last status for a command, and
    message_outcome is then its overall result: UNKNOWN if any frame may have
    been transmitted, SUCCEEDED or FAILED when every frame did, else PARTIAL.
    skipped lists index ranges the ground station never received in time and
    did not transmit.
    """
    return _decode(message, {
        STATUS_BATCH_STATUSES: _message("statuses", _decode_status),
        STATUS_BATCH_SKIPPED: _message("skipped", _decode_skipped),
    }, {"statuses": [], "skipped": []})


# ---- Protobuf wire format, writing --------------------------------------------

# ToStarPassMessage, the envelope on the command topics.
TO_STARPASS_STREAM_ID = 1
TO_STARPASS_INDEX = 2
TO_STARPASS_COMMAND = 3
TO_STARPASS_PASS_ID = 4
TO_STARPASS_MESSAGE_ID = 7


def _varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _field_varint(number, value):
    return _varint(number << 3) + _varint(value)


def _field_bytes(number, payload):
    if isinstance(payload, str):
        payload = payload.encode()
    return _varint(number << 3 | 2) + _varint(len(payload)) + payload


def encode_command_message(stream_id, pass_id, index, commands):
    """Encodes a ToStarPassMessage carrying satellite commands.

    Returns (message_id, encoded bytes). The message ID is how the resulting
    acknowledgment references this message. Resend these exact bytes if no
    acknowledgment arrives: the ground station transmits a message ID once
    and answers every copy with the same acknowledgment.
    """
    message_id = str(uuid.uuid4())
    message = (
        _field_bytes(TO_STARPASS_STREAM_ID, stream_id)
        + _field_varint(TO_STARPASS_INDEX, index)
        + b"".join(_field_bytes(TO_STARPASS_COMMAND, c) for c in commands)
        + _field_bytes(TO_STARPASS_PASS_ID, pass_id)
        + _field_bytes(TO_STARPASS_MESSAGE_ID, message_id)
    )
    return message_id, message
