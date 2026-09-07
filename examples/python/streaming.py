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
ACK_STATUS_NAMES = {0: "ACK", 1: "NACK"}


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


def decode_ack(message):
    """Decodes an Ack message into its acked message ID, status, and reason."""
    ack = {"acked_message_id": "", "status": "ACK", "reason": ""}
    for number, wire_type, value in fields(message):
        if number == ACK_ACKED_MESSAGE_ID and wire_type == 2:
            ack["acked_message_id"] = value.decode()
        elif number == ACK_STATUS and wire_type == 0:
            ack["status"] = ACK_STATUS_NAMES.get(value, str(value))
        elif number == ACK_REASON and wire_type == 2:
            ack["reason"] = value.decode()
    return ack


# ---- Protobuf wire format, writing --------------------------------------------

# ToStarPassMessage, the envelope on the command topics.
TO_STARPASS_STREAM_ID = 1
TO_STARPASS_INDEX = 2
TO_STARPASS_COMMAND = 3
TO_STARPASS_PASS_ID = 4
TO_STARPASS_SEND_COMMANDS = 5
TO_STARPASS_MESSAGE_ID = 7

# SendCommandsMessage
SEND_COMMANDS_STREAM_ID = 1
SEND_COMMANDS_PASS_ID = 2
SEND_COMMANDS_INDEX = 3
SEND_COMMANDS_COMMAND = 4


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
    acknowledgment references this message.
    """
    message_id = str(uuid.uuid4())
    send_commands = (
        _field_bytes(SEND_COMMANDS_STREAM_ID, stream_id)
        + _field_bytes(SEND_COMMANDS_PASS_ID, pass_id)
        + _field_varint(SEND_COMMANDS_INDEX, index)
        + b"".join(_field_bytes(SEND_COMMANDS_COMMAND, c) for c in commands)
    )
    message = (
        _field_bytes(TO_STARPASS_STREAM_ID, stream_id)
        + _field_varint(TO_STARPASS_INDEX, index)
        + b"".join(_field_bytes(TO_STARPASS_COMMAND, c) for c in commands)
        + _field_bytes(TO_STARPASS_PASS_ID, pass_id)
        + _field_bytes(TO_STARPASS_SEND_COMMANDS, send_commands)
        + _field_bytes(TO_STARPASS_MESSAGE_ID, message_id)
    )
    return message_id, message
