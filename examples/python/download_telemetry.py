# Copyright 2026 Infostellar, Inc.
#
# Downloads the telemetry StellarStation received during a pass.
#
# Telemetry is retrieved from cloud storage, so this works while a pass is
# executing and after it has completed, and a completed pass can be
# downloaded as often as needed. For receiving telemetry live instead, see
# stream_telemetry.py.
#
# The flow:
#   1. Request download access for the pass. The grant contains temporary
#      storage credentials, the storage locations, and each channel's rate
#      class.
#   2. Download each downlink channel's objects in order and append the
#      telemetry to one file per channel and framing under --dest.
#
# Depending on the producing ground station, a stored object is either raw
# telemetry bytes or a protobuf envelope wrapping them. Both are handled, so
# the output files contain only payload data.
#
# Usage:
#   python download_telemetry.py --pass-id <id> [--dest ./telemetry]

import argparse
import os
from collections import defaultdict

import boto3

import streaming
import toolkit


def channel_and_framing(key):
    """Reads the channel ID and framing type from a telemetry object key.

    High rate keys look like <pass>/<channel>/<framing>/<index> and low rate
    keys like <env>/pass/<pass>/channel/<channel>/downlink/<framing>/<index>.
    """
    parts = key.split("/")
    channel = parts[parts.index("channel") + 1] if "channel" in parts else parts[-3]
    return channel, parts[-2]


def list_keys(s3, bucket, prefix):
    """Lists the telemetry chunk keys under a prefix, sorted by index.

    A channel's prefix also holds acknowledgment records and other bookkeeping
    objects. The telemetry chunks are the keys ending in a numeric index, and
    that index is the order in which the payloads belong in the output.
    """
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    chunks = [k for k in keys if k.rsplit("/", 1)[-1].isdigit()]
    return sorted(chunks, key=lambda k: int(k.rsplit("/", 1)[-1]))


def download_prefix(s3, bucket, prefix, channels, dest):
    """Downloads one storage location, returning bytes written per output file.

    Only keys belonging to a channel in `channels` are downloaded, since a
    location can also hold copies of channels served elsewhere.
    """
    written = defaultdict(int)
    for key in list_keys(s3, bucket, prefix):
        channel, framing = channel_and_framing(key)
        if channel not in channels:
            continue
        obj = s3.get_object(Bucket=bucket, Key=key)
        # A zero length object marked END closes the stream; it carries no data.
        if obj["Metadata"].get("messagetype") == "END":
            continue
        payloads = streaming.payloads_from_object(obj["Body"].read())
        path = os.path.join(dest, f"{channel}-{framing}.bin")
        # Truncate on the first write of this run, then append in index order.
        with open(path, "ab" if path in written else "wb") as f:
            for payload in payloads:
                f.write(payload)
                written[path] += len(payload)
    return written


def main():
    parser = argparse.ArgumentParser(description="Download the telemetry received during a pass.")
    parser.add_argument("--pass-id", required=True, help="Pass ID, from list_passes.py")
    parser.add_argument("--dest", default="./telemetry", help="Directory to write into (default ./telemetry)")
    args = parser.parse_args()

    grant = streaming.authorize(toolkit.Client(), args.pass_id, downlink=True)
    streams = grant.get("streams") or {}
    high_rate = streaming.high_rate_channels(grant)

    # Each downlink channel is downloaded from the location matching its rate
    # class: high rate channels from the shared high rate location, low rate
    # channels from their own location.
    locations = []
    if streams.get("highRate") and high_rate:
        locations.append((streams["highRate"]["s3Prefix"], high_rate))
    for s in streams.get("lowRate") or []:
        channel = streaming.channel_of_topic(s["s3Prefix"])
        if channel not in high_rate:
            locations.append((s["s3Prefix"], {channel}))
    if not locations:
        toolkit.fail("The pass has no downlink channels to download.")

    s3 = boto3.client(
        "s3",
        region_name=grant["s3Region"],
        aws_access_key_id=grant["accessKeyId"],
        aws_secret_access_key=grant["secretAccessKey"],
        aws_session_token=grant["sessionToken"],
    )

    os.makedirs(args.dest, exist_ok=True)
    written = defaultdict(int)
    for number, (prefix, channels) in enumerate(locations, start=1):
        print(f"Downloading storage location {number} of {len(locations)}...", flush=True)
        for path, count in download_prefix(s3, grant["s3Bucket"], prefix, channels, args.dest).items():
            written[path] += count

    if not written:
        print("No telemetry found for this pass. If it has not executed yet, "
              "run this again after it completes.")
        return
    print("Downloaded telemetry, one file per channel and framing:")
    for path, count in sorted(written.items()):
        print(f"  {path}  {count} bytes")


if __name__ == "__main__":
    main()
