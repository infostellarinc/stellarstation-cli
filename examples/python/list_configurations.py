# Copyright 2026 Infostellar, Inc.
#
# Lists the execution configurations available for a satellite at a ground
# station. An execution configuration is a named radio setup (frequencies,
# modulation, bitrates); reserving a pass requires choosing one.
#
# Usage:
#   python list_configurations.py --satellite-id <id> --ground-station-id <id>

import argparse

import toolkit


def list_configurations(client, satellite_id, ground_station_id):
    response = client.get(
        f"/v1/satellites/{satellite_id}/configurations",
        params={"groundStationId": ground_station_id},
    )
    return response.get("configurations") or []


def describe_radio(radio):
    """Summarizes one direction of a configuration, for example 2.2 GHz BPSK 9600 bps."""
    if not radio:
        return "-"
    parts = []
    if radio.get("centerFrequencyHz"):
        parts.append(f"{radio['centerFrequencyHz'] / 1e6:.3f} MHz")
    if radio.get("modulation"):
        parts.append(radio["modulation"])
    if radio.get("bitrate"):
        parts.append(f"{radio['bitrate']:.0f} bps")
    if radio.get("framing"):
        parts.append(radio["framing"])
    return " ".join(parts) or "-"


def main():
    parser = argparse.ArgumentParser(description="List execution configurations for a satellite and ground station.")
    parser.add_argument("--satellite-id", required=True, help="Satellite ID, from list_satellites.py")
    parser.add_argument("--ground-station-id", required=True, help="Ground station ID, from list_visibilities.py")
    args = parser.parse_args()

    client = toolkit.Client()
    configurations = list_configurations(client, args.satellite_id, args.ground_station_id)

    if not configurations:
        print("No execution configurations found for this satellite and ground station.")
        return

    toolkit.print_table(
        ["ID", "Name", "Downlink", "Uplink"],
        [
            [c["id"], c["displayName"], describe_radio(c.get("downlink")), describe_radio(c.get("uplink"))]
            for c in configurations
        ],
    )


if __name__ == "__main__":
    main()
