# Copyright 2026 Infostellar, Inc.
#
# Uploads a TLE for a satellite. The uploaded elements become the active
# orbit data used for visibility prediction and antenna tracking.
#
# Uploads are rejected while the satellite's orbit data source is AUTOMATIC;
# get_orbit_data.py shows the current source.
#
# Usage:
#   python add_orbit_data.py --satellite-id <id> --tle-file ./mysat.tle

import argparse

import toolkit


def read_tle(path):
    """Reads a TLE file: two element lines, optionally preceded by a name line."""
    with open(path) as f:
        lines = [line.strip() for line in f if line.strip()]
    if len(lines) == 3:
        lines = lines[1:]
    if len(lines) != 2 or not lines[0].startswith("1 ") or not lines[1].startswith("2 "):
        toolkit.fail(f"{path} does not look like a TLE. Expected two element lines "
                     "starting with '1 ' and '2 ', optionally preceded by a name line.")
    return lines


def main():
    parser = argparse.ArgumentParser(description="Upload a TLE for a satellite.")
    parser.add_argument("--satellite-id", required=True, help="Satellite ID, from list_satellites.py")
    parser.add_argument("--tle-file", required=True, help="Path to a file containing the TLE")
    args = parser.parse_args()

    line1, line2 = read_tle(args.tle_file)

    client = toolkit.Client()
    result = client.post(f"/v1/satellites/{args.satellite_id}/orbit-data", {
        "orbital_data_type": "TLE",
        "orbital_data": {"line1": line1, "line2": line2},
        "source": "MANUAL",
        "epoch": toolkit.rfc3339(toolkit.utc_now()),
    })

    print("Orbit data uploaded and now active for scheduling.")
    print(f"  Record ID: {result.get('id', '')}")
    print(f"  Epoch:     {result.get('epoch', '')}")


if __name__ == "__main__":
    main()
