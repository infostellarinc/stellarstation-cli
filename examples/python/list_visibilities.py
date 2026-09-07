# Copyright 2026 Infostellar, Inc.
#
# Lists the upcoming visibilities for a satellite: the windows in which a
# ground station can see it. Each row is an opportunity to reserve a pass.
#
# Usage:
#   python list_visibilities.py --satellite-id <id> [--days 7]

import argparse
from datetime import timedelta

import toolkit


def list_visibilities(client, satellite_id, days):
    start = toolkit.utc_now()
    return client.get(
        "/v1/visibilities",
        params={
            "satellite_ids": satellite_id,
            "start": toolkit.rfc3339(start),
            "stop": toolkit.rfc3339(start + timedelta(days=days)),
        },
    ) or []


def main():
    parser = argparse.ArgumentParser(description="List upcoming visibilities for a satellite.")
    parser.add_argument("--satellite-id", required=True, help="Satellite ID, from list_satellites.py")
    parser.add_argument("--days", type=int, default=7, help="How many days ahead to search (default 7)")
    args = parser.parse_args()

    client = toolkit.Client()
    visibilities = list_visibilities(client, args.satellite_id, args.days)

    if not visibilities:
        print(f"No visibilities in the next {args.days} days.")
        return

    rows = []
    for v in visibilities:
        window = v["visibility"]
        aos = toolkit.parse_rfc3339(window["start"])
        los = toolkit.parse_rfc3339(window["stop"])
        rows.append([
            v["ground_station"].get("name", ""),
            v["ground_station"]["id"],
            toolkit.rfc3339(aos),
            toolkit.rfc3339(los),
            f"{(los - aos).total_seconds() / 60:.1f}",
            f"{v.get('max_elevation_degrees', 0):.1f}",
        ])
    toolkit.print_table(
        ["Ground station", "GS ID", "AOS (UTC)", "LOS (UTC)", "Minutes", "Max elev"],
        rows,
    )


if __name__ == "__main__":
    main()
