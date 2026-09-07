# Copyright 2026 Infostellar, Inc.
#
# Walks the whole reservation workflow: finds the next visibility for your
# satellite, selects an execution configuration, reserves the pass, reads it
# back, and finally cancels it so the example cleans up after itself.
#
# Reserving a pass books real antenna time. The reservation only lasts until
# the cancel step at the end of this script.
#
# Usage:
#   python reserve_and_cancel_pass.py --satellite-id <id> [--days 7]

import argparse

import toolkit
from list_configurations import list_configurations
from list_visibilities import list_visibilities


def main():
    parser = argparse.ArgumentParser(description="Reserve the next available pass, then cancel it.")
    parser.add_argument("--satellite-id", required=True, help="Satellite ID, from list_satellites.py")
    parser.add_argument("--days", type=int, default=7, help="How many days ahead to search (default 7)")
    args = parser.parse_args()

    client = toolkit.Client()

    # Step 1: find a visibility. Its ground station and window drive the rest.
    visibilities = list_visibilities(client, args.satellite_id, args.days)
    if not visibilities:
        toolkit.fail(f"No visibilities in the next {args.days} days, so there is nothing to reserve.")
    visibility = visibilities[0]
    ground_station = visibility["ground_station"]
    # Availability is the part of the window not blocked by existing
    # reservations. Book inside it rather than the full visibility.
    window = (visibility.get("availability") or [visibility["visibility"]])[0]
    print(f"Next visibility: {ground_station.get('name', ground_station['id'])}, "
          f"{window['start']} to {window['stop']}")

    # Step 2: select an execution configuration for this satellite and ground
    # station. This example takes the first one; check with your StellarStation
    # contact if you are unsure which your spacecraft expects.
    configurations = list_configurations(client, args.satellite_id, ground_station["id"])
    if not configurations:
        toolkit.fail("No execution configuration is available for this satellite and ground station.")
    configuration = configurations[0]
    print(f"Using execution configuration: {configuration['displayName']} ({configuration['id']})")

    # Step 3: reserve the pass, booking the window from step 1.
    booking = {"start": window["start"], "stop": window["stop"]}
    reserved = client.post("/v1/passes", {
        "satellite_id": args.satellite_id,
        "ground_station_id": ground_station["id"],
        "execution_config_id": configuration["id"],
        "booking": booking,
        "scheduled": booking,
    })
    print(f"Reserved pass {reserved['id']}")

    # Step 4: read the pass back. GET /v1/passes/{passId} is how you check a
    # reservation later; the execution status starts as PENDING and moves to
    # EXECUTING and then COMPLETE as the pass runs.
    fetched = client.get(f"/v1/passes/{reserved['id']}")
    print(f"Execution status: {fetched['execution']['status']}")

    # Step 5: cancel the pass so this example leaves no reservation behind.
    canceled = client.delete(f"/v1/passes/{reserved['id']}")
    print(f"Canceled pass {canceled['id']}, execution status: {canceled['execution']['status']}")


if __name__ == "__main__":
    main()
