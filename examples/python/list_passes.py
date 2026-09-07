# Copyright 2026 Infostellar, Inc.
#
# Lists your passes: reserved upcoming passes and, with --days-back, passes
# that already executed.
#
# Usage:
#   python list_passes.py [--satellite-id <id>] [--days-ahead 7] [--days-back 0]
#                         [--execution-status PENDING]

import argparse
from datetime import timedelta

import toolkit


def list_passes(client, satellite_id, start, stop, execution_status):
    params = {
        "start": toolkit.rfc3339(start),
        "stop": toolkit.rfc3339(stop),
    }
    if satellite_id:
        params["satellite_ids"] = satellite_id
    if execution_status:
        params["execution_status"] = execution_status
    return client.get("/v1/passes", params=params).get("passes") or []


def main():
    parser = argparse.ArgumentParser(description="List your passes.")
    parser.add_argument("--satellite-id", help="Only show passes for this satellite")
    parser.add_argument("--days-ahead", type=int, default=7, help="How many days ahead to search (default 7)")
    parser.add_argument("--days-back", type=int, default=0, help="How many days back to search (default 0)")
    parser.add_argument(
        "--execution-status",
        help="Only show passes in this state: PENDING, EXECUTING, COMPLETE, CANCELED, or ERROR",
    )
    args = parser.parse_args()

    now = toolkit.utc_now()
    client = toolkit.Client()
    passes = list_passes(
        client,
        args.satellite_id,
        now - timedelta(days=args.days_back),
        now + timedelta(days=args.days_ahead),
        args.execution_status,
    )

    if not passes:
        print("No passes found in the requested window.")
        return

    rows = []
    for p in passes:
        booking = p.get("booking") or {}
        execution = p.get("execution") or {}
        rows.append([
            p["id"],
            p["satellite"].get("name", ""),
            p["ground_station"].get("name", ""),
            booking.get("start", ""),
            booking.get("stop", ""),
            execution.get("status", ""),
        ])
    toolkit.print_table(
        ["Pass ID", "Satellite", "Ground station", "Booking start (UTC)", "Booking stop (UTC)", "Status"],
        rows,
    )


if __name__ == "__main__":
    main()
