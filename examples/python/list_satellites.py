# Copyright 2026 Infostellar, Inc.
#
# Lists the satellites your API key can access.
#
# Usage:
#   python list_satellites.py

import toolkit


def main():
    client = toolkit.Client()
    satellites = client.get("/v1/satellites").get("satellites") or []

    if not satellites:
        print("No satellites found. Ask your StellarStation administrator to grant your API key access.")
        return

    toolkit.print_table(
        ["ID", "Name", "Schedulable"],
        [[s["id"], s["displayName"], "yes" if s.get("schedulable") else "no"] for s in satellites],
    )


if __name__ == "__main__":
    main()
