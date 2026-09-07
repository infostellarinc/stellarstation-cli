# Copyright 2026 Infostellar, Inc.
#
# Shows the orbit data StellarStation holds for a satellite: the currently
# active elements, how they are sourced, and the recent activation history.
# StellarStation propagates visibilities from this data, so keeping it
# current keeps the predictions accurate.
#
# Usage:
#   python get_orbit_data.py --satellite-id <id>

import argparse

import toolkit


def main():
    parser = argparse.ArgumentParser(description="Show a satellite's orbit data.")
    parser.add_argument("--satellite-id", required=True, help="Satellite ID, from list_satellites.py")
    args = parser.parse_args()

    client = toolkit.Client()

    current = client.get(f"/v1/satellites/{args.satellite_id}/orbit-data/current")
    elements = current.get("orbital_data") or {}
    print("Active orbit data:")
    print(f"  Source: {current.get('source', '')}")
    print(f"  Epoch:  {current.get('epoch', '')}")
    if "line1" in elements:
        print(f"  {elements['line1']}")
        print(f"  {elements['line2']}")

    # Orbit parameters say where new elements come from: MANUAL means only
    # uploads are used, AUTOMATIC means StellarStation keeps them current from
    # public catalogues using the satellite's NORAD ID.
    parameters = client.get(f"/v1/satellites/{args.satellite_id}/orbit-parameters")
    print(f"\nOrbit data source: {parameters.get('orbital_data_source', '')}"
          + (f" (NORAD ID {parameters['norad_id']})" if parameters.get("norad_id") else ""))

    history = client.get(
        f"/v1/satellites/{args.satellite_id}/orbit-data/history",
        params={"limit": 5},
    )
    items = history.get("items") or []
    if items:
        print("\nRecent activations (newest first):")
        toolkit.print_table(
            ["Activated (UTC)", "Source", "Epoch (UTC)"],
            [[i.get("activated_at", ""), i.get("source", ""), i.get("epoch", "")] for i in items],
        )


if __name__ == "__main__":
    main()
