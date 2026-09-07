# StellarStation API examples in Python

Runnable examples showing how to call the StellarStation API from your own
software: listing satellites, finding visibilities, reserving passes,
managing orbit data, and downloading telemetry.

The [`stellar` CLI](../../README.md) covers the same operations interactively.
Use these examples when you want to integrate StellarStation into your own
tooling instead.

## Setup

3 steps, performed once.

**1. Install Python and the dependencies.** Python 3.9 or later is required.
From this directory:

```
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

On Windows PowerShell, activate with `.venv\Scripts\Activate.ps1` instead.

**2. Configure the API address.** Your StellarStation contact gives you an
API address, which looks like a web address:

```
export STELLAR_API_URL='https://api.example.stellarstation.com'
```

**3. Point at your API key.** In the StellarStation web console, go to
Organization then API Keys, create a key, and download it. The download is a
`.json` file. Then:

```
export STELLAR_CREDENTIALS=path/to/your-api-key.json
```

On Windows PowerShell, set both variables with `$env:NAME = 'value'`.

Verify the setup by listing your satellites:

```
python3 list_satellites.py
```

## The examples

Run them in this order the first time; each one produces the identifiers the
next one uses. Every script accepts `--help`.

| Script | What it does |
| --- | --- |
| `list_satellites.py` | Lists your satellites and their IDs |
| `list_visibilities.py` | Lists the upcoming windows in which a ground station can see a satellite |
| `list_configurations.py` | Lists the execution configurations (radio setups) for a satellite at a ground station |
| `reserve_and_cancel_pass.py` | Reserves the next available pass, reads it back, then cancels it to clean up |
| `list_passes.py` | Lists your reserved and executed passes |
| `stream_telemetry.py` | Receives telemetry live while a pass executes |
| `send_command.py` | Sends one command to the satellite during a pass and waits for the acknowledgment |
| `download_telemetry.py` | Downloads the telemetry received during a pass, live or afterwards |
| `get_orbit_data.py` | Shows a satellite's active orbit data, source, and activation history |
| `add_orbit_data.py` | Uploads a TLE for a satellite |

Two modules hold the code the examples share. `toolkit.py` exchanges your API
key for a bearer token using the OAuth2 client credentials grant, caches the
token until shortly before it expires, and sends it as
`Authorization: Bearer <token>` on every request. `streaming.py` requests
stream access for a pass, connects to the message broker with the certificate
the grant provisions, and reads and writes the protobuf messages carried on
the pass topics.

## Notes for integrators

- All timestamps are UTC in RFC3339 format, for example
  `2026-08-22T04:33:13Z`, in both requests and responses.
- Reserving a pass is a three step flow: find a visibility, select an
  execution configuration for that satellite and ground station, then create
  the pass booking a window inside the visibility.
  `reserve_and_cancel_pass.py` walks all three steps.
- Streaming is granted per pass by `POST /authorize`: the response carries
  temporary storage credentials, a client certificate for the message broker,
  the topic and storage location of every requested stream, and each
  channel's rate class. Low rate channels stream live over the broker
  (`stream_telemetry.py`, `send_command.py`); high rate channels are served
  from storage (`download_telemetry.py`).
- The streaming examples cover receiving telemetry and sending commands. They
  are passive consumers otherwise: they do not publish acknowledgments,
  recover dropped connections, or send ground station configuration requests.
  The `stellar` CLI implements the complete protocol, and these examples are
  the starting point for building your own client. Ask your StellarStation
  contact for the full protobuf definitions of the streaming messages.
