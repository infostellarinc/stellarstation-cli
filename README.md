# StellarStation CLI (`stellar`)

StellarStation is a network of ground stations you can reserve time on to
communicate with your satellite. While your satellite is visible from a ground
station you have reserved, StellarStation tracks your
satellite with the antenna, receives its downlinked telemetry, and transmits the
commands you supply.

`stellar` is the command-line client for StellarStation. You run it on your own
computer to schedule contacts, receive telemetry, and command your satellite.

## Terms used in this guide

StellarStation uses a few terms in specific ways, and this guide names one piece
of software you may not have used before.

| Term | Meaning in StellarStation |
| --- | --- |
| Ground station | An antenna site in the StellarStation network |
| Visibility | A future window in which a ground station can see your satellite. Reported with its AOS and LOS |
| Pass | A visibility you have reserved. StellarStation executes it at the scheduled time |
| Execution configuration | A named radio configuration (frequencies, bitrates, framing) for one satellite at one ground station. You select one when reserving |
| Terminal | The application in which you type commands: Terminal on macOS and Linux, PowerShell on Windows |

## Setting up

3 steps, performed once.

### 1. Install

3 parts. Confirm each one succeeded before continuing.

**1a. Install Go.** `stellar` is distributed as source and compiled on your own
machine, so you first need Go, the open-source toolchain that compiles it.
Download the installer for your operating system from <https://go.dev/dl/> and
run it.

Now close your terminal, open a new one, and type:

```
go version
```

You should see something like `go version go1.25.0 linux/amd64`. If instead you
see "command not found", Go did not finish installing. Restart your computer and
try `go version` again before going further.

**1b. Install the program.** Type this exactly as written, all on one line:

```
go install github.com/infostellarinc/stellarstation-cli/cmd/stellar@latest
```

The first time, this takes 1 to 2 minutes and prints a list of `downloading`
lines. That is normal. It is finished when your terminal gives you a fresh
prompt to type at, with no message starting with `go:` left on screen.

**1c. Check it works.**

```
stellar version
```

You should see a version number, such as `v0.1.0`.

If you see "command not found", the program installed correctly but your
computer does not know where to find it. Find out where it went:

```
go env GOPATH
```

That prints a folder, for example `/home/you/go`. The program is in the `bin`
folder inside it, so on that example it is `/home/you/go/bin/stellar`. You can
type that full path instead of `stellar` any time, so try:

```
/home/you/go/bin/stellar version
```

using the folder your own computer printed. To avoid typing the full path every
time, add that `bin` folder to your PATH, following Go's own instructions at
<https://go.dev/doc/install>.

### 2. Configure the API address

Your StellarStation contact gives you an API address, which looks like a web
address. On Mac or Linux:

```
export STELLAR_API_URL='https://api.example.stellarstation.com'
```

On Windows PowerShell:

```
$env:STELLAR_API_URL = 'https://api.example.stellarstation.com'
```

This lasts until you close the terminal window. To avoid retyping it, set the
variable permanently in your shell profile, or add `--api-url <address>` to
every command instead.

### 3. Activate your API key

In the StellarStation web console, go to Organization then API Keys, create a
key, and download it. The download is a `.json` file. Then run:

```
stellar auth activate-api-key path/to/your-api-key.json
```

The key is stored on your computer and every subsequent command authenticates
with it automatically. Repeat this step only if you are issued a new key.

### Verify the setup

```
stellar satellite list-satellites
```

A table listing your satellites confirms the setup is complete.

## Reserving a pass and receiving telemetry

This is the core workflow. Each step produces a value the next one requires.
The identifiers shown below are placeholders: substitute the ones your own
commands return.

**1. Identify your satellite.**

```
stellar satellite list-satellites
```

Note the value in the `ID` column for your satellite.

**2. Find a visibility.**

```
stellar satellite list-visibilities --satellite-id 11111111-2222-3333-4444-555555555555
```

This lists visibilities for the next 7 days. Select one and note 3 values
from that row: `GS_ID`, identifying the ground station, and its `AOS` and `LOS`.
The AOS and LOS are the times you reserve.

**3. Select an execution configuration.**

```
stellar satellite list-configurations \
  --satellite-id 11111111-2222-3333-4444-555555555555 \
  --ground-station-id aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
```

Note the `ID` of the configuration you want. If more than one applies and it is
not clear which your spacecraft expects, confirm with your StellarStation
contact.

**4. Reserve the pass**, using the AOS and LOS from step 2:

```
stellar satellite reserve-pass \
  --satellite-id 11111111-2222-3333-4444-555555555555 \
  --ground-station-id aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee \
  --execution-config-id 99999999-8888-7777-6666-000000000000 \
  --booking-start 2026-08-22T04:33:13Z \
  --booking-stop 2026-08-22T04:41:37Z
```

All times are UTC, in the format `2026-08-22T04:33:13Z`. The response includes a
pass ID, which identifies this pass in every later command. Retain it.

**5. Receive telemetry.** Start this at least 1 minute before AOS and leave it
running:

```
stellar satellite open-stream --pass-id ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb
```

This command only receives. If you also intend to send commands during the
pass, add `--interactive` now (see Commanding your satellite below).

Opening a stream provisions credentials and subscribes to the ground station's
data topics, which normally takes 20 to 30 seconds. Starting 1 minute early
leaves margin for that, so the client is listening before the first frame
arrives.

Downlinked data is written to a `downlink` folder in the directory you ran the
command from, as it arrives, and throughput is reported while the pass runs.
Press Ctrl-C to stop, or add `--enable-auto-close` to exit automatically when the
downlink ends. If your ground software expects a socket rather than files, add
`--proxy tcp` or `--proxy udp` to forward telemetry to it as it arrives (see
Forwarding to other software).

Starting before AOS is only the recommended case, not a requirement. The same
command also handles the 2 other cases:

- **Joining a pass already in progress.** The client retrieves the telemetry
  StellarStation has already received, then continues with the live downlink.
  Expect a delay at startup while it catches up, roughly proportional to how much
  of the pass it missed.
- **Streaming a completed pass.** Run the same command after LOS to retrieve the
  pass in full. A completed pass can be streamed again as often as you need, so
  this is also how you recover data if a live session was interrupted.

## Managing reservations

List the passes you have reserved, or show one in detail:

```
stellar satellite list-passes
stellar satellite get-pass <pass-id>
```

`list-passes` accepts `--satellite-id`, `--start` and `--stop` to narrow the
window, and `--execution-status` to show only passes in a given state.

Move a reserved pass to a different window, or release it entirely:

```
stellar satellite update-pass <pass-id> --scheduled-start <time> --scheduled-stop <time>
stellar satellite cancel-pass <pass-id>
```

## Commanding your satellite

Commands are transmitted during a pass, so commanding is part of `open-stream`.
Decide before starting: commanding cannot be switched on once a stream is
running. If you started a plain stream and need to send a command, stop it with
Ctrl-C and start again with one of the options below.

For live operations, start the stream in interactive mode:

```
stellar satellite open-stream --pass-id <pass-id> --interactive
```

Telemetry keeps arriving as normal, and the terminal accepts typed commands
while the pass runs. Every command starts with a word naming its destination:
`sat` transmits to the satellite, `gs` is for ground station configurations.
A bare payload with no prefix is rejected, so nothing is ever transmitted to a
destination you did not name.

```
sat 0A1B2C3D
gs {"receiverConfigurationRequest":{"bitrate":9600,"modulation":"BPSK"}}
exit
```

When the pass has more than one channel that accepts commands, you must name the
channel between the prefix and the payload: `sat <channel-id> 0A1B2C3D`. The available
channels and what each accepts are printed when the stream starts, and `exit`
(or Ctrl-C) ends the session.

For scripted use there are one-shot options, which send and then exit. A single
command, given as hexadecimal:

```
stellar satellite open-stream --pass-id <pass-id> --send-sat-command 0A1B2C3D
```

Typing and one-shot options are not the only way to command. With `--proxy udp`
or `--proxy tcp`, your own ground software connects to a local socket, and
anything it sends is transmitted to the satellite while telemetry is forwarded
back to it (see Forwarding to other software).

Only one client at a time holds commanding authority for a channel. If another
session already holds it, your command is refused and the message says so. Add
`--override-commanding-lock` to take authority over from that session.

Commands are only transmitted while the pass is booked. A command sent before
the booking starts, or after it stops, is refused and the message gives the
booking time, so nothing is transmitted when the ground station is not yours to
use. Interactive mode reports the window when the session opens and refuses
individual commands once the booking closes, leaving the stream running so
telemetry still arrives.

## Orbit data

StellarStation propagates visibilities from your satellite's orbit data, so
keeping it current keeps the predictions accurate.

```
stellar satellite get-orbit-data --satellite-id <id>
stellar satellite get-orbit-data-history --satellite-id <id>
```

Upload new elements as a TLE file, or inline with `--data`. `--data-type OMM`
accepts an OMM instead:

```
stellar satellite add-orbit-data --satellite-id <id> --data-file ./mysat.tle
```

Choose where the elements come from:

```
stellar satellite get-orbit-data-source --satellite-id <id>
stellar satellite set-orbit-data-source --satellite-id <id> --source automatic
```

With the source set to `automatic`, StellarStation keeps the orbit data current
from public catalogues. With `manual`, only the elements you upload are used, and
an upload is rejected while the source is `automatic`.

## Command help

Every command accepts `--help`, which describes it, lists all of its options, and
gives worked examples:

```
stellar satellite open-stream --help
```

Any command that produces a table also accepts `-o json` or `-o csv`, for
feeding results into other tooling.

## Troubleshooting

- **"command not found"**: see step 1c of Setting up.
- **"no API address configured"**: `STELLAR_API_URL` is not set. It is discarded
  when the terminal closes, so repeat step 2.
- **"no API key set up yet"**: repeat step 3.
- **"Your API key was not accepted"**: the key is most likely issued for a
  different StellarStation environment than the address in `STELLAR_API_URL`.
  Confirm both with your StellarStation contact.
- **A pass produced no telemetry**: confirm the window has started and the pass
  is still scheduled, with `stellar satellite get-pass <pass-id>`. If it has
  already completed, stream it again to retrieve whatever was received.

For anything else, add `--verbose` to the command that failed and send its output
to StellarStation support.

## Full option reference

The sections above cover normal use. This reference covers additional options should you need them. `--help` on any command also prints more information.

### Options accepted by every command

| Option | Default | Meaning |
| --- | --- | --- |
| `--api-url <address>` | `$STELLAR_API_URL` | The StellarStation API address |
| `--credentials <file>` | the activated key | Use a specific API key file |
| `-o`, `--output <format>` | `wide` | Output format: `wide` (readable table), `csv`, or `json` |

### Scheduling commands

| Command | Required | Optional |
| --- | --- | --- |
| `list-satellites` | none | none |
| `list-visibilities` | `--satellite-id` | `--start`, `--stop` |
| `list-configurations` | `--satellite-id`, `--ground-station-id` | none |
| `reserve-pass` | `--satellite-id`, `--ground-station-id`, `--execution-config-id`, `--booking-start`, `--booking-stop` | none |
| `list-passes` | none | `--satellite-id`, `--start`, `--stop`, `--execution-status` |
| `get-pass <pass-id>` | the pass ID | none |
| `update-pass <pass-id>` | the pass ID | `--scheduled-start` with `--scheduled-stop`, `--execution-config-id` |
| `cancel-pass <pass-id>` | the pass ID | none |

`--start` and `--stop` default to now and 7 days after the start, respectively. Omitting both
gives the next 7 days. `--execution-status` accepts values such as `RESERVED`,
`EXECUTING` and `COMPLETED`. On `list-passes` and `list-visibilities`,
`--satellite-id` may be repeated or comma-separated to cover several satellites.

`update-pass` requires `--scheduled-start` and `--scheduled-stop` together.
Supplying only one is rejected, so that a pass cannot be left with a start later
than its end.

### Orbit data commands

| Command | Required | Optional |
| --- | --- | --- |
| `get-orbit-data` | `--satellite-id` | none |
| `add-orbit-data` | `--satellite-id`, and one of `--data` or `--data-file` | `--data-type`, `--epoch`, `--source` |
| `get-orbit-data-history` | `--satellite-id` | `--limit`, `--cursor`, `--source` |
| `get-orbit-data-source` | `--satellite-id` | none |
| `set-orbit-data-source` | `--satellite-id` | `--source`, `--norad-id` |

`--data-type` is `TLE` (the default) or `OMM`. `--epoch` defaults to now. On
`add-orbit-data` and `get-orbit-data-history`, `--source` is `manual`,
`space-track` or `celestrak`; on `set-orbit-data-source` it is `automatic` or
`manual`, and `--norad-id` accompanies `automatic`. Always pass `--source`
explicitly to `set-orbit-data-source`: it is not a required flag, so omitting it
still submits a change. `--limit` on `get-orbit-data-history` accepts 1 to 100
and defaults to 50.

An upload is rejected while the source is `automatic`, so set the source to
`manual` first if you intend to supply elements yourself.

### Choosing what a stream carries

`open-stream` needs either `--pass-id` or `--satellite-id`. With
`--satellite-id` it opens that satellite's next upcoming pass, which saves
looking up the pass ID first.

| Option | Default | Meaning |
| --- | --- | --- |
| `--channels <ids>` | every channel | Stream only these channels, comma-separated |
| `--accepted-framing <types>` | every framing | Keep only these framing types, for example `BITSTREAM,IQ` |
| `--disable-downlink` | off | Do not receive telemetry |
| `--disable-monitoring` | off | Do not receive ground station monitoring data |
| `--disable-config-state` | off | Do not receive configuration state |
| `--disable-event` | off | Do not receive pass events |
| `--disable-uplink` | off | Do not send commands to the satellite |
| `--disable-config-requests` | off | Do not send ground station configuration requests |

The two narrowing options work at different points, which matters on a high rate
pass. `--channels` is sent when the stream is authorized, so excluded channels
are never streamed to you at all. `--accepted-framing` is applied by the client:
excluded high rate framings are not fetched from storage, while low rate
telemetry arrives over the live connection first and is then discarded, so there
it saves disk rather than bandwidth.

### Sending commands

Commanding authority is exclusive: one client at a time may command a given
channel, and satellite uplink and ground station configuration are held
separately, so different clients can hold each.

| Option | Default | Meaning |
| --- | --- | --- |
| `--send-sat-command <hex>` | none | Send one command, then exit |
| `--send-sat-commands <hex,hex>` | none | Send several in the given order, then exit |
| `--send-gs-config <json>` | none | Send one ground station configuration request, then exit |
| `--interactive` | off | Keep the stream open and type commands as the pass runs |
| `--override-commanding-lock` | off | Take commanding authority from the client that currently holds it |

`--interactive` is the option to use for live operations. It keeps receiving
telemetry while you enter commands, so you can react to what the satellite sends
during the pass. The one-shot options exit once the command has been sent, which
suits scripted use.

If commanding is refused because another session holds authority, either stop
that session or add `--override-commanding-lock`. Overriding takes authority
immediately and the previous holder stops being able to command, so confirm
nobody else is operating the pass before using it.

### Where received data goes

| Option | Default | Meaning |
| --- | --- | --- |
| `--dest <folder>` | `./downlink` | Folder to write into |
| `--output-file <path>` | none | Combine data into one file rather than one file per chunk |
| `--output-file-mode <modes>` | `all-combined` | How to group combined output: `all-combined`, `per-channel`, `per-framing`, `per-framing-channel` |
| `--stdout` | off | Also write raw telemetry to standard output, for piping into other tools |
| `--write-in-order` | on | Write chunks strictly in index order |
| `--disable-diagnostics` | off | Do not write the diagnostics summary at the end of the pass |

The diagnostics summary records what was received, what was acknowledged and
where time was spent. Keep it enabled if you may need to raise a support case
about a pass.

### Forwarding to other software

If your ground software expects a socket rather than files, `open-stream` can act
as a local proxy: telemetry is forwarded to it, and anything it sends back is
transmitted to the satellite. Uplink through the proxy uses the same commanding
path as interactive mode: it requires an activated API key, an uplink channel on
the pass, and commanding authority, and data is only transmitted while the pass
is booked. Each payload received on the socket is transmitted as one satellite
command.

| Option | Default | Meaning |
| --- | --- | --- |
| `--proxy <mode>` | `disabled` | Proxy mode: `disabled`, `tcp`, or `udp` |
| `--tcp-listen-addr <addr>` | `127.0.0.1:6001` | Address the TCP proxy listens on |
| `--udp-listen-addr <addr>` | `127.0.0.1:6000` | Address the UDP proxy accepts uplink data on |
| `--udp-send-addr <addr>` | `127.0.0.1:6001` | Address the UDP proxy sends downlink data to |
| `--proxy-allow-remote` | off | Allow the proxy to listen on a non-loopback address |

The proxy sockets carry no authentication of their own. Every process that can
connect to them receives the full downlink of the pass, and anything a
connected peer sends is transmitted to the satellite as a command. For that
reason the listen addresses default to loopback (`127.0.0.1`), so only software
on the same machine can connect. Binding any other interface is refused unless
you pass `--proxy-allow-remote`, and doing so prints a warning: on a shared
network that choice allows any host that can reach the address to read your
downlink and inject commands to your satellite. Only use it on a network you
fully control, and prefer an SSH tunnel or similar authenticated transport for
remote access to the proxy.

### Progress and diagnosis

| Option | Default | Meaning |
| --- | --- | --- |
| `--stats` | off | Show live throughput and timing, and a summary per channel at the end |
| `--enable-auto-close` | off | Exit when the satellite signals end of data, instead of waiting for Ctrl-C |
| `--verbose` | off | Show the technical detail of each step |
| `--debug` | off | Show low-level detail. Very noisy, and intended for support cases |

### Advanced tuning

These have defaults suited to normal passes and are listed for completeness.
Changing them without a specific reason is more likely to hurt than help.

| Option | Default | Meaning |
| --- | --- | --- |
| `--window` | 400 | Maximum number of in-flight downloads |
| `--s3-poll-interval` | 1s | How often to check storage for newly written data |
| `--mqtt-qos` | 1 | Delivery guarantee: 0 at most once, 1 at least once, 2 exactly once. The client deduplicates repeated messages, so 1 is recommended |
| `--source` | `s3` | Force a single data source, `s3` or `mqtt`. Normally selected automatically |

## For developers

Written in Go. To build from a working copy:

```
go build -o stellar ./cmd/stellar
```

The protobuf definitions for the telemetry and commanding stream are under
`protos/`, with the generated Go under `gen/pb/`.
