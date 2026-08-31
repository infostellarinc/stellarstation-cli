# Contributing

## Issues and questions

Both are welcome. Bug reports, unclear documentation, and missing capabilities
are all worth raising as issues.

A useful bug report includes the version (`stellar version`), your operating
system, the command you ran, and what happened. Adding `--verbose` to the command
shows what it was doing at each step, which usually identifies the problem
quickly. Remove any API keys, tokens or certificate identifiers from output
before attaching it.

If the CLI is behaving correctly but a pass did not produce what you expected,
that is usually a question about the service rather than the client. Raise it
with your StellarStation contact, who can see the pass from the other side.

## Pull requests

Please open an issue before writing a patch.

`stellar` is developed in an internal repository and exported here, so this
repository is a copy rather than the place the code lives. A pull request opened
against it cannot simply be merged: the change has to be applied upstream, or the
next export would overwrite it. Raising an issue first means the change can be
applied upstream, where it will not be lost.

Small documentation corrections are the exception, since they are quick to carry
upstream. Those are fine to send directly.

## Security

Do not report security issues as public issues. See
[SECURITY.md](SECURITY.md) for the private reporting route.
