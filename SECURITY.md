# Security policy

## Reporting a vulnerability

Please report security issues privately, not as a public issue.

Use the Security tab of this repository and choose "Report a vulnerability".
That opens a private channel visible only to the maintainers.

Please include enough detail to reproduce the problem: the version of `stellar`
(`stellar version`), what you did, and what happened. If a proof of concept is
easier than a description, attach it.

You will get an acknowledgement that the report has been received. If a fix is
needed, we will tell you when it ships.

## Scope

This repository contains the StellarStation command-line client. Issues in the
client itself are in scope, including:

- Mishandling of API keys or the access tokens minted from them
- Mishandling of the X.509 certificates provisioned for a stream
- Anything that writes outside the destination folder, or reads outside the
  files it was pointed at
- Anything that lets one operator reach another operator's passes or telemetry
  through the client

Vulnerabilities in the StellarStation service rather than the client are also
worth reporting, and the same private channel reaches the right people.

## Handling credentials while reporting

`stellar` stores an activated API key under `.stellarstation` in your home
directory, and `--verbose` and `--debug` output can contain tokens and
certificate identifiers. Please remove those before attaching logs. If a
credential has already been exposed, say so in the report and revoke the key in
the StellarStation console.
