# Copyright 2026 Infostellar, Inc.
#
# Shared helpers used by every example: configuration from the environment,
# bearer token handling, a small JSON REST wrapper, and table printing.

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

# A new token is fetched this many seconds before the current one expires, so
# a request sent just before the boundary does not fail with 401.
TOKEN_REFRESH_MARGIN_SECONDS = 60


def fail(message):
    """Prints a message to stderr and exits."""
    print(message, file=sys.stderr)
    sys.exit(1)


def _load_api_key(path):
    """Reads an API key file downloaded from the StellarStation web console.

    Accepts both the console download format, where the client ID is nested
    under "apiKey", and a flat file with clientId, clientSecret,
    tokenEndpoint, and optional scope.
    """
    try:
        with open(os.path.expanduser(path)) as f:
            raw = json.load(f)
    except OSError as err:
        fail(f"Could not read the API key file {path}: {err}")
    except json.JSONDecodeError as err:
        fail(f"The API key file {path} is not valid JSON: {err}")

    nested = raw.get("apiKey") or {}
    key = {
        "client_id": raw.get("clientId") or nested.get("clientId"),
        "client_secret": raw.get("clientSecret"),
        "token_endpoint": raw.get("tokenEndpoint"),
        "scope": raw.get("scope") or "",
    }
    missing = [name for name in ("client_id", "client_secret", "token_endpoint") if not key[name]]
    if missing:
        fail(f"The API key file {path} is missing fields: {', '.join(missing)}")
    return key


class Client:
    """A minimal client for the StellarStation REST API.

    The client exchanges your API key for a bearer token using the OAuth2
    client credentials grant and sends it as "Authorization: Bearer <token>"
    on every request. Tokens are cached until shortly before they expire.

    Configuration comes from two environment variables:

      STELLAR_API_URL      The StellarStation API address
      STELLAR_CREDENTIALS  Path to the API key file downloaded from the
                           StellarStation web console
    """

    def __init__(self):
        api_url = os.environ.get("STELLAR_API_URL")
        key_path = os.environ.get("STELLAR_CREDENTIALS")
        if not api_url:
            fail("STELLAR_API_URL is not set. Set it to your StellarStation API address.")
        if not key_path:
            fail("STELLAR_CREDENTIALS is not set. Set it to the path of your API key file.")
        self.api_url = api_url.rstrip("/")
        self._key = _load_api_key(key_path)
        self._token = None
        self._token_deadline = 0.0

    def token(self):
        """Returns a valid bearer token, fetching a new one when needed."""
        if self._token and time.monotonic() < self._token_deadline:
            return self._token

        response = requests.post(
            self._key["token_endpoint"],
            auth=(self._key["client_id"], self._key["client_secret"]),
            data={"grant_type": "client_credentials", "scope": self._key["scope"]},
            timeout=15,
        )
        if not response.ok:
            fail(
                f"The token endpoint returned {response.status_code}. Check that your API key "
                f"was issued for the environment in STELLAR_API_URL. Response: {response.text.strip()}"
            )
        body = response.json()
        self._token = body["access_token"]
        expires_in = body.get("expires_in", 3600)
        self._token_deadline = time.monotonic() + expires_in - TOKEN_REFRESH_MARGIN_SECONDS
        return self._token

    def request(self, method, path, params=None, body=None):
        """Performs one API request and returns the decoded JSON response.

        On a non-2xx response the error is printed and the script exits, so
        the examples can stay focused on the calls themselves.
        """
        response = requests.request(
            method,
            self.api_url + path,
            params=params,
            json=body,
            headers={"Authorization": "Bearer " + self.token()},
            timeout=30,
        )
        if not response.ok:
            detail = ""
            try:
                parsed = response.json()
                detail = parsed.get("error") or parsed.get("detail") or ""
            except ValueError:
                detail = response.text.strip()
            fail(f"{method} {path} returned {response.status_code}. {detail}".rstrip())
        if not response.content:
            return None
        return response.json()

    def get(self, path, params=None):
        return self.request("GET", path, params=params)

    def post(self, path, body):
        return self.request("POST", path, body=body)

    def put(self, path, body):
        return self.request("PUT", path, body=body)

    def delete(self, path):
        return self.request("DELETE", path)


def utc_now():
    return datetime.now(timezone.utc)


def rfc3339(dt):
    """Formats a datetime the way the API expects, for example 2026-08-22T04:33:13Z."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_rfc3339(value):
    """Parses an RFC3339 timestamp as returned by the API."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def print_table(headers, rows):
    """Prints rows as an aligned table. Values are converted to strings."""
    rows = [[str(cell) for cell in row] for row in rows]
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    print(line)
    print("-" * len(line))
    for row in rows:
        print("  ".join(cell.ljust(w) for cell, w in zip(row, widths)))
