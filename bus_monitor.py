#!/usr/bin/env python3
"""
bus_monitor.py

Checks how many buses are currently active on a RapidBus (Prasarana) route,
by talking to the same feed the kiosk map page uses, and sends a Telegram
alert if the count drops to 1 or 0.

HOW IT WORKS
------------
1. GET the kiosk page (e.g. https://myrapidbus.prasarana.com.my/kiosk?route=765&bus=)
   This page is server-rendered and embeds a few JS variables we need:
     - `no_route` : the internal route code used by the realtime feed
                    (often NOT the same as the "765" in the URL)
     - `prm`      : the provider code (e.g. "RKL")
2. Open a Socket.IO connection to rapidbus-socketio-avl.prasarana.com.my
   and emit "onFts-reload" with {sid, uid, provider, route}.
3. Listen for "onFts-client" -> payload is base64 + zlib-compressed JSON,
   a list of bus position records, each with a "route" field.
4. Count how many records match our route. That's the same number the
   map shows as bus icons.
5. If count <= 1, send a Telegram message.

IMPORTANT - THINGS YOU MAY NEED TO ADJUST
------------------------------------------
I could not test this live (sandboxed environment can't reach
prasarana.com.my), so treat this as a strong first draft:

- ROUTE_URL_PARAM below is "765" (matches your link). If the extraction
  of `no_route` / `prm` fails, print(html) near the marked line and search
  manually for "no_route" and "var prm" in the page source to find the
  right values, then hardcode them below as a fallback.
- The socket.io server is described (by the person who reverse-engineered
  it) as an OLD version (Socket.IO v2 / Engine.IO v3 protocol), which is
  why this uses `socketIO-client-2` instead of the modern `python-socketio`
  library (modern client speaks a newer, incompatible protocol version).
- The "route" field inside each bus record may be formatted differently
  than `no_route` (e.g. with a prefix). If FILTER isn't matching anything
  but you can see the raw payload has entries, print raw_data and compare.

INSTALL
-------
pip install requests beautifulsoup4 socketIO-client-2 --break-system-packages

USAGE
-----
python bus_monitor.py --route 765 --telegram-token XXX --telegram-chat-id YYY
"""

import argparse
import base64
import json
import re
import sys
import time
import zlib
import random
import string

import requests
from bs4 import BeautifulSoup
from socketIO_client_2 import SocketIO, BaseNamespace

KIOSK_BASE = "https://myrapidbus.prasarana.com.my/kiosk"
SOCKET_HOST = "rapidbus-socketio-avl.prasarana.com.my"


def random_sid(length=32):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def get_route_metadata(route_param):
    """Fetch the kiosk page and pull out the internal route code + provider."""
    url = f"{KIOSK_BASE}?route={route_param}&bus="
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        ),
        "Referer": KIOSK_BASE,
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    html = resp.text

    # --- adjust here if extraction fails ---
    no_route_match = re.search(r"var no_route\s*=.*?\?\s*'([^']+)'\s*:", html, re.S)
    prm_match = re.search(r"var prm\s*=\s*'([^']*)'", html)

    no_route = no_route_match.group(1) if no_route_match else route_param
    provider = prm_match.group(1) if prm_match else ""

    if not no_route_match:
        print("[warn] could not auto-extract 'no_route' from page; "
              "falling back to raw route param. Inspect page source if "
              "results look wrong.", file=sys.stderr)
    if not prm_match:
        print("[warn] could not auto-extract provider ('prm') from page; "
              "using empty string.", file=sys.stderr)

    return no_route, provider


def count_active_buses(no_route, provider, timeout=15):
    """Connect to the socket, request live data, count matching buses."""
    result = {"count": None, "raw": None}

    class FtsNamespace(BaseNamespace):
        def on_connect(self):
            self.emit("onFts-reload", {
                "sid": random_sid(),
                "uid": "",
                "provider": provider,
                "route": no_route,
            })

        def on_fts_client(self, data):
            try:
                decompressed = zlib.decompress(base64.b64decode(data))
                buses = json.loads(decompressed)
            except Exception:
                # data might already be plain JSON/text depending on server config
                try:
                    buses = json.loads(data)
                except Exception as e:
                    print(f"[error] could not decode payload: {e}", file=sys.stderr)
                    buses = []

            matching = [b for b in buses if str(b.get("route", "")) == str(no_route)]
            result["count"] = len(matching)
            result["raw"] = buses

    socket = SocketIO(f"https://{SOCKET_HOST}", 443,
                       Namespace=FtsNamespace,
                       transports=["websocket"])
    socket.on("onFts-client", lambda data: FtsNamespace.on_fts_client(socket.get_namespace(), data))

    start = time.time()
    while result["count"] is None and (time.time() - start) < timeout:
        socket.wait(seconds=1)

    socket.disconnect()
    return result["count"], result["raw"]


def send_telegram(token, chat_id, message):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, data={"chat_id": chat_id, "text": message}, timeout=10)
    resp.raise_for_status()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--route", default="765", help="Route number, as in the kiosk URL")
    parser.add_argument("--telegram-token", required=True)
    parser.add_argument("--telegram-chat-id", required=True)
    parser.add_argument("--alert-threshold", type=int, default=1,
                         help="Send alert if bus count <= this (default 1)")
    args = parser.parse_args()

    no_route, provider = get_route_metadata(args.route)
    print(f"[info] route param={args.route} -> no_route={no_route} provider={provider!r}")

    count, raw = count_active_buses(no_route, provider)

    if count is None:
        print("[error] no data received from feed within timeout", file=sys.stderr)
        send_telegram(
            args.telegram_token, args.telegram_chat_id,
            f"⚠️ Route {args.route}: couldn't reach the bus feed at all "
            f"(might be down, or script needs a config tweak)."
        )
        sys.exit(1)

    print(f"[info] active buses on route {args.route}: {count}")

    if count <= args.alert_threshold:
        send_telegram(
            args.telegram_token, args.telegram_chat_id,
            f"🚌 Route {args.route}: only {count} bus(es) currently active "
            f"(normal is 2). Check before heading out."
        )
    else:
        print("[info] normal operation, no alert sent")


if __name__ == "__main__":
    main()
