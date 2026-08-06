#!/usr/bin/env python3
"""
bus_monitor.py

Checks how many buses are currently active on a RapidBus (Prasarana) route,
by talking to the same feed the kiosk map page uses, and sends a Telegram
alert if the count of buses actually ON the route drops to 1 or 0.

"Actually on the route" matters because the feed sometimes reports a bus
under this route's code even though its GPS position is nowhere near the
route -- e.g. parked at a depot. Those are filtered out by checking each
bus's position against the route's own bus stops (also embedded in the
kiosk page) and requiring it to be within --max-distance-km of at least
one stop.

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
4. Count how many records match our route, then further filter to only
   those within --max-distance-km of one of the route's own bus stops.
5. If that filtered count <= 1, send a Telegram message.

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
import gzip
import json
import re
import sys
import time
import zlib
import random
import string

import requests
from bs4 import BeautifulSoup
from socketIO_client import SocketIO, BaseNamespace

KIOSK_BASE = "https://myrapidbus.prasarana.com.my/kiosk"
SOCKET_HOST = "rapidbus-socketio-avl.prasarana.com.my"


def random_sid(length=32):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def get_route_metadata(route_param):
    """Fetch the kiosk page and pull out the internal route code, provider,
    and the list of the route's bus stops (used later for the geofence
    check)."""
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
    bstp_match = re.search(r"var bstp\s*=\s*(\[.*?\])\s*;", html, re.S)

    no_route = no_route_match.group(1) if no_route_match else route_param
    provider = prm_match.group(1) if prm_match else ""

    if not no_route_match:
        print("[warn] could not auto-extract 'no_route' from page; "
              "falling back to raw route param. Inspect page source if "
              "results look wrong.", file=sys.stderr)
    if not prm_match:
        print("[warn] could not auto-extract provider ('prm') from page; "
              "using empty string.", file=sys.stderr)

    stops = []
    if bstp_match:
        try:
            stops = json.loads(bstp_match.group(1))
            print(f"[debug] extracted {len(stops)} bus stop(s) from page", file=sys.stderr)
            if stops:
                print(f"[debug]   sample stop record: {stops[0]}", file=sys.stderr)
        except Exception as e:
            print(f"[warn] found 'bstp' but couldn't parse it as JSON ({e}); "
                  "geofence check will be skipped.", file=sys.stderr)
    else:
        print("[warn] could not find 'bstp' (bus stop list) on the page; "
              "geofence check will be skipped, falling back to plain "
              "route-code matching.", file=sys.stderr)

    return no_route, provider, stops


def stop_lat_lon(stop):
    """Bus stop records may use different key names for coordinates --
    try the common ones."""
    lat_keys = ("latitude", "lat", "Lat", "y")
    lon_keys = ("longitude", "lon", "lng", "Lon", "Lng", "x")
    lat = next((stop[k] for k in lat_keys if k in stop and stop[k] not in (None, "")), None)
    lon = next((stop[k] for k in lon_keys if k in stop and stop[k] not in (None, "")), None)
    try:
        return float(lat), float(lon)
    except (TypeError, ValueError):
        return None


def haversine_km(lat1, lon1, lat2, lon2):
    from math import radians, sin, cos, asin, sqrt
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * 6371 * asin(sqrt(a))


def filter_on_route(buses, stops, max_distance_km):
    """Keep only buses whose GPS position is near at least one of the
    route's bus stops. If we don't have usable stop coordinates, skip the
    check entirely (return buses unchanged) rather than falsely zeroing
    everything out."""
    stop_coords = [c for c in (stop_lat_lon(s) for s in stops) if c is not None]
    if not stop_coords:
        print("[warn] no usable stop coordinates, skipping geofence check",
              file=sys.stderr)
        return buses

    on_route = []
    for b in buses:
        try:
            blat, blon = float(b.get("latitude")), float(b.get("longitude"))
        except (TypeError, ValueError):
            continue
        nearest = min(haversine_km(blat, blon, slat, slon) for slat, slon in stop_coords)
        print(f"[debug]   bus {b.get('bus_no', '?')}: {nearest:.2f} km from "
              f"nearest stop", file=sys.stderr)
        if nearest <= max_distance_km:
            on_route.append(b)
    return on_route


def try_parse_payload(data):
    """Try a few plausible encodings for the live-bus payload, since we
    can't verify live which one the server actually uses."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "buses", "list", "result"):
            if key in data and isinstance(data[key], list):
                return data[key]
        return None
    if not isinstance(data, str) or not data:
        return None
    # gzip (confirmed format: base64 payloads start with "H4sI", the
    # base64 signature for gzip's magic bytes 1f 8b 08)
    try:
        return json.loads(gzip.decompress(base64.b64decode(data)))
    except Exception:
        pass
    # zlib (kept as a fallback in case a different route ever uses this)
    try:
        return json.loads(zlib.decompress(base64.b64decode(data)))
    except Exception:
        pass
    # plain JSON string
    try:
        return json.loads(data)
    except Exception:
        pass
    # base64 -> plain JSON (no compression)
    try:
        return json.loads(base64.b64decode(data))
    except Exception:
        pass
    return None


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

    socket = SocketIO(f"https://{SOCKET_HOST}", 443,
                       Namespace=FtsNamespace,
                       transports=["websocket"])

    def handle_data(*args):
        print(f"[debug] onFts-client fired with {len(args)} arg(s)", file=sys.stderr)
        for i, a in enumerate(args):
            preview = repr(a)[:200]
            print(f"[debug]   arg[{i}] type={type(a).__name__} preview={preview}",
                  file=sys.stderr)

        buses = None
        for a in args:
            buses = try_parse_payload(a)
            if buses is not None:
                break

        if buses is None:
            print("[error] could not decode payload from any argument received "
                  "-- see [debug] lines above for the raw shape", file=sys.stderr)
            buses = []
        else:
            print(f"[debug] decoded payload OK, {len(buses)} record(s) total",
                  file=sys.stderr)
            if buses:
                print(f"[debug]   sample record: {buses[0]}", file=sys.stderr)
                distinct_routes = sorted(set(str(b.get("route", "")) for b in buses))
                print(f"[debug]   distinct 'route' values seen: {distinct_routes[:30]}"
                      f"{' ...' if len(distinct_routes) > 30 else ''}", file=sys.stderr)

        matching = [
            b for b in buses
            if str(b.get("route", "")).startswith(str(no_route))
        ]
        result["count"] = len(matching)
        result["raw"] = buses

    socket.on("onFts-client", handle_data)

    start = time.time()
    while result["count"] is None and (time.time() - start) < timeout:
        socket.wait(seconds=1)

    socket.disconnect()
    return result["count"], result["raw"]


def send_telegram(token, chat_id, message):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, data={"chat_id": chat_id, "text": message}, timeout=10)
    resp.raise_for_status()


def load_state(path):
    """Read the last known state from disk. Missing/corrupt file = unknown."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {"state": "unknown", "count": None}


def save_state(path, state, count):
    with open(path, "w") as f:
        json.dump({"state": state, "count": count, "checked_at": time.time()}, f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--route", default="765", help="Route number, as in the kiosk URL")
    parser.add_argument("--telegram-token", required=True)
    parser.add_argument("--telegram-chat-id", required=True)
    parser.add_argument("--alert-threshold", type=int, default=1,
                         help="Send alert if bus count <= this (default 1)")
    parser.add_argument("--state-file", default="status.json",
                         help="Where to persist last known state, so alerts "
                              "only fire on a CHANGE, not every check.")
    parser.add_argument("--max-distance-km", type=float, default=2.0,
                         help="A bus further than this from every stop on "
                              "the route is treated as not actually "
                              "running it (default 2.0 km).")
    args = parser.parse_args()

    previous = load_state(args.state_file)
    prev_state = previous.get("state", "unknown")

    no_route, provider, stops = get_route_metadata(args.route)
    print(f"[info] route param={args.route} -> no_route={no_route} provider={provider!r}")

    raw_matching_count, raw_matching = count_active_buses(no_route, provider)

    kiosk_link = f"{KIOSK_BASE}?route={args.route}&bus="

    if raw_matching_count is None:
        print("[error] no data received from feed within timeout", file=sys.stderr)
        if prev_state != "error":
            send_telegram(
                args.telegram_token, args.telegram_chat_id,
                f"⚠️ Route {args.route}: couldn't reach the bus feed at all "
                f"(might be down, or script needs a config tweak).\n{kiosk_link}"
            )
        save_state(args.state_file, "error", None)
        sys.exit(1)

    print(f"[info] buses reporting route {args.route}: {raw_matching_count}")

    on_route = filter_on_route(raw_matching, stops, args.max_distance_km)
    count = len(on_route)
    print(f"[info] of those, actually on the route (within "
          f"{args.max_distance_km}km of a stop): {count}")

    new_state = "low" if count <= args.alert_threshold else "normal"

    if new_state != prev_state:
        # Only message on an actual transition (normal->low, low->normal,
        # error->either). Repeated checks that find the same state stay quiet.
        if new_state == "low":
            send_telegram(
                args.telegram_token, args.telegram_chat_id,
                f"🚌 Route {args.route}: dropped to {count} bus(es) currently "
                f"active (normal is 2). Check before heading out.\n{kiosk_link}"
            )
        else:
            send_telegram(
                args.telegram_token, args.telegram_chat_id,
                f"✅ Route {args.route}: back to normal ({count} buses active).\n{kiosk_link}"
            )
    else:
        print(f"[info] state unchanged ({new_state}), no alert sent")

    save_state(args.state_file, new_state, count)


if __name__ == "__main__":
    main()
