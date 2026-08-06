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

Optionally (via --watch-stop-id / --watch-stop-name) it can also watch a
SPECIFIC stop and alert once a bus that was near it is no longer near it,
within a configurable time window around a scheduled departure (e.g. a
7:00am and 7:30am departure from your usual stop). Detection resolution
is limited by how often this script runs (every 10 min in the default
GitHub Actions schedule), so a "departed" alert can arrive up to ~10 min
after it actually happened.

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
            for s in stops:
                print(f"[debug]   stop: id={s.get('stop_id')!r} "
                      f"name={s.get('stop_name')!r}", file=sys.stderr)
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


def find_stop(stops, stop_id=None, name_contains=None):
    """Find a specific stop by id (preferred) or by name substring."""
    if stop_id:
        for s in stops:
            if str(s.get("stop_id", "")).upper() == stop_id.upper():
                return s
    if name_contains:
        for s in stops:
            if name_contains.upper() in str(s.get("stop_name", "")).upper():
                return s
    return None


def nearest_bus_distance_km(buses, target_lat, target_lon):
    """Closest distance from any bus in the list to a single point.
    Returns None if there are no buses with usable coordinates."""
    dists = []
    for b in buses:
        try:
            blat, blon = float(b.get("latitude")), float(b.get("longitude"))
        except (TypeError, ValueError):
            continue
        dists.append(haversine_km(blat, blon, target_lat, target_lon))
    return min(dists) if dists else None


def parse_departure_windows(departures_str, before_minutes=10, after_minutes=15):
    """Turn '07:00,07:30' into a list of (label, start_minutes_of_day,
    end_minutes_of_day) windows, e.g. 07:00 -> (06:50, 07:15)."""
    windows = []
    for part in departures_str.split(","):
        part = part.strip()
        if not part:
            continue
        h, m = map(int, part.split(":"))
        center = h * 60 + m
        windows.append((part, center - before_minutes, center + after_minutes))
    return windows


def current_myt_minutes_and_date():
    from datetime import datetime, timezone, timedelta
    myt = timezone(timedelta(hours=8))
    now = datetime.now(timezone.utc).astimezone(myt)
    return now.hour * 60 + now.minute, now.strftime("%Y-%m-%d")


def check_stop_departure(on_route_buses, stops, args, stop_watch_state, ts, kiosk_link,
                          telegram_token, telegram_chat_id):
    """Detect a bus departing a specific stop (e.g. the one you catch it
    from), inside configured time windows around scheduled departures.
    Mutates and returns the updated stop_watch_state dict."""
    if not args.watch_stop_id and not args.watch_stop_name:
        return stop_watch_state  # feature not enabled

    target = find_stop(stops, args.watch_stop_id, args.watch_stop_name)
    if target is None:
        print(f"[warn] watched stop '{args.watch_stop_id or args.watch_stop_name}' "
              f"not found in this route's stop list -- skipping departure watch. "
              f"Known stop_ids: {[s.get('stop_id') for s in stops]}", file=sys.stderr)
        return stop_watch_state

    print(f"[debug] watched stop resolved: id={target.get('stop_id')!r} "
          f"name={target.get('stop_name')!r}", file=sys.stderr)

    coords = stop_lat_lon(target)
    if coords is None:
        print(f"[warn] watched stop found but has no usable coordinates: {target}",
              file=sys.stderr)
        return stop_watch_state

    target_lat, target_lon = coords
    now_minutes, today = current_myt_minutes_and_date()

    if stop_watch_state.get("date") != today:
        stop_watch_state = {"date": today, "windows": {}}

    windows = parse_departure_windows(args.watch_departures)
    active_window = next(
        (label for label, start, end in windows if start <= now_minutes <= end), None
    )
    if active_window is None:
        print("[debug] stop-watch: outside any departure window right now, skipping",
              file=sys.stderr)
        return stop_watch_state

    w_state = stop_watch_state["windows"].setdefault(
        active_window, {"seen_near": False, "alerted": False}
    )
    if w_state["alerted"]:
        return stop_watch_state  # already alerted for this window today

    distance = nearest_bus_distance_km(on_route_buses, target_lat, target_lon)
    print(f"[debug] stop-watch [{active_window}]: nearest bus to "
          f"{target.get('stop_name', args.watch_stop_id)} is "
          f"{'n/a' if distance is None else f'{distance:.2f} km'} away "
          f"(seen_near so far: {w_state['seen_near']})", file=sys.stderr)

    if distance is not None and distance <= args.watch_near_km:
        w_state["seen_near"] = True
    elif w_state["seen_near"]:
        # was near, now isn't -> treat as departed
        send_telegram(
            telegram_token, telegram_chat_id,
            f"🚏 A route {args.route} bus has left {target.get('stop_name', args.watch_stop_id)}"
            f" (~{active_window} departure).\nAs of {ts}\n{kiosk_link}"
        )
        w_state["alerted"] = True

    return stop_watch_state


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


def count_active_buses_with_retries(no_route, provider, attempts=3, timeout=15, delay=5):
    """Call count_active_buses up to `attempts` times, only giving up
    (returning None) if every attempt fails to get data. A transient
    network hiccup shouldn't trigger a false 'feed is down' alert."""
    for attempt in range(1, attempts + 1):
        count, raw = count_active_buses(no_route, provider, timeout=timeout)
        if count is not None:
            if attempt > 1:
                print(f"[info] got data on attempt {attempt}/{attempts}", file=sys.stderr)
            return count, raw
        print(f"[warn] attempt {attempt}/{attempts} got no data from feed",
              file=sys.stderr)
        if attempt < attempts:
            time.sleep(delay)
    return None, None


def send_telegram(token, chat_id, message):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, data={"chat_id": chat_id, "text": message}, timeout=10)
    resp.raise_for_status()


def myt_timestamp():
    """Human-readable current time in Malaysia time, e.g. '6:42am MYT'."""
    from datetime import datetime, timezone, timedelta
    myt = timezone(timedelta(hours=8))
    now = datetime.now(timezone.utc).astimezone(myt)
    hour12 = now.strftime("%I").lstrip("0") or "12"
    return f"{hour12}:{now.strftime('%M%p').lower()} MYT"


def load_state(path):
    """Read the last known state from disk. Missing/corrupt file = unknown."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {"state": "unknown", "count": None, "last_alert_at": 0}


def save_state(path, state, count, last_alert_at, stop_watch):
    with open(path, "w") as f:
        json.dump({
            "state": state,
            "count": count,
            "checked_at": time.time(),
            "last_alert_at": last_alert_at,
            "stop_watch": stop_watch,
        }, f)


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
    parser.add_argument("--feed-retries", type=int, default=3,
                         help="How many times to retry the feed connection "
                              "before treating it as actually down (default 3).")
    parser.add_argument("--retry-delay", type=float, default=5.0,
                         help="Seconds to wait between feed retry attempts (default 5).")
    parser.add_argument("--reminder-minutes", type=float, default=25.0,
                         help="While still in a 'low' state, re-send a "
                              "reminder at most this often, in minutes "
                              "(default 25).")
    parser.add_argument("--watch-stop-id", default="",
                         help="Stop ID to watch for departures, e.g. 'KJ153'. "
                              "Leave blank to disable this feature.")
    parser.add_argument("--watch-stop-name", default="",
                         help="Fallback: match a stop by name substring "
                              "instead of/as well as --watch-stop-id, "
                              "e.g. 'MAHKOTA RESIDENCE'.")
    parser.add_argument("--watch-departures", default="07:00,07:30",
                         help="Comma-separated HH:MM (MYT) scheduled "
                              "departure times to watch for at the given "
                              "stop (default '07:00,07:30').")
    parser.add_argument("--watch-near-km", type=float, default=0.3,
                         help="A bus within this distance of the watched "
                              "stop counts as 'at' it (default 0.3 km).")
    args = parser.parse_args()

    previous = load_state(args.state_file)
    prev_state = previous.get("state", "unknown")
    last_alert_at = previous.get("last_alert_at", 0)
    stop_watch = previous.get("stop_watch", {"date": "", "windows": {}})

    no_route, provider, stops = get_route_metadata(args.route)
    print(f"[info] route param={args.route} -> no_route={no_route} provider={provider!r}")

    raw_matching_count, raw_matching = count_active_buses_with_retries(
        no_route, provider, attempts=args.feed_retries, delay=args.retry_delay
    )

    kiosk_link = f"{KIOSK_BASE}?route={args.route}&bus="
    ts = myt_timestamp()
    now = time.time()

    if raw_matching_count is None:
        print(f"[error] no data received from feed after {args.feed_retries} attempts",
              file=sys.stderr)
        if prev_state != "error":
            send_telegram(
                args.telegram_token, args.telegram_chat_id,
                f"⚠️ Route {args.route}: couldn't reach the bus feed at all "
                f"after {args.feed_retries} tries (might be down, or script "
                f"needs a config tweak).\nAs of {ts}\n{kiosk_link}"
            )
            last_alert_at = now
        save_state(args.state_file, "error", None, last_alert_at, stop_watch)
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
                f"active (normal is 2). Check before heading out.\n"
                f"As of {ts}\n{kiosk_link}"
            )
        else:
            send_telegram(
                args.telegram_token, args.telegram_chat_id,
                f"✅ Route {args.route}: back to normal ({count} buses active).\n"
                f"As of {ts}\n{kiosk_link}"
            )
        last_alert_at = now
    elif new_state == "low":
        # Same state as last time, but if it's still "low" and enough time
        # has passed, send a reminder rather than staying silent for the
        # whole disruption.
        minutes_since_alert = (now - last_alert_at) / 60
        if minutes_since_alert >= args.reminder_minutes:
            send_telegram(
                args.telegram_token, args.telegram_chat_id,
                f"🚌 Still only {count} bus(es) active on route {args.route}.\n"
                f"As of {ts}\n{kiosk_link}"
            )
            last_alert_at = now
        else:
            print(f"[info] still low, but only {minutes_since_alert:.1f} min "
                  f"since last alert (threshold {args.reminder_minutes}), "
                  f"staying quiet", file=sys.stderr)
    else:
        print(f"[info] state unchanged ({new_state}), no alert sent")

    stop_watch = check_stop_departure(
        on_route, stops, args, stop_watch, ts, kiosk_link,
        args.telegram_token, args.telegram_chat_id
    )

    save_state(args.state_file, new_state, count, last_alert_at, stop_watch)


if __name__ == "__main__":
    main()
