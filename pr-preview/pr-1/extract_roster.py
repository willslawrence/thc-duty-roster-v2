#!/usr/bin/env python3
"""
Extract duty roster data from Fleetplan's Bryntum scheduler.
Reads raw JSON from stdin (or file arg), outputs roster_data.json.

Usage:
  python3 extract_roster.py < raw_fleetplan.json > roster_data.json
  python3 extract_roster.py raw_fleetplan.json
"""

import json
import sys
from datetime import datetime, timedelta

# Pilot name normalization: Fleetplan full name → display name
NAME_MAP = {
    "William Stewart Lawrence": "Will Lawrence",
    "Matthias Klemp": "Matthias Klemp",
    "Roberto Piani": "Roberto Piani",
    "Gilles Plaisance": "Gilles Plaisance",
    "Nathan  Piper": "Nathan Piper",
    "Lisa  kate le Roux": "Lisa Le Roux",
    "Lindsay Claire Wegner Pentz": "Lindsay Pentz",
    "Dan Cristian Munteanu": "Dan Munteanu",
    "Matthew John OBrien": "Matt O'Brien",
    "David  Peter  Schicht": "David Schicht",
    "David  Joshua  Leipsig": "David Leipsig",
    "Kevin Allen": "Kevin Allen",
    "Julio Bonet": "Julio Bonet",
    "Marius Hertz": "Marius Hertz",
    "Rohit  Kaundinya": "Rohit Kaundinya",
    "Iwona Redlinska": "Ivona Redlinska",
    "Stephan Mayer": "Stephan Mayer",
}

MANAGEMENT = {"Will Lawrence", "Matthias Klemp", "Gilles Plaisance", "Roberto Piani"}

# Resources to skip (non-pilot entries)
SKIP_NAMES = {
    "Pilots", "Directory", "Operation P135", " Headquarters",
    "OETH (Thumamah Airport)", " XRSC (Alsalam)", "XNTM (Trojena)",
    "H125", "H125 A&P", "H125.MX 12/7", "H125.PAX135 12/5",
    "H125.PAX135 12/7", "H125.SURVEY 12/7", "Reserve H125",
    "DAY 03:00Z - 15:00Z", "MX - DAY 03:00Z - 15:00Z",
    "PIC", "Setup",
}

# Desired display order
PILOT_ORDER = [
    "Marius Hertz", "Dan Munteanu", "David Schicht", "Kevin Allen",
    "Julio Bonet", "Matt O'Brien", "Nathan Piper", "David Leipsig",
    "Lisa Le Roux", "Ivona Redlinska", "Lindsay Pentz", "Rohit Kaundinya",
    "Stephan Mayer", "Will Lawrence", "Matthias Klemp",
]


def parse_date(iso_str):
    """Parse ISO date string to date, adjusting for 21:00 UTC = next day local."""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    # Fleetplan uses 21:00 UTC as day boundary (midnight AST = UTC+3)
    # So 2026-01-30T21:00:00Z = 2026-01-31 local
    if dt.hour == 21:
        return (dt + timedelta(hours=3)).strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d")


def consolidate_events(events):
    """
    Group consecutive same-type day events into rotation blocks.
    STANDBY + DUTY = ON rotation. OFF = OFF rotation.
    TRAVEL and VACATION are kept as separate event markers.
    """
    # Sort by start date
    events.sort(key=lambda e: e["start"])

    # Separate into rotation events and marker events
    rotation_days = []  # (date, type) - for building ON/OFF blocks
    markers = []  # DUTY, TRAVEL, VACATION markers

    seen_dates = {}  # date -> primary type (to deduplicate)

    for e in events:
        date = parse_date(e["start"])
        etype = e["type"]

        if etype == "TRAVEL":
            markers.append({"type": "TRAVEL", "date": date})
            continue

        if etype == "VACATION":
            markers.append({"type": "VACATION", "date": date})
            continue

        if etype in ("STANDBY", "DUTY", "OFF"):
            # DUTY takes priority as a marker, but for rotation it's still "ON"
            if etype == "DUTY":
                markers.append({"type": "DUTY", "date": date})

            # For rotation blocks: STANDBY/DUTY = ON, OFF = OFF
            rot_status = "ON" if etype in ("STANDBY", "DUTY") else "OFF"

            if date not in seen_dates:
                seen_dates[date] = rot_status
                rotation_days.append((date, rot_status))
            elif rot_status == "ON" and seen_dates[date] == "OFF":
                # ON overrides OFF if both exist for same day
                seen_dates[date] = "ON"
                rotation_days = [(d, s if d != date else "ON") for d, s in rotation_days]

    # Sort rotation days
    rotation_days.sort(key=lambda x: x[0])

    # Build rotation blocks from consecutive same-status days
    rotations = []
    if rotation_days:
        current_start = rotation_days[0][0]
        current_status = rotation_days[0][1]
        current_end = rotation_days[0][0]

        for date, status in rotation_days[1:]:
            if status == current_status:
                current_end = date
            else:
                rotations.append({
                    "start": current_start,
                    "end": current_end,
                    "status": current_status
                })
                current_start = date
                current_status = status
                current_end = date

        rotations.append({
            "start": current_start,
            "end": current_end,
            "status": current_status
        })

    # Deduplicate markers
    seen_markers = set()
    unique_markers = []
    for m in markers:
        key = (m["type"], m["date"])
        if key not in seen_markers:
            seen_markers.add(key)
            unique_markers.append(m)
    unique_markers.sort(key=lambda m: m["date"])

    return rotations, unique_markers


def process(raw_data):
    """Process raw Fleetplan data into roster format."""
    # Deduplicate pilots: group by normalized name, take lowest ID
    pilot_map = {}  # display_name -> {id, fullName, events[]}

    for resource in raw_data:
        name = resource["name"].strip()

        # Skip non-pilot resources
        if name in SKIP_NAMES or not name:
            continue

        # Normalize name
        display_name = NAME_MAP.get(name)
        if display_name is None:
            # Unknown pilot - skip
            continue

        rid = resource["id"]
        events = resource.get("events", [])

        if display_name not in pilot_map or rid < pilot_map[display_name]["id"]:
            pilot_map[display_name] = {
                "id": rid,
                "fullName": name,
                "events": events
            }
        else:
            # Merge events from duplicate resource entries
            pilot_map[display_name]["events"].extend(events)

    # Build output
    pilots = []
    for display_name in PILOT_ORDER:
        if display_name not in pilot_map:
            continue

        data = pilot_map[display_name]
        is_mgmt = display_name in MANAGEMENT

        rotations, markers = consolidate_events(data["events"])

        # Management pilots are always ON if no rotation data
        if is_mgmt and not rotations:
            rotations = [{"start": "2026-01-01", "end": "2026-12-31", "status": "ON"}]

        pilots.append({
            "name": display_name,
            "fullName": data["fullName"],
            "isManagement": is_mgmt,
            "rotations": rotations,
            "events": markers
        })

    return {
        "year": 2026,
        "updated": datetime.now().astimezone().isoformat(),
        "source": "fleetplan",
        "pilots": pilots
    }


if __name__ == "__main__":
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            raw = json.load(f)
    else:
        raw = json.load(sys.stdin)

    result = process(raw)
    print(json.dumps(result, indent=2))
