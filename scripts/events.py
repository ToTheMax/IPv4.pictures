"""Dated events for the timeline, from IANA's IPv4 registries.

    uv run scripts/events.py

Special-purpose registry: blocks of /24 or larger, with allocation date,
termination date, and RFC. Address-space registry: multicast (224/4), the
first /8 listed per RIR, the month IANA handed out its last /8s, and dated
footnotes (14/8 recovered 2008, allocated to APNIC 2010).
"""

import ipaddress
import json
import re
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path

import requests

SPACE_XML = "https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.xml"
SPECIAL_XML = "https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xml"
SPACE_PAGE = "https://www.iana.org/assignments/ipv4-address-space"
SPECIAL_PAGE = "https://www.iana.org/assignments/iana-ipv4-special-registry"
OUT = Path(__file__).resolve().parent.parent / "data" / "events.json"
NS = {"r": "http://www.iana.org/assignments"}
RIRS = {"APNIC": "APNIC", "ARIN": "ARIN", "RIPE NCC": "RIPE NCC", "LACNIC": "LACNIC", "AFRINIC": "AFRINIC"}
MONTHS = ["January", "February", "March", "April", "May", "June", "July",
          "August", "September", "October", "November", "December"]

# Icon per kind of event, matched against the block's name.
ICONS = [
    ("private-use", "🏠"), ("shared address", "🔀"), ("loopback", "🔁"), ("link local", "🔗"),
    ("test-net", "📄"), ("documentation", "📄"), ("benchmark", "⏱️"), ("multicast", "📡"),
    ("future use", "🚧"), ("reserved", "🚧"), ("this network", "📍"), ("as112", "🕳️"),
    ("6to4", "🌉"), ("amt", "📡"), ("protocol assignments", "🧪"),
]


def icon_for(name: str) -> str:
    low = name.lower()
    return next((icon for key, icon in ICONS if key in low), "📌")


def text_of(el) -> str:
    """Element text with RFC cross-references written out ("RFC 6598")."""
    parts = [el.text or ""]
    for child in el:
        if child.tag.endswith("xref") and child.get("type") == "rfc":
            parts.append("[" + child.get("data", "").upper().replace("RFC", "RFC ") + "]")
        else:
            parts.append(text_of(child))
        parts.append(child.tail or "")
    return " ".join("".join(parts).split())


def month(text: str | None) -> str | None:
    m = re.match(r"^(\d{4})-(\d{2})", (text or "").strip())
    return f"{m[1]}-{m[2]}-01" if m else None


def block(prefix: str) -> dict:
    """Start and length in /24s, for the page to find and frame the block."""
    net = ipaddress.ip_network(prefix.strip(), strict=False)
    return {"prefix": str(net), "start": int(net.network_address) >> 8, "len": max(1, net.num_addresses >> 8)}


def rfcs(rec) -> list[str]:
    return sorted({x.get("data").upper().replace("RFC", "RFC ") for x in rec.iter(f"{{{NS['r']}}}xref")
                   if x.get("type") == "rfc"})


# Clearer names for a few registry entries.
RENAMES = {"Reserved": "Class E (reserved for future use)", "Deprecated (6to4 Relay Anycast)": "6to4 Relay Anycast"}


def special_events(root) -> list[dict]:
    """One event per (date, name, RFC): the three RFC 1918 blocks are one event."""
    grouped: dict[tuple, dict] = {}
    for rec in root.findall(".//r:record", NS):
        name = text_of(rec.find("r:name", NS)).strip('"')
        name = RENAMES.get(name, name)
        refs = rfcs(rec)
        for prefix in re.findall(r"\d+\.\d+\.\d+\.\d+/\d+", rec.findtext("r:address", "", NS)):
            if int(prefix.split("/")[1]) > 24:
                continue
            for kind, tag, icon in (("reserved", "allocation", icon_for(name)), ("deprecated", "termination", "❌")):
                if not (when := month(rec.findtext(f"r:{tag}", "", NS))):
                    continue
                ev = grouped.setdefault((when, name, kind), {
                    "date": when, "icon": icon, "rfc": refs, "blocks": [], "source": SPECIAL_PAGE,
                    "title": name if kind == "reserved" else f"{name} retired"})
                ev["blocks"].append(block(prefix))
    for (_, _, kind), ev in grouped.items():
        prefixes = ", ".join(b["prefix"] for b in ev["blocks"])
        via = f" ({', '.join(ev['rfc'])})" if ev["rfc"] else ""
        ev["detail"] = f"{prefixes} set aside by the IETF{via}" if kind == "reserved" else f"{prefixes} deprecated{via}"
    return list(grouped.values())


def space_events(root) -> list[dict]:
    events = []
    records = root.findall(".//r:record", NS)
    footnotes = {fn.get("anchor"): text_of(fn) for fn in root.findall(".//r:footnote", NS)}

    # Multicast, formerly Class D: 224/8-239/8, one event.
    multicast = [r for r in records if "multicast" in (r.findtext("r:designation", "", NS) or "").lower()]
    if multicast:
        when = min(month(r.findtext("r:date", "", NS)) or "9999" for r in multicast)
        events.append({"date": when, "icon": "📡", "title": "Multicast (Class D)",
                       "detail": "224.0.0.0/4 set aside for multicast", "rfc": ["RFC 5771"],
                       "blocks": [block("224.0.0.0/4")], "source": SPACE_PAGE})

    allocated = [(month(r.findtext("r:date", "", NS)), r.findtext("r:designation", "", NS), r.findtext("r:prefix", "", NS))
                 for r in records if r.findtext("r:status", "", NS) == "ALLOCATED"]
    allocated = [a for a in allocated if a[0]]
    # Each RIR's first /8.
    for designation, rir in RIRS.items():
        mine = sorted(a for a in allocated if a[1] == designation)
        if mine:
            when, _, prefix = mine[0]
            octet = int(prefix.split("/")[0])
            # IANA's table has these dates as it records them today; ARIN, for one,
            # only exists since 1997 (before that, InterNIC handed out the space).
            events.append({"date": when, "icon": "🏛️", "title": f"First /8 listed for {rir}",
                           "detail": f"{octet}.0.0.0/8, the earliest-dated /8 IANA's table lists for {rir}", "rfc": [],
                           "blocks": [block(f"{octet}.0.0.0/8")], "source": SPACE_PAGE})
    # IANA's free pool runs out: the last allocations, all in the same month.
    last = max(a[0] for a in allocated)
    final = sorted(a for a in allocated if a[0] == last)
    events.append({"date": last, "icon": "🏁", "title": "IANA hands out its last /8s",
                   "detail": "IANA's free pool is exhausted: " + ", ".join(
                       f"{int(p.split('/')[0])}/8 to {RIRS.get(d, d)}" for _, d, p in final),
                   "rfc": [], "blocks": [block(f"{int(p.split('/')[0])}.0.0.0/8") for _, _, p in final],
                   "source": SPACE_PAGE})

    # Dated history in the footnotes ("recovered in February 2008 ... allocated to APNIC in April 2010").
    for rec in records:
        prefix = rec.findtext("r:prefix", "", NS)
        for x in rec.findall(".//r:xref[@type='note']", NS):
            note = footnotes.get(x.get("data"), "")
            octet = int(prefix.split("/")[0])
            pattern = rf"(recovered|allocated to [A-Z ]+?|returned|reserved[^,.]*?) in ({'|'.join(MONTHS)}) (\d{{4}})"
            for action, mon, year in re.findall(pattern, note):
                when = f"{year}-{MONTHS.index(mon) + 1:02d}-01"
                title = f"{octet}/8 recovered by IANA" if action == "recovered" else f"{octet}/8 {action}"
                events.append({"date": when, "icon": "♻️" if action == "recovered" else "➡️", "title": title,
                               "detail": note, "rfc": [], "blocks": [block(f"{octet}.0.0.0/8")],
                               "source": SPACE_PAGE})
    return events


def build() -> list[dict]:
    space = ET.fromstring(requests.get(SPACE_XML, timeout=60).content)
    special = ET.fromstring(requests.get(SPECIAL_XML, timeout=60).content)
    events = space_events(space) + special_events(special)
    # The /8 table repeats special blocks that the special registry covers in more detail.
    seen, unique = set(), []
    for ev in sorted(events, key=lambda e: (e["date"], e["title"])):
        key = (ev["date"], ev["blocks"][0]["prefix"])
        if key not in seen:
            seen.add(key)
            unique.append(ev)
    return unique


def write() -> None:
    events = build()
    OUT.write_text(json.dumps({"generated": f"{date.today()}", "events": events}, ensure_ascii=False, indent=1))
    print(f"{OUT.relative_to(OUT.parent.parent)}: {len(events)} events, {events[0]['date']} – {events[-1]['date']}")


if __name__ == "__main__":
    write()
