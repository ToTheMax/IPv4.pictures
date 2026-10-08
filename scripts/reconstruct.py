"""Snapshots before the NRO combined file exists (2019-10-07).

    apnic    2001-05-01  (2001 files store a prefix length, not a count)
    ripencc  2001-06-01  (old-format/ until 2004-01; "UK"; status "returned")
    lacnic   2003-10-15
    arin     2003-11-20
    afrinic  2005-02-18  (before that, the other RIRs listed African space)

Before a RIR's first file, rows come from that file and are kept when the
allocation date is on or before the snapshot. Undated legacy rows start at
the IANA date of their /8. Marked "estimated". Misses returns and transfers
from before the first file, and the RIR is whoever holds the space now.
AFRINIC is not estimated.

snapshot() takes the latest file within MAX_AGE (else MAX_CARRY), extended
if there is one. RIPE extended files from 2011-06 to 2012-01 leave cc blank
on about half the rows; those are filled from the regular file of that day.

IANA's /8 table underneath:
  reserved /8s stay reserved;
  /8s handed out later are the IANA pool;
  legacy /8s given to one org (Apple, Ford, US DoD, ...) are assigned from
  their IANA date. Other legacy /8s stay with the RIR records. Those IANA
  dates are mostly when the table was cleaned up;
  an RIR's /8 is its free pool only when that RIR's file is in the snapshot.

Under the RIR rows, blocks today's NRO file lists as allocated or assigned
are painted from the date recorded there. Legacy dates are often placeholders
(RIPE uses 1993-09-01 for 1500+ rows). Example: ARIN handed 149.146.0.0/16
(HOLY-NET) to RIPE in 2003, and RIPE's files only list it from 2009.

Special-purpose blocks (10/8, 172.16/12, 192.168/16, 100.64/10, ...) are
reserved from their allocation date, same as the NRO file. RIR files usually
omit them.

IANA only stores a /8's current designation, so a /8 that was legacy, returned,
then reallocated shows as pool until that date unless an RIR file lists it.
Overlaps between RIRs are not resolved; the last layer wins.
"""

import bz2
import csv
import gzip
import io
import json
import re
from datetime import date, timedelta
from pathlib import Path

import requests

RIRS = ["apnic", "arin", "ripencc", "lacnic", "afrinic"]
ARCHIVES = {
    "apnic": "https://ftp.apnic.net/stats/apnic/{year}/",
    "arin": "https://ftp.arin.net/pub/stats/arin/archive/{year}/",
    "ripencc": "https://ftp.ripe.net/pub/stats/ripencc/{year}/",
    "lacnic": "https://ftp.lacnic.net/pub/stats/lacnic/archive/{year}/",
    "afrinic": "https://ftp.afrinic.net/pub/stats/afrinic/{year}/",
}
FIRST_YEAR = {"apnic": 2001, "arin": 2003, "ripencc": 2003, "lacnic": 2003, "afrinic": 2005}
# Archives outside the per-year folders.
EXTRA_ARCHIVES = {"ripencc": ["https://ftp.ripe.net/ripe/stats/old-format/"]}
# RIRs whose records are estimated from their first file before it exists.
BACKCAST = ["apnic", "arin", "ripencc", "lacnic"]
WHOIS = {"whois.apnic.net": "apnic", "whois.arin.net": "arin", "whois.ripe.net": "ripencc",
         "whois.lacnic.net": "lacnic", "whois.afrinic.net": "afrinic"}
IANA_CSV = "https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.csv"
IANA_SPECIAL_CSV = "https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry-1.csv"
IANA_DESIGNATIONS = {"APNIC": "apnic", "ARIN": "arin", "RIPE NCC": "ripencc", "LACNIC": "lacnic", "AFRINIC": "afrinic"}

# Use a RIR's latest file from this far before the snapshot day. If there is
# none (the archives have gaps, e.g. AFRINIC 2011-01..05), carry an older one
# forward rather than showing its space as unlisted.
MAX_AGE = timedelta(days=45)
MAX_CARRY = timedelta(days=180)

FILE_RE = re.compile(
    r"^(?:delegated-(?P<rir>[a-z]+)(?P<ext>-extended)?-(?P<ymd>\d{8})"
    r"|(?P<old>apnic)-(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})"
    r"|(?P<oldripe>ripencc)\.(?P<ymd2>\d{8}))(?:\.gz|\.bz2)?$"
)

# One row per delegation, same columns as the NRO file:
# registry, cc, type, start ip, count, date, status, opaque (holder) id
Row = list[str]


def _listing(url: str, cache_key: str, cache_dir: Path, frozen: bool) -> list[str]:
    """File names in one archive folder. Folders that no longer change are cached."""
    path = cache_dir / "listings" / f"{cache_key}.json"
    if path.exists():
        return json.loads(path.read_text())
    response = requests.get(url, timeout=120)
    if response.status_code == 404:
        names = []
    else:
        response.raise_for_status()
        names = sorted(set(re.findall(r'href="([^"/?#]+)"', response.text)))
    if frozen:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(names))
    return names


def archive_index(cache_dir: Path, until: date) -> dict[str, list[tuple[date, bool, str]]]:
    """Per RIR: (file date, is extended, url), sorted by date."""
    index = {}
    for rir in RIRS:
        folders = [(ARCHIVES[rir].format(year=y), f"{rir}-{y}", y < date.today().year)
                   for y in range(FIRST_YEAR[rir], until.year + 1)]
        folders += [(url, f"{rir}-extra{i}", True) for i, url in enumerate(EXTRA_ARCHIVES.get(rir, []))]
        files = []
        for url, key, frozen in folders:
            for name in _listing(url, key, cache_dir, frozen):
                m = FILE_RE.match(name)
                if not m or (m["rir"] or m["old"] or m["oldripe"]) != rir:
                    continue
                ymd = m["ymd"] or m["ymd2"]
                d = date(int(ymd[:4]), int(ymd[4:6]), int(ymd[6:])) if ymd else date(int(m["y"]), int(m["m"]), int(m["d"]))
                files.append((d, bool(m["ext"]), url + name))
        index[rir] = sorted(files)
    return index


def choose(files: list[tuple[date, bool, str]], day: date) -> tuple[date, bool, str] | None:
    """Latest file on or before `day` within MAX_AGE (else MAX_CARRY); extended if there is one."""
    for age in (MAX_AGE, MAX_CARRY):
        window = [f for f in files if day - age <= f[0] <= day]
        extended = [f for f in window if f[1]]
        if window:
            return (extended or window)[-1]
    return None


def _download(url: str) -> str:
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    data = response.content
    # Decide by content, not name: some servers already undo the compression.
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    elif data[:3] == b"BZh":
        data = bz2.decompress(data)
    return data.decode("utf-8", "replace")


def parse_rir(text: str, rir: str) -> list[Row]:
    rows = []
    for line in text.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 7 or parts[2] != "ipv4" or parts[1] == "*" or not parts[4].isdigit():
            continue
        opaque = parts[7] if len(parts) > 7 else ""
        cc = {"UK": "GB"}.get(parts[1].upper(), parts[1].upper())  # old RIPE files use "UK"
        status = {"returned": "available"}.get(parts[6], parts[6])  # returned to the RIR's pool
        rows.append([rir, cc, "ipv4", parts[3], parts[4], parts[5].replace("-", ""), status, opaque])
    # The earliest APNIC files give a prefix length instead of an address count.
    if rows and all(int(r[4]) <= 32 for r in rows):
        for r in rows:
            r[4] = str(2 ** (32 - int(r[4])))
    return rows


def _month(text: str) -> date | None:
    year, _, month = (text or "").strip().partition("-")
    return date(int(year), int(month), 1) if year.isdigit() and month.isdigit() else None


def iana_table() -> dict[str, list[dict]]:
    """IANA's /8 table and its special-purpose blocks (only those of /24 or larger)."""
    text = requests.get(IANA_CSV, timeout=60).text
    table = []
    for rec in csv.DictReader(io.StringIO(text)):
        prefix = rec.get("Prefix", "")
        if not prefix.endswith("/8"):
            continue
        designation = rec.get("Designation", "")
        table.append({
            "octet": int(prefix.split("/")[0]),
            "rir": IANA_DESIGNATIONS.get(designation),
            # Legacy /8s given to one organisation, rather than administered by a RIR.
            "org": None if designation.startswith("Administered by") else designation,
            "whois": WHOIS.get(rec.get("WHOIS", "")),
            "since": _month(rec.get("Date")),
            "status": rec.get("Status [1]", "").strip(),
        })

    special = []
    text = requests.get(IANA_SPECIAL_CSV, timeout=60).text
    for rec in csv.DictReader(io.StringIO(text)):
        # "Address Block" can hold footnotes ("192.0.0.0/24 [2]") or several blocks.
        for block in re.findall(r"(\d+\.\d+\.\d+\.\d+)/(\d+)", rec.get("Address Block", "")):
            ip, bits = block[0], int(block[1])
            if bits <= 24 and _month(rec.get("Allocation Date")):
                special.append({"ip": ip, "count": 2 ** (32 - bits), "since": _month(rec.get("Allocation Date"))})
    return {"slash8": table, "special": special}


def iana_rows(table: list[dict], day: date, present: set[str]) -> list[Row]:
    rows = []
    for rec in table["slash8"]:
        ip, count = f"{rec['octet']}.0.0.0", str(2**24)
        if rec["status"] == "RESERVED":
            rows.append(["iana", "ZZ", "ipv4", ip, count, "", "reserved", "iana"])
        elif rec["status"] == "ALLOCATED" and rec["since"] and rec["since"] > day:
            rows.append(["iana", "ZZ", "ipv4", ip, count, "", "ianapool", "iana"])
        elif rec["status"] == "ALLOCATED" and rec["rir"] in present:
            rows.append([rec["rir"], "ZZ", "ipv4", ip, count, "", "available", rec["rir"]])
        elif rec["status"] == "LEGACY" and rec["org"] and rec["whois"] and rec["since"] and rec["since"] <= day:
            # The RIR records (painted on top) give the country where they list it.
            rows.append([rec["whois"], "ZZ", "ipv4", ip, count, "", "assigned", rec["org"]])
    return rows


def special_rows(table: dict, day: date) -> list[Row]:
    """Special-purpose blocks in use by `day`, as the NRO file lists them."""
    return [["iana", "ZZ", "ipv4", rec["ip"], str(rec["count"]), f"{rec['since']:%Y%m%d}", "reserved", "ietf"]
            for rec in table["special"] if rec["since"] <= day]


def fill_missing_cc(rows: list[Row], rir: str, file_date: date, index: dict) -> None:
    """Take country codes from the same day's regular file where the extended one has none."""
    delegated = [r for r in rows if r[6] in ("allocated", "assigned")]
    missing = [r for r in delegated if not r[1]]
    if len(missing) <= 0.01 * len(delegated):
        return
    regular = [f for f in index[rir] if f[0] == file_date and not f[1]]
    if not regular:
        return
    cc = {r[3]: r[1] for r in parse_rir(_download(regular[0][2]), rir) if r[1]}
    for r in missing:
        r[1] = cc.get(r[3], "")


def backfill_rows(current: list[Row], day: date) -> list[Row]:
    """Blocks in today's NRO file that were already allocated or assigned on `day`, by their recorded date."""
    cutoff = f"{day:%Y%m%d}"
    return [r for r in current if r[6] in ("allocated", "assigned") and r[5].strip("0") and r[5] <= cutoff]


def snapshot(day: date, index: dict, table: dict, current: list[Row]) -> tuple[list[list[Row]], dict] | None:
    """Layers of rows (painted in order: IANA base, today's records by date, estimates, the RIR
    files of the day, special-purpose blocks) and which files were used. `current` is today's
    NRO file, for the backfill."""
    chosen = {rir: choose(index[rir], day) for rir in RIRS}
    chosen = {rir: f for rir, f in chosen.items() if f}
    backcast = {rir: index[rir][0] for rir in BACKCAST if rir not in chosen and index[rir] and day < index[rir][0][0]}
    if not chosen and not backcast:
        return None
    estimated = []
    since = {rec["octet"]: rec["since"] for rec in table["slash8"]}
    for rir, (_, _, url) in backcast.items():
        cutoff = f"{day:%Y%m%d}"
        for r in parse_rir(_download(url), rir):
            if r[5].strip("0"):
                keep = r[5] <= cutoff
            else:  # undated legacy record: from its /8's IANA date
                first = since.get(int(r[3].split(".")[0]))
                keep = first is None or first <= day
            if keep:
                estimated.append(r)
    rir_rows = []
    for rir, (file_date, extended, url) in chosen.items():
        rows = parse_rir(_download(url), rir)
        if extended:
            fill_missing_cc(rows, rir, file_date, index)
        rir_rows += rows
    rirs = {rir: f"{d:%Y-%m-%d}" + (" extended" if ext else "") for rir, (d, ext, _) in chosen.items()}
    rirs |= {rir: f"{d:%Y-%m-%d} estimated" for rir, (d, _, _) in backcast.items()}
    meta = {"source": "rir" if chosen else "estimated", "rirs": {rir: rirs[rir] for rir in RIRS if rir in rirs}}
    layers = [iana_rows(table, day, set(chosen) | set(backcast)), backfill_rows(current, day),
              estimated, rir_rows, special_rows(table, day)]
    return layers, meta
