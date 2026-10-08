# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy", "polars", "requests"]
# ///
"""Per-/24 timelines for the site.

    uv run scripts/retrieve.py
    uv run scripts/retrieve.py --since 2015-01

Monthly. From 2019-10-07, the NRO combined file (the 1st, or the next day a
file exists, plus latest). Before that, reconstruct.py from each RIR archive
and IANA's /8 table, back to 2001-05. Before 2001-05, one snapshot a year from
1982, from allocation dates.

One category per /24: registry, cc, status. Cached in scripts/.cache.
Output format is FORMAT below. Also writes data/events.json.
"""

import argparse
import functools
import gzip
import json
import struct
from concurrent.futures import ProcessPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import polars as pl
import requests

import events
import reconstruct

NRO_ARCHIVE = "https://ftp.ripe.net/pub/stats/ripencc/nro-stats"
# The file was called combined-stat until 2021; both names exist from 2022 on.
NRO_FILENAMES = ["nro-delegated-stats", "combined-stat"]
NRO_START = date(2019, 10, 7)
RIR_START = date(2001, 5, 1)
# Before any RIR published stats, one estimated snapshot per year (see reconstruct.py).
ESTIMATE_START = date(1982, 1, 1)

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data"
CACHE_DIR = Path(__file__).resolve().parent / ".cache"
CACHE_VERSION = 2  # bump when what's stored per snapshot changes
FIELDS = ["registry", "cc", "status"]
N_BLOCKS = 2**24  # number of /24s
EMPTY = 255  # /24 not listed in the stats
SAME = 254  # in a diff snapshot: unchanged since the previous snapshot

# FORMAT (all little-endian), gzip-compressed as a whole:
#   4s    magic b"IP4R"
#   u8    version (2)
#   u32   header length H
#   H     header, UTF-8 JSON: {field, names, snapshots: [{date, runs, source, rirs?}], sources, generated}
#   then per snapshot, in date order:
#     runs x u8      category id per run (index into names, 255 = unlisted, 254 = unchanged)
#     runs x varint  run length in /24s (unsigned LEB128)
# Each snapshot's runs tile all 2^24 /24s in order, so no addresses are stored.
# The first snapshot is complete; later ones use 254 for everything that didn't
# change, which collapses to a handful of long runs. Ids and lengths are stored
# as separate columns because each compresses far better on its own.
MAGIC = b"IP4R"
VERSION = 2


def download(url: str) -> str | None:
    response = requests.get(url, timeout=300)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.text


def parse_nro(text: str) -> list[reconstruct.Row]:
    rows = []
    for line in text.split("\n")[1:]:
        parts = line.split("|")
        # Summary lines ("nro|*|ipv4|*|N|summary") drop out here.
        if len(parts) >= 8 and parts[2] == "ipv4" and parts[1] != "*" and parts[4].isdigit():
            rows.append(parts[:8])
    return rows


def ipv4_frame(layers: list[list[reconstruct.Row]]) -> pl.DataFrame:
    """All rows, in painting order (layer, then address)."""
    df = pl.DataFrame(
        [row + [layer] for layer, rows in enumerate(layers) for row in rows],
        orient="row",
        schema=["registry", "cc", "type", "ip", "count", "date", "status", "opaque", "layer"],
    )
    ip = pl.col("ip").str.split_exact(".", 3)
    df = df.with_columns(
        (
            ip.struct.field("field_0").cast(pl.UInt64) * 2**24
            + ip.struct.field("field_1").cast(pl.UInt64) * 2**16
            + ip.struct.field("field_2").cast(pl.UInt64) * 2**8
            + ip.struct.field("field_3").cast(pl.UInt64)
        ).alias("ip_integer"),
        pl.col("count").cast(pl.UInt64),
        pl.col("status").str.to_lowercase(),
    ).with_columns(
        # Cover every /24 the row touches, so ranges smaller than /24 aren't lost.
        (pl.col("ip_integer") // 256).alias("block_start"),
        ((pl.col("ip_integer") + pl.col("count") + 255) // 256).alias("block_end"),
    ).filter(pl.col("block_end") <= N_BLOCKS).sort(["layer", "ip_integer"], maintain_order=True)
    return df


def rasterize(df: pl.DataFrame) -> dict:
    """Per field: one category id per /24 (most common first), run-length encoded."""
    starts = df["block_start"].to_numpy()
    ends = df["block_end"].to_numpy()
    grids, names, ids = {}, {}, {}
    for field in FIELDS:
        sizes = (
            df.group_by(field)
            .agg((pl.col("block_end") - pl.col("block_start")).sum().alias("n"))
            .sort(["n", field], descending=[True, False])
        )
        names[field] = sizes[field].to_list()
        lookup = {n: i for i, n in enumerate(names[field])}
        ids[field] = np.array([lookup[v] for v in df[field].to_list()], dtype=np.uint8)
        grids[field] = np.full(N_BLOCKS, EMPTY, dtype=np.uint8)
    for i in range(len(starts)):
        s, e = starts[i], ends[i]
        for field in FIELDS:
            grids[field][s:e] = ids[field][i]

    arrays = {}
    for field in FIELDS:
        run_ids, lengths = run_length_encode(grids[field])
        arrays[f"{field}_names"] = np.array(names[field])
        arrays[f"{field}_ids"] = run_ids
        arrays[f"{field}_lengths"] = lengths.astype(np.uint32)
    return arrays


def run_length_encode(grid: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    change = np.flatnonzero(grid[1:] != grid[:-1]) + 1
    starts = np.concatenate(([0], change))
    lengths = np.diff(np.concatenate((starts, [len(grid)])))
    return grid[starts], lengths


def cache_path(kind: str, day: str) -> Path:
    return CACHE_DIR / f"{kind}-{day}.npz"


def is_cached(path: Path) -> bool:
    if not path.exists():
        return False
    with np.load(path) as z:
        return "meta" in z and json.loads(str(z["meta"])).get("version") == CACHE_VERSION


def store(path: Path, layers: list, meta: dict) -> Path:
    arrays = rasterize(ipv4_frame(layers))
    meta = {**meta, "version": CACHE_VERSION}
    np.savez_compressed(path, meta=np.array(json.dumps(meta)), **arrays)
    return path


def nro_job(day: date) -> Path | None:
    """First NRO snapshot on or up to a week after `day`."""
    for offset in range(7):
        d = day + timedelta(days=offset)
        if d > date.today():
            break
        path = cache_path("nro", f"{d:%Y%m%d}")
        if is_cached(path):
            return path
        for name in NRO_FILENAMES:
            text = download(f"{NRO_ARCHIVE}/{d:%Y%m%d}/{name}")
            if text is not None:
                print(f"  {day}: NRO {d} ({len(text) / 1e6:.0f} MB)", flush=True)
                return store(path, [parse_nro(text)], {"date": f"{d:%Y-%m-%d}", "source": "nro"})
    print(f"  {day}: no NRO snapshot found, skipping", flush=True)
    return None


LATEST = CACHE_DIR / "nro-latest.txt"  # today's NRO file, also the source for reconstruct's backfill


@functools.cache
def current_rows() -> list[reconstruct.Row]:
    """Today's NRO records, parsed once per worker process."""
    return parse_nro(LATEST.read_text())


def nro_latest_job() -> Path:
    text = LATEST.read_text()
    serial = text.split("\n", 1)[0].split("|")[2]  # header: version|nro|serial|...
    d = datetime.strptime(serial, "%Y%m%d").date()
    path = cache_path("nro", serial)
    if is_cached(path):
        return path
    print(f"  latest: NRO {d}", flush=True)
    return store(path, [parse_nro(text)], {"date": f"{d:%Y-%m-%d}", "source": "nro"})


def rir_job(day: date, index: dict, table: dict) -> Path | None:
    path = cache_path("rir", f"{day:%Y%m%d}")
    if is_cached(path):
        return path
    result = reconstruct.snapshot(day, index, table, current_rows())
    if result is None:
        return None
    layers, meta = result
    print(f"  {day}: rebuilt from {', '.join(f'{k} {v}' for k, v in meta['rirs'].items())}", flush=True)
    return store(path, layers, {"date": f"{day:%Y-%m-%d}", **meta})


def months(start: date, end: date) -> list[date]:
    """The 1st of every month in [start, end), plus start itself if it isn't a 1st."""
    days, d = [start], date(start.year, start.month, 1)
    while True:
        d = date(d.year + d.month // 12, d.month % 12 + 1, 1)
        if d >= end:
            return days
        days.append(d)


def collect_snapshots(since: date) -> list[Path]:
    CACHE_DIR.mkdir(exist_ok=True)
    today = date.today()
    est_days = [date(y, 1, 1) for y in range(max(since, ESTIMATE_START).year, RIR_START.year + 1)
                if max(since, ESTIMATE_START) <= date(y, 1, 1) < RIR_START]
    rir_days = est_days + (months(max(since, RIR_START), NRO_START) if since < NRO_START else [])
    nro_days = months(max(since, NRO_START), today + timedelta(days=1))
    print(f"Collecting {len(rir_days)} rebuilt + {len(nro_days)} NRO monthly snapshots")

    text = download(f"{NRO_ARCHIVE}/latest/{NRO_FILENAMES[0]}")
    assert text is not None, "latest NRO snapshot missing"
    LATEST.write_text(text)
    index = reconstruct.archive_index(CACHE_DIR, NRO_START) if rir_days else {}
    table = reconstruct.iana_table() if rir_days else {}
    with ProcessPoolExecutor(max_workers=6) as pool:
        jobs = [pool.submit(rir_job, d, index, table) for d in rir_days]
        jobs += [pool.submit(nro_job, d) for d in nro_days]
        jobs.append(pool.submit(nro_latest_job))
        paths = [j.result() for j in jobs]
    # The latest file can coincide with a monthly one.
    return list(dict.fromkeys(p for p in paths if p is not None))


def varints(values: np.ndarray) -> bytes:
    out = bytearray()
    for v in values.tolist():
        while v >= 0x80:
            out.append((v & 0x7F) | 0x80)
            v >>= 7
        out.append(v)
    return bytes(out)


# The NRO file and the RIR files label some things differently. Normalise so a
# category doesn't appear to change just because the data source does.
ALIASES = {
    # NRO folds allocations (to LIRs/ISPs) and assignments (to end users) into
    # "assigned"; the RIR files still tell them apart. Only the union is comparable.
    "status": {"allocated": "assigned", "ietf": "reserved"},
    # Free space has no country in RIR extended files and "ZZ" in the NRO file.
    "cc": {"": "ZZ"},
}


def decode(snap, field: str) -> tuple[list[str], np.ndarray]:
    """A cached snapshot's category names for a field and its per-/24 grid of indices into them."""
    raw = [str(n) for n in snap[f"{field}_names"]]
    aliases = ALIASES.get(field, {})
    names = list(dict.fromkeys(aliases.get(n, n) for n in raw))
    remap = np.array([names.index(aliases.get(n, n)) for n in raw] + [EMPTY] * (256 - len(raw)), dtype=np.uint8)
    ids = snap[f"{field}_ids"]
    grid = np.repeat(remap[ids], snap[f"{field}_lengths"]).astype(np.uint8)
    if field == "status" and "available" in names:
        # IANA's free pool is "ianapool" in 2019 NRO files and rebuilt data, but
        # "available" with registry "iana" in later NRO files.
        reg_names, reg_grid = decode(snap, "registry")
        if "iana" in reg_names:
            if "ianapool" not in names:
                names.append("ianapool")
            pool = (grid == names.index("available")) & (reg_grid == reg_names.index("iana"))
            grid[pool] = names.index("ianapool")
    return names, grid


def ordered_names(field: str, loaded: list) -> list[str]:
    seen = []
    for snap in reversed(loaded):  # by size in the latest, then any that only occur earlier
        seen += [n for n in decode(snap, field)[0] if n not in seen]
    return seen


# Rebuilt files sometimes drop a block for a while (43/8 and 51/8 between ARIN
# and APNIC/RIPE, or a file missing rows). Keep the last category for up to
# this many snapshots, and only if it was listed two snapshots running.
# A block that shows up in exactly one snapshot (153.128/9 in 2005-04) is
# dropped. Returned space comes back as free, or stays gone. NRO is left as published.
MAX_HOLD = 36


def write_timelines(snapshots: list[Path]) -> None:
    loaded = [np.load(p) for p in snapshots]
    metas = [json.loads(str(z["meta"])) for z in loaded]
    order = sorted(range(len(loaded)), key=lambda i: metas[i]["date"])
    loaded, metas = [loaded[i] for i in order], [metas[i] for i in order]

    names = {f: ordered_names(f, loaded) for f in FIELDS}
    for f in FIELDS:
        assert len(names[f]) < SAME, f"too many {f} categories: {len(names[f])}"
    index = {f: {n: i for i, n in enumerate(names[f])} for f in FIELDS}

    def grids_of(snap) -> dict[str, np.ndarray]:
        grids = {}
        for f in FIELDS:
            snap_names, local = decode(snap, f)
            remap = np.array([index[f][n] for n in snap_names] + [EMPTY] * (256 - len(snap_names)), dtype=np.uint8)
            grids[f] = remap[local]
        return grids

    header_snaps, bodies, prev = [], {f: [] for f in FIELDS}, None
    upcoming = grids_of(loaded[0])
    held = np.zeros(N_BLOCKS, dtype=np.uint16)  # consecutive snapshots each /24 has been held
    listed = np.zeros(N_BLOCKS, dtype=np.uint16)  # consecutive snapshots each /24 was listed before that
    most_held = (0, "")
    for k, meta in enumerate(metas):
        grids = upcoming
        upcoming = grids_of(loaded[k + 1]) if k + 1 < len(loaded) else None
        rebuilt = lambda i: 0 <= i < len(metas) and metas[i]["source"] == "rir"

        if prev is not None and upcoming is not None and rebuilt(k - 1) and rebuilt(k) and rebuilt(k + 1):
            blip = (grids["registry"] != EMPTY) & (prev["registry"] == EMPTY) & (upcoming["registry"] == EMPTY)
            for f in FIELDS:
                grids[f][blip] = EMPTY

        if prev is not None and meta["source"] == "rir" and metas[k - 1]["source"] == "rir":
            gone = (grids["registry"] == EMPTY) & (prev["registry"] != EMPTY)
            hold = gone & (held < MAX_HOLD) & (listed >= 2)
            held = np.where(hold, held + 1, 0).astype(np.uint16)
            for f in FIELDS:
                grids[f][hold] = prev[f][hold]
            if hold.sum() > most_held[0]:
                most_held = (int(hold.sum()), meta["date"])
        else:
            held[:] = 0
        # Listing streaks: a held /24 keeps the streak it had when it disappeared.
        listed = np.where(grids["registry"] == EMPTY, 0, np.where(held > 0, listed, np.minimum(listed + 1, 60))).astype(np.uint16)

        entry = {"date": meta["date"], "source": meta["source"], "runs": {}}
        if "rirs" in meta:
            entry["rirs"] = meta["rirs"]
        for f in FIELDS:
            grid = grids[f]
            encoded = grid if prev is None else np.where(grid == prev[f], SAME, grid).astype(np.uint8)
            run_ids, run_lengths = run_length_encode(encoded)
            bodies[f] += [run_ids.astype(np.uint8).tobytes(), varints(run_lengths)]
            entry["runs"][f] = len(run_ids)
        header_snaps.append(entry)
        prev = grids
    for z in loaded:
        z.close()
    print(f"Held unlisted /24s: at most {most_held[0]:,} ({most_held[0] * 256 / 1e6:.1f}M addresses) on {most_held[1]}")

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for f in FIELDS:
        snaps = [{**e, "runs": e["runs"][f]} for e in header_snaps]
        header = json.dumps(
            {
                "field": f,
                "names": names[f],
                "snapshots": snaps,
                "sources": {
                    "nro": f"{NRO_ARCHIVE}/",
                    "rir": "Reconstructed from the RIR delegated-stats archives and the IANA IPv4 address space registry",
                },
                "generated": generated,
            },
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
        raw = MAGIC + struct.pack("<BI", VERSION, len(header)) + header + b"".join(bodies[f])
        path = OUT_DIR / f"{f}.bin.gz"
        # mtime=0 keeps output byte-identical when the data hasn't changed.
        path.write_bytes(gzip.compress(raw, compresslevel=9, mtime=0))
        runs = [s["runs"] for s in snaps]
        print(
            f"{path.relative_to(ROOT)}: {len(names[f])} categories, {len(runs)} snapshots, "
            f"median {int(np.median(runs[1:] or [0])):,} runs per diff, "
            f"{len(raw):,} B raw -> {path.stat().st_size:,} B gzip"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--since", default="1982-01", help="first month, YYYY-MM (default: 1982-01)")
    args = parser.parse_args()
    since = datetime.strptime(args.since, "%Y-%m").date()
    snapshots = collect_snapshots(since)
    OUT_DIR.mkdir(exist_ok=True)
    write_timelines(snapshots)
    events.write()


if __name__ == "__main__":
    main()
