# IPv4.pictures
Visualizations of the IPv4 address space using Hilbert Curves

See demo: 👉 https://ipv4.pictures 👈

## Data

`scripts/retrieve.py` builds the snapshots and writes one timeline per view to `data/*.bin.gz`: the first snapshot in full, then only the /24s that changed (format documented at the top of the script).

- **2019-10 onwards, monthly:** the [NRO combined delegated stats](https://ftp.ripe.net/pub/stats/ripencc/nro-stats/).
- **2001-05 to 2019-09, monthly:** rebuilt by `scripts/reconstruct.py` from each RIR's own delegated-stats archive (APNIC and RIPE NCC from 2001, LACNIC and ARIN from late 2003, AFRINIC from 2005) plus [IANA's /8 table](https://www.iana.org/assignments/ipv4-address-space). ARIN and LACNIC before their first file are estimated from the allocation dates in it.
- **1982 to 2001, yearly:** no RIR published stats yet. Estimated from the allocation dates in the oldest RIR files and the dates in IANA's /8 table (including legacy /8s given to single organisations).

Anything before Oct 2019 is approximate (returns and transfers before a RIR's first file, placeholder dates on legacy space). The page shows which source a date came from, and the Changes panel calls out jumps that are the files changing.

Views: **Registry**, **Country** and **Status**. The NRO file doesn't distinguish allocated from assigned space, so Status shows them as one throughout.

Processed snapshots are cached in `scripts/.cache/`, so re-runs only download new months.

```sh
uv run scripts/retrieve.py                  # everything since 1982
uv run scripts/retrieve.py --since 2015-01  # a shorter range
```

`scripts/events.py` writes `data/events.json`: dated events from IANA's IPv4 registries (special-purpose blocks such as private and shared address space, the first /8 per RIR, IANA's last /8s, and the footnoted history of 14/8). The page lists them in the Events tab and as dots on the timeline; clicking one jumps to its date and block.
