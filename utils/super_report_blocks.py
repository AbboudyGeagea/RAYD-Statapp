"""
utils/super_report_blocks.py — the Super Report's blocks (to-do items 42 and 43, 26 Nov 2026).

One list says what the page is made of: each block's key (the page's data-block, a preset's
"blocks", an email schedule's ticks, item 44's PDF sections), its label in plain words, and the
data sections of routes/super_report._collect_data it needs. A block the user hides is neither
queried nor written about: the analysis (utils/super_report_analysis.py) covers the blocks shown.

Sections not named by any block (kpis, orders, storage, volume, physicians, demographics, the
daily and modality series) are cheap and always computed; the slow ones run only for a block
that needs them.
"""

# (key, label, data sections it needs beyond the cheap ones)
BLOCKS = (
    ('briefing', 'Daily briefing', ()),                       # its own request (last 30/90 days, YTD)
    ('conflicts', 'Conflict patients', ()),                   # its own request
    ('summary', 'Executive summary', ()),                     # the analysis of the blocks shown
    ('kpis', 'Key figures', ('er_delayed',)),
    ('sites', 'Sites side by side', ()),                      # its own request, accounts with 2 sites
    ('volume', 'Volume and trends', ()),
    ('storage', 'Storage and images', ()),
    ('referrers', 'Top referring physicians', ()),
    ('orders', 'Orders and fulfilment', ()),
    ('tat', 'Turnaround time', ('tat',)),
    ('crn', 'Critical findings', ('crn',)),
    ('utilization', 'Device utilization', ('utilization',)),
    ('lead_time', 'Patient schedule lead time', ('lead_time',)),
    ('demographics', 'Patient demographics', ()),
    ('insights', 'Clinical insights', ()),
    ('radiologists', 'Reports per radiologist', ('rad_matrix', 'tat')),
    ('wet_reads', 'Wet reads per resident', ('resident_wetreads',)),
)

KEYS = tuple(k for k, _, _ in BLOCKS)
LABELS = {k: label for k, label, _ in BLOCKS}
_NEEDS = {k: needs for k, _, needs in BLOCKS}


def clean(keys):
    """The known block keys of keys, in the order given, each once; None or nothing given means
    every block, in the page's order."""
    out = []
    for k in keys or ():
        k = str(k).strip()
        if k in _NEEDS and k not in out:
            out.append(k)
    return out or list(KEYS)


def sections(keys):
    """The slow data sections the blocks keys need."""
    return {s for k in clean(keys) for s in _NEEDS[k]}
