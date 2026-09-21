"""
utils/ae_display.py
-------------------
One definition of "what do we call this device on screen".

aetitle_modality_map carries three name columns, filled by three different things:

    display_aetitle  manual override, set on the floor-plan admin screen
    room_name        RIS MODALITY.STATION_NAME -- and ALSO what a manual edit on the
                     mapping tab writes (it writes room_name only, never station_name)
    station_name     RIS MODALITY.STATION_NAME, written by ETL_JOBS/etl_ris_modality.py

with the raw DICOM `aetitle` as the last resort.

Report 25 resolved only `station_name` -- migration 0110's aetitle_display column,
rad_volume_matrix's by_aetitle, and report_cache.get_filter_options's aetitle_labels
each had their own copy of that same partial COALESCE. So on any install where the
room names were entered by hand on the mapping tab, or overridden on the floor plan,
or where etl_ris_modality.py never ran, station_name is empty and every one of those
paths silently fell back to showing the raw AE title.

The order here is the one routes/floor_plan_admin.py already established:
manual override wins, then the RIS room name, then the AE title. Both name columns are
checked because the RIS ETL writes both while a manual edit writes only room_name.

Deliberately NOT in the chain: `description`. It holds prose, not labels -- migration
0083 seeded it with strings like 'MRI 3T - Discovery 750W (RIS-registered AE: GEHCGEHC)'
which would make a poor axis label.
"""

AE_DISPLAY_COLUMNS = ("display_aetitle", "room_name", "station_name")


def ae_display_sql(map_alias="m", fallback="aetitle"):
    """SQL expression resolving a device to its display label.

    `map_alias` is the alias aetitle_modality_map is joined under; `fallback` is the
    caller's own last-resort expression (the raw AE title, however that call site
    spells it -- s.storing_ae, pps.performing_ae_title, m.aetitle...). Every name
    column is NULLIF'd on the trimmed value, so a row holding '' or '   ' falls
    through instead of rendering as a blank label.
    """
    parts = [f"NULLIF(BTRIM({map_alias}.{col}), '')" for col in AE_DISPLAY_COLUMNS]
    parts.append(fallback)
    return "COALESCE(" + ", ".join(parts) + ")"
