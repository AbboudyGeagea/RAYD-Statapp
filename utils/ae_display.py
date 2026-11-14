"""
utils/ae_display.py
-------------------
One definition of "what do we call this device on screen".

The RIS room name, then the AE title (operator, meeting of 2026-11-13: "room name from
the RIS instead of AE title everywhere"). The RIS is the master of the room name and
ETL_JOBS/etl_ris_modality.py writes it over every night:

    station_name     RIS MODALITY.STATION_NAME, written by ETL_JOBS/etl_ris_modality.py
    room_name        the same RIS value; on a device the RIS does not know (a gateway),
                     the name typed on the mapping tab, which writes room_name only

with the raw DICOM `aetitle` as the last resort.

`display_aetitle` (the "Display As" label typed in RAYD) is no longer read or written:
the operator dropped it the same day so that the RIS name wins. The column and its old
values stay in the table.

Report 25 resolved only `station_name` -- migration 0110's aetitle_display column,
rad_volume_matrix's by_aetitle, and report_cache.get_filter_options's aetitle_labels
each had their own copy of that same partial COALESCE. So on any install where the
room names were entered by hand on the mapping tab, or overridden on the floor plan,
or where etl_ris_modality.py never ran, station_name is empty and every one of those
paths silently fell back to showing the raw AE title.

Every screen takes its label from here (ae_display_sql); a copy of this chain anywhere else
is a bug, because the copies are how display_aetitle kept winning on some screens.

Deliberately NOT in the chain: `description`. It holds prose, not labels -- migration
0083 seeded it with strings like 'MRI 3T - Discovery 750W (RIS-registered AE: GEHCGEHC)'
which would make a poor axis label.
"""

AE_DISPLAY_COLUMNS = ("station_name", "room_name")


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


def ae_label_map(session):
    """{UPPER(TRIM(aetitle)): room name} for every device the AE map names, for pages that
    build their tables in Python. The lowest id wins, as in ae_label_join_sql."""
    from sqlalchemy import text
    rows = session.execute(text(
        f"SELECT aetitle, {ae_display_sql('m', 'NULL')} FROM aetitle_modality_map m "
        "WHERE aetitle IS NOT NULL ORDER BY id DESC")).fetchall()
    return {str(ae).strip().upper(): label for ae, label in rows if label}


def ae_label(labels, ae):
    """The room name of AE title `ae` from an ae_label_map(), else the AE title itself."""
    if ae is None:
        return None
    return labels.get(str(ae).strip().upper()) or ae


def ae_label_join_sql(ae_expr, alias="dev"):
    """A LEFT JOIN giving `<alias>.label`, the room name of the device `ae_expr` names
    (NULL when the AE map does not name it), for queries that hold an AE title and no
    join on aetitle_modality_map. LATERAL ... LIMIT 1: the map's UNIQUE is on the raw
    aetitle while this matches on UPPER(TRIM()), so 'ct1' and 'CT1' must not double a row.
    Select COALESCE(<alias>.label, <ae_expr>) to fall back to the AE title."""
    return (f"LEFT JOIN LATERAL (SELECT {ae_display_sql('d', 'NULL')} AS label "
            f"FROM aetitle_modality_map d "
            f"WHERE UPPER(TRIM(d.aetitle)) = UPPER(TRIM({ae_expr})) "
            f"ORDER BY d.id LIMIT 1) {alias} ON TRUE")
