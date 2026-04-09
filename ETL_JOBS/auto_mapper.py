"""
Auto-Mapper — strict column matching for Adapter Mapper.

Matching rules (in order):
  1. Exact lowercase match: source_col.lower() == target_col  -> confidence 1.0
  2. Known alias match: source_col.lower() in target aliases  -> confidence 0.9
  3. Unmatched: goes to unmapped_sources for human review

No fuzzy matching. No guessing. Human approval required.
"""

try:
    from ETL_JOBS.system_type_registry import SYSTEM_TYPES
except ImportError:
    from system_type_registry import SYSTEM_TYPES


def _detect_transform(ora_type, col_name):
    """Infer transform from Oracle data type + column name."""
    t = (ora_type or '').upper()
    n = (col_name or '').upper()

    if t == 'DATE' or 'TIMESTAMP' in t:
        if 'DATE' in n and 'TIME' not in n:
            return 'date'
        return 'timestamp'

    if n.startswith('HAS_') or n.startswith('IS_'):
        return 'boolean_yn'

    if t in ('CLOB', 'NCLOB', 'LONG'):
        return 'string_truncate'

    return 'direct'


def _build_alias_index(system_type_key):
    """
    Build a reverse index: {alias_lower: (std_table, target_col)} for fast lookup.
    Also includes exact column names as self-aliases.
    """
    st = SYSTEM_TYPES.get(system_type_key.upper())
    if not st:
        return {}, {}

    # alias_lower -> (std_table, target_col)
    alias_map = {}
    # target tables: {std_table: {col_name: col_def}}
    target_tables = {}

    for tbl_name, tbl_def in st["tables"].items():
        target_tables[tbl_name] = tbl_def["columns"]
        for col_name, col_def in tbl_def["columns"].items():
            # Self-alias (exact match)
            alias_map[col_name] = (tbl_name, col_name)
            # Known aliases
            for alias in col_def.get("aliases", []):
                alias_lower = alias.lower()
                if alias_lower not in alias_map:
                    alias_map[alias_lower] = (tbl_name, col_name)

    return alias_map, target_tables


def _score_table_match(source_table_name, std_table_name, std_def):
    """Score how well a source table matches a standard table. Higher = better."""
    src_words = set(source_table_name.upper().replace('.', '_').split('_'))
    tgt_words = set(std_table_name.upper().split('_'))
    # Remove noise words
    noise = {'MDB', 'STD', 'DIDB', 'ETL', 'MEDILINK', 'MEDISTORE', 'VIEW'}
    src_clean = src_words - noise
    tgt_clean = tgt_words - noise
    common = src_clean & tgt_clean
    return len(common)


def auto_map(dump_json, system_type_key):
    """
    Strict auto-mapper. Takes a schema dump dict and system type.
    Returns mapping JSON ready for human review.

    Args:
        dump_json: dict from schema discovery (has 'tables' list)
        system_type_key: 'PACS', 'RIS', 'LIS', or 'HIS'

    Returns:
        dict with 'tables' list in the Adapter Mapper format
    """
    alias_map, target_tables = _build_alias_index(system_type_key)
    if not target_tables:
        return {"tables": [], "notes": f"Unknown system type: {system_type_key}"}

    result_tables = []

    for src_table in dump_json.get('tables', []):
        src_name = src_table.get('name', '')
        src_schema = dump_json.get('schema_owner', '')
        full_source = f"{src_schema}.{src_name}" if src_schema else src_name

        # Find best matching standard table
        best_score = 0
        best_tbl = None
        for std_tbl_name, std_def in target_tables.items():
            score = _score_table_match(src_name, std_tbl_name, std_def)
            if score > best_score:
                best_score = score
                best_tbl = std_tbl_name

        # Also count how many columns match each target table (stronger signal)
        col_match_counts = {}
        for col_info in src_table.get('columns', []):
            src_lower = col_info.get('name', '').lower()
            if src_lower in alias_map:
                tbl = alias_map[src_lower][0]
                col_match_counts[tbl] = col_match_counts.get(tbl, 0) + 1

        # Pick table with most column matches, then fall back to name match
        if col_match_counts:
            best_by_cols = max(col_match_counts, key=col_match_counts.get)
            if col_match_counts[best_by_cols] >= 2:
                best_tbl = best_by_cols

        mapped_cols = []
        unmapped_src = []
        matched_targets = set()

        for col_info in src_table.get('columns', []):
            src_col = col_info.get('name', '')
            ora_type = col_info.get('type', '')
            src_lower = src_col.lower()
            transform = _detect_transform(ora_type, src_col)

            matched = False
            confidence = 0.0
            target_col = None
            notes = ''

            # 1. Check alias map (includes exact matches)
            if src_lower in alias_map:
                alias_tbl, alias_col = alias_map[src_lower]
                # Only use if it points to our best-matched table
                if best_tbl and alias_tbl == best_tbl:
                    target_col = alias_col
                    # Exact match = 1.0, alias = 0.90
                    if src_lower == alias_col:
                        confidence = 1.0
                        notes = 'exact match'
                    else:
                        confidence = 0.90
                        notes = f'alias: {src_col} -> {alias_col}'
                    matched = True
                    matched_targets.add(target_col)

            if matched:
                mapped_cols.append({
                    'source': src_col,
                    'target': target_col,
                    'confidence': confidence,
                    'transform': transform,
                    'notes': notes,
                })
            else:
                unmapped_src.append(src_col)

        # Unmapped targets
        unmapped_tgt = []
        if best_tbl and best_tbl in target_tables:
            for col_name in target_tables[best_tbl]:
                if col_name not in matched_targets and col_name != 'last_update':
                    unmapped_tgt.append(col_name)

        # Determine incremental key
        inc_key = None
        for col_info in src_table.get('columns', []):
            cn = col_info.get('name', '').upper()
            if cn.endswith('_DBID') or cn.endswith('_DB_UID') or cn == 'INSERT_TIME':
                inc_key = col_info['name']
                break

        result_tables.append({
            'source_table': full_source,
            'target_table': best_tbl,
            'incremental_key': inc_key,
            'columns': mapped_cols,
            'unmapped_sources': unmapped_src,
            'unmapped_targets': unmapped_tgt,
        })

    mapping = {
        'tables': result_tables,
        'notes': (
            f"Auto-mapped for system type {system_type_key}. "
            f"REVIEW REQUIRED: only exact matches and known aliases are mapped. "
            f"All other columns need manual review."
        ),
    }

    return mapping
