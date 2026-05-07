"""
One-shot CD surf schema discovery.
Run from the project root:
    python ETL_JOBS/discover_cd_surf.py --host 192.168.x.x --port 1521 --sid CDSURF --user myuser --password mypass --owner MYSCHEMA

Results are printed to console AND saved to ETL_JOBS/schema_dumps/cd_surf_*.json
"""
import argparse
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Keywords that suggest CD/print-related tables
CD_KEYWORDS = {'CD', 'PRINT', 'BURN', 'MEDIA', 'DISC', 'EXPORT', 'JOB',
               'STUDY', 'PATIENT', 'EXAM', 'IMAGE', 'DICOM', 'UID', 'LOG', 'HISTORY'}


def _connect(host, port, sid, user, password):
    import oracledb
    dsn = oracledb.makedsn(host, int(port), sid=sid)
    return oracledb.connect(user=user, password=password, dsn=dsn)


def _discover(conn, owner):
    cur = conn.cursor()
    cur.execute("""
        SELECT c.table_name, c.column_name, c.data_type,
               c.nullable, c.column_id,
               NVL(t.num_rows, -1) AS num_rows
        FROM   all_tab_columns c
        LEFT JOIN all_tables t
               ON t.table_name = c.table_name AND t.owner = c.owner
        WHERE  c.owner = UPPER(:owner)
        ORDER  BY c.table_name, c.column_id
    """, {"owner": owner})
    rows = cur.fetchall()
    cur.close()

    tables = {}
    for table_name, col_name, data_type, nullable, col_id, num_rows in rows:
        if table_name not in tables:
            tables[table_name] = {
                "name": table_name,
                "rows": int(num_rows) if num_rows >= 0 else None,
                "columns": []
            }
        tables[table_name]["columns"].append({
            "name": col_name, "type": data_type,
            "nullable": nullable == 'Y', "pos": int(col_id)
        })
    return tables


def _relevance_score(table_name):
    name_parts = set(table_name.upper().replace('_', ' ').split())
    return len(name_parts & CD_KEYWORDS)


def main():
    ap = argparse.ArgumentParser(description="Discover CD surf Oracle schema")
    ap.add_argument('--host',     required=True)
    ap.add_argument('--port',     default='1521')
    ap.add_argument('--sid',      required=True)
    ap.add_argument('--user',     required=True)
    ap.add_argument('--password', required=True)
    ap.add_argument('--owner',    required=True, help="Oracle schema owner (e.g. CDSURF)")
    ap.add_argument('--top',      type=int, default=20, help="Show columns for top N relevant tables")
    args = ap.parse_args()

    print(f"\nConnecting to Oracle {args.host}:{args.port}/{args.sid} as {args.user}...")
    try:
        conn = _connect(args.host, args.port, args.sid, args.user, args.password)
        print("Connected.\n")
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    print(f"Discovering schema owner: {args.owner.upper()}...")
    tables = _discover(conn, args.owner)
    conn.close()

    if not tables:
        print(f"No tables found for owner '{args.owner}'. Check the --owner value.")
        sys.exit(1)

    # Sort: relevant tables first, then by row count desc
    sorted_tables = sorted(
        tables.values(),
        key=lambda t: (_relevance_score(t['name']), t['rows'] or 0),
        reverse=True
    )

    print(f"Found {len(tables)} tables total.\n")
    print("=" * 70)
    print(f"{'TABLE':<40} {'ROWS':>10}  SCORE")
    print("-" * 70)
    for t in sorted_tables:
        score = _relevance_score(t['name'])
        rows  = str(t['rows']) if t['rows'] is not None else '?'
        marker = ' ◀' if score > 0 else ''
        print(f"  {t['name']:<38} {rows:>10}  {'★' * score}{marker}")

    # Print columns for top relevant tables
    relevant = [t for t in sorted_tables if _relevance_score(t['name']) > 0][:args.top]
    if relevant:
        print(f"\n{'=' * 70}")
        print(f"COLUMN DETAIL — top {len(relevant)} relevant tables")
        print('=' * 70)
        for t in relevant:
            rows = str(t['rows']) if t['rows'] is not None else '?'
            print(f"\n  {t['name']}  ({rows} rows)")
            print(f"  {'-' * 50}")
            for c in t['columns']:
                null_marker = '' if c['nullable'] else ' NOT NULL'
                print(f"    {c['pos']:>3}. {c['name']:<35} {c['type']}{null_marker}")

    # Save full dump to JSON
    os.makedirs('ETL_JOBS/schema_dumps', exist_ok=True)
    from datetime import datetime
    ts       = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = f"ETL_JOBS/schema_dumps/cd_surf_{args.owner}_{ts}.json"
    with open(out_path, 'w') as f:
        json.dump({
            "host": args.host, "sid": args.sid, "owner": args.owner.upper(),
            "discovered_at": datetime.now().isoformat(),
            "table_count": len(tables),
            "tables": sorted_tables,
        }, f, indent=2, default=str)

    print(f"\nFull dump saved → {out_path}\n")


if __name__ == '__main__':
    main()
