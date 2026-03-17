import json
import os
import time
import uuid
import unicodedata
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

# Local Postgres in GitHub runner (docker postgres:16 with password postgres)
LOCAL_DSN = "host=localhost port=5432 dbname=postgres user=postgres password=postgres"

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
PRINT_EVERY = int(os.environ.get("PRINT_EVERY", "10000"))

TABLE_A = "public.slovakia_companies_variant_a"
TABLE_B = "public.slovakia_companies_variant_b"
TABLE_CACHE = "public.slovakia_company_cache"

COUNTRY_CODE = os.environ.get("COUNTRY_CODE", "sk").strip().lower()
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
CONVERTER_PATH = Path(
    os.environ.get(
        "CONVERTER_PATH",
        str(REPO_ROOT / "lib" / "server" / "keywords" / f"{COUNTRY_CODE}_converter_database.json"),
    )
)

# Query: active Slovak companies from imported RPO
QUERY = """
WITH active_org AS (
  SELECT o.id, o.main_activity_code_id
  FROM rpo.organizations o
  WHERE o.terminated_on IS NULL
    AND o.deleted_at IS NULL
    AND o.source_register IS NOT NULL
),
ico_per_org AS (
  SELECT e.organization_id, lpad(e.ipo::text, 8, '0') AS ico
  FROM rpo.organization_identifier_entries e
  WHERE e.ipo IS NOT NULL
    AND e.effective_to IS NULL
),
name_per_org AS (
  SELECT DISTINCT ON (n.organization_id)
    n.organization_id,
    n.name AS business_name
  FROM rpo.organization_name_entries n
  WHERE n.effective_to IS NULL
  ORDER BY n.organization_id, n.effective_from DESC
),
address_per_org AS (
  SELECT DISTINCT ON (a.organization_id)
    a.organization_id,
    coalesce(
      a.formatted_address,
      concat_ws(', ',
        nullif(trim(concat_ws(' ',
          a.street,
          nullif(
            trim(both '/' from concat_ws('/',
              nullif(a.reg_number::text, ''),
              nullif(a.building_number, '')
            )),
            ''
          )
        )), ''),
        nullif(trim(concat_ws(' ', a.postal_code, a.municipality)), ''),
        nullif(a.district, ''),
        nullif(a.country, '')
      )
    ) AS address
  FROM rpo.organization_address_entries a
  WHERE a.effective_to IS NULL
  ORDER BY a.organization_id, a.effective_from DESC
),
activity_per_org AS (
  SELECT
    ea.organization_id,
    string_agg(ea.description, '; ' ORDER BY ea.description) AS business_activity
  FROM rpo.organization_economic_activity_entries ea
  WHERE ea.effective_to IS NULL
  GROUP BY ea.organization_id
)
SELECT
  i.ico,
  n.business_name,
  act.business_activity,
  mac.name AS sector,
  adr.address,
  NULL::float8 AS latitude,
  NULL::float8 AS longitude
FROM active_org o
JOIN ico_per_org i ON i.organization_id = o.id
LEFT JOIN name_per_org n ON n.organization_id = o.id
LEFT JOIN address_per_org adr ON adr.organization_id = o.id
LEFT JOIN activity_per_org act ON act.organization_id = o.id
LEFT JOIN rpo.main_activity_codes mac ON mac.id = o.main_activity_code_id
WHERE n.business_name IS NOT NULL;
"""


def normalize_text(value: str) -> str:
    s = str(value or "")
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower()
    s = s.replace("&", " and ")
    for ch in ["'", '"', "(", ")", "/", ",", ".", ";", ":", "+", "!", "?", "[", "]", "{", "}", "|", "\\", "-"]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    return s.strip()


def normalize_keyword(value: str) -> str:
    return normalize_text(value).replace(" ", "_")


def unique_strings(values):
    seen = set()
    out = []
    for value in values:
        s = str(value or "").strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def load_keyword_rules():
    if not CONVERTER_PATH.exists():
        raise RuntimeError(f"Converter database not found: {CONVERTER_PATH}")

    with open(CONVERTER_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)

    rules_raw = raw if isinstance(raw, list) else raw.get("rules", [])
    parsed = []

    for rule in rules_raw:
        keyword = normalize_keyword(str(rule.get("keyword", "")))
        terms = [
            normalize_text(str(term or ""))
            for term in (rule.get("terms") or [])
            if str(term or "").strip()
        ]
        terms = unique_strings(terms)
        if not keyword or not terms:
            continue

        min_hits = rule.get("minHits", 1)
        try:
            min_hits = max(1, int(min_hits))
        except Exception:
            min_hits = 1

        parsed.append({
            "keyword": keyword,
            "terms": terms,
            "min_hits": min_hits,
        })

    if not parsed:
        raise RuntimeError(f"No valid keyword rules loaded from: {CONVERTER_PATH}")

    return parsed


KEYWORD_RULES = load_keyword_rules()


def convert_company_keywords(business_name, business_activity, sector, limit=30):
    inputs = [
        business_name,
        business_activity,
        sector,
    ]

    normalized_inputs = unique_strings(
        normalize_text(str(x or "")) for x in inputs if str(x or "").strip()
    )
    haystack = " ".join(normalized_inputs).strip()

    if not haystack:
        return []

    scored = []

    for rule in KEYWORD_RULES:
        hits = 0
        for term in rule["terms"]:
            if term and term in haystack:
                hits += 1

        if hits >= rule["min_hits"]:
            scored.append((rule["keyword"], hits))

    scored.sort(key=lambda x: x[1], reverse=True)

    out = unique_strings([keyword for keyword, _hits in scored])
    return out[: max(1, min(limit, 100))]


def get_meta_flags(conn):
    """Return (a_active: bool|None, b_active: bool|None)."""
    with conn.cursor() as cur:
        cur.execute(f"select is_active from {TABLE_A} where row_type='meta' limit 1;")
        ra = cur.fetchone()
        cur.execute(f"select is_active from {TABLE_B} where row_type='meta' limit 1;")
        rb = cur.fetchone()
    a_active = ra[0] if ra else None
    b_active = rb[0] if rb else None
    return a_active, b_active


def ensure_meta_rows(conn):
    """Ensure exactly one meta row exists in each table (if not, insert defaults)."""
    with conn.cursor() as cur:
        cur.execute("set statement_timeout = 0;")

        cur.execute(f"select count(*) from {TABLE_A} where row_type='meta';")
        ca = cur.fetchone()[0]
        if ca == 0:
            cur.execute(
                f"insert into {TABLE_A} (row_type, is_active, batch_id, loaded_at, row_count) "
                f"values ('meta', true, %s, now(), 0);",
                (str(uuid.uuid4()),)
            )

        cur.execute(f"select count(*) from {TABLE_B} where row_type='meta';")
        cb = cur.fetchone()[0]
        if cb == 0:
            cur.execute(
                f"insert into {TABLE_B} (row_type, is_active, batch_id, loaded_at, row_count) "
                f"values ('meta', false, %s, now(), 0);",
                (str(uuid.uuid4()),)
            )


def ensure_cache_table(conn):
    """
    Ensure the cache table exists.
    Cache holds only supplemental fields keyed by ICO.
    """
    with conn.cursor() as cur:
        cur.execute("set statement_timeout = 0;")
        cur.execute(
            f"""
            create table if not exists {TABLE_CACHE} (
              ico text primary key,
              email text null,
              phone text null,
              keywords text[] not null default '{{}}'::text[],
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );
            """
        )


def choose_variants(a_active, b_active):
    """Return (active_table, inactive_table, active_label, inactive_label)."""
    if a_active is True and b_active is False:
        return TABLE_A, TABLE_B, "A", "B"
    if a_active is False and b_active is True:
        return TABLE_B, TABLE_A, "B", "A"
    raise RuntimeError(
        f"Invalid meta state: A.is_active={a_active}, B.is_active={b_active}. Exactly one must be true."
    )


def delete_company_rows(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("set statement_timeout = 0;")
        cur.execute(f"delete from {table_name} where row_type='company';")


def set_meta_active(conn, table_name, is_active, batch_id=None, row_count=None):
    with conn.cursor() as cur:
        cur.execute("set statement_timeout = 0;")
        if is_active:
            cur.execute(
                f"""
                update {table_name}
                set is_active = true,
                    batch_id = %s,
                    loaded_at = now(),
                    row_count = %s,
                    updated_at = now()
                where row_type='meta';
                """,
                (str(batch_id), int(row_count)),
            )
        else:
            cur.execute(
                f"""
                update {table_name}
                set is_active = false,
                    updated_at = now()
                where row_type='meta';
                """
            )


def create_temp_cache_stage(conn):
    """
    Create a session-local staging table for cache sync.
    This lets us publish cache changes atomically together with the A/B swap.
    """
    with conn.cursor() as cur:
        cur.execute("set statement_timeout = 0;")
        cur.execute(
            """
            create temp table if not exists tmp_slovakia_company_cache_sync (
              ico text primary key,
              keywords text[] not null default '{}'::text[]
            ) on commit preserve rows;
            """
        )
        cur.execute("truncate table tmp_slovakia_company_cache_sync;")


def stage_cache_rows(conn, rows):
    """
    Stage cache rows into the temp table.
    rows: list[(ico, keywords)]
    """
    if not rows:
        return

    dedup = {}
    for ico, keywords in rows:
        if not ico:
            continue
        dedup[str(ico)] = (str(ico), keywords or [])

    rows2 = list(dedup.values())
    if not rows2:
        return

    sql = """
      insert into tmp_slovakia_company_cache_sync (ico, keywords)
      values %s
      on conflict (ico) do update
      set keywords = excluded.keywords;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows2, page_size=len(rows2))


def sync_cache_from_stage(conn):
    """
    Atomically sync staged ICO->keywords into the real cache table.
    Existing email/phone values are preserved.
    Obsolete cache rows are deleted.
    """
    with conn.cursor() as cur:
        cur.execute("set statement_timeout = 0;")

        cur.execute(
            f"""
            insert into {TABLE_CACHE} (ico, email, phone, keywords, created_at, updated_at)
            select
              s.ico,
              null::text as email,
              null::text as phone,
              coalesce(s.keywords, '{{}}'::text[]) as keywords,
              now(),
              now()
            from tmp_slovakia_company_cache_sync s
            on conflict (ico) do update
            set
              keywords = excluded.keywords,
              updated_at = now();
            """
        )

        cur.execute(
            f"""
            delete from {TABLE_CACHE} c
            where not exists (
              select 1
              from tmp_slovakia_company_cache_sync s
              where s.ico = c.ico
            );
            """
        )


def load_into_inactive(local_conn, supa_conn, target_table):
    """
    Stream rows from local RPO DB and insert into target_table as row_type='company'.
    Also build a temp cache stage keyed by ICO with computed keywords.
    Returns total inserted row count.
    """
    t0 = time.time()
    total = 0
    base_batch = []
    cache_batch = []

   insert_sql = f"""
      insert into {target_table}
        (row_type, ico, business_name, business_activity, sector, address,
         latitude, longitude)
      values %s
      on conflict (ico, row_type) do update
      set
        business_name = excluded.business_name,
        business_activity = excluded.business_activity,
        sector = excluded.sector,
        address = excluded.address,
        latitude = excluded.latitude,
        longitude = excluded.longitude,
        updated_at = now()
      where {target_table}.row_type = 'company';
    """

    cur = local_conn.cursor(name="rpo_company_cursor")
    cur.itersize = 5000
    cur.execute(QUERY)

    def flush(base_rows, cache_rows):
        if not base_rows:
            return

        dedup_base = {}
        dedup_cache = {}

        for row in base_rows:
            dedup_base[row[1]] = row  # key = ico

        for row in cache_rows:
            dedup_cache[row[0]] = row  # key = ico

        base_rows2 = list(dedup_base.values())
        cache_rows2 = list(dedup_cache.values())

        with supa_conn.cursor() as c2:
            execute_values(c2, insert_sql, base_rows2, page_size=len(base_rows2))

        stage_cache_rows(supa_conn, cache_rows2)
        supa_conn.commit()

    for row in cur:
        ico, business_name, business_activity, sector, address, lat, lng = row

        if not ico or not business_name:
            continue

        keywords = convert_company_keywords(
            business_name=business_name,
            business_activity=business_activity,
            sector=sector,
            limit=30,
        )

       base_batch.append(
            (
                "company",
                ico,
                business_name,
                business_activity,
                sector,
                address,
                lat,
                lng,
            )
        )

        cache_batch.append(
            (
                ico,
                keywords,
            )
        )

        total += 1

        if len(base_batch) >= BATCH_SIZE:
            flush(base_batch, cache_batch)
            base_batch.clear()
            cache_batch.clear()

        if total % PRINT_EVERY == 0:
            print(
                f"Loaded {total:,} rows into {target_table} in {time.time() - t0:.1f}s",
                flush=True,
            )

    flush(base_batch, cache_batch)
    cur.close()

    print(f"DONE: Loaded {total:,} rows into {target_table} in {time.time() - t0:.1f}s", flush=True)
    return total


def main():
    t_start = time.time()
    batch_id = uuid.uuid4()

    local = psycopg2.connect(LOCAL_DSN)
    supa = psycopg2.connect(SUPABASE_DB_URL)
    supa.autocommit = False

    try:
        ensure_cache_table(supa)
        ensure_meta_rows(supa)
        create_temp_cache_stage(supa)
        supa.commit()

        a_active, b_active = get_meta_flags(supa)
        active_table, inactive_table, active_label, inactive_label = choose_variants(a_active, b_active)

        print(f"ACTIVE = {active_label} ({active_table})", flush=True)
        print(f"TARGET = {inactive_label} ({inactive_table})", flush=True)
        print(f"CACHE  = {TABLE_CACHE}", flush=True)
        print(f"DICT   = {CONVERTER_PATH}", flush=True)

        # 1) Clear target companies (keep meta)
        print(f"Clearing target companies in {inactive_label}...", flush=True)
        delete_company_rows(supa, inactive_table)
        supa.commit()

        # 2) Load into target + stage cache rows session-locally
        print("Loading new dataset into target and staging cache...", flush=True)
        total_rows = load_into_inactive(local, supa, inactive_table)

        # 3) Atomic publish:
        #    - sync staged cache to real cache
        #    - flip active flags
        print("Publishing cache + swapping active flags atomically...", flush=True)
        supa.commit()
        with supa:
            sync_cache_from_stage(supa)
            set_meta_active(supa, inactive_table, True, batch_id=batch_id, row_count=total_rows)
            set_meta_active(supa, active_table, False)

        # 4) Clear old active companies (now inactive) to avoid duplicates
        print(f"Clearing old companies in {active_label} (now inactive)...", flush=True)
        delete_company_rows(supa, active_table)
        supa.commit()

        dt = time.time() - t_start
        print(f"SUCCESS: batch_id={batch_id} rows={total_rows:,} total_time={dt:.1f}s", flush=True)

    finally:
        try:
            local.close()
        except Exception:
            pass
        try:
            supa.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
