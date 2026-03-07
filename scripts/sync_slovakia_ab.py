import os
import time
import uuid
import psycopg2
from psycopg2.extras import execute_values

SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

# Local Postgres in GitHub runner (docker postgres:16 with password postgres)
LOCAL_DSN = "host=localhost port=5432 dbname=postgres user=postgres password=postgres"

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
PRINT_EVERY = int(os.environ.get("PRINT_EVERY", "10000"))

TABLE_A = "public.slovakia_companies_variant_a"
TABLE_B = "public.slovakia_companies_variant_b"

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
  NULL::float8 AS longitude,
  NULL::text AS email,
  NULL::text AS phone
FROM active_org o
JOIN ico_per_org i ON i.organization_id = o.id
LEFT JOIN name_per_org n ON n.organization_id = o.id
LEFT JOIN address_per_org adr ON adr.organization_id = o.id
LEFT JOIN activity_per_org act ON act.organization_id = o.id
LEFT JOIN rpo.main_activity_codes mac ON mac.id = o.main_activity_code_id
WHERE n.business_name IS NOT NULL;
"""

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
        # A
        cur.execute(f"select count(*) from {TABLE_A} where row_type='meta';")
        ca = cur.fetchone()[0]
        if ca == 0:
            cur.execute(
                f"insert into {TABLE_A} (row_type, is_active, batch_id, loaded_at, row_count) "
                f"values ('meta', true, %s, now(), 0);",
                (str(uuid.uuid4()),)
            )
        # B
        cur.execute(f"select count(*) from {TABLE_B} where row_type='meta';")
        cb = cur.fetchone()[0]
        if cb == 0:
            cur.execute(
                f"insert into {TABLE_B} (row_type, is_active, batch_id, loaded_at, row_count) "
                f"values ('meta', false, %s, now(), 0);",
                (str(uuid.uuid4()),)
            )

def choose_variants(a_active, b_active):
    """Return (active_table, inactive_table, active_label, inactive_label)."""
    if a_active is True and b_active is False:
        return TABLE_A, TABLE_B, "A", "B"
    if a_active is False and b_active is True:
        return TABLE_B, TABLE_A, "B", "A"
    # If both None/False/True -> invalid state
    raise RuntimeError(f"Invalid meta state: A.is_active={a_active}, B.is_active={b_active}. Exactly one must be true.")

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
                (str(batch_id), int(row_count))
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

def load_into_inactive(local_conn, supa_conn, target_table):
    """
    Stream rows from local RPO DB and insert into target_table as row_type='company'.
    Returns total inserted row count.
    """
    t0 = time.time()
    total = 0
    batch = []

    insert_sql = f"""
      insert into {target_table}
        (row_type, ico, business_name, business_activity, sector, address,
         latitude, longitude, email, phone)
      values %s
      on conflict (ico, row_type) do update
      set
        business_name = excluded.business_name,
        business_activity = excluded.business_activity,
        sector = excluded.sector,
        address = excluded.address,
        latitude = excluded.latitude,
        longitude = excluded.longitude,
        email = excluded.email,
        phone = excluded.phone,
        updated_at = now()
      where {target_table}.row_type = 'company';
    """

    # server-side cursor to avoid loading all into memory
    cur = local_conn.cursor(name="rpo_company_cursor")
    cur.itersize = 5000
    cur.execute(QUERY)

    def flush(rows):
      if not rows:
          return

      dedup = {}
      for r in rows:
          dedup[r[1]] = r  # key = ico, posledný vyhráva
  
      rows2 = list(dedup.values())
  
      with supa_conn.cursor() as c2:
          execute_values(c2, insert_sql, rows2, page_size=len(rows2))
      supa_conn.commit()

    for row in cur:
        ico, business_name, business_activity, sector, address, lat, lng, email, phone = row

        # safety: should already be true due to WHERE, but keep it safe
        if not ico or not business_name:
            continue

        batch.append((
          "company",
          ico,
          business_name,
          business_activity,
          sector,
          address,
          lat,
          lng,
          email,
          phone
        ))
        total += 1

        if len(batch) >= BATCH_SIZE:
            flush(batch)
            batch.clear()

        if total % PRINT_EVERY == 0:
            print(f"Loaded {total:,} rows into {target_table} in {time.time()-t0:.1f}s", flush=True)

    flush(batch)
    cur.close()

    print(f"DONE: Loaded {total:,} rows into {target_table} in {time.time()-t0:.1f}s", flush=True)
    return total

def main():
    t_start = time.time()
    batch_id = uuid.uuid4()

    local = psycopg2.connect(LOCAL_DSN)
    supa = psycopg2.connect(SUPABASE_DB_URL)
    supa.autocommit = False

    try:
        # Make sure meta exists
        ensure_meta_rows(supa)
        supa.commit()

        a_active, b_active = get_meta_flags(supa)
        active_table, inactive_table, active_label, inactive_label = choose_variants(a_active, b_active)

        print(f"ACTIVE = {active_label} ({active_table})", flush=True)
        print(f"TARGET = {inactive_label} ({inactive_table})", flush=True)

        # 1) Clear target companies (keep meta)
        print(f"Clearing target companies in {inactive_label}...", flush=True)
        delete_company_rows(supa, inactive_table)
        supa.commit()

        # 2) Load into target
        print("Loading new dataset into target...", flush=True)
        total_rows = load_into_inactive(local, supa, inactive_table)

        # 3) Atomic swap: flip meta flags
        print("Swapping active flags (atomic)...", flush=True)
        supa.commit()
        with supa:
            # transaction block
            set_meta_active(supa, inactive_table, True, batch_id=batch_id, row_count=total_rows)
            set_meta_active(supa, active_table, False)

        # 4) Clear old active companies (now inactive) to avoid storing duplicates
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
