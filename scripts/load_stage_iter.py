import os
import time
import psycopg2
from psycopg2.extras import execute_values

SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

LOCAL_DSN = "host=localhost port=5432 dbname=postgres user=postgres password=postgres"

BATCH = int(os.environ.get("BATCH_SIZE", "1000"))          # zapis po 1000 (môžeš zmeniť)
PRINT_EVERY = int(os.environ.get("PRINT_EVERY", "10000"))  # progress každých 10k

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
    n.organization_id, n.name AS business_name
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
  SELECT ea.organization_id,
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

def main():
    t0 = time.time()

    local = psycopg2.connect(LOCAL_DSN)
    supa = psycopg2.connect(SUPABASE_DB_URL)

    # 1) vyčisti stage
    with supa.cursor() as cur:
        cur.execute("truncate public.companies_stage;")
    supa.commit()

    # 2) server-side cursor (neťahá celé do RAM)
    cur = local.cursor(name="companies_cursor")
    cur.itersize = 5000
    cur.execute(QUERY)

    insert_sql = """
      insert into public.companies_stage
        (ico, business_name, business_activity, sector, address, latitude, longitude, email, phone)
      values %s
    """

    batch = []
    total = 0

    def flush(rows):
        if not rows:
            return
        with supa.cursor() as c2:
            execute_values(c2, insert_sql, rows, page_size=len(rows))
        supa.commit()

    for row in cur:
        batch.append(row)
        total += 1

        if len(batch) >= BATCH:
            flush(batch)
            batch.clear()

        if total % PRINT_EVERY == 0:
            dt = time.time() - t0
            print(f"Loaded {total:,} rows into companies_stage in {dt:.1f}s", flush=True)

    flush(batch)

    dt = time.time() - t0
    print(f"DONE: Loaded {total:,} rows into companies_stage in {dt:.1f}s", flush=True)

    cur.close()
    local.close()
    supa.close()

if __name__ == "__main__":
    main()
