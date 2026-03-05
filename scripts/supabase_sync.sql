-- scripts/supabase_sync.sql
-- 1) Upsert (insert/update) aktívnych firiem zo stage do finálnej tabuľky
insert into public.companies (
  ico,
  business_name,
  business_activity,
  sector,
  address,
  latitude,
  longitude,
  email,
  phone,
  updated_at,
  created_at
)
select
  s.ico,
  s.business_name,
  s.business_activity,
  s.sector,
  s.address,
  s.latitude,
  s.longitude,
  s.email,
  s.phone,
  now(),
  now()
from public.companies_stage s
on conflict (ico) do update
set
  business_name     = excluded.business_name,
  business_activity = excluded.business_activity,
  sector            = excluded.sector,
  address           = excluded.address,

  -- enrichment nechaj existujúci, ak stage nemá hodnotu (NULL)
  email     = coalesce(excluded.email, public.companies.email),
  phone     = coalesce(excluded.phone, public.companies.phone),
  latitude  = coalesce(excluded.latitude, public.companies.latitude),
  longitude = coalesce(excluded.longitude, public.companies.longitude),

  updated_at = now();

delete from public.companies c
where not exists (
  select 1
  from public.companies_stage s
  where s.ico = c.ico
);
