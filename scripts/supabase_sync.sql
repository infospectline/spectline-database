set statement_timeout = 0;
set lock_timeout = 0;

-- 1) Upsert do companies (dedupe stage podľa ico)
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
from (
  select distinct on (ico)
    ico, business_name, business_activity, sector, address,
    latitude, longitude, email, phone
  from public.companies_stage
  where ico is not null and business_name is not null
  order by ico
) s
on conflict (ico) do update
set
  business_name     = excluded.business_name,
  business_activity = excluded.business_activity,
  sector            = excluded.sector,
  address           = excluded.address,
  email     = coalesce(excluded.email, public.companies.email),
  phone     = coalesce(excluded.phone, public.companies.phone),
  latitude  = coalesce(excluded.latitude, public.companies.latitude),
  longitude = coalesce(excluded.longitude, public.companies.longitude),
  updated_at = now();

-- 2) vymaž neaktívne (čo nie je v stage)
delete from public.companies c
where not exists (
  select 1 from public.companies_stage s where s.ico = c.ico
);
