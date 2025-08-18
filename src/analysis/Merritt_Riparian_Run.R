
library(dadmtools)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()
dst_schema <- "whse"
vector_schema <- "whse_vector"
tsanumber <- 18

## requested_buffers:

l1a_buffer <- 10
l1b_buffer <- 10
l2_buffer  <- 25
l3_buffer  <- 22.5
l4_buffer  <- 22.5

w1_buffer <- 40
w2_buffer <- 25
w3_buffer <- 22.5
w4_buffer <- 22.5
w5_buffer <- 40

s1a_buffer <- 75
s1b_buffer <- 65
s2_buffer  <- 45
s3_buffer  <- 35
s4_buffer  <- 22.5
s5_buffer  <- 22.5
s6_buffer  <- 15


## rivers
query <- glue("create table public.fwa_rivers_poly_tsa{tsanumber} AS
SELECT
  waterbody_poly_id::int,
  watershed_group_id::int,
  waterbody_type,
  gnis_name_1,
  riv.geom,
  riparian_class_reason,
  riparian_class,
  riparian_data_source,
  CASE
  	WHEN riparian_class = 'S1A' THEN {s1a_buffer}
  	WHEN riparian_class = 'S1B' THEN {s1b_buffer}
  	WHEN riparian_class = 'S2'  THEN {s2_buffer}
  	WHEN riparian_class = 'S3'  THEN {s3_buffer}
  	WHEN riparian_class = 'S4'  THEN {s4_buffer}
  	WHEN riparian_class = 'S5'  THEN {s5_buffer}
  	WHEN riparian_class = 'S6'  THEN {s6_buffer}
  END as riparian_buffer_width_m,
  CASE
  	WHEN riparian_class = 'S1A' THEN ST_Buffer(riv.geom, {s1a_buffer})
  	WHEN riparian_class = 'S1B' THEN ST_Buffer(riv.geom, {s1b_buffer})
  	WHEN riparian_class = 'S2'  THEN ST_Buffer(riv.geom, {s2_buffer})
  	WHEN riparian_class = 'S3'  THEN ST_Buffer(riv.geom, {s3_buffer})
  	WHEN riparian_class = 'S4'  THEN ST_Buffer(riv.geom, {s4_buffer})
  	WHEN riparian_class = 'S5'  THEN ST_Buffer(riv.geom, {s5_buffer})
  	WHEN riparian_class = 'S6'  THEN ST_Buffer(riv.geom, {s6_buffer})
  END as riparian_buffer_geom
FROM
  {vector_schema}.fwa_rivers_poly riv
  JOIN (select ST_Union(geom) as geom from {vector_schema}.tsa_boundaries_2020 where tsa IN ({tsanumber})) tsa
  ON ST_Intersects(tsa.geom, riv.geom)
  WHERE riparian_class IS NOT NULL")

run_sql_r(query, conn_list)

query <- glue("create table public.fwa_lakes_poly_tsa{tsanumber} AS
SELECT
  waterbody_poly_id::int,
  watershed_group_id::int,
  gnis_name_1,
  fwa_watershed_code,
  local_watershed_code,
  lake.geom,
  riparian_class_reason,
  riparian_class,
  riparian_data_source,
  CASE
	WHEN riparian_class = 'L1A' THEN {l1a_buffer}
	WHEN riparian_class = 'L1B' THEN {l1b_buffer}
	WHEN riparian_class = 'L2'  THEN {l2_buffer}
	WHEN riparian_class = 'L3'  THEN {l3_buffer}
	WHEN riparian_class = 'L4'  THEN {l4_buffer}
	END AS riparian_buffer_width_m,
  CASE
	WHEN riparian_class = 'L1A' THEN ST_Buffer(lake.geom, {l1a_buffer})
	WHEN riparian_class = 'L1B' THEN ST_Buffer(lake.geom, {l1b_buffer})
	WHEN riparian_class = 'L2'  THEN ST_Buffer(lake.geom, {l2_buffer})
	WHEN riparian_class = 'L3'  THEN ST_Buffer(lake.geom, {l3_buffer})
	WHEN riparian_class = 'L4'  THEN ST_Buffer(lake.geom, {l4_buffer})
	END AS riparian_buffer_geom
FROM {vector_schema}.fwa_lakes_poly lake
JOIN (select ST_Union(geom) as geom from {vector_schema}.tsa_boundaries_2020 where tsa IN ({tsanumber})) tsa
ON ST_Intersects(tsa.geom, lake.geom) WHERE riparian_class IS NOT NULL")

run_sql_r(query, conn_list)

## wetlands
query <-  glue("create table public.fwa_wetlands_poly_tsa{tsanumber} AS
SELECT
waterbody_poly_id::int,
watershed_group_id::int,
waterbody_type,
gnis_name_1,
wet.geom,
riparian_class_reason,
riparian_class,
riparian_data_source,
CASE
	WHEN riparian_class = 'W1'  THEN {w1_buffer}
	WHEN riparian_class = 'W2'  THEN {w2_buffer}
	WHEN riparian_class = 'W3'  THEN {w3_buffer}
	WHEN riparian_class = 'W4'  THEN {w4_buffer}
	WHEN riparian_class = 'W5'  THEN {w5_buffer}
END AS riparian_buffer_width_m,
CASE
	WHEN riparian_class = 'W1'  THEN ST_Buffer(wet.geom, {w1_buffer})
	WHEN riparian_class = 'W2'  THEN ST_Buffer(wet.geom, {w2_buffer})
	WHEN riparian_class = 'W3'  THEN ST_Buffer(wet.geom, {w3_buffer})
	WHEN riparian_class = 'W4'  THEN ST_Buffer(wet.geom, {w4_buffer})
	WHEN riparian_class = 'W5'  THEN ST_Buffer(wet.geom, {w5_buffer})
END AS riparian_buffer_geom

FROM
whse_vector.fwa_wetlands_poly wet
JOIN (select ST_Union(geom) as geom from whse_vector.tsa_boundaries_2020 where tsa IN ({tsanumber})) tsa
ON ST_Intersects(tsa.geom, wet.geom) WHERE riparian_class IS NOT NULL")

run_sql_r(query, conn_list)

## streams
query <- glue("create table public.fwa_stream_networks_sp_modelled_habitat_potential_tsa{tsanumber} AS
SELECT
fid,
linear_feature_id::int,
fish_habitat_id,
gnis_name,
fish_habitat,
slope,
slope_class,
stream_order,
stream_magnitude,
stream.geom,
channel_width,
channel_width_source,
community_watershed,
riparian_class,
riparian_class_reason,
riparian_data_source,
CASE
	WHEN riparian_class = 'S1A' THEN {s1a_buffer}
	WHEN riparian_class = 'S1B' THEN {s1b_buffer}
	WHEN riparian_class = 'S2'  THEN {s2_buffer}
	WHEN riparian_class = 'S3'  THEN {s3_buffer}
	WHEN riparian_class = 'S4'  THEN {s4_buffer}
	WHEN riparian_class = 'S5'  THEN {s5_buffer}
	WHEN riparian_class = 'S6'  THEN {s6_buffer}
END as riparian_buffer_width_m,
CASE
	WHEN riparian_class = 'S1A' THEN ST_Buffer(stream.geom, {s1a_buffer})
	WHEN riparian_class = 'S1B' THEN ST_Buffer(stream.geom, {s1b_buffer})
	WHEN riparian_class = 'S2'  THEN ST_Buffer(stream.geom, {s2_buffer})
	WHEN riparian_class = 'S3'  THEN ST_Buffer(stream.geom, {s3_buffer})
	WHEN riparian_class = 'S4'  THEN ST_Buffer(stream.geom, {s4_buffer})
	WHEN riparian_class = 'S5'  THEN ST_Buffer(stream.geom, {s5_buffer})
	WHEN riparian_class = 'S6'  THEN ST_Buffer(stream.geom, {s6_buffer})
END as riparian_buffer_geom
FROM whse.modelled_habitat_potential stream
JOIN (select ST_Union(geom) as geom from whse_vector.tsa_boundaries_2020 where tsa IN ({tsanumber})) tsa
ON ST_Intersects(tsa.geom, stream.geom) WHERE NOT inside_fwa_polygon AND riparian_class IS NOT NULL")

run_sql_r(query, conn_list)


query <- glue("drop table if exists public.riparian_buffers{tsanumber}")
run_sql_r(query, conn_list)
query <- glue("create table public.riparian_buffers{tsanumber} AS
WITH a AS (
SELECT
riparian_buffer_geom
FROM
public.fwa_stream_networks_sp_modelled_habitat_potential_tsa{tsanumber}
UNION ALL
SELECT
riparian_buffer_geom
FROM
public.fwa_wetlands_poly_tsa{tsanumber}
UNION ALL
SELECT
riparian_buffer_geom
FROM
public.fwa_rivers_poly_tsa{tsanumber}
UNION ALL
SELECT
riparian_buffer_geom
FROM
public.fwa_lakes_poly_tsa{tsanumber}
)
SELECT
ST_Union(riparian_buffer_geom) as geom
FROM
a")

run_sql_r(query, conn_list)

spatial_query <- glue("SELECT geom from public.riparian_buffers{tsanumber}")
conn <- DBI::dbConnect(conn_list["driver"][[1]],
                       host     = conn_list["host"][[1]],
                       user     = conn_list["user"][[1]],
                       dbname   = conn_list["dbname"][[1]],
                       password = conn_list["password"][[1]],
                       port     = conn_list["port"][[1]])

vect <- st_cast(st_read(conn, query = spatial_query, crs = 3005), "MULTIPOLYGON")


## vect <- st_cast(st_read(dsn = gdb_path, layer = layer_name, crs = 3005), "MULTIPOLYGON")

## Current the template_tif & mask_tif are set to the provincial grids - change to TSA specific for faster results
pixel_weight(
  template_tif     = "//spatialfiles2.bcgov/archive/FOR/VIC/HTS/ANA/workarea/PROVINCIAL/bc_01ha_gr_skey.tif",
  mask_tif         = "//spatialfiles2.bcgov/archive/FOR/VIC/HTS/ANA/workarea/PROVINCIAL/BC_Boundary_Terrestrial.tif",
  vect             = vect,
  crop_extent      = c(273287.5,1870587.5,367787.5,1735787.5),
  dst_schema       = "public",
  dst_tbl          = "riparian_buffers18_gr_skey",
  pg_conn_param    = get_pg_conn_list(),
  raster_path      = "C:\\projects\\FAIB_PROXY_THLB\\data\\output\\riparian_buffers18_gr_skey.tif"
)

query <- "DROP TABLE IF EXISTS public.merritt_scenario_2025_08_18;"
run_sql_r(query, conn_list)
query <- "CREATE TABLE public.merritt_scenario_2025_08_18 AS
with merritt_alteration as (
  SELECT
  pthlb.gr_skey,
  man_unit,
  n01_fmlb,
  n02_ownership,
  n03_ownership,
  n04_nonfor,
  p05_linear_features,
  n06_parks,
  n07_wha,
  n08_misc,
  coalesce(rip.fact,0) as p09_riparian,
  n10_arch,
  n11_harvest_restrictions,
  -- recreate what the R netdown does by adjusting physically inoperable to be 1 when a cutblocks exists (harvest start year is pulled from consolidated cutblocks layer)
  CASE
  WHEN harvest_start_year_calendar IS NOT NULL THEN 0
  ELSE p12_phys_inop
  END AS p12_phys_inop,
  -- recreate what the R netdown does by adjusting merchantability to be NULL when a cutblocks exists (harvest start year is pulled from consolidated cutblocks layer)
  CASE
  WHEN harvest_start_year_calendar IS NOT NULL THEN NULL
  ELSE n13_non_merchantable
  END AS n13_non_merchantable,
  -- recreate what the R netdown does by adjusting non commercial to be NULL when a cutblocks exists (harvest start year is pulled from consolidated cutblocks layer)
  CASE
  WHEN harvest_start_year_calendar IS NOT NULL THEN NULL
  ELSE n14_non_commercial
  END AS n14_non_commercial,
  0.09588 as p15_future_retention
  from
  whse.thlb_proxy_netdown pthlb
  LEFT JOIN public.riparian_buffers18_gr_skey rip on rip.gr_skey = pthlb.gr_skey
  where man_unit = 'Merritt TSA'
)
SELECT
gr_skey,
p09_riparian as rmz,
p15_future_retention as wtra,
CASE
WHEN
n01_fmlb IS NULL AND
n02_ownership IS NULL AND
n03_ownership IS NULL AND
n04_nonfor IS NULL
THEN 1 * (1-p05_linear_features)
ELSE 0
END as paflb,
CASE
WHEN
-- categorical netdowns
n01_fmlb IS NULL AND
n02_ownership IS NULL AND
n03_ownership IS NULL AND
n04_nonfor IS NULL AND
n06_parks IS NULL AND
n07_wha IS NULL AND
n08_misc IS NULL AND
n10_arch IS NULL AND
n11_harvest_restrictions IS NULL AND
n13_non_merchantable IS NULL AND
n14_non_commercial IS NULL
-- proportional netdowns
THEN 1 * (1-p05_linear_features) * (1-p09_riparian) * (1-p12_phys_inop) * (1-p15_future_retention)
ELSE 0
END as pthlb
FROM
merritt_alteration"

run_sql_r(query, conn_list)

query <- "DROP TABLE IF EXISTS public.merritt_proxy_2025_08_18;"
run_sql_r(query, conn_list)
query <- "CREATE TABLE public.merritt_proxy_2025_08_18 AS
with merritt_alteration as (
  SELECT
  pthlb.gr_skey,
  man_unit,
  n01_fmlb,
  n02_ownership,
  n03_ownership,
  n04_nonfor,
  p05_linear_features,
  n06_parks,
  n07_wha,
  n08_misc,
  p09_riparian,
  n10_arch,
  n11_harvest_restrictions,
  -- recreate what the R netdown does by adjusting physically inoperable to be 1 when a cutblocks exists (harvest start year is pulled from consolidated cutblocks layer)
  CASE
  WHEN harvest_start_year_calendar IS NOT NULL THEN 0
  ELSE p12_phys_inop
  END AS p12_phys_inop,
  -- recreate what the R netdown does by adjusting merchantability to be NULL when a cutblocks exists (harvest start year is pulled from consolidated cutblocks layer)
  CASE
  WHEN harvest_start_year_calendar IS NOT NULL THEN NULL
  ELSE n13_non_merchantable
  END AS n13_non_merchantable,
  -- recreate what the R netdown does by adjusting non commercial to be NULL when a cutblocks exists (harvest start year is pulled from consolidated cutblocks layer)
  CASE
  WHEN harvest_start_year_calendar IS NOT NULL THEN NULL
  ELSE n14_non_commercial
  END AS n14_non_commercial,
  p15_future_retention
  from
  whse.thlb_proxy_netdown pthlb
  LEFT JOIN public.riparian_buffers18_gr_skey rip on rip.gr_skey = pthlb.gr_skey
  where man_unit = 'Merritt TSA'
)
SELECT
gr_skey,
p09_riparian as rmz,
p15_future_retention as wtra,
CASE
WHEN
n01_fmlb IS NULL AND
n02_ownership IS NULL AND
n03_ownership IS NULL AND
n04_nonfor IS NULL
THEN 1 * (1-p05_linear_features)
ELSE 0
END as paflb,
CASE
WHEN
-- categorical netdowns
n01_fmlb IS NULL AND
n02_ownership IS NULL AND
n03_ownership IS NULL AND
n04_nonfor IS NULL AND
n06_parks IS NULL AND
n07_wha IS NULL AND
n08_misc IS NULL AND
n10_arch IS NULL AND
n11_harvest_restrictions IS NULL AND
n13_non_merchantable IS NULL AND
n14_non_commercial IS NULL
-- proportional netdowns
THEN 1 * (1-p05_linear_features) * (1-p09_riparian) * (1-p12_phys_inop) * (1-p15_future_retention)
ELSE 0
END as pthlb
FROM
merritt_alteration"

run_sql_r(query, conn_list)

query <- "drop table if exists public.merritt_scenario_vect_2025_08_18;"
run_sql_r(query, conn_list)

query <- "create table public.merritt_scenario_vect_2025_08_18 as
with diss as (
  select
  ST_Union(st_buffer(b.geom, 50, 'endcap=square')) as geom,
  a.rmz,
  a.wtra,
  a.paflb,
  a.pthlb
  from
  public.merritt_scenario_2025_08_18 a
  join whse.all_bc_gr_skey b using (gr_skey)
  group by
  a.rmz,
  a.wtra,
  a.paflb,
  a.pthlb
)
SELECT
(ST_Dump(geom)).geom as geom,
rmz,
wtra,
paflb,
pthlb
FROM
diss;"
run_sql_r(query, conn_list)
## exported to gdb using qgis

query <- "drop table if exists public.merritt_proxy_vect_2025_08_18;"
run_sql_r(query, conn_list)

query <- "create table public.merritt_proxy_vect_2025_08_18 as
with diss as (
  select
  ST_Union(st_buffer(b.geom, 50, 'endcap=square')) as geom,
  a.rmz,
  a.wtra,
  a.paflb,
  a.pthlb
  from
  public.merritt_proxy_2025_08_18 a
  join whse.all_bc_gr_skey b using (gr_skey)
  group by
  a.rmz,
  a.wtra,
  a.paflb,
  a.pthlb
)
SELECT
(ST_Dump(geom)).geom as geom,
rmz,
wtra,
paflb,
pthlb
FROM
diss;"
run_sql_r(query, conn_list)
## exported to gdb using qgis

## output saved to: G:\!Workgrp\Analysts\!DADM\THLB_Proxy\tsa18\2025_08_18_proxy_scenario_results
## and: G:\!Workgrp\Analysts\!DADM\THLB_Proxy\tsa18\2025_08_18_proxy_results
