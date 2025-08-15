
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

