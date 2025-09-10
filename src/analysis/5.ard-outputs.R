library(dadmtools)
library(dplyr)
source('src/utils/functions.R')
conn_list <- dadmtools::get_pg_conn_list()

repo_path <- 'C:/ard/FAIB_PROXY_THLB'
setwd(repo_path)

## !!! IMPORTANT MANUAL STEP - DONT MISS THIS: !!!
## move last years THLB Proxy to the archive folder
## S:\FOR\VIC\HTS\FAIB_DATA_FOR_DISTRIBUTION\THLB\THLB_Proxy\
## into S:\FOR\VIC\HTS\FAIB_DATA_FOR_DISTRIBUTION\THLB\THLB_Proxy\archive\2025

output_dir <- "S:\\FOR\\VIC\\HTS\\FAIB_DATA_FOR_DISTRIBUTION\\THLB\\THLB_Proxy"
## export the new metadata file:
file.copy(from = 'src\\analysis\\4.netdown.html', to = glue('{output_dir}\\provincial.thlb.netdown.and.documentation.html'))


## export the required raster exports for the ARD:
source('src/utils/GenerateTIFFS.R')

## export the required vector exports for the ARD:
## riparian
system(glue("ogr2ogr -overwrite -f \"FileGDB\" {output_dir}\\provincial_riparian_buffers.gdb PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}' password='{conn_list$password}'\"  -sql \"SELECT * FROM whse_vector.riparian_buffers\" -nlt MULTIPOLYGON -nln riparian_buffers"))

## pthlb
## create a temporary table of vector proxy thlb
query <- "DROP TABLE IF EXISTS public.provincial_pthlb;"
run_sql_r(query, conn_list)

query <- "CREATE TABLE public.provincial_pthlb AS
WITH first_dissolve AS (
SELECT
	ST_Union(st_buffer(b.geom, 50, 'endcap=square')) as geom,
	p.pthlb_net as pthlb_fact,
	p.version,
	p.man_unit
FROM
	whse.thlb_proxy_netdown p
JOIN
	whse.all_bc_gr_skey b USING (gr_skey)
GROUP BY
	p.pthlb_net,
	p.version,
	p.man_unit
	)
SELECT
	(ST_Dump(geom)).geom as geom,
	pthlb_fact,
	version,
	man_unit
FROM
	first_dissolve"
run_sql_r(query, conn_list)

## export to FAIB_DATA_FOR_DISTRIBUTION
system(glue("ogr2ogr -overwrite -f \"FileGDB\" {output_dir}\\provincial_pthlb.gdb PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}' password='{conn_list$password}'\"  -sql \"SELECT * FROM public.provincial_pthlb\" -nlt MULTIPOLYGON -nln provincial_pthlb"))

## drop the table as its not needed after exporting
query <- "DROP TABLE IF EXISTS public.provincial_pthlb;"
run_sql_r(query, conn_list)

## paflb
## create a temporary table of vector proxy aflb
query <- "DROP TABLE IF EXISTS public.provincial_paflb;"
run_sql_r(query, conn_list)

query <- "CREATE TABLE public.provincial_paflb AS
WITH first_dissolve AS (
SELECT
	ST_Union(st_buffer(b.geom, 50, 'endcap=square')) as geom,
	p.paflb as paflb_fact,
	p.version,
	p.man_unit
FROM
	whse.thlb_proxy_netdown p
JOIN
	whse.all_bc_gr_skey b USING (gr_skey)
GROUP BY
	p.paflb,
	p.version,
	p.man_unit
	)
SELECT
	(ST_Dump(geom)).geom as geom,
	paflb_fact,
	version,
	man_unit
FROM
	first_dissolve"
run_sql_r(query, conn_list)

## export to FAIB_DATA_FOR_DISTRIBUTION
system(glue("ogr2ogr -overwrite -f \"FileGDB\" {output_dir}\\provincial_paflb.gdb PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}' password='{conn_list$password}'\"  -sql \"SELECT * FROM public.provincial_paflb\" -nlt MULTIPOLYGON -nln provincial_paflb"))
## drop table as no longer needed after export
query <- "DROP TABLE IF EXISTS public.provincial_paflb;"
run_sql_r(query, conn_list)

## fmlb
## create a temporary table of vector fmlb
query <- "DROP TABLE IF EXISTS public.provincial_fmlb;"
run_sql_r(query, conn_list)
query <- "CREATE TABLE public.provincial_fmlb AS
WITH first_dissolve AS (
SELECT
	ST_Union(st_buffer(b.geom, 50, 'endcap=square')) as geom,
	p.fmlb,
	p.version,
	p.man_unit
FROM
	whse.thlb_proxy_netdown p
JOIN
	whse.all_bc_gr_skey b USING (gr_skey)
GROUP BY
	p.fmlb,
	p.version,
	p.man_unit
	)
SELECT
	(ST_Dump(geom)).geom as geom,
	fmlb,
	version,
	man_unit
FROM
	first_dissolve"

run_sql_r(query, conn_list)
## export to FAIB_DATA_FOR_DISTRIBUTION
system(glue("ogr2ogr -overwrite -f \"FileGDB\" {output_dir}\\provincial_fmlb.gdb PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}' password='{conn_list$password}'\"  -sql \"SELECT * FROM public.provincial_fmlb\" -nlt MULTIPOLYGON -nln provincial_fmlb"))
## drop table as no longer needed after export
query <- "DROP TABLE IF EXISTS public.provincial_fmlb;"
run_sql_r(query, conn_list)

## create a (non-compressed) SQL dump of the netdown table
system(glue('pg_dump --dbname=postgresql://{conn_list$user}:{conn_list$password}@{conn_list$host}:{conn_list$port}/{conn_list$dbname} --table=whse.thlb_proxy_netdown --format=plain --file thlb_proxy_netdown.sql'))
file.rename('thlb_proxy_netdown.sql', glue('{output_dir}\\thlb_proxy_netdown.sql'))
## after successful moving of the file, remove dump file
file.remove("thlb_proxy_netdown.sql")


## copying the latest THLB PROXY netdown table to the central db
## first move the netdown table into the public schema for ease of database transfer (ie. both databases have a public schema)
query <- "ALTER TABLE whse.thlb_proxy_netdown SET SCHEMA public;"
run_sql_r(query, conn_list)

## make a dumped compressed file
system(glue('pg_dump --dbname=postgresql://{conn_list$user}:{conn_list$password}@{conn_list$host}:{conn_list$port}/{conn_list$dbname} --table=public.thlb_proxy_netdown --format=custom --file thlb_proxy_netdown.sqlc'))

## put the netdown back into the whse schema
query <- "ALTER TABLE public.thlb_proxy_netdown SET SCHEMA whse;"
run_sql_r(query, conn_list)

## copy the proxy THLB netdown table over to the central db
## coordinate with Iaian about when you update the central db table so he knows when its updated
## central db admin credentials (get from Iaian):

cdb_user <-     ''
cdb_password <- ''
cdb_host <-     ''
cdb_dbname <-   ''
cdb_port <-     ''

system(glue('pg_restore -d postgresql://{cdb_user}:{cdb_password}@{cdb_host}:{cdb_port}/{cdb_dbname} thlb_proxy_netdown.sqlc'))

## after successful restore, remove dump file
file.remove("thlb_proxy_netdown.sqlc")

## connect to the central database as admir and move the table from the public to the prov_gr_skey schema
library(DBI)

con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = cdb_dbname,
  host     = cdb_host,
  port     = cdb_port,
  user     = cdb_user,
  password = cdb_password
)
dbSendQuery(con, "DROP TABLE IF EXISTS prov_grskey.thlb_proxy_netdown")
## move the table over to prov_grskey schema
dbSendQuery(con, "ALTER TABLE public.thlb_proxy_netdown SET SCHEMA prov_grskey")

## HDE Notes: ran Sept 10, 2025 - does not need to ran annually unless the base data changes. 
## exporting the riparian vectors with classes & buffer distance
# out_poly_gdb_path <- glue("{output_dir}\\provincial_riparian_classes_polygon.gdb")
# out_line_gdb_path <- glue("{output_dir}\\provincial_riparian_classes_line.gdb")
# ## write out 2 geodatabase - one with multipolygons and one with linestrings
# ## write out 2 as there have been errors when trying to write out different geometry types (ie. linestring & polygon) into the same gdb.
# ## as a workaround, write out a gdb for each geometry type

# ## rivers
# system(glue("ogr2ogr -overwrite -f \"FileGDB\" {out_poly_gdb_path} PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}'\" -nlt MULTIPOLYGON -nln fwa_rivers_poly -sql \"SELECT waterbody_poly_id::int, watershed_group_id::int, waterbody_type, gnis_name_1, fwa_watershed_code, local_watershed_code, watershed_group_code, left_right_tributary, feature_area_sqm, feature_length_m, riv.geom, riparian_class_reason, riparian_class, riparian_data_source FROM whse_vector.fwa_rivers_poly riv WHERE riparian_class IS NOT NULL\""))

# ## lakes
# system(glue("ogr2ogr -f \"FileGDB\" {out_poly_gdb_path} PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}'\" -nlt MULTIPOLYGON -nln fwa_lakes_poly -sql \"SELECT waterbody_poly_id::int, watershed_group_id::int, waterbody_type, gnis_name_1, fwa_watershed_code, local_watershed_code, watershed_group_code, left_right_tributary, feature_area_sqm, feature_length_m, lake.geom, riparian_class_reason, riparian_class, riparian_data_source FROM whse_vector.fwa_lakes_poly lake WHERE riparian_class IS NOT NULL\" -update"))

# ## wetlands
# system(glue("ogr2ogr -f \"FileGDB\" {out_poly_gdb_path} PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}'\" -nlt MULTIPOLYGON -nln fwa_wetlands_poly -sql \"SELECT waterbody_poly_id::int, watershed_group_id::int, waterbody_type, gnis_name_1, fwa_watershed_code, local_watershed_code, watershed_group_code, left_right_tributary, feature_area_sqm, feature_length_m, wet.geom, riparian_class_reason, riparian_class, riparian_data_source FROM whse_vector.fwa_wetlands_poly wet WHERE riparian_class IS NOT NULL\" -update"))

# ## streams
# system(glue("ogr2ogr -overwrite -f \"FileGDB\" {out_line_gdb_path} PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}'\" -nlt MULTILINESTRING -nln fwa_stream_networks_sp_modelled_habitat_potential -sql \"SELECT fid, linear_feature_id::int, fish_habitat_id, blue_line_key, watershed_key, gnis_name, fwa_watershed_code, watershed_group_code, downstream_route_measure, waterbody_key, fwa_fcode_label, observation_id, gradient_barrier_030_id,gradient_barrier_050_id,gradient_barrier_080_id,gradient_barrier_150_id,gradient_barrier_220_id,gradient_barrier_300_id,intermittent_id,fish_obstacle_point_id,obstruction_id,subsurfaceflow_id,fish_habitat,slope,slope_class,stream_order,stream_magnitude,stream.geom,channel_width,channel_width_source,community_watershed,riparian_class,riparian_class_reason,riparian_data_source FROM whse.modelled_habitat_potential stream WHERE riparian_class IS NOT NULL\" -update"))
