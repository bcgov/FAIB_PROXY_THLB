library(dadmtools)
library(dplyr)
source('src/utils/functions.R')
## one time runs are commented out to help with rerunning code
conn_list <- dadmtools::get_pg_conn_list()
dst_schema <- "whse"
vector_schema <- "whse_vector"

## WETLANDS
## Create new fields in the `{dst_schema}.fwa_wetlands_poly` layer for population
query <- glue("ALTER TABLE {vector_schema}.fwa_wetlands_poly DROP COLUMN IF EXISTS riparian_class;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_wetlands_poly ADD COLUMN IF NOT EXISTS riparian_class varchar(2);")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_wetlands_poly DROP COLUMN IF EXISTS riparian_class_reason;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_wetlands_poly ADD COLUMN IF NOT EXISTS riparian_class_reason varchar(200);")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_wetlands_poly DROP COLUMN IF EXISTS riparian_data_source;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_wetlands_poly ADD COLUMN IF NOT EXISTS riparian_data_source varchar(200);")
run_sql_r(query, conn_list)

## 48 (1) Wetlands have the following riparian classes:
## (a) W1, if the wetland is greater than 5 ha in size;"_
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829


print(glue('Processing W1 wetlands'))
query <- glue("UPDATE {vector_schema}.fwa_wetlands_poly SET
riparian_class = 'W1',
riparian_class_reason = 'wetland is greater than 5 ha in size',
riparian_data_source = 'FWA area'
WHERE feature_area_sqm/10000 > 5")
run_sql_r(query, conn_list)

## Calculate W2 wetland class:
##
## > _"(b) W2, if the wetland is not less than 1 ha and not more than 5 ha in size and is in one of the following biogeoclimatic zones or subzones:
##     (i)   Ponderosa Pine; (zone = 'pp')
##     (ii)  Bunch Grass; (zone = 'BP')
##     (iii) Interior Douglas-fir, very dry hot, very dry warm or very dry mild;  (zone = 'IDF' and subzone in ('xh', 'xw', 'xm'))
##     (iv)  Coastal Douglas-fir;  (zone = 'CDF')
##     (v)   Coastal Western Hemlock, very dry maritime, dry maritime or dry submaritime; (zone = 'CWH' and subzone in ('xm', 'dm', 'ds'))"_
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829


print(glue('Processing W2 wetlands'))
query <- glue("WITH clipped AS (
SELECT
	waterbody_poly_id,
	sum(ST_Area(ST_Intersection(wetland.geom, bec.geom))) as intersect_area,
	ST_Area(wetland.geom) as wetland_area
FROM
	{vector_schema}.bec_biogeoclimatic_poly bec
JOIN
	{vector_schema}.fwa_wetlands_poly wetland
ON
	ST_Intersects(bec.geom, wetland.geom)
AND
	(bec.zone in ('PP', 'BG', 'CDF')
OR
	(bec.zone = 'IDF' and bec.subzone IN ('xh', 'xw', 'xm'))
OR
	(bec.zone = 'CWH' and bec.subzone IN ('xm', 'dm', 'ds')))
AND
	wetland.feature_area_sqm/10000 <= 5 AND wetland.feature_area_sqm/10000 > 1
GROUP BY
	waterbody_poly_id, ST_Area(wetland.geom)
), w2 as (
SELECT
	waterbody_poly_id
FROM
	clipped
WHERE
	(intersect_area/wetland_area) > 0.5 -- Include when the majority of the wetland area (I.e., > 50%) overlaps with the bec zone/subzone, otherwise exclude
)
UPDATE
	{vector_schema}.fwa_wetlands_poly wet
SET
	riparian_class = 'W2',
	riparian_class_reason = 'wetland is not less than 1 ha and not more than 5 ha in size and is in one of the following biogeoclimatic zones or subzones PP, BG, CDF, IDF (xh, xw or xm), CWG (xm, dm or ds)',
	riparian_data_source = 'FWA area; WHSE_FOREST_VEGETATION.BEC_BIOGEOCLIMATIC_POLY'
FROM
	w2
WHERE
	w2.waterbody_poly_id = wet.waterbody_poly_id")
run_sql_r(query, conn_list)


## Calculate W3 wetland class:
##
## > _"(c) W3, if the wetland is not less than 1 ha and not more than 5 ha in size and is in a biogeoclimatic zone or subzone other than one referred to in paragraph (b)"_
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829

print(glue('Processing W3 wetlands'))
query <- glue("UPDATE
	{vector_schema}.fwa_wetlands_poly
SET
	riparian_class = 'W3',
  riparian_class_reason = 'wetland is not less than 1 ha and not more than 5 ha in size and is in a biogeoclimatic zone or subzone other than PP, BG, CDF, IDF (xh, xw or xm), CWG (xm, dm or ds)',
  riparian_data_source = 'FWA area; WHSE_FOREST_VEGETATION.BEC_BIOGEOCLIMATIC_POLY'
WHERE
	feature_area_sqm/10000 <= 5
AND
	feature_area_sqm/10000 > 1
AND
	riparian_class IS NULL")
run_sql_r(query, conn_list)


## Calculate W4 wetland class:
##
## > _"(d) W4, if the wetland is
## (i) not less than 0.25 ha and less than 1 ha in size and is in a biogeoclimatic zone or subzone referred to in paragraph (b) (i), (ii) or (iii), or
## (ii) not less than 0.5 ha and less than 1 ha in size and is in a biogeoclimatic zone or subzone referred to in paragraph (b) (iv) or (v)."_
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829


print(glue('Processing W4 wetlands'))
query <- glue("WITH clipped AS (
SELECT
	waterbody_poly_id,
	sum(ST_Area(ST_Intersection(wetland.geom, bec.geom))) as intersect_area,
	ST_Area(wetland.geom) as wetland_area
FROM
	{vector_schema}.bec_biogeoclimatic_poly bec
JOIN
	{vector_schema}.fwa_wetlands_poly wetland
ON
	ST_Intersects(bec.geom, wetland.geom)
AND
	(bec.zone in ('PP', 'BG')
OR
	(bec.zone = 'IDF' and bec.subzone IN ('xh', 'xw', 'xm')))
AND
	wetland.feature_area_sqm/10000 <= 1 AND wetland.feature_area_sqm/10000 > 0.25
GROUP BY
	waterbody_poly_id, ST_Area(wetland.geom)
), w4 as (
SELECT
	waterbody_poly_id
FROM
	clipped
WHERE
	(intersect_area/wetland_area) > 0.5 -- Include when the majority of the wetland area (I.e., > 50%) overlaps with the bec zone/subzone, otherwise exclude
)
UPDATE
	{vector_schema}.fwa_wetlands_poly wet
SET
	riparian_class = 'W4',
	riparian_class_reason = 'not less than 0.25 ha and less than 1 ha in size and is in a biogeoclimatic zone or subzone PP, BG, IDF (xh, xw, xm)',
	riparian_data_source = 'FWA area; WHSE_FOREST_VEGETATION.BEC_BIOGEOCLIMATIC_POLY'
FROM
	w4
WHERE
	w4.waterbody_poly_id = wet.waterbody_poly_id")
run_sql_r(query, conn_list)

query <- glue("WITH clipped AS (
SELECT
	waterbody_poly_id,
	sum(ST_Area(ST_Intersection(wetland.geom, bec.geom))) as intersect_area,
	ST_Area(wetland.geom) as wetland_area
FROM
	{vector_schema}.bec_biogeoclimatic_poly bec
JOIN
	{vector_schema}.fwa_wetlands_poly wetland
ON
	ST_Intersects(bec.geom, wetland.geom)
AND
	(bec.zone in ('CDF')
OR
	(bec.zone = 'CWH' and bec.subzone IN ('xm', 'dm', 'ds')))
AND
	wetland.feature_area_sqm/10000 <= 1 AND wetland.feature_area_sqm/10000 > 0.5
GROUP BY
	waterbody_poly_id, ST_Area(wetland.geom)
), w4 as (
SELECT
	waterbody_poly_id
FROM
	clipped
WHERE
	(intersect_area/wetland_area) > 0.5 -- Include when the majority of the wetland area (I.e., > 50%) overlaps with the bec zone/subzone, otherwise exclude
)
UPDATE
	{vector_schema}.fwa_wetlands_poly wet
SET
	riparian_class = 'W4',
	riparian_class_reason = 'not less than 0.25 ha and less than 1 ha in size and is in a biogeoclimatic zone or subzone CDF, CWH (xm, dm, ds)',
	riparian_data_source = 'FWA area; WHSE_FOREST_VEGETATION.BEC_BIOGEOCLIMATIC_POLY'
FROM
	w4
WHERE
	w4.waterbody_poly_id = wet.waterbody_poly_id")
run_sql_r(query, conn_list)

## Calculate W5 wetland class:
##
## > _"(2) Despite subsection (1), an area is to be treated as a single wetland with a riparian class of W5 if
##   (a)the area contains
##     (i)   two or more W1 wetlands located within 100 m of each other,
##     (ii)  a W1 wetland and one or more non-W1 wetlands, all of which are within 80 m of each other, or
##     (iii) two or more non-W1 wetlands located within 60 m of each other, and
##   (b)the combined size of the wetlands, excluding the upland areas, is 5 ha or larger."_
##
## As mentioned in section: References and resources > Wetland additional notes - further clarification states that a W5 class is a wetland complex meaning there are 2 or more individual wetlands with overlapping RMAs. A W1 has an RMA of 50. To have two of them overlap, you would need a distance of less than 100m between them. W2, W3, and W4 wetlands have RMAs of 30. (Source: Lisa Nordin, OCF, MOF) As such - for the analysis, buffers based on the wetland classification were created:
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829


query <- glue("ALTER TABLE {vector_schema}.fwa_wetlands_poly ADD COLUMN IF NOT EXISTS geom_buffer geometry(MultiPolygon, 3005);")
run_sql_r(query, conn_list)
query <- glue("UPDATE
  {vector_schema}.fwa_wetlands_poly
SET
  geom_buffer = CASE
                WHEN riparian_class = 'W1' THEN ST_Buffer(geom, 50)
                WHEN riparian_class IN ('W2', 'W3', 'W4') THEN ST_Buffer(geom, 30)
                END;")
run_sql_r(query, conn_list)
query <- glue("DROP INDEX IF EXISTS {vector_schema}.fwa_wetlands_poly_geom_buffer_idx;")
run_sql_r(query, conn_list)
query <- glue("CREATE INDEX fwa_wetlands_poly_geom_buffer_idx ON {vector_schema}.fwa_wetlands_poly USING gist(geom_buffer);")
run_sql_r(query, conn_list)
query <- glue("ANALYZE {vector_schema}.fwa_wetlands_poly")
run_sql_r(query, conn_list)


## identify any overlapping buffered polygons:
## SQL source: https://gis.stackexchange.com/questions/243565/check-whether-table-has-overlapping-polygons-in-postgis
query <- glue("SELECT
	a.waterbody_poly_id::integer as waterbody_poly_id,
	b.waterbody_poly_id::integer as overlapping_waterbody_poly_id
FROM
	{vector_schema}.fwa_wetlands_poly a
JOIN
	{vector_schema}.fwa_wetlands_poly b ON
   (a.geom_buffer && b.geom_buffer AND ST_Relate(a.geom_buffer, b.geom_buffer, '2********'))
WHERE
	a.waterbody_poly_id != b.waterbody_poly_id
AND
	a.riparian_class IS NOT NULL
AND
	b.riparian_class IS NOT NULL")
w5_candidates <-  sql_to_df(query, conn_list)

## walk through the resultant table of identified polygon ids that have overlap and identify groups of overlapping polygons to assess the second W5 criteria (I.e. area > 5 ha)
w5_results <- group_overlapping_ids(w5_candidates, 'waterbody_poly_id', 'overlapping_waterbody_poly_id')

## read temporary table into pg
df_to_pg('w5_temp_table', w5_results, conn_list)

## ensure the join is clean by checking to see if you have duplicates
## the following query will fail is duplicates exist in waterbody_poly_id field
query <- "ALTER TABLE public.w5_temp_table ADD PRIMARY KEY (waterbody_poly_id);"
run_sql_r(query, conn_list)

query <- glue("WITH area_greater_than_5 AS (
SELECT
  grouping_id,
  SUM(feature_area_sqm/10000)
FROM
  public.w5_temp_table
JOIN
  {vector_schema}.fwa_wetlands_poly
USING
  (waterbody_poly_id)
GROUP BY
  grouping_id
HAVING
  SUM(feature_area_sqm/10000) >= 5
), w5s AS (
SELECT
	waterbody_poly_id
FROM
	area_greater_than_5
JOIN
	public.w5_temp_table
USING
	(grouping_id)
)
UPDATE
	{vector_schema}.fwa_wetlands_poly
SET
	riparian_class = 'W5',
	riparian_class_reason = '2 or more W1 within 100m, a W1 and >= 1 non-W1s (not including non-classifieds) within 80m, 2 or more non-W1s (not including non-classifieds) within 60m AND combined size is >= 5ha',
	riparian_data_source = 'FWA area; WHSE_FOREST_VEGETATION.BEC_BIOGEOCLIMATIC_POLY'
FROM
	w5s
WHERE
	w5s.waterbody_poly_id = {vector_schema}.fwa_wetlands_poly.waterbody_poly_id")
run_sql_r(query, conn_list)

query <- "DROP TABLE IF EXISTS public.w5_temp_table;"
run_sql_r(query, conn_list)

## LAKES
## Create new fields in the `thlb_proxy.fwa_lakes_poly` layer for population

query <- glue("ALTER TABLE {vector_schema}.fwa_lakes_poly DROP COLUMN IF EXISTS riparian_class;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_lakes_poly ADD COLUMN IF NOT EXISTS riparian_class varchar(3);")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_lakes_poly DROP COLUMN IF EXISTS riparian_class_reason;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_lakes_poly ADD COLUMN IF NOT EXISTS riparian_class_reason varchar(200);")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_lakes_poly DROP COLUMN IF EXISTS riparian_data_source;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_lakes_poly ADD COLUMN IF NOT EXISTS riparian_data_source varchar(200);")
run_sql_r(query, conn_list)

## > _"49 (1) Lakes have the following riparian classes:
## (a) L1-A, if the lake is 1000 ha or greater in size;"_
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829

print(glue('Processing L1A lakes'))
query <- glue("UPDATE {vector_schema}.fwa_lakes_poly
SET
	riparian_class = 'L1A',
	riparian_class_reason = 'if the lake is 1000 ha or greater in size',
	riparian_data_source = 'FWA area'
WHERE
	feature_area_sqm/10000 >= 1000")
run_sql_r(query, conn_list)

## > _"(b) L1-B, if
##  (i)  the lake is greater than 5 ha but less than 1 000 ha in size, or
##  (ii) the minister designates the lake as L1-B;"_
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829

print(glue('Processing L1B lakes'))
query <- glue("UPDATE {vector_schema}.fwa_lakes_poly
SET
	riparian_class = 'L1B',
	riparian_class_reason = 'the lake is greater than 5 ha but less than 1000 h',
	riparian_data_source = 'FWA area'
WHERE
	feature_area_sqm/10000 < 1000
AND
	feature_area_sqm/10000 >= 5")
run_sql_r(query, conn_list)


## Calculate L2 lake class:
##
## > _"(c)L2, if the lake is not less than 1 ha and not more than 5 ha in size and is located in a biogeoclimatic zones or subzone that is
##     (i)   Ponderosa Pine; (zone = 'pp')
##     (ii)  Bunch Grass; (zone = 'BP')
##     (iii) Interior Douglas-fir, very dry hot, very dry warm or very dry mild;  (zone = 'IDF' and subzone in ('xh', 'xw', 'xm'))
##     (iv)  Coastal Douglas-fir;  (zone = 'CDF')
##     (v)   Coastal Western Hemlock, very dry maritime, dry maritime or dry submaritime; (zone = 'CWH' and subzone in ('xm', 'dm', 'ds'))"_
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829

print(glue('Processing L2 lakes'))
query <- glue("WITH clipped AS (
SELECT
	waterbody_poly_id,
	sum(ST_Area(ST_Intersection(lake.geom, bec.geom))) as intersect_area,
	ST_Area(lake.geom) as lake_area
FROM
	{vector_schema}.bec_biogeoclimatic_poly bec
JOIN
	{vector_schema}.fwa_lakes_poly lake
ON
	ST_Intersects(bec.geom, lake.geom)
AND
	(bec.zone in ('PP', 'BG', 'CDF')
OR
	(bec.zone = 'IDF' and bec.subzone IN ('xh', 'xw', 'xm'))
OR
	(bec.zone = 'CWH' and bec.subzone IN ('xm', 'dm', 'ds')))
AND
	lake.feature_area_sqm/10000 <= 5 AND lake.feature_area_sqm/10000 > 1
GROUP BY
	waterbody_poly_id, ST_Area(lake.geom)
), w2 as (
SELECT
	waterbody_poly_id
FROM
	clipped
WHERE
	(intersect_area/lake_area) > 0.5 -- Include when the majority of the lake area (I.e., > 50%) overlaps with the bec zone/subzone, otherwise exclude
)
UPDATE
	{vector_schema}.fwa_lakes_poly wet
SET
	riparian_class = 'L2',
	riparian_class_reason = 'lake is not less than 1 ha and not more than 5 ha in size and is located in a biogeoclimatic zones or subzone PP, BG, CDF, IDF (xh, xw, xm), CWH (xm, dm, ds)',
	riparian_data_source = 'FWA area; WHSE_FOREST_VEGETATION.BEC_BIOGEOCLIMATIC_POLY'
FROM
	w2
WHERE
	w2.waterbody_poly_id = wet.waterbody_poly_id")
run_sql_r(query, conn_list)

## Calculate L3 lake class:
##
##  _"(d)L3, if the lake is not less than 1 ha and not more than 5 ha in size and is in a biogeoclimatic zone or subzone other than one referred to in paragraph (c);"_
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829

print(glue('Processing L3 lakes'))
query <- glue("UPDATE
				{vector_schema}.fwa_lakes_poly
		  SET
		  	riparian_class = 'L3',
				riparian_class_reason = 'lake is not less than 1 ha and not more than 5 ha in size and is in a biogeoclimatic zone or subzone other than PP, BG, CDF, IDF (xh, xw, xm), CWH (xm, dm, ds)',
				riparian_data_source = 'FWA area; WHSE_FOREST_VEGETATION.BEC_BIOGEOCLIMATIC_POLY'
		  WHERE
		  		feature_area_sqm/10000 <= 5
		  AND
		  		feature_area_sqm/10000 > 1
		  AND
		  		riparian_class IS NULL")
run_sql_r(query, conn_list)

## Calculate L4 lake class (there is no L5 class):

## > _"(e)L4, if the lake is
## (i) not less than 0.25 ha and not more than 1 ha in size and is in a biogeoclimatic zone or subzone referred to in paragraph (c) (i), (ii) or (iii), or
## (ii) not less than 0.5 ha and not more than 1 ha in size and is in a biogeoclimatic zone or subzone referred to in paragraph (c) (iv) or (v)."_
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829

print(glue('Processing L4 lakes'))
query <- glue("WITH clipped AS (
SELECT
	waterbody_poly_id,
	sum(ST_Area(ST_Intersection(lake.geom, bec.geom))) as intersect_area,
	ST_Area(lake.geom) as lake_area
FROM
	{vector_schema}.bec_biogeoclimatic_poly bec
JOIN
	{vector_schema}.fwa_lakes_poly lake
ON
	ST_Intersects(bec.geom, lake.geom)
AND
	(bec.zone in ('PP', 'BG')
OR
	(bec.zone = 'IDF' and bec.subzone IN ('xh', 'xw', 'xm')))
AND
	lake.feature_area_sqm/10000 <= 1 AND lake.feature_area_sqm/10000 > 0.25
GROUP BY
	waterbody_poly_id, ST_Area(lake.geom)
), l4 as (
SELECT
	waterbody_poly_id
FROM
	clipped
WHERE
	(intersect_area/lake_area) > 0.5 -- Include when the majority of the lake area (I.e., > 50%) overlaps with the bec zone/subzone, otherwise exclude
)
UPDATE
	{vector_schema}.fwa_lakes_poly wet
SET
	riparian_class = 'L4',
	riparian_class_reason = 'not less than 0.25 ha and less than 1 ha in size and is in a biogeoclimatic zone or subzone PP, BG, IDF (xh, xw, xm)',
	riparian_data_source = 'FWA area; WHSE_FOREST_VEGETATION.BEC_BIOGEOCLIMATIC_POLY'
FROM
	l4
WHERE
	l4.waterbody_poly_id = wet.waterbody_poly_id")
run_sql_r(query, conn_list)


query <- glue("WITH clipped AS (
SELECT
	waterbody_poly_id,
	sum(ST_Area(ST_Intersection(lake.geom, bec.geom))) as intersect_area,
	ST_Area(lake.geom) as lake_area
FROM
	{vector_schema}.bec_biogeoclimatic_poly bec
JOIN
	{vector_schema}.fwa_lakes_poly lake
ON
	ST_Intersects(bec.geom, lake.geom)
AND
	(bec.zone in ('CDF')
OR
	(bec.zone = 'CWH' and bec.subzone IN ('xm', 'dm', 'ds')))
AND
	lake.feature_area_sqm/10000 <= 1 AND lake.feature_area_sqm/10000 > 0.5
GROUP BY
	waterbody_poly_id, ST_Area(lake.geom)
), l4 as (
SELECT
	waterbody_poly_id
FROM
	clipped
WHERE
	(intersect_area/lake_area) > 0.5 -- Include when the majority of the lake area (I.e., > 50%) overlaps with the bec zone/subzone, otherwise exclude
)
UPDATE
	{vector_schema}.fwa_lakes_poly wet
SET
	riparian_class = 'L4',
	riparian_class_reason = 'not less than 0.25 ha and less than 1 ha in size and is in a biogeoclimatic zone or subzone CDF, CWH (xm, dm, ds)',
	riparian_data_source = 'FWA area; WHSE_FOREST_VEGETATION.BEC_BIOGEOCLIMATIC_POLY'
FROM
	l4
WHERE
	l4.waterbody_poly_id = wet.waterbody_poly_id")
run_sql_r(query, conn_list)


## STREAMS
## Add fields to `thlb_proxy.modelled_habitat_potential` for later use
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential DROP COLUMN IF EXISTS riparian_class;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential ADD COLUMN IF NOT EXISTS riparian_class varchar(3);")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential DROP COLUMN IF EXISTS riparian_class_reason;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential ADD COLUMN IF NOT EXISTS riparian_class_reason text;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential DROP COLUMN IF EXISTS riparian_data_source;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential ADD COLUMN IF NOT EXISTS riparian_data_source text;")
run_sql_r(query, conn_list)

## Update `thlb_proxy.modelled_habitat_potential` with modeled channel_width
print(glue('Updating {dst_schema}.modelled_habitat_potential with channel width'))
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential ADD COLUMN IF NOT EXISTS channel_width real;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential ADD COLUMN IF NOT EXISTS channel_width_source text;")
run_sql_r(query, conn_list)
query <- glue("UPDATE {dst_schema}.modelled_habitat_potential SET channel_width_source = a.channel_width_source, channel_width = a.channel_width FROM {dst_schema}.fwa_stream_networks_channel_width a WHERE a.linear_feature_id = {dst_schema}.modelled_habitat_potential.linear_feature_id;")
run_sql_r(query, conn_list)

## In order to easily calculate which linestrings are within community watersheds, generate a geometry of the centerpoint on the linestring.
## The function requires linestring (present geometry is MultiLineString) so first calculate an interim Linestring geometry.
## Luckily all lines are single multilinestrings so it is a straight conversion


query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential ADD COLUMN IF NOT EXISTS geom_ls geometry(LineString, 3005);")
run_sql_r(query, conn_list)
query <- glue("WITH a AS (
SELECT
	fid,
	(ST_Dump(geom)).geom AS geom
FROM
	{dst_schema}.modelled_habitat_potential
)
UPDATE {dst_schema}.modelled_habitat_potential s SET geom_ls = a.geom
FROM
a WHERE s.fid = a.fid")
run_sql_r(query, conn_list)

## generate the centerpoint
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential ADD COLUMN IF NOT EXISTS line_center_point geometry(Point, 3005);")
run_sql_r(query, conn_list)
query <- glue("UPDATE {dst_schema}.modelled_habitat_potential s SET line_center_point = ST_LineInterpolatePoint(geom_ls, 0.5);")
run_sql_r(query, conn_list)


## drop the interim linestring geometry
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential DROP COLUMN IF EXISTS geom_ls")
run_sql_r(query, conn_list)

## create some indexes - high use table
query <- glue("DROP INDEX IF EXISTS {dst_schema}.modelled_habitat_potential_buffer_idx;")
run_sql_r(query, conn_list)
query <- glue("CREATE INDEX IF NOT EXISTS modelled_habitat_potential_buffer_idx
    ON {dst_schema}.modelled_habitat_potential USING gist
    (riparian_buffer_geom)
    TABLESPACE pg_default;")
run_sql_r(query, conn_list)

query <- glue("DROP INDEX IF EXISTS {dst_schema}.modelled_habitat_potential_center_point_idx;")
run_sql_r(query, conn_list)
query <- glue("CREATE INDEX IF NOT EXISTS modelled_habitat_potential_center_point_idx
    ON {dst_schema}.modelled_habitat_potential USING gist
    (line_center_point)
    TABLESPACE pg_default;")
run_sql_r(query, conn_list)

query <- glue("DROP INDEX IF EXISTS {dst_schema}.modelled_habitat_potential_comm_ws_idx;")
run_sql_r(query, conn_list)
query <- glue("CREATE INDEX IF NOT EXISTS modelled_habitat_potential_comm_ws_idx
    ON {dst_schema}.modelled_habitat_potential USING btree
    (community_watershed ASC NULLS LAST)
    TABLESPACE pg_default;")
run_sql_r(query, conn_list)

query <- glue("DROP INDEX IF EXISTS {dst_schema}.modelled_habitat_potential_fish_idx;")
run_sql_r(query, conn_list)
query <- glue("CREATE INDEX IF NOT EXISTS modelled_habitat_potential_fish_idx
    ON {dst_schema}.modelled_habitat_potential USING btree
    (fish_habitat)
    TABLESPACE pg_default;")
run_sql_r(query, conn_list)

query <- glue("DROP INDEX IF EXISTS {dst_schema}.modelled_habitat_potential_gnis_name_idx;")
run_sql_r(query, conn_list)
query <- glue("CREATE INDEX IF NOT EXISTS modelled_habitat_potential_gnis_name_idx
    ON {dst_schema}.modelled_habitat_potential USING btree
    (gnis_name)
    TABLESPACE pg_default;")
run_sql_r(query, conn_list)

query <- glue("DROP INDEX IF EXISTS {dst_schema}.modelled_habitat_potential_lfi_geom_geom_idx;")
run_sql_r(query, conn_list)
query <- glue("CREATE INDEX IF NOT EXISTS modelled_habitat_potential_lfi_geom_geom_idx
    ON {dst_schema}.modelled_habitat_potential USING gist
    (geom)
    TABLESPACE pg_default;")
run_sql_r(query, conn_list)

query <- glue("DROP INDEX IF EXISTS {dst_schema}.modelled_habitat_potential_lfi_idx;")
run_sql_r(query, conn_list)
query <- glue("CREATE INDEX IF NOT EXISTS modelled_habitat_potential_lfi_idx
    ON {dst_schema}.modelled_habitat_potential USING btree
    (linear_feature_id ASC NULLS LAST)
    TABLESPACE pg_default;")
run_sql_r(query, conn_list)

query <- glue("DROP INDEX IF EXISTS {dst_schema}.modelled_habitat_potential_width_idx;")
run_sql_r(query, conn_list)
query <- glue("CREATE INDEX IF NOT EXISTS modelled_habitat_potential_width_idx
    ON {dst_schema}.modelled_habitat_potential USING btree
    (channel_width ASC NULLS LAST)
    TABLESPACE pg_default;")
run_sql_r(query, conn_list)
query <- glue("ANALYZE {dst_schema}.modelled_habitat_potential;")
run_sql_r(query, conn_list)

## add community watershed field
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential ADD COLUMN IF NOT EXISTS community_watershed boolean DEFAULT False;")
run_sql_r(query, conn_list)

query <- glue("WITH a AS (
SELECT
	stream.linear_feature_id
FROM
	{dst_schema}.modelled_habitat_potential stream
JOIN
	{vector_schema}.wls_community_ws_pub_svw cw
ON
	ST_Intersects(cw.geom, stream.line_center_point)
)
UPDATE
  {dst_schema}.modelled_habitat_potential s
SET
  community_watershed = True
FROM
  a
WHERE
  a.linear_feature_id = s.linear_feature_id;")
run_sql_r(query, conn_list)

query <- "DROP TABLE IF EXISTS public.stream_reaches_with_contributing_areas_outside_bc;"
run_sql_r(query, conn_list)
## Bring in manually created dataset of stream id's within floodplain where the width was less than 100m
run_sql_psql(sql_var = NULL, sql_file = 'data//input//stream_reaches_within_floodplain_where_width_less_100m.sql', pg_db = conn_list$dbname, host = conn_list$host)

## It is known that any stream reach from the Fish Habitat Accessibility MODEL (I.e. fish passage dataset) that has a contributing area outside BC will be invalid. In order to identify those stream reaches, bring in the manually created dataset of stream id's where contributing area is outside BC. This table was initialized by using Simon Norris's [fwapg](https://github.com/smnorris/fwapg) package and then iterating over every blue_line_key in the province using this [function](https://github.com/smnorris/fwapg/blob/main/db/functions/FWA_UpstreamBorderCrossings.sql). See helper script here: `src/analysis/2.riparian-fwapg-pre-processing.R`. The results stream reaches were manually reviewed and removed if ~ 80-90% of the contributing area was within BC. Stream reaches identified as having contributing area's outside BC were compared with Foundry Spatial's BC Water Tools watershed reporting tool (where watersheds extend outside the province in the following NRO regions: Skeena, Peace, Omineca, Kootenay).
## The channel width for identified stream reaches with contributing area's outside BC's channel_width are set to NULL.
query <- glue("UPDATE {dst_schema}.modelled_habitat_potential SET riparian_class = NULL, riparian_class_reason = NULL")
run_sql_r(query, conn_list)
run_sql_psql(sql_var = NULL, sql_file = 'data//input//stream_reaches_with_contributing_areas_outside_BC.sql', pg_db = conn_list$dbname, host = conn_list$host)
query <- glue("UPDATE {dst_schema}.modelled_habitat_potential a SET channel_width = NULL FROM public.stream_reaches_with_contributing_areas_outside_bc outside_bc where outside_bc.linear_feature_id = a.linear_feature_id;")
run_sql_r(query, conn_list)

## Add in natural resource area field
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential DROP COLUMN IF EXISTS adm_nr_areas;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential ADD COLUMN adm_nr_areas varchar(5);")
run_sql_r(query, conn_list)
query <- glue("UPDATE {dst_schema}.modelled_habitat_potential fishy SET adm_nr_areas =
CASE
  WHEN adm.area_name = 'Coast Natural Resource Area' then 'coast'
  WHEN adm.area_name = 'South Natural Resource Area' then 'south'
  WHEN adm.area_name = 'North Natural Resource Area' then 'north'
END
FROM
  {vector_schema}.adm_nr_areas_sp adm
WHERE
ST_Intersects(adm.geom, fishy.line_center_point);")
run_sql_r(query, conn_list)


## "(2) A stream that is a fish stream or is located in a community watershed has the following riparian class:
##  (a) S1A, if the stream averages, over a one km length, either a stream width or an active flood plain width of 100 m or greater;"_
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829


query <- glue("WITH main_rivers AS (
	-- retrieve streams with channel width >= 100 and in community watershed or has fish
	SELECT
		fid
	FROM
		{dst_schema}.modelled_habitat_potential
	WHERE
		channel_width >= 100
	AND
		(
			fish_habitat != 'NON FISH HABITAT'
		OR
			community_watershed
		)
	AND
		-- remove certain fcode labels as they place an endpoint on the stream at confluences we want to avoid ending at
		-- in order to get the full river length
		fwa_fcode_label IN ('Construction Line - Main Connector','Construction Line - Main Flow')
	UNION -- UNION will remove duplicates between upper & lower results (rather than UNION ALL)
	-- combine with streams within floodplain where plain width >= 100 and in community watershed or has fish
	SELECT
	  fishy_streams.fid
	FROM
	  {dst_schema}.modelled_habitat_potential fishy_streams
	JOIN
	  {vector_schema}.cwb_floodplains_bc_area_sp floodplain
	ON
	  ST_Contains(floodplain.geom, fishy_streams.line_center_point)
	LEFT JOIN
	  public.stream_reaches_within_floodplain_where_width_less_100m to_exclude
	ON
		to_exclude.linear_feature_id != fishy_streams.linear_feature_id
	WHERE
			(
				fishy_streams.fish_habitat != 'NON FISH HABITAT'
			OR
				fishy_streams.community_watershed
			)
	AND
	  fwa_fcode_label IN ('Construction Line - Main Connector','Construction Line - Main Flow')

) , main_rivers_merged AS (
	SELECT
		(ST_Dump(ST_LineMerge(ST_Collect(geom)))).geom as geom
	FROM
		main_rivers
	JOIN
		{dst_schema}.modelled_habitat_potential USING (fid)
), main_rivers_merged_length_g_1km AS (
	SELECT
 		geom
	FROM
		main_rivers_merged
	WHERE
		ST_Length(geom) > 1000 -- filter where selected merged streams' length > 1000m
), secondary_rivers AS (
	-- retrieve streams with channel width >= 100 and in community watershed or has fish
	SELECT
		fid
	FROM
		{dst_schema}.modelled_habitat_potential
	WHERE
		channel_width >= 100
	AND
		(
			fish_habitat != 'NON FISH HABITAT'
		OR
			community_watershed
		)
	AND
		-- remove other fcode labels as they place an endpoint on the stream at confluences we want to avoid ending at
		-- in order to get the full river length
		fwa_fcode_label NOT IN ('Construction Line - Main Connector','Construction Line - Main Flow', 'Construction Line - Lake Arm')
	UNION -- UNION will remove duplicates between upper & lower results (rather than UNION ALL)
	-- combine with streams within floodplain where plain width >= 100
	SELECT
	  fishy_streams.fid
	FROM
	  {dst_schema}.modelled_habitat_potential fishy_streams
	JOIN
	  {vector_schema}.cwb_floodplains_bc_area_sp floodplain
	ON
	  ST_Contains(floodplain.geom, fishy_streams.line_center_point)
	LEFT JOIN
	  public.stream_reaches_within_floodplain_where_width_less_100m to_exclude
	ON
		to_exclude.linear_feature_id != fishy_streams.linear_feature_id
	WHERE
			(
				fishy_streams.fish_habitat != 'NON FISH HABITAT'
			OR
				fishy_streams.community_watershed
			)
	AND
	  fwa_fcode_label NOT IN ('Construction Line - Main Connector','Construction Line - Main Flow', 'Construction Line - Lake Arm')
), secondary_rivers_merged AS (
	SELECT
		(ST_Dump(ST_LineMerge(ST_Collect(geom)))).geom as geom
	FROM
		secondary_rivers
	JOIN
		{dst_schema}.modelled_habitat_potential USING (fid)
), secondary_rivers_merged_length_g_1km AS (
	SELECT
 		geom
	FROM
		secondary_rivers_merged
	WHERE
		ST_Length(geom) > 1000 -- filter where selected merged streams' length > 1000m
), combined as (
	SELECT
  		geom
	FROM
	 	secondary_rivers_merged_length_g_1km
	UNION ALL
	SELECT
  		geom
	FROM
	  	main_rivers_merged_length_g_1km
), filtered_s1a as (
SELECT
	fishy_streams.fid,
	fishy_streams.geom
FROM
	{dst_schema}.modelled_habitat_potential fishy_streams
JOIN
	combined
ON
	ST_DWithin(fishy_streams.line_center_point, combined.geom, 1) -- join with original table to get the fid
)
UPDATE
  {dst_schema}.modelled_habitat_potential
SET
  riparian_class = 'S1A',
	riparian_class_reason = 'stream averages, over a one km length, either a stream width or an active flood plain width of 100 m or greater',
	riparian_data_source = 'Fish Habitat Accessibility MODEL; WHSE_WATER_MANAGEMENT.WLS_COMMUNITY_WS_PUB_SVW; WHSE_BASEMAPPING.CWB_FLOODPLAINS_BC_AREA_SP; Modeled channel width'
FROM
	filtered_s1a
WHERE
	filtered_s1a.fid = thlb_proxy.modelled_habitat_potential.fid")
run_sql_r(query, conn_list)


## "(b) S1B, if the stream width is greater than 20 m but the stream does not have a riparian class of S1A"_
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829

query <- glue("UPDATE
	{dst_schema}.modelled_habitat_potential
SET
	riparian_class = 'S1B',
	riparian_class_reason = 'stream that is a fish stream or is located in a community watershed and width is greater than 20 m but the stream does not have a riparian class of S1A',
	riparian_data_source = 'Fish Habitat Accessibility MODEL; WHSE_WATER_MANAGEMENT.WLS_COMMUNITY_WS_PUB_SVW; WHSE_BASEMAPPING.CWB_FLOODPLAINS_BC_AREA_SP; Modeled channel width'
WHERE
	channel_width >= 20
AND
	channel_width < 100
AND
	(
		fish_habitat != 'NON FISH HABITAT'
	OR
		community_watershed
  )
AND
  riparian_class IS NULL;")
run_sql_r(query, conn_list)

## "(c) S2, if the stream width is not less than 5 m but not more than 20 m"
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829

query <- glue("UPDATE
	{dst_schema}.modelled_habitat_potential
SET
	riparian_class = 'S2',
	riparian_class_reason = 'stream that is a fish stream or is located in a community watershed and width is not less than 5 m but not more than 20 m',
	riparian_data_source = 'Fish Habitat Accessibility MODEL; WHSE_WATER_MANAGEMENT.WLS_COMMUNITY_WS_PUB_SVW; Modeled channel width'
WHERE
	channel_width >= 5
AND
	channel_width < 20
AND
	(
		fish_habitat != 'NON FISH HABITAT'
	OR
		community_watershed
  )
AND
  riparian_class IS NULL;")
run_sql_r(query, conn_list)

## (d)S3, if the stream width is not less than 1.5 m but is less than 5 m;
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829

query <- glue("UPDATE
	{dst_schema}.modelled_habitat_potential
SET
	riparian_class = 'S3',
	riparian_class_reason = 'stream that is a fish stream or is located in a community watershed and width is not less than 1.5 m but is less than 5 m;',
	riparian_data_source = 'Fish Habitat Accessibility MODEL; WHSE_WATER_MANAGEMENT.WLS_COMMUNITY_WS_PUB_SVW; Modeled channel width'
WHERE
	channel_width >= 1.5
AND
	channel_width < 5
AND
	(
		fish_habitat != 'NON FISH HABITAT'
	OR
		community_watershed
  )
AND
  riparian_class IS NULL;")
run_sql_r(query, conn_list)

## (e) S4, if the stream width is less than 1.5 m."
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829

query <- glue("UPDATE
	{dst_schema}.modelled_habitat_potential
SET
	riparian_class = 'S4',
	riparian_class_reason = 'stream that is a fish stream or is located in a community watershed and width is less than 1.5 m',
	riparian_data_source = 'Fish Habitat Accessibility MODEL; WHSE_WATER_MANAGEMENT.WLS_COMMUNITY_WS_PUB_SVW; Modeled channel width'
WHERE
	channel_width < 1.5
AND
	(
		fish_habitat != 'NON FISH HABITAT'
	OR
		community_watershed
  )
AND
  riparian_class IS NULL;")
run_sql_r(query, conn_list)

## (3)A stream that is not a fish stream and is located outside of a community watershed has the following riparian class:
## (a)S5, if the stream width is greater than 3 m;"_
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829


query <- glue("UPDATE
	{dst_schema}.modelled_habitat_potential
SET
	riparian_class = 'S5',
	riparian_class_reason = 'stream that is not a fish stream and is located outside of a community watershed and width is greater than 3 m;',
	riparian_data_source = 'Fish Habitat Accessibility MODEL; WHSE_WATER_MANAGEMENT.WLS_COMMUNITY_WS_PUB_SVW; Modeled channel width'
WHERE
	channel_width > 3
AND
	(
		fish_habitat = 'NON FISH HABITAT'
	OR
		NOT community_watershed
  )
AND
  riparian_class IS NULL;")
run_sql_r(query, conn_list)


## (3)A stream that is not a fish stream and is located outside of a community watershed has the following riparian class:
## "(b)S6, if the stream width is 3 m or less."
## source https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829


query <- glue("UPDATE
	{dst_schema}.modelled_habitat_potential
SET
	riparian_class = 'S6',
	riparian_class_reason = 'stream that is not a fish stream and is located outside of a community watershed and width is 3 m or less',
	riparian_data_source = 'Fish Habitat Accessibility MODEL; WHSE_WATER_MANAGEMENT.WLS_COMMUNITY_WS_PUB_SVW; Modeled channel width'
WHERE
	channel_width <= 3
AND
	(
		fish_habitat = 'NON FISH HABITAT'
	OR
		NOT community_watershed
  )
AND
  riparian_class IS NULL;")
run_sql_r(query, conn_list)

## when riparian_class IS NULL, then assign riparian class based on stream order and natural resource area.
query <- glue("UPDATE
	{dst_schema}.modelled_habitat_potential
SET
	riparian_class = CASE
	WHEN adm_nr_areas = 'coast' THEN
    CASE
    	WHEN
    	  stream_order >= 5 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed) THEN 'S1B'
    	WHEN
    	  stream_order = 4 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed) THEN 'S2'
    	WHEN
    	  stream_order IN (2,3) AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed) THEN 'S3'
    	WHEN
    	  stream_order = 1 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed) THEN 'S4'
    	WHEN
    	  stream_order > 1 AND (fish_habitat = 'NON FISH HABITAT' OR NOT community_watershed) THEN 'S5'
    	WHEN
    	  stream_order = 1 AND (fish_habitat = 'NON FISH HABITAT' OR NOT community_watershed) THEN 'S6'
    END
  ELSE
    CASE
    	WHEN
    	  stream_order >= 4 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed) THEN 'S1B'
    	WHEN
    	  stream_order = 3 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed) THEN 'S2'
    	WHEN
    	  stream_order = 2 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed) THEN 'S3'
    	WHEN
    	  stream_order = 1 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed) THEN 'S4'
    	WHEN
    	  stream_order > 1 AND (fish_habitat = 'NON FISH HABITAT' OR NOT community_watershed) THEN 'S5'
    	WHEN
    	  stream_order = 1 AND (fish_habitat = 'NON FISH HABITAT' OR NOT community_watershed) THEN 'S6'
    END
  END,
	riparian_class_reason = CASE
	WHEN adm_nr_areas = 'coast' THEN
	  CASE
    	WHEN
    	  stream_order >= 5 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed)
    	  THEN 'coast admin area and stream order >= 5 and fish presence or community watershed'
    	WHEN
    	  stream_order = 4 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed)
    	  THEN 'coast admin area and stream order = 4 and fish presence or community watershed'
    	WHEN
    	  stream_order IN (2,3) AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed)
    	  THEN 'coast admin area and stream order either 2 or 3 and fish presence or community watershed'
    	WHEN
    	  stream_order = 1 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed)
    	  THEN 'coast admin area and stream order = 1 and fish presence or community watershed'
    	WHEN
    	  stream_order > 1 AND (fish_habitat = 'NON FISH HABITAT' OR NOT community_watershed)
    	  THEN 'coast admin area and stream order > 1 and fish absence or not community watershed'
    	WHEN
    	  stream_order = 1 AND (fish_habitat = 'NON FISH HABITAT' OR NOT community_watershed)
    	  THEN 'coast admin area and stream order = 1 and fish absence or not community watershed'
    END
  ELSE
	  CASE
    	WHEN
    	  stream_order >= 4 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed)
    	  THEN 'not coast area and stream order >= 4 and fish presence or community watershed'
    	WHEN
    	  stream_order = 3 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed)
    	  THEN 'not coast area and stream order = 3 and fish presence or community watershed'
    	WHEN
    	  stream_order = 2 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed)
    	  THEN 'not coast area and stream order = 2 and fish presence or community watershed'
    	WHEN
    	  stream_order = 1 AND (fish_habitat != 'NON FISH HABITAT' OR community_watershed)
    	  THEN 'not coast area and stream order = 1 and fish presence or community watershed'
    	WHEN
    	  stream_order > 1 AND (fish_habitat = 'NON FISH HABITAT' OR NOT community_watershed)
    	  THEN 'not coast area and stream order > 1 and fish absence or not community watershed'
    	WHEN
    	  stream_order = 1 AND (fish_habitat = 'NON FISH HABITAT' OR NOT community_watershed)
    	  THEN 'not coast area and stream order = 1 and fish absence or not community watershed'
    END
  END,
	riparian_data_source = 'Fish Habitat Accessibility MODEL; WHSE_WATER_MANAGEMENT.WLS_COMMUNITY_WS_PUB_SVW, WHSE_ADMIN_BOUNDARIES.ADM_NR_AREAS_SP; North Island TSA Data Packages'
  where riparian_class IS NULL;")
run_sql_r(query, conn_list)

## FWA Rivers
## For stream classification, the FWA rivers polygonal dataset is usually used in Timber Supply Analyses. It is important to include it to ensure the river area's are included in the riparian zones. In order to determine the stream classification per FWA River Poly, the most common (I.e. mode) classification was used.
query <- glue("ALTER TABLE {vector_schema}.fwa_rivers_poly DROP COLUMN IF EXISTS riparian_class;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_rivers_poly DROP COLUMN IF EXISTS riparian_class_reason;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_rivers_poly DROP COLUMN IF EXISTS riparian_data_source;")
run_sql_r(query, conn_list)

query <- glue("ALTER TABLE {vector_schema}.fwa_rivers_poly ADD COLUMN riparian_class varchar(3);")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_rivers_poly ADD COLUMN riparian_class_reason text;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_rivers_poly ADD COLUMN riparian_data_source text;")
run_sql_r(query, conn_list)

query <- glue("WITH most_common_class AS (
	SELECT
		rivers.waterbody_poly_id,
		MODE() WITHIN GROUP (order by fishy.riparian_class) AS riparian_class_model
	FROM
		{vector_schema}.fwa_rivers_poly rivers
	JOIN
		{dst_schema}.modelled_habitat_potential fishy
	ON
		ST_Intersects(rivers.geom, fishy.line_center_point)
	GROUP BY
		waterbody_poly_id
)
UPDATE
	thlb_proxy.fwa_rivers_poly riv
SET
	riparian_class = a.riparian_class_model,
	riparian_class_reason = 'Most common stream classification within rivers polygon',
	riparian_data_source = 'Fish Habitat Accessibility MODEL; WHSE_WATER_MANAGEMENT.WLS_COMMUNITY_WS_PUB_SVW; WHSE_BASEMAPPING.CWB_FLOODPLAINS_BC_AREA_SP; Modeled channel width; FWA Rivers'
FROM
  most_common_class a
WHERE
	riv.waterbody_poly_id = a.waterbody_poly_id;")
run_sql_r(query, conn_list)

query <- glue("WITH nearest_river_polys AS (
    SELECT
        riv_with_nulls.waterbody_poly_id,
        l.waterbody_poly_id AS nearest_waterbody_poly_id,
		l.distance,
		l.riparian_class
    FROM
        {vector_schema}.fwa_rivers_poly riv_with_nulls
    CROSS JOIN LATERAL (
        SELECT
		l.waterbody_poly_id,
		l.riparian_class,
		ST_Distance(riv_with_nulls.geom, l.geom) as distance
        FROM
			{vector_schema}.fwa_rivers_poly l
		WHERE
			l.riparian_class IS NOT NULL
        ORDER BY riv_with_nulls.geom <-> l.geom
        LIMIT 1
    ) l
	WHERE
		riv_with_nulls.riparian_class IS NULL
)
UPDATE {vector_schema}.fwa_rivers_poly r
SET
	riparian_class = nearest_river_polys.riparian_class
FROM
	nearest_river_polys
WHERE
	nearest_river_polys.waterbody_poly_id = r.waterbody_poly_id;")
run_sql_r(query, conn_list)

query <- glue("UPDATE {vector_schema}.fwa_rivers_poly
          SET
            riparian_class = CASE
                              WHEN riparian_class = 'S6' THEN 'S5'
                              WHEN riparian_class IN ('S3', 'S4') THEN 'S2'
                              ELSE riparian_class
                            END;")
run_sql_r(query, conn_list)


## Since we will only export streams outside of FWA polygons, update the streams layer by adding a field that indicates whether it overlaps with lakes, wetlands, or rivers polygons. This field will be used in a subsequent layer export step.
## update streams that overlap with rivers
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential DROP COLUMN IF EXISTS inside_fwa_polygon;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential ADD COLUMN inside_fwa_polygon boolean default FALSE;")
run_sql_r(query, conn_list)

query <- glue("UPDATE {dst_schema}.modelled_habitat_potential fishy SET inside_fwa_polygon = TRUE
FROM
  {vector_schema}.fwa_rivers_poly riv
WHERE
  ST_Intersects(riv.geom, fishy.line_center_point)")
run_sql_r(query, conn_list)

query <- glue("UPDATE {dst_schema}.modelled_habitat_potential fishy SET inside_fwa_polygon = TRUE
FROM
  {vector_schema}.fwa_lakes_poly lake
WHERE
  ST_Intersects(lake.geom, fishy.line_center_point)")
run_sql_r(query, conn_list)


query <- glue("UPDATE {dst_schema}.modelled_habitat_potential fishy SET inside_fwa_polygon = TRUE
FROM
  {vector_schema}.fwa_wetlands_poly wetland
WHERE
  ST_Intersects(wetland.geom, fishy.line_center_point)")
run_sql_r(query, conn_list)



l1a_buffer <- 0
l1b_buffer <- 10
l2_buffer  <- 15
l3_buffer  <- 7.5
l4_buffer  <- 7.5

w1_buffer <- 20
w2_buffer <- 15
w3_buffer <- 7.5
w4_buffer <- 7.5
w5_buffer <- 20

s1a_buffer <- 50
s1b_buffer <- 60
s2_buffer  <- 40
s3_buffer  <- 30
s4_buffer  <- 7.5
s5_buffer  <- 7.5
s6_buffer  <- 1


## lakes
query <- glue("ALTER TABLE {vector_schema}.fwa_lakes_poly DROP COLUMN IF EXISTS riparian_buffer_geom;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_lakes_poly ADD COLUMN riparian_buffer_geom geometry(MultiPolygon, 3005);")
run_sql_r(query, conn_list)

query <- glue("ALTER TABLE {vector_schema}.fwa_lakes_poly DROP COLUMN IF EXISTS riparian_buffer_width_m;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_lakes_poly ADD COLUMN riparian_buffer_width_m smallint;")
run_sql_r(query, conn_list)

query <- glue("UPDATE {vector_schema}.fwa_lakes_poly SET riparian_buffer_width_m = CASE
	WHEN riparian_class = 'L1A' THEN {l1a_buffer}
	WHEN riparian_class = 'L1B' THEN {l1b_buffer}
	WHEN riparian_class = 'L2'  THEN {l2_buffer}
	WHEN riparian_class = 'L3'  THEN {l3_buffer}
	WHEN riparian_class = 'L4'  THEN {l4_buffer}
	END;")
run_sql_r(query, conn_list)
query <- glue("UPDATE {vector_schema}.fwa_lakes_poly SET riparian_buffer_geom = ST_Buffer(geom, riparian_buffer_width_m);")
run_sql_r(query, conn_list)

query <- glue("CREATE INDEX IF NOT EXISTS fwa_lakes_poly_buffer_idx
    ON {vector_schema}.fwa_lakes_poly USING gist
    (riparian_buffer_geom)
    TABLESPACE pg_default;")
run_sql_r(query, conn_list)

query <- glue("ANALYZE {vector_schema}.fwa_lakes_poly;")
run_sql_r(query, conn_list)

## wetland
query <- glue("ALTER TABLE {vector_schema}.fwa_wetlands_poly DROP COLUMN IF EXISTS riparian_buffer_geom;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_wetlands_poly ADD COLUMN riparian_buffer_geom geometry(MultiPolygon, 3005);")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_wetlands_poly DROP COLUMN IF EXISTS riparian_buffer_width_m;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_wetlands_poly ADD COLUMN riparian_buffer_width_m smallint;")
run_sql_r(query, conn_list)

query <- glue("UPDATE {vector_schema}.fwa_wetlands_poly SET riparian_buffer_width_m = CASE
	WHEN riparian_class = 'W1'  THEN {w1_buffer}
	WHEN riparian_class = 'W2'  THEN {w2_buffer}
	WHEN riparian_class = 'W3'  THEN {w3_buffer}
	WHEN riparian_class = 'W4'  THEN {w4_buffer}
	WHEN riparian_class = 'W5'  THEN {w5_buffer}
	END;")
run_sql_r(query, conn_list)
query <- glue("UPDATE {vector_schema}.fwa_wetlands_poly SET riparian_buffer_geom = ST_Buffer(geom, riparian_buffer_width_m);")
run_sql_r(query, conn_list)

query <- glue("CREATE INDEX IF NOT EXISTS fwa_wetlands_poly_buffer_idx
    ON {vector_schema}.fwa_wetlands_poly USING gist
    (riparian_buffer_geom)
    TABLESPACE pg_default;")
run_sql_r(query, conn_list)

query <- glue("ANALYZE {vector_schema}.fwa_wetlands_poly;")
run_sql_r(query, conn_list)

## rivers
query <- glue("ALTER TABLE {vector_schema}.fwa_rivers_poly DROP COLUMN IF EXISTS riparian_buffer_geom;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_rivers_poly ADD COLUMN riparian_buffer_geom geometry(MultiPolygon, 3005);")
run_sql_r(query, conn_list)

query <- glue("ALTER TABLE {vector_schema}.fwa_rivers_poly DROP COLUMN IF EXISTS riparian_buffer_width_m;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {vector_schema}.fwa_rivers_poly ADD COLUMN riparian_buffer_width_m smallint;")
run_sql_r(query, conn_list)

query <- glue("UPDATE {vector_schema}.fwa_rivers_poly SET riparian_buffer_width_m = CASE
	WHEN riparian_class = 'S1A' THEN {s1a_buffer}
	WHEN riparian_class = 'S1B' THEN {s1b_buffer}
	WHEN riparian_class = 'S2'  THEN {s2_buffer}
	WHEN riparian_class = 'S3'  THEN {s3_buffer}
	WHEN riparian_class = 'S4'  THEN {s4_buffer}
	WHEN riparian_class = 'S5'  THEN {s5_buffer}
	WHEN riparian_class = 'S6'  THEN {s6_buffer}
	END;")
run_sql_r(query, conn_list)
query <- glue("UPDATE {vector_schema}.fwa_rivers_poly SET riparian_buffer_geom = ST_Buffer(geom, riparian_buffer_width_m);")
run_sql_r(query, conn_list)

query <- glue("CREATE INDEX IF NOT EXISTS fwa_rivers_poly_buffer_idx
    ON {vector_schema}.fwa_rivers_poly USING gist
    (riparian_buffer_geom)
    TABLESPACE pg_default;")
run_sql_r(query, conn_list)

query <- glue("ANALYZE {vector_schema}.fwa_rivers_poly;")
run_sql_r(query, conn_list)

## stream network
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential DROP COLUMN IF EXISTS riparian_buffer_geom;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential ADD COLUMN riparian_buffer_geom geometry(MultiPolygon, 3005);")
run_sql_r(query, conn_list)

query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential DROP COLUMN IF EXISTS riparian_buffer_width_m;")
run_sql_r(query, conn_list)
query <- glue("ALTER TABLE {dst_schema}.modelled_habitat_potential ADD COLUMN riparian_buffer_width_m smallint;")
run_sql_r(query, conn_list)
query <- glue("UPDATE {dst_schema}.modelled_habitat_potential SET riparian_buffer_width_m = CASE
	WHEN riparian_class = 'S1A' THEN {s1a_buffer}
	WHEN riparian_class = 'S1B' THEN {s1b_buffer}
	WHEN riparian_class = 'S2'  THEN {s2_buffer}
	WHEN riparian_class = 'S3'  THEN {s3_buffer}
	WHEN riparian_class = 'S4'  THEN {s4_buffer}
	WHEN riparian_class = 'S5'  THEN {s5_buffer}
	WHEN riparian_class = 'S6'  THEN {s6_buffer}
	END;")
run_sql_r(query, conn_list)
query <- glue("UPDATE {dst_schema}.modelled_habitat_potential SET riparian_buffer_geom = ST_Buffer(geom, riparian_buffer_width_m) WHERE riparian_buffer_width_m IS NOT NULL;")
run_sql_r(query, conn_list)

query <- glue("CREATE INDEX IF NOT EXISTS modelled_habitat_potential_buffer_idx
    ON {dst_schema}.modelled_habitat_potential USING gist
    (riparian_buffer_geom)
    TABLESPACE pg_default;")
run_sql_r(query, conn_list)

query <- glue("ANALYZE {dst_schema}.modelled_habitat_potential;")
run_sql_r(query, conn_list)


## Convert buffers into riparian raster
## First step is to merge all the vectors (buffered lakes, streams wetlands and rivers) together into one buffer layer
## Exceeds memory to do it in one SQL command, instead, iterate over each 50k mapsheet

grid_table <- glue("{vector_schema}.nts_50k_grid")
grid_loop_fld <- "map_tile"
grid_geom_fld <- "geom"
buffer_table_name <- glue("{vector_schema}.riparian_buffers")

query <- glue("DROP TABLE IF EXISTS {buffer_table_name};")
run_sql_r(query, conn_list)
query <- glue("CREATE TABLE {buffer_table_name} (
  {grid_loop_fld} varchar,
  geom geometry(MultiPolygon, 3005)
);")
run_sql_r(query, conn_list)

query <- glue("SELECT {grid_loop_fld} FROM {grid_table}")
grid_df <- sql_to_df(query, conn_list)


query <- sprintf("INSERT INTO {buffer_table_name} ({grid_loop_fld}, geom)
WITH all_buffers_appended AS (
  -- streams
  SELECT
    riparian_buffer_geom as geom
  FROM
    %s.modelled_habitat_potential streams
  JOIN
    {grid_table} grd
  ON
    ST_Intersects(grd.geom, streams.geom)
  WHERE
    riparian_class IS NOT NULL
	and
		grd.{grid_loop_fld} = '{grid_row}'
  UNION ALL

  -- lakes
  SELECT
    riparian_buffer_geom
  FROM
    %s.fwa_lakes_poly lakes
  JOIN
    {grid_table} grd
  ON
    ST_Intersects(grd.geom, lakes.geom)
  WHERE
    riparian_class IS NOT NULL
	and
		grd.{grid_loop_fld} = '{grid_row}'
  UNION ALL

  -- wetland
  SELECT
    riparian_buffer_geom
  FROM
    %s.fwa_wetlands_poly wet
  JOIN
    {grid_table} grd
  ON
    ST_Intersects(grd.geom, wet.geom)
  WHERE
    riparian_class IS NOT NULL
	and
		grd.{grid_loop_fld} = '{grid_row}'
  UNION ALL

  -- streams
  SELECT
    riparian_buffer_geom
  FROM
    %s.fwa_rivers_poly streams
  JOIN
    {grid_table} grd
  ON
    ST_Intersects(grd.geom, streams.geom)
  WHERE
    riparian_class IS NOT NULL
	and
		grd.{grid_loop_fld} = '{grid_row}'
)
SELECT
  grd.{grid_loop_fld},
  ST_Intersection(ST_Union(vect.geom), grd.{grid_geom_fld}) as geom
FROM
  all_buffers_appended vect
JOIN
  {grid_table} grd
ON
  ST_Intersects(grd.geom, vect.geom)
AND
	grd.{grid_loop_fld} = '{grid_row}'
GROUP BY
	grd.{grid_geom_fld}, grd.{grid_loop_fld}", dst_schema, vector_schema, vector_schema, vector_schema)

for (i in 1:nrow(grid_df)) {
	iteration_start_time <- Sys.time()
	grid_row <- grid_df[i, ]
	all_rows <- nrow(grid_df)
	print(glue("On map tile: {grid_row}, {i}/{all_rows}, {format(iteration_start_time, '%Y-%m-%d %I:%M:%S %p')}"))
	run_sql_r(glue(query), conn_list)
}

## Now that we have a riparian polygon for the whole province, rasterize 100m gr_skey grid
## Ran into memory issues rasterizing the whole province, instead, iterating over 50K mapsheets
grid_loop_fld <- "map_tile"

spatial_query <- "SELECT
	geom
FROM
	{buffer_table_name} vect
WHERE
	{grid_loop_fld} = '{grid_row}'"

spatial_query_when_error <- "SELECT
	geom
FROM
	{buffer_table_name} vect
WHERE
	{grid_loop_fld} = '{grid_row}'"

tbl_comment <- "COMMENT ON TABLE {dst_schema}.{dst_tbl} IS 'Table created at {today_date}.
Table contains the gr_skey pixel percent coverage by the following layers:
Layer: {buffer_table_name}
Buffer: riparian_buffer_width_m'"

dst_tbl <- "bc_riparian_buffers_gr_skey"

linear_weight(template_tif           = "data\\input\\bc_01ha_gr_skey.tif",  ## "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif"
			mask_tif                 = "data\\input\\BC_Boundary_Terrestrial.tif", ## 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
			crop_extent              = c(273287.5,1870587.5,367787.5,1735787.5),
			grid_tbl                 = glue("{vector_schema}.nts_50k_grid"),
			grid_loop_fld            = "map_tile",
			grid_geom_fld            = "geom",
			dst_schema               = dst_schema,
			dst_tbl                  = dst_tbl,
			pg_conn_param            = conn_list,
			create_vector_lyr        = TRUE,
			spatial_query            = spatial_query,
			spatial_query_when_error = spatial_query_when_error,
			tbl_comment              = tbl_comment)

## Check output
query <- glue("select * from {dst_schema}.{dst_tbl} where fact > 1")
review_rip <- sql_to_df(query, conn_list)
## at time of processing, there were 475 cells where fact > 1 and the largest was 1.01963
## That level of error is acceptable - adjust down to 1
if (max(review_rip$fact) < 1.01964 && nrow(review_rip) == 475) {
	query <- glue("UPDATE {dst_schema}.{dst_tbl} SET fact = 1 WHERE fact > 1")
	run_sql_r(query, conn_list)
}


## Example code of outputting riparian vectors into a file geodatabase
## Used when outputting the riparian vectors to other analysts

tsa_number <- 30
out_poly_gdb_path <- glue("C:\\projects\\FAIB_PROXY_THLB\\data\\output\\riparian_classes_tsa_{tsa_number}_poly.gdb")
out_line_gdb_path <- glue("C:\\projects\\FAIB_PROXY_THLB\\data\\output\\riparian_classes_tsa_{tsa_number}_line.gdb")
## write out 2 geodatabase - one with multipolygons and one with linestrings
## write out 2 as there have been errors when trying to write out different geometry types (ie. linestring & polygon) into the same gdb. 
## as a workaround, write out a gdb for each geometry type

## rivers
system(glue("ogr2ogr -overwrite -f \"FileGDB\" {out_poly_gdb_path} PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}'\" -nlt MULTIPOLYGON -nln fwa_rivers_poly -sql \"SELECT waterbody_poly_id::int, watershed_group_id::int, waterbody_type, gnis_name_1, fwa_watershed_code, local_watershed_code, watershed_group_code, left_right_tributary, feature_area_sqm, feature_length_m, riv.geom, riparian_class_reason, riparian_class, riparian_data_source FROM whse_vector.fwa_rivers_poly riv JOIN (select ST_Union(geom) as geom from whse_vector.tsa_boundaries_2020 where tsa IN ({tsa_number})) tsa ON ST_Intersects(tsa.geom, riv.geom) WHERE riparian_class IS NOT NULL\""))

## lakes
system(glue("ogr2ogr -f \"FileGDB\" {out_poly_gdb_path} PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}'\" -nlt MULTIPOLYGON -nln fwa_lakes_poly -sql \"SELECT waterbody_poly_id::int, watershed_group_id::int, waterbody_type, gnis_name_1, fwa_watershed_code, local_watershed_code, watershed_group_code, left_right_tributary, feature_area_sqm, feature_length_m, lake.geom, riparian_class_reason, riparian_class, riparian_data_source FROM whse_vector.fwa_lakes_poly lake JOIN (select ST_Union(geom) as geom from whse_vector.tsa_boundaries_2020 where tsa IN ({tsa_number})) tsa ON ST_Intersects(tsa.geom, lake.geom) WHERE riparian_class IS NOT NULL\" -update"))

## wetlands
system(glue("ogr2ogr -f \"FileGDB\" {out_poly_gdb_path} PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}'\" -nlt MULTIPOLYGON -nln fwa_wetlands_poly -sql \"SELECT waterbody_poly_id::int, watershed_group_id::int, waterbody_type, gnis_name_1, fwa_watershed_code, local_watershed_code, watershed_group_code, left_right_tributary, feature_area_sqm, feature_length_m, wet.geom, riparian_class_reason, riparian_class, riparian_data_source FROM whse_vector.fwa_wetlands_poly wet JOIN (select ST_Union(geom) as geom from whse_vector.tsa_boundaries_2020 where tsa IN ({tsa_number})) tsa ON ST_Intersects(tsa.geom, wet.geom) WHERE riparian_class IS NOT NULL\" -update"))

## streams
system(glue("ogr2ogr -overwrite -f \"FileGDB\" C:\\projects\\FAIB_PROXY_THLB\\data\\output\\riparian_classes_tsa_30_line.gdb PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}'\" -nlt MULTILINESTRING -nln fwa_stream_networks_sp_modelled_habitat_potential -sql \"SELECT fid, linear_feature_id::int, fish_habitat_id, blue_line_key, watershed_key, gnis_name, fwa_watershed_code, watershed_group_code, downstream_route_measure, waterbody_key, fwa_fcode_label, observation_id, gradient_barrier_030_id,gradient_barrier_050_id,gradient_barrier_080_id,gradient_barrier_150_id,gradient_barrier_220_id,gradient_barrier_300_id,intermittent_id,fish_obstacle_point_id,obstruction_id,subsurfaceflow_id,fish_habitat,slope,slope_class,stream_order,stream_magnitude,stream.geom,channel_width,channel_width_source,community_watershed,riparian_class,riparian_class_reason,riparian_data_source FROM whse.modelled_habitat_potential stream JOIN (select ST_Union(geom) as geom from whse_vector.tsa_boundaries_2020 where tsa IN ({tsa_number})) tsa ON ST_Intersects(tsa.geom, stream.geom) WHERE NOT inside_fwa_polygon AND riparian_class IS NOT NULL\" -update"))
