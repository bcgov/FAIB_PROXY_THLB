library(terra)
library(dplyr)
library(dadmtools)
library(DBI)

## https://www.paulamoraga.com/book-spatial/the-terra-package-for-raster-and-vector-data.html
## bc streams layer: WHSE_BASEMAPPING.FWA_STREAM_NETWORKS_SP

linear_weight <- function(
                               template_tif     = "data\\input\\bc_01ha_gr_skey.tif",  ## "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif"
                               mask_tif         = "data\\input\\BC_Boundary_Terrestrial.tif", ## 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
                               crop_extent      = c(273287.5,1870587.5,367787.5,1735787.5),
							   grid_tbl         = "whse_sp.nts_50k_grid",
							   grid_loop_fld    = "map_tile",
							   grid_geom_fld    = "geom",
							   dst_schema       = "whse",
							   dst_tbl          = "bc_riparian_fdp_fact",
							   pg_conn_param    = pg_conn_param

)
{
	script_start_time <- Sys.time()
	print(glue("Script started at {format(script_start_time, '%Y-%m-%d %I:%M:%S %p')}"))
	## Source - file://sfp.idir.bcgov/s164/S63016/!Workgrp/Analysts/!Project/TSA_Projects/active/KootenayLake_TSA13/TSR4/AnalysisReport/Analysis_Report/_TSA13_Documentation/03-data-analysis.html#riparian-analysis
	## Goal
	## https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/14_2004#division_d2e9829
	## A stream that is a fish stream or is located in a community watershed has the following riparian class:
	## (a)S1A, if the stream averages, over a one km length, either a stream width or an active flood plain width of 100 m or greater;
	## (b)S1B, if the stream width is greater than 20 m but the stream does not have a riparian class of S1A;
	## (c)S2, if the stream width is not less than 5 m but not more than 20 m;
	## (d)S3, if the stream width is not less than 1.5 m but is less than 5 m;
	## (e)S4, if the stream width is less than 1.5 m.

	## The S1-A stream was identified using the BCGW rivers layer classification of S1A. 
	## FWA The FWA rivers layer was considered S1
	## For all streams within the FWA rivers layer, an estimated length was calculated as 
	## half the estimated FEATURE_LENGTH, as this length includes both river banks, essentially 
	## twice the length of the river.

	## The estimated width was the FEATURE_AREA_SQM divided by the estimated length. 
	## Where the estimated length was over 1 KM, and the estimated width was over 100 m, the stream was assigned S1A.
	## Additional streams that extended from this selection were also called S1A, or were manually identified 
	## and checked. All others within the FWA River layer were called S1B.

	## TODO - is the integrated road file good enough to replace:
	## WHSE_BASEMAPPING.DRA_DGTL_ROAD_ATLAS_MPAR_SP
	## WHSE_FOREST_TENURE.FTEN_ROAD_SECTION_LINES_SVW
	
	## Buffer and Layer Rationale: Kootenay Lake data package
	## Data Package: https://www2.gov.bc.ca/assets/gov/farming-natural-resources-and-industry/forestry/stewardship/forest-analysis-inventory/tsr-annual-allowable-cut/13ts_dpkg_2020_november.pdf


	spatial_query <- "SELECT
		geom as geom
	FROM
		whse_sp.riparian_buffers_fdp_union
	WHERE
		{grid_loop_fld} = '{grid_row}'"

	spatial_query_when_error <- "SELECT
		ST_Intersection(vect.geom, grid.{grid_geom_fld}) as geom
	FROM
		public.kamloops_fdp_diff_11 vect
	JOIN
		{grid_tbl} grid
	ON
		ST_Intersects(vect.geom, grid.{grid_geom_fld})
	WHERE
		grid.{grid_loop_fld} = '{grid_row}'
	GROUP BY 	
		grid.{grid_geom_fld}, vect.geom"
	## create a terra extent object
	terra_extent <- terra::ext(crop_extent[1], crop_extent[2], crop_extent[3], crop_extent[4])
	print(glue('Reading in raster: {template_tif}'))
	template_rast <- terra::rast(template_tif)
	template_raster_datatype <- datatype(template_rast)
	
	print(glue('Reading in raster: {mask_tif}'))
	mask_rask <- terra::rast(mask_tif)

	rast_lift <- list(template_rast, mask_rask)
	print(glue('Cropping gr_skey grid and mask to BC extent...'))
	crop_list <- lapply(rast_lift, function(x){
			crs(x) <-  "epsg:3005"
			terra::crop(x, terra_extent, datatype='INT4S')
			}
		)
	## reassign newly cropped layers to original variable
	template_rast <- crop_list[[1]]
  	mask_rask <- crop_list[[2]]

	## Create a new masked gr_skey raster
	gr_skey_rast <- terra::mask(template_rast, mask_rask, datatype = template_raster_datatype)

	## release large rasters from memory
	template_rast <- NULL
	mask_rask <- NULL
	
	conn <- DBI::dbConnect(pg_conn_param["driver"][[1]],
					host     = pg_conn_param["host"][[1]],
					user     = pg_conn_param["user"][[1]],
					dbname   = pg_conn_param["dbname"][[1]],
					password = pg_conn_param["password"][[1]],
					port     = pg_conn_param["port"][[1]])

	query <- glue("CREATE TABLE IF NOT EXISTS {dst_schema}.{dst_tbl}_status (
		{grid_loop_fld} character varying(32) NOT NULL PRIMARY KEY,
		status text NOT NULL default 'ready',
		modified_at timestamp with time zone DEFAULT now());")
	run_sql_r(query, pg_conn_param)

	query <- glue("
	WITH a AS (
		SELECT
			{grid_loop_fld}
		FROM
			whse_sp.riparian_buffers_fdp vect
		GROUP BY 
			{grid_loop_fld}
	)
	INSERT INTO {dst_schema}.{dst_tbl}_status ({grid_loop_fld})
		SELECT
			grid.map_tile
		FROM
			a grid
		LEFT JOIN
			{dst_schema}.{dst_tbl}_status status
		USING ({grid_loop_fld})
			WHERE status.{grid_loop_fld} is null;
		")
	run_sql_r(query, pg_conn_param)


	query <- glue("CREATE TABLE IF NOT EXISTS {dst_schema}.{dst_tbl} (
		gr_skey INTEGER NOT NULL PRIMARY KEY,
		fact numeric NOT NULL,
		map_tile text NOT NULL);")
	run_sql_r(query, pg_conn_param)
	
	## retrieve map tiles to loop over from status table
	query <- glue("SELECT {grid_loop_fld} FROM {dst_schema}.{dst_tbl}_status where status = 'ready'")
	grid_df <- sql_to_df(query, pg_conn_param)

	for (i in 1:nrow(grid_df)) {
		iteration_start_time <- Sys.time()
		grid_row <- grid_df[i, ]
		all_rows <- nrow(grid_df)
		print(glue("On map tile: {grid_row}, {i}/{all_rows}, {format(iteration_start_time, '%Y-%m-%d %I:%M:%S %p')}"))

		tryCatch({
			vect <- st_cast(st_read(conn, query = glue(spatial_query), crs = 3005), "MULTIPOLYGON")
		}, error = function(e){
			## in the case of an error - wrap a buffer within 0.0001 width to 'fix'
			print('error, running error query')
			vect <- st_cast(st_read(conn, query = glue(spatial_query), crs = 3005), "MULTIPOLYGON")
		})
		
		# print('1')
		if (nrow(vect) < 1) {
			# print('2')
			## skip to the next map tile if not results were found within the mapsheet
			query <- glue("UPDATE {dst_schema}.{dst_tbl}_status SET status = 'completed', modified_at = now() WHERE {grid_loop_fld} = '{grid_row}'")
			run_sql_r(query, pg_conn_param)
			print(glue('No records inserted for map tile: {grid_row}'))
			next
		}
		# print('3')
		vect_extent <- terra::ext(vect)
		rast_clipped <- terra::crop(gr_skey_rast, vect_extent)
		results <- terra::extract(rast_clipped, vect, weights = TRUE, na.rm = TRUE)
		# print('4')
		## within the results, records sometimes exist where bc_01ha_gr_skey IS NULL
		## this happen on the coast when the raster has been masked but the linear features 
		## exists outside the mask
		## They are not needed, remove records with NULL values in bc_01ha_gr_skey
		results <- results[complete.cases(results$bc_01ha_gr_skey), ]
		sum_weight_by_bc_01ha_gr_skey <- results %>%
			group_by(bc_01ha_gr_skey) %>%
			summarise(fact = sum(weight))
		# print('5')
		colnames(sum_weight_by_bc_01ha_gr_skey) <- c('gr_skey', 'fact')
		## write results to a temporary table
		df_to_pg(Id(schema = dst_schema, table = glue('{dst_tbl}_tmp')), sum_weight_by_bc_01ha_gr_skey, pg_conn_param, overwrite=TRUE)
		query <- glue("INSERT INTO {dst_schema}.{dst_tbl} AS d (gr_skey, fact, {grid_loop_fld})
		SELECT
			gr_skey,
			fact,
			'{grid_row}'::text as {grid_loop_fld}
		FROM 
			{dst_schema}.{dst_tbl}_tmp
		ON CONFLICT (gr_skey)
		DO UPDATE set fact = 
		CASE 
			-- when insert has already happened, overwrite, otherwise add
			WHEN d.{grid_loop_fld} = EXCLUDED.{grid_loop_fld} THEN EXCLUDED.fact
			ELSE EXCLUDED.fact + d.fact
		END;")
		# print('6')
		run_sql_r(query, pg_conn_param)
		query <- glue("UPDATE {dst_schema}.{dst_tbl}_status SET status = 'completed', modified_at = now() WHERE {grid_loop_fld} = '{grid_row}'")
		run_sql_r(query, pg_conn_param)
		# print('7')
	}
	## build a helpful table comment
	today_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
	tbl_comment <- glue("COMMENT ON TABLE {dst_schema}.{dst_tbl} IS 'Table created at {today_date}.
	Table contains the gr_skey pixel percent coverage by the following layers:
	Layer: public.kamloops_fdp_diff_11
	Buffer: No buffer, layer already buffered in Analysis Ready Dataset'")
	run_sql_r(tbl_comment, pg_conn_param)
	run_sql_r(glue("DROP TABLE {dst_schema}.{dst_tbl}_tmp;"), pg_conn_param)
	end_time <- Sys.time()
	duration <- round(difftime(end_time, script_start_time, units = "mins"), 2)
	print(glue('Script finished. Duration: {duration} minutes.'))
}

pg_conn_param = get_pg_conn_list()
linear_weight(pg_conn_param = pg_conn_param)
