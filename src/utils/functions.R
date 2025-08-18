library(dadmtools)
library(dplyr)

linear_weight <- function(
                               template_tif      = "data\\input\\bc_01ha_gr_skey.tif",  ## "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif"
                               mask_tif          = "data\\input\\BC_Boundary_Terrestrial.tif", ## 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
                               crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5),
							   grid_tbl          = "whse_sp.nts_50k_grid",
							   grid_loop_fld     = "map_tile",
							   grid_geom_fld     = "geom",
							   dst_schema        = "whse",
							   dst_tbl           = "bc_linear_features",
							   pg_conn_param     = pg_conn_param,
							   create_vector_lyr = TRUE,
							   spatial_query,
							   spatial_query_when_error,
							   tbl_comment
)
{
	script_start_time <- Sys.time()
	print(glue("Script started at {format(script_start_time, '%Y-%m-%d %I:%M:%S %p')}"))

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

	query <- glue("INSERT INTO {dst_schema}.{dst_tbl}_status ({grid_loop_fld})
				SELECT
					grid.{grid_loop_fld}
				FROM
					{grid_tbl} grid
				LEFT JOIN
					{dst_schema}.{dst_tbl}_status status
				USING ({grid_loop_fld})
					WHERE status.{grid_loop_fld} is null;
				")
	run_sql_r(query, pg_conn_param)


	query <- glue("CREATE TABLE IF NOT EXISTS {dst_schema}.{dst_tbl} (
		gr_skey INTEGER NOT NULL PRIMARY KEY,
		fact numeric NOT NULL,
		{grid_loop_fld} character varying(32) NOT NULL);")
	run_sql_r(query, pg_conn_param)

	## retrieve map tiles to loop over from status table
	query <- glue("SELECT {grid_loop_fld} FROM {dst_schema}.{dst_tbl}_status where status = 'ready'")
	grid_df <- sql_to_df(query, pg_conn_param)
	if (nrow(grid_df) < 1) {
		print(glue("No records in {dst_schema}.{dst_tbl}_status where status = 'ready'"))
		return()
	}
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

	for (i in 1:nrow(grid_df)) {
		iteration_start_time <- Sys.time()
		grid_row <- grid_df[i, ]
		all_rows <- nrow(grid_df)
		print(glue("On map tile: {grid_row}, {i}/{all_rows}, {format(iteration_start_time, '%Y-%m-%d %I:%M:%S %p')}"))

		tryCatch({
			vect <- st_cast(st_read(conn, query = glue(spatial_query), crs = 3005), "MULTIPOLYGON")
		}, error = function(e){
			## in the case of an error - wrap a buffer within 0.0001 width to 'fix'
			vect <- st_cast(st_read(conn, query = glue(spatial_query_when_error), crs = 3005), "MULTIPOLYGON")
		})
		if (nrow(vect) < 1) {
			## skip to the next map tile if no results were found within the mapsheet
			query <- glue("UPDATE {dst_schema}.{dst_tbl}_status SET status = 'completed', modified_at = now() WHERE {grid_loop_fld} = '{grid_row}'")
			run_sql_r(query, pg_conn_param)
			print(glue('No records inserted for map tile: {grid_row}'))
			next
		}

		vect_extent <- terra::ext(vect)
		rast_clipped <- terra::crop(gr_skey_rast, vect_extent)
		## terra extract link:
		## https://www.paulamoraga.com/book-spatial/the-terra-package-for-raster-and-vector-data.html
		results <- terra::extract(rast_clipped, vect, weights = TRUE, na.rm = TRUE)
		## within the results, records sometimes exist where bc_01ha_gr_skey IS NULL
		## this happen on the coast when the raster has been masked but the linear features
		## exists outside the mask
		## They are not needed, remove records with NULL values in bc_01ha_gr_skey
		results <- results[complete.cases(results$bc_01ha_gr_skey), ]
		sum_weight_by_bc_01ha_gr_skey <- results %>%
			group_by(bc_01ha_gr_skey) %>%
			summarise(fact = sum(weight))
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
			ELSE CASE WHEN EXCLUDED.fact + d.fact > 1 then 1 ELSE EXCLUDED.fact + d.fact END
		END;")
		run_sql_r(query, pg_conn_param)
		query <- glue("UPDATE {dst_schema}.{dst_tbl}_status SET status = 'completed', modified_at = now() WHERE {grid_loop_fld} = '{grid_row}'")
		run_sql_r(query, pg_conn_param)
	}
	## build a helpful table comment
	today_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
	if (!(is.null(tbl_comment))) {
		run_sql_r(glue(tbl_comment), pg_conn_param)
	}
	run_sql_r(glue("DROP TABLE IF EXISTS {dst_schema}.{dst_tbl}_tmp;"), pg_conn_param)
	end_time <- Sys.time()
	duration <- round(difftime(end_time, script_start_time, units = "mins"), 2)
	print(glue('Script finished. Duration: {duration} minutes.'))
	DBI::dbDisconnect(conn)
}


import_bcgw_to_pg <- function(
                               src_schema     = "WHSE_BASEMAPPING",
                               src_layer      = "GBA_RAILWAY_TRACKS_SP",
							   fdw_schema     = "load",
							   dst_schema     = "whse_sp",
							   dst_layer      = "GBA_RAILWAY_TRACKS_SP",
                               fields_to_keep = "railway_track_id",
							   geometry_name  = "shape",
							   geometry_type  = "MultiLineString",
							   grouping_name  = NULL,
							   where_clause   = NULL,
							   pg_conn_list

) {
	query <- glue('DROP FOREIGN TABLE IF EXISTS {fdw_schema}.{src_layer};')
	run_sql_r(query, pg_conn_list)
	query <- glue('IMPORT FOREIGN SCHEMA "{src_schema}" LIMIT TO ({src_layer}) FROM SERVER oradb INTO {fdw_schema};')
	run_sql_r(query, pg_conn_list)
	query <- glue('DROP TABLE IF EXISTS {dst_schema}.{dst_layer};')
	run_sql_r(query, pg_conn_list)
	if (where_clause == '' || is.null(where_clause) || is.na(where_clause)) {
    	query_escaped <- ''
		where_clause <- ''
  	} else {
	    query_escaped <- gsub("\'","\'\'", where_clause)
		where_clause <- glue('WHERE {where_clause}')
  	}
	if (all(is.null(geometry_name), is.null(grouping_name))) {
		query <- glue("CREATE TABLE {dst_schema}.{dst_layer} as
		SELECT
			{fields_to_keep}
		FROM
			{fdw_schema}.{src_layer}
		{where_clause};")
	} else if (is.null(geometry_name)) {
		query <- glue("CREATE TABLE {dst_schema}.{dst_layer} as
		SELECT
			{fields_to_keep},
			'{grouping_name}' as grouping_name
		FROM
			{fdw_schema}.{src_layer}
		{where_clause};")
	} else if (is.null(grouping_name)) {
		query <- glue("CREATE TABLE {dst_schema}.{dst_layer} as
		SELECT
			{fields_to_keep},
			ST_Force2d({geometry_name})::geometry({geometry_type}, 3005) as geom
		FROM
			{fdw_schema}.{src_layer}
		{where_clause};")
	} else {
		query <- glue("CREATE TABLE {dst_schema}.{dst_layer} as
		SELECT
			{fields_to_keep},
			'{grouping_name}' as grouping_name,
			ST_Force2d({geometry_name})::geometry({geometry_type}, 3005) as geom
		FROM
			{fdw_schema}.{src_layer}
		{where_clause};")
	}

	tryCatch({
    	run_sql_r(query, pg_conn_list)
	}, error = function(e) {
		if (grepl("SRID", e$message)) {
		# Remove the SRID cast and create the table without it
		create_table_query_no_srid <- if (is.null(grouping_name)) {
			glue("CREATE TABLE {dst_schema}.{dst_layer} AS
				SELECT
					{fields_to_keep},
					ST_Force2d({geometry_name}) AS geom
				FROM
					{fdw_schema}.{src_layer}
				{where_clause}")
		} else {
			glue("CREATE TABLE {dst_schema}.{dst_layer} AS
				SELECT
					{fields_to_keep},
					{grouping_name} AS grouping_name,
					ST_Force2d({geometry_name}) AS geom
				FROM
					{fdw_schema}.{src_layer}
				{where_clause}")
		}

		# Run the query without SRID and set the SRID manually
		run_sql_r(create_table_query_no_srid, pg_conn_list)
		srid_query <- glue('ALTER TABLE {dst_schema}.{dst_layer} ALTER COLUMN geom TYPE geometry({geometry_type}, 3005) USING ST_SetSRID(geom, 3005)')
		run_sql_r(srid_query, pg_conn_list)
		} else {
		stop(e)
		}
	})
	if (!(is.null(geometry_name))) {
		query <- glue('CREATE INDEX {dst_layer}_geom_idx on {dst_schema}.{dst_layer} USING gist(geom);')
		run_sql_r(query, pg_conn_list)
		query <- glue('ANALYZE {dst_schema}.{dst_layer};')
		run_sql_r(query, pg_conn_list)
	}
	todays_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
	query <- glue("COMMENT ON TABLE {dst_schema}.{dst_layer} IS 'Table created by the import_bcgw_to_pg R function at {todays_date}.
	Data source details:
	Source type: oracle
	South path: bcgw
	Source layer: {src_schema}.{src_layer}
	Source where query: {query_escaped}
	Source fields kept: {fields_to_keep}'")
	run_sql_r(query, pg_conn_list)
}

check_id_in_df <- function(df, id, grouping_id) {
  # Check if there are any rows where waterbody_poly_id equals current_waterbody_poly_id
  if (is.null(grouping_id)){
    any(df$waterbody_poly_id == id)
  } else {
	any(df$waterbody_poly_id == id & df$grouping_id == grouping_id)
  }
}


remove_from_list <- function(ids_to_check, id) {
  keep <- sapply(ids_to_check, function(x) x != id)
  return(ids_to_check[keep])
}

group_overlapping_ids <- function(df, field1, field2) {
  ## create an empty df and name the columns
  new_df <- data.frame(matrix(ncol = 2, nrow = 0))
  names(new_df) <- c('grouping_id', field1)

  # Loop through each row of the original data frame
  for (wetland_i in 1:nrow(df)) {
    # Get the current row's waterbody_poly_id and overlapping_waterbody_poly_id
    # ensure to handle if there are multiple waterbody_poly_id's that match the waterbody_poly_id of i
    matching_rows <- which(df[, field1] == df[, field1][wetland_i])
    current_waterbody_poly_id <- df[, field1][wetland_i]
    current_overlapping_waterbody_poly_id <- df[, field2][matching_rows]
  	## if new_df already has an instance on the current waterbody_poly_id, skip
    if (check_id_in_df(new_df, current_waterbody_poly_id, grouping_id=NULL)) {
    	next
    }
    # Append the current grouping_id and waterbody_poly_id to the new data frame
    # print(glue('Appending A {wetland_i}, {current_waterbody_poly_id}'))
    new_df <- rbind(new_df, data.frame(grouping_id = wetland_i, setNames(list(current_waterbody_poly_id), field1)))
    # Append the current grouping_id and current_waterbody_poly_id(s) to the new data frame
    # print(glue('Appending A {rep(wetland_i, length(current_overlapping_waterbody_poly_id))}, {current_overlapping_waterbody_poly_id}'))
	new_rows <- data.frame(
  							grouping_id = rep(wetland_i, length(current_overlapping_waterbody_poly_id)),
  							setNames(list(current_overlapping_waterbody_poly_id), field1)
	)
    new_df <- rbind(new_df, new_rows)

    # Assign overlapping ids to a list
    if (length(current_overlapping_waterbody_poly_id) > 1){
      ids_to_check <- as.list(current_overlapping_waterbody_poly_id)
    } else {
  	  ids_to_check <- list(current_overlapping_waterbody_poly_id)
    }
	## Iterate over the overlapping ids in a recursive like loop to capture the ids that overlap
	## the iteration ensure to capture the entire wetland complex
    while (length(ids_to_check) > 0) {
      for (id_to_check in ids_to_check) {

  		# print(glue('Assessing id: {id_to_check}'))
        # Check for any records in waterbody_poly_id that match the current overlapping_waterbody_poly_id
        matching_rows <- which(df[, field1] == id_to_check)
        # If there are matching rows, append their overlapping_waterbody_poly_id to the new data frame with the initial grouping_id
        if (length(matching_rows) > 0) {
  		  current_overlapping_waterbody_poly_id <- df[, field2][matching_rows]
  		  if (length(current_overlapping_waterbody_poly_id) > 1){
  		  	new_ids_to_check <- as.list(current_overlapping_waterbody_poly_id)
  		  } else {
  		  	new_ids_to_check <- list(current_overlapping_waterbody_poly_id)
  		  }
  		  # Append the current grouping_id and overlap_id to the new data frame
  		  for (overlap_id in new_ids_to_check) {
  		    ## only rbind if it doesn't already exist
  		    if (!(check_id_in_df(new_df, overlap_id, wetland_i))) {
  		  	# print(glue('Appending L {wetland_i}, {overlap_id}'))
		  	new_df <- rbind(new_df, data.frame(grouping_id = wetland_i, setNames(list(overlap_id), field1)))
  		  	# print(glue('Adding {overlap_id} to ids_to_check'))
  		  	ids_to_check <- append(ids_to_check, list(overlap_id))
  		    }
  		  }
  		  # remove id from ids to check
  		  # print(glue('removing a {id_to_check} from ids_to_check'))
  		  ids_to_check <- remove_from_list(ids_to_check, id_to_check)
        } else {
  		  # print(glue('removing b {id_to_check} from ids_to_check'))
  		  ids_to_check <- remove_from_list(ids_to_check, id_to_check)
  	    }
      }
    }
  }
  return(new_df)
}

## Test out group_overlapping_ids function
# test_input <- data.frame(
#   waterbody_poly_id             = c(10, 10, 11, 21, 20, 20, 24, 55, 22, 22, 22, 20),
#   overlapping_waterbody_poly_id = c(20, 21, 22, 23, 24, 25, 55, 66, 88, 89, 11, 10)
# )

# expected_results <- data.frame(
# 	grouping_id       = c(1 ,1 ,1 ,1 ,1 ,1 ,1 ,1 ,3 ,3 ,3 ,3 ),
# 	waterbody_poly_id = c(10,20,21,24,25,23,55,66,11,22,88,89)
# )

# results <- group_overlapping_ids(test_input, 'waterbody_poly_id', 'overlapping_waterbody_poly_id')
# all.equal(results, expected_results)

## get_dem function extracts the unit's dem from the image warehouse...need network access/VPN active and a mapped path to the image warehouse
## imagefiles.bcgov\imagery (mapped to R: on my computer)
# function has two arguments:
  # bnd_path: path tsa fgdb that includes a 'bnd' feature class.
  # clip: the bounding box for the tsa.

get_dem <- function(clip) {
	message("Executing get_dem function to extract provincial dem from image warehouse")
	dem_path <-  "R:\\dem\\elevation\\trim_25m\\bcalbers\\tif\\bc_elevation_25m_bcalb.tif"
	bc_dem <- rast(dem_path)  #get provincial dem and convert to terra::SpatRaster
	unit_elev <- terra::crop(bc_dem, clip)  #clip provincial raster to unit bounding box
	rm(bc_dem) # remove the provincial dem
	return(mask(unit_elev, clip)) # clip elevation raster from bounding box down to the unit boundary
}

## The get_slope() function converts a unit's dem to percent slope
  # one function argument of dem: the dem raster (e.g., 'elevation' in operability script.)

get_slope<- function(dem, clip){
	message("executing get_slope function to convert unit dem to percent slope")
	slp_degrees <- terra::terrain(dem, "slope") # use terra::terrain function to convert dem to degrees slope
	bc_slope<- tan(pi / 180 * slp_degrees) * 100 # convert degrees to percent
	##  HDE: no longer calculating slope on the fly as it takes too long to run the province
	##  instead, slope has been written to: S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_slope_25m_bcalb.tif
	##  leaving above code to see how it was originally calculated
	# slope_path <- "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_slope_25m_bcalb.tif"
	# bc_slope <- rast(slope_path)  #get provincial slope and convert to terra::SpatRaster
	unit_slope <- terra::crop(bc_slope, clip)  #clip provincial raster to unit bounding box
	rm(bc_slope) # remove the provincial slope
	return(mask(unit_slope, clip)) # clip slope raster from bounding box down to the unit boundary
}

## get_stability function extracts stability table to spatial object
# stability table needs to exist in postgres. Function arguments:
  # db = postgress connection
  # stab_query = sql query to the terrain stability data
  # ras_template_25m is the 25m raster template bounding box. E.g., ras_template_25m <-ras_template25(out_extents,tsa_lbl,25)
# NOTE: "class2" is the name given to the binary field in the stability table in postgress. consider adding this as a function argument so that one can easily adjust based on the variable name.

get_stability<-function(db, stab_query, ras_template_25m, field){ # extract stability table from postgres using 25m template ("x")
  op <- st_read(db, query = stab_query) #use sf::st_read function to create stability spatial object
  spatial_op <- vect(op) # convert to terra: SpatVector
  return(terra::rasterize(spatial_op, ras_template_25m, field)) # rasterize on binary field "class2"
}


# the create_sampler() function extracts the blocks within the unit (specified in the query) - creates a spatial vector of blks
# for example, sampler<-create_sampler(db,blk_query)
# function arguments:
  # db = database connection
  # q = SQL query to the data. for example, blk_query<-"select cc_harvest_year, wkb_geometry from tsa02_ar_table join tsa02_skey using (ogc_fid) where cc_harvest_year > 0;"

create_sampler <- function(db, q){
  blks <- st_read(db, query = q)
  return(vect(blks))
}

get_raster_99th_perc <- function(in_raster, sampler) {
	blk_elev <- terra::extract(in_raster, sampler) # extract the in_raster within the boundary of 'sampler' in this case cutblocks.
	blkelev99 <- quantile(blk_elev[,2], probs = 0.99, na.rm = TRUE) # determine 99th percentile
	message("Cut block 99 percentile is ", blkelev99)
	rm(blk_elev)
	return(blkelev99)
}

# The elev_inop() function determines the 99 percentile of elevation within blocks ('sampler' output)
# function arguments:
  # elevation: the 25m elevation raster
  # sampler: the spatial vector of blks
  # unit: The spatial vector of the boundary of interest (e.g., unit<-create_unit(bnd_path))

calc_inop <- function(in_raster, unit, mgmt_unit, clip_99th, threshold_type, conn_list, schema, threshold_table){

	unit_elev2 <- terra::extract(in_raster, unit) # extract in_raster for the unit
	unit100th <- quantile(unit_elev2[,2], probs = 1, na.rm = TRUE) # determine maximum elevation within the unit.
	message("The unit maximum is ", unit100th)
	rm(unit_elev2)

	if (unit100th < clip_99th) {
		clip_99th <- unit100th
	}

	rast_cutoff <- c(-Inf, clip_99th, 0, clip_99th, unit100th, 1, unit100th, Inf, 0)
	rast_matrix <- matrix(rast_cutoff, ncol = 3, byrow = TRUE)
	### reclassify elevation raster based on cutoffs
	df <- data.frame(cutblock_percentile_99 = clip_99th, unit_max = unit100th, man_unit = mgmt_unit, grid = threshold_type)
	df_to_pg(Id(schema = schema, table = threshold_table), df, conn_list, overwrite=FALSE, append=TRUE)
	return(terra::classify(in_raster, rast_matrix))
}


# determine the 99th percentile of slope within block boundaries.
slp_inop <- function(slope, sampler, unit, mgmt_unit, clip_99th, conn_list){
	message("Executing slp_inop function to sample slope by cut block to determine 99 percent cutoff")
	blk_slp <- terra::extract(slope,sampler)
	blk_slp99 <- quantile(blk_slp$slope, probs = 0.99, na.rm = TRUE)
	message("Cut block slope 99 percentile is ", blk_slp99)
	rm(blk_slp)

	unit_slp <- terra::extract(slope, unit)
	unit_slp100 <- quantile(unit_slp$slope, probs = 1, na.rm = TRUE)
	message("The unit maximum is ", unit_slp100)
	rm(unit_slp)

	slp_cutoff <- c(-Inf, blk_slp99, 0, blk_slp99, unit_slp100, 1, unit_slp100, Inf, 0)
	slp_matrix <- matrix(slp_cutoff, ncol = 3, byrow = TRUE)
	df <- data.frame(cutblock_percentile_99 = blk_slp99, unit_max = unit_slp100, mgmt_unit = mgmt_unit, grid = 'slope')
	df_to_pg(Id(schema = 'thlb_proxy', table = glue('inoperable_thresholds')), df, conn_list, overwrite=FALSE, append=TRUE)

	#### reclassify elevation slope based on cutoff
	return(terra::classify(slope, slp_matrix))
}

# The aggregate function combines the area that is inoperable due to elevation, stability and slope and aggregates to 100m resolution.
# function arguments:
  # stability: binary raster where 1 = unstable (inoperable); 0 = stable (operable)
  # inoperable_elevation: binary raster where 1 = high elevation > 99th percentile (inoperable); 0 = elevation within practice limits (operable)
  # inoperable_slope: binary raster where 1 = high slope > 99th percentile (inoperable); 0 slope within practice limits (operable)
  # out_ras_template: the 100m resolution raster template for the unit.

aggregate <- function(stability, inoperable_elevation, inoperable_slope, out_ras_template) {
	message("Executing aggregation function to calculate proportion inoperable")
	# combine stability, slope and elevation rasters to get overall inoperable raster. values range from 0 (operable) to 3 (all three variables inoperable)
	# message("Performing raster math")
	res25m <- stability + inoperable_slope + inoperable_elevation
	# writeRaster(res25m, "data\\analysis\\res25m.tif", overwrite=TRUE)

	### reclassify the raster values:
	## if the value in the res25m raster cell is between -Inf and 0, the new value is 0;
	## between 1 and 3, the new value is 1;
	## between 3 and Inf, value is 0.
	## Essentially any cell where at least one factor is 'inoperable' gets a value of 1.
	res_cutoff <- c(-Inf, 0, 0, 1, 3, 1, 3, Inf, 0)
	res_matrix <- matrix(res_cutoff, ncol = 3, byrow = TRUE)
	# message("Reclassifying raster math product")
	res_reclass <- terra::classify(res25m, res_matrix)
	# writeRaster(res_reclass, "data\\analysis\\res_reclass.tif", overwrite=TRUE)


	#plot(res_reclass)
	## aggregate to 1 ha.cells with a value of 16 (4*4) representing 100% inoperable
	## cells with values < 16 and are partially inoperable
	# message("Aggregating from 25m to 100m raster")

	res_agg <- terra::aggregate(res_reclass, fact = 4, fun = "sum", na.rm = TRUE)
	# writeRaster(res_agg, "data\\analysis\\res_agg.tif", overwrite=TRUE)


	# plot(res_agg)
	## calculate the proportion of cell that is inoperable - scale by 1000.
	resultant <- round(res_agg * 1000 / 16)
	# writeRaster(resultant, "data\\analysis\\resultant.tif", overwrite=TRUE)

	# plot(resultant)

	# set the extent of the result to match the 100m raster template
	ext(resultant) <- ext(out_ras_template)

	# identify extent of 100m raster template:
	#e<-ext(out_ras_template)
	# assign the 100m raster template extent to the resultant:
	#ext(resultant)<-e
	# convert the results and the template to data frames
	tmp2 <- as.data.frame(out_ras_template, cells = TRUE)
	tmp3 <- as.data.frame(resultant, cells = TRUE)

	# merge the data frame based on cell ID
	return(left_join(tmp2, tmp3, by = join_by("cell")))
}


# This function reads and cleans two ecas data files, combines them and performs data filtering and calculation.
# ecas_interior_path_2023 Path to the 2023 data CSV file.
# ecas_interior_path_2016 Path to the 2016 data CSV file.
# tsa_num = TSA number for filtering.

# the function returns a list containing the 1st percentile minimum harvest volume (mhv) and the filtered data frame "tcas_df".
#
get_ecas <- function(ecas_interior_path_2023, ecas_interior_path_2016, tsa_num=27){
	message("Executing get_ecas function which cleans and combines stone queries: ignore warnings")

	# Read data from CSV files:
	ecas2023 <- read_csv(ecas_interior_path_2023,
						show_col_types = FALSE) %>%
	rename_all(~ str_replace(., "^\\S* ", "")) %>% # Strip any leading non-whitespace characters and spaces
	clean_names() %>% # make labels snake_case (each space is replaced with an underscore)
	mutate_if(is.numeric, ~ replace_na(., 0)) # if numeric replace NAs with zeros



	ecas2016 <- read_csv(ecas_interior_path_2016,
						show_col_types = FALSE) %>%
		# Remove all starting 'x' from column names
		# rename_all(~ str_replace(., "^x", "")) %>%
		# Strip any leading non-whitespace characters and spaces
		rename_all(~ str_replace(., "^\\S* ", "")) %>%
		clean_names() %>%
		# Convert variables from character to numeric
		mutate(
			man_unit = as.numeric(man_unit),
			indicated_rate = as.numeric(indicated_rate),
			total_toa_amount = as.numeric(total_toa_amount)
		) %>%
		mutate_if(is.numeric, ~ replace_na(., 0))

  # Identify columns with type mismatches and remove them:
  col_list <- compare_df_cols(ecas2023,  # dump cols from 2016 with type mismatch
                              ecas2016,
                              return = "mismatch") %>%
    select(column_name) %>%
    pull()

	#   ecas2016 <- ecas2016 %>% select(all_of(-col_list)) # removes columns from ecas2016 that are not in ecas 2023
	ecas2016 <- ecas2016 %>%
		select(-one_of(col_list))  # Use -one_of() to drop the columns


  # combine the clean data:
  ecas <- bind_rows(
    list(
      e2023 = ecas2023,
      e2016 = ecas2016
    ),
    .id = "id"
  ) %>%
    mutate_if(is.numeric, ~ replace_na(., 0)) %>%
    remove_empty("cols") %>%
    group_by(mark) %>%
    # only keep record with most recent appraisal effective date.
    arrange(desc(app_eff_date)) %>%
    slice(1) %>%
    ungroup()

  # Filter data based on TSA number and licence types (limit the sample to major licensees and BCTS):
  tsa <- ecas %>%
    filter(man_unit == tsa_num) %>%
    filter(!str_detect(licence, "T|L|W")) # filter to records where the licence does not contain "T", "L", or "W".

  # calculate the volume per hectare based on harvest system.
  # gscc = ground skidding clear cut
  # chgcc = cable/highlead volume
  tsa <- tsa %>% mutate(tvph = case_when( # assign system values to TVPH variable
    gscc_vol_ha > 0 & chgcc_vol_ha == 0 ~ gscc_vol_ha,
    chgcc_vol_ha > 0 & gscc_vol_ha == 0 ~ chgcc_vol_ha,
    TRUE ~ ncv / tot_merch_area
  ))

  # filter to ground skid clear cut timber marks.
  sub <- tsa %>% filter(gscc_vol > 0 & chgcc_vol_ha == 0)

  mhv01<-quantile(sub$gscc_vol_ha,
           probs = .01)
  message(" The 1 percentile minimum harvest volume is ", mhv01 )

  # store/return as a list with the mhv variable (1st percentile of observed min vol/ha) and the combined/cleaned ecas data frame.
  # minimum vol/ha is for ground_skid only and includes all species.
  return(list(mhv = mhv01, tcas_df = tsa))
}

#netdown functions

# table styling. Argument requires the input of a dataframe, x:
# specifies commas as 1000 separator, font size, caption, etc
pretty_table<-function(x){
  kable(x,
        booktabs = T,
        format.args = list(big.mark = ","),
        caption =  "Netdown Summary Table" ) %>%
    kable_styling(bootstrap_options = "striped",
                  font_size = 10)
}


# function that looks for items 'not in' a variable/list.
`%notin%` <- Negate(`%in%`) # create a 'not in' list function

# setup the netdown summary table:
# required input is the netdown table
setup_tracking_variable<-function(netdown_tab){
  landclass <- "Provincial_Boundary"
  total <- nrow(netdown_tab)
  unit <- nrow(netdown_tab)
  percent <- 100
  net_excluded <- 0
  running_total <- 0
  netdown<-unit - running_total

  netdown_summary <- data.frame(landclass, total, percent, net_excluded, running_total, netdown)

}

# function to pul the 'running_total' value from the netdown summary for a given landclass. RUnning total is the cumulative area removed at a given landclass.
# Required function arguments are a netdown summary table and the landclass

get_running_total<-function(netdown_summary,lc){
  netdown_summary%>%
    filter(landclass == lc)%>%
    select(running_total)%>%
    # pull out the running total
    pull()
}

# function to pull the netdown value from the netdown summary table.
get_netdown<-function(netdown_summary,lc){
  netdown_summary%>%
    filter(landclass == lc)%>%
    select(netdown)%>%
    pull()
}

# function to update the areas in the netdown table:
# Function arguments: netdown table, variable name.
# update areas 1 is used prior to defining AFLB. Once AFLB is defined, it will not be adjusted following future netdown steps. Move to update_areas2 for updates to thlb_net and gthlb_net variables.
update_areas_fmlb <- function(netdown_tab,var_name) {
  netdown_tab%>%
    # update the aflb, thlb_net and gthlb_net variables. If the variable specified is NA, then leave aflb, thlb_net, and gthlb as is. If the variable is NA then update aflb, thlb_net and gthlb to 0.
    mutate(fmlb = if_else(is.na(get(var_name)), fmlb, 0),
           falb = if_else(is.na(get(var_name)), falb, 0),
		   aflb = if_else(is.na(get(var_name)), aflb, 0),
           thlb_net = if_else(is.na(get(var_name)), thlb_net, 0))

}

update_areas_falb <- function(netdown_tab,var_name) {
  netdown_tab%>%
    # update the aflb, thlb_net and gthlb_net variables. If the variable specified is NA, then leave aflb, thlb_net, and gthlb as is. If the variable is NA then update aflb, thlb_net and gthlb to 0.
    mutate(falb = if_else(is.na(get(var_name)), falb, 0),
		   aflb = if_else(is.na(get(var_name)), aflb, 0),
           thlb_net = if_else(is.na(get(var_name)), thlb_net, 0))
}

update_areas_aflb <- function(netdown_tab,var_name) {
  netdown_tab%>%
    # update the aflb, thlb_net and gthlb_net variables. If the variable specified is NA, then leave aflb, thlb_net, and gthlb as is. If the variable is NA then update aflb, thlb_net and gthlb to 0.
    mutate(aflb = if_else(is.na(get(var_name)), aflb, 0),
           thlb_net = if_else(is.na(get(var_name)), thlb_net, 0))
}


# see comments for update_areas1
update_areas_thlb <- function(netdown_tab,var_name) {
  netdown_tab%>%
    mutate(thlb_net = if_else(is.na(get(var_name)), thlb_net, 0))
}


# this function is used when the feature being netted out of the land base is categorical or 100% removed.
# a new row is added to the netdown summary table
# function arguments: netdown table, netdown summary, running_total, land class, netdown step
netdown100pct<-function(netdown_tab, net_summary, running_total, lclass, n_step){
  landclass <- lclass

  # determine the area of the unit (i.e. number of rows in netdown table)
  unit <- nrow(netdown_tab)

  # determine the total area in the netdown step, where n_step is one of the netdown steps (e.g., n01_ownership)
  total <- netdown_tab %>%
    filter(!is.na(get(n_step))) %>% # filter to where netdown step is NOT NA.
    # count number of rows (i.e. area as 1 row = 1 ha)
    count()%>%
    # pull the count value
    pull()

  # determine the percent of TSA area (total of netdown class / total of TSA * 100)
  percent <- total / nrow(netdown_tab) * 100

  # determine the area excluded:
  net_excluded  <- netdown_tab %>%
    # filter to where netdown step variable is NOT NA.
    filter(!is.na(get(n_step))) %>%
    # calculate thlb_net area
    summarise(x = sum(thlb_net, na.rm=TRUE)) %>%
    pull() # pull value

  # calculate new running total value
  running_total <- running_total+net_excluded

  # determine new netdown.
  netdown <- unit - running_total

  # create a one-row data frame with each of the netdown summary variables determined for the netdown step.
  netdown_step <- data.frame(landclass, total, percent, net_excluded, running_total, netdown)

  # add the new netdown step to the netdown summary table.
  netdown_summary <- bind_rows(netdown_summary, netdown_step)

}

# function for determining the proportional netdown factors.
# ONLY works if landclasses are named "Lineal_Features" or "Retention".
netdown_prop<-function(netdown_tab, netdown_summary, running_total, lclass, n_step){
  landclass <- lclass
  unit <- nrow(netdown_tab)
  # conditional statement - slightly different calculation depending on whether landclass is lineal features or retention.
	if (landclass %in% c("Linear_Features", "Physical_Inoperable", "Riparian_Buffers")) {
	total <- netdown_tab %>%
		summarise(x = sum(get(n_step), na.rm = TRUE)) %>%
		pull()
	} else if (landclass == "Future Retention") {
	total <- netdown_tab %>%
		summarise(x = sum(get(n_step), na.rm = TRUE)) %>%
		pull()
	} else {
	total <- netdown_tab %>%
		filter(get(n_step) > 0) %>%
		count() %>%
		pull()
	}

  # calculate percent of total area removed
  percent <- total / unit * 100
  # determine the area excluded
  net_excluded  <- netdown_tab %>%
    filter(thlb_net > 0) %>%
    summarise(x = sum(thlb_net * get(n_step), na.rm=T)) %>%
    pull()

  # calculate running total
  running_total <- running_total + net_excluded

  # calculate netdown
  netdown<-unit - running_total

  # create data frame with netdown summary information for given landclass
  netdown_prop <- data.frame(landclass, total, percent, net_excluded, running_total,netdown)

  # add to netdown summary table:
  netdown_summary <- bind_rows(netdown_summary, netdown_prop)

}

# this function is used when a landbase summary is needed. I.e., AFLB, grossTHLB, THLB:
landbase_sum <- function(netdown_tab,net_summary,running_total,lclass,netdown,which_landbase){
  landclass <- lclass
  total <- netdown_tab %>%
    summarise(x = sum({{ which_landbase }})) %>%
    pull()
  percent <- total / nrow(netdown_tab) * 100
  net_excluded <-running_total


  netdown_landbase <- data.frame(landclass, total, percent, net_excluded , running_total,netdown)
  netdown_summary <- bind_rows(netdown_summary, netdown_landbase)

}


## function only works when input layers are in BC Albers / EPSG: 3005
pixel_weight <- function(template_tif,
                         mask_tif,
                         vect,
                         crop_extent,
                         dst_schema,
                         dst_tbl,
                         pg_conn_param,
                         write_out_raster,
                         raster_path
){
  script_start_time <- Sys.time()
  print(glue("Script started at {format(script_start_time, '%Y-%m-%d %I:%M:%S %p')}"))

  ## create a terra extent object
  terra_extent <- terra::ext(crop_extent[1], crop_extent[2], crop_extent[3], crop_extent[4])
  print(glue('Reading in raster: {template_tif}'))
  template_rast <- terra::rast(template_tif)
  template_raster_datatype <- datatype(template_rast)

  print(glue('Reading in raster: {mask_tif}'))
  mask_rask <- terra::rast(mask_tif)

  rast_lift <- list(template_rast, mask_rask)
  print(glue('Cropping gr_skey grid and mask crop_extent...'))
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


  query <- glue("DROP TABLE IF EXISTS {dst_schema}.{dst_tbl};")
  run_sql_r(query, pg_conn_param)

  query <- glue("CREATE TABLE IF NOT EXISTS {dst_schema}.{dst_tbl} (
		gr_skey INTEGER NOT NULL PRIMARY KEY,
		fact numeric NOT NULL)")
  run_sql_r(query, pg_conn_param)

  vect_extent <- terra::ext(vect)
  rast_clipped <- terra::crop(gr_skey_rast, vect_extent)

  ## the MAGIQUE!
  results <- terra::extract(rast_clipped, vect, weights = TRUE, na.rm = TRUE)

  ## within the results, records sometimes exist where bc_01ha_gr_skey IS NULL
  ## this happen on the coast when the raster has been masked but the linear features
  ## exists outside the mask
  ## They are not needed, remove records with NULL values in bc_01ha_gr_skey
  results <- results[complete.cases(results$bc_01ha_gr_skey), ]

  ## if there is more than 1 gr_skey being returned - sum all duplicates together and if the resultant is > 1, set to 1.
  sum_weight_by_bc_01ha_gr_skey <- results %>%
    group_by(bc_01ha_gr_skey) %>%
    summarise(fact = sum(weight), .groups = "drop") %>%
    mutate(fact = ifelse(fact > 1, 1, fact))

  colnames(sum_weight_by_bc_01ha_gr_skey) <- c('gr_skey', 'fact')
  ## write results to a postgres table
  df_to_pg(Id(schema = dst_schema, table = dst_tbl), sum_weight_by_bc_01ha_gr_skey, pg_conn_param, overwrite=TRUE)

  ## build a helpful table comment
  today_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
  tbl_comment <- glue("COMMENT ON TABLE {dst_schema}.{dst_tbl} IS 'Table created at {today_date}.")
  run_sql_r(query, pg_conn_param)

  if (!is.null(raster_path)) {
    print(glue("Writing out result raster to {raster_path}"))

    # Create a copy of gr_skey_rast to hold the output values
    out_rast <- gr_skey_rast
    values(out_rast) <- NA

    # Extract raster values (gr_skey per cell)
    gr_skey_vals <- values(gr_skey_rast)

    # Lookup table
    fact_df <- sum_weight_by_bc_01ha_gr_skey

    # Use match() to align raster cell values with fact values
    idx <- match(gr_skey_vals, fact_df$gr_skey)

    # Fill output raster with matched fact values (NA where no match)
    out_vals <- fact_df$fact[idx]

    # Assign new values
    values(out_rast) <- out_vals

    # Write out
    terra::writeRaster(out_rast, raster_path, overwrite = TRUE)
  }
  end_time <- Sys.time()
  duration <- round(difftime(end_time, script_start_time, units = "mins"), 2)
  print(glue('Script finished. Duration: {duration} minutes.'))
}


#### ### recreation
#### Below is a specific list of ownership classes excluded:
####
#### + 66N, Crown - Recreation Area
#### + 68U, Crown - Forest Recreation
####
#### **Data sources:** BCGW file: WHSE_FOREST_VEGETATION.F_OWN
####
#### Forest Recreation sites were not excluded from the THLB as there are no harvesting restrictions.
####
#### ```{r thlb_rec, eval= FALSE}
#### ## NOT RAN
#### lclass<-"Recreation_Features"
#### n_step<-"n08_rec"
####
#### netdown_summary<-netdown100pct(netdown_tab,netdown_summary,running_total,lclass,n_step)
#### netdown_tab<-update_areas_thlb(netdown_tab,n_step)
#### running_total<-get_running_total(netdown_summary,lclass)
#### netdown<-get_netdown(netdown_summary,lclass)
#### pretty_table(netdown_summary)
#### ```
