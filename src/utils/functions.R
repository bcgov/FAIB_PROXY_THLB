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
					(select * FROM {grid_tbl} grd JOIN (select * from whse_sp.tsa_boundaries_2020 where tsa IN (11)) tsa ON ST_Intersects(grd.geom, tsa.geom)) grid
				LEFT JOIN
					{dst_schema}.{dst_tbl}_status status
				USING ({grid_loop_fld})
					WHERE status.{grid_loop_fld} is null
				GROUP BY grid.{grid_loop_fld};
				")
	print(query)
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
			print(glue(spatial_query))
			browser()
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
			ELSE EXCLUDED.fact + d.fact
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
}


import_bcgw_to_pg <- function(
                               src_schema    = "WHSE_BASEMAPPING",
                               src_layer     = "GBA_RAILWAY_TRACKS_SP",
							   fdw_schema    = "load",
							   dst_schema    = "whse_sp",
							   dst_layer     = "GBA_RAILWAY_TRACKS_SP",
                               layer_id      = "railway_track_id",
							   geometry_name = "shape",
							   geometry_type = "MultiLineString",
							   grouping_name = "railway",
							   pg_conn_list

) {
	query <- glue('DROP FOREIGN TABLE IF EXISTS {fdw_schema}.{src_layer};')
	run_sql_r(query, pg_conn_list)
	query <- glue('IMPORT FOREIGN SCHEMA "{src_schema}" LIMIT TO ({src_layer}) FROM SERVER oradb INTO {fdw_schema};')
	run_sql_r(query, pg_conn_list)
	query <- glue('DROP TABLE IF EXISTS {dst_schema}.{dst_layer};')
	run_sql_r(query, pg_conn_list)
	if (is.null(grouping_name)){
	query <- glue("CREATE TABLE {dst_schema}.{dst_layer} as
	SELECT
		{layer_id},
		ST_Force2d({geometry_name})::geometry({geometry_type}, 3005) as geom
	FROM
		{fdw_schema}.{src_layer};")
	} else {
	query <- glue("CREATE TABLE {dst_schema}.{dst_layer} as
	SELECT
		{layer_id},
		'{grouping_name}'::text as grouping_name,
		ST_Force2d({geometry_name})::geometry({geometry_type}, 3005) as geom
	FROM
		{fdw_schema}.{src_layer};")
	}
	run_sql_r(query, pg_conn_list)
	query <- glue('CREATE INDEX {dst_layer}_geom_idx on {dst_schema}.{dst_layer} USING gist(geom);')
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
