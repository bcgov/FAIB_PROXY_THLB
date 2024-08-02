library(dadmtools)
library(dplyr)

linear_weight <- function(
                               template_tif      = "data\\input\\bc_01ha_gr_skey.tif",  ## "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif"
                               mask_tif          = "data\\input\\BC_Boundary_Terrestrial.tif", ## 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
                               crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5),
							   dst_schema        = "public",
							   dst_tbl           = "riparian_gr_skey",
							   pg_conn_param     = pg_conn_param,
							   fgdb_filename     = "data\\input\\rip.gdb",
							   fgdb_layername    = "buffers"
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



	query <- glue("CREATE TABLE IF NOT EXISTS {dst_schema}.{dst_tbl} (
		gr_skey INTEGER NOT NULL PRIMARY KEY,
		fact numeric NOT NULL);")
	run_sql_r(query, pg_conn_param)
	

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

	sp_layer <- st_read(dsn = fgdb_filename, layer = fgdb_layername) 
	tryCatch({
		vect <- st_cast(sp_layer, "MULTIPOLYGON")
	}, error = function(e){
		print(glue("reading in of vector did not work. Error: {e}"))
		## in the case of an error - wrap a buffer within 0.0001 width to 'fix'
		## if you need later
		# vect <- st_cast(st_read(conn, query = glue(spatial_query_when_error), crs = 3005), "MULTIPOLYGON")
	})



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
	df_to_pg(Id(schema = dst_schema, table = glue('{dst_tbl}')), sum_weight_by_bc_01ha_gr_skey, pg_conn_param, overwrite=TRUE)
	## build a helpful table comment
	end_time <- Sys.time()
	duration <- round(difftime(end_time, script_start_time, units = "mins"), 2)
	print(glue('Script finished. Duration: {duration} minutes.'))
}


library(dadmtools)
library(dplyr)
conn_list <- get_pg_conn_list()

linear_weight(template_tif    = "data\\input\\bc_01ha_gr_skey.tif",  ## "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif"
			mask_tif          = "data\\input\\BC_Boundary_Terrestrial.tif", ## 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
			crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5),
			dst_schema        = "public",
			dst_tbl           = "riparian_gr_skey",
			pg_conn_param     = conn_list,
			fgdb_filename     = "data\\input\\rip.gdb",
			fgdb_layername    = "buffers"
)