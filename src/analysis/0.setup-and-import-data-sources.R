#  install.packages("RPostgres")
#  install.packages("glue")
#  install.packages("terra")
#  install.packages("keyring")
#  install.packages("sf")
#  install.packages("devtools")
#  install.packages("DT")
#  library(devtools)
#  install_github("bcgov/FAIB_DADMTOOLS")
library(dadmtools)

repo_path <- 'C:/projects/FAIB_PROXY_THLB'
setwd(repo_path)

library(keyring)
keyring_create("localpsql")
key_set("dbuser", keyring = "localpsql", prompt = 'Postgres keyring dbuser:')
key_set("dbpass", keyring = "localpsql", prompt = 'Postgres keyring password:')
key_set("dbhost", keyring = "localpsql", prompt = 'Postgres keyring host:')
key_set("dbname", keyring = "localpsql", prompt = 'Postgres keyring dbname:') ## thlb_proxy

keyring_create("oracle")
key_set("dbuser", keyring = "oracle", prompt = 'Oracle keyring dbuser:')
key_set("dbpass", keyring = "oracle", prompt = 'Oracle keyring password:')
key_set("dbhost", keyring = "oracle", prompt = 'Oracle keyring host:')
key_set("dbservicename", keyring = "oracle", prompt = 'Oracle keyring serviceName:')
key_set("dbserver", keyring = "oracle", prompt = 'Oracle keyring server:')

conn_list <- dadmtools::get_pg_conn_list()

## If starting from a fresh db, install required extensions & schemas
query <- "CREATE EXTENSION IF NOT EXISTS postgis;"
run_sql_r(query, conn_list)

query <- "CREATE EXTENSION IF NOT EXISTS postgis_raster;"
run_sql_r(query, conn_list)

query <- "CREATE EXTENSION IF NOT EXISTS oracle_fdw;"
run_sql_r(query, conn_list)

query <- "CREATE SCHEMA IF NOT EXISTS raster;"
run_sql_r(query, conn_list)

query <- "CREATE SCHEMA IF NOT EXISTS whse;"
run_sql_r(query, conn_list)



## Run if you don't have the following layer in whse schema:
import_gr_skey_tif_to_pg_rast(
    out_crop_tif_name = glue('{repo_path}/data/output/bc_01ha_gr_skey.tif'),
    template_tif      = "//spatialfiles2.bcgov/archive/FOR/VIC/HTS/ANA/workarea/PROVINCIAL/bc_01ha_gr_skey.tif",
    mask_tif          = "//spatialfiles2.bcgov/archive/FOR/VIC/HTS/ANA/workarea/PROVINCIAL/BC_Boundary_Terrestrial.tif",
    crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5),
    pg_conn_param     = dadmtools::get_pg_conn_list(),
    dst_tbl           = 'whse.all_bc_gr_skey',
    rast_sch          = "raster",
    pg_rast_name      = "grskey_bc_land",
    geom_type         = 'Centroid'
)

## If starting from a fresh db, manually download the following layers prior to running the next snippet
## 1: Downloaded 2016 from: https://catalogue.data.gov.bc.ca/dataset/vri-historical-vegetation-resource-inventory-2002-2022-
## to <repo_path>\data\input\VEG_COMP_LYR_R1_POLY1_2016\VEG_COMP_LYR_R1_POLY.gdb

## 2. Download the following layers to <repo_path>\data\input
## \\spatialfiles2.bcgov\work\FOR\VIC\HTS\DAM\Staff_WorkArea\heckstrand\thlb_proxy\local_inputs\fwa_stream_networks_channel_width.csv
## \\spatialfiles2.bcgov\work\FOR\VIC\HTS\DAM\Staff_WorkArea\heckstrand\thlb_proxy\local_inputs\BCTS_Field_Streams.gpkg
## \\spatialfiles2.bcgov\work\FOR\VIC\HTS\DAM\Staff_WorkArea\heckstrand\thlb_proxy\local_inputs\fishpassage.gpkg
## \\spatialfiles2.bcgov\work\FOR\VIC\HTS\DAM\Staff_WorkArea\heckstrand\thlb_proxy\local_inputs\stream_reaches_with_contributing_areas_outside_bc.sql
## \\spatialfiles2.bcgov\work\FOR\VIC\HTS\DAM\Staff_WorkArea\heckstrand\thlb_proxy\local_inputs\stream_reaches_within_floodplain_where_width_less_100m.sql
## \\spatialfiles2.bcgov\work\FOR\VIC\HTS\DAM\Staff_WorkArea\heckstrand\thlb_proxy\local_inputs\final_gaps_current_noncurrent_lidar_treedPercent.sqlite (for FMLB)
## \\spatialfiles2.bcgov\work\FOR\VIC\HTS\DAM\Staff_WorkArea\heckstrand\thlb_proxy\local_inputs\final_gaps_lidarProgam_contained_treedpercent.sqlite (for FMLB)
## \\spatialfiles2.bcgov\archive\FOR\VIC\HTS\ANA\workarea\PROVINCIAL\bc_01ha_gr_skey.tif (for linear_features rasterizing)
## \\spatialfiles2.bcgov\archive\FOR\VIC\HTS\ANA\workarea\PROVINCIAL\BC_Boundary_Terrestrial.tif (for linear_features rasterizing)
##



## Go through the config_parameters.csv and update the data source path & layer name to the most recent layer for the following layers:
## VEG_COMP_LYR_R1_POLY_INTERNAL --> new year
## harvest restrictions --> most recent
##


batch_import_to_pg_gr_skey(
  in_csv            = glue('{repo_path}/data/input/config_parameters.csv'),
  pg_conn_param     = dadmtools::get_pg_conn_list(),
  ora_conn_param    = dadmtools::get_ora_conn_list(),
  crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5),
  gr_skey_tbl       = 'whse.all_bc_gr_skey',
  raster_schema     = 'raster',
  template_tif      = "//spatialfiles2.bcgov/archive/FOR/VIC/HTS/ANA/workarea/PROVINCIAL/bc_01ha_gr_skey.tif",
  mask_tif          = "//spatialfiles2.bcgov/archive/FOR/VIC/HTS/ANA/workarea/PROVINCIAL/BC_Boundary_Terrestrial.tif",
  data_src_tbl      = 'whse.data_sources',
  out_tif_path      = glue('{repo_path}/data/output/'),
  import_rast_to_pg = FALSE
)


## after you've successfully imported all the needed data sources or updated where you need to run, run the following scripts
## The following recreates the man_unit_gr_skey table needed for later analysis.

## 1.scripts: importing data sources:
run_sql_psql(sql_var=NULL, sql_file = 'src/analysis/0.recreate_man_unit_gr_skey.sql', 'thlb_proxy')

## Note: the function: import_bcgw_to_pg relies on he oracle foreign server: oradb existing. If working on a fresh db that doesn't have the oracle foreign server set up yet - be sure to import layers using dadmtools first as it will set up the oracle foreign server
source('src/analysis/1.fmlb-import-layers.R')
source('src/analysis/1.linear-features-import-layers.R')
source('src/analysis/1.non-commercial-species-import-layers.R')
## RHIAN no need to rerun the creation of physical inoperability while Hailey is away
source('src/analysis/1.physical-inoperability-import-layers.R')
source('src/analysis/1.retention-import-layers.R')
source('src/analysis/1.riparian-import-layers.R')

## 2.scripts: processing data sources:
source('src/analysis/2.fmlb-creation.R')
source('src/analysis/2.merchantability-processing.R')
source('src/analysis/2.non-commercial-species-processing.R')

## note: if bad_alloc ERROR happens - run at the end of the day & close all other applications - very RAM intensive script
source('src/analysis/2.physical-inoperability-processing.R')
source('src/analysis/2.retention-processing.R')
## the following script does not need to be reran again (possibly ever) unless a bug is found
# source('src/analysis/2.riparian-fwapg-pre-processing.R')

## 3.scripts classification/rasterization/netdown creation
source('src/analysis/3.linear-features-raster-weight.R')
source('src/analysis/3.merchantability-classification.R')
source('src/analysis/3.non-commercial-species-classification.R')
source('src/analysis/3.physical-inoperability-classification.R')
source('src/analysis/3.netdown-create-table.R')

## open up 4.netdown.Rmd in RStudio and run with knit on.
