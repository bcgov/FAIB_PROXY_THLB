library(dadmtools)
source('src/utils/functions.R')
## relies on install_github("bcgov/FAIB_DATA_MANAGEMENT") being installed at some point
conn_list <- dadmtools::get_pg_conn_list()
start_time <- Sys.time()
print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))

query <- 'DROP TABLE IF EXISTS thlb_proxy.prov_netdown'
run_sql_r(query, conn_list)

query <- 'CREATE TABLE IF NOT EXISTS thlb_proxy.prov_netdown
(
    gr_skey integer NOT NULL,
    geom geometry(Point,3005) NOT NULL,
    man_unit character varying COLLATE pg_catalog."default" NOT NULL,
    tsa_rank1 character varying(32) COLLATE pg_catalog."default",
    own text COLLATE pg_catalog."default",
    natural_disturbance character varying(12) COLLATE pg_catalog."default",
    bgc_label character varying(27) COLLATE pg_catalog."default",
    zone character varying(12) COLLATE pg_catalog."default",
    subzone character varying(9) COLLATE pg_catalog."default",
    harvest_start_year_calendar integer,
    opening_id double precision,
    opening_number character varying COLLATE pg_catalog."default",
    bclcs_level_1 character varying COLLATE pg_catalog."default",
    bclcs_level_2 character varying COLLATE pg_catalog."default",
    bclcs_level_3 character varying COLLATE pg_catalog."default",
    bclcs_level_4 character varying COLLATE pg_catalog."default",
    bclcs_level_5 character varying COLLATE pg_catalog."default",
    project character varying COLLATE pg_catalog."default",
    non_productive_descriptor_cd character varying COLLATE pg_catalog."default",
    land_cover_class_cd_1 character varying COLLATE pg_catalog."default",
    land_cover_class_cd_2 character varying COLLATE pg_catalog."default",
    land_cover_class_cd_3 character varying COLLATE pg_catalog."default",
    non_veg_cover_type_1 character varying COLLATE pg_catalog."default",
    for_mgmt_land_base_ind character varying COLLATE pg_catalog."default",
    site_index double precision,
    species_cd_1 character varying COLLATE pg_catalog."default",
    present_land_use_label character varying(100) COLLATE pg_catalog."default",
    waterbody_type character varying(1) COLLATE pg_catalog."default",
    class2 integer,
    number_of_cutblocks integer,
    elev_99th double precision,
    slope_99th double precision,
    inop_fact numeric NOT NULL,
    land_designation_type_code character varying COLLATE pg_catalog."default",
    harvest_restriction_class_rank integer,
    harvest_restriction_class_name character varying COLLATE pg_catalog."default",
    wha_label character varying(50) COLLATE pg_catalog."default",
    non_commercial text COLLATE pg_catalog."default",
    merchantability integer,
    current_retention text COLLATE pg_catalog."default",
    n01_fmlb text COLLATE pg_catalog."default",
    n02_ownership text COLLATE pg_catalog."default",
    n03_ownership text COLLATE pg_catalog."default",
    n04_nonfor text COLLATE pg_catalog."default",
    p05_linear_features numeric,
    n06_parks text COLLATE pg_catalog."default",
    n07_wha character varying COLLATE pg_catalog."default",
    n08_misc text COLLATE pg_catalog."default",
    p09_riparian numeric,
    n10_arch text COLLATE pg_catalog."default",
    n11_harvest_restrictions text COLLATE pg_catalog."default",
    p12_phys_inop numeric,
    n13_non_merchantable text COLLATE pg_catalog."default",
    n14_non_commercial text COLLATE pg_catalog."default",
    CONSTRAINT prov_netdown_pkey PRIMARY KEY (gr_skey)
);'
run_sql_r(query, conn_list)

query <- 'SELECT tsa_rank1 from whse.man_unit_gr_skey WHERE tsa_rank1 is not null group by tsa_rank1'
tsas <- sql_to_df(query, conn_list)


for (i in 1:nrow(tsas)){
	tsa <- tsas$tsa_rank1[i]
	print(glue("INSERTING {tsa} into thlb_proxy.prov_netdown {i}/{nrow(tsas)}"))

	query <- glue("INSERT INTO thlb_proxy.prov_netdown
	WITH vri_species_cd_datadict AS (
		SELECT
			CASE
				WHEN upper(species_cd) IN ('AC','ACB','ACT','AD','AX','DG', 'E','EA','EB','EP', 'ES', 'EW','M','MB','MN', 'MV', 'QG', 'RA', 'VB', 'W', 'WS', 'XH', 'ZH') then 'decid'
				WHEN upper(species_cd) IN ('AT', 'SS', 'SB', 'PA') THEN lower(species_full_name)
				WHEN upper(species_cd) IN ('D', 'DR') THEN 'alder'
				WHEN upper(species_cd) LIKE 'F%' THEN 'fir'
				WHEN upper(species_cd) LIKE 'C%' then 'cedar'
				WHEN upper(species_cd) LIKE 'H%' then 'hemlock'
				WHEN upper(species_cd) LIKE 'S%' then 'spruce'
				WHEN upper(species_cd) LIKE 'B%' then 'balsam'
				WHEN upper(species_cd) IN ('PF','PW') then 'white pine'
				WHEN upper(species_cd) IN ('P','PL','PLI','PLC', 'PJ') then 'pine'
				WHEN upper(species_cd) IN ('PY') then 'yellow pine'
				WHEN upper(species_cd)  LIKE 'L%' then 'larch'
				WHEN upper(species_cd) IN ('YC') then 'cypress'
				ELSE 'other'
			END as species_grouping,
			species_cd,
			species_full_name,
			type
		FROM
			thlb_proxy.vri_species_cd_datadict
	)
	SELECT
		bc.gr_skey,
		bc.geom,
		man_unit.man_unit,
		man_unit.tsa_rank1,
		fown.own_sched as own,

		---- BEC
		bec.natural_disturbance,
		bec.bgc_label,
		bec.zone,
		bec.subzone,

		---- CONSOLIDATED CUTBLOCKS
		cc.harvest_start_year_calendar,

		---- VRI
		vri.opening_id,
		vri.opening_number,
		vri.bclcs_level_1,
		vri.bclcs_level_2,
		vri.bclcs_level_3,
		vri.bclcs_level_4,
		vri.bclcs_level_5,
		vri.project,
		vri.non_productive_descriptor_cd,
		vri.land_cover_class_cd_1,
		vri.land_cover_class_cd_2,
		vri.land_cover_class_cd_3,
		vri.non_veg_cover_type_1,
		vri.for_mgmt_land_base_ind,
		vri.site_index,
		vri.species_cd_1,

		---- BASE THEMATIC MAPPING
		btm.present_land_use_label,

		---- FWA WETLANDS
		wet.waterbody_type,

		--- INOPERABILITY
		inop.class2,
		inop.number_of_cutblocks,
		inop.elev_99th,
		inop.slope_99th,
		CASE
			WHEN inop.class2 > 250 THEN inop.class2::numeric/1000
			ELSE 0
		END AS inop_fact,

		-- HARVEST RESTRICTIONS
		rr.land_designation_type_code,
		rr.harvest_restriction_class_rank,
		rr.harvest_restriction_class_name,

		-- wildlife habitat area
		wha.TIMBER_HARVEST_CODE as wha_label,

		-- non-commercial
		non_com.non_commercial,

		-- merchantability
		merch.merchantability,

		CASE
			WHEN res.silv_reserve_code = 'G' AND res.silv_reserve_objective_code not in ('TIM')
			-- res_inv has filters applied to the subquery
			-- opening is NOT NULL where forest_cover_when_updated >= '2012-01-01' AND DISTURBANCE_START_DATE > '2012-01-01' AND timber_mark is not null
			AND res_inv.opening_id IS NOT NULL
			THEN 'retention: ' || res.silv_reserve_objective_code
			ELSE NULL
		END as current_retention,

		 -- FMLB
		 CASE
		 	WHEN bec.natural_disturbance = 'NDT5' THEN 'NDT5'
		 	-- if its been harvested then its forested
		 	WHEN (nullif(vri.opening_id::text,'0') is not null or nullif(vri.opening_number::text,'0') is not null or cc.harvest_start_year_calendar is not null or ccg.gr_skey is not null) THEN NULL
		 	--
		 	WHEN (coalesce(vri.bclcs_level_1,'') IN ('U', '') AND btm.present_land_use_label IN ('Old Forest', 'Recently Logged', 'Selectively Logged','Young Forest')) THEN NULL
		 	---- WHEN upper(bec_zone || bec_subzone) IN ('SWBMKS', 'SWBUNS', 'SWBVKS') then 'N'
		 	-- exclude where vegetated, not treed and not fire_update
		 	WHEN vri.bclcs_level_1 = 'V' AND coalesce(vri.bclcs_level_2,'') <> 'T' AND vri.project NOT LIKE 'FIRE_UPDATE%' THEN 'not_treed_not_fire'
		 	-- exclude urban that has been classified as forested
		 	WHEN ((trim(vri.non_productive_descriptor_cd) = 'U' OR vri.bclcs_level_5 = 'UR' OR vri.land_cover_class_cd_1 = 'UR' OR vri.land_cover_class_cd_2 = 'UR' OR vri.land_cover_class_cd_3 = 'UR' OR vri.non_veg_cover_type_1 = 'UR')) AND (coalesce(vri.for_mgmt_land_base_ind,'Y') = 'Y') THEN 'URBAN and fmlb_orig = Y'
		 	WHEN vri.for_mgmt_land_base_ind = 'Y' THEN NULL
		 	WHEN vri.for_mgmt_land_base_ind = 'N' THEN 'fmlb_orig = N'
		 END AS n01_fmlb,
		 CASE WHEN fown.own_sched IN ('40N', '41N', '52N', '80N') THEN own_sched_desc ELSE NULL END AS n02_ownership,
		 CASE WHEN fown.own_sched IN ('50N', '51N', '53N', '54N') THEN own_sched_desc ELSE NULL END AS n03_ownership,
		 CASE
		 	-- ALPINE
		 	WHEN vri.bclcs_level_1 = 'N' AND vri.bclcs_level_3 = 'A' THEN 'non_vegetated_alpine_lcs' -- Alpine Rock and Ice
		 	WHEN vri.bclcs_level_2 = 'N' AND vri.bclcs_level_3 = 'A' THEN 'non_treed_alpine_lcs' -- Shrub/Lichen
		 	WHEN bec.bgc_label IN ('BAFAun', 'IMA') THEN 'alpine_bec' -- # BEC sourced Alpine,
		 	-- RIPARIAN
		 	WHEN vri.bclcs_level_1 = 'N' AND vri.bclcs_level_5 = 'LA' THEN 'lake_lcs' -- lakes
		 	WHEN vri.bclcs_level_1 = 'N' AND vri.bclcs_level_5 = 'RE' THEN 'reservoir_lcs' -- reservoir
		 	WHEN vri.bclcs_level_1 = 'N' AND vri.bclcs_level_5 = 'RI' THEN 'riparian_lcs' -- riparian
		 	WHEN vri.bclcs_level_2 = 'T' AND vri.bclcs_level_3 = 'W' THEN 'treed_wetland_lcs' -- treed wetland
		 	WHEN vri.bclcs_level_2 = 'N' AND vri.bclcs_level_3 = 'W' THEN 'non_treed_wetland_lcs'
		 	WHEN vri.bclcs_level_3 = 'W' THEN 'wetlands_lcs' -- nontreed wetland
		 	WHEN wet.waterbody_type = 'W' THEN 'wetland_fwa' -- FWA classification
		 	--  NonVegetated/low productivity
		 	WHEN vri.bclcs_level_1 = 'N' AND cc.harvest_start_year_calendar is null THEN 'non_vegetated_lcs' -- nonvegetated and not an opening
		 	WHEN vri.bclcs_level_2 = 'N' AND vri.bclcs_level_4 NOT IN ('ST', 'SL') AND cc.harvest_start_year_calendar is null THEN 'non_treed_herb_lcs' -- nontreed but not in a cutblock
		 	WHEN vri.bclcs_level_4 IN ('ST', 'SL') AND cc.harvest_start_year_calendar is null THEN 'non_treed_shrub_lcs'
		 	WHEN vri.site_index < 5 AND cc.harvest_start_year_calendar is null THEN 'non_productive_si_vri' -- low productivity stands
		 	WHEN vri.non_productive_descriptor_cd is not null AND cc.harvest_start_year_calendar is null THEN 'FC1_' || vri.non_productive_descriptor_cd -- stand classified in the FC1 as nonproductive
		 	WHEN vri.bclcs_level_1 || vri.bclcs_level_2 = 'VT' AND vri.species_cd_1 is null THEN 'no_species_cd_1' -- species label
		 	WHEN vri.bclcs_level_1 = 'U' OR vri.bclcs_level_1 is null THEN 'unclassified'
		 	ELSE NULL
		 END as n04_nonfor,
		 coalesce(lin.fact,0) as p05_linear_features,
		 CASE WHEN fown.own_sched IN ('60N', '81U') THEN own_sched_desc ELSE NULL END AS n06_parks,
		 CASE WHEN wha.timber_harvest_code IN ('NO HARVEST ZONE', 'NO HARVEST') THEN wha.timber_harvest_code ELSE NULL END AS n07_wha,
		 -- CASE WHEN fown.own_sched IN ('68U', '66N') THEN own_sched_desc ELSE NULL END AS n08_rec,
		 CASE WHEN fown.own_sched IN ('69U', '99N') THEN own_sched_desc ELSE NULL END AS n08_misc,
		 coalesce(rip.fact,0) as p09_riparian,
		 CASE
		 	WHEN arch.pgid IS NOT NULL THEN 'arch sites'
			ELSE NULL
		END AS n10_arch,
		 CASE
		 	WHEN rr.harvest_restriction_class_name IN ('Prohibited', 'Protected') THEN harvest_restriction_class_name || ' - ' || land_designation_type_code
		 	ELSE NULL
		 END AS n11_harvest_restrictions,
		 coalesce(inop.inop_fact,0) as p12_phys_inop,
		 CASE WHEN merch.merchantability = 0 THEN 'non_merchantable' ELSE NULL END as n13_non_merchantable,
		 non_com.non_commercial as n14_non_commercial

	FROM
	whse.all_bc_gr_skey bc
	JOIN (SELECT * from whse.man_unit_gr_skey WHERE tsa_rank1 = '{tsa}') man_unit on man_unit.gr_skey = bc.gr_skey
	LEFT JOIN thlb_proxy.seral_2023_tap_method fmlb on fmlb.gr_skey = bc.gr_skey
	LEFT JOIN whse.f_own_gr_skey fown_key ON bc.gr_skey = fown_key.gr_skey
	LEFT JOIN (SELECT own || schedule as own_sched, own || schedule || ' - ' || ownership_description AS own_sched_desc, pgid FROM whse.f_own) fown USING (pgid)
	LEFT JOIN whse.bec_biogeoclimatic_poly_gr_skey bec_key on bec_key.gr_skey = bc.gr_skey
	LEFT JOIN whse.bec_biogeoclimatic_poly bec ON bec.pgid = bec_key.pgid
	LEFT JOIN thlb_proxy.btm_present_land_use_v1_svw_gr_skey btmg on btmg.gr_skey = bc.gr_skey
	LEFT JOIN thlb_proxy.btm_present_land_use_v1_svw btm on btm.pgid = btmg.pgid
	LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2023_gr_skey vri_key on vri_key.gr_skey = bc.gr_skey
	LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2023 vri on vri.pgid = vri_key.pgid
	LEFT JOIN vri_species_cd_datadict on vri.species_cd_1 = vri_species_cd_datadict.species_cd
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp_jan2025_gr_skey ccg on ccg.gr_skey = bc.gr_skey
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp_jan2025 cc on cc.pgid = ccg.pgid
	LEFT JOIN whse.fwa_wetlands_gr_skey wet_key ON wet_key.gr_skey = bc.gr_skey
	LEFT JOIN whse.fwa_wetlands wet ON wet.pgid = wet_key.pgid
	LEFT JOIN thlb_proxy.bc_linear_features lin ON lin.gr_skey = bc.gr_skey
	LEFT JOIN thlb_proxy.bc_riparian_buffers rip ON rip.gr_skey = bc.gr_skey
	LEFT JOIN thlb_proxy.bc_inoperable_gr_skey inop ON inop.gr_skey = bc.gr_skey
	LEFT JOIN thlb_proxy.bc_merchantability_gr_skey merch ON merch.gr_skey = bc.gr_skey
	LEFT JOIN thlb_proxy.non_commercial_lu_table non_com on non_com.tsa = man_unit.tsa_rank1 and vri_species_cd_datadict.species_grouping = non_com.species_grouping
	LEFT JOIN whse.rr_restriction_202408_gr_skey rr_key on rr_key.gr_skey = bc.gr_skey
	LEFT JOIN whse.rr_restriction_202408 rr on rr.pgid = rr_key.pgid
	LEFT JOIN thlb_proxy.wcp_wildlife_habitat_area_poly_gr_skey wha_key ON wha_key.gr_skey = bc.gr_skey
	LEFT JOIN thlb_proxy.wcp_wildlife_habitat_area_poly wha ON wha.pgid = wha_key.pgid
	LEFT JOIN thlb_proxy.raad_tfm_sites_svw_gr_skey arch_key on bc.gr_skey = arch_key.gr_skey
	LEFT JOIN thlb_proxy.raad_tfm_sites_svw arch ON arch.pgid = arch_key.pgid
	LEFT JOIN thlb_proxy.rslt_forest_cover_reserve_svw_20250110_gr_skey res_key ON res_key.gr_skey = bc.gr_skey
	LEFT JOIN thlb_proxy.rslt_forest_cover_reserve_svw_20250110 res ON res.pgid = res_key.pgid
	LEFT JOIN (SELECT distinct on (opening_id) opening_id FROM thlb_proxy.rslt_forest_cover_inv_svw cov JOIN thlb_proxy.rslt_opening_svw opening USING (opening_id) WHERE forest_cover_when_updated >= '2012-01-01' AND DISTURBANCE_START_DATE > '2012-01-01' AND timber_mark is not null ORDER BY opening_id, forest_cover_when_updated desc) res_inv ON res.opening_id = res_inv.opening_id") ## TODO join with opening id so can filter on disturbance date
	run_sql_r(query, conn_list)
}

query <- "ALTER TABLE thlb_proxy.prov_netdown ADD PRIMARY KEY (gr_skey)"
run_sql_r(query, conn_list)

query <- "ALTER TABLE thlb_proxy.prov_netdown ALTER COLUMN fmlb SET NOT NULL"
run_sql_r(query, conn_list)

query <- "ALTER TABLE thlb_proxy.prov_netdown ALTER COLUMN falb SET NOT NULL"
run_sql_r(query, conn_list)

query <- "ALTER TABLE thlb_proxy.prov_netdown ALTER COLUMN paflb SET NOT NULL"
run_sql_r(query, conn_list)

query <- "ALTER TABLE thlb_proxy.prov_netdown ALTER COLUMN pthlb_net SET NOT NULL"
run_sql_r(query, conn_list)

query <- "CREATE INDEX prov_netdown_man_unit_idx ON thlb_proxy.prov_netdown USING btree(man_unit);"
run_sql_r(query, conn_list)


query <- "CREATE INDEX prov_netdown_tsa_rank1_idx ON thlb_proxy.prov_netdown USING btree(tsa_rank1);"
run_sql_r(query, conn_list)

query <- "CREATE INDEX prov_netdown_geom_idx ON thlb_proxy.prov_netdown USING gist(geom);"
run_sql_r(query, conn_list)

query <- "ANALYZE thlb_proxy.prov_netdown;"
run_sql_r(query, conn_list)

todays_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
query <- glue("COMMENT ON TABLE thlb_proxy.prov_netdown IS 'Table created at {todays_date}.'")
run_sql_r(query, conn_list)

end_time <- Sys.time()
duration <- difftime(end_time, start_time, units = "mins")
print(glue("Script started at {format(end_time, '%Y-%m-%d %I:%M:%S %p')}"))
print(glue("Script duration: {duration} minutes\n"))
