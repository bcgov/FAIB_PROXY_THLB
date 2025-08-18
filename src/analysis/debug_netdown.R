library(dadmtools)
library(dplyr)
library(knitr)
library(kableExtra)
library(tidyverse)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()
## The following query was ran prior
## src\analysis\3.netdown-create-table.R
query <- "SELECT
  p.gr_skey,
  p.tsa_rank1,
  p.wha_label,
  p.harvest_restriction_class_name,
  p.land_designation_type_code,
  p.harvest_start_year_calendar AS cc_year,
	p.current_retention,
	p.n01_fmlb,
	p.n02_ownership,
	p.n03_ownership,
	p.n04_nonfor,
	p.p05_linear_features,
	p.n06_parks,
	p.n07_wha,
	p.n08_misc,
	coalesce(rip.fact,0) as p09_riparian,
  p.n10_arch,
	p.n11_harvest_restrictions,
	p.p12_phys_inop,
	p.n13_non_merchantable,
	p.n14_non_commercial
FROM whse.thlb_proxy_netdown p
LEFT JOIN public.riparian_buffers18_gr_skey rip on rip.gr_skey = p.gr_skey
WHERE man_unit IN ('Merritt TSA')--, 'Fort St. John TSA', 'Fort Nelson TSA')
"
netdown_tab <- sql_to_df(query, conn_list)
netdown_summary <- setup_tracking_variable(netdown_tab)
running_total <- 0
pretty_table(netdown_summary)

netdown_tab <- netdown_tab %>%
  mutate(
    fmlb = 1,
    falb = 1,
    aflb = 1,
    thlb_net = 1
  )

lclass<-"FMLB"
n_step<-"n01_fmlb"

netdown_summary <- netdown100pct(netdown_tab,netdown_summary,running_total,lclass,n_step)
netdown_tab <- update_areas_fmlb(netdown_tab,n_step)
running_total <- get_running_total(netdown_summary,lclass)
netdown <- get_netdown(netdown_summary,lclass)

pretty_table(netdown_summary)

lclass<-"LAND BASE SUMMARY - FMLB"

netdown_summary <- landbase_sum(netdown_tab,netdown_summary,running_total,lclass,netdown,which_landbase=fmlb)

pretty_table(netdown_summary)

lclass<-"Ownership_Areas_Excluded_from_FALB"
n_step<-"n02_ownership"

netdown_summary<-netdown100pct(netdown_tab,netdown_summary,running_total,lclass,n_step)
netdown_tab<-update_areas_falb(netdown_tab,n_step)
running_total<-get_running_total(netdown_summary,lclass)
netdown <- get_netdown(netdown_summary,lclass)
pretty_table(netdown_summary)

lclass<-"LAND BASE SUMMARY - FALB"

netdown_summary <- landbase_sum(netdown_tab,netdown_summary,running_total,lclass,netdown,which_landbase=falb)

pretty_table(netdown_summary)

lclass<-"Ownership_Areas_Excluded_from_pAFLB"
n_step<-"n03_ownership"

netdown_summary<-netdown100pct(netdown_tab,netdown_summary,running_total,lclass,n_step)
netdown_tab<-update_areas_aflb(netdown_tab,n_step)
running_total<-get_running_total(netdown_summary,lclass)
netdown <- get_netdown(netdown_summary,lclass)

pretty_table(netdown_summary)

lclass<-"Non_Forest_and_Non_Productive_Forest"
n_step<-"n04_nonfor"

netdown_summary<-netdown100pct(netdown_tab,netdown_summary,running_total,lclass,n_step)

netdown_tab<-update_areas_aflb(netdown_tab,n_step)

running_total<-get_running_total(netdown_summary,lclass)

pretty_table(netdown_summary)

lclass<-"Linear_Features"
n_step<-"p05_linear_features"

netdown_summary<-netdown_prop(netdown_tab,netdown_summary,running_total,lclass,n_step)


netdown_tab <- netdown_tab %>%
mutate( aflb = aflb * (1-p05_linear_features),
thlb_net = thlb_net * (1-p05_linear_features)
)

running_total<-get_running_total(netdown_summary,lclass)

netdown<-get_netdown(netdown_summary,lclass)
pretty_table(netdown_summary)

lclass<-"Parks_and_Protected_Areas"
n_step<-"n06_parks"

netdown_summary<-netdown100pct(netdown_tab,netdown_summary,running_total,lclass,n_step)
netdown_tab<-update_areas_thlb(netdown_tab,n_step)
running_total<-get_running_total(netdown_summary,lclass)
netdown<-get_netdown(netdown_summary,lclass)
pretty_table(netdown_summary)

lclass<-"Wildlife_Habitat_Areas"
n_step<-"n07_wha"

netdown_summary<-netdown100pct(netdown_tab,netdown_summary,running_total,lclass,n_step)
netdown_tab<-update_areas_thlb(netdown_tab,n_step)
running_total<-get_running_total(netdown_summary,lclass)
netdown<-get_netdown(netdown_summary,lclass)
pretty_table(netdown_summary)

lclass <- "Misc_Tenures"
n_step <- "n08_misc"

netdown_summary<-netdown100pct(netdown_tab,netdown_summary,running_total,lclass,n_step)
netdown_tab<-update_areas_thlb(netdown_tab,n_step)
running_total<-get_running_total(netdown_summary,lclass)
netdown<-get_netdown(netdown_summary,lclass)
pretty_table(netdown_summary)




lclass<-"Riparian_Buffers"
n_step<-"p09_riparian"

netdown_summary<-netdown_prop(netdown_tab,netdown_summary,running_total,lclass,n_step)

netdown_tab <- netdown_tab %>%
  mutate(thlb_net = thlb_net * (1-p09_riparian)
  )

netdown<-get_netdown(netdown_summary,lclass)
running_total<-get_running_total(netdown_summary,lclass)
pretty_table(netdown_summary)

lclass<-"Non-Cultural_Heritage_Features"
n_step<-"n10_arch"

netdown_summary<-netdown100pct(netdown_tab,netdown_summary,running_total,lclass,n_step)
netdown_tab<-update_areas_thlb(netdown_tab,n_step)
running_total<-get_running_total(netdown_summary,lclass)
netdown<-get_netdown(netdown_summary,lclass)
pretty_table(netdown_summary)

lclass<-"Harvest_Restrictions"
n_step<-"n11_harvest_restrictions"

netdown_summary<-netdown100pct(netdown_tab,netdown_summary,running_total,lclass,n_step)
netdown_tab<-update_areas_thlb(netdown_tab,n_step)
running_total<-get_running_total(netdown_summary,lclass)
netdown<-get_netdown(netdown_summary,lclass)
pretty_table(netdown_summary)


lclass<-"Physical_Inoperable"
n_step<-"p12_phys_inop"

## adjust  physical inoperable variable prior to running summary statistics on it
netdown_tab <- netdown_tab %>%
  mutate(p12_phys_inop = case_when(
    (!is.na(cc_year)) ~ 0, ## change to 0 which will be altered to 1 in later step
    is.na(cc_year) ~ p12_phys_inop
    )
  )

netdown_summary<-netdown_prop(netdown_tab,netdown_summary,running_total,lclass,n_step)

netdown_tab <- netdown_tab %>%
  mutate(thlb_net = thlb_net * (1-p12_phys_inop)
  )

running_total<-get_running_total(netdown_summary,lclass)
netdown<-get_netdown(netdown_summary,lclass)
pretty_table(netdown_summary)

lclass<-"Non-Merchantable"
n_step<-"n13_non_merchantable"

netdown_tab <- netdown_tab %>%
  mutate(n13_non_merchantable = case_when(
    (!is.na(cc_year)) ~ NA, ##adjust non-merchantable value to NA if cell was ever previously a cutblock (I.e., consolidated cutblock year exists)
    is.na(cc_year) ~ n13_non_merchantable
    )
  )
netdown_summary<-netdown100pct(netdown_tab,netdown_summary,running_total,lclass,n_step)
netdown_tab<-update_areas_thlb(netdown_tab,n_step)
running_total<-get_running_total(netdown_summary,lclass)
netdown<-get_netdown(netdown_summary,lclass)
pretty_table(netdown_summary)

lclass<-"Non-Commercial"
n_step<-"n14_non_commercial"

netdown_tab <- netdown_tab %>%
  mutate(n14_non_commercial = case_when(
    (!is.na(cc_year)) ~ NA, ##adjust non-merchantable value to NA if cell was ever previously a cutblock (I.e., consolidated cutblock year exists)
    is.na(cc_year) ~ n14_non_commercial
    )
  )

netdown_summary<-netdown100pct(netdown_tab,netdown_summary,running_total,lclass,n_step)
netdown_tab<-update_areas_thlb(netdown_tab,n_step)
running_total<-get_running_total(netdown_summary,lclass)
netdown<-get_netdown(netdown_summary,lclass)
pretty_table(netdown_summary)




netdown_tab$p15_future_retention <- 0.09588



lclass<-"Future Retention"
n_step<-"p15_future_retention"


netdown_summary<-netdown_prop(netdown_tab,netdown_summary,running_total,lclass,n_step)

netdown_tab <- netdown_tab %>%
  mutate(thlb_net = thlb_net * (1-p15_future_retention))

running_total<-get_running_total(netdown_summary,lclass)
netdown<-get_netdown(netdown_summary,lclass)
pretty_table(netdown_summary)


lclass<-"LAND BASE SUMMARY - pTHLB"

netdown_summary<-landbase_sum(netdown_tab,netdown_summary,running_total,lclass,netdown,which_landbase=thlb_net)

pretty_table(netdown_summary)
