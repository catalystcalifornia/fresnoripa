# Script filters and cleans Fresno RIPA data based on CADOJ RIPA data prepped and cleaned as a part of Catalyst California's RACE COUNTS project
# Original data download link: https://openjustice.doj.ca.gov/data (2022 RIPA Stop Data)
# Data dictionary: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-01/RIPA%20Dataset%20Read%20Me%202022.pdf
# table from RDA databases crime_and_justice.cadoj_ripa_2022

# Set up environment ----
library(RPostgreSQL)
library(dplyr)

# Connect to RDA databases
source("W:\\RDA Team\\R\\credentials_source.R")

fres <- connect_to_db("eci_fresno_ripa")
rda <- connect_to_db("rda_shared_data")


# pull in tables from postgres
cadoj<-dbGetQuery(rda, "SELECT * FROM crime_and_justice.cadoj_ripa_2022")

# Prep Fresno data tables ----
# filter for Fresno PD data
fresno_ripa<-cadoj%>%filter(agency_name=="FRESNO PD")

# count unique stop ids first
number_of_stops<-length(unique(fresno_ripa$doj_record_id))

# number of people stopped
number_of_people<-fresno_ripa%>%distinct(doj_record_id,person_number)%>%nrow()

#### subset unique stop records ----
# people stopped by stop id
stops_persons<-fresno_ripa%>%group_by(doj_record_id)%>%summarise(persons_count=n())

rel_stops<-fresno_ripa%>%
  left_join(stops_persons)%>%
  distinct(doj_record_id,agency_ori,agency_name,time_of_stop,date_of_stop,stop_duration,call_for_service,closest_city,school_code,school_name,stop_student,k12_school_grounds,persons_count)%>%
  rename(stop_id=doj_record_id)
# is student was flagged as a person record for SD project, but here seems value is the same across all people in the stop

#### create relational tables for actions taken ----
# used for outlier analysis

# use of force codes
# use of force includes
# Baton or other impact weapon used,
# Canine bit or held person,
# Chemical spray used,
# Electronic control device used,
# Firearm pointed at person,
# Firearm discharged or used,
# Person removed from vehicle by physical contact,
# Physical or Vehicle contact,
# Impact projectile discharged or used';

# create list of use of force columns
force_list<-c("ads_removed_vehicle_phycontact","ads_firearm_point","ads_firearm_discharge","ads_elect_device","ads_impact_discharge","ads_canine_bite","ads_baton","ads_chem_spray","ads_other_contact")

# calculate actions taken by person -- total actions and types of actions for each person in the stop
actions<- fresno_ripa%>%rowwise()%>% select(doj_record_id,person_number,contains("ads"))%>%
  mutate(removed_from_vehicle=sum(c(ads_removed_vehicle_order,ads_removed_vehicle_phycontact), na.rm = TRUE),
         actions_count=sum(c_across(contains("ads")), na.rm = TRUE)-sum(c(ads_search_pers_consen,ads_search_prop_consen),na.rm=TRUE),
         handcuffed=ads_handcuffed,
         detained=sum(c(ads_patcar_detent,ads_curb_detent),na.rm=TRUE),
         use_of_force=sum(c_across(contains(force_list)), na.rm = TRUE),
         action_taken=ifelse(ads_no_actions==1,0,1),
         )%>%
  select(doj_record_id,person_number,action_taken,actions_count,removed_from_vehicle,handcuffed,detained,use_of_force,contains("ads"))%>%
  ungroup()

# summarise actions taken by unique stop
rel_stops_action<-actions%>%select(-person_number,-contains("ads"))%>%
  group_by(doj_record_id)%>%
  summarise_all(sum)

# summarise with 0/1 variables by unique stop, keep total actions taken
rel_stops_action <- rel_stops_action %>% rename(stop_id=doj_record_id)%>%
  mutate(across(.cols = c(2,4:7), .fns = function(x) ifelse(x >= 1, 1, 0)))

#### create relational tables for searches ----
# used for outlier analysis

property_seized=sum(c_across(contains("tps")), na.rm = TRUE),


# subset person records
person_records<-fresno_ripa%>%
  select(doj_record_id, person_number,13:143)%>%
  select(-call_for_service)


# Push to postgres data ---
