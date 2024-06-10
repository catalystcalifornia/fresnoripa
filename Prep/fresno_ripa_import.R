# Script filters and cleans Fresno RIPA data based on CADOJ RIPA data prepped and cleaned as a part of Catalyst California's RACE COUNTS project
# Original data download link: https://openjustice.doj.ca.gov/data (2022 RIPA Stop Data)
# Data dictionary: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-01/RIPA%20Dataset%20Read%20Me%202022.pdf
# table from RDA databases crime_and_justice.cadoj_ripa_2022

# Set up environment ----
library(RPostgreSQL)
library(dplyr)
library(lubridate)
library(hms)

# Connect to RDA databases
source("W:\\RDA Team\\R\\credentials_source.R")

fres <- connect_to_db("eci_fresno_ripa")
rda <- connect_to_db("rda_shared_data")

# pull in Fresno data from postgres
fresno_ripa<-dbGetQuery(rda, "SELECT * FROM crime_and_justice.cadoj_ripa_2022 WHERE agency_name = 'FRESNO PD'")

## clean up time-zone
fresno_ripa <- fresno_ripa %>% mutate(date_reformatted=as.POSIXct(date_of_stop,tz="UTC"),
                               time_of_stop_reformatted=as_hms(time_of_stop))
# test result
View(data.frame(fresno_ripa$doj_record_id,fresno_ripa$date_of_stop,fresno_ripa$date_reformatted,fresno_ripa$time_of_stop,fresno_ripa$time_of_stop_reformatted))

# replace
fresno_ripa<-fresno_ripa%>%mutate(date_of_stop=date_reformatted, time_of_stop=time_of_stop_reformatted)%>%select(-date_reformatted,-time_of_stop_reformatted)

# Prep Fresno data tables ----
# count unique stop ids first
number_of_stops<-length(unique(fresno_ripa$doj_record_id))

# number of people stopped
number_of_people<-fresno_ripa%>%distinct(doj_record_id,person_number)%>%nrow()

#### subset unique stop records ----
# count of people stopped by stop id
stops_persons<-fresno_ripa%>%group_by(doj_record_id)%>%summarise(persons_count=n())

# main relational stop table
rel_stops<-fresno_ripa%>%
  left_join(stops_persons)%>%
  distinct(doj_record_id,agency_ori,agency_name,time_of_stop,date_of_stop,stop_duration,call_for_service,closest_city,school_code,school_name,stop_student,k12_school_grounds,persons_count)%>%
  rename(stop_id=doj_record_id)
# is student was flagged as a person record for SD project, but here seems value is the same across all people in the stop

#### create relational tables for actions taken ----
# used for outlier analysis

# use of force codes includes:
# Baton or other impact weapon used,  "ads_baton"
# Canine bit or held person, "ads_canine_bite"
# Chemical spray used, "ads_chem_spray"
# Electronic control device used, "ads_elect_device"
# Firearm pointed at person, "ads_firearm_point"
# Firearm discharged or used, "ads_firearm_discharge"
# Person removed from vehicle by physical contact, "ads_removed_vehicle_phycontact"
# Physical or Vehicle contact,
# Impact projectile discharged or used'; "ads_impact_discharge"

# create list of use of force columns
force_list<-c("ads_removed_vehicle_phycontact",
              "ads_firearm_point",
              "ads_firearm_discharge",
              "ads_elect_device",
              "ads_impact_discharge",
              "ads_canine_bite",
              "ads_baton",
              "ads_chem_spray",
              "ads_other_contact")

# other actions to track
removed_from_vehicle_list <- c("ads_removed_vehicle_order","ads_removed_vehicle_phycontact")
handcuffed_list <- c("ads_handcuffed")
detained_list <- c("ads_patcar_detent","ads_curb_detent")
actions_list <- colnames(fresno_ripa)[grep("^ads_", colnames(fresno_ripa), fixed = FALSE)] # all actions taken columns
excluded_actions_list <- c("ads_search_pers_consen","ads_search_prop_consen","ads_no_actions")
included_actions_list <-  setdiff(actions_list, excluded_actions_list) # just actions that are actually taken vs. consent or no action

# calculate actions taken by person -- total actions and types of actions for each person in the stop
actions<- fresno_ripa%>%
  rowwise()%>% 
  select(doj_record_id,person_number, all_of(actions_list))%>% # select relevant columns
  # summarise relevant indicators by row or person
  mutate(removed_from_vehicle=sum(c_across(removed_from_vehicle_list), na.rm = TRUE), # removed from vehicle by force or order
         actions_count=sum(c_across(all_of(included_actions_list)), na.rm = TRUE), # total actions taken excluding actions about consent and no action taken flag
         handcuffed=sum(c_across(handcuffed_list), na.rm = TRUE),
         detained=sum(c_across(detained_list),na.rm=TRUE), # detained
         use_of_force=sum(c_across(force_list), na.rm = TRUE), # force used
         action_taken=ifelse(ads_no_actions==1,0,1), # recode no action taken to be true for action taken
         )%>%
  select(doj_record_id,person_number,action_taken,actions_count,removed_from_vehicle,handcuffed,detained,use_of_force,everything())%>% # reorders columns
  ungroup()

# summarise actions taken by unique stop
rel_stops_action<-actions%>%select(-person_number,-all_of(actions_list))%>% # reduce column
  group_by(doj_record_id)%>%
  summarise_all(sum) # total counts of all actions selected by stop

# summarise with 0/1 variables by unique stop, keep total actions taken
rel_stops_action <- rel_stops_action %>% rename(stop_id=doj_record_id)%>%
  mutate(across(!stop_id & !actions_count, .fns = function(x) ifelse(x >= 1, 1, 0))) # 1 means true that action was taken for at least one person in the stop

#### create relational tables for searches ----
# used for outlier analysis
search_list<-c("ads_search_person","ads_search_property")
consent_list<-c("ads_search_pers_consen","ads_search_prop_consen")
ced_list<-colnames(fresno_ripa)[grep("^ced_", colnames(fresno_ripa), fixed = FALSE)]
contraband_list<-setdiff(ced_list, c("ced_none_contraband"))


# calculate searches conducted by person -- total searches and whether contraband was found for each person in the stop
searches<- fresno_ripa%>%
  rowwise()%>%
  # subset columns
  select(doj_record_id,person_number,all_of(search_list),all_of(consent_list),all_of(ced_list)
         )%>%
  # summarise relevant indicators by row or person
  mutate(searches_count=sum(c_across(all_of(search_list)), na.rm = TRUE), # total searches done on person
         contraband_count=sum(c_across(all_of(contraband_list)), na.rm = TRUE), # total contraband found subtracting flag for no contraband
         contraband_found=ifelse(ced_none_contraband==1,0,1), # recode no contraband found to be true if contraband found
         search_person=ads_search_person, # flag if person was searched
         search_property=ads_search_property, # flag if property was searched
         consent_search_person=ads_search_pers_consen, # flag if consent was given
         consent_search_property=ads_search_prop_consen)  # flag if consent was given

# reduce columns
searches<-searches%>%
  select(doj_record_id,person_number,searches_count,contraband_found,contraband_count,search_person,search_property,consent_search_person,consent_search_property)%>%
  ungroup()

# summarise total searches by unique stop
rel_stops_searches<-searches%>%select(-person_number)%>%
  group_by(doj_record_id)%>%
  summarise_all(sum,na.rm=TRUE)

# summarise with 0/1 variables by unique stop, keep searches total
rel_stops_searches <- rel_stops_searches %>% rename(stop_id=doj_record_id)%>%
  mutate(search=ifelse(searches_count>=1,1,0), # 1 means true
         contraband_found=ifelse(contraband_found>=1,1,0))%>% # 1 means true
  select(stop_id,search,contraband_found,everything())%>%
  rename(search_person_count=search_person,
         search_property_count=search_property,
         consent_search_person_count=consent_search_person,
         consent_search_property_count=consent_search_property)

#### subset person records ----
stop_columns_list <- c("agency_ori","agency_name","time_of_stop","date_of_stop",
                       "stop_duration","closest_city","school_code","school_name",
                       "stop_student", "k12_school_grounds","call_for_service","county")
# records that vary by person in the stop for all analysis other than time spent
person_records<-fresno_ripa%>%
  select(-all_of(stop_columns_list))%>%
  rename(stop_id=doj_record_id)


# Push to postgres data ----
# function for adding table and column comments from RC
add_table_comments <- function(con, schema, table_name, indicator, source, column_names, column_comments) {
  comments <- character()
  comments <- c(comments, paste0("
    COMMENT ON TABLE ", schema, ".", table_name, " IS '", table_comment, "';"))
  for (i in seq_along(column_names)) {
    comment <- paste0("
      COMMENT ON COLUMN ", schema, ".", table_name, ".", column_names[i], " IS '", column_comments[i], "';
      ")
    comments <- c(comments, comment)
  }
  sql_commands <- paste(comments, collapse = "")
  dbSendQuery(con, sql_commands)
}


#### Fresno stops ----
table_name <- "cadoj_ripa_fresno_2022"
schema <- 'data'

indicator <- "Universe of all fresno pd stops from 2022 - original data filtered from crime_and_justice.cadoj_ripa_2022."
source <- "CADOJ RIPA 2022
See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_fresno_ripa_import.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Prep/fresno_ripa_import.R
Data dictionary: source: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-01/RIPA%20Dataset%20Read%20Me%202022.pdf"
table_comment <- paste0(indicator, source)

# # write table
# dbWriteTable(fres, c(schema, table_name),fresno_ripa,
#        overwrite = FALSE, row.names = FALSE)
# 
# #comment on table and columns
# 
# column_names <- colnames(fresno_ripa) # get column names
# 
# column_comments <- c(
#   "A unique system-generated incident identification number. Alpha-numeric",
#   "A system-generated number that is assigned to each individual who is involved in the stop or encounter. Numeric",
#   "The number for the reporting Agency. Nine digit alpha-numeric",
#   "Agency name. Alpha-numeric",
#   "Time of stop",
#   "Date of stop. ",
#   "Duration of stop in minutes",
#   "Location of stop closest city. Alpha",
#   "School code. Fourteen digit alphanumeric Blank",
#   "School name. Alpha Blank",
#   "Stop of student. 0 No 1 Yes",
#   "Stop on K12 school grounds. 0 No 1 Yes",
#   "Perceived Race or Ethnicity of Person stopped. Individuals perceived as more than one race/ethnicity  are counted as Multiracial for this variable. 1 Asian 2 Black/African American 3 Hispanic/Latino 4 Middle Eastern/South Asian 5 Native American 6 Pacific Islander 7 White 8 Multiracial",
#   "Perceived race or ethnicity of person stopped Asian. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Black or African American. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Hispanic or Latino. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Middle Eastern or South Asian. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Native. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Pacific Islander. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped White. 0 No 1 Yes",
#   "Indicates that the officer selected multiple values for the perceived race/ethnicity of the person stopped. columns N thru T",
#   "Perceived gender of person stopped.1 Male 2 Female 3 Transgender Man/Boy 4 Transgender Woman/Girl 5 Gender Nonconforming",
#   "Perceived gender of person stopped male. 0 No 1 Yes",
#   "Perceived gender of person stopped female. 0 No 1 Yes",
#   "Perceived gender of person stopped transgender man/boy. 0 No 1 Yes",
#   "Perceived gender of person stopped transgender woman/girl. 0 No 1 Yes",
#   "Perceived gender of person stopped gender non-conforming. 0 No 1 Yes",
#   "Perceived gender of person stopped gender non-conforming and one other gender group. 0 No 1 Yes",
#   "Person stopped perceived to be LGBT. 0 No 1 Yes",
#   "Perceived age of person stopped. Numeric",
#   "Perceived age of person stopped categorized into groups. 1 (1 to 9), 2 (10 to 14), 3 (15 to 17), 4 (18 to 24), 5 (25 to 34), 6 (35 to 44), 7 (45 to 54), 8 (55 to 64), 9 (65 and older)",
#   "Person stopped has limited or no English fluency. 0 No 1 Yes",
#   "Perceived or known disability of person stopped. Individuals perceived as having one or more disabilities columns. 0 No Disability 1 Deafness 2 Speech Impairment 3 Blind 4 Mental Health Condition 5 Development 6 Hyperactivity 7 Other 8 Multiple Disability",
#   "Perceived or known disability of person stopped deafness or difficulty hearing. 0 No 1 Yes",
#   "Perceived or known disability of person stopped speech impairment or limited use of language. 0 No 1 Yes",
#   "Perceived or known disability of person stopped blind or limited vision. 0 No 1 Yes",
#   "Perceived or known disability of person stopped mental health condition. 0 No 1 Yes",
#   "Perceived or known disability of person stopped intellectual or developmental. 0 No",
#   "Perceived or known disability of person stopped disability related to hyperactivity or impulsive behavior. 0 No 1 Yes",
#   "Perceived or known disability of person stopped other disability. 0 No 1 Yes",
#   "Perceived or known disability of person stopped no disability. 0 No 1 Yes",
#   "Perceived or known disability of person stopped no disability, one disability, or more than one disability. 0 No Disability 1 One Disability 2 Multiple Disabilities",
#   "Reason for stop traffic violation, reasonable suspicion, parole/probation arrest warrant, investigation for truancy, consensual encounter. 1 Traffic Violation 2 Reasonable suspicion 3 Parole/probation/PRCS/ mandatory supervision 4 Knowledge of outstanding arrest/wanted person 5 Investigation to determine whether person was truant 6 Consensual encounter resulting in search 7 Possible conduct under Education Code 8 Determine whether student violated school policy",
#   "Type of traffic violation. 1 Moving 2 Equipment 3 Nonmoving Blank",
#   "Code section related to traffic violation. CJIS offense table code Blank",
#   "If known, code for suspected reason for stop violation. CJIS offense table code Blank",
#   "Reasonable suspicion officer witnessed commission of a crime. 0 No 1 Yes Blank",
#   "Reasonable suspicion matched suspect description. 0 No 1 Yes Blank",
#   "Reasonable suspicion witness or victim identification of suspect at the scene. 0 No 1 Yes Blank",
#   "Reasonable suspicion carrying suspicious object. 0 No 1 Yes Blank",
#   "Reasonable suspicion actions indicative of casing a victim or location. 0 No 1 Yes Blank",
#   "Reasonable suspicion suspected of acting as a lookout. 0 No 1 Yes Blank",
#   "Reasonable suspicion actions indicative of a drug transaction. 0 No 1 Yes Blank",
#   "Reasonable suspicion actions indicative of engaging a violent crime. 0 No 1 Yes Blank",
#   "Reasonable suspicion other reasonable suspicion of a crime. 0 No 1 Yes Blank",
#   "Section Code. 1 48900 2 48900.2 3 48900.3 4 48900.4 5 48900.7 Blank",
#   "When EC 48900 is selected, specify the subdivision. 1 48900a1 2 48900a2 3 48900b 4 48900 c 5 48900 d 6 48900 e 7 48900 f 8 48900 g",
#   "Stop made in response to a call for service. 0 No 1 Yes",
#   "Action taken by officer during stop person removed from vehicle by order. 0 No 1 Yes",
#   "Action taken by officer during stop person removed from vehicle by physical contact. 0 No 1 Yes",
#   "Action taken by officer during stop field sobriety test. 0 No 1 Yes",
#   "Action taken by officer during stop curbside detention. 0 No 1 Yes",
#   "Action taken by officer during stop handcuffed or flex cuffed. 0 No 1 Yes",
#   "Action taken by officer during stop patrol car detention. 0 No 1 Yes",
#   "Action taken by officer during stop canine removed search. 0 No 1 Yes",
#   "Action taken by officer during stop firearm pointed at person. 0 No 1 Yes",
#   "Action taken by officer during stop firearm discharged or used. 0 No 1 Yes",
#   "Action taken by officer during stop electronic device used. 0 No 1 Yes",
#   "Action taken by officer during stop impact projectile discharged or used e.g. blunt impact projectile, rubber bullets, bean bags. 0 No 1 Yes",
#   "Action taken by officer during stop canine bit or held person. 0 No 1 Yes",
#   "Action taken by officer during stop baton or other impact weapon used. 0 No 1 Yes",
#   "Action taken by officer during stop chemical spray use pepper spray, mace, tear gas, or other chemical irritants. 0 No 1 Yes",
#   "Action taken by officer during stop other physical or vehicle contact. 0 No 1 Yes",
#   "Action taken by officer during stop person photographed. 0 No 1 Yes",
#   "Action taken by officer during stop asked for consent to search person. 0 No 1 Yes",
#   "Action taken by officer during stop search of person was conducted. 0 No 1 Yes",
#   "Action taken by officer during stop asked for consent to search. 0 No",
#   "Action taken by officer during stop search of property was conducted. 0 No 1 Yes",
#   "Action taken by officer during stop property was seized. 0 No 1 Yes",
#   "Action taken by officer during stop vehicle impound. 0 No 1 Yes",
#   "Action taken by officer during stop admission or written statement obtained from student. 0 No 1 Yes",
#   "Action taken by officer during stop none. 0 No 1 Yes",
#   "Action taken by officer during stop specify if consent was given for search of person. 0 No 1 Yes Blank",
#   "Action taken by officer during stop specify if consent was given for search of property. 0 No 1 Yes Blank",
#   "Basis for search consent given. 0 No 1 Yes Blank",
#   "Basis for search officer safety/safety of others. 0 No 1 Yes Blank",
#   "Basis for search search warrant. 0 No 1 Yes Blank",
#   "Basis for search condition of parole/probation/PRCS/mandatory supervision. 0 No 1 Yes Blank",
#   "Basis for search suspected weapons. 0 No 1 Yes Blank",
#   "Basis for search visible contraband. 0 No 1 Yes Blank",
#   "Basis for search odor of contraband. 0 No 1 Yes Blank",
#   "Basis for search canine detection. 0 No 1 Yes Blank",
#   "Basis for search evidence of crime. 0 No 1 Yes Blank",
#   "Basis for search incident to arrest. 0 No 1 Yes Blank",
#   "Basis for search exigent circumstances. 0 No 1 Yes Blank",
#   "Basis for search vehicle inventory for search property only. 0 No 1 Yes Blank",
#   "Basis for search suspected violation of school policy. 0 No 1 Yes",
#   "Contraband or evidence discovered none. 0 No 1 Yes",
#   "Contraband or evidence discovered firearm. 0 No",
#   "Contraband or evidence discovered ammunition. 0 No 1 Yes",
#   "Contraband or evidence discovered weapon. 0 No 1 Yes",
#   "Contraband or evidence discovered drugs/narcotics. 0 No 1 Yes",
#   "Contraband or evidence discovered alcohol. 0 No 1 Yes",
#   "Contraband or evidence discovered money. 0 No 1 Yes",
#   "Contraband or evidence discovered drug paraphernalia. 0 No 1 Yes",
#   "Contraband or evidence discovered stolen property. 0 No 1 Yes",
#   "Contraband or evidence discovered cell phone or electronic device. 0 No 1 Yes",
#   "Contraband or evidence discovered other contraband or evidence. 0 No 1 Yes",
#   "Basis for property seizure safekeeping as allowed by law/statute. 0 No 1 Yes Blank",
#   "Basis for property seizure contraband. 1 No 1 Yes Blank",
#   "Basis for property seizure evidence. 2 No 1 Yes Blank",
#   "Basis for property seizure impound of vehicle. 3 No 1 Yes Blank",
#   "Basis for property seizure abandoned property. 4 No 1 Yes Blank",
#   "Basis for property seizure suspected violation of school policy. 0 No 1 Yes Blank",
#   "Type of property seized firearm. 0 No 1 Yes Blank",
#   "Type of property seized ammunition. 0 No 1 Yes Blank",
#   "Type of property seized weapon other than firearm. 0 No 1 Yes Blank",
#   "Type of property seized drugs/narcotics. 0 No 1 Yes Blank",
#   "Type of property seized alcohol. 0 No 1 Yes Blank",
#   "Type of property seized money. 0 No 1 Yes Blank",
#   "Type of property seized drug paraphernalia. 0 No 1 Yes Blank",
#   "Type of property seized stolen property. 0 No 1 Yes Blank",
#   "Type of property seized cellphone. 0 No 1 Yes Blank",
#   "Type of property seized vehicle. 0 No 1 Yes Blank",
#   "Type of property seized other contraband. 0 No 1 Yes Blank",
#   "Result of stop no action. 0 No 1 Yes",
#   "Result of stop warning verbal or written. 0 No",
#   "Result of stop citation for infraction. 0 No 1 Yes",
#   "Result of stop in field cite and release. 0 No 1 Yes",
#   "Result of stop custodial pursuant to outstanding warrant. 0 No 1 Yes",
#   "Result of stop custodial arrest without warrant. 0 No 1 Yes",
#   "Result of stop field interview card completed. 0 No 1 Yes",
#   "Result of stop noncriminal transport or caretaking transport including transport by officer, transport by ambulance, or transport by another agency. 0 No 1 Yes",
#   "Result of stop contacted parent/legal guardian or other person responsible for minor. 0 No 1 Yes",
#   "Result of stop psychiatric hold. 0 No 1 Yes",
#   "Result of stop referred to US Department of Homeland Security ICE. 0 No 1 Yes",
#   "Result of stop referral to school administrator. 0 No 1 Yes",
#   "Result of stop referral to school counselor or other support staff. 0 No 1 Yes",
#   "Result of stop warning code. Five digit numeric code Blank",
#   "Result of stop citation for infraction codes. Five digit numeric code Blank",
#   "Result of stop in field cite and release codes. Five digit numeric code Blank",
#   "Result of stop custodial arrest without warrant codes. Five digit numeric code Blank",
#   "County Name"
# )
# 
# add_table_comments(fres, schema, table_name, indicator, source, column_names, column_comments)

#### Relational stops ----
table_name <- "rel_stops"
schema <- 'data'

indicator <- "Unique fresno pd stops from 2022 - these are fields that are unique to each stop id"
source <- "CADOJ RIPA 2022 cadoj_ripa_fresno_2022
See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_fresno_ripa_import.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Prep/fresno_ripa_import.R
Data dictionary: source: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-01/RIPA%20Dataset%20Read%20Me%202022.pdf"
table_comment <- paste0(indicator, source)

# # write table
# dbWriteTable(fres, c(schema, table_name),rel_stops,
#              overwrite = FALSE, row.names = FALSE)

# #comment on table and columns
# 
# column_names <- colnames(rel_stops) # get column names
# 
# column_comments <- c(
#   "A unique system-generated incident identification number. Alpha-numeric. previously doj_record_id",
#   "The number for the reporting Agency. Nine digit alpha-numeric",
#   "Agency name. Alpha-numeric",
#   "Time of stop",
#   "Date of stop. ",
#   "Duration of stop in minutes",
#   "Stop made in response to a call for service. 0 No 1 Yes",
#   "Location of stop closest city. Alpha",
#   "School code. Fourteen digit alphanumeric Blank",
#   "School name. Alpha Blank",
#   "Stop of student. 0 No 1 Yes",
#   "Stop on K12 school grounds. 0 No 1 Yes",
#   "total people stopped in the stop"
# )
# 
# add_table_comments(fres, schema, table_name, indicator, source, column_names, column_comments)

#### Relational persons stopped ----
table_name <- "rel_persons"
schema <- 'data'

indicator <- "Universe of unique people stopped from 2022 - person records"
source <- "CADOJ RIPA 2022 cadoj_ripa_fresno_2022
See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_fresno_ripa_import.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Prep/fresno_ripa_import.R
Data dictionary: source: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-01/RIPA%20Dataset%20Read%20Me%202022.pdf"
table_comment <- paste0(indicator, source)

# write table
# dbWriteTable(fres, c(schema, table_name),person_records,
#        overwrite = FALSE, row.names = FALSE)

# comment on table and columns

# column_names <- colnames(person_records) # get column names
# 
# column_comments <- c(
#   "A unique system-generated incident identification number. Alpha-numeric previously doj_record_id",
#   "A system-generated number that is assigned to each individual who is involved in the stop or encounter. Numeric",
#   "Perceived Race or Ethnicity of Person stopped. Individuals perceived as more than one race/ethnicity  are counted as Multiracial for this variable. 1 Asian 2 Black/African American 3 Hispanic/Latino 4 Middle Eastern/South Asian 5 Native American 6 Pacific Islander 7 White 8 Multiracial",
#   "Perceived race or ethnicity of person stopped Asian. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Black or African American. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Hispanic or Latino. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Middle Eastern or South Asian. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Native. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Pacific Islander. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped White. 0 No 1 Yes",
#   "Indicates that the officer selected multiple values for the perceived race/ethnicity of the person stopped. columns N thru T",
#   "Perceived gender of person stopped.1 Male 2 Female 3 Transgender Man/Boy 4 Transgender Woman/Girl 5 Gender Nonconforming",
#   "Perceived gender of person stopped male. 0 No 1 Yes",
#   "Perceived gender of person stopped female. 0 No 1 Yes",
#   "Perceived gender of person stopped transgender man/boy. 0 No 1 Yes",
#   "Perceived gender of person stopped transgender woman/girl. 0 No 1 Yes",
#   "Perceived gender of person stopped gender non-conforming. 0 No 1 Yes",
#   "Perceived gender of person stopped gender non-conforming and one other gender group. 0 No 1 Yes",
#   "Person stopped perceived to be LGBT. 0 No 1 Yes",
#   "Perceived age of person stopped. Numeric",
#   "Perceived age of person stopped categorized into groups. 1 (1 to 9), 2 (10 to 14), 3 (15 to 17), 4 (18 to 24), 5 (25 to 34), 6 (35 to 44), 7 (45 to 54), 8 (55 to 64), 9 (65 and older)",
#   "Person stopped has limited or no English fluency. 0 No 1 Yes",
#   "Perceived or known disability of person stopped. Individuals perceived as having one or more disabilities columns. 0 No Disability 1 Deafness 2 Speech Impairment 3 Blind 4 Mental Health Condition 5 Development 6 Hyperactivity 7 Other 8 Multiple Disability",
#   "Perceived or known disability of person stopped deafness or difficulty hearing. 0 No 1 Yes",
#   "Perceived or known disability of person stopped speech impairment or limited use of language. 0 No 1 Yes",
#   "Perceived or known disability of person stopped blind or limited vision. 0 No 1 Yes",
#   "Perceived or known disability of person stopped mental health condition. 0 No 1 Yes",
#   "Perceived or known disability of person stopped intellectual or developmental. 0 No",
#   "Perceived or known disability of person stopped disability related to hyperactivity or impulsive behavior. 0 No 1 Yes",
#   "Perceived or known disability of person stopped other disability. 0 No 1 Yes",
#   "Perceived or known disability of person stopped no disability. 0 No 1 Yes",
#   "Perceived or known disability of person stopped no disability, one disability, or more than one disability. 0 No Disability 1 One Disability 2 Multiple Disabilities",
#   "Reason for stop traffic violation, reasonable suspicion, parole/probation arrest warrant, investigation for truancy, consensual encounter. 1 Traffic Violation 2 Reasonable suspicion 3 Parole/probation/PRCS/ mandatory supervision 4 Knowledge of outstanding arrest/wanted person 5 Investigation to determine whether person was truant 6 Consensual encounter resulting in search 7 Possible conduct under Education Code 8 Determine whether student violated school policy",
#   "Type of traffic violation. 1 Moving 2 Equipment 3 Nonmoving Blank",
#   "Code section related to traffic violation. CJIS offense table code Blank",
#   "If known, code for suspected reason for stop violation. CJIS offense table code Blank",
#   "Reasonable suspicion officer witnessed commission of a crime. 0 No 1 Yes Blank",
#   "Reasonable suspicion matched suspect description. 0 No 1 Yes Blank",
#   "Reasonable suspicion witness or victim identification of suspect at the scene. 0 No 1 Yes Blank",
#   "Reasonable suspicion carrying suspicious object. 0 No 1 Yes Blank",
#   "Reasonable suspicion actions indicative of casing a victim or location. 0 No 1 Yes Blank",
#   "Reasonable suspicion suspected of acting as a lookout. 0 No 1 Yes Blank",
#   "Reasonable suspicion actions indicative of a drug transaction. 0 No 1 Yes Blank",
#   "Reasonable suspicion actions indicative of engaging a violent crime. 0 No 1 Yes Blank",
#   "Reasonable suspicion other reasonable suspicion of a crime. 0 No 1 Yes Blank",
#   "Section Code. 1 48900 2 48900.2 3 48900.3 4 48900.4 5 48900.7 Blank",
#   "When EC 48900 is selected, specify the subdivision. 1 48900a1 2 48900a2 3 48900b 4 48900 c 5 48900 d 6 48900 e 7 48900 f 8 48900 g",
#   "Action taken by officer during stop person removed from vehicle by order. 0 No 1 Yes",
#   "Action taken by officer during stop person removed from vehicle by physical contact. 0 No 1 Yes",
#   "Action taken by officer during stop field sobriety test. 0 No 1 Yes",
#   "Action taken by officer during stop curbside detention. 0 No 1 Yes",
#   "Action taken by officer during stop handcuffed or flex cuffed. 0 No 1 Yes",
#   "Action taken by officer during stop patrol car detention. 0 No 1 Yes",
#   "Action taken by officer during stop canine removed search. 0 No 1 Yes",
#   "Action taken by officer during stop firearm pointed at person. 0 No 1 Yes",
#   "Action taken by officer during stop firearm discharged or used. 0 No 1 Yes",
#   "Action taken by officer during stop electronic device used. 0 No 1 Yes",
#   "Action taken by officer during stop impact projectile discharged or used e.g. blunt impact projectile, rubber bullets, bean bags. 0 No 1 Yes",
#   "Action taken by officer during stop canine bit or held person. 0 No 1 Yes",
#   "Action taken by officer during stop baton or other impact weapon used. 0 No 1 Yes",
#   "Action taken by officer during stop chemical spray use pepper spray, mace, tear gas, or other chemical irritants. 0 No 1 Yes",
#   "Action taken by officer during stop other physical or vehicle contact. 0 No 1 Yes",
#   "Action taken by officer during stop person photographed. 0 No 1 Yes",
#   "Action taken by officer during stop asked for consent to search person. 0 No 1 Yes",
#   "Action taken by officer during stop search of person was conducted. 0 No 1 Yes",
#   "Action taken by officer during stop asked for consent to search. 0 No",
#   "Action taken by officer during stop search of property was conducted. 0 No 1 Yes",
#   "Action taken by officer during stop property was seized. 0 No 1 Yes",
#   "Action taken by officer during stop vehicle impound. 0 No 1 Yes",
#   "Action taken by officer during stop admission or written statement obtained from student. 0 No 1 Yes",
#   "Action taken by officer during stop none. 0 No 1 Yes",
#   "Action taken by officer during stop specify if consent was given for search of person. 0 No 1 Yes Blank",
#   "Action taken by officer during stop specify if consent was given for search of property. 0 No 1 Yes Blank",
#   "Basis for search consent given. 0 No 1 Yes Blank",
#   "Basis for search officer safety/safety of others. 0 No 1 Yes Blank",
#   "Basis for search search warrant. 0 No 1 Yes Blank",
#   "Basis for search condition of parole/probation/PRCS/mandatory supervision. 0 No 1 Yes Blank",
#   "Basis for search suspected weapons. 0 No 1 Yes Blank",
#   "Basis for search visible contraband. 0 No 1 Yes Blank",
#   "Basis for search odor of contraband. 0 No 1 Yes Blank",
#   "Basis for search canine detection. 0 No 1 Yes Blank",
#   "Basis for search evidence of crime. 0 No 1 Yes Blank",
#   "Basis for search incident to arrest. 0 No 1 Yes Blank",
#   "Basis for search exigent circumstances. 0 No 1 Yes Blank",
#   "Basis for search vehicle inventory for search property only. 0 No 1 Yes Blank",
#   "Basis for search suspected violation of school policy. 0 No 1 Yes",
#   "Contraband or evidence discovered none. 0 No 1 Yes",
#   "Contraband or evidence discovered firearm. 0 No",
#   "Contraband or evidence discovered ammunition. 0 No 1 Yes",
#   "Contraband or evidence discovered weapon. 0 No 1 Yes",
#   "Contraband or evidence discovered drugs/narcotics. 0 No 1 Yes",
#   "Contraband or evidence discovered alcohol. 0 No 1 Yes",
#   "Contraband or evidence discovered money. 0 No 1 Yes",
#   "Contraband or evidence discovered drug paraphernalia. 0 No 1 Yes",
#   "Contraband or evidence discovered stolen property. 0 No 1 Yes",
#   "Contraband or evidence discovered cell phone or electronic device. 0 No 1 Yes",
#   "Contraband or evidence discovered other contraband or evidence. 0 No 1 Yes",
#   "Basis for property seizure safekeeping as allowed by law/statute. 0 No 1 Yes Blank",
#   "Basis for property seizure contraband. 1 No 1 Yes Blank",
#   "Basis for property seizure evidence. 2 No 1 Yes Blank",
#   "Basis for property seizure impound of vehicle. 3 No 1 Yes Blank",
#   "Basis for property seizure abandoned property. 4 No 1 Yes Blank",
#   "Basis for property seizure suspected violation of school policy. 0 No 1 Yes Blank",
#   "Type of property seized firearm. 0 No 1 Yes Blank",
#   "Type of property seized ammunition. 0 No 1 Yes Blank",
#   "Type of property seized weapon other than firearm. 0 No 1 Yes Blank",
#   "Type of property seized drugs/narcotics. 0 No 1 Yes Blank",
#   "Type of property seized alcohol. 0 No 1 Yes Blank",
#   "Type of property seized money. 0 No 1 Yes Blank",
#   "Type of property seized drug paraphernalia. 0 No 1 Yes Blank",
#   "Type of property seized stolen property. 0 No 1 Yes Blank",
#   "Type of property seized cellphone. 0 No 1 Yes Blank",
#   "Type of property seized vehicle. 0 No 1 Yes Blank",
#   "Type of property seized other contraband. 0 No 1 Yes Blank",
#   "Result of stop no action. 0 No 1 Yes",
#   "Result of stop warning verbal or written. 0 No",
#   "Result of stop citation for infraction. 0 No 1 Yes",
#   "Result of stop in field cite and release. 0 No 1 Yes",
#   "Result of stop custodial pursuant to outstanding warrant. 0 No 1 Yes",
#   "Result of stop custodial arrest without warrant. 0 No 1 Yes",
#   "Result of stop field interview card completed. 0 No 1 Yes",
#   "Result of stop noncriminal transport or caretaking transport including transport by officer, transport by ambulance, or transport by another agency. 0 No 1 Yes",
#   "Result of stop contacted parent/legal guardian or other person responsible for minor. 0 No 1 Yes",
#   "Result of stop psychiatric hold. 0 No 1 Yes",
#   "Result of stop referred to US Department of Homeland Security ICE. 0 No 1 Yes",
#   "Result of stop referral to school administrator. 0 No 1 Yes",
#   "Result of stop referral to school counselor or other support staff. 0 No 1 Yes",
#   "Result of stop warning code. Five digit numeric code Blank",
#   "Result of stop citation for infraction codes. Five digit numeric code Blank",
#   "Result of stop in field cite and release codes. Five digit numeric code Blank",
#   "Result of stop custodial arrest without warrant codes. Five digit numeric code Blank"
# )
# 
# add_table_comments(fres, schema, table_name, indicator, source, column_names, column_comments)
# 
#### Relational actions taken stops ----
table_name <- "rel_stops_actions"
schema <- 'data'

indicator <- "Key actions taken fields summarised for unique fresno pd stops from 2022. Includes actions taken that are used in outlier analysis. For use in time spent calcs"
source <- "See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_fresno_ripa_import.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Prep/fresno_ripa_import.R"
table_comment <- paste0(indicator, source)

# write table
# dbWriteTable(fres, c(schema, table_name),rel_stops_action,
#              overwrite = FALSE, row.names = FALSE)

# comment on table and columns

# column_names <- colnames(rel_stops_action) # get column names
# 
# column_comments <- c(
#   "A unique system-generated incident identification number. Alpha-numeric. previously doj_record_id",
#   "0/1 flag for whether any action was taken during the stop",
#   "Total actions taken during the stop",
#   "0/1 flag for whether any person was removed from the vehicle by physical contact or order",
#   "0/1 flag for whether any person was handcuffed",
#   "0/1 flag for whether any person was detained in patrol car or curbside",
#   "0/1 flag for whether any force was used - defined as Baton or other impact weapon used,Canine bit or held person,Chemical spray used,Electronic control device used,Firearm pointed at person,Firearm discharged or used,Person removed from vehicle by physical contact,Physical or Vehicle contact,Impact projectile discharged or used"
#   )
# 
# add_table_comments(fres, schema, table_name, indicator, source, column_names, column_comments)

#### Relational searches stops ----
table_name <- "rel_stops_searches"
schema <- 'data'

indicator <- "Searches and contraband/evidence fields summarised for unique fresno pd stops from 2022. For use in outlier analysis and time spent calcs"
source <- "See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_fresno_ripa_import.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Prep/fresno_ripa_import.R"
table_comment <- paste0(indicator, source)

# write table
# dbWriteTable(fres, c(schema, table_name),rel_stops_searches,
#              overwrite = FALSE, row.names = FALSE)

# comment on table and columns

column_names <- colnames(rel_stops_searches) # get column names

column_comments <- c(
  "A unique system-generated incident identification number. Alpha-numeric. previously doj_record_id",
  "0/1 flag for whether any search was done during the stop - person or property",
  "0/1 flag for whether any contraband or evidencewas found during the stop",
  "Total searches of person or property done during the stop",
  "Total contraband or evidence found",
  "Total searches of person done during the stop",
  "Total searches of property done during the stop",
  "Total times consent of search of person was received during the stop",
  "Total times consent of search of property was received during the stop"
    )

add_table_comments(fres, schema, table_name, indicator, source, column_names, column_comments)
