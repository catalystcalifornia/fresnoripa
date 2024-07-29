#### Overview ####
# Output de-duplicated tables of stop reason by person stopped and unique stop #

#### Set up workspace ####

library(RPostgreSQL)
library(tidyr)
library(dplyr)

#### Load data ####
source("W:\\RDA Team\\R\\credentials_source.R")
conn <- connect_to_db("eci_fresno_ripa")

# pull in relational tables
persons<-dbGetQuery(conn," SELECT * FROM rel_persons") %>%
  # keep reason for stop and related ("rfs_") columns codes ("_code")
  select(stop_id, person_number, reason_for_stop, starts_with("rfs_")) %>% # , -ends_with("_code")
  mutate_all(as.character)

#### Recode stop reason at person level (8 categories) ####
reasons <- persons %>%
  mutate(reason = case_when(
    reason_for_stop == "1" ~ "Traffic violation",
    reason_for_stop == "2" ~ "Reasonable suspicion",
    reason_for_stop == "3" ~ "Parole/probation/PRCS/ mandatory supervision",
    reason_for_stop == "4" ~ "Knowledge of outstanding arrest/wanted person",
    reason_for_stop == "5" ~ "Investigation to determine whether person was truant",
    reason_for_stop == "6" ~ "Consensual encounter resulting in search",
    reason_for_stop == "7" ~ "Possible conduct under Education Code", 
    reason_for_stop =="8" ~ "Determine whether student violated school policy",
    .default = NA
  )) %>%
  mutate(traffic_violation_type = case_when(
    rfs_traffic_violation_type == "1" ~ "Moving",
    rfs_traffic_violation_type == "2" ~ "Equipment",
    rfs_traffic_violation_type == "3" ~ "Non-moving",
    .default = NA))

#### Explore the data ####
# only one stop reason is provided per person
reasons_per_person<-reasons%>%
  group_by(stop_id, person_number) %>%
  summarize(reason_count=n())

# no missing traffic violations
check_traffic<-reasons%>%filter(reason=="Traffic violation" & is.na(traffic_violation_type))

# the most reasons provided per stop is the same as the number of people in a stop, each person can have different stop reasons (e.g., stop_id = U100522026C92252B7E5)
# even if persons in the same stop have the same stop reason, other reason-related information can differ (e.g., stop_id: U100522003F666D1FBB7, both persons are stopped for traffic violation, but traffic violation type is different)
reasons_per_stop<-reasons%>%
  group_by(stop_id) %>%
  summarize(reason_count=n())%>%
  filter(reason_count>1)%>%
  ungroup() %>% 
  left_join(persons)

# table of stop reasons
# note: there are no instances of 7: Possible conduct under Education Code 8 Determine whether student violated school policy 
stop_reasons_freq<-table(reasons$reason)%>%
  as.data.frame()

sum(stop_reasons_freq$Freq)


##### Step 1: get a table that shows all the stop_ids and reasons of stop by stop #####

##### Step 1: group by stop to see which ones have more than 1 row, in other words more than 1 stop reason #####
step1a <- reasons %>%
  select(-person_number)%>%
  group_by(stop_id)%>%
  summarise(stop_reason_count=n(),
            stop_reason_list=list(reason))
  

step1b <- step1a %>%
  group_by(stop_id) %>%
  mutate(unique_stop_reason_count=length(unique(stop_reason_list[[1]])),
         multiplereasonstop = ifelse(unique_stop_reason_count > 1, "Multiple reasons", "Single reason"))


##### Step 2: Recode stop reason to simplified version while retaining original reasons #####
## if more than 1 reason, then let's count as Two or More reasons, for all others we keep the original stop reason
step2<-step1b%>%
  mutate(stop_reason_simple=ifelse(multiplereasonstop=="Multiple reasons", "Two or More reasons", stop_reason_list[[1]][1]))%>%
  select(stop_id, stop_reason_simple, stop_reason_list, stop_reason_count, unique_stop_reason_count)


##### Double check it worked #####
final_check<-step2%>%
  group_by(stop_id,stop_reason_simple)%>%
  summarise(count=n())

stops_unique_check<-final_check%>%
  distinct(stop_id)%>%
  nrow()


#### Push recoded tables to postgres ####

# clean up table
rel_stops_reason<-step2 %>%
  mutate(stop_reason_list=paste(stop_reason_list, sep = ", ", collapse=NULL))%>%
  select(-c(stop_reason_count))

rel_persons_reason <- reasons %>%
  select(stop_id, person_number, reason, everything()) %>%
  select(-c(reason_for_stop))


dbWriteTable(conn,  "rel_stops_reason", rel_stops_reason, 
             overwrite = FALSE, row.names = FALSE)

# write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE rel_stops_reason  IS 'Simplified reason for the stop by unique stop. 
Stops with two or more reasons are recoded to two or more reasons (e.g. including 2 or more people stopped for different reasons).
Use this table when calculating time spent on stops based on stop reason.
See W:/Project/ECI/Fresno RIPA/GitHub/HK/fresnoripa/Prep/rel_stops_reason.R';

COMMENT ON COLUMN rel_stops_reason.stop_id IS 'Unique stop id';
COMMENT ON COLUMN rel_stops_reason.stop_reason_simple IS 'Simplified reason for the stop calculated based on all persons involved. Two or More Reasons indicates stop included multiple people stopped for different reasons'';
COMMENT ON COLUMN rel_stops_reason.stop_reason_list IS 'Complete list of reasons for the stop. One reason may be listed more than once if multiple people in the stop were stopped for the same reason';
COMMENT ON COLUMN rel_stops_reason.stop_reason_count IS 'Number of stop reasons in the stop (essentially number of people stopped)';
COMMENT ON COLUMN rel_stops_reason.unique_stop_reason_count IS 'Number of unique stop reasons in the stop';
                        ")

# send table comment + column metadata
dbSendQuery(conn = conn, table_comment)


dbWriteTable(conn,  "rel_persons_reason", rel_persons_reason, 
             overwrite = FALSE, row.names = FALSE)

# write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE rel_persons_reason  IS 'Recoded reason for person stopped. 
Persons are only cited one reason for stop. original data codes the reason from 1-7, this table replaces the code with the descriptive category and includes related reason for stop (RFS) columns.
See W:/Project/ECI/Fresno RIPA/GitHub/HK/fresnoripa/Prep/rel_stops_reason.R';

COMMENT ON COLUMN data.rel_persons_reason.stop_id IS 'A unique system-generated incident identification number. Alpha-numeric';
COMMENT ON COLUMN data.rel_persons_reason.person_number IS 'A system-generated number that is assigned to each individual who is involved in the stop or encounter. Numeric';
COMMENT ON COLUMN data.rel_persons_reason.reason IS 'The descriptive reason for stop of person';
COMMENT ON COLUMN data.rel_persons_reason.rfs_traffic_violation_type IS 'Type of traffic violation. 1 Moving 2 Equipment 3 Nonmoving Blank';
COMMENT ON COLUMN data.rel_persons_reason.rfs_traffic_violation_code IS 'Code section related to traffic violation. CJIS offense table code Blank';
COMMENT ON COLUMN data.rel_persons_reason.rfs_rs_code IS 'If known, code for suspected reason for stop violation. CJIS offense table code Blank';
COMMENT ON COLUMN data.rel_persons_reason.rfs_rs_off_witness IS 'Reasonable suspicion officer witnessed commission of a crime. 0 No 1 Yes Blank';
COMMENT ON COLUMN data.rel_persons_reason.rfs_rs_match_suspect IS 'Reasonable suspicion matched suspect description. 0 No 1 Yes Blank';
COMMENT ON COLUMN data.rel_persons_reason.rfs_rs_witness_id IS 'Reasonable suspicion witness or victim identification of suspect at the scene. 0 No 1 Yes Blank';
COMMENT ON COLUMN data.rel_persons_reason.rfs_rs_carry_sus_object IS 'Reasonable suspicion carrying suspicious object. 0 No 1 Yes Blank';
COMMENT ON COLUMN data.rel_persons_reason.rfs_rs_actions_indicative IS 'Reasonable suspicion actions indicative of casing a victim or location. 0 No 1 Yes Blank';
COMMENT ON COLUMN data.rel_persons_reason.rfs_rs_suspect_look IS 'Reasonable suspicion suspected of acting as a lookout. 0 No 1 Yes Blank';
COMMENT ON COLUMN data.rel_persons_reason.rfs_rs_drug_trans IS 'Reasonable suspicion actions indicative of a drug transaction. 0 No 1 Yes Blank';
COMMENT ON COLUMN data.rel_persons_reason.rfs_rs_violent_crime IS 'Reasonable suspicion actions indicative of engaging a violent crime. 0 No 1 Yes Blank';
COMMENT ON COLUMN data.rel_persons_reason.rfs_rs_reason_susp IS 'Reasonable suspicion other reasonable suspicion of a crime. 0 No 1 Yes Blank';
COMMENT ON COLUMN data.rel_persons_reason.rfs_ec_discipline_code IS 'Section Code. 1 48900 2 48900.2 3 48900.3 4 48900.4 5 48900.7 Blank';
COMMENT ON COLUMN data.rel_persons_reason.rfs_ec_discipline IS 'When EC 48900 is selected, specify the subdivision. 1 48900a1 2 48900a2 3 48900b 4 48900 c 5 48900 d 6 48900 e 7 48900 f 8 48900 g';
                        ")

# send table comment + column metadata
dbSendQuery(conn = conn, table_comment)

dbDisconnect(conn)
