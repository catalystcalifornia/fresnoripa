#### Overview ####
# Output de-duplicated tables of stop reason by unique stop #

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

#### Recode stop reason at person level (7 categories) ####
reasons <- persons %>%
  mutate(reason = case_when(
    reason_for_stop == "1" ~ "Traffic Violation",
    reason_for_stop == "2" ~ "Reasonable suspicion",
    reason_for_stop == "3" ~ "Parole/probation/PRCS/ mandatory supervision",
    reason_for_stop == "4" ~ "Knowledge of outstanding arrest/wanted person",
    reason_for_stop == "5" ~ "Investigation to determine whether person was truant",
    reason_for_stop == "6" ~ "Consensual encounter reasoning in search",
    reason_for_stop == "7" ~ "Possible conduct under Education Code 8 Determine whether student violated school policy",
    .default = NA
  ))

# Explore the data
# only one stop reason is provided per person
reasons_per_person<-reasons%>%
  group_by(stop_id, person_number) %>%
  summarize(reason_count=n())

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
step1 <- reasons %>%
  select(-person_number)%>%
  group_by(stop_id)%>%
  mutate(stop_reason_list=paste(reason, collapse = ", "))%>%
  summarise(stop_reason_count=n(),
            stop_reason_list=min(stop_reason_list))%>%
  mutate(multiplereasonstop = ifelse(stop_reason_count > 1, "Multiple reasons", "Single reason"))


##### Step 2: Recode stop reason to simplified version while retaining original reasons #####
## if more than 1 reason, then let's count as Two or More reasons, for all others we keep the original stop reason
step2<-step1%>%
  mutate(stop_reason_simple=ifelse(multiplereasonstop=="Multiple reasons", "Two or More reasons", stop_reason_list))%>%
  select(stop_id, stop_reason_simple, stop_reason_list, stop_reason_count)

# NOTE: the code for rel_stops_reason and rel_stops_result do not consider if the reasons/results are different (i.e., a stop with multiple persons stopped for the same reason, will list "Two or more reasons")

##### Double check it worked #####
final_check<-step2%>%
  group_by(stop_id,stop_reason_simple)%>%
  summarise(count=n())

stops_unique_check<-final_check%>%
  distinct(stop_id)%>%
  nrow()


#### Push recoded table to postgres ####

# clean up table
rel_stops_reason<-step2

dbWriteTable(conn,  "rel_stops_reason", rel_stops_reason, 
             overwrite = FALSE, row.names = FALSE)

# write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE rel_stops_reason  IS 'Simplified reason for the stop by unique stop. 
Stops with two or more reasons are recoded to two or more reasons (e.g. including 2 or more people stopped for different reasons).
Use this table when calculating time spent on stops based on stop reason.
See W:/Project/ECI/Fresno RIPA/GitHub/HK/fresnoripa/Prep/rel_stops_reason.R';

COMMENT ON COLUMN rel_stops_reason.stop_id IS 'Unique stop id';
COMMENT ON COLUMN rel_stops_reason.stop_reason_simple IS 'Simplified reason for the stop calculated based on all persons involved. Two or More Reasons indicates stop included multiple people for either the same or different reasons';
COMMENT ON COLUMN rel_stops_reason.stop_reason_list IS 'Complete list of reasons for the stop. For stops involving only 1 reason, only 1 reason will be listed, matching stop_reason_simple';
COMMENT ON COLUMN rel_stops_reason.stop_reason_count IS 'Number of unique stop reasons in the stop';
                        ")

# send table comment + column metadata
dbSendQuery(conn = conn, table_comment)

dbDisconnect(conn)
