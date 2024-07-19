#### Overview ####
# Output de-duplicated tables of stop result by person stopped  and for unique stops #

#### Set up workspace ####

library(RPostgreSQL)
library(tidyr)
library(dplyr)

#### Load data ####
source("W:\\RDA Team\\R\\credentials_source.R")
conn <- connect_to_db("eci_fresno_ripa")

# pull in relational tables rel_persons to get result of stop (ros) data
persons <- dbGetQuery(conn," SELECT * FROM rel_persons") %>%
  # keep result of stop ("ros_") columns but drop ros columns with codes ("_cds")
  select(stop_id, person_number, starts_with("ros_"), -ends_with("_cds"))

#### Explore data ####
# pivot persons longer to get all results in one column
persons_longer <- persons %>%
  mutate_all(as.character) %>%
  pivot_longer(cols = starts_with("ros_"), names_to = "result_colname", values_to = "value")

# table(persons_longer$value)

persons_longer <- persons_longer %>%
  mutate(result_colname = case_when(value == "0" ~ NA,
                                  value == "1" ~ result_colname))

# filter out results that are NA (0) for each person
results <- persons_longer %>%
  select(stop_id, person_number, result_colname) %>%
  filter(!is.na(result_colname))

# Note: there is a person_number==0: U100522081C534CFE830
# Note: the original data uses person_number but for other RIPA databases we use person_id

#### Recode stop result at person level (13 categories) ####
results <- results %>%
  mutate(result = case_when(
    result_colname == "ros_no_action" ~ "no action",
    result_colname == "ros_warning" ~ "warning verbal or written",
    result_colname == "ros_citation" ~ "citation for infraction",
    result_colname == "ros_in_field_cite_release" ~ "in field cite and release",
    result_colname == "ros_custodial_warrant" ~ "custodial pursuant to outstanding warrant",
    result_colname == "ros_custodial_without_warrant" ~ "custodial arrest without warrant",
    result_colname == "ros_field_interview_card" ~ "field interview card completed",
    result_colname == "ros_noncriminal_transport" ~ "noncriminal transport or caretaking transport including transport by officer, transport by ambulance, or transport by another agency",
    result_colname == "ros_contact_legal_guardian" ~ "contacted parent/legal guardian or other person responsible for minor",
    result_colname == "ros_psych_hold" ~ "psychiatric hold",
    result_colname == "ros_us_homeland" ~ "referred to US Department of Homeland Security ICE",
    result_colname == "ros_referral_school_admin" ~ "referral to school administrator",
    result_colname == "ros_referral_school_counselor" ~ "referral to school counselor or other support staff",
    .default = NA
  ))


# table of stop results
stop_results_freq<-table(results$result)%>%
  as.data.frame()

sum(stop_results_freq$Freq)

# number of unique people
persons_unique<-results%>%
  distinct(stop_id, person_number)%>%
  nrow()

persons_unique

# number of unique stops
stops_unique<-results%>%
  distinct(stop_id)%>%
  nrow()

stops_unique

##### Step 1: get a table that shows all the stop_ids and results of stop by stop #####

##### Step 1: group by stop to see which ones have more than 1 row, in other words more than 1 stop result #####
step1 <- results %>%
  select(-person_number)%>%
  group_by(stop_id)%>%
  mutate(stop_result_list=paste(result, collapse = ", "))%>%
  summarise(stop_result_count=n(),
            stop_result_list=min(stop_result_list))%>%
  mutate(multipleresultstop = ifelse(stop_result_count > 1, "Multiple results", "Single result"))


##### Step 2: Recode stop result to simplified version while retaining original results #####
## if more than 1 result, then let's count as Two or More Results, for all others we keep the original stop result
step2<-step1%>%
  mutate(stop_result_simple=ifelse(multipleresultstop=="Multiple results", "Two or More Results", stop_result_list))%>%
  select(stop_id, stop_result_simple, stop_result_list, stop_result_count)

##### Double check it worked #####
final_check<-step2%>%
  group_by(stop_id,stop_result_simple)%>%
  summarise(count=n())

stops_unique_check<-final_check%>%
  distinct(stop_id)%>%
  nrow()


#### Push recoded table to postgres ####

# clean up table
rel_stops_result<-step2

dbWriteTable(conn,  "rel_stops_result", rel_stops_result, 
             overwrite = FALSE, row.names = FALSE)

# write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE data.rel_stops_result IS 'Simplified result of the stop by unique stop. 
Stops with two or more results in their stop are recoded to Two or More Results (in other words people stopped for more than 1 result).
Use this table when calculating time spent on stops by stop result.
Note result corresponds to result categories included in RIPA data. A person may have 2 general results, e.g. citation for infraction and warning, but may receive multiple charges under 1 result, e.g. 3 infractions.
See W:/Project/ECI/Fresno RIPA/GitHub/HK/fresnoripa/Prep/rel_stops_result.R';

COMMENT ON COLUMN data.rel_stops_result.stop_id IS 'Unique stop id';

COMMENT ON COLUMN data.rel_stops_result.stop_result_simple IS 'Simplified result of the stop. Two or More Results indicates the stop resulted in more than 1 result';

COMMENT ON COLUMN data.rel_stops_result.stop_result_list IS 'Complete list of results of the stop. For stops that involved only 1 result, only 1 result will be listed, matching stop_result_simple';

COMMENT ON COLUMN data.rel_stops_result.stop_result_count IS 'Number of result categories recorded in the stop';
")


# send table comment + column metadata
dbSendQuery(conn = conn, table_comment)

dbDisconnect(conn)