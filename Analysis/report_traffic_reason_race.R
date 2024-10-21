###Analysis: Traffic stops by stop reason and race

#Set up work space---------------------------------------
library(RPostgreSQL)
library(dplyr)
library(stringr)

#connect to postgres
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("eci_fresno_ripa")

# pull in necessary analysis tables
stop <- dbGetQuery(con, "SELECT * FROM rel_stops") %>%
  filter(call_for_service==0) # officer-initiated stops
person_reason <- dbGetQuery(con, "SELECT * FROM rel_persons_reason")
person_race <- dbGetQuery(con, "SELECT * FROM rel_races_recode")
pop <- dbGetQuery(con, "SELECT * FROM population_race_fresno_city")
offense_codes <- dbGetQuery(con, "SELECT * FROM cadoj_ripa_offense_codes_2023")

# join necessary tables and filter for traffic violation reason analysis 
df <- stop %>%
  left_join(person_reason) %>%
  left_join(person_race, by=c("stop_id", "person_number")) %>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code")) %>%
  filter(reason=="Traffic violation") %>%
  select(nh_race, traffic_violation_type, statute_literal_25, offense_type_of_charge, ends_with("_flag"))


# Sub-Analysis 1---------------------
# Table of top 3-5 traffic code reasons by race and traffic violation type (moving, equipment, non-moving) 
# Analyze
violation_type_counts <- df %>%
  select(traffic_violation_type) %>%
  group_by(traffic_violation_type) %>%
  mutate(total=n()) %>%
  distinct(.)

### NH ###
df1_nh <- df %>%
  # get total counts for violation types
  left_join(violation_type_counts, by=c("traffic_violation_type")) %>% 
  # get counts of each traffic reason per nh_race
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge, nh_race) %>%
  mutate(count=n(),
         rate=count/total*100)%>%
  ungroup() %>%
  distinct() %>%
  # drop unneeded columns
  select(-c(ends_with("_flag"), offense_type_of_charge)) %>% 
  # get top 5 traffic violations for each nh_race X traffic_violation_type combo (can return less than 5)
  arrange(nh_race, traffic_violation_type, -rate)%>%
  group_by(nh_race, traffic_violation_type) %>%
  slice(1:5) 

### AIAN ###
df1_aian <- df %>%
  filter(aian_flag==1) %>%
  left_join(violation_type_counts, by=c("traffic_violation_type")) %>%
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge) %>%
  mutate(count=n(),
         rate=count/total*100) %>%
  ungroup() %>%
  distinct() %>%
  mutate(nh_race="aian_aoic") %>%
  select(-c(ends_with("_flag"), offense_type_of_charge)) %>%
  arrange(traffic_violation_type, -rate) %>%
  group_by(traffic_violation_type) %>%
  slice(1:5)

### NHPI ###
df1_nhpi <- df %>%
  filter(nhpi_flag==1) %>%
  left_join(violation_type_counts, by=c("traffic_violation_type")) %>%
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge) %>%
  mutate(count=n(),
         rate=count/total*100) %>%
  ungroup() %>%
  distinct() %>%
  mutate(nh_race="nhpi_aoic") %>%
  select(-c(ends_with("_flag"), offense_type_of_charge)) %>%
  arrange(traffic_violation_type, -rate) %>%
  group_by(traffic_violation_type) %>%
  slice(1:5)

### SWANA/SA ###
df1_sswana <- df%>%
  filter(sswana_flag==1) %>%
  left_join(violation_type_counts, by=c("traffic_violation_type")) %>%
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge) %>%
  mutate(count=n(),
         rate=count/total*100)%>%
  ungroup() %>%
  distinct() %>%
  mutate(nh_race="sswana_aoic") %>%
  select(-c(ends_with("_flag"), offense_type_of_charge)) %>%
  arrange(traffic_violation_type, -rate) %>%
  group_by(traffic_violation_type) %>%
  slice(1:5)

## Combine all tables together ##
df1 <- rbind(df1_nh, df1_aian, df1_nhpi, df1_sswana) %>%
  rename("race"="nh_race")


# Sub-Analysis 2---------------------
# Table of top 3-5 traffic code reasons by race WITHOUT traffic stop type

#### Denom 1: Per racial group. i.e.)  % of Latinx stopped for registration / all Latinx traffic stops ####
##### NH #####
df2.1_nh <- df %>%
  group_by(nh_race) %>%
  mutate(total=n()) %>%
  group_by(statute_literal_25, offense_type_of_charge, nh_race) %>%
  mutate(count=n(), 
         rate=count/total*100) %>%
  ungroup() %>%
  distinct() %>%
  mutate(denom="traffic_stop_race") %>%
  select(nh_race, denom, statute_literal_25, total, count, rate) %>%
  arrange(nh_race, -rate)

##### AIAN #####
df2.1_aian <- df %>%
  filter(aian_flag==1) %>%
  mutate(total=n()) %>%
  group_by(statute_literal_25, offense_type_of_charge) %>%
  mutate(count=n(),
         rate=count/total*100) %>%
  ungroup() %>%
  distinct() %>%
  mutate(nh_race="aian_aoic",
         denom="traffic_stop_race")%>%
  select(nh_race, denom, statute_literal_25, total, count, rate)%>%
  arrange(-rate)

##### NHPI #####
df2.1_nhpi <- df %>%
  filter(nhpi_flag==1) %>%
  mutate(total=n()) %>%
  group_by(statute_literal_25, offense_type_of_charge) %>%
  mutate(count=n(),
         rate=count/total*100) %>%
  ungroup() %>%
  distinct() %>%
  mutate(nh_race="nhpi_aoic",
         denom="traffic_stop_race") %>%
  select(nh_race, denom, statute_literal_25, total, count, rate) %>%
  arrange(-rate)

##### SWANA/SA #####
df2.1_sswana <- df %>%
  filter(sswana_flag==1) %>%
  mutate(total=n()) %>%
  group_by(statute_literal_25, offense_type_of_charge) %>%
  mutate(count=n(),
         rate=count/total*100) %>%
  ungroup() %>%
  distinct() %>%
  mutate(nh_race="sswana_aoic",
         denom="traffic_stop_race") %>%
  select(nh_race, denom, statute_literal_25, total, count, rate) %>%
  arrange(-rate)

#### Combine all tables together ####
df2.1<-rbind(df2.1_nh, df2.1_aian, df2.1_nhpi, df2.1_sswana)%>%
  rename("race"="nh_race")


#### Denom 2: By stop reason i.e.) %  of Latinx stopped for registration / all people stopped for registration ####
###### Traffic stop reason counts ######
traffic_reason_counts <- df %>%
  select(statute_literal_25, offense_type_of_charge) %>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(total=n()) %>%
  distinct()

###### NH #####
df2.2_nh <- df %>%
  left_join(traffic_reason_counts, by=c("statute_literal_25", "offense_type_of_charge")) %>%
  group_by(statute_literal_25, offense_type_of_charge, nh_race) %>%
  mutate(count=n(),
         rate=count/total*100) %>%
  ungroup() %>%
  distinct() %>%
  mutate(denom="traffic_reason") %>%
  select(nh_race, denom, statute_literal_25, total, count, rate) %>%
  arrange(nh_race, -count) %>%
  group_by(nh_race)

###### AIAN #####
df2.2_aian <- df %>%
  filter(aian_flag==1) %>%
  left_join(traffic_reason_counts, by=c("statute_literal_25", "offense_type_of_charge")) %>%
  group_by(statute_literal_25, offense_type_of_charge) %>%
  mutate(count=n(),
         rate=count/total*100) %>%
  ungroup() %>%
  distinct() %>%
  mutate(nh_race="aian_aoic",
         denom="traffic_reason") %>%
  select(nh_race, denom, statute_literal_25, total, count, rate) %>%
  arrange(nh_race, -count) %>%
  group_by(nh_race)

###### NHPI #####
df2.2_nhpi <- df %>%
  filter(nhpi_flag==1) %>%
  left_join(traffic_reason_counts, by=c("statute_literal_25", "offense_type_of_charge")) %>%
  group_by(statute_literal_25, offense_type_of_charge) %>%
  mutate(count=n(),
         rate=count/total*100) %>%
  ungroup() %>%
  distinct() %>%
  mutate(nh_race="nhpi_aoic",
         denom="traffic_reason") %>%
  select(nh_race, denom, statute_literal_25, total, count, rate) %>%
  arrange(nh_race, -count)

###### SSWANA #####
df2.2_sswana <- df %>%
  filter(sswana_flag==1) %>%
  left_join(traffic_reason_counts, by=c("statute_literal_25", "offense_type_of_charge")) %>%
  group_by(statute_literal_25, offense_type_of_charge) %>%
  mutate(count=n(),
         rate=count/total*100)%>%
  ungroup() %>%
  distinct() %>%
  mutate(nh_race="sswana_aoic",
         denom="traffic_reason")%>%
  select(nh_race, denom, statute_literal_25, total, count, rate)%>%
  arrange(nh_race, -count)

#### Combine all race tables ####
df2.2 <- rbind(df2.2_nh, df2.2_aian, df2.2_nhpi, df2.2_sswana)%>%
  rename("race"="nh_race")


# Final join of tables for both denominators--------------------------------------
df2 <- rbind(df2.1, df2.2)


# Push all tables to postgres------------------------------
#### Sub-Analysis 1 ####
# set column types
charvect <- sapply(df1, class) %>%
  str_replace_all(., "character", "varchar") %>%
  str_replace_all(., "integer", "numeric")

# add df colnames to the character vector
names(charvect) <- colnames(df1)

dbWriteTable(con,  "report_traffic_reason_type_race", df1,
             overwrite = FALSE, row.names = FALSE,
             field.types = charvect)

# # write comment to table, and column metadata
table_comment <- paste0("COMMENT ON TABLE report_traffic_reason_type_race  IS 'Top 5 stop reasons by race for each stop type in officer-initiated traffic stops'
R script used to analyze and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Analysis\\report_traffic_reason_race.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_traffic_reason_race.docx';

COMMENT ON COLUMN report_traffic_reason_type_race.race IS 'Perceived race';
COMMENT ON COLUMN report_traffic_reason_type_race.traffic_violation_type IS 'Type of traffic violation (moving, nonmoving, equipment)';
COMMENT ON COLUMN report_traffic_reason_type_race.statute_literal_25 IS 'Text description of the traffic stop reason corresponding with the traffic stop reason code';
COMMENT ON COLUMN report_traffic_reason_type_race.total IS 'Total number of officer-initiated traffic stops within each traffic stop type (rate calc denominator)';
COMMENT ON COLUMN report_traffic_reason_type_race.count IS 'Count of officer-initiated traffic stop reasons within each traffic stop type for each race (rate calc numerator)';
COMMENT ON COLUMN report_traffic_reason_type_race.rate IS 'Rate of officer-initiated traffic stop reasons per race out by traffic stop type';
")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)


#### Sub-Analysis 2 ####
# set column types
charvect <- sapply(df2, class) %>%
  str_replace_all(., "character", "varchar") %>%
  str_replace_all(., "integer", "numeric")

# add df colnames to the character vector
names(charvect) <- colnames(df2)

dbWriteTable(con,  "report_traffic_reason_race", df2,
             overwrite = FALSE, row.names = FALSE,
             field.types = charvect)

# # write comment to table, and column metadata
table_comment <- paste0("COMMENT ON TABLE report_traffic_reason_race  IS 'Analyzing officer-initiated traffic stops by traffic stop reason for each racial group.
This analysis contains rates with two types of denominators: 1) out of all traffic stops within each racial group and 2) out of all stops that resulted in that traffic stop reason.
The denominator used is denoted in a denom column. 
R script used to analyze and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Analysis\\report_traffic_reason_race.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_traffic_reason_race.docx';

COMMENT ON COLUMN report_traffic_reason_race.race IS 'Perceived race';
COMMENT ON COLUMN report_traffic_reason_race.denom IS 'Denominator type used for the Total and Rate column. 
If denom==traffic_stop_race then total represents total number of officer-initiated traffic stops for that racial group.
If denom==traffic_reason then total represents total number of officer-initiated traffic stops for that stop reason.';
COMMENT ON COLUMN report_traffic_reason_race.statute_literal_25 IS 'Text description of the traffic stop reason corresponding with the traffic stop reason code';
COMMENT ON COLUMN report_traffic_reason_race.total IS 'Denominator count used in rate calc (see denom column for type); 
COMMENT ON COLUMN report_traffic_reason_race.count IS 'Count of officer-initiated traffic stop reasons for that race (numerator for rate calc)';
COMMENT ON COLUMN report_traffic_reason_race.rate IS 'Rate is count of officer-initiated traffic stop reasons by race divided by total (which depends on denom type)';
")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)

dbDisconnect(con)
