###Analysis: Traffic stops by stop reason and race

#Set up work space---------------------------------------


library(RPostgreSQL)
library(dplyr)
library(tidyr)
library(stringr)

#connect to postgres

source("W:\\RDA Team\\R\\credentials_source.R")

con <- connect_to_db("eci_fresno_ripa")

# pull in necessary analysis tables

stop<-dbGetQuery(con, "SELECT * FROM rel_stops")

person_reason<-dbGetQuery(con, "SELECT * FROM rel_persons_reason")
person_race<-dbGetQuery(con, "SELECT * FROM rel_races_recode")

pop<-dbGetQuery(con, "SELECT * FROM population_race_fresno_city")

offense_codes<-dbGetQuery(con, "SELECT * FROM cadoj_ripa_offense_codes_2023")

# join necessary tables for analysis 

df<-stop%>%
  filter(call_for_service==0)%>%
  left_join(person_reason)%>%
  left_join(person_race, , by=c("stop_id", "person_number"))

# Sub-Analysis 1---------------------
# Table of top 3-5 traffic code reasons by race and traffic violation type (moving, equipment, non-moving) 

# rate options:

## 1) total stops within each stop type and race / all stops within stop type for that race i.e. all latinx moving stops

# Analyze

### NH ###

df1<-df%>%
  filter(reason=="Traffic violation")%>%
  group_by(traffic_violation_type, nh_race)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge, nh_race)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  select(nh_race, traffic_violation_type, rfs_traffic_violation_code, statute_literal_25, total, count, rate)%>%
  arrange(nh_race, traffic_violation_type, -rate)%>%
  group_by(nh_race, traffic_violation_type)%>%
  slice(1:5)

### AIAN ###

df1_aian<-df%>%
  filter(reason=="Traffic violation" & aian_flag==1)%>%
  group_by(traffic_violation_type)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="aian_aoic")%>%
  select(nh_race, traffic_violation_type, rfs_traffic_violation_code, statute_literal_25, total, count, rate)%>%
  arrange(traffic_violation_type, -rate)%>%
  group_by(traffic_violation_type)%>%
  slice(1:5)


### NHPI ###

df1_nhpi<-df%>%
  filter(reason=="Traffic violation" & nhpi_flag==1)%>%
  group_by(traffic_violation_type)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="nhpi_aoic")%>%
  select(nh_race, traffic_violation_type, rfs_traffic_violation_code, statute_literal_25, total, count, rate)%>%
  arrange(traffic_violation_type, -rate)%>%
  group_by(traffic_violation_type)%>%
  slice(1:5)

### SWANA/SA ###

df1_sswana<-df%>%
  filter(reason=="Traffic violation" & sswana_flag==1)%>%
  group_by(traffic_violation_type)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="sswana_aoic")%>%
  select(nh_race, traffic_violation_type, rfs_traffic_violation_code, statute_literal_25, total, count, rate)%>%
  arrange(traffic_violation_type, -rate)%>%
  group_by(traffic_violation_type)%>%
  slice(1:5)

## Combine all tables together ##

df1<-rbind(df1, df1_aian, df1_nhpi, df1_nhpi)%>%
  rename("race"="nh_race")

# Sub-Analysis 2---------------------
# Table of top 3-5 traffic code reasons by race and traffic violation type (moving, equipment, non-moving) 

#### Denom 1: Per racial group. i.e.)  % of Latinx stopped for registration / all Latinx traffic stops ####

##### NH #####

df2.1<-df%>%
  filter(reason=="Traffic violation")%>%
  group_by(nh_race)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(statute_literal_25, offense_type_of_charge, nh_race)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(denom="traffic_stop_race")%>%
  select(nh_race, denom, rfs_traffic_violation_code, statute_literal_25, total, count, rate)%>%
  arrange(nh_race, -rate)%>%
  group_by(nh_race)%>%
  slice(1:5)

##### AIAN #####

df2.1_aian<-df%>%
  filter(reason=="Traffic violation" & aian_flag==1)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="aian_aoic",
         denom="traffic_stop_race")%>%
  select(nh_race, denom, rfs_traffic_violation_code, statute_literal_25, total, count, rate)%>%
  arrange(rate)%>%
  slice(1:5)


##### NHPI #####

df2.1_nhpi<-df%>%
  filter(reason=="Traffic violation" & nhpi_flag==1)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="nhpi_aoic",
         denom="traffic_stop_race")%>%
  select(nh_race, denom, rfs_traffic_violation_code, statute_literal_25, total, count, rate)%>%
  arrange(rate)%>%
  slice(1:5)

##### SWANA/SA #####

df2.1_sswana<-df%>%
  filter(reason=="Traffic violation" & sswana_flag==1)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="sswana_aoic",
         denom="traffic_stop_race")%>%
  select(nh_race, denom, rfs_traffic_violation_code, statute_literal_25, total, count, rate)%>%
  arrange(rate)%>%
  slice(1:5)

## Combine all tables together ##

df2.1<-rbind(df2.1, df2.1_aian, df2.1_nhpi, df2.1_nhpi)%>%
  rename("race"="nh_race")

#### Denom 2: By stop reason i.e.) %  of Latinx stopped for registration / all people stopped for registration ####

###### NH #####

df2.2<-df%>%
  filter(reason=="Traffic violation")%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(total=n())%>%
  group_by(statute_literal_25, offense_type_of_charge, nh_race)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(denom="traffic_reason")%>%
  select(nh_race, denom, rfs_traffic_violation_code, statute_literal_25, total, count, rate)%>%
  arrange(nh_race, -rate)%>%
  group_by(nh_race)%>%
  slice(1:5)





# Push all tables to postgres------------------------------

# set column types
charvect = rep('varchar', ncol(df)) 
charvect <- replace(charvect, c(3,4,5), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df)

dbWriteTable(con,  "report_traffic_result_stop", df,
             overwrite = TRUE, row.names = FALSE,
             field.types = charvect)


# # write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE report_traffic_result_stop  IS 'Analyzing officer-initiated traffic stops by simple stop result.
R script used to recode and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Analysis\\report_traffic_result_stop.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_traffic_result_stop.docx';

COMMENT ON COLUMN report_traffic_result_stop.stop_reason_simple IS 'Reason for stop (which will only be traffic violations for this analysis)';
COMMENT ON COLUMN report_traffic_result_stop.stop_result_simple IS 'Simple result for stop';
COMMENT ON COLUMN report_traffic_result_stop.total IS 'Total number of officer-initiated traffic stops (denominator in rate calc)';
COMMENT ON COLUMN report_traffic_result_stop.count IS 'Count of officer-initiated traffic stops for each stop result (numerator for rate calc)';
COMMENT ON COLUMN report_traffic_result_stop.rate IS 'Rate of officer-initiated traffic stops by stop result';
")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)

