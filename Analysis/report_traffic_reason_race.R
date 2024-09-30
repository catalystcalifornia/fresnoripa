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
  left_join(person_race, , by=c("stop_id", "person_number"))%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))

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
  ungroup()%>%
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge, nh_race)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  select(nh_race, traffic_violation_type,  rfs_traffic_violation_code, statute_literal_25, total, count, rate)%>%
  arrange(nh_race, traffic_violation_type, -rate)%>%
  group_by(nh_race, traffic_violation_type)%>%
  slice(1:5)

### AIAN ###

df1_aian<-df%>%
  filter(reason=="Traffic violation" & aian_flag==1)%>%
  group_by(traffic_violation_type)%>%
  mutate(total=n())%>%
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


#### JZ SOMETHING weird is happening with  rfs traffic code == '54657' my code is calculating a total of 362 instances of this but I am only seeing 303 in the actual dfs

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


###### AIAN #####

df2.2_aian<-df%>%
  filter(reason=="Traffic violation")%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(total=n())%>%
  filter(aian_flag==1)%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="aian_aoic",
         denom="traffic_reason")%>%
  select(nh_race, denom, rfs_traffic_violation_code, statute_literal_25, total, count, rate)%>%
  arrange(nh_race, -rate)%>%
  group_by(nh_race)%>%
  slice(1:5)

# Final join of tables for both denominators--------------------------------------

df<-rbind(df2.1, df2.2)


# Push all tables to postgres------------------------------

#### Sub-Analysis 1 ####

# set column types
charvect = rep('varchar', ncol(df1)) 
charvect <- replace(charvect, c(5,6,7), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df1)

dbWriteTable(con,  "report_traffic_reason_type_race", df1,
             overwrite = TRUE, row.names = FALSE,
             field.types = charvect)


# # write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE report_traffic_reason_type_race  IS 'Analyzing officer-initiated traffic stops by simple stop reason by traffic stop type for each racial group.
R script used to analyze and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Analysis\\report_traffic_reason_race.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_traffic_reason_race.docx';

COMMENT ON COLUMN report_traffic_reason_type_race.race IS 'Perceived race';
COMMENT ON COLUMN report_traffic_reason_type_race.traffic_violation_type IS 'Type of traffic violation (moving, nonmoving, equipment)';
COMMENT ON COLUMN report_traffic_reason_type_race.rfs_traffic_violation_code IS 'Traffic stop reason code';
COMMENT ON COLUMN report_traffic_reason_type_race.statute_literal_25 IS 'Text description of the traffic stop reason corresponding with the traffic stop reason code';
COMMENT ON COLUMN report_traffic_reason_type_race.total IS 'Total number of officer-initiated traffic stops within each traffic stop type for each race (denominator in rate calc)';
COMMENT ON COLUMN report_traffic_reason_type_race.count IS 'Count of officer-initiated traffic stop reasonswithin each traffic stop type for each race (numerator for rate calc)';
COMMENT ON COLUMN report_traffic_reason_type_race.rate IS 'Rate of officer-initiated traffic stop reasons for each type of traffic stop type by race';
")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)

