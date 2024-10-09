# Analysis : Most common traffic citations resulting from stop by traffic stop type ----------------------------------
# Top offense codes by traffic stop reason (moving, equipment, non-moving)

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
stop_reason<-dbGetQuery(con, "SELECT * FROM rel_stops_reason")
stop_result<-dbGetQuery(con, "SELECT * FROM rel_stops_result")

person_reason<-dbGetQuery(con, "SELECT * FROM rel_persons_reason")
person_result<-dbGetQuery(con, "SELECT * FROM rel_persons_result")
person_race<-dbGetQuery(con, "SELECT * FROM rel_races_recode")

offense_codes<-dbGetQuery(con, "SELECT * FROM cadoj_ripa_offense_codes_2023")


# Join tables------------------------------------

df<-stop%>%
  filter(call_for_service==0)%>%
  left_join(person_reason)%>%
  filter(reason=="Traffic violation")%>%
  left_join(person_result)

# Analysis-------------------------------------------

# Rate: Offense codes / all citations WITHIN each traffic type --moving/nonmoving/equipment

df1<-df%>%
  filter(stop_result_simple=="citation for infraction")%>%
  group_by(traffic_violation_type)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  select(traffic_violation_type, statute_literal_25, total, count, rate)%>%
  arrange(traffic_violation_type, -rate)%>%
  group_by(traffic_violation_type)%>%
  slice(1:5)

# Push to postgres-----------------------------

# set column types
charvect = rep('varchar', ncol(df1)) 
charvect <- replace(charvect, c(3,4,5), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df1)

dbWriteTable(con,  "report_citation_traffic_type_stop", df1,
             overwrite = TRUE, row.names = FALSE,
             field.types = charvect)


# # write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE report_citation_traffic_type_stop  IS 'Analyzing top traffic citations given as a result of a traffic stop within each traffic stop type (moving, nonmoving, equipment).
The denominator (total column) for this analysis is all traffic stops resulting in a citation within each traffic stop type.
R script used to recode and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Analysis\\report_citation_traffic_type_stop.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_citation_traffic_type_stop.docx';

COMMENT ON COLUMN report_citation_traffic_type_stop.traffic_violation_type IS 'Traffic stop type (moving, nonmoving, equipment)';
COMMENT ON COLUMN report_citation_traffic_type_stop.statute_literal_25 IS 'Text description for accompnaying offense code';
COMMENT ON COLUMN report_citation_traffic_type_stop.total IS 'Total number (denominator in rate calc) of traffic stops that resulted in a citation within each traffic stop type';
COMMENT ON COLUMN report_citation_traffic_type_stop.count IS 'Count of each specific citation offense code within each traffic stop type';
COMMENT ON COLUMN report_citation_traffic_type_stop.rate IS 'Rate of Rate of traffic stops resulting in a citation by each citation code out of all traffic stops resulting in a citation
within each traffic stop type';
")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)



# EXPLORATION: Analysis by race AND stop type: -----------------------------------------------

# JZ: I am not sure how useful this is but am going to push to postgres to explore more

# Rate: Percent of citation offense code results out of all citation results within each traffic type for each race
# i.e.) number offense code A for perceived Latinx person in non-moving stop / all citations of Latinx people in non-moving stops

#### NH ####

df_nh<-df%>%
  left_join(person_race)%>%
  filter(stop_result_simple=="citation for infraction")%>%
  group_by(traffic_violation_type, nh_race)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge, nh_race)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  select(nh_race, traffic_violation_type, statute_literal_25,
         total, count, rate)%>%
  arrange(nh_race, traffic_violation_type, -rate)%>%
  group_by(nh_race, traffic_violation_type)%>%
  slice(1:5)

#### AIAN ####

df_aian<-df%>%
  left_join(person_race)%>%
  filter(stop_result_simple=="citation for infraction" & aian_flag == 1)%>%
  group_by(traffic_violation_type)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(nh_race="aian_aoic")%>%
  ungroup()%>%
  select(nh_race, traffic_violation_type, statute_literal_25,  total, count, rate)%>%
  arrange(nh_race, traffic_violation_type, -rate)

#### NHPI ####

df_nhpi<-df%>%
  left_join(person_race)%>%
  filter(stop_result_simple=="citation for infraction" & nhpi_flag == 1)%>%
  group_by(traffic_violation_type)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
ungroup()%>%
  mutate(nh_race="nhpi_aoic")%>%
  select(nh_race, traffic_violation_type, statute_literal_25,  total, count, rate)%>%
  arrange(nh_race, traffic_violation_type, -rate)


#### SSWANA ####

df_sswana<-df%>%
  left_join(person_race)%>%
  filter(stop_result_simple=="citation for infraction" & sswana_flag == 1)%>%
  group_by(traffic_violation_type)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(traffic_violation_type, statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
ungroup()%>%
  mutate(nh_race="sswana_aoic")%>%
  select(nh_race, traffic_violation_type, statute_literal_25,  total, count, rate)%>%
  arrange(nh_race, traffic_violation_type, -rate)%>%
  group_by(nh_race, traffic_violation_type)%>%
  slice(1:5)

#### Final combine of all race tables ####

df_race_type<-rbind(df_nh, df_aian, df_nhpi, df_sswana)%>%
  rename("race"="nh_race")

# Push RACE + STOP TYPE table to postgres----------------------------------------

# set column types
charvect = rep('varchar', ncol(df_race_type)) 
charvect <- replace(charvect, c(5,6,7), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df_race_type)

dbWriteTable(con,  "report_citation_traffic_type_race", df_race_type,
             overwrite = TRUE, row.names = FALSE,
             field.types = charvect)


# # write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE report_citation_traffic_type_race  IS 'Analyzing top 5 traffic citation rates given as a result of a traffic stop within each traffic stop type (moving, nonmoving, equipment)
for each perceived racial group.
The denominator (total column) for this analysis is all traffic stops resulting in a citation within each traffic stop type
for each perceived racial group.
R script used to recode and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Analysis\\report_citation_traffic_type_stop.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_citation_traffic_type_stop.docx';

COMMENT ON COLUMN report_citation_traffic_type_race.race IS 'Perceived race';
COMMENT ON COLUMN report_citation_traffic_type_race.traffic_violation_type IS 'Traffic stop type (moving, nonmoving, equipment)';
COMMENT ON COLUMN report_citation_traffic_type_race.statute_literal_25 IS 'Text description for accompnaying offense code';
COMMENT ON COLUMN report_citation_traffic_type_race.total IS 'Total number (denominator in rate calc) of traffic stops that resulted in a citation within each traffic stop type';
COMMENT ON COLUMN report_citation_traffic_type_race.count IS 'Count of each specific citation offense code within each traffic stop type';
COMMENT ON COLUMN report_citation_traffic_type_race.rate IS 'Rate of Rate of traffic stops resulting in a citation by each citation code out of all traffic stops resulting in a citation
within each traffic stop type';
")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)

# EXPLORATION: Analysis by race WITHOUT stop type: -----------------------------------------------

# Rate: Percent of citation offense code results out of all citation results within each race
# i.e.) number offense code A for perceived Latinx person in non-moving stop / all citations of Latinx people

#### NH ####

df_nh<-df%>%
  left_join(person_race)%>%
  filter(stop_result_simple=="citation for infraction")%>%
  group_by(nh_race)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(statute_literal_25, offense_type_of_charge, nh_race)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  select(nh_race, statute_literal_25,
         total, count, rate)%>%
  arrange(nh_race, -rate)%>%
  group_by(nh_race)

#### AIAN ####

df_aian<-df%>%
  left_join(person_race)%>%
  filter(stop_result_simple=="citation for infraction" & aian_flag == 1)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(nh_race="aian_aoic")%>%
  ungroup()%>%
  select(nh_race, statute_literal_25,  total, count, rate)%>%
  arrange(nh_race, -rate)

#### NHPI ####

df_nhpi<-df%>%
  left_join(person_race)%>%
  filter(stop_result_simple=="citation for infraction" & nhpi_flag == 1)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="nhpi_aoic")%>%
  select(nh_race, statute_literal_25,  total, count, rate)%>%
  arrange(nh_race, -rate)


#### SSWANA ####

df_sswana<-df%>%
  left_join(person_race)%>%
  filter(stop_result_simple=="citation for infraction" & sswana_flag == 1)%>%
  mutate(total=n())%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  group_by( statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="sswana_aoic")%>%
  select(nh_race, statute_literal_25,  total, count, rate)%>%
  arrange(nh_race, -rate)%>%
  group_by(nh_race)

#### Final combine of all race tables ####

df_race<-rbind(df_nh, df_aian, df_nhpi, df_sswana)%>%
  rename("race"="nh_race")

# Push RACE table to postgres----------------------------------------

# set column types
charvect = rep('varchar', ncol(df_race)) 
charvect <- replace(charvect, c(5,6,7), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df_race)

dbWriteTable(con,  "report_citation_traffic_race", df_race,
             overwrite = TRUE, row.names = FALSE,
             field.types = charvect)


# # write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE report_citation_traffic_race  IS 'Analyzing top 5 traffic citation rates given as a result of a traffic stop within each traffic stop type (moving, nonmoving, equipment)
for each perceived racial group.
The denominator (total column) for this analysis is all traffic stops resulting in a citation within each traffic stop type
for each perceived racial group.
R script used to recode and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Analysis\\report_citation_traffic_type_stop.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_citation_traffic_type_stop.docx';

COMMENT ON COLUMN report_citation_traffic_race.race IS 'Perceived race';
COMMENT ON COLUMN report_citation_traffic_race.statute_literal_25 IS 'Text description for accompnaying offense code';
COMMENT ON COLUMN report_citation_traffic_race.total IS 'Total number (denominator in rate calc) of traffic stops that resulted in a citation within each traffic stop type';
COMMENT ON COLUMN report_citation_traffic_race.count IS 'Count of each specific citation offense code within each traffic stop type';
COMMENT ON COLUMN report_citation_traffic_race.rate IS 'Rate of Rate of traffic stops resulting in a citation by each citation code out of all traffic stops resulting in a citation
within each traffic stop type';
")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)


