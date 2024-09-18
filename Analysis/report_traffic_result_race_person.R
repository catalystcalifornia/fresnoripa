# Analysis : Results of traffic stops by race ----------------------------------
# Include rate out of each racial group and rate out of each result type (e.g., % of Latinx stops resulting in a warning and % of warning stops that involved Latinx person)

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
# stop_reason<-dbGetQuery(con, "SELECT * FROM rel_stops_reason")
# stop_result<-dbGetQuery(con, "SELECT * FROM rel_stops_result")
# stop_race<-dbGetQuery(con, "SELECT * FROM rel_stops_race")

person<-dbGetQuery(con, "SELECT * FROM rel_persons")
person_reason<-dbGetQuery(con, "SELECT * FROM rel_persons_reason")
person_result<-dbGetQuery(con, "SELECT * FROM rel_persons_result")
person_race<-dbGetQuery(con, "SELECT * FROM rel_races_recode")

pop<-dbGetQuery(con, "SELECT * FROM population_race_fresno_city")

# Join tables--------------------------------

# join stops to person level tables to filter for only calls for service

df<-stop%>%
  filter(call_for_service==0)%>%
  left_join(person_reason)%>%
  left_join(person_result, by=c("stop_id", "person_number"))%>%
  left_join(person_race)

# Analysis----------------------------------------

#### Denominator 1: Out of all traffic stops for each racial group ####

###### NH ######

df1<-df%>%
  filter(reason=='Traffic violation')%>%
  group_by(nh_race)%>%
mutate(total=n())%>%
  group_by(nh_race, stop_result_simple)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="traffic_stop_race")%>%
  select(nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange(nh_race, -rate)%>%
  filter(!grepl("nh_aian|nh_nhpi|nh_sswana", nh_race))

###### SWANA ######

df1_sswana<-df%>%
  filter(reason=='Traffic violation' & sswana_flag==1)%>%
  mutate(total=n())%>%
  group_by(stop_result_simple)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="traffic_stop_race",
         nh_race="sswana_aoic")%>%
  ungroup()%>%
  select(nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange( -rate)

###### AIAN ######

df1_aian<-df%>%
  filter(reason=='Traffic violation' & aian_flag==1)%>%
  mutate(total=n())%>%
  group_by(stop_result_simple)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="traffic_stop_race",
         nh_race="aian_aoic")%>%
  ungroup()%>%
  select(nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange( -rate)

###### NHPI ######


df1_nhpi<-df%>%
  filter(reason=='Traffic violation' & nhpi_flag==1)%>%
  mutate(total=n())%>%
  group_by(stop_result_simple)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="traffic_stop_race",
         nh_race="nhpi_aoic")%>%
  ungroup()%>%
  select(nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange( -rate)

# Join all tables together

df1<-rbind(df1, df1_aian, df1_nhpi, df1_sswana)%>%
  rename("race"="nh_race")

#### Denominator 2: Out of all traffic stop results ####

###### NH ######

df2<-df%>%
  filter(reason=='Traffic violation')%>%
  group_by(stop_result_simple)%>%
  mutate(total=n())%>%
  group_by(nh_race, stop_result_simple)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="traffic_result")%>%
  select(nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange(nh_race, -rate)%>%
  filter(!grepl("nh_aian|nh_nhpi|nh_sswana", nh_race))

###### SWANA ######

df2_sswana<-df%>%
  filter(reason=='Traffic violation')%>%
  group_by(stop_result_simple)%>%
  mutate(total=n())%>%
  group_by(stop_result_simple, sswana_flag)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  filter(sswana_flag==1)%>%
  mutate(denom="traffic_result",
         nh_race="sswana_aoic")%>%
  ungroup()%>%
  select( nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange( -rate)

###### AIAN ######

df2_aian<-df%>%
  filter(reason=='Traffic violation')%>%
  group_by(stop_result_simple)%>%
  mutate(total=n())%>%
  group_by(stop_result_simple, aian_flag)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  filter(aian_flag==1)%>%
  mutate(denom="traffic_result",
         nh_race="aian_aoic")%>%
  ungroup()%>%
  select( nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange( -rate)

###### NHPI ######

df2_nhpi<-df%>%
  filter(reason=='Traffic violation')%>%
  group_by(stop_result_simple)%>%
  mutate(total=n())%>%
  group_by(stop_result_simple, nhpi_flag)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  filter(nhpi_flag==1)%>%
  mutate(denom="traffic_result",
         nh_race="nhpi_aoic")%>%
  ungroup()%>%
  select( nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange( -rate)

# Join all tables together

df2<-rbind(df2, df2_aian, df2_nhpi, df2_sswana)%>%
  rename("race"="nh_race")


#### Denominator 3: Per 1k total population for each racial group ####

###### NH ######

df3<-df%>%
  left_join(pop, by=c("nh_race"="race"))%>%
  rename("pop_total"="count")%>%
  filter(reason=="Traffic violation")%>%
  group_by(nh_race, stop_result_simple)%>%
  mutate(count=n(),
  rate_per_1k=count/pop_total*1000,
  denom="population"
  )%>%
  slice(1)%>%
  select(nh_race, reason, denom, stop_result_simple, pop_total, count, rate_per_1k)%>%
  filter(!grepl("nh_aian|nh_nhpi|nh_sswana", nh_race))

###### SWANA ######

df3_sswana<-df%>%
  filter(sswana_flag==1)%>%
  left_join(pop, by=c("sswana_label"="race"))%>%
  rename("pop_total"="count")%>%
  filter(reason=="Traffic violation")%>%
  group_by(stop_result_simple)%>%
  mutate(count=n(),
         rate_per_1k=count/pop_total*1000,
         denom="population",
         nh_race="sswana_aoic")%>%
  slice(1)%>%
  ungroup()%>%
  select(nh_race, reason, denom, stop_result_simple, pop_total, count, rate_per_1k)

###### AIAN ######

df3_aian<-df%>%
  filter(aian_flag==1)%>%
  left_join(pop, by=c("aian_label"="race"))%>%
  rename("pop_total"="count")%>%
  filter(reason=="Traffic violation")%>%
  group_by(stop_result_simple)%>%
  mutate(count=n(),
         rate_per_1k=count/pop_total*1000,
         denom="population",
         nh_race="aian_aoic")%>%
  slice(1)%>%
  ungroup()%>%
  select(nh_race, reason, denom, stop_result_simple, pop_total, count, rate_per_1k)

###### NHPI ######

df3_nhpi<-df%>%
  filter(nhpi_flag==1)%>%
  left_join(pop, by=c("nhpi_label"="race"))%>%
  rename("pop_total"="count")%>%
  filter(reason=="Traffic violation")%>%
  group_by(stop_result_simple)%>%
  mutate(count=n(),
         rate_per_1k=count/pop_total*1000,
         denom="population",
         nh_race="nhpi_aoic")%>%
  slice(1)%>%
  ungroup()%>%
  select(nh_race, reason, denom, stop_result_simple, pop_total, count, rate_per_1k)

# Combine all tables together

df3<-rbind(df3, df3_aian, df3_nhpi, df3_sswana)%>%
  rename("race"="nh_race")

# Final combination of all analysis tables -----------------------------------

df<-rbind(df1, df2, df3)

# Push table to postgres-----------------------------------------

# set column types
charvect = rep('varchar', ncol(df)) 
charvect <- replace(charvect, c(5,6,7,8,9), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df)

dbWriteTable(con,  "report_traffic_result_race_person", df,
             overwrite = TRUE, row.names = FALSE,
             field.types = charvect)


# # write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE report_traffic_result_race_person  IS 'Analyzing officer-initiated traffic stops by simple stop result by race. This table includes rates using three different denominators 1) out of all traffic stops witin each racial group 2) out of all traffic stop results 3) per 1k of the population of each racial group in Fresno city.
The denominator is indicated by the denom column in the table. 
R script used to recode and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Analysis\\report_traffic_result_race_person.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_traffic_result_race_person.docx';

COMMENT ON COLUMN report_traffic_result_race_person.race IS 'Perceived race';
COMMENT ON COLUMN report_traffic_result_race_person.reason IS 'Reason for stop (which will only be traffic violations for this analysis)';
COMMENT ON COLUMN report_traffic_result_race_person.denom IS 'Denominator used in analysis';
COMMENT ON COLUMN report_traffic_result_race_person.stop_result_simple IS 'Simple result for stop';
COMMENT ON COLUMN report_traffic_result_race_person.total IS 'Total number (denominator in rate calc) see denom column for which denominator is used';
COMMENT ON COLUMN report_traffic_result_race_person.count IS 'Count of officer-initiated traffic stops for each stop result for each racial group (numerator for rate calc)';
COMMENT ON COLUMN report_traffic_result_race_person.rate IS 'Rate of traffic stop results by race: count/total*100 (see denom colunm for more clarity on which demominator is being used to create the rate)';
COMMENT ON COLUMN report_traffic_result_race_person.pop_total IS 'Total population for racial group in Fresno City based on census data';
COMMENT ON COLUMN report_traffic_result_race_person.rate_per_1k IS 'Rate of traffic stops by result of stop for each perceived racial group per 1k total population of each racial gorup in Fresno city';
")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)