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

# Join tables--------------------------------

# join stops to person level tables to filter for only calls for service

df<-stop%>%
  filter(call_for_service==0)%>%
  left_join(person_reason)%>%
  left_join(person_result, by=c("stop_id", "person_number"))%>%
  left_join(person_race)

# Analysis----------------------------------------

#### Denominator 1: Out of all traffic stops for each racial group ####

## NH races

df1<-df%>%
  filter(reason=='Traffic violation')%>%
  group_by(nh_race)%>%
mutate(total=n())%>%
  group_by(nh_race, stop_result_simple)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="Traffic violations per race")%>%
  select(nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange(nh_race, -rate)%>%
  filter(!grepl("nh_aian|nh_nhpi|nh_sswana", nh_race))

## SSWANA/NHPI/AIAN

df1_sswana<-df%>%
  filter(reason=='Traffic violation' & sswana_flag==1)%>%
  mutate(total=n())%>%
  group_by(stop_result_simple)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="Traffic violations per race",
         nh_race="sswana_aoic")%>%
  ungroup()%>%
  select(nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange( -rate)

df1_aian<-df%>%
  filter(reason=='Traffic violation' & aian_flag==1)%>%
  mutate(total=n())%>%
  group_by(stop_result_simple)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="Traffic violations per race",
         nh_race="aian_aoic")%>%
  ungroup()%>%
  select(nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange( -rate)

df1_nhpi<-df%>%
  filter(reason=='Traffic violation' & nhpi_flag==1)%>%
  mutate(total=n())%>%
  group_by(stop_result_simple)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="Traffic violations per race",
         nh_race="nhpi_aoic")%>%
  ungroup()%>%
  select(nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange( -rate)

# Join all tables together

df1<-rbind(df1, df1_aian, df1_nhpi, df1_sswana)%>%
  rename("race"="nh_race")

#### Denominator 2: Out of all traffic stop results ####

## NH races

df2<-df%>%
  filter(reason=='Traffic violation')%>%
  group_by(stop_result_simple)%>%
  mutate(total=n())%>%
  group_by(nh_race, stop_result_simple)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="Traffic results")%>%
  select(nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange(nh_race, -rate)%>%
  filter(!grepl("nh_aian|nh_nhpi|nh_sswana", nh_race))

## SSWANA/NHPI/AIAN

df2_sswana<-df%>%
  filter(reason=='Traffic violation')%>%
  group_by(stop_result_simple)%>%
  mutate(total=n())%>%
  group_by(stop_result_simple, sswana_flag)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  filter(sswana_flag==1)%>%
  mutate(denom="Traffic results",
         nh_race="sswana_aoic")%>%
  ungroup()%>%
  select( nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange( -rate)


df2_aian<-df%>%
  filter(reason=='Traffic violation')%>%
  group_by(stop_result_simple)%>%
  mutate(total=n())%>%
  group_by(stop_result_simple, aian_flag)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  filter(aian_flag==1)%>%
  mutate(denom="Traffic results",
         nh_race="aian_aoic")%>%
  ungroup()%>%
  select( nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange( -rate)


df2_nhpi<-df%>%
  filter(reason=='Traffic violation')%>%
  group_by(stop_result_simple)%>%
  mutate(total=n())%>%
  group_by(stop_result_simple, nhpi_flag)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  filter(nhpi_flag==1)%>%
  mutate(denom="Traffic results",
         nh_race="nhpi_aoic")%>%
  ungroup()%>%
  select( nh_race, reason, denom, stop_result_simple, total, count, rate)%>%
  arrange( -rate)

# Join all tables together

df2<-rbind(df2, df2_aian, df2_nhpi, df2_sswana)%>%
  rename("race"="nh_race")


# Denominator 3: Per 1k total population for each racial group (?)




