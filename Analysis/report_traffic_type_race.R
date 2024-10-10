# Analysis : MAnalyze traffic stops by traffic violation type (moving, non-moving, equipment) and race ----------------------------------
# include different measurements all in one table, e.g., per1k_rate, traffic_race_rate, traffic_type_rate


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

# Join tables------------------------------------

df<-stop%>%
  filter(call_for_service==0)%>%
  left_join(person_reason)%>%
  filter(reason=="Traffic violation")%>%
  left_join(person_race)

# Analyze: Not by race-------------------------

#Denominator is out of all officer-initiated traffic stops. 

df1 <- df %>%
  group_by(traffic_violation_type) %>%
  summarise(nh_race="total",
            denom="traffic_total",
            count = n(), .groups = 'drop') %>%
  mutate(total=sum(count),
         rate = count/sum(count) * 100)%>%
  select(nh_race, traffic_violation_type, denom, total, count, rate)

# Analyze: By race---------------------------

#### Denom 1: Out of all traffic stops in each racial group ####

##### NH #####

df1.1<-df%>%
  group_by(nh_race) %>%
  mutate(total=n())%>%
  group_by(nh_race, traffic_violation_type)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="traffic_race")%>%
  select(nh_race, traffic_violation_type, denom, total, count,rate)

##### AIAN #####

df1.1_aian<-df%>%
  filter(aian_flag==1) %>%
  mutate(total=n())%>%
  group_by(traffic_violation_type)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="traffic_race",
         nh_race="aian_aoic")%>%
  select(nh_race, traffic_violation_type, denom, total, count,rate)

##### NHPI #####

df1.1_nhpi<-df%>%
  filter(nhpi_flag==1) %>%
  mutate(total=n())%>%
  group_by(traffic_violation_type)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="traffic_race",
         nh_race="nhpi_aoic")%>%
  select(nh_race, traffic_violation_type, denom, total, count,rate)

##### SSWANA #####

df1.1_sswana<-df%>%
  filter(sswana_flag==1) %>%
  mutate(total=n())%>%
  group_by(traffic_violation_type)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="traffic_race",
         nh_race="sswana_aoic")%>%
  select(nh_race, traffic_violation_type, denom, total, count,rate)


#### Combine all tables for denom 1 ####

df1.1<-rbind(df1.1, df1.1_aian, df1.1_nhpi, df1.1_sswana)

#### Denom 2: Out of all traffic stops in each traffic stop type ####

##### NH #####

df1.2<-df%>%
  group_by(traffic_violation_type) %>%
  mutate(total=n())%>%
  group_by(nh_race, traffic_violation_type)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="traffic_type")%>%
  select(nh_race, traffic_violation_type, denom, total, count,rate)

##### AIAN #####

df1.2_aian<-df%>%
  group_by(traffic_violation_type) %>%
  mutate(total=n())%>%
  filter(aian_flag==1)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="traffic_type",
         nh_race="aian_aoic")%>%
  select(nh_race, traffic_violation_type, denom, total, count,rate)

##### NHPI #####

df1.2_nhpi<-df%>%
  group_by(traffic_violation_type) %>%
  mutate(total=n())%>%
  filter(nhpi_flag==1)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="traffic_type",
         nh_race="nhpi_aoic")%>%
  select(nh_race, traffic_violation_type, denom, total, count,rate)

##### SSWANA #####

df1.2_sswana<-df%>%
  group_by(traffic_violation_type) %>%
  mutate(total=n())%>%
  filter(sswana_flag==1)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(denom="traffic_type",
         nh_race="sswana_aoic")%>%
  select(nh_race, traffic_violation_type, denom, total, count,rate)

##### Combine all tables for denominator 2 #####

df1.2<-rbind(df1.2, df1.2_aian, df1.2_nhpi, df1.2_sswana)


### Denom 3: Population per 1k ####
