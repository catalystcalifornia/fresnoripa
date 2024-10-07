# Task use of force by race and gender
#  % of traffic stops of each racial group that resulted in a use of force, and % of use of force incidents by race group (e.g., % of Latinx traffic stops that involved use of force and % of use of force traffic stops that were of latinx people)—>consider adding gender

# Environment set up ----
# Load Packages
library(tidyverse)
library(RPostgreSQL)
library(dplyr)
source("W:\\RDA Team\\R\\credentials_source.R")
conn <- connect_to_db("eci_fresno_ripa")

# Import data
p_races <- dbGetQuery(conn, "SELECT * FROM data.rel_races_recode")
stops <- dbGetQuery(conn, "SELECT * FROM data.rel_stops")
persons<-dbGetQuery(conn, "SELECT * FROM data.rel_persons")
p_reasons <- dbGetQuery(conn, "SELECT * FROM data.rel_persons_reason")
p_actions<-dbGetQuery(conn, "SELECT * FROM data.rel_persons_actions")
p_results<-dbGetQuery(conn, "SELECT * FROM data.rel_persons_result")

# Stop universe selection and prep ----
# Get count/df for officer-initiated stops
ois <- stops %>% filter(call_for_service==0)%>%select(stop_id)

# Filter for people stopped just for traffic violations
traffic <- p_reasons%>%filter(reason=='Traffic violation')%>%select(stop_id,person_number,reason)

# Filter traffic stops just for ois
ois_traffic<-traffic%>%filter(stop_id %in% ois$stop_id)

# Join race groups and uof to traffic stops
ois_traffic<-ois_traffic%>%left_join(p_races)%>%select(-race_count_re)%>%
  left_join(p_actions%>%mutate(person_number=as.character(person_number)))%>%
  left_join(persons%>%mutate(person_number=as.character(person_number))%>%select(stop_id,person_number,g_full))


# Clean up gender labels
ois_traffic <- ois_traffic %>%
  mutate(gender = recode(g_full,
                         '1'= 'Male',
                         '2' = 'Female',
                         '3' = "Transgender/Gender nonconforming",
                         '4' = 'Transgender/Gender nonconforming',
                         '5' = 'Transgender/Gender nonconforming'))

# Use of force df
traffic_uof<-ois_traffic%>%filter(use_of_force==1)

# UOF BY RACE ----
## Use of force rates by race as a function of % of all traffic stops --------
# Measure: what % of uof incidents does each racial group comprise
uof_traffic_rates<-traffic_uof%>%
  group_by(nh_race)%>%
  summarise(uof_race_count=n(),.groups='drop')%>%
  mutate(uof_traffic_total=sum(uof_race_count),
         uof_traffic_rate=uof_race_count/sum(uof_race_count))%>%
  rename(race=nh_race)

# check for aian, swana, nhpi use of force
sum(traffic_uof$aian_flag) # 0 
sum(traffic_uof$sswana_flag) # same as nh_sswana
sum(traffic_uof$nhpi_flag) # 0

## Use of force rates by race as a function of % of all race group traffic stops --------
# Measure: what % of traffic stops of each race group results in use of force
uof_race_rates<-ois_traffic%>%
  group_by(nh_race)%>%
  summarise(traffic_race_count=n(),
            uof_race_count=sum(use_of_force),
            uof_race_rate=sum(use_of_force)/traffic_race_count,
            .groups='drop')%>%
  rename(race=nh_race)

df_race_rates<-uof_traffic_rates%>%left_join(uof_race_rates)

# UOF BY RACE AND GENDER ----
## Use of force rates by race and gender as a function of % of all traffic stops --------
# Measure: what % of uof incidents does each racial and gender group comprise
uof_gender_traffic_rates<-traffic_uof%>%
  group_by(nh_race,gender)%>%
  summarise(uof_count=n(),.groups='drop')%>%
  mutate(uof_total=sum(uof_count),
         uof_total_rate=uof_count/sum(uof_count))%>%
  rename(race=nh_race)

## Use of force rates by race and gender as a function of % of traffic stops by race --------
# Measure: what % of traffic stops of each race and gender group results in uof
uof_gender_race_rates<-ois_traffic%>%
  group_by(nh_race,gender)%>%
  summarise(uof_count=sum(use_of_force),
            gender_race_traffic_total=n(),
            uof_gender_race_rate=sum(use_of_force)/gender_race_traffic_total,
            .groups='drop')%>%
  rename(race=nh_race)

## Clean up for postgres ----
df_gender_rates<-uof_gender_race_rates%>%left_join(uof_gender_traffic_rates)

# assign total uof to all
total<-unique(uof_gender_traffic_rates$uof_total)

df_gender_rates$uof_total<-total

# place NAs with 0
0->df_gender_rates[is.na(df_gender_rates)]
  
# check uof stops
print(select(traffic_uof,stop_id,person_number,nh_race)%>%arrange(stop_id))
# UOF stops of people perceived as asian occurred during 1 stop

# Export to postgres
table_name <- "report_traffic_uof"
schema <- 'data'

indicator <- "Use of force rates by race and gender calculated both as a % of all traffic stops that resulted in use of force and % of traffic stops by race. Note that all uof incidents against Asian males occured during the same stop"
source <- "See QA doc for details: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_traffic_uof_detained.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Analysis/report_traffic_uof_detained.R"
table_comment <- paste0(indicator, source)

# write table
# dbWriteTable(conn, c(schema, table_name),df_gender_rates,
#              overwrite = FALSE, row.names = FALSE)

# comment on table and columns
column_names <- colnames(df_gender_rates) # get column names

column_comments <- c(
  "Perceived race group. Only nh groups are included given that aian, nhpi, and sswana rates did not differ from the nh groups for these",
  "Perceived gender group. For purpose of sample size, we combine transgender and gender nonconforming categories",
  "Use of force count for each race and gender group combo - only uof that occurred during traffic stops",
  "Total number of traffic stops of each race and gender group combo",
  "Rate of uof used against race and gender group calculated as a percentage of the total traffic stops that were conducted for that race and gender group",
  "Total uof incidents that took place during traffic stops in all of fresno across gender and race groups",
  "Share of uof incidents that were conducted for that gender and race group out of all instances of use of force during traffic stops"
)

# add_table_comments(conn, schema, table_name, indicator, source, column_names, column_comments)

# Explore other actions ----
actions_sum<-ois_traffic%>%select(is.numeric)%>%summarise_all(funs(sum),na.rm=TRUE)%>%
  pivot_longer(everything(),names_to='variable',values_to='count')
# detained and removed from vehicle highest counts

# DETAINED BY RACE AND GENDER ----
## Detained rates by race and gender as a function of % of all traffic stops --------
# Measure: what % of detained incidents does each racial and gender group comprise
detained_gender_traffic_rates<-ois_traffic%>%filter(detained==1)%>%
  group_by(nh_race,gender)%>%
  summarise(detained_count=n(),.groups='drop')%>%
  mutate(detained_total=sum(detained_count),
         detained_total_rate=detained_count/sum(detained_count))%>%
  rename(race=nh_race)

# check for aian, swana, nhpi use of force
sum(ois_traffic[ois_traffic$detained == 1,]$aian_flag) # 1
sum(ois_traffic[ois_traffic$detained == 1,]$sswana_flag) # same as nh_sswana
sum(ois_traffic[ois_traffic$detained == 1,]$nhpi_flag)# 0

# add aian to the dataframe
detained_aian_traffic_rates<-ois_traffic%>%filter(detained==1)%>%
  group_by(aian_label,gender)%>%
  summarise(detained_count=n(),.groups='drop')%>%
  mutate(detained_total=sum(detained_count),
         detained_total_rate=detained_count/sum(detained_count))%>%
  rename(race=aian_label)%>%
  filter(race=='aian')

detained_gender_traffic_rates<-rbind(detained_gender_traffic_rates,detained_aian_traffic_rates)

## Detained rates by race and gender as a function of % of traffic stops by race --------
# Measure: what % of traffic stops of each race and gender group results in being detained
detained_gender_race_rates<-ois_traffic%>%
  group_by(nh_race,gender)%>%
  summarise(detained_count=sum(detained),
            gender_race_traffic_total=n(),
            detained_gender_race_rate=sum(detained)/gender_race_traffic_total,
            .groups='drop')%>%
  rename(race=nh_race)

# add aian to the dataframe
detained_aian_gender_rates<-ois_traffic%>%
  group_by(aian_label,gender)%>%
  summarise(detained_count=sum(detained),
            gender_race_traffic_total=n(),
            detained_gender_race_rate=sum(detained)/gender_race_traffic_total,
            .groups='drop')%>%
  rename(race=aian_label)%>%
  filter(race=='aian')

detained_gender_race_rates<-rbind(detained_gender_race_rates,detained_aian_gender_rates)

## Clean up for postgres ----
df_gender_rates_detained<-detained_gender_race_rates%>%left_join(detained_gender_traffic_rates)

# assign total uof to all
total<-unique(detained_gender_traffic_rates$detained_total)

df_gender_rates_detained$detained_total<-total

# place NAs with 0
0->df_gender_rates_detained[is.na(df_gender_rates_detained)]

# Export to postgres
table_name <- "report_traffic_detained"
schema <- 'data'

indicator <- "Detained rates by race and gender calculated both as a % of all traffic stops that resulted in detainment and % of traffic stops by race."
source <- "See QA doc for details: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_traffic_uof_detained.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Analysis/report_traffic_uof_detained.R"
table_comment <- paste0(indicator, source)

# write table
# dbWriteTable(conn, c(schema, table_name),df_gender_rates_detained,
#              overwrite = FALSE, row.names = FALSE)

# comment on table and columns
column_names <- colnames(df_gender_rates_detained) # get column names

column_comments <- c(
  "Perceived race group. Only nh groups are included for nhpi given that nh and aoic groups did not differ for this group",
  "Perceived gender group. For purpose of sample size, we combine transgender and gender nonconforming categories",
  "Detained count for each race and gender group combo - only detained incidents that occurred during traffic stops",
  "Total number of traffic stops of each race and gender group combo",
  "Rate of at which each race and gender group were detained during traffic stops calculated as a percentage of the total traffic stops that were conducted for that race and gender group",
  "Total people detained during traffic stops in all of fresno across gender and race groups",
  "Share of total people detained that were from that gender and race group out of all instances of detainment during traffic stops"
)

# add_table_comments(conn, schema, table_name, indicator, source, column_names, column_comments)

# test results of detained incidents
d_results<-ois_traffic%>%filter(detained==1)%>%left_join(p_results)

test<-d_results%>%group_by(nh_race,stop_result_simple)%>%
  summarise(count=n(),.groups='drop')%>%
  group_by(nh_race)%>%
  mutate(rate=count/sum(count))
# nh_white most likely to result in custodial arrest with or without warrant

#disconnect
dbDisconnect(conn)