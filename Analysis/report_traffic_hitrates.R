# Task search and hit rates by race
#  % of traffic stops of each racial group that resulted in a search, and % of those searches that resulted in a no contraband
# consider gender component

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
p_searches<-dbGetQuery(conn, "SELECT * FROM data.rel_persons_searches")


# Stop universe selection and prep ----
# Get count/df for officer-initiated stops
ois <- stops %>% filter(call_for_service==0)%>%select(stop_id)

# Filter for people stopped just for traffic violations
traffic <- p_reasons%>%filter(reason=='Traffic violation')%>%select(stop_id,person_number,reason)

# Filter traffic stops just for ois
ois_traffic<-traffic%>%filter(stop_id %in% ois$stop_id)

# Join race groups and searches to traffic stops
ois_traffic_searches<-ois_traffic%>%left_join(p_races)%>%select(-race_count_re)%>%
  left_join(p_searches%>%mutate(person_number=as.character(person_number)))%>%
  left_join(persons%>%mutate(person_number=as.character(person_number))%>%select(stop_id,person_number,g_full))


# Clean up gender labels
ois_traffic_searches<- ois_traffic_searches%>%
  mutate(gender = recode(g_full,
                         '1'= 'Male',
                         '2' = 'Female',
                         '3' = "Transgender/Gender nonconforming",
                         '4' = 'Transgender/Gender nonconforming',
                         '5' = 'Transgender/Gender nonconforming'))


# recode searches_count to 0/1 varibales
table(ois_traffic_searches$searches_count)
ois_traffic_searches$searches_count<-replace(ois_traffic_searches$searches_count, ois_traffic_searches$searches_count>=1, 1) 
table(ois_traffic_searches$searches_count)


# SEARCHES AND HIT RATES BY RACE ----
# Search and hit rates by race as a function of % of traffic stops for that race group 
# Measure: what % of traffic stops for each race group result in a search and what % of those result in contraband
# check for data errors
ois_traffic_searches%>%group_by(searches_count,contraband_found)%>%summarise(count=n()) # searches 0/1 by contraband 0/1
# 8 times where supposedly no search took place and contraband was found

nrow(ois_traffic_searches[ois_traffic_searches$searches_count >= 1,]) # total searches
# 74 other searches
# 8/74
# about 10% where no search recorded

# perhaps contraband was found without the search, ignore cases for now given 10% of the data

## Total traffic stops by race ----
# total traffic stops by race
traffic_counts<-ois_traffic_searches%>%
  group_by(nh_race)%>%
  summarise(traffic_total=n(),.groups='drop')


## Search and hit counts for traffic stops by race----
search_counts <- ois_traffic_searches%>%filter(searches_count==1)%>%
  group_by(nh_race)%>%
  summarise(searches_count=sum(searches_count), # total searches
            contraband_found=sum(contraband_found), # searches where contraband found
            .groups='drop')

## Search and hit rates for traffic stops by race ---
nh_search_rates<-traffic_counts%>%left_join(search_counts)%>%
  mutate(searches_count=ifelse(is.na(searches_count),0,searches_count), # assume 0 searches if no search reported
    search_rate=searches_count/traffic_total, # search rate out of traffic stops 
    contraband_rate=contraband_found/searches_count, # contraband/hit rate out of searches
    no_contraband_rate=1-contraband_rate, # no hit rate, false search
    gender='total')%>%
  rename(race=nh_race)

# check nh group differences
sum(ois_traffic_searches[ois_traffic_searches$searches_count == 1,]$aian_flag) # 1
sum(ois_traffic_searches[ois_traffic_searches$searches_count == 1,]$sswana_flag) # same as nh_sswana
sum(ois_traffic_searches[ois_traffic_searches$searches_count == 1,]$nhpi_flag)# 0

## Search and hit rates for AIAN ----
# total traffic
aian_traffic<-nrow(ois_traffic_searches%>%filter(aian_flag==1))

# search and hit rates
aian_search_rates <- ois_traffic_searches%>%filter(searches_count==1 & aian_flag==1)%>%
  group_by(aian_label)%>%
  summarise(searches_count=sum(searches_count), # total searches
            contraband_found=sum(contraband_found), # searches where contraband found
            .groups='drop')%>%
mutate(traffic_total=aian_traffic, # add in total # of traffic stops of aian aoic
  searches_count=ifelse(is.na(searches_count),0,searches_count), # assume 0 searches if no search reported
       search_rate=searches_count/aian_traffic, # search rate out of traffic stops 
       contraband_rate=contraband_found/searches_count, # contraband/hit rate out of searches
       no_contraband_rate=1-contraband_rate, # no hit rate, false search
       gender='total')%>%
  rename(race=aian_label)

# join data frames
df_race_search_rates<-rbind(nh_search_rates%>%
                           select(race,gender,traffic_total,everything()),
                         aian_search_rates%>%select(race,gender,traffic_total,everything()))


# SEARCHES AND HIT RATES BY RACE AND GENDER ----
# Search and hit rates by race and gender as a function of % of traffic stops for that race and gender group 
# Measure: what % of traffic stops for each race/gender group result in a search and what % of those result in contraband

## Total traffic stops by race and gender ----
traffic_counts<-ois_traffic_searches%>%
  group_by(nh_race,gender)%>%
  summarise(traffic_total=n(),.groups='drop')


## Search and hit counts for traffic stops by race and gender----
search_counts <- ois_traffic_searches%>%filter(searches_count==1)%>%
  group_by(nh_race,gender)%>%
  summarise(searches_count=sum(searches_count), # total searches
            contraband_found=sum(contraband_found), # searches where contraband found
            .groups='drop')

## Search and hit rates for traffic stops by race ---
nh_gender_search_rates<-traffic_counts%>%left_join(search_counts)%>%
  mutate(searches_count=ifelse(is.na(searches_count),0,searches_count), # assume 0 searches if no search reported
         search_rate=searches_count/traffic_total, # search rate out of traffic stops 
         contraband_rate=contraband_found/searches_count, # contraband/hit rate out of searches
         no_contraband_rate=1-contraband_rate # no hit rate, false search
        )%>%
  rename(race=nh_race)

# not include aian aoic given only 1 search so would get filtered out later for data protection

# join data frames
df<-rbind(df_race_search_rates,nh_gender_search_rates)%>%
  mutate(level='traffic violation stops')

# Export to postgres
table_name <- "report_traffic_hitrates"
schema <- 'data'

indicator <- "Search and hit rates by race and gender"
source <- "See QA doc for details: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_traffic_hitrates.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Analysis/report_traffic_hitrates.R"
table_comment <- paste0(indicator, source)

# write table
# dbWriteTable(conn, c(schema, table_name),df,
#              overwrite = FALSE, row.names = FALSE)
# 
# comment on table and columns
column_names <- colnames(df) # get column names

column_comments <- c(
  "Perceived race group. NHPI and SSWANA aoic are excluded given their counts did not differ from the nh nhpi and nh sswana groups",
  "Perceived gender group. For purpose of sample size, we combine transgender and gender nonconforming categories",
  "Total traffic stops that occured for race/gender group",
  "Total number of traffic stops of each race and gender group combo that included a search of person or property",
  "Total number of traffic stops of each race/gender group combo that resulted in search and contraband/evidence found",
  "Rate of traffic stops that resulted in searches for that race/gender group",
  "Rate of searches during traffic stops that resulted in contraband found for that race/gender group",
  "Rate of searches during traffic stops that resulted in no contraband found for that race/gender group",
  "Level or universe of analysis--stops for traffic violation reasons"
 )

# add_table_comments(conn, schema, table_name, indicator, source, column_names, column_comments)
