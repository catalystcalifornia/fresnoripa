# Task: Calculate stop rates for traffic violations by gender and race in Fresno city ----------------

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


# Stop universe selection and prep ----
# Get count/df for officer-initiated stops
ois <- stops %>% filter(call_for_service==0)%>%select(stop_id)

# Filter for people stopped just for traffic violations
traffic <- p_reasons%>%filter(reason=='Traffic violation')%>%select(stop_id,person_number,reason)

# Filter traffic stops just for ois
ois_traffic<-traffic%>%filter(stop_id %in% ois$stop_id)

# Join race and gender groups to traffic stops
ois_traffic<-ois_traffic%>%left_join(p_races)%>%select(-race_count_re)%>%
  left_join(persons%>%mutate(person_number=as.character(person_number))%>%select(stop_id,person_number,g_full))

# Clean up gender labels
ois_traffic <- ois_traffic %>%
  mutate(gender = recode(g_full,
                         '1'= 'Male',
                         '2' = 'Female',
                         '3' = "Transgender/Gender nonconforming",
                         '4' = 'Transgender/Gender nonconforming',
                         '5' = 'Transgender/Gender nonconforming'))

# Stop rates as a function of % of all traffic stops --------
# Measure: what % of traffic stops does each racial/gender group comprise

# Non-hispanic alone rates
traffic_nh_race <- ois_traffic %>% group_by(nh_race,gender) %>% 
  summarize(traffic_count = n(),.groups='drop')%>% # count of traffic stops
  mutate(traffic_total=sum(traffic_count), # total traffic stops in universe
         traffic_rate=traffic_count/sum(traffic_count)) %>% # percent out of the universe
rename(race=nh_race)

# ALL SSWANA
traffic_sswana <-  ois_traffic %>% group_by(sswana_label,gender) %>% 
  summarize(traffic_count = n(),.groups='drop')%>%
  mutate(traffic_total=sum(traffic_count),
         traffic_rate=traffic_count/sum(traffic_count)) %>% 
  rename(race = sswana_label)%>%
  filter(race == "sswana")

# ALL AIAN
traffic_aian <-  ois_traffic %>% group_by(aian_label,gender) %>% 
  summarize(traffic_count = n(),.groups='drop')%>%
  mutate(traffic_total=sum(traffic_count),
         traffic_rate=traffic_count/sum(traffic_count)) %>% 
  rename(race = aian_label)%>%
  filter(race == "aian")

# ALL NHPI
traffic_nhpi <-  ois_traffic %>% group_by(nhpi_label,gender) %>% 
  summarize(traffic_count = n(),.groups='drop')%>%
  mutate(traffic_total=sum(traffic_count),
         traffic_rate=traffic_count/sum(traffic_count)) %>% 
  rename(race = nhpi_label)%>%
  filter(race == "nhpi")

# combine estimates
traffic_race_gender_table <- bind_rows(traffic_nh_race, traffic_sswana, traffic_aian, traffic_nhpi)

# Stop rates as a function of % of traffic stops for each racial group --------
# Measure: what % of traffic stops for each racial group are made for each gender

nh_race<-ois_traffic%>%
  group_by(nh_race)%>%
  summarise(traffic_race_total=n())%>%
  rename(race=nh_race)

# sswana, aian, nhpi
sswana<-data.frame(race='sswana',traffic_race_total=sum(ois_traffic$sswana_flag))
aian<-data.frame(race='aian',traffic_race_total=sum(ois_traffic$aian_flag))
nhpi<-data.frame(race='nhpi',traffic_race_total=sum(ois_traffic$nhpi_flag))

ois_race_table <- bind_rows(ois_race_table, ois_sswana_table, ois_aian_table, ois_nhpi_table)

traffic_race_table<-traffic_race_table%>%left_join(ois_race_table)%>%
  mutate(ois_rate=traffic_count/ois_count) # percent of ois stops for that race group that were for traffic violations

# Stop rates as a function of population --------
# Measure: Rate of traffic stops per 1K people of same race in fresno

# Join population data
df<-traffic_race_table%>%left_join(population_race%>%select(race,count)%>%rename(pop_count=count))

# Add in rate for nh_asian that excludes south asian from population will rename race to nh_asian_wo_sa for clarity and joining
nh_asian_wo_sa<-traffic_race_table%>%filter(race=='nh_asian')%>%mutate(race='nh_asian_wo_sa')%>%
  left_join(population_race%>%select(race,count)%>%rename(pop_count=count))

df<-bind_rows(df,nh_asian_wo_sa) # combine with df

df$pop_1k_rate<-df$traffic_count/df$pop_count*1000 # calculate rates per 1K

# Add in total rates ----
# Add in measures for total trends in Fresno
tot_pop<-population_race$count[population_race$race=='total'] # store total population

total_df<-data.frame(race='total',
                     traffic_count=NA,
                     traffic_total=nrow(ois_traffic), # total traffic stops in fresno
                     ois_count=nrow(ois), # total ois stops in fresno
                     ois_rate=nrow(ois_traffic)/nrow(ois), # % of ois stops that are for traffic
                     pop_count=tot_pop, # population
                     pop_1k_rate=nrow(ois_traffic)/tot_pop*1000) # rate of traffic stops per 1K people in pop

df<-bind_rows(df,total_df)


# Export Data ----
# filter out nh_sswana given we don't have population figures
df<-df%>%filter(race!='nh_sswana')

# create level
df <- df %>% mutate(reason = "Traffic violation")

# function for adding table and column comments
add_table_comments <- function(con, schema, table_name, indicator, source, column_names, column_comments) {
  comments <- character()
  comments <- c(comments, paste0("
    COMMENT ON TABLE ", schema, ".", table_name, " IS '", table_comment, "';"))
  for (i in seq_along(column_names)) {
    comment <- paste0("
      COMMENT ON COLUMN ", schema, ".", table_name, ".", column_names[i], " IS '", column_comments[i], "';
      ")
    comments <- c(comments, comment)
  }
  sql_commands <- paste(comments, collapse = "")
  dbSendQuery(con, sql_commands)
}


table_name <- "report_traffic_race"
schema <- 'data'

indicator <- "Rate of people stopped by race for traffic violation reasons calculated 3 ways--out of all traffic stops, out of officer-initiated stops for each racial group, and out of the population per 1K for each racial group.
Stops included are people stopped for only a reason of traffic violation during officer-initiated stops, excluding calls for service"
source <- "CADOJ RIPA 2022
See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_report_traffic_race.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Analysis/report_traffic_race.R"
table_comment <- paste0(indicator, source)

# write table
# dbWriteTable(conn, c(schema, table_name),df,
#        overwrite = FALSE, row.names = FALSE)

# comment on table and columns

column_names <- colnames(df) # get column names

column_comments <- c('Racial group. All groups are non-Hispanic other than sswana, aian, and nhpi. Includes an extra row for stop rates calculated as a function of Asian population excluding South Asian given RIPA race category SWANA/SA',
                     'Number of people stopped for traffic stops for that racial group',
                     'Total people in Fresno stopped for traffic stops',
                     'Out of all traffic stops in Fresno, the rate or percent of those stops comprised of that racial group',
                     'The number of officer-initiated stops for that racial group',
                     'The percentage of officer-initiated stops for that racial group that were for traffic violations',
                     'The population in Fresno for that racial group',
                     'The stop rate for traffic violations per 1K people of the same race group in Fresno city',
                     'Level or universe of analysis--traffic violations')

# add_table_comments(conn, schema, table_name, indicator, source, column_names, column_comments)

