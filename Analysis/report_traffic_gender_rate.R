# Task: Calculate stop rates for traffic violations by gender and race in Fresno city ----------------
# Include rate per 1K, % of all traffic stops either across all traffic stops or within race or within gender

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

# Stop rates as a function of % of traffic stops for each gender --------
# Measure: what % of traffic stops for each gender are made for each racial group

gender_totals<-traffic_nh_race%>%
  filter(!race %in% c('sswana','nhpi','aian'))%>% #exclude the aoic combos so we aren't overcounting gender totals
  group_by(gender)%>%
  summarise(traffic_gender_total=sum(traffic_count))

traffic_race_gender_table<-traffic_race_gender_table%>%left_join(gender_totals)%>%
  mutate(traffic_gender_rate=traffic_count/traffic_gender_total)

# check gender totals
table(ois_traffic$gender)
# checks out comparing to data in traffic_race_gender_table

# Stop rates as a function of % of traffic stops for each racial group --------
# Measure: what % of traffic stops for each racial group are made for each perceived gender

race_totals<-traffic_race_gender_table%>%
  group_by(race)%>%
  summarise(traffic_race_total=sum(traffic_count))

# check totals
table(ois_traffic$nh_race)
sum(ois_traffic$sswana_flag)
sum(ois_traffic$aian_flag)
sum(ois_traffic$nhpi_flag)
# looks good compared to race_totals table

traffic_race_gender_table<-traffic_race_gender_table%>%left_join(race_totals)%>%
  mutate(traffic_race_rate=traffic_count/traffic_race_total)


# Export Data ----
df<-traffic_race_gender_table
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


table_name <- "report_traffic_race_gender"
schema <- 'data'

indicator <- "Rate of people stopped by race and gender for traffic violation reasons calculated 3 ways--out of all officer-initiated traffic stops, out of officer-initiated traffic stops for each gender, and out of all officer-initiated traffic stops for each racial group
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

column_comments <- c('Perceived racial group. All groups are non-Hispanic other than sswana, aian, and nhpi.',
                     'Perceived gender - male, female, transgender/gender non-conforming',
                     'Number of people stopped for officer-initiated traffic stops for that racial group and gender combination',
                     'Total people in Fresno stopped for officer-initiated traffic stops',
                     'Out of all officer-initiated traffic stops in Fresno, the rate or percent of those stops comprised of that racial group and gender combination',
                     'The number of officer-initiated traffic stops for that overall gender group',
                     'The percentage of officer-initiated traffic stops for that gender group that were of that racial group and gender combo',
                     'The number of officer-initiated traffic stops for that overall racial group',
                     'The percentage of officer-initiated traffic stops for that racial group that were of that gender group and racial group combo',
                     'Level or universe of analysis--traffic violations')

# add_table_comments(conn, schema, table_name, indicator, source, column_names, column_comments)

