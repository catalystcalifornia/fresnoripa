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

# Use of force rates by race as a function of % of all traffic stops --------
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

# Use of force rates by race as a function of % of all race group traffic stops --------
# Measure: what % of traffic stops of each race group results in use of force
uof_race_rates<-ois_traffic%>%
  group_by(nh_race)%>%
  summarise(traffic_race_count=n(),
            uof_race_count=sum(use_of_force),
            uof_race_rate=sum(use_of_force)/traffic_race_count,
            .groups='drop')%>%
  rename(race=nh_race)



# Use of force rates by race and gender as a function of % of all traffic stops --------
# Measure: what % of uof incidents does each racial group comprise
uof_gender_rates<-traffic_uof%>%
  group_by(nh_race,gender)%>%
  summarise(uof_gender_race_count=n(),.groups='drop')%>%
  mutate(uof_traffic_total=sum(uof_gender_race_count),
         uof_traffic_gender_rate=uof_gender_race_count/sum(uof_gender_race_count))%>%
  rename(race=nh_race)

# check uof stops
print(select(traffic_uof,stop_id,person_number,nh_race)%>%arrange(stop_id))
# UOF stops of people perceived as asian occurred during 1 stop

actions_sum<-numcolwise(sum)(ois_traffic)%>%
  pivot_longer(everything(),names_to='variable',values_to='count')
# detained and removed from vehicle highest counts

# Use of force rates by race as a function of % of all traffic stops --------
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

# Use of force rates by race as a function of % of all race group traffic stops --------
# Measure: what % of traffic stops of each race group results in use of force
uof_race_rates<-ois_traffic%>%
  group_by(nh_race)%>%
  summarise(traffic_race_count=n(),
            uof_race_count=sum(use_of_force),
            uof_race_rate=sum(use_of_force)/traffic_race_count,
            .groups='drop')%>%
  rename(race=nh_race)

