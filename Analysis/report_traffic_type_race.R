# Analysis : Analyze traffic stops by traffic violation type (moving, non-moving, equipment) and race ----------------------------------
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

pop<-dbGetQuery(con, "SELECT * FROM population_race_fresno_city")

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

# join the population table

df_pop<-df%>%
  left_join(pop%>%mutate(race=ifelse( race %in% "nh_twoormor", "nh_multiracial", race)),
                         by=c("nh_race"="race"))%>%
  rename("total"="count")%>%
  mutate(total=ifelse(aian_flag==1, 16015,
                      ifelse(nhpi_flag==1, 3192, 
                             ifelse(sswana_flag==1, 33158,  total))))%>%
  mutate(total = case_when(
    nh_race == "nh_multiracial" & sswana_flag==1 ~ 17479,
    TRUE ~ total))%>%  # hard code the one person who is sswana and multiracial to use the multiracial denominator:
  filter(nh_race!="nh_sswana") # no population estimates for nh_sswana

##### NH #####

df1.3<-df_pop%>%
  group_by(nh_race, traffic_violation_type)%>%
  mutate(count=n(),
         rate=count/total*1000)%>%
  slice(1)%>%
  mutate(denom="population_per_1k")%>%
  select(nh_race, traffic_violation_type, denom, total, count,rate)

##### AIAN #####

df1.3_aian<-df_pop%>%
  filter(aian_flag==1)%>%
  group_by(traffic_violation_type)%>%
  mutate(count=n(),
         rate=count/total*1000)%>%
  slice(1)%>%
  mutate(denom="population_per_1k",
         nh_race="aian_aoic")%>%
  select(nh_race, traffic_violation_type, denom, total, count,rate)

##### NHPI #####

df1.3_nhpi<-df_pop%>%
  filter(nhpi_flag==1)%>%
  group_by(traffic_violation_type)%>%
  mutate(count=n(),
         rate=count/total*1000)%>%
  slice(1)%>%
  mutate(denom="population_per_1k",
         nh_race="nhpi_aoic")%>%
  select(nh_race, traffic_violation_type, denom, total, count,rate)

##### SSWANA #####

df1.3_sswana<-df_pop%>%
  filter(sswana_flag==1)%>%
  group_by(traffic_violation_type)%>%
  mutate(count=n(),
         rate=count/total*1000)%>%
  slice(1)%>%
  mutate(denom="population_per_1k",
         nh_race="sswana_aoic")%>%
  select(nh_race, traffic_violation_type, denom, total, count,rate)

#### ASIAN W/O SA DENOMINATOR ####

df1.3_asian<-df%>%
  mutate(nh_race=ifelse(nh_race %in% "nh_asian", "nh_asian_wo_sa", nh_race))%>%
  left_join(pop, by=c("nh_race"="race"))%>%
  rename("total"="count")%>%
  group_by(nh_race, traffic_violation_type)%>%
  mutate(count=n(),
         rate=count/total*1000,
         denom="population_per_1k"
  )%>%
  slice(1)%>%
  select(nh_race, traffic_violation_type, denom, total, count,rate)%>%
filter(grepl("nh_asian_wo_sa", nh_race))

#### Final combine all ables for denom 3 ####

df1.3<-rbind(df1.3, df1.3_aian, df1.3_nhpi, df1.3_sswana, df1.3_asian)

# Final combine of ALL denominator tables-------------------------------

df_final<-rbind(df1, df1.1, df1.2, df1.3)%>%
  rename("race"="nh_race")


# Finalize and push to postgres----------------------------------------

# set column types
charvect = rep('varchar', ncol(df_final)) 
charvect <- replace(charvect, c(4,5,6), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df_final)

dbWriteTable(con,  "report_traffic_type_race", df_final,
             overwrite = TRUE, row.names = FALSE,
             field.types = charvect)


# # write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE report_traffic_type_race  IS 'Analyzing officer-initiated traffic stops by type of traffic stop and by race.
This table includes rates using three different denominators 1) out of all traffic stops witin each racial group (denom==traffic_race)
2) out of all stops within each traffic stop type (deom==traffic_type_)
3) per 1k of the population of each racial group in Fresno city. (denom==population_per_1k)
The denominator is indicated by the denom column in the table. 
R script used to recode and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Analysis\\report_traffic_type_race.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_traffic_type_race.docx';

COMMENT ON COLUMN report_traffic_type_race.race IS 'Perceived race';
COMMENT ON COLUMN report_traffic_type_race.traffic_violation_type IS 'Type of traffic stop (moving, non-moving, equipment)';
COMMENT ON COLUMN report_traffic_type_race.denom IS 'Denominator used in analysis. This table includes three different denominators 1) out of all traffic stops witin each racial group (denom==traffic_race)
2) out of all stops within each traffic stop type (deom==traffic_type_)
3) per 1k of the population of each racial group in Fresno city. (denom==population_per_1k)';
COMMENT ON COLUMN report_traffic_type_race.total IS 'Total number (denominator in rate calc) see denom column for which denominator is used';
COMMENT ON COLUMN report_traffic_type_race.count IS 'Count of officer-initiated traffic stops for each traffic type for each racial group (numerator for rate calc)';
COMMENT ON COLUMN report_traffic_type_race.rate IS 'Rate of traffic stop types for each racial group. Please note that where denom==population that the rate is per 1k of the total population.
All other denom rates are multiplied by 100.';
")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)

