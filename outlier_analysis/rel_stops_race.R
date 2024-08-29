#### Overview ####
# Output: Create stops by race table with one race value per stop #

## Set out workspace ##

library(RPostgreSQL)
library(tidyr)
library(dplyr)

## Load data ##
source("W:\\RDA Team\\R\\credentials_source.R")
conn <- connect_to_db("eci_fresno_ripa")

# Pull in recoded race table
races<-dbGetQuery(conn," SELECT * FROM rel_races_recode")

# Create object for number of unique stops
unique_stops <- races%>%
  distinct(stop_id)%>%
  nrow()

## Recoding race variable to stop level ##


### Step 1: get a table that shows all the stop_ids and list of persons races in the  stop ###

# group by stop id and race
race_step1<-races%>%
  group_by(stop_id, nh_race)%>%
  summarise(count=n())

###  Step 2: identify stops that involved a combination of folks of different races as perceived by officers and count number of people in the stop ### 
race_step2<-race_step1%>%
  group_by(stop_id)%>%
  mutate(nh_race_list=paste(nh_race, collapse = ", "))%>%
  summarise(person_count=sum(count),
            race_count=n(),
            nh_race_list=min(nh_race_list))%>%
  mutate(multiracestop = ifelse(race_count > 1, "Multiple races", "Single race"))

### Step 3: Recode stop result to simplified version while retaining original results ###
race_step3<-race_step2%>%
  mutate(stop_nh_race=ifelse(multiracestop=='Multiple races', 'nh_multi_race',nh_race_list))%>%
  select(stop_id,stop_nh_race,nh_race_list,race_count,person_count)

### Step 4: Double check it worked ###
final_check<-race_step3%>%
  group_by(stop_id,stop_nh_race)%>%
  summarise(count=n())

unique_stops_check<-final_check%>%
  distinct(stop_id)%>%
  nrow()

  ## Final check has as many distinct stop ids as original data

# check that NAs get included in people counts
na<-races%>%
  filter(is.na(nh_race))

  ## 0 NAs for race in original races data

### AIAN-NHPI-SSWANA Let's recode aian field to stop level ###
# include a flag if everyone in the stop was all-aian, all-nhpi, all-sswana, etc.

### Step 1: Select flag categories and group at stop level ###
# Identify the count of people in each stop that was aian, sswana, or nhpi
step1<-races%>%
  select(stop_id, person_number, sswana_flag, aian_flag, nhpi_flag)

step2<-step1%>%
  group_by(stop_id)%>%
  summarise(aian=sum(aian_flag),
            sswana=sum(sswana_flag),
            nhpi=sum(nhpi_flag),
            person_count=n())

### Step 2: Recode to flag stops where everyone in the stop was aian, nhpi, or sswana for each ###
step3<-step2%>%
  mutate(aian_flag=ifelse(aian==person_count,1,0),
         nhpi_flag=ifelse(nhpi==person_count,1,0),
         sswana_flag=ifelse(sswana==person_count,1,0))%>%
  select(stop_id, aian_flag, nhpi_flag, sswana_flag)

### Join nh_race and aian, nhpi, sswana tables ###
final_step<-race_step3%>%
  left_join(step3,by="stop_id")%>%
  select(stop_id, person_count, stop_nh_race, race_count, nh_race_list, aian_flag, nhpi_flag, sswana_flag)

#### Push summarized table to postgres ####
# write and push table
dbWriteTable(conn,  "rel_stops_race", final_step, 
             overwrite = TRUE, row.names = FALSE)

# write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE rel_stops_race  IS 
'Table recoding race at the stop level for regression analysis and time spent analysis. Table also includes people in the stop
Race based on our recoded race fields. Includes mutually exclusive nh_race fields as well as flags for aian, nhpi, and sswana
See W:/Project/ECI/Fresno RIPA/GitHub/IB/fresnoripa/outlier_analysis/rel_stops_race.R';
COMMENT ON COLUMN rel_stops_race.stop_id IS 'unique stop id';
COMMENT ON COLUMN rel_stops_race.person_count IS 'Number of people in the stop';
COMMENT ON COLUMN rel_stops_race.stop_nh_race IS 'Combined race of people involved in the stop. nh_multi_race means stop included people of different races, e.g., Black and Latinx, Black and Two or More, Latinx, NHPI, AIAN, etc..';
COMMENT ON COLUMN rel_stops_race.race_count IS 'Number of unique races included in the stop, includes count of NA if applicable for null races';
COMMENT ON COLUMN rel_stops_race.nh_race_list IS 'List of unique races of people included in the stop';
COMMENT ON COLUMN rel_stops_race.aian_flag IS 'Flag of 1 if all people in the stop where AIAN alone or in combo';
COMMENT ON COLUMN rel_stops_race.nhpi_flag IS 'Flag of 1 if all people in the stop where NHPI alone or in combo';
COMMENT ON COLUMN rel_stops_race.sswana_flag IS 'Flag of 1 if all people in the stop where SSWANA alone or in combo';
                        ")

# send table comment + column metadata
dbSendQuery(conn = conn, table_comment)
