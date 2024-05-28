# Script filters and cleans Fresno RIPA data based on CADOJ RIPA data prepped and cleaned as a part of Catalyst California's RACE COUNTS project
# Original data download link: https://openjustice.doj.ca.gov/data (2022 RIPA Stop Data)
# Data dictionary: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-01/RIPA%20Dataset%20Read%20Me%202022.pdf
# table from RDA databases crime_and_justice.cadoj_ripa_2022

# Set up environment ----
library(RPostgreSQL)
library(dplyr)

# Connect to RDA databases
source("W:\\RDA Team\\R\\credentials_source.R")

fres <- connect_to_db("eci_fresno_ripa")
rda <- connect_to_db("rda_shared_data")


# pull in tables from postgres
cadoj<-dbGetQuery(rda, "SELECT * FROM crime_and_justice.cadoj_ripa_2022")

# Prep Fresno data tables ----
# filter for Fresno PD data
fresno_ripa<-cadoj%>%filter(agency_name=="FRESNO PD")

# count unique stop ids first
number_of_stops<-length(unique(fresno_ripa$doj_record_id))

# number of people stopped
number_of_people<-fresno_ripa%>%distinct(doj_record_id,person_number)%>%nrow()

# people stopped by stop id
stops_persons<-fresno_ripa%>%group_by(doj_record_id)%>%summarise(persons_count=n())

# subset unique stop records
stop_records<-fresno_ripa%>%
  left_join(stops_persons)%>%
  distinct(doj_record_id,agency_ori,agency_name,time_of_stop,date_of_stop,stop_duration,call_for_service,closest_city,school_code,school_name,stop_student,k12_school_grounds,persons_count)
# is student was flagged as a person record for SD project, but here seems value is the same across all people in the stop

# subset person records
person_records<-fresno_ripa%>%
  select(doj_record_id, person_number,13:143)%>%
  select(-call_for_service)


# Push to postgres data ---

persons<-dbGetQuery(con, "SELECT * FROM rel_persons") #person-level, has age
race<-dbGetQuery(con, "SELECT * FROM rel_stops_race") #stop-level