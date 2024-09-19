# Analysis : Most common traffic citations resulting from stop by traffic stop type ----------------------------------
# Top offense codes by traffic stop reason (moving, equipment, non-moving)

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
stop_reason<-dbGetQuery(con, "SELECT * FROM rel_stops_reason")
stop_result<-dbGetQuery(con, "SELECT * FROM rel_stops_result")

person_reason<-dbGetQuery(con, "SELECT * FROM rel_persons_reason")
person_result<-dbGetQuery(con, "SELECT * FROM rel_persons_result")

offense_codes<-dbGetQuery(con, "SELECT * FROM cadoj_ripa_offense_codes_2023")


# Join tables------------------------------------

df<-stop%>%
  filter(call_for_service==0)%>%
  left_join(person_reason)%>%
  filter(reason=="Traffic violation")%>%
  left_join(person_result)

# Analysis-------------------------------------------

# Rate: Offense codes / all citations WITHIN each traffic type --moving/nonmoving/equipment


df%>%
  filter(stop_result_simple=="citation for infraction")%>%
  group_by(traffic_violation_type)%>%
  mutate(total=n())%>%
  group_by(traffic_violation_type, rfs_traffic_violation_code)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  select(traffic_violation_type, rfs_traffic_violation_code, statute_literal_25,  total, count, rate)%>%
  arrange(traffic_violation_type, -rate)%>%
  group_by(traffic_violation_type)%>%
  slice(1:10)
  
  
