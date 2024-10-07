###Analysis: Traffic stops by citation stop result and by race
###Also focusing on traffic stops that result in a 'driving without a license' citation

#Set up work space---------------------------------------

library(RPostgreSQL)
library(dplyr)
library(tidyr)
library(stringr)

#connect to postgres

source("W:\\RDA Team\\R\\credentials_source.R")

con <- connect_to_db("eci_fresno_ripa")

# pull in necessary analysis tables

stops<-dbGetQuery(con, "SELECT * FROM rel_stops")

person<-dbGetQuery(con, "SELECT * FROM rel_persons")%>%
  mutate(person_number=as.character(person_number))

person_reason<-dbGetQuery(con, "SELECT * FROM rel_persons_reason")
person_result<-dbGetQuery(con, "SELECT * FROM rel_persons_result")
person_citation_result<-dbGetQuery(con, "SELECT * FROM rel_persons_citations")%>%
  mutate(person_number=as.character(person_number))
person_race<-dbGetQuery(con, "SELECT * FROM rel_races_recode")

pop<-dbGetQuery(con, "SELECT * FROM population_race_fresno_city")

offense_codes<-dbGetQuery(con, "SELECT * FROM cadoj_ripa_offense_codes_2023")

# join necessary tables for analysis 

df<-person%>%
  left_join(stops)%>%
  left_join(person_reason, by =c("stop_id","person_number"))%>%
  filter(call_for_service==0 & reason=='Traffic violation')%>%
  left_join(person_result)%>%
  left_join(person_citation_result)%>%
  filter(stop_result_simple=="citation for infraction" & 
           result_category == "Citation for infraction")%>%
  left_join(person_race)%>%
  left_join(offense_codes, by=c("offense_code"="offense_code"))

names(df) <- gsub(x = names(df), pattern = "\\.x", replacement = "")  


# Sub-Analysis 1---------------------
# Table of top 3-5 traffic code results by race

#### Denom 1: all citations within each racial group ####

# Analyze

### NH ###

df1.1<-df%>%
  group_by(nh_race)%>%
  mutate(total=n())%>%
  group_by(nh_race, statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100,
         denom="citation_race")%>%
  slice(1)%>%
  ungroup()%>%
  select(nh_race, denom, statute_literal_25, total, count,rate)%>%
  arrange(-rate)%>%
  arrange(nh_race, -rate)%>%
  group_by(nh_race)%>%
  slice(1:5)

### AIAN ###

df1.1_aian<-df%>%
  filter(aian_flag==1)%>%
  mutate(total=n())%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="aian_aoic",
         denom="citation_race")%>%
  select(nh_race,  denom, statute_literal_25, total, count, rate)%>%
  arrange(-rate)%>%
  slice(1:5)

### NHPI ###

df1.1_nhpi<-df%>%
  filter(nhpi_flag==1)%>%
  mutate(total=n())%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="nhpi_aoic",
         denom="citation_race")%>%
  select(nh_race, denom,  statute_literal_25, total, count, rate)%>%
  arrange(-rate)%>%
  slice(1:5)

### SWANA/SA ###

df1.1_sswana<-df%>%
  filter(sswana_flag==1)%>%
  mutate(total=n())%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="sswana_aoic",
         denom="citation_race")%>%
  select(nh_race, denom, statute_literal_25, total, count, rate)%>%
  arrange(-rate)%>%
  slice(1:5)

## Combine all tables together ##

df1.1<-rbind(df1.1, df1.1_aian, df1.1_nhpi, df1.1_sswana)%>%
  rename("race"="nh_race")


#### Denom 2: all people stopped within each citation result ####

# Analyze

### NH ###

df1.2<-df%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(total=n())%>%
  group_by(nh_race, statute_literal_25, offense_type_of_charge)%>%
  mutate(count=n(),
         rate=count/total*100,
         denom="citation_result")%>%
  slice(1)%>%
  ungroup()%>%
  select(nh_race, denom, statute_literal_25, total, count,rate)%>%
  arrange(nh_race, -count)%>%
  group_by(nh_race)%>%
  slice(1:5)

### AIAN ###

df1.2_aian<-df%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(total=n())%>%
  filter(aian_flag==1)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="aian_aoic",
         denom="citation_result")%>%
  select(nh_race,  denom, statute_literal_25, total, count, rate)%>%
  arrange(-count)

### NHPI ###

df1.2_nhpi<-df%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(total=n())%>%
  filter(nhpi_flag==1)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="nhpi_aoic",
         denom="citation_result")%>%
  select(nh_race,  denom, statute_literal_25, total, count, rate)%>%
  arrange(-count)

### SWANA/SA ###

df1.2_sswana<-df%>%
  group_by(statute_literal_25, offense_type_of_charge)%>%
  mutate(total=n())%>%
  filter(sswana_flag==1)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="sswana_aoic",
         denom="citation_result")%>%
  select(nh_race,  denom, statute_literal_25, total, count, rate)%>%
  arrange(-count)%>%
  slice(1:5)

## Combine all tables together ##

df1.2<-rbind(df1.2, df1.2_aian, df1.2_nhpi, df1.2_sswana)%>%
  rename("race"="nh_race")

## FINAL COMBINE all tables with both denominator options for sub-analysis 1:

df1<-rbind(df1.1, df1.2)


# Sub-Analysis 2---------------------
# % of traffic stops resulting in 'driving without license' citation by race

#### Denom 1: Per racial group. i.e.)  % of Latinx stopped resulting in driving without license / all Latinx traffic stops resulting in a citation ####

##### NH #####

df2.1<-df%>%
  group_by(nh_race)%>%
  mutate(total=n())%>%
  filter(offense_statute=="12500(A)" |
           offense_statute=="12500(B)" |
           offense_statute=="12500(C)" |
           offense_statute=="12500(D)"|
           offense_statute=="12951(A)")%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  select(nh_race, statute_literal_25, total, count,rate)%>%
  arrange(-rate)

### AIAN ###

#  For AIAN there are no traffic stops resulting in a driving without license citation so coding as 0
df2.1_aian<-df%>%
  filter(aian_flag==1)%>%
  mutate(total=n())%>%
  filter(offense_statute=="12500(A)" |
           offense_statute=="12500(B)" |
           offense_statute=="12500(C)" |
           offense_statute=="12500(D)"|
           offense_statute=="12951(A)")%>%
  mutate(count=0,
         rate=0)%>%
  slice(1)%>%
  mutate(nh_race="aian_aoic",
         denom="citation_race")%>%
  select(nh_race, denom, statute_literal_25, total, count,rate)%>%
  arrange(-rate)

##### NHPI ##### ----there are none

df2.1_nhpi<-df%>%
  filter(nhpi_flag==1)%>%
  mutate(total=n())%>%
  filter(offense_statute=="12500(A)" |
           offense_statute=="12500(B)" |
           offense_statute=="12500(C)" |
           offense_statute=="12500(D)"|
           offense_statute=="12951(A)")%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(nh_race="nhpi_aoic",
         denom="citation_race")%>%
  select(nh_race, denom, statute_literal_25, total, count,rate)%>%
  arrange(-rate)

##### SWANA/SA #####

df2.1_sswana<-df%>%
  filter(sswana_flag==1)%>%
  mutate(total=n())%>%
  filter(offense_statute=="12500(A)" |
           offense_statute=="12500(B)" |
           offense_statute=="12500(C)" |
           offense_statute=="12500(D)"|
           offense_statute=="12951(A)")%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  mutate(nh_race="nhpi_aoic",
         denom="citation_race")%>%
  select(nh_race, denom, statute_literal_25, total, count,rate)%>%
  arrange(-rate)

#### Combine all tables together ####

df2.1<-rbind(df2.1, df2.1_aian, df2.1_nhpi, df2.1_sswana)%>%
  rename("race"="nh_race")

#### Denom 2: By stop result i.e.) %  of Latinx stopped resulting in no license / all people stopped resulting in no license ####

###### NH #####

df2.2<-df%>%
  filter(offense_statute=="12500(A)" |
           offense_statute=="12500(B)" |
           offense_statute=="12500(C)" |
           offense_statute=="12500(D)"|
           offense_statute=="12951(A)")%>%
  mutate(total=n())%>%
  group_by(nh_race)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(denom="citation_reason")%>%
  select(nh_race, denom, statute_literal_25, total, count, rate)%>%
  arrange(nh_race, -count)%>%
  group_by(nh_race)


###### AIAN ##### 

# ----there are none

df2.2_aian<-df%>%
  filter(offense_statute=="12500(A)" |
           offense_statute=="12500(B)" |
           offense_statute=="12500(C)" |
           offense_statute=="12500(D)"|
           offense_statute=="12951(A)")%>%
  mutate(total=n())%>%
  filter(aian_flag==1)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="aian_aoic",
         denom="citation_reason")%>%
  select(nh_race, denom, statute_literal_25, total, count, rate)%>%
  arrange(nh_race, -count)

###### NHPI ##### -

 #---there are none

df2.2_nhpi<-df%>%
  filter(offense_statute=="12500(A)" |
           offense_statute=="12500(B)" |
           offense_statute=="12500(C)" |
           offense_statute=="12500(D)"|
           offense_statute=="12951(A)")%>%
  mutate(total=n())%>%
  filter(nhpi_flag==1)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="nhpi_aoic",
         denom="citation_reason")%>%
  select(nh_race, denom, statute_literal_25, total, count, rate)%>%
  arrange(nh_race, -count)


###### SSWANA #####

df2.2_sswana<-df%>%
  filter(offense_statute=="12500(A)" |
           offense_statute=="12500(B)" |
           offense_statute=="12500(C)" |
           offense_statute=="12500(D)"|
           offense_statute=="12951(A)")%>%
  mutate(total=n())%>%
  filter(sswana_flag==1)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  ungroup()%>%
  mutate(nh_race="sswana_aoic",
         denom="citation_reason")%>%
  select(nh_race, denom, statute_literal_25, total, count, rate)%>%
  arrange(nh_race, -count)


#### Combine all race tables ####

df2.2<-rbind(df2.2, df2.2_aian, df2.2_nhpi, df2.2_sswana)%>%
  rename("race"="nh_race")


# Final join of tables for both denominators--------------------------------------

df2<-rbind(df2.1, df2.2)


# Push all tables to postgres------------------------------

#### Sub-Analysis 1 ####

# set column types
charvect = rep('varchar', ncol(df1)) 
charvect <- replace(charvect, c(4,5,6), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df1)

dbWriteTable(con,  "report_traffic_result_citation_race", df1,
             overwrite = TRUE, row.names = FALSE,
             field.types = charvect)


# # write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE report_traffic_result_citation_race  IS 'Analyzing officer-initiated traffic stops by citation results for each racial group. Note
this analysis calculates rates with two denominator options: 1) out of all citations made within each racial group and 2) out of all stops made within each citation result. Which denominator is
used for which rate calculation is denoted by the denom column in the tbale.
R script used to analyze and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Analysis\\report_traffic_reason_race.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_traffic_reason_race.docx';

COMMENT ON COLUMN report_traffic_result_citation_race.race IS 'Perceived race';
COMMENT ON COLUMN report_traffic_result_citation_race.denom IS 'Which denominator was used in rate calc. if denom==citation_race that means the denominator is all citations within the racial group. If denom == citation_result that means out of all people stopped with that citation result';
COMMENT ON COLUMN report_traffic_result_citation_race.statute_literal_25 IS 'Text description of the traffic stop citation result';
COMMENT ON COLUMN report_traffic_result_citation_race.total IS 'Denominator for the rate calc. See denom column to see which denominator value is used';
COMMENT ON COLUMN report_traffic_result_citation_race.count IS 'Count of officer-initiated traffic stops that resulted in each citation by perceived race (numerator for rate calc)';
COMMENT ON COLUMN report_traffic_result_citation_race.rate IS 'Rate of officer-initiated traffic stops by citation stop result for each perceived race';
")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)



#### Sub-Analysis 2 ####

# set column types
charvect = rep('varchar', ncol(df2)) 
charvect <- replace(charvect, c(4,5,6), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df2)

dbWriteTable(con,  "report_traffic_reason_race", df2,
             overwrite = TRUE, row.names = FALSE,
             field.types = charvect)


# # write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE report_traffic_reason_race  IS 'Analyzing officer-initiated traffic stops by traffic stop reason for each racial group.
This analysis contains rates with two types of denominators: 1) out of all traffic stops within each racial group and 2) out of all stops that resulted in that traffic stop reason.
The denominator used is denoted in a denom column. 
R script used to analyze and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Analysis\\report_traffic_reason_race.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_traffic_reason_race.docx';

COMMENT ON COLUMN report_traffic_reason_race.race IS 'Perceived race';
COMMENT ON COLUMN report_traffic_reason_race.denom IS 'Which denominator is used for the Total and Rate column. 
If denom==traffic_stop_race then the total column represents total number of traffic stops within each racial group.
If denom==traffic_reason then the total represents the total number of traffic stops with that stop reason.';

COMMENT ON COLUMN report_traffic_reason_race.statute_literal_25 IS 'Text description of the traffic stop reason corresponding with the traffic stop reason code';
COMMENT ON COLUMN report_traffic_reason_race.total IS 'Denominator in rate calc which is noted in the denom column as there are two options. 
If denom==traffic_stop_race then the total column represents total number of traffic stops within each racial group. 
If denom==traffic_reason then the total represents the total number of traffic stops with that stop reason.';
COMMENT ON COLUMN report_traffic_reason_race.count IS 'Count of officer-initiated traffic stop reasons within each race (numerator for rate calc)';
COMMENT ON COLUMN report_traffic_reason_race.rate IS 'Rate of officer-initiated traffic stop reasons by race using both denominators for the rate calc';
")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)

