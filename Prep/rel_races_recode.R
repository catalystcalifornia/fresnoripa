###Data Prep: Recode race/ethnicity categories from RIPA data t conform with RDA race category standards.
###Create AIAN/NHPI/SSWANA flag that defines AIAN/NHPI/SSWANA as alone or in combination with any other race


#### Set up ####

library(RPostgreSQL)
library(dplyr)
library(tidyr)
library(stringr)
# install.packages("chron")
library(chron)

#connect to postgres

source("W:\\RDA Team\\R\\credentials_source.R")

con <- connect_to_db("eci_fresno_ripa")

# pull in persons table

person<-dbGetQuery(con, "SELECT * FROM rel_persons")

# select only racial group columns

race<-person%>%
  select(stop_id, person_number, rae_full, rae_asian, rae_black_african_american, rae_hispanic_latino, rae_middle_eastern_south_asian, rae_multiracial, rae_native_american,
         rae_pacific_islander, rae_white)

#### Explore Multiple Perceived Races ####

#  Going to calculate our own total races counted by summing the race 1/0 columns

race<-race%>%
  mutate(race_count_re=rowSums(.[4:11]))

table(race$race_count_re) # Max count is 3 perceived races

# Exploring the records where multiracial=1

multi<-race%>%filter(rae_multiracial==1) # 13/18 of the multiracial records include latinx. Thinking we should recode those as latinx, and the remaining 15 records as multiracial


#### RECODE racial groups ####

race<-race%>%
  mutate(nh_race=ifelse( rae_asian==1 & rae_hispanic_latino==0, 'nh_asian',
                        ifelse(rae_black_african_american	==1 & rae_hispanic_latino==0, 'nh_black',
                               ifelse(rae_middle_eastern_south_asian==1 & rae_hispanic_latino==0,  'nh_sswana',
                                      ifelse(rae_native_american==1 & rae_hispanic_latino==0,'nh_aian',
                                             ifelse(rae_pacific_islander==1 & rae_hispanic_latino==0,'nh_nhpi',
                                                    ifelse(rae_white==1 & rae_hispanic_latino==0, 'nh_white', 
                                                           ifelse(rae_multiracial==1 & rae_hispanic_latino==0, 'multiracial', 
                                                           'latinx'))))))))%>%
  mutate(sswana_flag=ifelse(nh_race %in% 'nh_sswana', 1,0),
         sswana_label=ifelse(sswana_flag %in% 1, 'sswana', 
                             "not sswana"),
         
         aian_flag=ifelse(nh_race %in% 'nh_aian', 1,0),
         aian_label=ifelse(aian_flag %in% 1, 'aian', 
                           "not aian"),
         
         nhpi_flag=ifelse(nh_race %in% 'nh_nhpi', 1,0),
         nhpi_label=ifelse(nhpi_flag %in% 1, 'nhpi', 
                           "not nhpi")
         
  )



# Add nhpi and sswana flags for multiracial groups: If any of the multiple races perceived is sswana/nhpi, add a flag. Even if that includes a latinx category with it. 

race<-race%>%
  mutate(sswana_flag=ifelse(rae_multiracial==1 & rae_middle_eastern_south_asian==1, 1, sswana_flag),
         sswana_label=ifelse(sswana_flag %in% 1, 'sswana', 
                             "not sswana"),
         
         nhpi_flag=ifelse(rae_multiracial==1 & rae_pacific_islander==1, 1, nhpi_flag),
         nhpi_label=ifelse(nhpi_flag %in% 1, 'nhpi', 
                           "not nhpi"),
         
         aian_flag=ifelse(rae_multiracial==1 & rae_native_american==1,1,aian_flag),
         aian_label=ifelse(aian_flag %in% 1, 'aian', 
                           "not aian"))

# Select final columns of interest

race<-race%>%
  select(1,2,12:19)

#### Finalize tables and push to postgres ####

# set column types
charvect = rep('varchar', ncol(race)) 
charvect <- replace(charvect, c(3,5,7,9), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(race)

# dbWriteTable(con,  "rel_races_recode", race,
#              overwrite = TRUE, row.names = FALSE,
#              field.types = charvect)


# write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE rel_races_recode  IS 'Recoded racial categories from 
2022 Fresno RIPA data.
R script used to recode and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Prep\\rel_races_recode.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_rel_races_recode.docx
NHPI, sswana and AIAN are alone or in combination with Latinx or another race. all nh_race fields are exclusive of Latinx other than the Latinx category.';

COMMENT ON COLUMN rel_races_recode.stop_id IS 'Stop ID';
COMMENT ON COLUMN rel_races_recode.person_number IS 'Person number';
COMMENT ON COLUMN rel_races_recode.race_count_re IS 'Number of perceived race(s) of person stopped as indicated by officer based on summing total number of race categories an officer indicated for a person';
COMMENT ON COLUMN rel_races_recode.nh_race IS 'Recoded non-Hispanic race or Latinx value for stopped person. Those coded as multiracial had 3 races indicated. If a person had 3 races indicated but one of them was latinx they were coded as latinx';
COMMENT ON COLUMN rel_races_recode.sswana_flag IS 'South Asian, Southwest Asian, or North African Alone or in Combination (yes =1 or no=0)';
COMMENT ON COLUMN rel_races_recode.sswana_label IS 'South Asian, Southwest Asian, or North African Alone or in Combination label originally in data as Middle Eastern or South Asian';
COMMENT ON COLUMN rel_races_recode.aian_flag IS 'American Indian/Alaskan Native Alone or in Combination (yes =1 or no=0)';
COMMENT ON COLUMN rel_races_recode.aian_label IS 'American Indian/Alaskan Native Alone or in Combination label originally in data as Native American';
COMMENT ON COLUMN rel_races_recode.nhpi_flag IS 'Native Hawaiian/Pacific Islander Alone or in Combination  (yes =1 or no=0)';
COMMENT ON COLUMN rel_races_recode.nhpi_label IS 'Native Hawaiian/Pacific Islander Alone or in Combination label originally in data as Pacific Islander';")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)

# add indices

# dbSendQuery(con, paste0("create index rel_races_recode_stop_id on data.rel_races_recode (stop_id);
# create index rel_races_recode_person_number on data.rel_races_recode (person_number);
#                         create index rel_races_recode_nh_race on data.rel_races_recode (nh_race);"))


