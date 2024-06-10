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
  select(stop_id, person_number, rae_full, rae_hispanic_latino, rae_middle_eastern_south_asian, rae_multiracial, rae_native_american,
         rae_pacific_islander, rae_white)

#### Explore Multiple Perceived Races ####

table(race$rae_full) # 1403 people are coded as having 7 perceived races, 18 people are coded as having 8 perceived races.

# But when you actually look at races indicated even if rae_full > 1 the person will have have 1 race marked.

# Can calculate our own total races counted by summing the race 1/0 columns

race<-race%>%
  mutate(race_re=ifelse(rae_full=="7", "NULL",
                        ifelse(rae_full == "8", "NULL", "race")))

#### RECODE where number of races = 1 ####


race_1<-race%>%
  filter(n==1)%>%
  mutate(nh_race=ifelse(race %in% 'Asian', 'nh_asian',
                        ifelse(race %in% 'Black/African American', 'nh_black',
                               ifelse(race %in% 'Middle Eastern or South Asian', 'nh_sswana',
                                      ifelse(race %in% 'Native American', 'nh_aian',
                                             ifelse(race %in% 'Pacific Islander', 'nh_nhpi',
                                                    ifelse(race %in% 'White', 'nh_white', 'latinx')))))))%>%
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

#### RECODE where number of races >=2####

# first lets flatten the data so each person shows up as 1 row

race_2<-race%>%
  filter(n>=2)%>%
  group_by(stop_id, person_id)%>%
  summarise(n, race = paste(race, collapse = ","), .groups='keep')%>%
  slice(1)


# If a person is perceived multiple races and one of them is latinx, recode that person's race as 'latinx'
# if there is NO latinx among the perceived races, recode as nh_twoormor

race_2<-race_2%>%
  group_by(stop_id,person_id)%>%
  mutate(nh_race = ifelse(str_detect(race, 'Hispanic/Latino/a'), 'latinx',race))

# Add nhpi and sswana flags. If any of the multiple races perceived is sswana/nhpi, add a flag. Even if that includes a latinx category with it. 

race_2<-race_2%>%
  mutate(sswana_flag=ifelse(str_detect(race, "Middle Eastern or South Asian"),1,0),
         sswana_label=ifelse(sswana_flag %in% 1, 'sswana', 
                             "not sswana"),
         
         nhpi_flag=ifelse(str_detect(race, "Pacific Islander"),1,0),
         nhpi_label=ifelse(nhpi_flag %in% 1, 'nhpi', 
                           "not nhpi"),
         
         aian_flag=ifelse(str_detect(race, "Native American"),1,0),
         aian_label=ifelse(aian_flag %in% 1, 'aian', 
                           "not aian"))

####RECODE where n = 7 as NULL, and sswana_flag/aian/nhpi = NULL when n == 7####

race_2<-race_2%>%
  ungroup()%>%
  mutate(nh_race=ifelse(n %in% 7, "NULL", nh_race),
         sswana_flag=ifelse(n %in% 7, "NULL", sswana_flag),
         sswana_label=ifelse(n %in% 7, 'NULL', 
                             sswana_label),
         nhpi_flag=ifelse(n %in% 7, "NULL", nhpi_flag),
         nhpi_label=ifelse(n %in% 7,  'NULL', 
                           nhpi_label),
         aian_flag=ifelse(n %in% 7, "NULL", aian_flag),
         aian_label=ifelse(n %in% 7,  'NULL', 
                           aian_label))%>%
  mutate(across(5:11, na_if, "NULL"))

#### EXPLORE when n >= 5 and < 7 ####

sum(race_1$aian_flag) #1378 single race aian 

aian_flag<-race_2%>%
  filter(aian_flag==1)%>% 
  group_by(n)%>%
  summarise(multi_count=n())%>%
  mutate(single_count=1378)%>%
  mutate(group='aian_flag')


sum(race_1$nhpi_flag)

nhpi_flag<-race_2%>%
  filter(nhpi_flag==1)%>% 
  group_by(n)%>%
  summarise(multi_count=n())%>%
  mutate(single_count=5307)%>%
  mutate(group='nhpi_flag')

sum(race_1$sswana_flag)

sswana_flag<-race_2%>%
  filter(sswana_flag==1)%>% 
  group_by(n)%>%
  summarise(multi_count=n())%>%
  mutate(single_count=19005)%>%
  mutate(group='sswana_flag')

# CONCLUSION: I think we should recode n = 3|4|5 as nh_twoormore, and KEEP the nhpi, aian and sswana flags. 
# For n = 6 also recode as NULL and EXCLUDE the sswana/aian/nhpi flags

## REVISED 8/7/23: Change the threshold to be perceived num races >=6

#### FINALIZE RECODING multirace table ####

race_2<-race_2%>%
  mutate(nh_race=ifelse(n %in% 6, "NULL", nh_race),
         
         sswana_flag=ifelse(n %in% 6, "NULL", sswana_flag),
         sswana_label=ifelse(n %in% 6, "NULL", sswana_label),
         
         nhpi_flag=ifelse(n %in% 6, "NULL", nhpi_flag),
         nhpi_label=ifelse(n %in% 6, "NULL", nhpi_label),
         
         aian_flag=ifelse(n %in% 6, "NULL", aian_flag),
         aian_label=ifelse(n %in% 6, "NULL", aian_label),
         
         nh_race=ifelse(n %in% 2 & nh_race != 'latinx', "nh_twoormor", nh_race),
         nh_race=ifelse(n %in% 3 & nh_race != 'latinx', "nh_twoormor", nh_race),
         nh_race=ifelse(n %in% 4 & nh_race != 'latinx', "nh_twoormor", nh_race),
         nh_race=ifelse(n %in% 5 & nh_race != 'latinx', "nh_twoormor", nh_race)
         
  )%>%
  mutate(across(5:11, na_if, "NULL"))%>%
  mutate(sswana_flag=as.numeric(sswana_flag),
         nhpi_flag=as.numeric(nhpi_flag),
         aian_flag=as.numeric(aian_flag)
  )

#### COMBINE both race tables and push to postgres ####

race_final<-rbind(race_1, race_2)%>%
  rename('num_perceived_race'='n')

# set column types
charvect = rep('varchar', ncol(race_final)) #create vector that is "numeric" for the number of columns in df
charvect <- replace(charvect, c(3,6,8,10), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(race_final)

##### Export Data #####

dbWriteTable(con,  "rel_races_recode", race_final, 
             overwrite = TRUE, row.names = FALSE,
             field.types = charvect)


# write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE rel_races_recode  IS 'Recoded racial categories from 
2023 SD RIPA data.
R script used to recode and import table: W:/Project/RJS/Pillars/R/Data and Geo Prep/rel_races_recode.R
NHPI, sswana and AIAN are alone or in combination with Latinx or another race. all nh_race fields are exclusive of Latinx other than the Latinx category.
Persons (rows) with 6 or more races are recoded as NULL across nh_race and all NHPI/sswana/AIAN flags and labels.';

COMMENT ON COLUMN rel_races_recode.stop_id IS 'Stop ID';
COMMENT ON COLUMN rel_races_recode.person_id IS 'Person ID';
COMMENT ON COLUMN rel_races_recode.race IS 'Perceived race of person stopped as indicated by officer';
COMMENT ON COLUMN rel_races_recode.num_perceived_race IS 'Number of perceived race(s) of person stopped as indicated by officer';
COMMENT ON COLUMN rel_races_recode.nh_race IS 'Recoded non-Hispanic race or Latinx value for stopped person';
COMMENT ON COLUMN rel_races_recode.sswana_flag IS 'South Asian, Southwest Asian, or North African Alone or in Combination (yes =1 or no=0)';
COMMENT ON COLUMN rel_races_recode.sswana_label IS 'South Asian, Southwest Asian, or North African Alone or in Combination label originally in data as Middle Eastern or South Asian';
COMMENT ON COLUMN rel_races_recode.aian_flag IS 'American Indian/Alaskan Native Alone or in Combination (yes =1 or no=0)';
COMMENT ON COLUMN rel_races_recode.aian_label IS 'American Indian/Alaskan Native Alone or in Combination label originally in data as Native American';
COMMENT ON COLUMN rel_races_recode.nhpi_flag IS 'Native Hawaiian/Pacific Islander Alone or in Combination  (yes =1 or no=0)';
COMMENT ON COLUMN rel_races_recode.nhpi_label IS 'Native Hawaiian/Pacific Islander Alone or in Combination label originally in data as Pacific Islander';")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)

# add indices

dbSendQuery(con, paste0("create index rel_races_recode_stop_id on data.rel_races_recode (stop_id);
create index rel_races_recode_person_id on data.rel_races_recode (person_id);
                        create index rel_races_recode_nh_race on data.rel_races_recode (nh_race);"))


