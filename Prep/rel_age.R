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

# select only age group columns

age<-person%>%
  select(stop_id, person_number, age, age_group)

# recode age brackets

age<-age%>%
  mutate(age_group_re=ifelse(age<=19, "19 and under",
                             ifelse(age>=20 & age <=24, "20-24",
                                    ifelse(age>=25 & age <= 34, "25-34",
                                           ifelse(age>=35 & age <=44, "35-44",
                                                  ifelse(age>=45 & age <=54, "45-54",
                                                         ifelse(age>=55 & age <=59,"55-59",
                                                                ifelse(age>=60 & age <=64,"60-64",
                                                                "65 and older"))))))))%>%
  select(-age_group)

# set order of the age_group_re column

x <- c("19 and under", "20-24", "25-34","35-44","45-54","55-59","60-64","65 and older","Total")
  

age<-age%>%
  mutate(age_group_re =  factor(age_group_re, levels = x))%>%
  arrange(age_group_re)

#### Finalize and push to postgres ####

# set column types
charvect = rep('varchar', ncol(age)) #create vector that is "numeric" for the number of columns in df

# add df colnames to the character vector

names(charvect) <- colnames(age)

##### Export Data #####

dbWriteTable(con,  "rel_age", age, 
             overwrite = TRUE, row.names = FALSE,
             field.types = charvect)


# write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE rel_age  IS 'Recoded age brackets from 
2022 SD RIPA data.
R script used to recode and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Prep\\rel_age.R
Create age brackets based off age column that align with census age brackets';

COMMENT ON COLUMN rel_age.stop_id IS 'Stop ID';
COMMENT ON COLUMN rel_age.person_number IS 'Person ID';
COMMENT ON COLUMN rel_age.age IS 'Perceived age of person stopped as indicated by officer';
COMMENT ON COLUMN rel_age.age_group_re IS 'Age bracket';")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)

# add indices

dbSendQuery(con, paste0("create index rel_age_stop_id on data.rel_age (stop_id);
create index rel_age_person_number on data.rel_age (person_number);"))


