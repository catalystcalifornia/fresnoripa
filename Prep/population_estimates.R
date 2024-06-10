#### Calculate population estimates by race for Fresno County #### 

#### Set up workspace ####
library(RPostgreSQL)
library(knitr)
library(dplyr)
library(sf)
library(data.table)
library(tidyr)
library(tidyverse)
library(tidycensus)
library(Hmisc)
library(glue)
library(labelled)
library(readxl)
library(srvyr)

# Connect to postgres

source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("eci_fresno_ripa")
con_shared<-connect_to_db("rda_shared_data")

# Read in tables from postgres

df<- dbGetQuery(con_shared, "SELECT * FROM demographics.acs_5yr_multigeo_2021_race_long")

# calculate estimates for Fresno city ####

city<-df%>%
  filter(name=='Fresno city, California')

## add in percentages out of total
city<-city%>%
  left_join(city%>%
              select('geoid','name','variable','count')%>%
              rename('total'='variable')%>%rename('population'='count')%>%
              filter(total=='total'),
            by=c('geoid','name'))%>%
  mutate(rate=count/population)%>%
  select('geoid','name','variable','count','moe','rate')


city<-city%>%rename('race'='variable')

# Finalize and push table to postgres --------------------------------
# set field types for postgresql db
charvect = rep('varchar', ncol(city)) #create vector that is "numeric" for the number of columns in df

charvect <- replace(charvect, c(4,5,6), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(city)

dbWriteTable(con,  "population_race_fresno_city", city, 
             overwrite = TRUE, row.names = FALSE,field.types = charvect
)

# add indices
dbSendQuery(con, paste0("create index population_race_fresno_city on population_race_fresno_city (race);"))

# add table comment
dbSendQuery(con, paste0("comment on table population_race_fresno_city is 
                        'Population estimates and percentages for Fresno city by race for non-Hispanic categories, Latinx, and all-AIAN, all-NHPI, and all-SSWANA(coded as MESA) based on DP05 and B04006(ancestry) for mena and B02018 for south asian. Prefix nh indicates that it is a non-Hispanic/Latinx category.
                         See W:\\Project\\ECI\\Fresno RIPA\\R\\Data and Geo Prep\\population_estimates.R for more info';"))

dbSendQuery(con, paste0("COMMENT ON COLUMN 
                         population_race_fresno_city.race
                         IS 
                         'Racial group or total for which count, moe, or rate is for nh prefix indicates Latinx exclusive. aian, nhpi, and mesa(SSWANA) include alone or in combination with another race including Latinx. SSWANA is based on ancestry data for mena and asian alone or in combo with south asian';
                         COMMENT ON COLUMN 
                         population_race_fresno_city.count
                         IS 
                         'Estimated count of racial group or total based on city level ACS data';
                         COMMENT ON COLUMN 
                         population_race_fresno_city.rate
                         IS 
                         'Estimated percent of racial group (in decimal format) out of total based on city level ACS data';"))



