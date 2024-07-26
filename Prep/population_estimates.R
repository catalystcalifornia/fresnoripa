#### Calculate population estimates by race for Fresno City #### 

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

# Read in tables from postgres ----

df<- dbGetQuery(con_shared, "SELECT * FROM demographics.acs_5yr_multigeo_2022_race_long")

# South asian estimates ----
# calculate south asian pop to subtract from nh_asian pop 
# getting data and vars from census api
v22 <- load_variables(2022, "acs5", cache = TRUE)%>%
  filter(grepl("B02015",name))

asian <- get_acs(geography ='place', 
                         table = c("B02015"), #subject table code
                    state="CA",
                         year = 2022,
                         survey = 'acs5')

# create a list of South Asian vars
# reference from race recoding script - soasian_list<-list("Asian Indian", "Bangladeshi", "Bhutanese","Maldivian","Nepalese","Pakistani","Sikh","Sindhi","Sri Lankan","Other South Asian")
soasian_variables<-v22%>%filter(grepl("South Asian",label))

# Convert to a list
soasian_columns<-as.list(soasian_variables$name)

# Narrow down variables from acs table based on the list of South Asian variables
soasian<-asian[grep(paste(soasian_columns,collapse="|"), asian$variable,ignore.case=TRUE),]
colnames(soasian)<-tolower(names(soasian))

## Calculate South Asian Count in fresno
soasian_fres<-soasian%>%
  filter(name=='Fresno city, California')%>%
  group_by(name,geoid)%>%
  summarise(soasian_count=sum(estimate),
            soasian_moe=moe_sum(moe,estimate))


# Calculate race estimates for Fresno city ----

##### standard race groups  ------
city<-df%>%
  filter(name=='Fresno city, California')

##### asian minus south asian  ------
# reference for calcs using tidycensus https://psrc.github.io/psrccensus/articles/calculate-reliability-moe-transformed-acs.html
# get nh asian counts from dp05 table and join to soasian counts
asian_wo_soasian<-city%>%filter(variable=='nh_asian')%>%left_join(soasian_fres)

# calculate new count for nh asian without south asian
asian_wo_soasian<-asian_wo_soasian%>%
  mutate(new_count=(count-soasian_count))

# calculate new moe for nh asian without south asian
asian_wo_soasian<-asian_wo_soasian%>%
  rowwise()%>%
  mutate(new_moe=moe_sum(estimate=c(count,soasian_count), 
                               moe=c(moe,soasian_moe)))

# clean up
asian_wo_soasian<-asian_wo_soasian%>%
  mutate(variable="nh_asian_wo_sa")%>%
  select(geolevel,name,geoid,variable,new_count,new_moe)%>%
  rename(count=new_count,
         moe=new_moe)

##### add in percentages out of total and cvs -----
df_final<-rbind(city,asian_wo_soasian)%>%
  left_join(city%>%
              select('geoid','name','variable','count')%>%
              rename('total'='variable')%>%rename('population'='count')%>%
              filter(total=='total'),
            by=c('geoid','name'))%>%
  mutate(rate=count/population,
         cv=moe/1.645/count*100)%>%
  select('geoid','name','variable','count','moe','rate','cv')

# Finalize and push table to postgres --------------------------------
df_final<-df_final%>%
  rename('race'='variable')%>%
  filter(race!='swana') # don't need swana for ripa estimates just sswana

# set field types for postgresql db
charvect = rep('varchar', ncol(df_final)) #create vector that is "numeric" for the number of columns in df

charvect <- replace(charvect, c(4,5,6,7), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df_final)

dbWriteTable(con,  "population_race_fresno_city", df_final, 
             overwrite = FALSE, row.names = FALSE,field.types = charvect
)

dbSendQuery(con, paste0("comment on table population_race_fresno_city is 
                        'Population estimates and percentages for Fresno city by race for non-Hispanic categories, Latinx, and all-AIAN, all-NHPI, and all-SSWANA(coded as swana) based on DP05 and B04006(ancestry) for sswana and B02018 for south asian. 
                        Prefix nh indicates that it is a non-Hispanic/Latinx category.
                        Added nh_asian_wo_sa as an additional category for testing which removes south asian alone individuals from nh_asian using B02015 estimates
                         See W:\\Project\\ECI\\Fresno RIPA\\R\\Data and Geo Prep\\population_estimates.R and
                        W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_population_estimates.docx for more info';"))

dbSendQuery(con, paste0("COMMENT ON COLUMN 
                         population_race_fresno_city.race
                         IS 
                         'Racial group or total for which count, moe, or rate is for nh prefix indicates Latinx exclusive. aian, nhpi, and sswana include alone or in combination with another race including Latinx. SSWANA is based on ancestry data for mena and south asian alone or in combo. Nh_asian_wo_sa excludes south asian alone individuals from nh_asian estimates';
                         COMMENT ON COLUMN 
                         population_race_fresno_city.count
                         IS 
                         'Estimated count of racial group or total based on city level ACS data';
                         COMMENT ON COLUMN 
                         population_race_fresno_city.rate
                         IS 
                         'Estimated percent of racial group (in decimal format) out of total based on city level ACS data';"))



