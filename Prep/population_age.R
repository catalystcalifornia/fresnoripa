#### Calculate population estimates by age for Fresno County #### 

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

## Update Table S0101 in postgres to use for this analysis: 
#####DO NOT RE-RUN just QA the table in postgres: rda_shared_data.acs_5yr_s0101_multigeo_2022 ------------------------

# Source ACS update function because we need to add the most recent S0101 table to demographics.rda_shared_data
source("W:\\RDA Team\\R\\Github\\RDA Functions\\acs_rda_shared_tables_adds0101.R") ##JZ change filepath when HK updates main function script

# Script file path, for postgres table comment
filepath <- "W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Prep\\population_age.R"

# Define arguments for ACS table update fx
yr <- 2022 # update for the ACS data/ZCTA vintage needed

### If you add a new table, you must also update table_vars below
rc_acs_indicators <- list(
  "S0101"   # sex and age

) 

## Run fx to get updates ACS table(s)
update_acs(yr=yr, acs_tables=rc_acs_indicators,filepath)

# Analyze age population estimates using updated S0101 table-------------------------

# Read in table from postgres and select columns and geography of interest. Rename columns for clarity

df<- dbGetQuery(con_shared, "SELECT geoid, name, 
        s0101_c01_001e,  s0101_c01_001m,
         s0101_c01_022e, s0101_c01_022m,
         s0101_c01_023e, s0101_c01_023m,
         s0101_c01_007e, s0101_c01_007m,
         s0101_c01_008e ,s0101_c01_008m,
         s0101_c01_009e, s0101_c01_009m,
         s0101_c01_010e, s0101_c01_010m,
         s0101_c01_011e, s0101_c01_011m,
         s0101_c01_012e, s0101_c01_012m,
         s0101_c01_013e, s0101_c01_013m,
         s0101_c01_014e, s0101_c01_014m, 
         s0101_c01_030e, s0101_c01_030m
                FROM demographics.acs_5yr_s0101_multigeo_2022")

# Filter for Fresno city

df<-df%>%
  filter(geoid == '0627000')


# Rename columns, then create additional age bracket groupings via aggregating for age 25-34, 35-44, and 45-54 

 df<-df%>%
   rename("total"="s0101_c01_001e",
          "total_moe"="s0101_c01_001m",
          "age0_17"="s0101_c01_022e",
          "age0_17_moe"="s0101_c01_022m",
          "age18_24"="s0101_c01_023e",
          "age18_24_moe"="s0101_c01_023m",
          "age25_29"= "s0101_c01_007e",
          "age25_29_moe"="s0101_c01_007m",
          "age30_34" ="s0101_c01_008e",
          "age30_34_moe"="s0101_c01_008m",
          "age35_39"="s0101_c01_009e",
          "age35_39_moe"="s0101_c01_009m",
          "age40_44"="s0101_c01_010e",
          "age40_44_moe"="s0101_c01_010m",
          "age45_49"= "s0101_c01_011e",
          "age45_49_moe"="s0101_c01_011m",
          "age50_54"= "s0101_c01_012e",
          "age50_54_moe"="s0101_c01_012m",
          "age55_59"= "s0101_c01_013e",
          "age55_59_moe"="s0101_c01_013m",
          "age60_64"= "s0101_c01_014e",
          "age60_64_moe"= "s0101_c01_014m",
          "age65over"= "s0101_c01_030e",
          "age65over_moe"="s0101_c01_030m"
           )%>%
   mutate(age25_34=sum(age25_29, age30_34),
          age25_34_moe=moe_sum(estimate=c(age25_29, age30_34),
                           moe=c(age25_29_moe, age30_34_moe)),
age35_44=sum(age35_39,age40_44),
age35_44_moe=moe_sum(estimate=c(age35_39,age40_44),
                      moe=c(age35_39_moe, age40_44_moe)),
age45_54=sum(age45_49,age50_54),
age45_54_moe=moe_sum(estimate=c(age45_49,age50_54),
                     moe=c(age45_49_moe, age50_54_moe))
)%>%
   select(name, geoid, total, total_moe, age0_17, age0_17_moe, age18_24, age18_24_moe,
          age25_34, age25_34_moe, age35_44, age35_44_moe, age45_54, age45_54_moe, 
          age55_59, age55_59_moe, age60_64, age60_64_moe, age65over,   age65over_moe)

## pivot table to long format

df_e<-df %>%
  pivot_longer(3:20, names_to = "variable", values_to = "age")%>%
  filter(!grepl("moe", variable))

df_m<-df %>%
  pivot_longer(3:20, names_to = "variable", values_to = "moe")%>%
  filter(grepl("moe", variable))

df_long<-cbind(df_e, df_m)

df_long<-df_long%>%
  select(1:4, 8)%>%
  rename("estimate"="age")

# recode age variables
df_long<-df_long%>%
  mutate(age_re=ifelse(variable %in% "total", "Total",
                       ifelse(variable %in% "age0_17", "17 and under",
                               ifelse(variable %in% "age18_24", "18-24",
                                                   ifelse(variable %in% "age25_34", "25-34",
                                                          ifelse(variable %in% "age35_44", "35-44",
                                                                 ifelse(variable %in% "age45_54", "45-54",
                                                                        ifelse(variable %in% "age55_59", "55-59",
                                                                               ifelse(variable %in% "age60_64", "60-64",
                                                                                      ifelse(variable %in% "age65over", "65 and older",
                                                                                                    "NULL"
                                                                                                    ))))))))))%>%
  filter(age_re != "NULL")
                                                                                                           
# set order of age_re column manually

x <- c("17 and under", "18-24", "25-34","35-44","45-54","55-59","60-64","65 and older","Total")

df_long<-df_long%>%
   mutate(age_re =  factor(age_re, levels = x)) %>%
   arrange(age_re)%>%
   select(-c(variable, geoid))%>%
   select(name, age_re, estimate, moe)

# Calculate rates for each age bracket -------------------------

df_long<-df_long%>%
  mutate(rate=estimate/541528*100)
         
# Finalize and push table to postgres --------------------------------
# set field types for postgresql db
charvect = rep('varchar', ncol(df_long)) #create vector that is "numeric" for the number of columns in df

charvect <- replace(charvect, c(3,4,5), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df_long)

# dbWriteTable(con,  "population_age_fresno_city", df_long, 
#              overwrite = TRUE, row.names = FALSE, field.types = charvect
# )


# add table comment
dbSendQuery(con, paste0("comment on table population_age_fresno_city is 
                        'Population estimates and percentages for Fresno city by age-brackets
                         See W:\\Project\\eCI\\Fresno RIPA\\R\\Data and Geo Prep\\population_age.R for more info
                        QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_population_age_fresno_city.docx';"))

# dbSendQuery(con, paste0("COMMENT ON COLUMN 
#                          population_age_fresno_city.name
#                          IS 
#                          'name of city';
#                          COMMENT ON COLUMN 
#                          population_age_fresno_city.age_re
#                          IS 'Recoded age brackets';
#                          COMMENT ON COLUMN 
#                          population_age_fresno_city.estimate
#                          IS 
#                          'estimated count of age bracket or total based on city level ACS data';
#                          COMMENT ON COLUMN 
#                          population_age_fresno_city.moe
#                          IS 
#                          'Margin of error for estimated count of age bracket or total based on city level ACS data';
#                          COMMENT ON COLUMN 
#                          population_age_fresno_city.rate
#                          IS 
#                          'estimated percent of age bracket (in decimal format) out of total based on city level ACS data';"))
# 


