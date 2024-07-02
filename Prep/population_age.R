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

# Read in tables from postgres and select columns of interest

df<- dbGetQuery(con_shared, "SeLeCT * FROm demographics.acs_5yr_dp05_multigeo_2022")%>%
  select(geoid, name, 
         dp05_0001e, dp05_0001m,dp05_0005e,dp05_0005m,dp05_0006e,dp05_0006m,dp05_0007e, dp05_0007m,dp05_0008e,
         dp05_0008m,dp05_0019e, dp05_0019m, dp05_0008e, dp05_0008m, dp05_0009e, dp05_0009m, dp05_0010e,
         dp05_0010m, dp05_0011e,dp05_0011m, dp05_0012e,dp05_0012m, dp05_0013e,dp05_0013m,
         dp05_0014e,dp05_0014m,dp05_0015e,dp05_0015m,dp05_0016e,dp05_0016m,dp05_0017e,dp05_0017m)%>%
  filter(name=='Fresno city, California')
  

 df<-df%>%
   mutate(sixtyfiv_e=sum(dp05_0015e, dp05_0016e, dp05_0017e),
sixtyfiv_m=moe_sum(estimate=c(dp05_0015e, dp05_0016e, dp05_0017e),
                           moe=c(dp05_0015m, dp05_0016m, dp05_0017m)),
undr19_e=sum(dp05_0005e,dp05_0006e,dp05_0007e, dp05_0008e),
undr19_m=moe_sum(estimate=c(dp05_0005e,dp05_0006e,dp05_0007e, dp05_0008e),
                        moe=c(dp05_0005m,dp05_0006m,dp05_0007m, dp05_0008m))
          )

## pivot table to long format

df_e<-df %>%
  pivot_longer(3:36, names_to = "variable", values_to = "age")%>%
  filter(grepl("e", variable))

df_m<-df %>%
  pivot_longer(3:36, names_to = "variable", values_to = "moe")%>%
  filter(grepl("m", variable))

df_long<-cbind(df_e, df_m)

df_long<-df_long%>%
  select(1:4, 8)

# recode age variables
df_long<-df_long%>%
  mutate(age_re=ifelse(variable %in% "dp05_0001e", "Total",
                       ifelse(variable %in% "undr19_e", "19 and under",
                              # ifelse(variable %in% "dp05_0019e", "17 and under",
                                     # ifelse(variable %in% "dp05_0008e", "15-19",
                                            ifelse(variable %in% "dp05_0009e", "20-24",
                                                   ifelse(variable %in% "dp05_0010e", "25-34",
                                                          ifelse(variable %in% "dp05_0011e", "35-44",
                                                                 ifelse(variable %in% "dp05_0012e", "45-54",
                                                                        ifelse(variable %in% "dp05_0013e", "55-59",
                                                                               ifelse(variable %in% "dp05_0014e", "60-64",
                                                                                      ifelse(variable %in% "sixtyfiv_e", "65 and older",
                                                                                                    "NULL"
                                                                                                    ))))))))))%>%
  filter(age_re != "NULL")
                                                                                                           
# set order of age_re column manually

x <- c("19 and under", "20-24", "25-34","35-44","45-54","55-59","60-64","65 and older","Total")

df_long<-df_long%>%
   mutate(age_re =  factor(age_re, levels = x)) %>%
   arrange(age_re)%>%
   select(-c(variable, geoid))%>%
   rename("count"="age")%>%
   select(name, age_re, count, moe)

# Calculate rates for each age bracket -------------------------

df_long<-df_long%>%
  mutate(rate=count/541528*100)
         
# Finalize and push table to postgres --------------------------------
# set field types for postgresql db
charvect = rep('varchar', ncol(df_long)) #create vector that is "numeric" for the number of columns in df

charvect <- replace(charvect, c(3,4,5), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df_long)

dbWriteTable(con,  "population_age_fresno_city", df_long, 
             overwrite = TRUE, row.names = FALSE, field.types = charvect
)


# add table comment
dbSendQuery(con, paste0("comment on table population_age_fresno_city is 
                        'Population estimates and percentages for Fresno city by age-brackets
                         See W:\\Project\\eCI\\Fresno RIPA\\R\\Data and Geo Prep\population_age.R for more info';"))

dbSendQuery(con, paste0("COMMENT ON COLUMN 
                         population_age_fresno_city.name
                         IS 
                         'name of city';
                         COMMENT ON COLUMN 
                         population_age_fresno_city.age_re
                         IS 'Recoded age brackets';
                         COMMENT ON COLUMN 
                         population_age_fresno_city.count
                         IS 
                         'estimated count of age bracket or total based on city level ACS data';
                         COMMENT ON COLUMN 
                         population_age_fresno_city.rate
                         IS 
                         'estimated percent of age bracket (in decimal format) out of total based on city level ACS data';"))



