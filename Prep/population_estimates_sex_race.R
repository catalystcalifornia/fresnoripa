#### Calculate population estimates by race and sex for Fresno city PUMAS #### 
# Data source: ACS, 2018-2022, public use microdata sample
# PUMAS in Fresno City: 
## Fresno County (North Central)--Fresno City (North) PUMA; California 0601902
## Fresno County (Central)--Fresno City (East Central) PUMA; California 0601903
## Fresno County (Central)--Fresno City (Southwest) PUMA; California 0601904
## Fresno County (Central)--Fresno City (Southeast) PUMA; California 0601905
# identified through https://data.census.gov/table/ACSDP5Y2022.DP05?q=fresno%20county&g=795XX00US0601902,0601903,0601904,0601905


# install packages if not already installed
list.of.packages <- c("data.table","stringr","dplyr","RPostgreSQL","dbplyr","srvyr",
                      "tidycensus","tidyr","rpostgis", "here", "sf", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

#Load libraries
library(data.table)
library(stringr)
library(dplyr)
library(RPostgreSQL)
library(dbplyr)
library(srvyr)
library(tidycensus)
library(tidyr)
library(rpostgis)
library(here)
library(sf)
library(usethis)

options(scipen = 100) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

# define year of ACS data
curr_yr <- 2022 

# Get PUMA-COUNTY Crosswalks -----------------------------------------------------------
# and rename fields to distinguish vintages
crosswalk10 <- st_read(con, query = "select county_id AS geoid, county_name AS geoname, puma, num_county from crosswalks.puma_county_2021")
crosswalk10 <- crosswalk10 %>% rename(puma10 = puma, geoid10 = geoid, geoname10 = geoname) 

crosswalk20 <- st_read(con, query = "select county_id AS geoid, county_name AS geoname, puma, num_county from crosswalks.puma_county_2022")
crosswalk20 <- crosswalk20 %>% rename(puma20 = puma, geoid20 = geoid, geoname20 = geoname) 

# Get PUMS Data -----------------------------------------------------------
# Data Dictionary: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2022.pdf
# path where my data lives
start_yr <- curr_yr - 4  # autogenerate start yr of 5yr estimates
root <- paste0("W:/Data/Demographics/PUMS/CA_", start_yr, "_", curr_yr, "/")

# Load ONLY the PUMS columns needed
cols <- colnames(fread(paste0(root, "psam_p06.csv"), nrows=0)) # get all PUMS cols 
cols_ <- grep("^PWGTP*", cols, value = TRUE)                                # filter for PUMS weight colnames
ppl <- fread(paste0(root, "psam_p06.csv"), header = TRUE, data.table = FALSE, select = c(cols_, "SEX", "PUMA10", "PUMA20", "ANC1P", "ANC2P", "HISP", "RAC1P", "RAC2P", "RAC3P", "RACAIAN", "RACPI", "RACNH"),
             colClasses = list(character = c("SEX", "PUMA10", "PUMA20", "ANC1P", "ANC2P", "HISP", "RAC1P", "RAC2P", "RAC3P", "RACAIAN", "RACPI", "RACNH")))

# Add state_geoid to ppl, add state_geoid to PUMA id, so it aligns with crosswalks.puma_county_2020
ppl$state_geoid <- "06"
ppl$puma_id10 <- paste0(ppl$state_geoid, ppl$PUMA10)
ppl$puma_id20 <- paste0(ppl$state_geoid, ppl$PUMA20)

# create list of replicate weights
repwlist = rep(paste0("PWGTP", 1:80))

# save copy of original data
orig_data <- ppl

# join county crosswalk using left join function
ppl <- left_join(orig_data, crosswalk10, by=c("puma_id10" = "puma10"))    # specify the field join
ppl <- left_join(ppl, crosswalk20, by=c("puma_id20" = "puma20"))    # specify the field join

# create one field using both crosswalks
ppl <- ppl %>% mutate(geoid = ifelse(is.na(ppl$geoid10) & is.na(ppl$geoid20), NA, 
                                     ifelse(is.na(ppl$geoid10), ppl$geoid20, ppl$geoid10)))


ppl <- ppl %>% mutate(geoname = ifelse(is.na(ppl$geoname10) & is.na(ppl$geoname20), NA, 
                                       ifelse(is.na(ppl$geoname10), ppl$geoname20, ppl$geoname10)))

# Filter for Fresno County and City PUMAS ----------------------------------------------------
# select Fresno County PUMAS
ppl <- ppl %>%
  filter(geoname=='Fresno')

# check PUMA IDs
unique(ppl$geoname)
table(ppl$PUMA10)
table(ppl$PUMA20)

# 0009 is missing PUMA meaning data record is based on the other PUMA year/column
check <- ppl %>%
  filter(PUMA10!=PUMA20) %>%
  select(PUMA10,PUMA20)

# select Fresno City PUMAS based on data.census.gov
ppl <- ppl %>%
  filter(PUMA10 %in% c('01902','01903','01904','01905') | PUMA20 %in% c('01902','01903','01904','01905'))


# Reclassify Race/Ethnicity -----------------------------------------------------------------------------------------
source("W:/RDA Team/R/Github/RDA Functions/main/RDA-Functions/PUMS_Functions_new.R")

# latino includes all races. AIAN is AIAN alone/combo latino/non-latino, NHPI is alone/combo latino/non-latino, SWANA includes all races and latino/non-latino
ppl <- race_reclass(ppl)

# review data 
View(ppl[c("HISP","latino","RAC1P","race","RAC2P","RAC3P","ANC1P","ANC2P", "aian", "pacisl", "swana")])

# review
check <- ppl %>%
  group_by(race,latino) %>%
  summarise(count=n())

# I need latino and race in one column
# modified from https://github.com/catalystcalifornia/boldvision_youththriving/blob/main/Data%20Prep%20and%20Quality/Survey%20Weighting/acs_pums_race_15_24.R
## nh_race groups
ppl$nh_race = as.factor(ifelse(ppl$RAC1P == 1 & ppl$latino =="not latino", "nh_white",
                                ifelse(ppl$RAC1P == 1 & ppl$latino =="latino", "latino",
                                       ifelse(ppl$RAC1P == 2 & ppl$latino =="not latino", "nh_black",
                                              ifelse(ppl$RAC1P == 2 & ppl$latino =="latino", "latino",
                                                     ifelse(ppl$RAC1P == 6 & ppl$latino =="not latino", "nh_asian",
                                                            ifelse(ppl$RAC1P == 6 & ppl$latino =="latino", "latino",
                                                                   ifelse(ppl$RAC1P == 8 & ppl$latino =="not latino", "nh_other", 
                                                                          ifelse(ppl$RAC1P == 8 & ppl$latino =="latino", "latino",
                                                                                 ifelse(ppl$RAC1P == 9 & ppl$latino =="not latino", "nh_twoormor",
                                                                                        ifelse(ppl$RAC1P == 9 & ppl$latino =="latino", "latino",
                                                                                               ifelse(ppl$RAC1P %in% c(3,4,5) & ppl$latino =="latino", "latino",
                                                                                                      ifelse(ppl$RAC1P %in% c(3,4,5) & ppl$latino =="not latino", "nh_aian",
                                                                                                             ifelse(ppl$RAC1P==7 & ppl$latino =="latino", "latino",
                                                                                                                    ifelse(ppl$RAC1P==7 & ppl$latino =="not latino", "nh_pacisl",
                                                                                                                           
                                                                                                                           
                                                                                                                           NA)))))))))))))))
# review
check <- ppl %>%
  group_by(race,latino,nh_race) %>%
  summarise(count=n())
         
# Pop estimates by race and gender -------------------------------------------------------------
# Factor sex as indicator 
ppl$sex_re <- as.factor(ifelse(ppl$SEX == "1", "Male", 
                            ifelse(ppl$SEX=="2", "Female", NA)))

# review
check <- ppl %>%
  group_by(sex_re,SEX) %>%
  summarise(count=n())
# looks good

# prep data and survey design
repwlist = rep(paste0("PWGTP", 1:80)) # create list of replicate weights

weight <- 'PWGTP' 

ppl_srvy<- ppl %>%               
  as_survey_rep(
    variables = c(geoid, nh_race, swana, sex_re),   # dplyr::select grouping variables
    weights = all_of(weight),                       # person weight
    repweights = all_of(repwlist),                  # list of replicate weights
    combined_weights = TRUE,                # tells the function that replicate weights are included in the data
    mse = TRUE,                             # tells the function to calc mse
    type="other",                           # statistical method
    scale=4/80,                             # scaling set by ACS
    rscale=rep(1,80)                        # setting specific to ACS-scaling
  )

# run rates by sex and gender for total population for nh race 
nh_race_df <- ppl_srvy %>%
  group_by(geoid, sex_re, nh_race) %>%   # group by race and sex
  summarise(
    count = survey_total(na.rm=T), # get the (survey weighted) count for the numerators - count of every race and sex combo
    sex_rate = survey_mean()) %>%        # get the (survey weighted) proportion for the numerator - this is out of each sex, what % was of each race
  left_join(ppl_srvy %>%                                        # left join in the denominators
              group_by(geoid) %>%                                     # group by geo
              summarise(pop = survey_total(na.rm=T))) %>%              # get the weighted total for overall Fresno city approximated PUMAs
  mutate(sex_rate=sex_rate*100,
         sex_rate_moe = sex_rate_se*1.645*100,    # calculate the margin of error for the rate based on se
         sex_rate_cv = ((sex_rate_moe/1.645)/sex_rate) * 100, # calculate cv for rate
         count_moe = count_se*1.645, # calculate moe for numerator count based on se
         count_cv = ((count_moe/1.645)/count) * 100, # calculate cv for numerator count
         pop_rate=count/pop*100)  # what percent of total Fresno city population approximated is of each race/sex combo

## clean up columns
final_df<-nh_race_df%>%
  select(geoid,nh_race,sex_re,count,count_moe,count_cv,sex_rate,sex_rate_moe,sex_rate_cv,pop,pop_rate)%>%
  rename(sex=sex_re)

# Push to postgres -----
source("W:\\RDA Team\\R\\credentials_source.R")
source("W:\\RDA Team\\R\\Github\\RDA Functions\\main\\RDA-Functions\\Utility_Functions.R")
# create connection for rda database
con <- connect_to_db("eci_fresno_ripa")

source <-" ACS, PUMS 2018-2022 "
schema <- 'data'
qa_filepath<- " Script population_estimates_sex_race  "
indicator <- " Fresno population by sex and non-hispanic race based on 4 PUMAS in and around Fresno City"
table_name <- "population_race_sex_fresno_city"

# write table
# dbWriteTable(con, c(schema, table_name),final_df,
#              overwrite = FALSE, row.names = FALSE)

# Write table comments
column_names <- colnames(final_df) # Get column names
column_comments <- c(
  'GEOID- Fresno county geoid',
  'non-hispanic race shorthand, includes latino as a row',
  'sex at birth based on census',
  'count in each race and sex combo',
  'MOE for race and sex combo',
  'CV for race and sex combo',
  '% of total sex population that is of each race',
  'moe for sex_rate',
  'cv for sex_rate',
  'total population in 4 fresno city PUMAs--not equivalent to fresno city pop',
  '% of total population in 4 Fresno city PUMAs that are of that race and sex combo')

add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
