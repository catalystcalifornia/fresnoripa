# Script pivots citation codes used against people in Fresno RIPA 2022 data
# Uses prepped and QA-ed tables

# Set up environment ----
library(RPostgreSQL)
library(dplyr)
library(tidyr)

# Connect to RDA databases
source("W:\\RDA Team\\R\\credentials_source.R")

fres <- connect_to_db("eci_fresno_ripa")

# pull in Fresno data from postgres
fresno_ripa<-dbGetQuery(fres, "SELECT * FROM rel_persons")


citations<-fresno_ripa %>% 
  select(stop_id,person_number,ros_citation,ros_citation_cds)%>%
  separate_rows(ros_citation_cds)

# test nothing dropped
length(unique(citations$stop_id))
length(unique(citations[c("stop_id","person_number")]))
# looks good
