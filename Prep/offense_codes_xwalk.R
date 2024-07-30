# Import table of CJIS offense codes compared to offense statutes and type of charge
# Data sources: CADOJ Law Enforcement Code Tables RIPA - Stop Data Collection System 6/29/2023
# Link: https://oag.ca.gov/law/code-tables

# set-up environment ----
library(RPostgreSQL)
library(dplyr)
library(readxl)
# library(sf)
# library(httr)
# library(jsonlite)
# library(sp)
library(janitor)
# library(lubridate)
# library(dplyr)

source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("eci_fresno_ripa")

options(scipen = 100)

# pull-in table and clean up ----
offense_codes <- read_excel("W:/Project/ECI/Fresno RIPA/Data/sdcs-look-up-table-2023.v2.xlsx",
     sheet = "Offense Table", col_types="text") %>% clean_names()

# rearrange
df<-offense_codes%>%
  mutate(statute_full=paste(offense_type_of_statute_cd,"",offense_statute))%>% # add full column for corresponding statute
  select(offense_code,statute_full,offense_statute,offense_type_of_statute_cd,statute_literal_25,offense_type_of_charge,everything()) # put most important columns first

# Push to postgres ----
# function for adding table and column comments from RC
add_table_comments <- function(con, schema, table_name, indicator, source, column_names, column_comments) {
  comments <- character()
  comments <- c(comments, paste0("
    COMMENT ON TABLE ", schema, ".", table_name, " IS '", table_comment, "';"))
  for (i in seq_along(column_names)) {
    comment <- paste0("
      COMMENT ON COLUMN ", schema, ".", table_name, ".", column_names[i], " IS '", column_comments[i], "';
      ")
    comments <- c(comments, comment)
  }
  sql_commands <- paste(comments, collapse = "")
  dbSendQuery(con, sql_commands)
}

table_name <- "cadoj_ripa_offense_codes_2023"
schema <- 'data'

indicator <- "Universe of all fresno pd stops from 2022 - original data filtered from crime_and_justice.cadoj_ripa_2022."
source <- "CADOJ RIPA 2022
See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_fresno_ripa_import.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Prep/fresno_ripa_import.R
Data dictionary: source: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-01/RIPA%20Dataset%20Read%20Me%202022.pdf"
table_comment <- paste0(indicator, source)

# # write table
# dbWriteTable(fres, c(schema, table_name),fresno_ripa,
#        overwrite = FALSE, row.names = FALSE)
# 
# #comment on table and columns
# 
# column_names <- colnames(fresno_ripa) # get column names

column_comments <- c(
  "A unique system-generated incident identification number. Alpha-numeric. previously doj_record_id",
  "0/1 flag for whether any search was done during the stop - person or property",
  "0/1 flag for whether any contraband or evidencewas found during the stop",
  "Total searches of person or property done during the stop",
  "Total contraband or evidence found",
  "Total searches of person done during the stop",
  "Total searches of property done during the stop",
  "Total times consent of search of person was received during the stop",
  "Total times consent of search of property was received during the stop"
)

add_table_comments(fres, schema, table_name, indicator, source, column_names, column_comments)
