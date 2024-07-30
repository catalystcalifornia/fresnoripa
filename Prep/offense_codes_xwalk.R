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

indicator <- "Table of CJIS Offense Codes from RIPA data with statute equivalents. For help in identifying VC statutes of interest. Use offense_code column to match to charge codes in RIPA data "
source <- "Source: https://oag.ca.gov/law/code-tables
See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_offense_codes_import.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Prep/offense_codes_xwalk.R"
table_comment <- paste0(indicator, source)

# write table
dbWriteTable(con, c(schema, table_name),df,
       overwrite = FALSE, row.names = FALSE)

# comment on table and columns

column_names <- colnames(df) # get column names

column_comments <- c(
   "Offense Code",
  "Offense statute and offense type of statute CD combined",
  "Offense Statute",
 "Offense Type of Statute CD, e.g., VC for vehicle charge",
 "Statute Literal 25 description of offense",
  "Offense Type of Charge, e.g., Misdemeanor (M), Infraction (I), Felony (F)",
  "Changes",
 "Offense Validation CD",
  "Offense Txn Type CD",
  "Offense Default Type of Charge",
  "Offense Literal Identifier CD",
  "Offense Degree",
  "BCS Hierarchy CD",
  "Offense Enacted",
  "Offense Repealed",
  "ALPS Cognizant CD"
)

add_table_comments(con, schema, table_name, indicator, source, column_names, column_comments)
