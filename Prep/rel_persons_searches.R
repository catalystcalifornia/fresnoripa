
# Set up environment ----
library(RPostgreSQL)
library(dplyr)

# Connect to RDA databases
source("W:\\RDA Team\\R\\credentials_source.R")

fres <- connect_to_db("eci_fresno_ripa")

# pull in Fresno data from postgres
fresno_ripa <- dbGetQuery(conn=fres, "SELECT * FROM cadoj_ripa_fresno_2022;")

#### create relational tables for searches ----
# Creating lists of columns from the table cadoj_ripa_fresno_2022. Column comments are added here for clarity.
# ads_search_person IS 'Action taken by officer during stop search of person was conducted. 0 No 1 Yes';
# ads_search_property IS 'Action taken by officer during stop search of property was conducted. 0 No 1 Yes';
search_list <- c("ads_search_person","ads_search_property")

# ads_search_pers_consen IS 'Action taken by officer during stop specify if consent was given for search of person. 0 No 1 Yes Blank';
# ads_search_prop_consen IS 'Action taken by officer during stop specify if consent was given for search of property. 0 No 1 Yes Blank';
consent_list <- c("ads_search_pers_consen","ads_search_prop_consen")

# Gets all the columns that start with “ced_”. These columns indicate whether a specific type of contraband was found. All the types of contraband are firearm, ammunition, weapon, drugs, alcohol, money, drug paraphernalia, stolen property, cell phone/electronic device, and other.
ced_list <- colnames(fresno_ripa)[grep("^ced_", colnames(fresno_ripa), fixed = FALSE)]

# contraband_list is ced_list without the column ced_none_contraband. ced_none_contraband IS 'Contraband or evidence discovered none. 0 No 1 Yes';
contraband_list <- setdiff(ced_list, c("ced_none_contraband"))


# calculate searches conducted by person -- total searches and whether contraband was found for each person in the stop
searches <- fresno_ripa %>%
  # subset columns
  # doj_record_id IS 'A unique system-generated incident identification number. Alpha-numeric';
  # person_number IS 'A system-generated number that is assigned to each individual who is involved in the stop or encounter. Numeric'; 
  select(doj_record_id, person_number, all_of(search_list), all_of(consent_list), all_of(ced_list))%>%
  rowwise() %>%
  # summarise relevant indicators by row or person
  mutate(searches_count=sum(c_across(all_of(search_list)), na.rm = TRUE), # total searches done on person
         contraband_count=sum(c_across(all_of(contraband_list)), na.rm = TRUE), # total contraband found subtracting flag for no contraband
         contraband_found=ifelse(ced_none_contraband==1,0,1), # recode no contraband found to be true if contraband found
         search_person=ads_search_person, # flag if person was searched
         search_property=ads_search_property, # flag if property was searched
         consent_search_person=ads_search_pers_consen, # flag if consent was given
         consent_search_property=ads_search_prop_consen)  # flag if consent was given

# reduce columns
searches <- searches%>%
  select(doj_record_id, person_number, searches_count, contraband_found, contraband_count, 
         search_person, search_property, consent_search_person, consent_search_property)%>%
  ungroup()

# create final table
rel_persons_searches <- searches %>% 
  rename(stop_id=doj_record_id)

# Push to postgres data ----
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

#### Relational searches stops ----
table_name <- "rel_persons_searches"
schema <- 'data'

indicator <- "A person-level table of person and property searches (if they occurred, quantity of contraband found as a result) for 2022 Fresno stop data."
source <- "See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_rel_persons_searches.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/HK/fresnoripa/Prep/rel_persons_searches.R"
table_comment <- paste0(indicator, source)

# # write table
# dbWriteTable(fres, c(schema, table_name), rel_persons_searches,
#              overwrite = FALSE, row.names = FALSE)

# comment on table and columns

column_names <- colnames(rel_persons_searches) # get column names

column_comments <- c(
  "A unique system-generated incident identification number. Alpha-numeric. previously doj_record_id",
  "Person ID within a given stop; Creates unique person identifier when used with stop_id",
  "Total searches of person and property done during the stop",
  "0/1 flag for whether any contraband or evidencewas found during the stop",
  "Total contraband and evidence found",
  "0/1 flag for whether search of person was done during the stop",
  "0/1 flag for whether search of property was done during the stop",
  "0/1 flag for whether consent of search of person was received during the stop",
  "0/1 flag for whether consent of search of property was received during the stop"
)



# add_table_comments(fres, schema, table_name, indicator, source, column_names, column_comments)

dbDisconnect(fres)