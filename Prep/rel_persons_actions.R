# Script creates a table that summarizes actions taken, including any use of force used, against persons stopped by Fresno PD in 2022
# Original data download link: https://openjustice.doj.ca.gov/data (2022 RIPA Stop Data)
# Data dictionary: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-01/RIPA%20Dataset%20Read%20Me%202022.pdf
# table from RDA Fresno data rel_persons

# Set up environment ----
library(RPostgreSQL)
library(dplyr)

# Connect to RDA databases
source("W:\\RDA Team\\R\\credentials_source.R")

fres <- connect_to_db("eci_fresno_ripa")

# pull in Fresno data from postgres
fresno_ripa<-dbGetQuery(fres, "SELECT * FROM rel_persons")

#### create relational table for actions taken ----
# based on code QA-ed as a part of fresno_ripa_import.R for rel_stops_actions

# use of force codes includes:
# Baton or other impact weapon used,  "ads_baton"
# Canine bit or held person, "ads_canine_bite"
# Chemical spray used, "ads_chem_spray"
# Electronic control device used, "ads_elect_device"
# Firearm pointed at person, "ads_firearm_point"
# Firearm discharged or used, "ads_firearm_discharge"
# Person removed from vehicle by physical contact, "ads_removed_vehicle_phycontact"
# Physical or Vehicle contact,
# Impact projectile discharged or used'; "ads_impact_discharge"

##### create list of use of force columns -----
force_list<-c("ads_removed_vehicle_phycontact",
              "ads_firearm_point",
              "ads_firearm_discharge",
              "ads_elect_device",
              "ads_impact_discharge",
              "ads_canine_bite",
              "ads_baton",
              "ads_chem_spray",
              "ads_other_contact")

# other key actions to track
removed_from_vehicle_list <- c("ads_removed_vehicle_order","ads_removed_vehicle_phycontact")
handcuffed_list <- c("ads_handcuffed")
detained_list <- c("ads_patcar_detent","ads_curb_detent")
actions_list <- colnames(fresno_ripa)[grep("^ads_", colnames(fresno_ripa), fixed = FALSE)] # all actions taken columns
excluded_actions_list <- c("ads_search_pers_consen","ads_search_prop_consen","ads_no_actions")
included_actions_list <-  setdiff(actions_list, excluded_actions_list) # just actions that are actually taken vs. consent or no action

# calculate actions taken by person -- total actions and types of actions for each person in the stop
actions<- fresno_ripa%>%
  rowwise()%>% 
  select(stop_id,person_number, all_of(actions_list))%>% # select relevant columns
  # summarise relevant indicators by row or person
  mutate(removed_from_vehicle=sum(c_across(removed_from_vehicle_list), na.rm = TRUE), # removed from vehicle by force or order
         actions_count=sum(c_across(all_of(included_actions_list)), na.rm = TRUE), # total actions taken excluding actions about consent and no action taken flag
         handcuffed=sum(c_across(handcuffed_list), na.rm = TRUE),
         detained=sum(c_across(detained_list),na.rm=TRUE), # detained
         use_of_force=sum(c_across(force_list), na.rm = TRUE), # force used
         action_taken=ifelse(ads_no_actions==1,0,1), # recode no action taken to be true for action taken
  )%>%
  select(stop_id,person_number,action_taken,actions_count,removed_from_vehicle,handcuffed,detained,use_of_force,everything())%>% # reorders columns
  ungroup()

# clean up and summarise with 0/1 variables by person stopped for key flagged actions
rel_persons_action <- actions %>%
  mutate(across(c("removed_from_vehicle","handcuffed","detained","use_of_force"), .fns = function(x) ifelse(x >= 1, 1, 0))) %>% # 1 means true that action was taken
  select(-c(ads_no_actions,ads_search_pers_consen,ads_search_prop_consen)) # remove columns that aren't included in actions taken count


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

#### Relational actions taken stops ----
table_name <- "rel_persons_actions"
schema <- 'data'

indicator <- "Key actions taken fields summarised for people stopped by Fresno PD from 2022. Includes flag for whether Use of force was used against the person"
source <- "See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_rel_persons_actions.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Prep/rel_persons_actions.R"
table_comment <- paste0(indicator, source)

# write table
# dbWriteTable(fres, c(schema, table_name),rel_persons_action,
#              overwrite = FALSE, row.names = FALSE)

# comment on table and columns

# column_names <- colnames(rel_persons_action) # get column names
# 
# column_comments <- c(
#   "A unique system-generated incident identification number. Alpha-numeric. previously doj_record_id",
#   "A system-generated number that is assigned to each individual who is involved in the stop or encounter. Numeric",
#   "0/1 flag for whether any action was taken against the person",
#   "Total actions taken against the person",
#   "0/1 flag for whether person was removed from the vehicle by physical contact or order",
#   "0/1 flag for whether person was handcuffed",
#   "0/1 flag for whether person was detained in patrol car or curbside",
#   "0/1 flag for whether any force was used - defined as Baton or other impact weapon used,Canine bit or held person,Chemical spray used,Electronic control device used,Firearm pointed at person,Firearm discharged or used,Person removed from vehicle by physical contact,Physical or Vehicle contact,Impact projectile discharged or used",
#   "Action taken by officer during stop person removed from vehicle by order. 0 No 1 Yes",
#   "Action taken by officer during stop person removed from vehicle by physical contact. 0 No 1 Yes",
#   "Action taken by officer during stop field sobriety test. 0 No 1 Yes",
#   "Action taken by officer during stop curbside detention. 0 No 1 Yes",
#   "Action taken by officer during stop handcuffed or flex cuffed. 0 No 1 Yes",
#   "Action taken by officer during stop patrol car detention. 0 No 1 Yes",
#   "Action taken by officer during stop canine removed search. 0 No 1 Yes",
#   "Action taken by officer during stop firearm pointed at person. 0 No 1 Yes",
#   "Action taken by officer during stop firearm discharged or used. 0 No 1 Yes",
#   "Action taken by officer during stop electronic device used. 0 No 1 Yes",
#   "Action taken by officer during stop impact projectile discharged or used e.g. blunt impact projectile, rubber bullets, bean bags. 0 No 1 Yes",
#   "Action taken by officer during stop canine bit or held person. 0 No 1 Yes",
#   "Action taken by officer during stop baton or other impact weapon used. 0 No 1 Yes",
#   "Action taken by officer during stop chemical spray use pepper spray, mace, tear gas, or other chemical irritants. 0 No 1 Yes",
#   "Action taken by officer during stop other physical or vehicle contact. 0 No 1 Yes",
#   "Action taken by officer during stop person photographed. 0 No 1 Yes",
#   "Action taken by officer during stop asked for consent to search person. 0 No 1 Yes",
#   "Action taken by officer during stop search of person was conducted. 0 No 1 Yes",
#   "Action taken by officer during stop asked for consent to search. 0 No",
#   "Action taken by officer during stop search of property was conducted. 0 No 1 Yes",
#   "Action taken by officer during stop property was seized. 0 No 1 Yes",
#   "Action taken by officer during stop vehicle impound. 0 No 1 Yes",
#   "Action taken by officer during stop admission or written statement obtained from student. 0 No 1 Yes"
#    )

# add_table_comments(fres, schema, table_name, indicator, source, column_names, column_comments)
