# Script pivots citation codes used against people in Fresno RIPA 2022 data
# Uses prepped and QA-ed tables

# Set up environment ----
library(RPostgreSQL)
library(dplyr)
library(tidyr)

# Connect to RDA databases ----
source("W:\\RDA Team\\R\\credentials_source.R")

fres <- connect_to_db("eci_fresno_ripa")

# pull in Fresno data from postgres
fresno_ripa<-dbGetQuery(fres, "SELECT * FROM rel_persons")

# Citations pivoted ----
citations<-fresno_ripa %>% 
  select(stop_id,person_number,ros_citation,ros_citation_cds)%>%
  separate_rows(ros_citation_cds)

# test nothing dropped
length(unique(citations$stop_id))
n_distinct(citations[c("stop_id","person_number")])
# looks good

# qa check
test<-fresno_ripa %>% 
  select(stop_id,person_number,ros_citation,ros_citation_cds)
# looked at stop id U10052200194C71F882B, looks good, two citations 

# test for instances where officer failed to report a citation
test<-citations%>%filter(ros_citation==1 & is.na(ros_citation_cds))

# Warnings pivoted ----
warnings<-fresno_ripa %>% 
  select(stop_id,person_number,ros_warning,ros_warning_cds)%>%
  separate_rows(ros_warning_cds)

# test nothing dropped
length(unique(warnings$stop_id))
n_distinct(warnings[c("stop_id","person_number")])
# looks good

# test for instances where officer failed to report a citation
test<-warnings%>%filter(ros_warning==1 & is.na(ros_warning_cds))

# In field cite and release pivoted ----
cite_release<-fresno_ripa %>% 
  select(stop_id,person_number,ros_in_field_cite_release,ros_in_field_cite_release_cds)%>%
  separate_rows(ros_in_field_cite_release_cds)

# test nothing dropped
length(unique(cite_release$stop_id))
n_distinct(cite_release[c("stop_id","person_number")])
# looks good

# test for instances where officer failed to report a citation
test<-cite_release%>%filter(ros_in_field_cite_release==1 & is.na(ros_in_field_cite_release_cds))


# Combine results and clean up -----
df<-rbind(
  citations%>%filter(ros_citation==1)%>%
  rename(result_category=ros_citation,
         offense_code=ros_citation_cds)%>%
  mutate(result_category='Citation for infraction'),
  warnings%>%filter(ros_warning==1)%>%
    rename(result_category=ros_warning,
           offense_code=ros_warning_cds)%>%
    mutate(result_category='Warning (verbal or written)'),
  cite_release%>%filter(ros_in_field_cite_release==1)%>%
    rename(result_category=ros_in_field_cite_release,
           offense_code=ros_in_field_cite_release_cds)%>%
    mutate(result_category='In field cite and release'))


# Push to postgres -----
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

table_name <- "rel_persons_citations"
schema <- 'data'

# write table
# dbWriteTable(fres, c(schema, table_name),df,
#              overwrite = FALSE, row.names = FALSE)

# comment on table and columns
indicator <- "Long table format of all offense codes officer charged against people stopped. Include CJIS offense codes for any warnings or citations given or for in-field cite and release. One person may have multiple rows if they were given different citations or offenses.
For the most part offenses provided under citation are the same as offenses provided under in-field cite and release with some differences 
Only people that received at least one warning, citation, or in-field cite and release are included "
source <- "See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_rel_citations.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Prep/rel_citations.R"
table_comment <- paste0(indicator, source)

column_names <- colnames(df) # get column names

column_comments <- c(
  "A unique system-generated incident identification number. Alpha-numeric. previously doj_record_id",
  "person id for person within the stop",
  "The type of result for which offense was charged--either warning, citation for infraction, or in-field cite and release",
  "The CJIS offense code specified by the officer for the corresponding stop result"
  )

# add_table_comments(fres, schema, table_name, indicator, source, column_names, column_comments)

