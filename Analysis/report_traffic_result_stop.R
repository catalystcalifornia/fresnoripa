###Analysis: Traffic stops by result of stop and result code. Includes a cut by race. 

#Set up work space---------------------------------------


library(RPostgreSQL)
library(dplyr)
library(tidyr)
library(stringr)

#connect to postgres

source("W:\\RDA Team\\R\\credentials_source.R")

con <- connect_to_db("eci_fresno_ripa")

# pull in necessary analysis tables

stop<-dbGetQuery(con, "SELECT * FROM rel_stops")
stop_reason<-dbGetQuery(con, "SELECT * FROM rel_stops_reason")
stop_result<-dbGetQuery(con, "SELECT * FROM rel_stops_result")

# Analysis:  Traffic stops by result ---------------------------
# General table of people stopped for traffic violations by simple result

# Take stop table and filter for calls for service, then join all needed tables

df<-stop%>%
  filter(call_for_service==0)%>%
  left_join(stop_reason)%>%
  left_join(stop_result)

# Analyze

df<-df%>%
  filter(stop_reason_simple=="Traffic violation")%>%
  mutate(total=n())%>%
  group_by(stop_result_simple)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  select(stop_reason_simple, stop_result_simple, total, count, rate)%>%
  arrange(-rate)

# Push table to postgres------------------------------

# set column types
charvect = rep('varchar', ncol(df)) 
charvect <- replace(charvect, c(3,4,5), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df)

dbWriteTable(con,  "report_traffic_result_stop", df,
             overwrite = TRUE, row.names = FALSE,
             field.types = charvect)


# # write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE report_traffic_result_stop  IS 'Analyzing officer-initiated traffic stops by simple stop result.
R script used to recode and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Analysis\\report_traffic_result_stop.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_traffic_result_stop.docx';

COMMENT ON COLUMN report_traffic_result_stop.stop_reason_simple IS 'Reason for stop (which will only be traffic violations for this analysis)';
COMMENT ON COLUMN report_traffic_result_stop.stop_result_simple IS 'Simple result for stop';
COMMENT ON COLUMN report_traffic_result_stop.total IS 'Total number of officer-initiated traffic stops (denominator in rate calc)';
COMMENT ON COLUMN report_traffic_result_stop.count IS 'Count of officer-initiated traffic stops for each stop result (numerator for rate calc)';
COMMENT ON COLUMN report_traffic_result_stop.rate IS 'Rate of officer-initiated traffic stops by stop result';
")

# send table comment + column metadata
 dbSendQuery(conn = con, table_comment)

