# Analysis : Most common traffic citations resulting from stop by traffic stop type ----------------------------------
# Top offense codes by traffic stop reason (moving, equipment, non-moving)

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

person_reason<-dbGetQuery(con, "SELECT * FROM rel_persons_reason")
person_result<-dbGetQuery(con, "SELECT * FROM rel_persons_result")

offense_codes<-dbGetQuery(con, "SELECT * FROM cadoj_ripa_offense_codes_2023")


# Join tables------------------------------------

df<-stop%>%
  filter(call_for_service==0)%>%
  left_join(person_reason)%>%
  filter(reason=="Traffic violation")%>%
  left_join(person_result)

# Analysis-------------------------------------------

# Rate: Offense codes / all citations WITHIN each traffic type --moving/nonmoving/equipment

df<-df%>%
  filter(stop_result_simple=="citation for infraction")%>%
  group_by(traffic_violation_type)%>%
  mutate(total=n())%>%
  group_by(traffic_violation_type, rfs_traffic_violation_code)%>%
  mutate(count=n(),
         rate=count/total*100)%>%
  slice(1)%>%
  left_join(offense_codes, by=c("rfs_traffic_violation_code"="offense_code"))%>%
  select(traffic_violation_type, rfs_traffic_violation_code, statute_literal_25,  total, count, rate)%>%
  arrange(traffic_violation_type, -rate)

# Push to postgres-----------------------------

# set column types
charvect = rep('varchar', ncol(df)) 
charvect <- replace(charvect, c(4,5,6), c("numeric"))

# add df colnames to the character vector

names(charvect) <- colnames(df)

dbWriteTable(con,  "report_citation_traffic_type_stop", df,
             overwrite = TRUE, row.names = FALSE,
             field.types = charvect)


# # write comment to table, and column metadata

table_comment <- paste0("COMMENT ON TABLE report_citation_traffic_type_stop  IS 'Analyzing top traffic citations given as a result of a traffic stop within each traffic stop type (moving, nonmoving, equipment).
The denominator (total column) for this analysis is all traffic stops resulting in a citation within each traffic stop type.
R script used to recode and import table: W:\\Project\\ECI\\Fresno RIPA\\GitHub\\JZ\\fresnoripa\\Analysis\\report_citation_traffic_type_stop.R
QA document: W:\\Project\\ECI\\Fresno RIPA\\Documentation\\QA_report_citation_traffic_type_stop.docx';

COMMENT ON COLUMN report_citation_traffic_type_stop.traffic_violation_type IS 'Traffic stop type (moving, nonmoving, equipment)';
COMMENT ON COLUMN report_citation_traffic_type_stop.rfs_traffic_violation_code IS 'Traffic stop resulting in a citation specific citation code';
COMMENT ON COLUMN report_citation_traffic_type_stop.statute_literal_25 IS 'Text description for accompnaying offense code';
COMMENT ON COLUMN report_citation_traffic_type_stop.total IS 'Total number (denominator in rate calc) of traffic stops that resulted in a citation within each traffic stop type';
COMMENT ON COLUMN report_citation_traffic_type_stop.count IS 'Count of each specific citation offense code within each traffic stop type';
COMMENT ON COLUMN report_citation_traffic_type_stop.rate IS 'Rate of Rate of traffic stops resulting in a citation by each citation code out of all traffic stops resulting in a citation
within each traffic stop type';
")

# send table comment + column metadata
dbSendQuery(conn = con, table_comment)
  
  
