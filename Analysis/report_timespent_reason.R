# Task 1: Calculate time spent by stop reason ----------------

# Environment set up ----
# Load Packages
library(tidyverse)
library(RPostgreSQL)
library(dplyr)
source("W:\\RDA Team\\R\\credentials_source.R")
conn <- connect_to_db("eci_fresno_ripa")

# Import data
time <- dbGetQuery(conn, "SELECT * FROM data.rel_stop_duration_capped")
stops <- dbGetQuery(conn, "SELECT * FROM data.rel_stops")
reasons <- dbGetQuery(conn, "SELECT * FROM data.rel_stops_reason")

# Stop universe selection and prep ----
# join stop reasons and capped time
stops<-stops%>%select(stop_id,call_for_service)%>%
  left_join(reasons)%>%
  left_join(time)

# Time spent by stop reason --------
# Measure: what % of time did officers spent on each stop reason out of officer initiated stops
df<-stops%>%filter(call_for_service==0)%>%
  group_by(stop_reason_simple)%>%
  summarise(stop_count=n(),
            hours_count=sum(stop_duration_capped)/60,
            minutes_average=mean(stop_duration_capped),
            minutes_median=median(stop_duration_capped),
            .groups='drop')%>%
  mutate(hours_rate=hours_count/sum(hours_count),
         hours_total=sum(hours_count),
         stop_rate=stop_count/sum(stop_count),
         stop_total=sum(stop_count),
         level="officer-initiated stops")%>%
  rename(stop_reason=stop_reason_simple)

# Time spent by ois vs. call for service --------
# Measure: % of time spent on officer-initated stops vs. calls for service
df_cfs<-stops%>%
  group_by(call_for_service)%>%
  summarise(stop_count=n(),
            hours_count=sum(stop_duration_capped)/60,
            minutes_average=mean(stop_duration_capped),
            minutes_median=median(stop_duration_capped),
            .groups='drop')%>%
  mutate(hours_rate=hours_count/sum(hours_count),
         hours_total=sum(hours_count),
         stop_rate=stop_count/sum(stop_count),
         stop_total=sum(stop_count),
         level="all stops",
         call_for_service=ifelse(call_for_service==0,"Officer-initiated stops","Calls for service"))

# Export Data ----
# rearrange columns
df<-df%>%
  select(1,minutes_average,minutes_median,hours_count, hours_total, hours_rate,stop_count,stop_total,stop_rate,level)

df_cfs<-df_cfs%>%
  select(1,minutes_average,minutes_median,hours_count, hours_total, hours_rate,stop_count,stop_total,stop_rate,level)

# function for adding table and column comments
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

### By stop reason ----
table_name <- "report_timespent_reason"
schema <- 'data'

indicator <- "Time spent on officer-initiated stops by stop reason, including percent of time, average, median. Times are in hours or minutes as noted"
source <- "CADOJ RIPA 2022
See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_report_timespent_reason_race.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Analysis/report_timespent_reason.R"
table_comment <- paste0(indicator, source)

# write table
# dbWriteTable(conn, c(schema, table_name),df,
#        overwrite = FALSE, row.names = FALSE)
# 
# comment on table and columns
 
column_names <- colnames(df) # get column names

column_comments <- c('Reason for the stop simplified to be unique by stop',
                     'average minutes spent on stop reason',
                     'median minutes spent on stop reason',
                     'hours spent on stop reason',
                     'total hours spent on officer-initiated stops',
                     'rate/percent of hours spent on stop reason out of ois stops',
                     'total stops for stop reason',
                     'total officer-initiated stops',
                     'rate/percent of stops out of ois stops',
                     'universe of stops')

# add_table_comments(conn, schema, table_name, indicator, source, column_names, column_comments)

### Ois by calls for service ----
table_name <- "timespent_ois_cfs"
schema <- 'data'

indicator <- "Time spent on stops by officer-inititated or calls for service, including percent of time, average, median. Times are in hours or minutes as noted"
source <- "CADOJ RIPA 2022
See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_report_timespent_reason_race.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Analysis/report_timespent_reason.R"
table_comment <- paste0(indicator, source)

# write table
# dbWriteTable(conn, c(schema, table_name),df_cfs,
#        overwrite = FALSE, row.names = FALSE)

# comment on table and columns

column_names <- colnames(df_cfs) # get column names

column_comments <- c('Calls for service or officer-initiated stops',
                     'average minutes spent',
                     'median minutes spent',
                     'hours spent',
                     'total hours spent on all stops',
                     'rate/percent of hours spent out of all stops',
                     'stop count',
                     'total stops',
                     'rate/percent of stops out of all stops',
                     'universe of stops')

# add_table_comments(conn, schema, table_name, indicator, source, column_names, column_comments)

