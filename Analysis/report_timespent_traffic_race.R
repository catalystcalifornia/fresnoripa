# Task 1: Calculate time spent on traffic stops by race ----------------

# Environment set up ----
# Load Packages
library(RPostgreSQL)
library(dplyr)
library(purrr)
source("W:\\RDA Team\\R\\credentials_source.R")
conn <- connect_to_db("eci_fresno_ripa")

# Import data
time <- dbGetQuery(conn, "SELECT * FROM data.rel_stop_duration_capped")
stops <- dbGetQuery(conn, "SELECT * FROM data.rel_stops")
p_reasons <- dbGetQuery(conn, "SELECT * FROM data.rel_persons_reason")
p_races <- dbGetQuery(conn, "SELECT * FROM data.rel_races_recode")
races <- dbGetQuery(conn, "SELECT * FROM data.rel_stops_race")
p_results<-dbGetQuery(conn, "SELECT * FROM data.rel_persons_result")


# Stop universe selection and prep ----
# Get df for officer-initiated stops
ois <- stops %>% filter(call_for_service==0)%>%select(stop_id)

# Filter for people stopped just for traffic violations
traffic <- p_reasons%>%filter(reason=='Traffic violation')%>%select(stop_id,person_number,reason)

# Filter traffic stops just for ois
ois_traffic<-traffic%>%filter(stop_id %in% ois$stop_id)

# join race, result, and capped time
ois_traffic_race<-ois_traffic%>%
  left_join(p_races)%>%select(-race_count_re)%>%
  left_join(time)%>%
  left_join(p_results)

# add bipoc column 
ois_traffic_race<-ois_traffic_race%>%
  mutate(bipoc=ifelse(nh_race=='nh_white','White','BIPOC'))

table(ois_traffic_race$stop_result_simple)

# filter just for stops that resulted in one or more of the three: citation for infraction, warning, no action
target<-list("citation for infraction","no action","warning verbal or written")

# stop_result_list is not an actual list - need to convert and then can create  
# column that checks if elements in that list are not in our target list 
df <- ois_traffic_race %>%
  # will use eval(parse()) to convert to list, but need to reformat single result stops to have "c()" syntax
  mutate(stop_result_list2 = case_when(unique_stop_result_count==1 ~ paste0('c("', stop_result_list, '")'),
                                       .default = stop_result_list)) %>%
  # convert to list type (in View will still say character - 
  # can confirm with this code: typeof(df[1450, "stop_result_list3"]) VS typeof(df[1450, "stop_result_list"]) )
  mutate(stop_result_list3 = map(stop_result_list2, ~eval(parse(text=.x)))) %>%
  # actual check: returns TRUE if any of the elements in stop_result_list3 are not in our target list
  mutate(target_check = map_lgl(stop_result_list3, ~any(!(. %in% target)))) %>%
  # filter for only stops where all results are in target list
  filter(target_check == FALSE) %>%
  select(-c(stop_result_list2, stop_result_list3, target_check))

# confirm worked
table(df$stop_result_list)

# Time spent by race --------
# Measure: 5-number summary of time spent by race
df_race<-df%>%
  group_by(nh_race)%>%
  summarise(person_count=n(),
            average_time=mean(stop_duration_capped),
            median_time=median(stop_duration_capped),
            q25_time=quantile(stop_duration_capped,probs=c(.25)),
            q75_time=quantile(stop_duration_capped,probs=c(.75)),
            q95_time=quantile(stop_duration_capped,probs=c(.95)),
            min_time=min(stop_duration_capped),
            max_time=max(stop_duration_capped),
            .groups='drop')%>%
  rename(race=nh_race)

# Measure: 5-number summary of time spent by bipoc
df_bipoc<-df%>%
  group_by(bipoc)%>%
  summarise(person_count=n(),
            average_time=mean(stop_duration_capped),
            median_time=median(stop_duration_capped),
            q25_time=quantile(stop_duration_capped,probs=c(.25)),
            q75_time=quantile(stop_duration_capped,probs=c(.75)),
            q95_time=quantile(stop_duration_capped,probs=c(.95)),
            min_time=min(stop_duration_capped),
            max_time=max(stop_duration_capped),
            .groups='drop')%>%
  rename(race=bipoc)

df_final<-rbind(df_race,df_bipoc)

# Export Data ----
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

table_name <- "report_timespent_traffic_race"
schema <- 'data'

indicator <- "Time spent on officer-initiated traffic stops that resulted in citation for infraction, no action, or warning by race and by BIPOC or not. Includes 5-number summary. Times are in minutes. "
source <- "CADOJ RIPA 2022
See QA doc for details: W:/Project/ECI/Fresno RIPA/Documentation/QA_report_timespent_reason_race.docx
Script: W:/Project/ECI/Fresno RIPA/GitHub/EMG/fresnoripa/Analysis/report_timespent_reason.R"
table_comment <- paste0(indicator, source)

# write table
# dbWriteTable(conn, c(schema, table_name),df_final,
#        overwrite = FALSE, row.names = FALSE)
# 
# comment on table and columns

column_names <- colnames(df_final) # get column names

column_comments <- c('Non-hispanic race or BIPOC/not bipoc',
                     'total persons stopped in group',
                     'average minutes spent in group',
                     'median minutes spent',
                     '25th percentile of minutes spent',
                     '75th percentile of minutes spent',
                     '95th percentile of minutes spent',
                     'minimum minutes spent',
                     'maximum minutes spent')

# add_table_comments(conn, schema, table_name, indicator, source, column_names, column_comments)
