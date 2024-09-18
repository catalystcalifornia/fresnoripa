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