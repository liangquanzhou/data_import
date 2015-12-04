### set work dir ###

setwd("C:/Users/Liangquan/Desktop/PAE0601M 09_30_2015  21_48_47")

######## Read data ########
source("functions and pckg.R")

### cleaning plaintiff 
source("plaintiff.R")
### cleaning attorney
source("attorney.R")
################################################################################
# then manually update the attorney info at update attorney.csv file and save it
################################################################################
source("update attorney.R")


### cleaning apn - unfinished - at "EMALPLE 5 of Removing SELECT Duplicates
source("apn unfinished.R")