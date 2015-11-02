### set work dir ###
setwd("C:/Users/Liangquan/Google Drive/GovPilot/Teams/Data Aggregation _ Integrity Team/New Jersey/Data Import/PAE0601M 07_31_2015  22_05_20")

setwd("C:/Users/Liangquan Zhou/Desktop/govpilot/data_import/data_import")

############ packages needed ##############

pckg = c('tidyr', 'stringi', 'data.table', 'reshape2', 'plyr','RgoogleMaps', 'httr', 'rjson', 'XML',
  'choroplethrMaps', 'choroplethr', 'RODBC', 'zipcode','RCurl','jsonlite', 'RJSONIO', 'ggmap') 

is.installed <- function(mypkg){
  is.element(mypkg, installed.packages()[,1])
} 

for(i in 1:length(pckg)) {
  if (!is.installed(pckg[i])){
    install.packages(pckg[i])
  }
  library(pckg[i], character.only = T)
}

### given a dataframe and an array of names to drop,
### drop these names and rename the df from alphabetically

drop_and_rename = function(df, drops = NA){
  names(df) = LETTERS[1:length(names(df))]
  df = df[, !(names(df) %in% drops)]
  names(df) = LETTERS[1:length(names(df))]
  return(df)
}


######## Read data ########

fc = file('pampac0601.txt')
mylist = strsplit(readLines(fc), ";")
close(fc)


options(stringAsFactors=F)
l2 = lapply(mylist, t)
l3 = lapply(l2, as.data.frame)
d = as.data.frame(rbindlist(l3, fill = T))

d1 = d[d$V6 == "0",]
d2 = d[d$V6 == "1",]
d3 = d[d$V6 == "3",]

######### Plaintiff ##########
d1s = d1[,c(10, 5, 4, 9)]

d1s$V10 = as.character(d1s$V10)
d1s$V10[d1s$V10 == "088"] = "T"
d1s$V10[d1s$V10 == "089"] = "T"
d1s$V10[d1s$V10 == "091"] = "T"
d1s$V10[d1s$V10 == "0CD"] = "C"
d1s$V10[d1s$V10 == "0CF"] = "M"
d1s$V10[d1s$V10 == "0FP"] = "M"
d1s$V10[d1s$V10 == "0RF"] = "M"
d1s$V10[d1s$V10 == "0TS"] = "M"
d1s$V10 = as.factor(d1s$V10)

## find and replace ##
d1s$V9 = as.character(d1s$V9)
d1s$V9 = sub(" VS ", ";", d1s$V9)

d1s = separate(d1s, V9, into = c('V9.1', 'V9.2'), sep = ';')
d1s$V9.2 = stri_trim_both(d1s$V9.2)

write.table(d1s, file = "Plaintiff.csv", sep = ',', row.names = F, col.names = F)

######## Attorney #########
names(d2) = LETTERS[1:length(names(d2))]
d2s = d2[,c('E','D','P','Q','R')]
names(d2s) = LETTERS[1:length(names(d2s))]

## combine C,D,E ##
## C,D,E together = Attorney full name ##
i <- sapply(d2s, is.factor)
d2s[i] <- lapply(d2s[i], as.character)

d2s$F = paste(d2s$C, d2s$D, d2s$E, sep = '')
d2s = d2s[,-c(3,4,5)]
names(d2s) = LETTERS[1:length(names(d2s))]

## drop rows with blank column C ##
# trim column C
d2s$C = trimws(d2s$C)
d2s = d2s[!(is.na(d2s$C) | d2s$C==""), ]

# drops = c("D")
# d2s = d2s[,!(names(d2s) %in% drops)]

## drop duplicates ##
d2s = d2s[!duplicated(d2s[,"B"]),]

#### connect to acess database ###
# accessname = 'Fix Attorneys.accdb'
# dbpath = paste(getwd(), '/', accessname, sep = '')

# con = odbcConnectAccess2007(dbpath)

# sqlTables(con, tableType = "TABLE")$TABLE_NAME

# Att <- sqlFetch(con, "List")
# str(Att)
# View(Att)



write.table(d2s, file = "Attorney.csv", sep = ',', row.names = F, col.names = F)

################# APN #########################
head(d3)
View(d3)

names(d3) = LETTERS[1:length(names(d3))]

drops = c('B', 'C', 'F', 'G', 'H', 'I', 'M', 'N', 'O', 'P', 'Q', 'R')
d3s = d3[,!(names(d3) %in% drops)]
head(d3s)

names(d3s) = LETTERS[1:length(names(d3s))]

### remove rows have no address (D)
# View(d3s1[order(d3s1$D),])
d3s$D = gsub("^$|^ *$", NA, d3s$D)
d3s = (d3s[!(is.na(d3s$D) | d3s$D==""), ])


# trim column D, E, F
d3s$D = trimws(d3s$D)
d3s$E = trimws(d3s$E)
d3s$F = trimws(d3s$F)

## remove duplicates 
dup = which(d3s$B %in% d3s[duplicated(d3s[,"B"]),]$B) ## duplicated records in B
dup.lev = d3s[duplicated(d3s[,"B"]),]$B #duplicated docket numbers

## remove these records without blk and lot numbers
d3s[dup,c("G","H")] = apply(d3s[dup,c("G","H")], 2, function(x) gsub("^ *$", NA, x))
d3s = d3s[!(is.na(d3s$G) | is.na(d3s$G)), ]


## remove these rocords with same blk and lot numbers  -- NOT COMPLETED
# View(d3s[(duplicated(d3s[,"B"]) & duplicated(d3s[,"G"]) & duplicated(d3s[,"H"])),])

## column E 
d3s$E = gsub("^ *$", NA, d3s$E)
View(d3s[order(d3s$E),])
View(d3s)

## combine D E F as address
head(d3s)
c1 = paste(paste(d3s$D, ifelse(!is.na(d3s$E), d3s$E, ''),sep = ' '), d3s$F, 'NJ', sep = ',')
d3s$c1 = c1
c1s = c1[1:500]

#############################################################
#############################################################
## use google geocoding api to clean address
apikey='AIzaSyCql9P7PeLE6dPVmXtkxN14MSgXCxXpcBU'
c1s = c1[1:500]

geourl = 'https://maps.googleapis.com/maps/api/geocode/json?address='

api_req = paste0(geourl, c1s[1], '&key=', apikey)

##download.file(api_req, destfile = 't.json')

## use to get data
# fromJSON(api_req)


## Better way: use geocode in ggmap packages to batch processing
View(geocode(c1s[197],output = 'more',source = 'google'))

add = geocode(c1s,output = 'more',source = 'google')
b = add

# choose right format of address
b$premise = ifelse(!is.na(b$premise), b$premise, '')
b$subpremise = ifelse(!is.na(b$subpremise), b$subpremise, '')
b = paste(paste(b$street_number,b$route,b$premise,b$subpremise,sep = ' '),b$locality,b$administrative_area_level_2, sep = ',')

## compare results
d3s1 = d3s[1:500,]
apn = data.frame(as.character(d3s1$A),as.character(add$administrative_area_level_2),as.character(d3s1$B),
  as.character(add$type),as.character(add$loctype), as.character(d3s1$C),as.character(c1s),
  as.character(add$address),as.character(d3s1$G),as.character(d3s1$H),as.character(d3s1$J),
  as.character(add$locality))

View(apn)

apn = drop_and_rename(apn)
apn_good = apn[which(apn$D == 'street_address' | apn$E == 'rooftop'),]
apn_bad = apn[-which(apn$D == 'street_address' | apn$E == 'rooftop'),]
write.table(apn_good, file = "apn_good.csv", sep = ',', row.names = F, col.names = F)
write.table(apn_bad, file = "apn_bad.csv", sep = ',', row.names = F, col.names = F)



###############################################################
###############################################################

### load mulicipalities code data ### 

d3s$D = d3s$c1
drops = c('E','F','c1')
d3s = d3s[,!(names(d3s) %in% drops)]
head(d3s1)
names(d3s) = LETTERS[1:length(names(d3s))]


