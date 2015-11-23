### set work dir ###
setwd("C:/Users/Liangquan/Google Drive/GovPilot/Teams/Data Aggregation _ Integrity Team/New Jersey/Data Import/PAE0601M 07_31_2015  22_05_20")

setwd("C:/Users/Liangquan Zhou/Desktop/govpilot/data_import/data_import")

############ packages needed ##############

pckg = c('tidyr', 'stringi', 'data.table', 'reshape2', 'plyr','RgoogleMaps', 'httr', 'rjson', 'XML',
  'choroplethrMaps', 'choroplethr', 'RODBC', 'zipcode','RCurl','jsonlite', 'RJSONIO', 'ggmap', 'xlsx') 

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
d = as.data.frame(rbindlist(l3, fill = T),stringAsFactors = F)

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


## drop duplicates ##
d2s = d2s[!duplicated(d2s[,"B"]),]

#### connect to acess database ###

 accessname = 'Fix Attorneys.accdb'
 folderpath = "C:/Users/Liangquan/Google Drive/GovPilot/Teams/Data Aggregation _ Integrity Team/New Jersey/Data Import/Required Files"
 dbpath = paste(folderpath, '/', accessname, sep = '')

 con = odbcConnectAccess2007(dbpath)

 sqlTables(con, tableType = "TABLE")$TABLE_NAME
 
 List <- sqlFetch(con, "List")
 str(List)
 # write.table(List,file= 'List.csv', sep = ",", row.names = F, col.names=F) # a list of all anttorney we have now
 
 List$`Wrong Attorney` = trimws(List$`Wrong Attorney`)
 List$`Right Attorney` = trimws(List$`Right Attorney`)
 # update the attorney name on d2s
 # which records in d2s in already in the List
 a = which(d2s$C %in% List$`Wrong Attorney`)
 #replace those records in d2s
 d2s$D = NA
 
 f = function(x){
   n = which(List$`Wrong Attorney` == x)
   x = List$`Right Attorney`[n] 
   return(x)
 }
 d2s$D[a] = as.character(sapply(d2s$C[a], f))
 
 # output all NAs in d2s$D, which are attorneys need to be updated
 d2s1 = d2s[is.na(d2s$D),]
 d2s12 = d2s1[!duplicated(d2s1[,"C"]),] # contains no duplicates
 d2s12 = drop_and_rename(d2s12, c("A", "B"))
 write.table(d2s12, 'update attorney.csv', col.names = F, row.names = F, sep = ",")
 
################################################################################
# then manually update the attorney info at update attorney.csv file and save it
################################################################################
## fix the attorney table and update it in the access database
 
 d2s2 = read.table(file = 'update attorney.csv',sep = ',')
 d2s2 = drop_and_rename(d2s2)
 names(d2s2) = c("Wrong Attorney","Right Attorney")
# d2s2$ID = seq(from = length(List$ID)+1,length.out = length(d2s2$ID))

 sqlQuery(con, "create table temp(ID int, `Wrong Attorney` varchar(255),`Right Attorney` varchar(255), PRIMARY KEY (ID));")
 sqlSave(con, d2s2, tablename = "temp")
 
 sqlQuery(con, "insert into List (`Wrong Attorney`, `Right Attorney`) select WrongAttorney,RightAttorney from temp;")
 sqlQuery(con, 'drop table temp;')
# now the database has been updated and now update the corresponding Right Attorney in d2s

 List <- sqlFetch(con, "List")
 List$`Wrong Attorney` = trimws(List$`Wrong Attorney`)
 List$`Right Attorney` = trimws(List$`Right Attorney`)
 a = which(d2s$C %in% List$`Wrong Attorney`)
 d2s$D[a] = as.character(sapply(d2s$C[a], f))
 
write.table(d2s, file = "Attorney.csv", sep = ',', row.names = F, col.names = F)

################# APN #########################
head(d3)
View(d3)

d3s = drop_and_rename(d3, drops)
head(d3s)

# trim all columns (including column D, E, F)
d3s = data.frame(apply(d3s, 2, trimws), stringsAsFactors = F)

### remove rows have no address (D)
# View(d3s1[order(d3s1$D),])
d3s$D = gsub("^$|^ *$", NA, d3s$D) #replace blanks address as NA
d3s = (d3s[!(is.na(d3s$D) | d3s$D==""), ]) # delete rows with address = NA


## remove duplicates 
dup = which(d3s$B %in% d3s[duplicated(d3s[,"B"]),]$B) ## duplicated records in B
dup.lev = d3s[duplicated(d3s[,"B"]),]$B #duplicated docket numbers

## remove these records without blk and lot numbers in duplicates
d3s[dup,c("G","H")] = apply(d3s[dup,c("G","H")], 2, function(x) gsub("^ *$", NA, x)) #replace blanks in G, H as NA
View(d3s[dup,])
d3s = d3s[!(is.na(d3s$G) | is.na(d3s$G)), ]

## remove these rocords with same blk and lot numbers  -- NOT COMPLETED
# dup in B, G, H 

d3sdupBGH = d3s[duplicated(d3s[,c("B","G","H")]) | duplicated(d3s[,c("B","G","H")],fromLast = T),]

d3snondup = d3s[!(duplicated(d3s[,c("B","G","H")]) | duplicated(d3s[,c("B","G","H")],fromLast = T)),]

## output these dups and clean it manually

write.xlsx(d3sdupBGH, file = "APNdups.xlsx",  col.names = F, row.names = F)

# after clean it, reload it

d3sdupBGH_cleaned = read.xlsx(file = "APNdups.xlsx", sheetIndex = 'Sheet1',stringsAsFactors = F,header = F)
d3sdupBGH_cleaned = drop_and_rename(d3sdupBGH_cleaned)

d3snew = rbind(d3snondup, d3sdupBGH_cleaned)

d3s = d3snew

## column E 

## fix all UNIT, APT, APTARTMENT in column E: paste it to column D
d3s$E = gsub("^ *$", NA, d3s$E)
#View(d3s[order(d3s$E),])
d3sE = d3s[!is.na(d3s$E),]
d3snoE = d3s[is.na(d3s$E),]

E = d3sE$E

appendE = function(x){
  return(length(grep("^(UNIT)|^(APT)|^(APARTMENT)",x)) == 1)
}

E1 = sapply(E, appendE, USE.NAMES = F)

for (i in 1:length(E1)){
  if (E1[i] == T) {
    d3sE$D[i] = paste(d3sE$D[i], E[i])
    d3sE$E[i] = NA
  }
}
#View(d3sE)

d3s = rbind(d3sE, d3snoE)
View(d3s)

## output all d3sE need to be manually fixed

d3sEnew = d3s[!is.na(d3s$E),]
d3s2 = d3s[is.na(d3s$E),]

write.xlsx(d3sEnew, file = "APNcolumnE.xlsx", col.names = F, row.names = F)

## import after fixed it
d3sEgood = read.xlsx(file = "APNcolumnE.xlsx", sheetIndex = "Sheet1", header = F)
d3sEgood = drop_and_rename(d3sEgood)
d3s = rbind(d3sEgood, d3s2)
d3s = data.frame(d3s, stringAsFactor = F)

# now the column E should be all NAs

## then update the county and town names and muni code

accessname2 = 'Fix Town Names.accdb'
folderpath2 = "C:/Users/Liangquan/Google Drive/GovPilot/Teams/Data Aggregation _ Integrity Team/New Jersey/Data Import/Required Files"
dbpath2 = paste(folderpath2, '/', accessname2, sep = '')

con2 = odbcConnectAccess2007(dbpath2)

sqlTables(con2, tableType = "TABLE")$TABLE_NAME

Key <- sqlFetch(con2, "Key")
str(Key)
# write.table(List,file= 'List.csv', sep = ",", row.names = F, col.names=F) # a list of all anttorney we have now

Key = as.data.frame(apply(Key, 2, trimws))
Key1 = Key[,c(2,5)]
Key1 = unique(Key1)

# update the attorney name on d2s
# which records in d2s in already in the List
b = which(d3s[,c("A","F")] %in% Key[,c("COUNTY","TOWN NAME")])

b1 = which(d3s[,c("F")] %in% Key[,c("TOWN NAME")])

# x should be a dataframe with 2 columns
f1 = function(x){
  n = which(Key$`TOWN NAME` == x[2] & Key$COUNTY == x[1])
  x = cbind(Key$COUNTY[n], Key$`TOWN NAME`[n])
  return(x)
}

bb = apply(d3s[,c("A","F")][b1,],1,f1)



bb = as.character(apply(d3s[,c("A","F")],f1))


#replace those records in d2s
d2s$D = NA

f = function(x){
  n = which(List$`Wrong Attorney` == x)
  x = List$`Right Attorney`[n] 
  return(x)
}
d2s$D[a] = as.character(sapply(d2s$C[a], f))

## combine D E F as address
head(d3s)
c1 = paste(paste(d3s$D, ifelse(!is.na(d3s$E), d3s$E, ''),sep = ''), d3s$F, 'NJ', sep = ',')
d3s$c1 = c1
c1s = c1[1:500]

c1 = paste(d3s$F,'NJ',sep = ",")
c1s = c1[1:500]
add = geocode(c1s,output = 'more',source = 'google')
addsub = add[,c("locality","administrative_area_level_2")]

View(add)
View(cbind(addsub,d3s[1:500,c("A","F")]))



OAKLAND BORO,Bergen,nj











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


