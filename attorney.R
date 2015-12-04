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
# folderpath = "C:/Users/Liangquan/Google Drive/GovPilot/Teams/Data Aggregation _ Integrity Team/New Jersey/Data Import/Required Files"
 folderpath = getwd()
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
write.table(d2s12, 'update attorney.csv', col.names = F, row.names = F, sep = ",",na = "update here")

################################################################################
# then manually update the attorney info at update attorney.csv file and save it
################################################################################
