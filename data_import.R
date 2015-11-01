### set work dir ###
setwd("C:/Users/Liangquan/Google Drive/GovPilot/Teams/Data Aggregation _ Integrity Team/New Jersey/Data Import/PAE0601M 07_31_2015  22_05_20")


############ packages needed ##############
pckg = c('tidyr', 'stringi', 'data.table', 'reshape2', 'plyr', 'choroplethrMaps', 'choroplethr', 'RODBC', 'zipcode') 

is.installed <- function(mypkg){
  is.element(mypkg, installed.packages()[,1])
} 

for(i in 1:length(pckg)) {
  if (!is.installed(pckg[i])){
    install.packages(pckg[i])
  }
  library(pckg[i], character.only = T)
}



######## Read data ########

fc = file('pampac0601.txt')
mylist = strsplit(readLines(fc), ";")
close(fc)

l1 = lapply(mylist, as.data.frame)
l2 = lapply(l1, t)
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
d2s$D = trimws(d2s$C)
d2s = d2s[!(is.na(d2s$D) | d2s$D==""), ]
# View(d2s[order(d2s$D),])
drops = c("D")
d2s = d2s[,!(names(d2s) %in% drops)]


## drop duplicates ##
d2s = d2s[!duplicated(d2s[,"B"]),]

#### connect to acess database ###
accessname = 'Fix Attorneys.accdb'
dbpath = paste(getwd(), '/', accessname, sep = '')

con = odbcConnectAccess2007(dbpath)

sqlTables(con, tableType = "TABLE")$TABLE_NAME

Att <- sqlFetch(con, "List")
str(Att)
View(Att)




write.table(d2s, file = "Attorney.csv", sep = ',', row.names = F, col.names = F)

################# APN #########################
head(d3)
View(d3)

names(d3) = LETTERS[1:length(names(d3))]

drops = c('B', 'C', 'F', 'G', 'H', 'I', 'M', 'N', 'O', 'P', 'Q', 'R')
d3s = d3[,!(names(d3) %in% drops)]
View(d3s)

names(d3s) = LETTERS[1:length(names(d3s))]
View(d3s[order(d3s$D),])

# trim column D, E, F
d3s$D = trimws(d3s$D)
d3s$E = trimws(d3s$E)
d3s$F = trimws(d3s$F)

d3s = d3s[!(is.na(d3s$D) | d3s$D==""), ]
View(d3s)

### read mulicipalities code data ###
m = read.table('file:///C:/Users/Liangquan/Desktop/revmuni.txt', sep = '', col.names = paste0('V', seq_len(3)), fill = T)
View(m)


muni = file('revmuni.txt')
munilist = strsplit(readLines(muni), ";")
close(muni)

m1 = lapply(munilist, as.data.frame)

######################


