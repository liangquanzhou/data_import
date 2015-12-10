##　apn new  
# download the tax data for the whole nj state, extract the blk, lot, qual, property address, from the data

# example with atlantic county

setwd("C:/Users/Liangquan/Desktop/tax data")

filename = "Atlantic15.txt"
con = file(filename)

# read the txt file, return a dataframe
f = function(x) {
  x1 = substring(x, startnum, endnum)
  names(x1) = name
  return(x1)
}

startnum = c(1, 5, 10, 14, 19, 23, 59, 176, 522, 527, 531, 536, 540)
endnum = c(4, 9, 13, 18, 22, 33, 83, 210, 526, 530, 535, 539, 550)
name = c("muni", "blk", "blk.s", "lot", "lot.s", "qual", "address", "owner", "p.blk","p.blk.s","p.lot", "p.lot.s","p.qual")


output = function(filename) {
  # create a function to truncate the string with
  
  con = file(filename)
  l = strsplit(readLines(con), "\n")
  close(con)
  l1 = lapply(l, f)
  l2 = lapply(l1, t)
  d1 = do.call("rbind", l2)
  d2 = as.data.frame(d1, stringsAsFactors = F)
  return(d2)
}

#atlantic = output("Atlantic15.txt")
county = list()

for (i in 1:length(dir())) {
  county[[i]] = output(dir()[i])
}

nj = rbindlist(county)
nj = apply(nj, 2, as.character)
nj = apply(nj, 2, trimws)

nj = as.data.frame(nj, stringsAsFactors = F)

nj.temp = nj
nj = nj.temp



# bind the blk and blk suffix, lot and lot suffix
#nj$blk_suffix = gsub("^$|^ *$", "", nj$blk_suffix)
#nj$lot_suffix = gsub("^$|^ *$", "", nj$lot_suffix)
#nj$qual = gsub("^$|^ *$", "", nj$qual)

nj$blk.raw = nj$blk
nj$lot.raw = nj$lot
nj$p.blk.raw = nj$p.blk
nj$p.lot.raw = nj$p.lot

nj$blk = as.character(as.numeric(nj$blk))
nj$blk[is.na(nj$blk)] = ""
nj$lot = as.character(as.numeric(nj$lot))
nj$lot[ia.na(nj$lot)] = ""
nj$p.blk = as.character(as.numeric(nj$p.blk))
nj$p.blk[is.na(nj$p.blk)] = ""
nj$p.lot = as.character(as.numeric(nj$p.lot))
nj$p.lot[is.na(nj$p.lot)] = ""

nj$blk = gsub("( +)","", nj$blk)
nj$lot = gsub("( +)", "", nj$lot)
nj$p.blk = gsub("( +)","", nj$p.blk)
nj$p.lot = gsub("( +)", "", nj$p.lot)

nj$blk.s = trimws(nj$blk.s)
nj$lot.s = trimws(nj$lot.s)
nj$p.blk.s = trimws(nj$p.blk.s)
nj$p.lot.s = trimws(nj$p.lot.s)

nj$qual = trimws(nj$qual)
nj$p.qual = trimws(nj$p.qual)

nj$blk[which(nj$blk.s != "")] = paste(nj$blk[which(nj$blk.s != "")], ".", sep = "")
nj$lot[which(nj$lot.s != "")] = paste(nj$lot[which(nj$lot.s != "")], ".", sep = "")
nj$p.blk[which(nj$p.blk.s != "")] = paste(nj$p.blk[which(nj$p.blk.s != "")], ".", sep = "")
nj$p.lot[which(nj$p.lot.s != "")] = paste(nj$p.lot[which(nj$p.lot.s != "")], ".", sep = "")

nj$blk = paste(nj$blk, nj$blk.s, sep = "")
nj$lot = paste(nj$lot, nj$lot.s, sep = "")
nj$p.blk = paste(nj$p.blk, nj$p.blk.s, sep = "")
nj$p.lot = paste(nj$p.lot, nj$p.lot.s, sep = "")

#drops = c("blk.s","lot.s")
#nj = nj[,!(names(nj) %in% drops)]
#View(nj[1:1000,])

nj$lotqual = paste(nj$lot,nj$qual,sep = "")
nj$p.lotqual = paste(nj$p.lot, nj$p.qual, sep = "")

#write.table(nj, file = "nj.csv", sep = ",", row.names = F)
#nj1 = read.csv("nj.csv")
## clean blk and lot data 

blk = d3s$G
lot = d3s$H

## clean blk
blkpattern1 = "(^\\d+\\b)(\\s+)([0]\\d+\\b)"
blkpattern2 = "(^\\d+[.]*\\d*)(\\s*)(((FKA)|(F/K/A)|(QUAL)|(AKA)|(A/K/A)|(FK)|(F/K)|(AND))(.*))"

blk1 = gsub(blkpattern1, "\\1.\\3", blk)
blk2 = gsub(blkpattern2, "\\1", blk1)

#View(cbind(blk2, blk1, blk))
blkn = blk2

# clean lot$qual
#View(cbind(lot3, lot2, lot1, lot))
lotpattern1 = "(F/K/A.*)|(AKA.*)|(A/K/A.*)|(FK.*)|(F/K.*)|(AND.*)|(&.*)|(THRU.*))|(:)|(QUAL)|(\\()|(\\))|[,]"
lotpattern2 = "(^\\d+\\b)(\\s+)([0]\\d+.*\\b)"
lotpattern3 = "(^\\d+\\b)(\\s+)(\\d+\\b)(\\s+)(\\d+\\b)"

lot1 = gsub(lotpattern1, "", lot)
lot2 = gsub(lotpattern2, "\\1.\\3", lot1)
lot3 = gsub(lotpattern3, "\\1", lot2)

lotn = gsub(" ", "", lot3)


## search blk

apn = cbind(d3s$J, blkn, lotn)
apn = as.data.frame(apn, stringsAsFactors = F)
apn$blk.raw = rep(NA)
apn$blk.suffix.raw = rep(NA)
apn$lot.raw = rep(NA)
apn$lot.suffix.raw = rep(NA)
apn$qual = rep(NA)
apn$address = rep(NA)
apn = cbind(apn, d3s$D, d3s$F)


library(RODBC)





# connect to database and write data into database
sqlHost <- "GP-4\\SQLEXPRESS"
sqlDatabase <- "NJ_property"
dsnString <- "driver={SQL Server};server=%s;database=%s;trusted_connection=true"
dsn <- sprintf(dsnString, sqlHost, sqlDatabase)
con<- odbcDriverConnect(dsn)

#sqlSave(con, nj, tablename = "njproperty", rownames = F, colnames = F, fast = T)

nj0101 = sqlQuery(con, "select * from njproperty where muni = '0101'")

sqlQuery(con, "select * into muni0101 from njproperty where muni = '0101'")

muniv = read.table("muni.csv",as.is = T, sep = ",", colClasses = "character")

ptm <- proc.time()
for (i in 1:length(muniv[,1])){
  query = sprintf("select * into muni%s from njproperty where muni = '%s'", muniv[i,1], muniv[i,1])
  sqlQuery(con, query)
}
proc.time() - ptm



ptm <- proc.time()
table1 = data.frame()
table2 = data.frame()
for (i in 1:dim(d3s)[1]){
  a = c(d3s$J[i], blkn[i], lotn[i], paste(d3s$D[i], d3s$F[i], sep = ", "), d3s$C[i], d3s$B[i])
  query1 = sprintf("select * from muni%s where blk = '%s' and lotqual = '%s'", a[1], a[2], a[3])
  query2 = sprintf("select * from muni%s where pblk = '%s' and plotqual = '%s'", a[1], a[2], a[3])
  query = sprintf("if exists (%s) begin %s end else begin %s end", query1, query1, query2)
  tmp = sqlQuery(con, query, as.is = T, na.strings = "", stringsAsFactors = F)
  dim1 = dim(tmp)[1]
  dim2 = dim(tmp)[2]
  
  if (dim1 == 0) {
  tmp[1,] = rep(NA, dim2)
  tmp$indicator = 2
  } else tmp$indicator = 1
  tmp$rmuni = a[1]
  tmp$rblk = a[2]
  tmp$rlot = a[3]
  tmp$raddress = a[4]
  tmp$ryear = a[5]
  tmp$docket = a[6]

  if (tmp$indicator == 1){
    table1 = rbind(table1, tmp)
  } else table2 = rbind(table2, tmp)
}
proc.time() - ptm



# get issued records i.e. table2
# match the muni and blk, geocode the address of raw data and the returned data frame, see if the address match
ptm <- proc.time()
for (i in 1:dim(table2)[1]){
  b = c(table2$rmuni[i], table2$rblk[i])
  query = sprintf("select * from muni%s where blk = '%s'", b[1], b[2])
  tmp2 = sqlQuery(con, query, as.is = T, na.strings = "", stringsAsFactors = F)
  dim1 = dim(tmp2)[1]
  addtmp = geocode(table2$raddress[i], output = "more", source = "google")
  
  if (dim1 < 10 & !is.null(addtmp$address)){
    addv = geocode(paste(tmp2$address,'nj',sep = ','), output = "more", source = "google")
    if (addtmp$address %in% addv$address) {
      drops = c("indicator", "rmuni", "rblk", "rlot", "raddress","ryear","docket")
      table2[i,!(names(table2) %in% drops)] = tmp2[addv$address == addtmp$address,]
      table2[i, "indicator"] = 3
      }
  }
}
proc.time() - ptm

# this cleaned 10 - 15% of data, which lot is correct but lot&qual is not correct

#table1.temp = table1
table1 = table1.temp
#table2.temp = table2
table2 = table2.temp

table3 = table2[table2$indicator == 3,]
table2 = table2[table2$indicator == 2,]



raddress = table2$raddress
### then try to clean a little bit about the address and try to match the exact address -- 4

raddress = gsub("\\bAVENUE\\b","AVE", raddress)
raddress = gsub("\\bCOURT\\b","CT", raddress)
raddress = gsub("\\bROAD\\b","RD", raddress)
raddress = gsub("\\bSTREET\\b","ST", raddress)
raddress = gsub("\\bDRIVE\\b","DR", raddress)
raddress = gsub("\\bLANE\\b","LN", raddress)
raddress = gsub("\\bPLACE\\b","PL", raddress)
raddress = gsub("(.*),(.*)","\\1", raddress)
raddress = gsub("\\bNORTH\\b","N", raddress)
raddress = gsub("\\bSOUTH\\b","S", raddress)


table2$raddress = raddress



for (i in 1:dim(table2)[1]){
  b = c(table2$rmuni[i], table2$raddress[i])
  query = sprintf("select * from muni%s where address = '%s'", b[1], b[2])
  tmp2 = sqlQuery(con, query, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 4
  }
  if (dim(tmp2)[1] > 1) {
    write.table(tmp, file = "table2.csv", append = T, sep = ",", row.names = F, col.names = F)
  }
}

table4 = table2[table2$indicator == 4,]
table2 = table2[table2$indicator == 2,]

## if not only one return, table2 are records need be manually checked

## output table2 for manually check
write.xlsx(table2, "table2.xlsx", col.names = F, row.names = F, showNA = F)


# apn = apply(apn, 2, as.character) #?why return a matrix














f = function(x){
  x = as.character(x)
  #a = x[2] %in% nj[which(nj$muni == x[1]),]$blk
  a = which(nj$muni == x[1] & nj$blk == x[2] & nj$lotqual == x[3])
  b = which(nj$muni == x[1] & nj$p.blk == x[2] & nj$p.lotqual == x[3])
  if (length(a) == 1) {
    nj$blk.raw[a] -> x[4]
    nj$blk.s[a] -> x[5]
    nj$lot.raw[a] -> x[6]
    nj$lot.s[a] -> x[7]
    nj$qual[a] -> x[8]
    nj$address[a] -> x[9]
  } else if (length(b) == 1) {
    nj$blk.raw[b] -> x[4]
    nj$blk.s[b] -> x[5]
    nj$lot.raw[b] -> x[6]
    nj$lot.s[b] -> x[7]
    nj$qual[b] -> x[8]
    nj$address[b] -> x[9]
  }
  return(x)
}

test = apply(apn[1:300,], 1, f)
test = t(test)
View(test1)
