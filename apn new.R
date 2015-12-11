

# -- set the path to the tax data to import the data
# for pc
setwd("C:/Users/Liangquan/Desktop/tax data") 
# for mac
setwd('/Users/liangquanzhou/Documents/data/nj tax data/')

# -- function: read the txt file, return a dataframe
f = function(x) {
  x1 = substring(x, startnum, endnum)
  names(x1) = name
  return(x1)
}

# -- refer to the tax data dictionary, select all the columns we need and name it 
# _s means suffix, p means previous
startnum = c(1, 5, 10, 14, 19, 23, 59, 176, 522, 527, 531, 536, 540)
endnum = c(4, 9, 13, 18, 22, 33, 83, 210, 526, 530, 535, 539, 550)
name = c("muni", "blk", "blk_s", "lot", "lot_s", "qual", "address", 
         "owner", "p_blk","p_blk_s","p_lot", "p_lot_s","p_qual")

# -- function: in the folder of data, input the filename then import 
#              data in that file 
output = function(filename) {
  con = file(filename)
  l = strsplit(readLines(con), "\n")
  close(con)
  l1 = lapply(l, f)
  l2 = lapply(l1, t)
  d1 = do.call("rbind", l2)
  d2 = as.data.frame(d1, stringsAsFactors = F)
  return(d2)
}

# -- list of every county's data
county = list()

# -- loop: go through every filename in the folder and generate data 
#          for every county
for (i in 1:length(dir())) {
  county[[i]] = output(dir()[i])
}

# -- complie every county data to nj data
nj = rbindlist(county)
nj = apply(nj, 2, as.character) # set as character
nj = apply(nj, 2, trimws) # trim the beginning and ending spaces
nj = as.data.frame(nj, stringsAsFactors = F)

# -- create backup dataset incase messed up
nj.temp = nj
nj = nj.temp

# -- clean up blk and lot:
# columns we have: blk, blk suffix, lot, lot suffix, qual AND:
#                  previous blk, previous blk suffix, etc.
# combine blk and blk_s as blkn, lot and lot_s as lotn, previous ones as well

# -- create back up to install raw blk and lot.
nj$blk_raw = nj$blk
nj$lot_raw = nj$lot
nj$p_blk_raw = nj$p_blk
nj$p_lot_raw = nj$p_lot

# -- trim and set NA as ""(blank)
nj$blk = as.character(as.numeric(nj$blk))
nj$blk[is.na(nj$blk)] = ""
nj$lot = as.character(as.numeric(nj$lot))
nj$lot[ia.na(nj$lot)] = ""
nj$p_blk = as.character(as.numeric(nj$p_blk))
nj$p_blk[is.na(nj$p_blk)] = ""
nj$p_lot = as.character(as.numeric(nj$p_lot))
nj$p_lot[is.na(nj$p_lot)] = ""

# -- incase of any space in the middle, drop them
nj$blk = gsub("( +)", "", nj$blk)
nj$lot = gsub("( +)", "", nj$lot)
nj$p_blk = gsub("( +)", "", nj$p_blk)
nj$p_lot = gsub("( +)", "", nj$p_lot)

# -- trim blk_s, lot_s, for combine blk and blk_s, etc.
nj$blk_s = trimws(nj$blk_s)
nj$lot_s = trimws(nj$lot_s)
nj$p_blk_s = trimws(nj$p_blk_s)
nj$p_lot_s = trimws(nj$p_lot_s)
nj$qual = trimws(nj$qual)
nj$p_qual = trimws(nj$p_qual)

# -- for exist suffix, then append it to the blk or lot.
nj$blk[which(nj$blk_s != "")] = paste(nj$blk[which(nj$blk_s != "")], ".", sep = "")
nj$lot[which(nj$lot_s != "")] = paste(nj$lot[which(nj$lot_s != "")], ".", sep = "")
nj$p_blk[which(nj$p_blk_s != "")] = paste(nj$p_blk[which(nj$p_blk_s != "")], ".", sep = "")
nj$p_lot[which(nj$p_lot_s != "")] = paste(nj$p_lot[which(nj$p_lot_s != "")], ".", sep = "")

# -- updata blk, lot with suffix
nj$blk = paste(nj$blk, nj$blk_s, sep = "")
nj$lot = paste(nj$lot, nj$lot_s, sep = "")
nj$p_blk = paste(nj$p_blk, nj$p_blk_s, sep = "")
nj$p_lot = paste(nj$p_lot, nj$p_lot_s, sep = "")

# -- combine lot and qual, since it's combined in foreclosure data, we will use it to search
nj$lotqual = paste(nj$lot,nj$qual,sep = "")
nj$p_lotqual = paste(nj$p_lot, nj$p_qual, sep = "")

# -- then the nj tax data is done, store it in database
#####################################
# for sql server:
# -- connect to sql server database and write data into database
sqlHost <- "GP-4\\SQLEXPRESS"
sqlDatabase <- "NJ_property"
dsnString <- "driver={SQL Server};server=%s;database=%s;trusted_connection=true"
dsn <- sprintf(dsnString, sqlHost, sqlDatabase)
con<- odbcDriverConnect(dsn)

# -- save the data as a table 

sqlSave(con, nj, tablename = "njproperty", rownames = F, colnames = F, fast = T) # ~ 900 seconds to run

# -- for quicker search, split the nj table for every county, corresponding to its muni code
muniv = sqlQuery(con, "select distinct muni from njproperty", as.is = T)

# problem muni codes: 0288, 1109, 1110, 1300, 1500, 2118

ptm <- proc.time()
for (i in 1:length(muniv[,1])){
  query = sprintf("select * into muni%s from njproperty where muni = '%s'", muniv[i,1], muniv[i,1])
  sqlQuery(con, query)
}
proc.time() - ptm
# ~ 140 seconds

#####################################
# for mysql:
# -- connect to mysql database and write data into database
install.packages("RMySQL")
library(RMySQL)
library(psych)

con <- dbConnect(MySQL(),
                 user = 'root',
                 password = '',
                 host = '',
                 dbname='NJ_property')
dbWriteTable(conn = con, name = 'njproperty', value = as.data.frame(nj))



# -- clean up the blk and lot column in d3s
blk = d3s$G
lot = d3s$H

# -- some patterns to clean blk
# 1. numbers + space + 0 + number, usually the space should be a dot,
#    e.g. 34 01 should be 34.01
blkpattern1 = "(^\\d+\\b)(\\s+)([0]\\d+\\b)"
# 2. some unuseful words or letters, like FKA, AKA, etc., drop every thing
#    after these words
blkpattern2 = "(^\\d+[.]*\\d*)(\\s*)(((FKA)|(F/K/A)|(QUAL)|
(AKA)|(A/K/A)|(FK)|(F/K)|(AND))(.*))"

blk1 = gsub(blkpattern1, "\\1.\\3", blk)
blk2 = gsub(blkpattern2, "\\1", blk1)

# -- blk column done
blkn = blk2
blkn = trimws(blkn)

# -- some patterns to clean lot
# 1. same as blk
lotpattern1 = "(F/K/A.*)|(AKA.*)|(A/K/A.*)|(FK.*)|(F/K.*)|(AND.*)|
(&.*)|(THRU.*))|(:)|(QUAL)|(\\()|(\\))|[,]"
# 2. same as blk
lotpattern2 = "(^\\d+\\b)(\\s+)([0]\\d+.*\\b)"
# 3. with 3 numbers, e.g. 11 12 13, only keep the first number. 
lotpattern3 = "(^\\d+\\b)(\\s+)(\\d+\\b)(\\s+)(\\d+\\b)"
# 4. numbers + space + letters
lotpattern4 = "(\\d+\\b)(\\s+)(\\b[A-Za-z])"

lot1 = gsub(lotpattern1, "", lot)
lot2 = gsub(lotpattern2, "\\1.\\3", lot1)
lot3 = gsub(lotpattern3, "\\1", lot2)
lot4 = gsub(lotpattern4, "\\1\\3", lot3)

# -- lot column done
lotn = lot4
lotn = trimws(lotn)

# -- search method:
# 1. if muni, blk, lot in foreclosure data match exactly the muni, blk, lotqual,
#    means these are 'good' data, set indicator = 1, store in table1
# 2. if not perfect matched, set indicator = 2, store in table2
ptm <- proc.time()
table1 = data.frame()
table2 = data.frame()
for (i in 1:dim(d3s)[1]){
  a = c(d3s$J[i], blkn[i], lotn[i], paste(d3s$D[i], d3s$F[i], sep = ", "), d3s$C[i], d3s$B[i])
  query1 = sprintf("select * from muni%s where blk = '%s' and lotqual = '%s'", a[1], a[2], a[3])
  query2 = sprintf("select * from muni%s where p_blk = '%s' and p_lotqual = '%s'", a[1], a[2], a[3])
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


# -- with table2: indicator = 2 
# guess: maybe the muni and blk are correct, only the lot&qual is wrong
# remedy: mathch the muni and blk, if have returns, those returns have address
#         use google geocode tools to search the formatted address of 
#         these returns, and also use google geocode tools the formatted 
#         address of the property in table2, if they are exact the same, 
#         use the lot&qual in the matched records to replace the lot&qual of 
#         the property

#ptm <- proc.time()
#for (i in 1:dim(table2)[1]){
#  b = c(table2$rmuni[i], table2$rblk[i])
#  query = sprintf("select * from muni%s where blk = '%s'", b[1], b[2])
#  tmp2 = sqlQuery(con, query, as.is = T, na.strings = "", stringsAsFactors = F)
#  dim1 = dim(tmp2)[1]
#  addtmp = geocode(table2$raddress[i], output = "more", source = "google")
#  # -- only search those has returns < 10
#  if (dim1 < 10 & !is.null(addtmp$address)){
#    addv = geocode(paste(tmp2$address,'nj',sep = ','), output = "more", source = "google")
#    if (addtmp$address %in% addv$address) {
#      drops = c("indicator", "rmuni", "rblk", "rlot", "raddress","ryear","docket")
#      table2[i,!(names(table2) %in% drops)] = tmp2[addv$address == addtmp$address,]
#      table2[i, "indicator"] = 3
#      }
#  }
#}
#proc.time() - ptm 

# this could cleaned 10 - 15% of data in table2

# -- store those google geocode search matching records with indicator = 3
#table3 = table2[table2$indicator == 3,]
#table2 = table2[table2$indicator == 2,]


# table2.temp = table2 # backup
#table2 = table2.temp

# -- another guess: muni is correct, but blk could be wrong
# in this way we can not use geocode tools. 
# try to format the address in table2 and search it 

# -- format the addresses in table2

raddress = table2$raddress

raddress = gsub("\\bAVENUE\\b","AVE", raddress)
raddress = gsub("\\bCOURT\\b","CT", raddress)
raddress = gsub("\\bROAD\\b","RD", raddress)
raddress = gsub("\\bSTREET\\b","ST", raddress)
raddress = gsub("\\bDRIVE\\b","DR", raddress)
raddress = gsub("\\bLANE\\b","LN", raddress)
raddress = gsub("\\bPLACE\\b","PL", raddress)
raddress = gsub("\\bCIRCLE\\b","CI", raddress)
raddress = gsub("\\bPOINT\\b","PT", raddress)
raddress = gsub("\\bBOULEVARD\\b","BLVD", raddress)

raddress = gsub("(.*),(.*)","\\1", raddress) 
raddress = gsub("\\bNORTH\\b","N", raddress)
raddress = gsub("\\bSOUTH\\b","S", raddress)
raddress = gsub("\\bEAST\\b","E", raddress)
raddress = gsub("\\bWEST\\b","W", raddress)

table2$raddress = raddress

# -- match the formatted address, if there is a unique record in that county
#    matched the formatted address, use it and set indicator = 4
# -- then output all the data with indicator = 2, means we have to do it manually.

for (i in 1:dim(table2)[1]){
  b = c(table2$rmuni[i], table2$raddress[i])
  query = sprintf("select * from muni%s where address = '%s'", b[1], b[2])
  tmp2 = sqlQuery(con, query, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 4
  }
  # with >1 records: usually these records are big properties with same blk and 
  # lot but qual is different, output all of them for reference, and set 
  # indicator = 5
  if (dim(tmp2)[1] > 1) {
    table2[i, "indicator"] = 5
    write.table(tmp2, file = "table5reference.xlsx", append = T, row.names = F, col.names = F, qmethod = "double")
    
  }
}

table4 = table2[table2$indicator == 4,]
table5 = table2[table2$indicator == 5,]
table2 = table2[table2$indicator == 2,]

## table5 need to be manually checked, but luckly table5 has reference

## output table2 for manually check
write.xlsx(table5, "table5.xlsx", col.names = F, row.names = F, showNA = F)

# -- many records in table2 seems hava good format of blk and lot
# -- another guess: blk and lot coulb correct but the muni is wrong
# search it in the whole nj tax data table 
# -- set indicator = 6 for wrong muni but other right

for (i in 1:dim(table2)[1]){
  a = c(table2$rblk[i], table2$rlot[i], table2$raddress[i])
  query1 = sprintf("select * from njproperty where blk = '%s' and lotqual = '%s' and address like '!%s!'", a[1], a[2], a[3], a[4])
  query2 = gsub("!","%",query1)
  
  tmp2 = sqlQuery(con, query2, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 6
  }
}
# this gonna take long  time to run

table6 = table2[table2$indicator == 6,]
table2 = table2[table2$indicator == 2,]

# -- match part of the address in table 2 and exact blk number

addpattern = "(\\b.*\\b)(\\s+)(\\b.*\\b)(\\s+).*"

for (i in 1:dim(table2)[1]){
  a = c(table2$rblk[i], table2$raddress[i])
  part_add = gsub(addpattern, "\\1\\2\\3", table2$raddress[i])
  query1 = sprintf("select * from njproperty where blk = '%s' and address like '%s!'", a[1], part_add)
  query2 = gsub("!","%",query1)
  
  tmp2 = sqlQuery(con, query2, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 7
  }
}

# set fixed indicator = 7

table7 = table2[table2$indicator == 7,]
table2 = table2[table2$indicator == 2,]

# -- muni is correct, blk or lotqual wrong, match part of = address

addpattern = "(\\b.*\\b)(\\s+)(\\b.*\\b)(\\s+).*"

for (i in 1:dim(table2)[1]){
  a = c(table2$rmuni[i], table2$raddress[i])
  part_add = gsub(addpattern, "\\1\\2\\3", table2$raddress[i])
  query1 = sprintf("select * from njproperty where muni = '%s' and address like '%s!'", a[1], part_add)
  query2 = gsub("!","%",query1)
  
  tmp2 = sqlQuery(con, query2, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 8
  }
}

# set fixed indicator = 8 

table8 = table2[table2$indicator == 8,]
table2 = table2[table2$indicator == 2,]

# -- lotqual correct, muni or blk wrongm match part of address

addpattern = "(\\b.*\\b)(\\s+)(\\b.*\\b)(\\s+).*"

for (i in 1:dim(table2)[1]){
  a = c(table2$rlot[i], table2$raddress[i])
  part_add = gsub(addpattern, "\\1\\2\\3", table2$raddress[i])
  query1 = sprintf("select * from njproperty where lotqual = '%s' and address like '%s!'", a[1], part_add)
  query2 = gsub("!","%",query1)
  
  tmp2 = sqlQuery(con, query2, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 9
  }
}
# set fixed indicator = 9 

table9 = table2[table2$indicator == 9,]
table2 = table2[table2$indicator == 2,]







table134 = rbind(table1, table3, table4)
write.xlsx(table134, "table134.xlsx", col.names = F, row.names = F, showNA = F)













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
