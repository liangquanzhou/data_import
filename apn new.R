
d3s = drop_and_rename(d3, drops = c("B","C","F","G","H","I","M","N","O","P","Q","R"))
#head(d3s)

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
#View(d3s[dup,])
d3s = d3s[!(is.na(d3s$G) | is.na(d3s$G)), ]

# View(d3s)
d3s$J[d3s$J  == "1109"] = "1114"
d3s$J[d3s$J  == "1110"] = "1114"

# use docket number to refer the owner name in the plaintiff dataset
for (i in 1:dim(d3s)[1]){
  if (length(d1s$V9.2[which(d1s$V4 == d3s$B[i])]) != 1) print(i)
  d3s$owner_full[i] = d1s$V9.2[which(d1s$V4 == d3s$B[i])]
}

d3s$owner_first = gsub("(\\b\\S*\\b)(\\s.*)", "\\1", d3s$owner_full)
K
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

# -- connect to sql server database and write data into database
sqlHost <- "GP-4\\SQLEXPRESS"
sqlDatabase <- "NJ_property"
dsnString <- "driver={SQL Server};server=%s;database=%s;trusted_connection=true"
dsn <- sprintf(dsnString, sqlHost, sqlDatabase)
con<- odbcDriverConnect(dsn)


# -- search method:
# 1. if muni, blk, lot, address, owner in foreclosure data match exactly the muni, blk, lotqual,
#    means these are 'good' data, set indicator = 1, store in table1
# 2. if not perfect matched, set indicator = 2, store in table2
ptm <- proc.time()
table1 = data.frame()
table2 = data.frame()
for (i in 1:dim(d3s)[1]){
  a = c(d3s$J[i], blkn[i], lotn[i], paste(d3s$D[i], d3s$F[i], sep = ", "), d3s$C[i], d3s$B[i], d3s$owner_full[i], d3s$owner_first[i])
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
  tmp$owner_full = a[7]
  tmp$owner_first = a[8]

  if (tmp$indicator == 1){
    table1 = rbind(table1, tmp)
  } else table2 = rbind(table2, tmp)
}
proc.time() - ptm
# about 90 sec

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


table2.temp = table2 # backup
table2 = table2.temp

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

ptm <- proc.time()
for (i in 1:dim(table2)[1]){
  b = c(table2$rmuni[i], table2$raddress[i])
  query = sprintf("select * from muni%s where address = '%s'", b[1], b[2])
  tmp2 = sqlQuery(con, query, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket", "owner_full", "owner_first")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 4
  }
  # with >1 records: usually these records are big properties with same blk and 
  # lot but qual is different, output all of them for reference, and set 
  # indicator = 5
  #if (dim(tmp2)[1] > 1) {
  #  table2[i, "indicator"] = 5
  #  write.table(tmp2, file = "table5reference.xlsx", append = T, row.names = F, col.names = F, qmethod = "double")
    
  #}
}
proc.time() - ptm

table2.temp = table2 # backup
table2 = table2.temp

table4 = table2[table2$indicator == 4,]
# table5 = table2[table2$indicator == 5,]
table2 = table2[table2$indicator == 2,]

## table5 need to be manually checked, but luckly table5 has reference

## output table2 for manually check
# write.xlsx(table5, "table5.xlsx", col.names = F, row.names = F, showNA = F)

# -- many records in table2 seems hava good format of blk and lot
# -- another guess: blk and lot coulb correct but the muni is wrong
# search it in the whole nj tax data table 
# -- set indicator = 6 for wrong muni but other right

ptm <- proc.time()
for (i in 1:dim(table2)[1]){
  a = c(table2$rblk[i], table2$rlot[i], table2$raddress[i])
  query1 = sprintf("select * from njproperty where blk = '%s' and lotqual = '%s' and address like '!%s!'", a[1], a[2], a[3], a[4])
  query2 = gsub("!","%",query1)
  
  tmp2 = sqlQuery(con, query2, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket", "owner_full", "owner_first")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 6
  }
}
proc.time() - ptm # very slow about 170 sec

# this gonna take long time to run

table6 = table2[table2$indicator == 6,]
table2 = table2[table2$indicator == 2,]

table2.temp = table2 # backup
table2 = table2.temp

# -- match part of the address in table 2 and exact blk number

addpattern = "(\\b\\S+\\b)(\\s+)(\\b\\S+\\b)(\\s+).*"

ptm <- proc.time()
for (i in 1:dim(table2)[1]){
  a = c(table2$rblk[i], table2$raddress[i])
  part_add = gsub(addpattern, "\\1\\2\\3", table2$raddress[i])
  query1 = sprintf("select * from njproperty where blk = '%s' and address like '%s!'", a[1], part_add)
  query2 = gsub("!","%",query1)
  
  tmp2 = sqlQuery(con, query2, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket", "owner_full", "owner_first")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 7
  }
}
proc.time() - ptm # about 110 sec

# set fixed indicator = 7

table2.temp = table2 # backup
table2 = table2.temp

table7 = table2[table2$indicator == 7,]
table2 = table2[table2$indicator == 2,]

# -- muni is correct, blk or lotqual wrong, match part of = address

addpattern = "(\\b\\S+\\b)(\\s+)(\\b\\S+\\b)(\\s+).*"

ptm <- proc.time()
for (i in 1:dim(table2)[1]){
  a = c(table2$rmuni[i], table2$raddress[i])
  part_add = gsub(addpattern, "\\1\\2\\3", table2$raddress[i])
  query1 = sprintf("select * from njproperty where muni = '%s' and address like '%s!'", a[1], part_add)
  query2 = gsub("!","%",query1)
  
  tmp2 = sqlQuery(con, query2, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket", "owner_full", "owner_first")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 8
  }
}
proc.time() - ptm # about 60 sec

# set fixed indicator = 8 

table2.temp = table2 # backup
table2 = table2.temp

table8 = table2[table2$indicator == 8,]
table2 = table2[table2$indicator == 2,]

# -- lotqual correct, muni or blk wrongm match part of address

addpattern = "(\\b\\S+\\b)(\\s+)(\\b\\S+\\b)(\\s+).*"

ptm <- proc.time()
for (i in 1:dim(table2)[1]){
  a = c(table2$rlot[i], table2$raddress[i])
  part_add = gsub(addpattern, "\\1\\2\\3", table2$raddress[i])
  query1 = sprintf("select * from njproperty where lotqual = '%s' and address like '%s!'", a[1], part_add)
  query2 = gsub("!","%",query1)
  
  tmp2 = sqlQuery(con, query2, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket", "owner_full", "owner_first")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 9
  }
}
proc.time() - ptm # about 50 sec

# set fixed indicator = 9 

table2.temp = table2 # backup
table2 = table2.temp

table9 = table2[table2$indicator == 9,]
table2 = table2[table2$indicator == 2,]

## search the part of the address and the owner's first name

addpattern = "(\\b\\S+\\b)(\\s+)(\\b\\S+\\b)(\\s+).*"

ptm <- proc.time()
for (i in 1:dim(table2)[1]){
  a = c(table2$raddress[i], table2$owner_first[i])
  part_add = gsub(addpattern, "\\1\\2\\3", table2$raddress[i])
  query1 = sprintf("select * from njproperty where address like '%s!' and owner like '%s!'", part_add, a[2])
  query2 = gsub("!","%",query1)
  
  tmp2 = sqlQuery(con, query2, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket", "owner_full", "owner_first")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 10
  }
}
proc.time() - ptm # about 40 sec

table2.temp = table2 # backup
table2 = table2.temp

table10 = table2[table2$indicator == 10,]
table2 = table2[table2$indicator == 2,]

## -- some of address could spell wrong, only search the number of address and the muni, with the first name

addpattern2 = "(\\b\\S+\\b)(\\s+).*"

ptm <- proc.time()
for (i in 1:dim(table2)[1]){
  a = c(table2$rmuni[i], table2$raddress[i], table2$owner_first[i])
  part_add = gsub(addpattern2, "\\1", table2$raddress[i])
  query1 = sprintf("select * from njproperty where muni like '%s' and address like '%s!' and owner like '%s!'", a[1], part_add, a[3])
  query2 = gsub("!","%",query1)
  
  tmp2 = sqlQuery(con, query2, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket", "owner_full", "owner_first")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 11
  }
}
proc.time() - ptm # about 30 sec

table2.temp = table2 # backup
table2 = table2.temp

table11 = table2[table2$indicator == 11,]
table2 = table2[table2$indicator == 2,]


# address also could be wrong, so use the search in 11, but use blk instead of muni

addpattern2 = "(\\b\\S+\\b)(\\s+).*"

ptm <- proc.time()
for (i in 1:dim(table2)[1]){
  a = c(table2$rblk[i], table2$raddress[i], table2$owner_first[i])
  part_add = gsub(addpattern2, "\\1", table2$raddress[i])
  query1 = sprintf("select * from njproperty where blk like '%s' and address like '%s!' and owner like '%s!'", a[1], part_add, a[3])
  query2 = gsub("!","%",query1)
  
  tmp2 = sqlQuery(con, query2, as.is = T, na.strings = "", stringsAsFactors = F)
  if (dim(tmp2)[1] == 1) {
    drops = c("indicator", "rmuni", "rblk", "rlot", "raddress", "ryear", "docket", "owner_full", "owner_first")
    table2[i,!(names(table2) %in% drops)] = tmp2[1,]
    table2[i, "indicator"] = 12
  }
}
proc.time() - ptm # about 30 sec

table2.temp = table2 # backup
table2 = table2.temp

table12 = table2[table2$indicator == 12,]
table2 = table2[table2$indicator == 2,]

# now the table2 is impossible to imporve anymore...

## gather all data we have 

table = rbind(table1, table4, table6, table7, table8, table9, table10, table11, table12)

write.table(table, "good_apn.csv", sep = ",", row.names = F, col.names = T, qmethod = "double", na = "")
write.table(table2, "bad_apn.csv", sep = ",", row.names = F, col.names = T, qmethod = "double", na = "")

