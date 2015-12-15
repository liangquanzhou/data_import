

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
#install.packages("RMySQL")
#library(RMySQL)
#library(psych)

#con <- dbConnect(MySQL(),
#                 user = 'root',
#                 password = '',
#                 host = '',
#                 dbname='NJ_property')
#dbWriteTable(conn = con, name = 'njproperty', value = as.data.frame(nj))
