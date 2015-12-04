##　apn new  
# download the tax data for the whole nj state, extract the blk, lot, qual, property address, from the data

# example with atlantic county

setwd("C:/Users/Liangquan/Desktop/tax data")

filename = "Atlantic15.txt"

# read the txt file, return a dataframe
f = function(x) {
  x1 = substring(x, start, end)
  names(x1) = name
  return(x1)
}

output = function(filename) {
  # create a function to truncate the string with
  
  start = c(1, 5, 10, 14, 19, 23, 59, 176)
  end = c(4, 9, 13, 18, 22, 33, 83, 210)
  name = c("muni", "blk", "blk_suffix", "lot", "lot_suffix", "qual", "address", "owner")
  
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
nj$blk = as.character(as.numeric(nj$blk))
nj$lot = as.character(as.numeric(nj$lot))
nj$blk = gsub("( +)","", nj$blk)
nj$lot = gsub("( +)", "", nj$lot)
nj$blk_suffix = trimws(nj$blk_suffix)
nj$lot_suffix = trimws(nj$lot_suffix)
nj[which(nj$blk_suffix != ""),c("blk_suffix")] = paste(".", nj[which(nj$blk_suffix != ""),c("blk_suffix")], sep = "")
nj$lot_suffix[which(nj$lot_suffix != "")] = paste(".", nj$lot_suffix[which(nj$lot_suffix != "")], sep = "")

nj$blk = paste(nj$blk, nj$blk_suffix, sep = "")
nj$lot = paste(nj$lot, nj$lot_suffix, sep= "")

drops = c("blk_suffix","lot_suffix")
nj = nj[,!(names(nj) %in% drops)]

View(nj[1:1000,])

nj$lotqual = paste(nj$lot,nj$qual,sep = "")

write.table(nj, file = "nj.csv", sep = ",", row.names = F)
nj1 = read.csv("nj.csv")
## clean blk and lot data 

blk = d3s$G
lot = d3s$H

## clean blk
blkpattern1 = "(^\\d+\\b)(\\s+)([0]\\d+\\b)"
blkpattern2 = "(^\\d+[.]*\\d*)(\\s*)(((FKA)|(F/K/A)|(QUAL)|(AKA)|(A/K/A)|(FK)|(F/K)|(AND))(.*))"

blk1 = gsub(blkpattern1, "\\1.\\3", blk)
blk2 = gsub(blkpattern2, "\\1", blk1)

View(cbind(blk2, blk1, blk))
blkn = blk2

# clean lot$qual
View(cbind(lot3, lot2, lot1, lot))
lotpattern1 = "(F/K/A.*)|(AKA.*)|(A/K/A.*)|(FK.*)|(F/K.*)|(AND.*)|(&.*)|(THRU.*))|(:)|(QUAL)|(\\()|(\\))|[,]"
lotpattern2 = "(^\\d+\\b)(\\s+)([0]\\d+.*\\b)"
lotpattern3 = "(^\\d+\\b)(\\s+)(\\d+\\b)(\\s+)(\\d+\\b)"

lot1 = gsub(lotpattern1, "", lot)
lot2 = gsub(lotpattern2, "\\1.\\3", lot1)
lot3 = gsub(lotpattern3, "\\1", lot2)

lotn = gsub(" ", "", lot3)


## search blk

apn = cbind.data.frame(d3s$J, blkn, lotn)
names(apn) = c("muni","blk","lotnqual")
apn$blk.n = rep(NA)
# apn = apply(apn, 2, as.character) #?why return a matrix

f = function(x){
  a = which(x$blk %in% nj[which(nj$muni == x$muni),]$blk)
  x$blk.n[a] = as.character(x$blk[a])
  return(x)
}

test = apply(apn, 1, f)
