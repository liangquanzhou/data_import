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
nj$lot = as.character(as.numeric(nj$lot))
nj$p.blk = as.character(as.numeric(nj$p.blk))
nj$p.lot = as.character(as.numeric(nj$p.lot))

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



#names(apn) = c("muni","blk","lotnqual")



# apn = apply(apn, 2, as.character) #?why return a matrix

f = function(x){
  x = as.character(x)
  #a = x[2] %in% nj[which(nj$muni == x[1]),]$blk
  which(nj$muni == x[1] & nj$blk == x[2] & nj$lotqual == x[3])
  
  ifelse(x[2] %in% nj[which(nj$muni == x[1]),]$blk, x[c(4,5)] <- nj[which(nj$muni == x[1] & nj$blk == x[2]),][1,c("blk.raw", "blk_suffix")], x[c(4, 5)] <- NA)
  return(t(x))
}

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
