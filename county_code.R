library(tidyr)
library(stringr)
muni = read.table("http://www.state.nj.us/treasury/revenue/revmuni.txt", sep = '\n', row.names = NULL, fill = T, strip.white = T)

m2 = sub("(\\W)+(\\s)([0-9])", "\\1\\2;\\3", muni[,1])

###
### muni2 = sub("(\\s)([0-9])", "\\1;\\2", muni[,1])
### muni3 = sub("[^.]", "", muni[,2])
###

muni$V2 = m2

muni = separate(muni, V2, into  = c("V2.1", "V2.2"), sep = ';')

n = c(which(is.na(muni$V2.2 == T)))
c = muni$V2.1[n]
t = diff(append(n, length(muni$V2.2)+1))


muni$V1 = rep(c,times = t)
muni = na.omit(muni)

m1 = muni$V1
m1s = sub("County", "", m1)
muni$V1 = m1s

muni$V1 = trimws(str_to_upper(muni$V1))
muni$V2.1 = trimws(str_to_upper(muni$V2.1))

names(muni) = c('County', 'Township', 'Code')

View(muni)
