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