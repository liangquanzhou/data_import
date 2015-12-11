
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

write.table(d3s, file = "APN.csv",sep = ",", na = "", row.names = F, col.names = F)
write.csv(d3s, file = "APN.csv")
