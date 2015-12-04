## fix the attorney table and update it in the access database

d2s2 = read.table(file = 'update attorney.csv',sep = ',')
d2s2 = drop_and_rename(d2s2)
names(d2s2) = c("Wrong Attorney","Right Attorney")
# d2s2$ID = seq(from = length(List$ID)+1,length.out = length(d2s2$ID))

# sqlQuery(con, "create table temp(ID int, `Wrong Attorney` varchar(255),`Right Attorney` varchar(255), PRIMARY KEY (ID));")
sqlSave(con, d2s2, tablename = "temp")

sqlQuery(con, "insert into List (`Wrong Attorney`, `Right Attorney`) select WrongAttorney,RightAttorney from temp;")
sqlQuery(con, 'drop table temp;')
# now the database has been updated and now update the corresponding Right Attorney in d2s

List <- sqlFetch(con, "List")
List$`Wrong Attorney` = trimws(List$`Wrong Attorney`)
List$`Right Attorney` = trimws(List$`Right Attorney`)
a = which(d2s$C %in% List$`Wrong Attorney`)
d2s$D[a] = as.character(sapply(d2s$C[a], f))

write.table(d2s, file = "Attorney.csv", sep = ',', row.names = F, col.names = F)
odbcClose(con)
