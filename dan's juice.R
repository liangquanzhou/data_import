
setwd("C:/Users/Liangquan/Desktop/dan")
document = dir()

sqlHost <- "GP-4\\SQLEXPRESS"
sqlDatabase <- "DCA"
dsnString <- "driver={SQL Server};server=%s;database=%s;trusted_connection=true"
dsn <- sprintf(dsnString, sqlHost, sqlDatabase)
channel <- odbcDriverConnect(dsn)
sqlDrop(channel, "juice")

for (i in 1:length(document)) {
  filename = document[i]
  con = file(filename)
  filelist = strsplit(readLines(con), split = "\\|")
  filelist1 = readLines(con)
  close(con)
  header = filelist[[1]]
  filelist = filelist[-1]
  m = do.call("rbind", filelist)
  d = data.frame(m)
  names(d) = header
  d$sources = filename
  d = apply(d, 2, trimws)
  d = apply(d, 2, as.character)
  d = data.frame(d)
  sqlSave(channel, d, tablename = "juice", append = T, rownames = F, colnames = F)
  #write.table(d, file = "dan.csv", append = F, sep = ",", row.names = F, col.names = (i == 1), qmethod = "double")
}

