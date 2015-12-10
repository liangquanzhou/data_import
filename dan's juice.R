
setwd()
document = dir()

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
  #write.table(d, file = "dan.csv", append = F, sep = ",", row.names = F, col.names = (i == 1), qmethod = "double")
}

