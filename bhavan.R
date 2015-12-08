library(xlsx)
s1 = read.xlsx("Copy of Assignment Sale 2015 (002).xlsx", sheetIndex = "Sheet1")
View(s1)

c1 = s1$OWNERS.NAME...ADDRESS
c1 = as.character(c1)
a = which(!is.na(c1))

length(c1)
for (i in 1:length(a)){
  if (a[i]+1 == a[i+1]) c1[a[i]] = paste(c1[a[i]],c1[a[i+1]],sep = " ")
  else i = i+1
}