############ packages needed ##############
pckg = c('tidyr', 'stringi', 'data.table', 'reshape2', 'plyr','RgoogleMaps', 'httr', 'rjson', 'XML',
         'choroplethrMaps', 'choroplethr', 'RODBC', 'zipcode','RCurl','jsonlite', 'RJSONIO', 'ggmap', 'xlsx') 

is.installed <- function(mypkg){
  is.element(mypkg, installed.packages()[,1])
} 

for(i in 1:length(pckg)) {
  if (!is.installed(pckg[i])){
    install.packages(pckg[i])
  }
  library(pckg[i], character.only = T)
}

### given a dataframe and an array of names to drop,
### drop these names and rename the df from alphabetically

drop_and_rename = function(df, drops = NA){
  names(df) = LETTERS[1:length(names(df))]
  df = df[, !(names(df) %in% drops)]
  names(df) = LETTERS[1:length(names(df))]
  return(df)
}

## data import

fc = file('pampac0601.txt')
mylist = strsplit(readLines(fc), ";")
close(fc)

options(stringAsFactors=F)
l2 = lapply(mylist, t)
l3 = lapply(l2, as.data.frame)
d = as.data.frame(rbindlist(l3, fill = T),stringAsFactors = F)

d1 = d[d$V6 == "0",] #plaintiff
d2 = d[d$V6 == "1",] #attorney
d3 = d[d$V6 == "3",] #apn