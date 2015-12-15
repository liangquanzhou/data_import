##　just for fun

## get a sample of data in forecolsure data, and try to plot it on the map, using ggmap package

sampledata = subset(table, muni =="0905")
View(sampledata)

library(ggmap)
address = paste(sampledata$address, ", hoboken city, nj")

googleadd = geocode(address, source = "google", output = "more")
df = subset(googleadd, select = c(lon,lat))

map <- get_googlemap('hoboken city', markers = df, path = df, zoom = 12)

ggmap(map)
