setwd("~/Desktop/um_models")
# convert from R to csv


load("data/in_data.RData")

write.csv2(d3_am, file = 'data/d3_am.csv')

