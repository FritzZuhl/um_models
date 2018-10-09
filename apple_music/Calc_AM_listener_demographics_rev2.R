#! <she-bang for path for R script>

library(bigrquery)
# library(FFNutilities)
library(plyr)
source("listener_stat_functions.R")

#
date_id <- "2018-9-29"
method  <- 'second'
overwrite_results_table <- TRUE
table_name <- 'tommy-boy.FritzZuhl.out_apple_music'
dataset <- ""
project <- "tommy-boy"
#

con <- bigrquery::dbConnect(bigquery(), project)



# Get the data from Big Query
query_get_daily_data <- "
select
s.anonymized_person_id
,s.apple_identifier
,s.action_type
,cd.age_band
,cd.gender
,cd.streams
from
`applemusic_analytics.am_streams` s
join
`applemusic_analytics.am_contentdemographics` cd
on
s.apple_identifier = cd.apple_identifier
and
s.membership_mode = cd.membership_mode
and
s.membership_type = cd.membership_type
and
s.ingest_datestamp = cd.ingest_datestamp
and
s.storefront_name = cd.storefront_name
and
s.action_type = cd.action_type
and
s.datestamp = cd.datestamp
where
s.ingest_datestamp = '%s'
order by 1,2"


res <- DBI::dbSendQuery(con, sprintf(query_get_daily_data, date_id))
BQ_data <- DBI::dbFetch(res)
dbClearResult(res)

# save(BQ_data, file = "BQ_data.RData")
# write.csv2(BQ_data, file = "BQ_data.csv", sep=",")


if (method == 'first') {
  out <-plyr::ddply(BQ_data, .(anonymized_person_id), get_listener_stat1)
  out <- out[,c("anonymized_person_id", "gender", "age_band")]
  out <- out[order(out$anonymized_person_id),]
} else {
  out <- get_listener_stat2(BQ_data)
}
# give it a date tag
out$date_id <- date_id

# Write to BigQuery
if (overwrite_results_table) {
  if (DBI::dbExistsTable(con, table_name)) 
    DBI::dbRemoveTable(con, table_name, quiet=TRUE)
  
  dbWriteTable(con, table_name, out, append=FALSE, overwrite=TRUE, quiet=TRUE)
} else {
  dbWriteTable(con, table_name, out, append=TRUE, overwrite=FALSE, quiet=TRUE)
}  
# end
