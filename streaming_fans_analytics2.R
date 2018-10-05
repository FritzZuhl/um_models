#!/usr/bin/Rscript
# Engagement
# Jira 730

# platform <- "apple_music" # ENG-730
# platform <- "spotify" # ENG-571

pword <- 'bates-lory-cracker'
Write2Postgres <- FALSE
this.N <- 14
this.threshold <- NULL
conflate_platforms <- TRUE

home_dir <- getwd()

library(bigrquery,   quietly = TRUE, verbose = FALSE, warn.conflicts = FALSE)
library(RPostgreSQL, quietly = TRUE, verbose = FALSE, warn.conflicts = FALSE)
library(plyr,        quietly = TRUE, verbose = FALSE, warn.conflicts = FALSE)
library(dplyr,       quietly = TRUE, verbose = FALSE, warn.conflicts = FALSE)
library(lubridate,   quietly = TRUE, verbose = FALSE, warn.conflicts = FALSE)
library(FFNutilities,quietly = TRUE, verbose = FALSE, warn.conflicts = FALSE)


# Connect to Postgres
# This is just a placeholder when the real connection is made.
con <- RPostgreSQL::dbConnect(dbDriver("PostgreSQL"), dbname = "insights",
                              host = "127.0.0.1", port = 65432,
                              user = "root", password = pword)

# Spotify
d_spot <- RPostgreSQL::dbReadTable(con,  "spot_engagement_analytic")
d_spot <- dplyr::tbl_df(d_spot)
renames_spot <- c("streams_user_id" = "listener_id", "streams_timestamp" = "timestamp")
#
d1_spot <- plyr::rename(d_spot, renames_spot )
d2_spot <- d1_spot[complete.cases(d1_spot),]
#
d2_spot$timestamp_rounded <- round_date(d2_spot$timestamp, unit="minute")
tmp <- d2_spot[c("listener_id", "timestamp_rounded")]
d3_spot  <- d2_spot[!duplicated(tmp),]
rm(tmp, d_spot, d1_spot, d2_spot)

# Apple
d_am <- RPostgreSQL:: dbReadTable(con, "am_engagement_analytic")
d_am <- dplyr::tbl_df(d_am)
renames_am <- c("anonymized_person_id" = "listener_id")
#
d1_am <- plyr::rename(d_am, renames_am )
d2_am <- d1_am[complete.cases(d1_am),]
#
d2_am$timestamp_rounded <- round_date(d2_am$timestamp, unit="minute")
tmp <- d2_am[c("listener_id", "timestamp_rounded")]
d3_am  <- d2_am[!duplicated(tmp),]
rm(tmp, d_am, d1_am, d2_am)

# save(d3_am, d3_spot, file = "in_data.RData")

# get super fans
determine_super_fans <- function(df, threshold = this.threshold, N=this.N) {
  tmp <- as.data.frame(table(df$listener_id))
  tmp$Var1 <- as.character(tmp$Var1)
  names(tmp) <- c("listener_id", "streams")
  tmp$prop_rank <- rank(tmp$streams)/nrow(tmp)
  
  if (!is.null(threshold))
    great_fans <- subset(tmp, prop_rank >= threshold)
  
  if (!is.null(N)) 
    great_fans <- subset(tmp, streams >= N)
  
  count <- min(great_fans$streams)
  gfans <- great_fans$listener_id
  return(list(fans=gfans, min_streams = count))
}
super_fans_am <- determine_super_fans(d3_am)
super_fans_spot <- determine_super_fans(d3_spot)

sf_am <- super_fans_am$fans
sf_spot <- super_fans_spot$fans

super_fans_am$min_streams
super_fans_spot$min_streams


calculate_super_fans_per_artist <- function(df, all_super_fans) {
  options(stringAsFactors=FALSE)
  listener_ids <- unique(df$listener_id)
  fans <- sum(listener_ids %in% all_super_fans)
  return(fans)
}

out <- plyr::ddply(d3_am, .(artist_id), calculate_super_fans_per_artist, all_super_fans=sf_am)
out$platform <- 2
names(out) <- c("artist_id", "super_fan_count", "platform")
out_am <- out

out <- plyr::ddply(d3_spot, .(artist_id), calculate_super_fans_per_artist, all_super_fans=sf_spot)
out$platform <- 1
names(out) <- c("artist_id", "super_fan_count", "platform")
out_spot <- out

# Combine
artists_fans <- rbind(out_am, out_spot)

# Conflate Platforms
if (conflate_platforms) {
  artists_fans <- plyr::ddply(artists_fans, .(artist_id), summarize, 
                              super_fan_count = sum(super_fan_count))
  
}
artists_fans$date_id <- format(Sys.Date(), "%Y%m%d")

# Write to Postrgres
artists_fans <- data.frame(artists_fans) # make sure it's a data.frame and not a tidyverse thing.
if (Write2Postgres) {
  RPostgreSQL::dbWriteTable(con, "streaming_fan_scores",
                          value=artists_fans, overwrite=TRUE, append=FALSE, row.names=FALSE)
}
