#!/usr/bin/Rscript
# Engagement
# Jira 730

# platform <- "apple_music" # ENG-730
# platform <- "spotify" # ENG-571

pword <- 'bates-lory-cracker'

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








# load in function that calculates engagement
source("super_fans_f.R")

# function artists_apply
artists_apply <- function(this_data) {
  options(stringsAsFactors=FALSE)
  a <- unique(this_data$artist_id)
  res_dep <- vector("list",length(a))
  for (i in 1:length(a)) {
    artist1 <- subset(this_data, artist_id == a[i])
    out_super_fans <- super_fans(artist1)
    res <- c(artist_id=a[i], out_super_fans)

    res_dep[[i]] <- res
  }
  res.df <- do.call('rbind', res_dep)
  res.df <- as.data.frame(res.df)
  return(res.df)
}

artists_fans_spot <- artists_apply(d3_spot)
artists_fans_spot$platform_id <- 1
artists_fans_am <- artists_apply(d3_am)
artists_fans_am$platform_id <- 2

# Combine
artists_fans <- rbind(artists_fans_spot, artists_fans_am)
# Lable
artists_fans$date_id <- format(Sys.Date(), "%Y%m%d")

# Write to Postrgres
artists_fans <- data.frame(artists_fans) # make sure it's a data.frame and not a tidyverse thing.
RPostgreSQL::dbWriteTable(con, "streaming_fan_scores",
                          value=artists_fans, overwrite=TRUE, append=FALSE, row.names=FALSE)

