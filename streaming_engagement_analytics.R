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
source("engagement_artist_f.R")

artists_summary_spot <- artists_apply(d3_spot)
artists_summary_spot$platform_id <- 1
artists_summary_am <- artists_apply(d3_am)
artists_summary_am$platform_id <- 2

# Combine
artists_summary <- rbind(artists_summary_spot, artists_summary_am)
# Lable
artists_summary$date_id <- format(Sys.Date(), "%Y%m%d")

# Write to Postrgres
artists_summary <- data.frame(artists_summary)
RPostgreSQL::dbWriteTable(con, "engagement_streaming_scores",
                          value=artists_summary, overwrite=TRUE, append=FALSE, row.names=FALSE)



# function artists_apply
artists_apply <- function(this_data) {

  # apply the engagement function to each artist_id
  a <- unique(this_data$artist_id)
  res <- vector(mode="list", length=length(a))
  for (i in 1:length(a)) {
    artist1 <- subset(this_data, artist_id == a[i])
    res[[i]] <- engagement(artist1)
    res[[i]] <- c("artist_id" = a[i], res[[i]])
  }
  res.df <- do.call("rbind.data.frame", res)
  res.df$analytics_timestamp <- Sys.time()
  res.df <- res.df[order(res.df$trend28days, decreasing = TRUE),]

  # rankings
  res.df$mau_percentile <- rank(res.df$mau)/nrow(res.df)

  # 1 - ranking the improving engagement by trend28days (absolute)
  tmp <- (rank(res.df$trend28days))/nrow(res.df)
  res.df$engagement_change_percentile1 <- ifelse(is.na(res.df$trend28days),0,tmp)

  # 2 - ranking the improving engagement by trend28days_adj (relative to their size)
  tmp <- (rank(res.df$trend28days_adj))/nrow(res.df)
  res.df$engagement_change_percentile2 <- ifelse(is.na(res.df$trend28days_adj),0,tmp)
  return(res.df)
}

