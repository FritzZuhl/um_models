# get super_fans_f.R
# rank all listeners for this day
library(plyr)

# get super fans
determine_super_fans <- function(df, threshold = 0.9) {
  tmp <- as.data.frame(table(df$listener_id))
  tmp$Var1 <- as.character(tmp$Var1)
  names(tmp) <- c("listener_id", "streams")
  tmp$prop_rank <- rank(tmp$streams)/nrow(tmp)
  great_fans <- subset(tmp, prop_rank >= threshold)
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
out$platform <- "apple_music"
names(out) <- c("artist_id", "super_fan_count", "platform")
out_am <- out

out <- plyr::ddply(d3_spot, .(artist_id), calculate_super_fans_per_artist, all_super_fans=sf_spot)
out$platform <- "spotify"
names(out) <- c("artist_id", "super_count", "platform")
out_spot <- out

