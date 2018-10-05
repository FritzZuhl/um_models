
engagement <- function(d) {
  require(lubridate)
  require(plyr)

  # days, weeks
  # d$days_ago <- round(as.numeric(difftime(max(d$timestamp), d$timestamp,units = 'days')),0)
  d$dow <- weekdays(d$timestamp)
  d$weeknum <- week(d$timestamp)
  d$doy <- yday(d$timestamp)
  d$doyN <- (d$doy - min(d$doy)) + 1

  # Response Vector
  # res <- list(artist_id = this_artist)
  res <- list()

  # General Feature 1, How many unique listeners?
  unique_listeners <- function(x) {
    length(unique(x$listener_id))
  }
  mau <- unique_listeners(d)
  res <- c(res, "mau" = mau)

  # General Feature 2, How many total streams
  streams <- function(x) {
    nrow(x)
  }
  total_streams <- streams(d)

  res <- c(res, "total_streams" = total_streams)

  # average steams/isrc
  x <- data.frame(table(d$listener_id))
  ave_streamsPerListeners    <- round(mean(x$Freq),2)
  median_streamsPerListeners <- median(x$Freq)
  res <- c(res, "average_streamsPerListener"=ave_streamsPerListeners, "median_streamsPerListeners"=median_streamsPerListeners)

  # General Feature 3, stream count trend
  streamsByDays <- function(x, normalize=TRUE) {
    # stream count by doyN
    # cannot us table()
    # if no streams present, then count is 0
    N <- max(x$doyN)
    streams <- numeric(N)
    for (i in 1:N) {
      streams[i] <- nrow(subset(x, doyN==i))
    }
    x.agg <- data.frame("doyN"=1:N, "streams" = streams)

    if (normalize) {
      x.agg$streamsN <- (x.agg$streams - min(x.agg$streams)) / (max(x.agg$streams) - min(x.agg$streams))
    }
    return(x.agg)
  }

  # Remove days that are extreams from norms
  removeExtreams <- function(x, purgeBYscale=FALSE) {
    # purge by scale
    if (purgeBYscale) {
      x$scaled <- scale(x$streams)
      std_dev <- sd(x$streams)
      signmas_out <- 1.5
      index <- signmas_out > abs(x$scaled)
    } else {
      # purge by day
      index <- 1:27
    }
    x2 <- x[index, ]
    return(x2)
  }
  agg <- streamsByDays(d, normalize=TRUE)
  agg2 <- removeExtreams(agg)

  getTrend <- function(x) {
    out <- try(lm(streams ~ doyN, na.action=na.exclude, data=x), silent = TRUE)
    slope <- unname(coef(out)[2])
    if (is.na(slope))
      slope <- NA
    return(slope)
  }

  # only call getTrend if there are enough days to do regression
  options(show.error.messages = FALSE)
  trend28days <- getTrend(agg2)
  #
  last14days <- subset(agg2, doyN >= 14)
  if (nrow(last14days) > 10) {
    trend14days <- getTrend(last14days)
  } else {
    trend14days <- NA
  }

  trend28days_adj <- trend28days / total_streams
  trend14days_adj <- trend14days / total_streams


  # List top ISRC
  get_top_isrc <- function(x, topN=3) {
    x2 <- as.data.frame(prop.table(table(x$isrc)))
    x3 <- x2[order(x2$Freq, decreasing = TRUE),"Var1"]
    x4 <- as.character(x3[1:topN])
    names(x4) <- paste("isrc", 1:topN, sep="")
    x4 <- as.list(x4)
    return(x4)
  }
  top_isrc <- get_top_isrc(d)
  res <- c(res, "trend28days" = trend28days, "trend14days" = trend14days, "trend28days_adj" = trend28days_adj,
           "trend14days_adj" = trend14days_adj, top_isrc)

  return(data.frame(res))
}

