super_fans <- function(this_artist, N=10, cutoff = 5) {
  library(FFNutilities)

  if (nrow(this_artist) < 50) {
    return(rep(NA,N))
  }

  df1 <- count_vector(this_artist$listener_id)
  df1$portion <- NULL
  df2 <- subset(df1, count >= cutoff)
  N2 <- nrow(df2)
  if (N > N2) {    # pad returns so that it is N length
    array_NA <- rep(NA, length(N-N2))
    fans <- df2[1:N2,'item']
    fans <- c(fans, array_NA)
    fans <- fans[1:N]
  } else {
    fans <- df2[1:N,'item']
  }
  names(fans) <- paste("fan_",seq(1,length(fans)),sep="")
  return(fans)
}
