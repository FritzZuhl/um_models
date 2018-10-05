get_listener_stat2 <- function(df) {
  
  # Get age_bands and genders vectors
  tmp_age    <- tapply(df$streams, list(df$age_band), sum)
  tmp_gender <- tapply(df$streams, list(df$gender), sum)
  
  # Get unique listeners, Scale output to span unique listeners
  person_id <- unique(df$anonymized_person_id)
  scale = length(person_id)/sum(tmp_age)
  
  tmp_age    = round(tmp_age*scale) 
  tmp_gender = round(tmp_gender*scale )
  
  # create age/gender vectors to append
  age <- character()
  for(i in seq_along(tmp_age)) {
    age <- c(age,rep(names(tmp_age[i]), tmp_age[i]))
  }
  age <- sample(age) #randomize this series
  
  # to make all vectors equal length to make data.frame
  if (length(age) < length(person_id)) {
    tmp <- sample(age, length(person_id)-length(age))
    age <- c(age,tmp)
  }
  
  gender <- character()
  for (i in seq_along(tmp_gender)) {
    gender <- c(gender,rep(names(tmp_gender[i]), tmp_gender[i]))
  }
  gender <- sample(gender) #randomize this series
  
  # to make all vectors equal length to make data.frame
  if (length(gender) < length(person_id)) {  
    tmp <- sample(gender, length(person_id)-length(gender))
    gender <- c(gender,tmp)
  }
  
  res <- data.frame(anonymized_person_id=person_id, age_band=age, gender=gender)
  return(res)
}

get_listener_stat1 <- function(df) {
  
  tmp_age <- tapply(df$streams, list(df$age_band), sum)
  age_bands <- sort(rank(prop.table(tmp_age), ties.method = "random"),
                    decreasing = TRUE)
  most_likely_age_band <- names(age_bands[1])
  
  tmp_gender <- tapply(df$streams, list(df$gender), sum)
  genders <- sort(rank(prop.table(tmp_gender), ties.method = "random"),
                  decreasing = TRUE)
  most_likely_gender <- names(genders[1])
  
  # if age_band is unknown, so is gender
  if (most_likely_age_band == "Data Unknown" | most_likely_gender == 0) {
    most_likely_gender <- 0
    most_likely_age_band <- "Data Unknown"
  }
  
  answer <- data.frame(gender = most_likely_age_band, age_band = most_likely_gender )
  return(answer)
}


