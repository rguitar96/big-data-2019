
data <- read.csv("clean-twitter-data.csv", fileEncoding = "latin1")

data$Date <- as.Date(data$Date)
data$HashTags = regmatches(data$Tweet.content, gregexpr("#(\\d|\\w)+", data$Tweet.content))

#Creating a column that joins the date and time.
data<-mutate(data, times = paste(Date, Hour))

#Counting the number of tweets per timestamp.
time.series<-plyr::count(dplyr::select(data, c(times, Date)))
times<-(time.series$times)
time.format<- "%Y-%m-%d %H:%M"
timestamp.formatted <-(as.POSIXct(times, format=time.format)) 
time.series$times<- timestamp.formatted
time.series$Date<- as.Date(time.series$Date)

names(time.series) <- c("Time", "Date", "Tweets")

