require(zoo)
require(readr)
require(xts)
require(forecast)
require(lubridate)
require(anytime)
require(dygraphs)

## Needs to be sent to a different script
#install.packages("devtools")
#devtools::install_github("twitter/AnomalyDetection")
library(AnomalyDetection)

ts_file <- read_csv("~/Downloads/test_.csv")
stamps <- ts_file$inception

vector_flag <- FALSE

dt <- data.frame(ts_file)

getTimeStamps <- function(vec){
  ts <- as.POSIXlt(vec,format = "%a %b %d %H:%M:%S %z %Y")
  return(ts)
}





getAnomaliesVec <- function(topic,ts){
  
  t <- xts(topic,order.by = ts)
  res <- AnomalyDetectionVec(topic,direction = "both",max_anoms = 0.49,plot = TRUE,period = 5)
  return(res$anoms)
  #print(res$anoms)
}

getAnomaliesTS <- function(topic,ts){
  
  t <- xts(topic,order.by = ts)
  tx <- to.minutes(t)
  stamps <- anytime(index(tx))
  
  df <- data.frame(stamps,as.vector(tx))
  res <- AnomalyDetectionTs(df,direction = "both",max_anoms = 0.49,plot = TRUE)
  print(res$anoms)
  
}

topic_names <- names(ts_file)  

len <- length(topic_names)
len2 <- nrow(dt)

ts <- getTimeStamps(stamps)

decision_frame <- data.frame(ts_file)

for(i in 1:(len-1)){
  
  tp <- parse(text=paste("dt$",topic_names[i],sep=""))
  #print(tp)
  tp <- eval(tp)
  #print(tp)
  
  anoms <- getAnomaliesVec(tp,ts)
  dec_vector <- matrix(nrow = len2)
  if(nrow(anoms)==0){
    for(j in 1:len2){
      dec_vector[j]<-0
    }
  }
  else{
    for(j in 1:len2){
      if(any(anoms$index==j)){
        dec_vector[j]<-1
      }
      else{
        dec_vector[j]<-0
      }
    }
  }
  
  
  decision_frame[i]<-dec_vector
  print(i)
}




decision_frame <- decision_frame[-(i+1)]
write.table(decision_frame,file = "~/Desktop/decision_frame.csv",sep = ",")


