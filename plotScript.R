#Plotting file

plotTopics <- function(df,ts){
  
  #View(df)
  srs <- df[1:(len-1)]
  View(srs)
  srs <- data.frame(srs)
  srs <- xts(x=srs,order.by = ts)
    
  #View(srs)
  dygraph(srs) %>% dyRangeSelector(height = 50) %>% dyOptions(fillGraph = TRUE,fillAlpha = 0.4,stackedGraph = TRUE)
  
}

plotTopicsIndividual <- function(df,ts){
  
  
  
}
