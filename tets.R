ts <- as.POSIXlt(stamps,format = "%a %b %d %H:%M:%S %z %Y")

names(ts_file)



t1 <- xts(x = ts_file$eve,order.by = ts)
t3 <- xts(x = ts_file$blackfriday,order.by = ts)
t2 <- data.frame(ts,ts_file$eve)

t_min <- to.minutes(t1)
ids <- anytime(index(t_min))

t <- data.frame(ids,as.vector(tx))
res <- AnomalyDetectionTs(t,direction = "both",max_anoms = 0.01,plot = TRUE)

a <- parse(text=paste("test_$","sale",sep=""))
eval(a)


require(dygraphs)


ts
topic <- ts_file$blackfriday

dyHighlight(highlightSeriesOpts = list(strokeWidth = 3))

dygraph(df) %>% dyRangeSelector(height = 50) %>% dyOptions(fillGraph = TRUE,fillAlpha = 0.4,stackedGraph = TRUE) %>% dyHighlight(highlightCircleSize = 5, highlightSeriesBackgroundAlpha = 0.2,hideOnMouseOut = FALSE)
