setwd("G:/NYU_Class/BIGDATA/project/ouput")
# **************************************************
# processing date data
# **************************************************
weather.frame <- read.csv("merge.csv")
# plot
plot(weather.frame$TMAX, weather.frame$count, main = "MAX Temperature with Crimes", xlab = "Max Temperature", ylab = "Count")
plot(weather.frame$TMIN, weather.frame$count, main = "MIN Temperature with Crimes", xlab = "MIN Temperature", ylab = "Count")
plot(weather.frame$PRCP, weather.frame$count, main = "dot", xlab = "PRACI", ylab = "Count")

hist(weather.frame$PRCP)
weather.frame$precipitation <- sapply(weather.frame$PRCP, function(x) 
  if (x < 0.5) return ("0 - 0.5") 
  else if (x < 1) return ("0.5 - 1")
  else if (x < 2) return ("1 - 2")
  else return ("2 - more")
)
# box plot
library(ggplot2)
p <- ggplot(weather.frame, aes(precipitation, count))
p + geom_boxplot()

# wind
plot(weather.frame$AWND, weather.frame$count, main = "Wind with Crimes", xlab = "Average daily wind speed", ylab = "Count")
# merge with merge rate
rate.frame <- read.csv("merge_rate.csv")
weather.frame <- merge(weather.frame, rate.frame, key = "DATE")
library(plot3D)
#c("Wind", "Inside Rate", "Count")
plot(weather.frame$AWND, weather.frame$rate, main = "Wind with Crimes", xlab = "Average daily wind speed", ylab = "Rate")
plot(weather.frame$count, weather.frame$rate, main = "Wind with Crimes", xlab = "count", ylab = "Rate")
scatter3D(weather.frame$AWND, weather.frame$rate, weather.frame$count, xlab = "Wind", 
          ylab = "Inside Rate", zlab = "Count",
          theta = 30, phi = 50
          )