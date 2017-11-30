setwd("G:/NYU_Class/BIGDATA/project/ouput")
# **************************************************
# processing date data
# **************************************************
date.frame <- read.csv("date.csv")
date.frame$X <- NULL
colnames(date.frame) <- c("number", "date")
date.frame$date <- as.Date(date.frame$date, "%m/%d/%Y")
date.frame <- date.frame[order(date.frame$date),]
date.range.vector <- range(date.frame$date)
date.range.vector <- seq(date.range.vector[1], date.range.vector[2], 1)
length(date.range.vector) == length(date.frame$date)
# plot
library(ggplot2)
# bar chart
date.bar.plot <- ggplot(data=date.frame, aes(x = date, y = number)) +
  geom_bar(stat="identity")
# histogram
date.histogram.plot <- ggplot(data=date.frame, aes(number), xlab = "number") + geom_histogram(binwidth= 4)
# boxplot
ggplot(date.frame, aes("number", number)) + geom_boxplot()

# find outlier with date
date.frame[date.frame$number < 750 ,]
date.frame[date.frame$number > 1800 ,]

# **************************************************
# processing offense classification
# **************************************************
ky.frame <- read.csv("KY_CD.csv")
ky.frame$X <- NULL
colnames(ky.frame) <- c("number", "KY_CD")
# sort
ky.frame <- ky.frame[order(ky.frame$number),]

pd.frame <- read.csv("pd.csv")
pd.frame$X <- NULL
colnames(pd.frame) <- c("number", "PD_CD")
# sort
pd.frame <- pd.frame[order(pd.frame$number),]

ky.date.frame <- read.csv("ky_num_date_count.csv")
ky.date.frame$X <- NULL
colnames(ky.date.frame) <- c("KY_CD", "date_number")
# histogram
ggplot(data=ky.date.frame, aes(date_number)) + geom_histogram(binwidth= 40)

pd.date.frame <- read.csv("pd_num_date_count.csv")
pd.date.frame$X <- NULL
colnames(pd.date.frame) <- c("PD_CD", "date_number")
# histogram
ggplot(data=pd.date.frame, aes(date_number)) + geom_histogram(binwidth= 40)