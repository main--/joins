library(ggplot2)

data <- read.table('/dev/stdin', header = TRUE)
data$Algorithm <- factor(data$Algorithm, level = c("SortMergeJoin", "ProgressiveMergeJoin", "XJoin", "HashMergeJoin")) # fix order

# intuples stat with optimality comparison
ggplot(data, aes(Index, TuplesIn, col=Algorithm)) + geom_line() + xlab("Tuples produced") + ylab("Tuples received") + stat_function(fun = function(x) { 2 * sqrt(x * 1e6) }, colour = "green", linetype = "longdash")
# full IO stats (boring)
ggplot(data, aes(Index, TuplesIn + DiskIn + DiskOut, col=Algorithm)) + geom_line() + xlab("Tuples produced") + ylab("I/O operations")

