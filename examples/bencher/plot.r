library(ggplot2)

scale <- scale_colour_manual(values=c('SortMergeJoin' = '#1b9e77', 'ProgressiveMergeJoin' = '#d95f02', 'XJoin' = '#7570b3', 'HashMergeJoin' = '#e7298a', 'Statistical maximum' = '#66a61e'), breaks=c("SortMergeJoin", "ProgressiveMergeJoin", "XJoin", "HashMergeJoin", "Statistical maximum"))

run_plot <- function(codename) {
	print(paste("Reading", codename))
	data <- read.table(paste('timings_', codename, '_adaptive.txt', sep=''), header = TRUE)
	#data$Algorithm <- factor(data$Algorithm, level = c("SortMergeJoin", "ProgressiveMergeJoin", "XJoin", "HashMergeJoin")) # fix order

	# intuples stat with optimality comparison
	plot <- ggplot(data, aes(Index, TuplesIn, col=Algorithm)) + geom_line() + xlab("Tuples produced") + ylab("Tuples received") + scale
	if (endsWith(codename, '_random')) {
		plot <- plot + stat_function(aes(col='Statistical maximum'), fun = function(x) { 2 * sqrt(x * 1e6) }, linetype = "longdash", n = 1e5)
	} else if (endsWith(codename, '_10pct')) {
		plot <- plot + stat_function(aes(col='Statistical maximum'), fun = function(x) { ifelse(x <= 20000, sqrt(x * (9e6 / 2)), x * 10 + 1e5) }, linetype = "longdash", n = 1e5)
	}
	ggsave(paste('intuples_', codename, '_full.png', sep=''), plot, width = 12.80, height = 7.20)
	print("Plot 1 done")
	# full IO stats (boring)
	plot <- ggplot(data, aes(Index, TuplesIn + DiskIn + DiskOut, col=Algorithm)) + geom_line() + xlab("Tuples produced") + ylab("I/O operations") + scale
	ggsave(paste('all_io_', codename, '_full.png', sep=''), plot, width = 12.80, height = 7.20)
	if (codename == 'reliable_random') {
		ggsave(paste('all_io_', codename, '_crop.png', sep=''), plot + xlim(0, 15000), width = 12.80, height = 7.20)
	}
}
run_plot('reliable_random')
run_plot('burst_random')
run_plot('mixed_random')
run_plot('mixed_10pct')

