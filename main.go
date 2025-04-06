package main

import (
	"data_aggregator/service"
	"flag"
	"github.com/spf13/viper"
	"log/slog"
	"os"
	"time"
)

func init() {
	var (
		inputPath   = "input.csv"
		outputPath  = "output.csv"
		workerCount = 1
		numShards   = 10
		interval    = time.Minute * 15
	)

	flag.StringVar(&inputPath, "inputPath", "example_input.csv", "Path to input CSV file")
	flag.StringVar(&outputPath, "outputPath", "output.csv", "Path to output CSV file")
	flag.IntVar(&workerCount, "workerCount", 1, "Number of workers to process the file")
	flag.IntVar(&numShards, "numShards", 10, "Number of shards for map data distribution")
	flag.DurationVar(&interval, "interval", time.Minute*15, "Interval for data timestamp")
	flag.Parse()

	viper.Set("inputPath", inputPath)
	viper.Set("outputPath", outputPath)
	viper.Set("workerCount", workerCount)
	viper.Set("numShards", numShards)
	viper.Set("interval", interval)
}

func main() {

	// ---------- setup slog ----------------
	lvl := new(slog.LevelVar)
	lvl.Set(slog.LevelDebug)
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: lvl,
	})))

	if err := service.ProcessCSV(); err != nil {
		slog.Error(err.Error())
	}
}
