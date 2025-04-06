package service

import (
	"bufio"
	"context"
	"fmt"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// ParseFile reads a file (currently assuming CSV), processes each row concurrently, and adds the values to the shards.
func ParseFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("could not open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	jobs := make(chan string, 1000)
	eg, ctx := errgroup.WithContext(context.Background())

	// skip header
	if scanner.Scan() {
		scanner.Text()
	}

	// Start worker goroutines
	for i := 0; i < viper.GetInt("workerCount"); i++ {
		eg.Go(func() error {
			return processRow(jobs)
		})
	}

	// Start a goroutine to read lines from the file and send them to the jobs channel
	go func() {
		defer close(jobs)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			case jobs <- scanner.Text():
			}
		}
		// Check for errors from the scanner
		if err = scanner.Err(); err != nil {
			slog.Error("scanner error", slog.String("filePath", filePath), slog.String("error", err.Error()))
		}
	}()

	if err = eg.Wait(); err != nil {
		return err
	}

	return nil
}

// processRow takes a job from the jobs channel, processes it, and add values to the shards. Errors will be logged.
func processRow(jobs <-chan string) error {
	for job := range jobs {

		// Split into 3 values
		row := strings.Split(job, ",")
		if len(row) != 3 {
			slog.Error("Invalid row format", slog.String("row", job))
			continue
		}

		// get household ID
		householdID, err := strconv.Atoi(row[0])
		if err != nil || householdID < 0 {
			slog.Error("Invalid household ID", slog.String("household ID", row[0]))
		}

		// get value of consumption
		consumption, err := strconv.ParseFloat(row[1], 64)
		if err != nil || consumption < 0 {
			slog.Error("Invalid consumption value", slog.String("consumption", row[1]))
		}

		// get timestamp
		timeStampInt, err := strconv.ParseInt(row[2], 10, 64)
		if err != nil || timeStampInt < 0 {
			slog.Error("Invalid timestamp value", slog.String("timestamp", row[2]))
		}
		timestamp := time.Unix(timeStampInt, 0).In(time.FixedZone("CET", 1*60*60))

		// Log all the errors, then skip to the next row
		if err != nil {
			continue
		}

		// Add to the shards
		addToShard(householdID, getQuarter(timestamp), consumption)
	}

	return nil
}
