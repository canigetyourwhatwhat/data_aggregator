package service

import (
	"github.com/spf13/viper"
	"log"
	"log/slog"
)

func ProcessCSV() error {

	InitializeShards()

	if err := ParseFile(viper.GetString("inputPath")); err != nil {
		log.Fatalf("Failed to parse file: %v", err)
	}

	data := aggregate()

	if err := WriteCSV(viper.GetString("outputPath"), data); err != nil {
		log.Fatalf("Failed to write output: %v", err)
	}

	slog.Info("Processed CSV")
	return nil
}
