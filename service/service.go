package service

import (
	"github.com/spf13/viper"
	"log/slog"
)

func ProcessCSV() error {

	InitializeShards()

	if err := ParseFile(viper.GetString("inputPath")); err != nil {
		return err
	}

	data := aggregate()

	if err := WriteCSV(viper.GetString("outputPath"), data); err != nil {
		return err
	}

	slog.Info("Processed CSV")
	return nil
}
