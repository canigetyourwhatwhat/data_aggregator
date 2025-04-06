package service

import (
	"github.com/spf13/viper"
	"os"
	"testing"
)

func TestParseFile(t *testing.T) {

	viper.Set("workerCount", 3)
	viper.Set("numShards", 10)
	InitializeShards()

	tests := []struct {
		name             string
		fileContent      string
		mockProcessError error
		expectErr        bool
	}{
		{
			name: "Valid file, no errors",
			fileContent: `"Household ID","Consumption (KWh)","Timestamp"
1,10.5,1609459200
1,10.5,1609459200
`,
			mockProcessError: nil,
			expectErr:        false,
		},
		{
			name:        "File not found",
			fileContent: "",
			expectErr:   true,
		},
		// currently worker just logs the error
		//		{
		//			name: "Worker returns error",
		//			fileContent: `"Household ID","Consumption (KWh)","Timestamp"
		//1f,10.5,1609459200
		//`,
		//			mockProcessError: errors.New("mock error"),
		//			expectErr:        true,
		//		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var testFilePath string
			var err error

			if tt.name != "File not found" {
				// Create temp file
				tmpFile, tmpErr := os.CreateTemp("", "testdata*.csv")
				if tmpErr != nil {
					t.Fatalf("failed to create temp file: %v", tmpErr)
				}
				defer os.Remove(tmpFile.Name())
				testFilePath = tmpFile.Name()

				if _, err := tmpFile.Write([]byte(tt.fileContent)); err != nil {
					t.Fatalf("failed to write to temp file: %v", err)
				}
				tmpFile.Close()
			} else {
				testFilePath = "nonexistent.csv"
			}

			err = ParseFile(testFilePath)
			if (err != nil) != tt.expectErr {
				t.Errorf("ParseFile() error = %v, wantErr %v", err, tt.expectErr)
			}
		})
	}
}

func Test_processRow(t *testing.T) {

	viper.Set("numShards", 10)
	InitializeShards()

	type args struct {
		jobs <-chan string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "valid input",
			args: args{
				jobs: func() <-chan string {
					ch := make(chan string, 1)
					ch <- "1,10.5,1609459200"
					close(ch)
					return ch
				}(),
			},
			wantErr: false,
		},
		{
			name: "invalid row format",
			args: args{
				jobs: func() <-chan string {
					ch := make(chan string, 1)
					ch <- "1,10.5"
					close(ch)
					return ch
				}(),
			},
			wantErr: false,
		},
		{
			name: "invalid household ID",
			args: args{
				jobs: func() <-chan string {
					ch := make(chan string, 1)
					ch <- "abc,10.5,1609459200"
					close(ch)
					return ch
				}(),
			},
			wantErr: false,
		},
		{
			name: "invalid consumption value",
			args: args{
				jobs: func() <-chan string {
					ch := make(chan string, 1)
					ch <- "1,abc,1609459200"
					close(ch)
					return ch
				}(),
			},
			wantErr: false,
		},
		{
			name: "invalid timestamp",
			args: args{
				jobs: func() <-chan string {
					ch := make(chan string, 1)
					ch <- "1,10.5,abc"
					close(ch)
					return ch
				}(),
			},
			wantErr: false,
		},
		{
			name: "multiple valid rows",
			args: args{
				jobs: func() <-chan string {
					ch := make(chan string, 3)
					ch <- "1,10.5,1609459200"
					ch <- "2,15.5,1609545600"
					ch <- "3,20.5,1609632000"
					close(ch)
					return ch
				}(),
			},
			wantErr: false,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := processRow(tt.args.jobs); (err != nil) != tt.wantErr {
				t.Errorf("processRow() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
