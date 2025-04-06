# Quarterly Aggregation of Meter Data


## Description
This project consumes CSV file, aggregates the data into yearly, quarterly intervals per household. and generates a CSV file.

## Business logic
1. It takes an argument of CSV file path and others to run the program
2. CSV file is read line by line with goroutines and processed
3. Processed data is stored in the sharded storage (slice of maps).
4. Once files are completely scanned, all data is processed, and stored in the sharded storage, it then aggregates all the data into one data source (map).
5. Once data is aggregated, it is then written into a CSV file.


## Project explanation
To handle extremely large dataset, this project uses two strategy to increase efficiency
- Sharding: The data is sharded into multiple maps to allow concurrent processing of the data. This allows for faster processing of the data as multiple goroutines can work on different shards at the same time.
- Reading each line by multiple goroutines: Since the file is scanned concurrently, it allows for faster processing of the data as multiple goroutines can work on different lines at the same time.


## Requirements to run
- Golang version 1.24


## How to run
1. Fulfill all the requirements above
2. Run the command below to see the available flags
    ```shell
    go run *.go -help
    ```
3. Run the command below to see the program with default flags
    ```shell
    go run *.go
    ```
4. CSV is generated


## Unit Tests 
Integration tests mainly cover the below two points
 - Valid data processing (correct quarterly aggregation). 
 - Error handling (skipping invalid rows).

Run the command below to run the integration tests 
```shell  
    go test ./service
```


## Future extension
- Functionality to process with distributed system
  - The current solution is fit for the file size approximately several GBs. If the file size is larger than that, then it is recommended to use a distributed system
  - It can detect the size of the file, separate the file into small chunks. As soon as one chunk is ready, send to the different server to process the data.
  - Once all the chunks are processed, send the message to the one root server, then aggregate all the data into one data source.