package service

import (
	"fmt"
	"github.com/spf13/viper"
	"hash/fnv"
	"strconv"
	"sync"
	"time"
)

// getQuarter returns the quarter in the format "Q1-2023", "Q2-2023" for the given record of timestamp.
func getQuarter(t time.Time) string {
	// since the data is in before the intervals, we need to add time to the timestamp
	t.Add(time.Minute * viper.GetDuration("interval"))
	month := t.Month()
	year := t.Year()
	switch {
	case month >= 1 && month <= 3:
		return fmt.Sprintf("Q1-%d", year)
	case month >= 4 && month <= 6:
		return fmt.Sprintf("Q2-%d", year)
	case month >= 7 && month <= 9:
		return fmt.Sprintf("Q3-%d", year)
	default:
		return fmt.Sprintf("Q4-%d", year)
	}
}

type shard struct {
	sync.Mutex
	data map[int]map[string]float64
}

// shards holds the all dataset to later aggregate
var shards []*shard

// InitializeShards initializes the shards with the number of shards defined in the config.
func InitializeShards() {
	shards = make([]*shard, viper.GetInt("numShards"))
	for i := range shards {
		shards[i] = &shard{data: make(map[int]map[string]float64)}
	}
}

// hashKey hashes the key and returns the index for the shard.
func hashKey(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % viper.GetInt("numShards")
}

// addToShard finds the appropriate shard, then adds the consumption value.
func addToShard(householdID int, quarter string, consumption float64) {
	index := hashKey(strconv.Itoa(householdID))
	s := shards[index]

	s.Lock()
	defer s.Unlock()

	if _, ok := s.data[householdID]; !ok {
		s.data[householdID] = make(map[string]float64)
	}
	s.data[householdID][quarter] += consumption
}

// aggregate combines all the shards into a single data source.
func aggregate() map[int]map[string]float64 {
	result := make(map[int]map[string]float64)
	for _, s := range shards {
		for household, quarters := range s.data {
			if _, ok := result[household]; !ok {
				result[household] = make(map[string]float64)
			}
			for quarterName, consumption := range quarters {
				result[household][quarterName] += consumption
			}
		}
	}
	return result
}
