package service

import (
	"github.com/spf13/viper"
	"reflect"
	"testing"
	"time"
)

func Test_getQuarter(t *testing.T) {
	type args struct {
		t time.Time
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "January beginning should be Q1",
			args: args{
				t: time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC),
			},
			want: "Q1-2023",
		},
		{
			name: "Random date in Q2",
			args: args{
				t: time.Date(2023, time.May, 17, 14, 32, 45, 0, time.UTC),
			},
			want: "Q2-2023",
		},
		{
			name: "Quarter boundary - first second of Q3",
			args: args{
				t: time.Date(2023, time.July, 1, 0, 0, 0, 0, time.UTC),
			},
			want: "Q3-2023",
		},

		{
			name: "Random date in Q4",
			args: args{
				t: time.Date(2023, time.November, 5, 10, 45, 30, 0, time.UTC),
			},
			want: "Q4-2023",
		},
		{
			name: "Different year - Q1",
			args: args{
				t: time.Date(2022, time.February, 14, 12, 0, 0, 0, time.UTC),
			},
			want: "Q1-2022",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getQuarter(tt.args.t); got != tt.want {
				t.Errorf("getQuarter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInitializeShards(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"Initialize shards"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			InitializeShards()
		})
	}
}

func Test_hashKey(t *testing.T) {
	const numShards = 4
	viper.Set("numShards", numShards)

	type args struct {
		key string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Simple household ID",
			args: args{key: "123"},
			want: 123 % numShards,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hashKey(tt.args.key); got != tt.want {
				t.Errorf("hashKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_addToShard(t *testing.T) {
	viper.Set("numShards", 5)

	type args struct {
		householdID int
		quarter     string
		consumption float64
	}

	tests := []struct {
		name   string
		args   args
		setup  func()
		verify func(t *testing.T)
	}{
		{
			name: "Add single consumption value to a shard",
			args: args{
				householdID: 123,
				quarter:     "Q1-2023",
				consumption: 10.5,
			},
			setup: func() {
				InitializeShards()
			},
			verify: func(t *testing.T) {
				index := hashKey("123")
				s := shards[index]

				s.Lock()
				defer s.Unlock()

				if val, ok := s.data[123]["Q1-2023"]; !ok || val != 10.5 {
					t.Errorf("Expected consumption 10.5 for household 123 in Q1-2023, got %v", val)
				}
			},
		},
		{
			name: "Add multiple consumption values for the same household and quarter",
			args: args{
				householdID: 456,
				quarter:     "Q2-2023",
				consumption: 15.5,
			},
			setup: func() {
				InitializeShards()
				// Pre-add consumption
				addToShard(456, "Q2-2023", 10.0)
			},
			verify: func(t *testing.T) {
				index := hashKey("456")
				s := shards[index]

				s.Lock()
				defer s.Unlock()

				if val, ok := s.data[456]["Q2-2023"]; !ok || val != 25.5 {
					t.Errorf("Expected aggregated consumption 25.5 for household 456 in Q2-2023, got %v", val)
				}
			},
		},
		{
			name: "Add consumption values for different quarters",
			args: args{
				householdID: 789,
				quarter:     "Q3-2023",
				consumption: 20.0,
			},
			setup: func() {
				InitializeShards()
				// Pre-add consumption for a different quarter
				addToShard(789, "Q2-2023", 15.0)
			},
			verify: func(t *testing.T) {
				index := hashKey("789")
				s := shards[index]

				s.Lock()
				defer s.Unlock()

				if val, ok := s.data[789]["Q2-2023"]; !ok || val != 15.0 {
					t.Errorf("Expected consumption 15.0 for household 789 in Q2-2023, got %v", val)
				}
				if val, ok := s.data[789]["Q3-2023"]; !ok || val != 20.0 {
					t.Errorf("Expected consumption 20.0 for household 789 in Q3-2023, got %v", val)
				}
			},
		},
		{
			name: "Complex scenario with multiple households and quarters",
			args: args{
				householdID: 101,
				quarter:     "Q4-2023",
				consumption: 30.5,
			},
			setup: func() {
				InitializeShards()
				// Setup data for multiple households across different quarters
				addToShard(101, "Q1-2023", 5.5)
				addToShard(101, "Q2-2023", 7.5)
				addToShard(101, "Q3-2023", 12.0)
				addToShard(202, "Q1-2023", 18.2)
				addToShard(202, "Q4-2023", 22.1)
				addToShard(303, "Q2-2023", 9.9)
			},
			verify: func(t *testing.T) {
				// Check our target household data
				index101 := hashKey("101")
				s101 := shards[index101]

				if val, ok := s101.data[101]["Q1-2023"]; !ok || val != 5.5 {
					t.Errorf("Expected consumption 5.5 for household 101 in Q1-2023, got %v", val)
				}
				if val, ok := s101.data[101]["Q2-2023"]; !ok || val != 7.5 {
					t.Errorf("Expected consumption 7.5 for household 101 in Q2-2023, got %v", val)
				}
				if val, ok := s101.data[101]["Q3-2023"]; !ok || val != 12.0 {
					t.Errorf("Expected consumption 12.0 for household 101 in Q3-2023, got %v", val)
				}
				if val, ok := s101.data[101]["Q4-2023"]; !ok || val != 30.5 {
					t.Errorf("Expected consumption 30.5 for household 101 in Q4-2023, got %v", val)
				}

				// Verify other households' data persists correctly
				index202 := hashKey("202")
				s202 := shards[index202]
				if val, ok := s202.data[202]["Q1-2023"]; !ok || val != 18.2 {
					t.Errorf("Expected consumption 18.2 for household 202 in Q1-2023, got %v", val)
				}
				if val, ok := s202.data[202]["Q4-2023"]; !ok || val != 22.1 {
					t.Errorf("Expected consumption 22.1 for household 202 in Q4-2023, got %v", val)
				}

				index303 := hashKey("303")
				s303 := shards[index303]
				if val, ok := s303.data[303]["Q2-2023"]; !ok || val != 9.9 {
					t.Errorf("Expected consumption 9.9 for household 303 in Q2-2023, got %v", val)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			addToShard(tt.args.householdID, tt.args.quarter, tt.args.consumption)
			tt.verify(t)
		})
	}
}

func Test_aggregate(t *testing.T) {
	viper.Set("numShards", 4)

	tests := []struct {
		name string
		want map[int]map[string]float64
	}{
		{
			name: "Aggregate consumption data from multiple shards",
			want: map[int]map[string]float64{
				101: {
					"Q1-2023": 25.0,
					"Q2-2023": 35.5,
				},
				202: {
					"Q1-2023": 18.2,
					"Q3-2023": 40.0,
				},
				303: {
					"Q4-2023": 31.4,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test data
			InitializeShards()

			// Populate shards with test data
			addToShard(101, "Q1-2023", 15.0)
			addToShard(101, "Q1-2023", 10.0)
			addToShard(101, "Q2-2023", 35.5)
			addToShard(202, "Q1-2023", 18.2)
			addToShard(202, "Q3-2023", 40.0)
			addToShard(303, "Q4-2023", 15.7)
			addToShard(303, "Q4-2023", 15.7)

			// Run the aggregation
			got := aggregate()

			// Verify results
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("aggregate() = %v, want %v", got, tt.want)
			}
		})
	}

}
