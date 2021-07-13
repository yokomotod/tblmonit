package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetTodaysClockObject(t *testing.T) {
	tests := []struct {
		clock   time.Time
		current time.Time
		wantRes time.Time
	}{

		{
			clock:   time.Date(0, 0, 0, 1, 2, 3, 0, time.Local),
			current: time.Date(2020, 1, 2, 0, 0, 0, 0, time.Local),
			wantRes: time.Date(2020, 1, 2, 1, 2, 3, 0, time.Local),
		},
		{
			clock:   time.Date(2020, 1, 2, 3, 4, 5, 6, time.Local),
			current: time.Date(2020, 1, 5, 0, 0, 0, 0, time.Local),
			wantRes: time.Date(2020, 1, 5, 3, 4, 5, 0, time.Local),
		},
	}
	for _, tt := range tests {
		actual := getTodaysClockObject(tt.clock, tt.current)
		expected := tt.wantRes
		assert.Equal(t, expected, actual)
	}
}

func TestGetSuitableTableID(t *testing.T) {
	now := time.Date(2021, time.July, 1, 11, 0, 0, 0, time.Local)
	futureTime := now.Add(time.Hour)

	tests := []struct {
		name    string
		tc      TableConfig
		wantRes string
	}{
		{
			name: "non_sharded_table",
			tc: TableConfig{
				Table:         "non_sharded_table",
				DateForShards: "",
				TimeThreshold: nil,
			},
			wantRes: "non_sharded_table",
		},
		{
			name: "TODAY no threshold",
			tc: TableConfig{
				Table:         "sample_table_on_",
				DateForShards: "TODAY",
				TimeThreshold: nil,
			},
			wantRes: "sample_table_on_20210701",
		},
		{
			name: "ONE_DAY_AGO no threshold",
			tc: TableConfig{
				Table:         "sample_table_on_",
				DateForShards: "ONE_DAY_AGO",
				TimeThreshold: nil,
			},
			wantRes: "sample_table_on_20210630",
		},
		{
			name: "FIRST_DAY_OF_THE_MONTH no threshold",
			tc: TableConfig{
				Table:         "sample_table_on_",
				DateForShards: "FIRST_DAY_OF_THE_MONTH",
				TimeThreshold: nil,
			},
			wantRes: "sample_table_on_20210701",
		},

		// future threshold
		{
			name: "TODAY future threshold",
			tc: TableConfig{
				Table:         "sample_table_on_",
				DateForShards: "TODAY",
				TimeThreshold: &TimeThreshold{Time: futureTime},
			},
			wantRes: "sample_table_on_20210630",
		},
		{
			name: "ONE_DAY_AGO future threshold",
			tc: TableConfig{
				Table:         "sample_table_on_",
				DateForShards: "ONE_DAY_AGO",
				TimeThreshold: &TimeThreshold{Time: futureTime},
			},
			wantRes: "sample_table_on_20210629",
		},
		{
			name: "FIRST_DAY_OF_THE_MONTH future threshold",
			tc: TableConfig{
				Table:         "sample_table_on_",
				DateForShards: "FIRST_DAY_OF_THE_MONTH",
				TimeThreshold: &TimeThreshold{Time: futureTime},
			},
			wantRes: "sample_table_on_20210601",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := getSuitableTableID(tt.tc, now)
			expected := tt.wantRes
			assert.Equal(t, expected, actual)
		})
	}
}

func TestIsOld(t *testing.T) {
	location, _ := time.LoadLocation("Asia/Tokyo")

	tests := map[string]struct {
		tc           TableConfig
		current      time.Time
		lastModified time.Time

		// output
		isOld  bool
		reason []string
	}{
		"lastModified -> timethreshold is correct": {
			tc: TableConfig{
				TimeThreshold: &TimeThreshold{
					Time: time.Date(2020, 1, 1, 12, 0, 0, 0, location),
				},
			},
			lastModified: time.Date(2020, 1, 1, 11, 0, 0, 0, location),
			current:      time.Date(2020, 1, 1, 12, 30, 0, 0, location),
			isOld:        false,
		},
		"timethreshold -> lastModified is incorrect": {
			tc: TableConfig{
				TimeThreshold: &TimeThreshold{
					Time: time.Date(2020, 1, 1, 11, 0, 0, 0, location),
				},
			},
			lastModified: time.Date(2020, 1, 1, 12, 0, 0, 0, location),
			current:      time.Date(2020, 1, 1, 12, 30, 0, 0, location),
			isOld:        true,
			reason:       []string{"The table should be created by 11:00, but last modified time is 12:00"},
		},
		"Duration from lastModified to current is in durationThreshold": {
			tc: TableConfig{
				DurationThreshold: &DurationThreshold{
					Duration: time.Hour,
				},
			},
			lastModified: time.Date(2020, 1, 1, 11, 0, 0, 0, location),
			current:      time.Date(2020, 1, 1, 11, 30, 0, 0, location),
			isOld:        false,
		},
		"Duration from lastModified to current is over durationThreshold": {
			tc: TableConfig{
				DurationThreshold: &DurationThreshold{
					Duration: time.Hour,
				},
			},
			lastModified: time.Date(2020, 1, 1, 11, 0, 0, 0, location),
			current:      time.Date(2020, 1, 1, 12, 30, 0, 0, location),
			isOld:        true,
			reason:       []string{"The table should be modified in 1h0m0s, but not modified in 1h30m0s"},
		},
		"Both timeThoreshold and durationThreshold are correct": {
			tc: TableConfig{
				DurationThreshold: &DurationThreshold{
					Duration: time.Hour,
				},
				TimeThreshold: &TimeThreshold{
					Time: time.Date(2020, 1, 1, 11, 0, 0, 0, location),
				},
			},
			lastModified: time.Date(2020, 1, 1, 11, 0, 0, 0, location),
			current:      time.Date(2020, 1, 1, 11, 30, 0, 0, location),
			isOld:        false,
		},
		"Both timeThoreshold and durationThreshold are incorrect": {
			tc: TableConfig{
				DurationThreshold: &DurationThreshold{
					Duration: time.Hour,
				},
				TimeThreshold: &TimeThreshold{
					Time: time.Date(2020, 1, 1, 10, 0, 0, 0, location),
				},
			},
			lastModified: time.Date(2020, 1, 1, 11, 0, 0, 0, location),
			current:      time.Date(2020, 1, 1, 12, 30, 0, 0, location),
			isOld:        true,
			reason: []string{
				"The table should be created by 10:00, but last modified time is 11:00",
				"The table should be modified in 1h0m0s, but not modified in 1h30m0s",
			},
		},
		"current < timethreshold: lastModified -> timethreshold is correct": {
			tc: TableConfig{
				TimeThreshold: &TimeThreshold{
					Time: time.Date(2020, 1, 1, 12, 0, 0, 0, location),
				},
			},
			lastModified: time.Date(2019, 12, 31, 11, 0, 0, 0, location),
			current:      time.Date(2020, 1, 1, 11, 30, 0, 0, location),
			isOld:        false,
		},
		"current < timethreshold: timethreshold -> lastModified is incorrect": {
			tc: TableConfig{
				TimeThreshold: &TimeThreshold{
					Time: time.Date(2020, 1, 1, 11, 0, 0, 0, location),
				},
			},
			lastModified: time.Date(2019, 12, 31, 12, 0, 0, 0, location),
			current:      time.Date(2020, 1, 1, 10, 30, 0, 0, location),
			isOld:        true,
			reason:       []string{"The table should be created by 11:00, but last modified time is 12:00"},
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			actual, reason := tt.tc.isOld(tt.current, tt.lastModified)
			assert.Equal(t, tt.isOld, actual)
			assert.Equal(t, tt.reason, reason)
		})
	}
}
