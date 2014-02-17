package workload

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestSimpleSchedule(t *testing.T) {

	sb := NewScheduleBuilder(1, 1, 1*time.Second, 10*time.Second, 30*time.Second, 60*time.Second)

	schedules := sb.BuildSchedules()

	if len(schedules) != 1 {
		t.Fatalf("expected 1 schedule, got %d", len(schedules))
	}

	if len(schedules[0]) != 1 {
		t.Fatalf("expected schedule to only contain 1 transition, got %d", len(schedules[0]))
	}

	if schedules[0][0].start != time.Duration(0) {
		t.Errorf("expected schedule transition at offset 0, got %v", schedules[0][0])
	}
}

// these tests require that we have an exact desired schedule that we can compare against
// this only works if minOffTime = maxOffTime so that there is no randomness
func TestScheduleBuilderExact(t *testing.T) {
	tests := []struct {
		sb      *ScheduleBuilder
		results []RunSchedule
	}{
		// test 2 users, 2 active max, second one should ramp up 1 second later (no one goes offline)
		{
			sb: NewScheduleBuilder(2, 2, 1*time.Second, 10*time.Second, 10*time.Second, 30*time.Second),
			results: []RunSchedule{
				RunSchedule{
					&RunInterval{
						start: 0,
						end:   -1,
					},
				},
				RunSchedule{
					&RunInterval{start: 1 * time.Second,
						end: -1,
					},
				},
			},
		},
		// test 4 users, 2 active max
		{
			sb: NewScheduleBuilder(4, 2, 1*time.Second, 10*time.Second, 10*time.Second, 30*time.Second),
			results: []RunSchedule{
				RunSchedule{
					&RunInterval{
						start: 0,
						end:   10 * time.Second,
					},
					&RunInterval{
						start: 20 * time.Second,
						end:   -1,
					},
				},
				RunSchedule{
					&RunInterval{
						start: 1 * time.Second,
						end:   10 * time.Second,
					},
					&RunInterval{
						start: 20 * time.Second,
						end:   -1,
					},
				},
				RunSchedule{
					&RunInterval{
						start: 10 * time.Second,
						end:   20 * time.Second,
					},
				},
				RunSchedule{
					&RunInterval{
						start: 10 * time.Second,
						end:   20 * time.Second,
					},
				},
			},
		},
	}

	for _, test := range tests {
		schedules := test.sb.BuildSchedules()
		if !reflect.DeepEqual(schedules, test.results) {
			t.Errorf("Expected schedule %v, got %v", test.results, schedules)
		}
	}
}

func TestScheduleBuilderInexact(t *testing.T) {
	tests := []struct {
		sb                 *ScheduleBuilder
		expectedMaxActive  int
		expectedMinOffTime time.Duration
		expectedMaxOffTime time.Duration
	}{
		// // test 2 users, 2 active max, second one should ramp up 1 second later (no one goes offline)
		// {
		// 	sb:                 NewScheduleBuilder(2, 2, 1*time.Second, 10*time.Second, 10*time.Second, 30*time.Second),
		// 	expectedMaxActive:  2,
		// 	expectedMinOffTime: 0, // 0 is OK here because no one ever goes offline
		// 	expectedMaxOffTime: 10 * time.Second,
		// },
		// // test 4 users, 2 active max
		// {
		// 	sb:                 NewScheduleBuilder(4, 2, 1*time.Second, 10*time.Second, 10*time.Second, 30*time.Second),
		// 	expectedMaxActive:  2,
		// 	expectedMinOffTime: 10 * time.Second,
		// 	expectedMaxOffTime: 10 * time.Second,
		// },
		// // test 40 users, 8 active max
		// {
		// 	sb:                 NewScheduleBuilder(40, 8, 1*time.Second, 10*time.Second, 1*time.Minute, 1*time.Hour),
		// 	expectedMaxActive:  8,
		// 	expectedMinOffTime: 10 * time.Second,
		// 	expectedMaxOffTime: 1 * time.Minute,
		// },
		// test 4 users, 1 active max
		{
			sb:                 NewScheduleBuilder(4, 2, (6000/4)*time.Millisecond, 10*time.Second, 1*time.Minute, 2*time.Hour),
			expectedMaxActive:  2,
			expectedMinOffTime: 10 * time.Second,
			expectedMaxOffTime: 1 * time.Minute,
		},
	}

	for _, test := range tests {
		schedules := test.sb.BuildSchedules()
		maxActive, minOffTime, maxOfftime := modelSchedule(schedules, 30*time.Second)
		if maxActive > test.expectedMaxActive {
			t.Errorf("expected no more than %d active, got %d", test.expectedMaxActive, maxActive)
			t.Logf("schedules: %v", schedules)
		}
		if minOffTime < test.expectedMinOffTime {
			t.Errorf("expected off time no less than %v, got %v", test.expectedMinOffTime, minOffTime)
			t.Logf("schedules: %v", schedules)
		}
		if maxOfftime > test.expectedMaxOffTime {
			t.Errorf("expected off time no greater than %v, got %v", test.expectedMaxOffTime, maxOfftime)
			t.Logf("schedules: %v", schedules)
		}
		t.Errorf("schedules: %v", schedules)
	}
}

func modelSchedule(schedules []RunSchedule, total time.Duration) (int, time.Duration, time.Duration) {

	var minOffTime, maxOffTime time.Duration

	// combile all the schedules into one
	allTheOns := make(RunSchedule, 0)
	for _, schedule := range schedules {
		lastOffTime := time.Duration(-1)
		for _, runInterval := range schedule {
			if lastOffTime != -1 {
				offTime := runInterval.start - lastOffTime
				if offTime > maxOffTime {
					maxOffTime = offTime
				}
				if offTime < minOffTime || minOffTime == 0 {
					minOffTime = offTime
				}
			}
			allTheOns = append(allTheOns, runInterval)
			lastOffTime = runInterval.end
		}
	}

	// now that we have all of the on ranges, sort them
	sort.Sort(allTheOns)

	// now walk through them, keeping track of how many are on at once
	maxActive := 0

	endTimes := make([]time.Duration, 0)
	currentTime := time.Duration(0)
	for _, r := range allTheOns {
		currentTime = r.start

		newEndTimes := make([]time.Duration, 0)
		// add the new end time
		newEndTimes = append(newEndTimes, r.end)

		// keep the end times that are still in the future
		for _, et := range endTimes {
			if et > currentTime {
				newEndTimes = append(newEndTimes, et)
			}
		}

		if len(newEndTimes) > maxActive {
			maxActive = len(newEndTimes)
		}
		endTimes = newEndTimes
	}

	return maxActive, minOffTime, maxOffTime
}
