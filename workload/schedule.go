package workload

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

type RunInterval struct {
	start time.Duration
	end   time.Duration
}

func (ri *RunInterval) String() string {
	return fmt.Sprintf("{start: %v - end: %v}", ri.start, ri.end)
}

type RunSchedule []*RunInterval

func (r RunSchedule) Len() int           { return len(r) }
func (r RunSchedule) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r RunSchedule) Less(i, j int) bool { return r[i].start < r[j].start }

func (r RunSchedule) String() string {
	rv := "["
	for i, ri := range r {
		if i != 0 {
			rv += ", "
		}
		rv += fmt.Sprintf("%v\n", ri)
	}
	rv += "]\n"
	return rv
}

type ScheduleBuilder struct {
	// state
	schedules     []RunSchedule
	activeUsers   map[int]bool
	inactiveUsers map[int]bool

	// configuration
	numUsers       int
	maxActiveUsers int
	rampUpDelay    time.Duration
	minOffTime     time.Duration
	maxOffTime     time.Duration
	totalRuntime   time.Duration
}

func NewScheduleBuilder(numUsers, maxActiveUsers int, rampUpDelay, minOffTime, maxOffTime, totalRuntime time.Duration) *ScheduleBuilder {
	return &ScheduleBuilder{
		schedules:      make([]RunSchedule, numUsers),
		activeUsers:    make(map[int]bool),
		inactiveUsers:  make(map[int]bool),
		numUsers:       numUsers,
		maxActiveUsers: maxActiveUsers,
		rampUpDelay:    rampUpDelay,
		minOffTime:     minOffTime,
		maxOffTime:     maxOffTime,
		totalRuntime:   totalRuntime,
	}
}

func (s *ScheduleBuilder) BuildSchedules() []RunSchedule {

	// ramp up users
	s.rampUp()

	// walk timeline activating/deactivating users
	s.walktimeline()

	return s.schedules
}

func (s *ScheduleBuilder) walktimeline() {

	for t := time.Duration(0); t < s.totalRuntime; t += 1 * time.Second {
		// see if any inactive users need to be started
		for inactiveUser, _ := range s.inactiveUsers {
			inactiveUserNextRunInterval := s.userMostRecent(inactiveUser)
			if inactiveUserNextRunInterval.start > (t-1*time.Second) && inactiveUserNextRunInterval.start <= t {
				inactiveUserNextRunInterval.start = t
				// time to start this user
				// see if we need to deactivate anyone else?
				if len(s.activeUsers) >= s.maxActiveUsers {
					oldestUser := s.oldestActiveUser(t)
					s.deactivateUser(oldestUser, t)
				}
				s.activateUser(inactiveUser)
			}
		}

	}

}

func (s *ScheduleBuilder) activateUser(i int) {
	delete(s.inactiveUsers, i)
	s.activeUsers[i] = true
}

func (s *ScheduleBuilder) deactivateUser(i int, t time.Duration) {
	delete(s.activeUsers, i)
	userRunInterval := s.userMostRecent(i)
	// close out that run interval
	userRunInterval.end = t
	// schdule next interval
	random := t + time.Duration(rand.Float64()*float64(s.maxOffTime-s.minOffTime)) + s.minOffTime
	// if the next start time is during this test, add it
	if random < s.totalRuntime {
		s.startUserAt(i, random)
	}
	s.inactiveUsers[i] = true
}

func (s *ScheduleBuilder) rampUp() {

	rampOffset := time.Duration(0)
	for i := 0; i < s.numUsers; i++ {

		if i < s.maxActiveUsers {
			// ramp up this user
			s.startUserAt(i, rampOffset)
			rampOffset += s.rampUpDelay
		} else {
			// schedule random start time within regular boundaries
			random := time.Duration(rand.Float64()*float64(s.maxOffTime-s.minOffTime)) + s.minOffTime
			s.startUserAt(i, random)
		}

		// start all users in the inactive state
		s.inactiveUsers[i] = true
	}
}

func (s *ScheduleBuilder) startUserAt(i int, t time.Duration) {
	s.schedules[i] = append(s.schedules[i], &RunInterval{start: t, end: time.Duration(-1)})
}

func (s *ScheduleBuilder) userMostRecent(i int) *RunInterval {
	return s.schedules[i][len(s.schedules[i])-1]
}

func (s *ScheduleBuilder) oldestActiveUser(t time.Duration) int {
	oldestIndex := -1
	oldestTime := time.Duration(0)
	for activeUser, _ := range s.activeUsers {
		activeUserRunInterval := s.userMostRecent(activeUser)
		activeTime := t - activeUserRunInterval.start
		if activeTime > oldestTime {
			oldestIndex = activeUser
			oldestTime = activeTime
		}
	}

	if oldestIndex == -1 {
		log.Printf("active users are: %v", s.activeUsers)
		log.Fatalf("uanble to find oldest active user at %v: %v", t, s.schedules)
	}
	return oldestIndex
}
