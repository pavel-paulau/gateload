package main

import (
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/couchbaselabs/gateload/api"
	"github.com/couchbaselabs/gateload/workload"
)

func createSession(admin *api.SyncGatewayClient, user *workload.User, config workload.Config) {
	userMeta := api.UserAuth{Name: user.Name, Password: "password", AdminChannels: []string{user.Channel}}
	admin.AddUser(user.Name, userMeta)

	session := api.Session{Name: user.Name, TTL: 2592000} // 1 month
	user.Cookie = admin.CreateSession(user.Name, session)
}

func runUser(user *workload.User, config workload.Config, wg *sync.WaitGroup) {
	c := api.SyncGatewayClient{}
	c.Init(config.Hostname, config.Database)
	c.AddCookie(&user.Cookie)

	log.Printf("Starting new %s (%s)", user.Type, user.Name)
	if user.Type == "pusher" {
		go workload.RunNewPusher(user.Schedule, user.Name, &c, user.Channel, config.DocSize, config.DocSizeDistribution, user.SeqId, config.SleepTimeMs, wg)
	} else {
		go workload.RunNewPuller(user.Schedule, &c, user.Channel, user.Name, config.FeedType, wg)
	}

}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// start up an http server, just to serve up expvars
	go http.ListenAndServe(":9876", nil)

	var config workload.Config
	workload.ReadConfig(&config)

	admin := api.SyncGatewayClient{}
	admin.Init(config.Hostname, config.Database)
	if !admin.Valid() {
		log.Fatalf("unable to connect to sync_gateway, check the hostname and database")
	}

	pendingUsers := make(chan *workload.User)
	users := make([]*workload.User, config.NumPullers+config.NumPushers)

	// start a routine to place pending users into array
	go func() {
		for pendingUser := range pendingUsers {

			// users = append(users, pendingUser)
			users[pendingUser.SeqId-config.UserOffset] = pendingUser
		}
	}()

	rampUpDelay := config.RampUpIntervalMs / (config.NumPullers + config.NumPushers)
	// use a fixed number of workers to create the users/sessions
	userIterator := workload.UserIterator(config.NumPullers, config.NumPushers, config.UserOffset, config.ChannelActiveUsers, config.ChannelConcurrentUsers, config.MinUserOffTimeMs, config.MaxUserOffTimeMs, rampUpDelay, config.RunTimeMs)
	adminWg := sync.WaitGroup{}
	worker := func() {
		defer adminWg.Done()
		for user := range userIterator {
			createSession(&admin, user, config)
			pendingUsers <- user
		}
	}

	for i := 0; i < 16; i++ {
		adminWg.Add(1)
		go worker()
	}

	// wait for all the workers to finish
	adminWg.Wait()
	// close the pending users channel to free that routine
	close(pendingUsers)

	numChannels := (config.NumPullers + config.NumPushers) / config.ChannelActiveUsers
	channelRampUpDelayMs := time.Duration(config.RampUpIntervalMs/numChannels) * time.Millisecond
	wg := sync.WaitGroup{}
	channel := ""
	for _, user := range users {
		nextChannel := user.Channel
		if channel != nextChannel {
			if channel != "" {
				time.Sleep(channelRampUpDelayMs)
			}
			channel = nextChannel
		}
		wg := sync.WaitGroup{}
		go runUser(user, config, &wg)
		wg.Add(1)
	}

	if config.RunTimeMs > 0 {
		time.Sleep(time.Duration(config.RunTimeMs-config.RampUpIntervalMs) * time.Millisecond)
		log.Println("Shutting down clients")
	} else {
		wg.Wait()
	}
}
