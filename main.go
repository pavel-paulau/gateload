package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/couchbaselabs/gateload/api"
	"github.com/couchbaselabs/gateload/workload"
)

const (
	AUTH_TYPE_SESSION = "session"
	AUTH_TYPE_BASIC   = "basic"
)

func main() {

	if os.Getenv("GOMAXPROCS") == "" {
		log.Printf("Setting GOMAXPROCS to %v", runtime.NumCPU())
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		log.Printf("GOMAXPROCS is set at %v", os.Getenv("GOMAXPROCS"))
	}

	// start up a goroutine that will write the expvars to a file
	go func() {
		<-time.After(time.Second * 60)
		writeExpvarsToFile()
	}()

	// start up an http server, just to serve up expvars
	go http.ListenAndServe(":9876", nil)

	var config workload.Config
	workload.ReadConfig(&config)

	admin := api.SyncGatewayClient{}
	admin.Init(
		config.Hostname,
		config.Database,
		config.Port,
		config.AdminPort,
		config.LogRequests,
	)
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
	userIterator := workload.UserIterator(
		config.NumPullers,
		config.NumPushers,
		config.UserOffset,
		config.ChannelActiveUsers,
		config.ChannelConcurrentUsers,
		config.MinUserOffTimeMs,
		config.MaxUserOffTimeMs,
		rampUpDelay,
		config.RunTimeMs,
	)
	adminWg := sync.WaitGroup{}
	worker := func() {
		defer adminWg.Done()
		for user := range userIterator {
			createSession(&admin, user, config)
			pendingUsers <- user
		}
	}

	for i := 0; i < 200; i++ {
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

	// capture final snapshot of expvars
	writeExpvarsToFile()

}

func createSession(admin *api.SyncGatewayClient, user *workload.User, config workload.Config) {

	userMeta := api.UserAuth{
		Name:          user.Name,
		Password:      config.Password,
		AdminChannels: []string{user.Channel},
	}
	admin.AddUser(user.Name, userMeta, user.Type)

	if config.AuthType == AUTH_TYPE_SESSION {

		session := api.Session{Name: user.Name, TTL: 2592000} // 1 month
		log.Printf("====== Creating new session for %s (%s)", user.Type, user.Name)
		user.Cookie = admin.CreateSession(user.Name, session)
		log.Printf("====== Done Creating new session for %s (%s)", user.Type, user.Name)

	}

}

func runUser(user *workload.User, config workload.Config, wg *sync.WaitGroup) {
	c := api.SyncGatewayClient{}
	c.Init(
		config.Hostname,
		config.Database,
		config.Port,
		config.AdminPort,
		config.LogRequests,
	)
	if config.AuthType == AUTH_TYPE_SESSION {
		c.AddCookie(&user.Cookie)
	} else {
		c.AddUsername(user.Name)
		c.AddPassword(config.Password)
	}

	log.Printf("Starting new %s (%s)", user.Type, user.Name)
	if user.Type == "pusher" {
		go workload.RunNewPusher(
			user.Schedule,
			user.Name,
			&c,
			user.Channel,
			config.DocSize,
			config.SendAttachment,
			config.DocSizeDistribution,
			user.SeqId,
			config.SleepTimeMs,
			wg,
			config.AddDocToTargetUser,
		)
	} else {
		go workload.RunNewPuller(
			user.Schedule,
			&c,
			user.Channel,
			user.Name,
			config.FeedType,
			wg,
		)
	}
	log.Printf("------ Done Starting new %s (%s)", user.Type, user.Name)

}

// At the end of the run, write the full list of expvars to a file
func writeExpvarsToFile() {

	// read http
	url := "http://localhost:9876/debug/vars"
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error writing expvars, failed connection to: %v", url)
	}
	defer resp.Body.Close()

	// write to file
	destFileName := "gateload_expvars.json"
	destFile, err := os.Create(destFileName)
	if err != nil {
		log.Fatalf("Error opening file for writing to :%v", destFileName)
	}
	_, err = io.Copy(destFile, resp.Body)
	if err != nil {
		log.Fatalf("Error copying from %v -> %v", url, destFile)
	}

	log.Printf("Wrote results to %v", destFileName)

}
