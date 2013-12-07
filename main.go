package main

import (
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/pavel-paulau/gateload/api"
	"github.com/pavel-paulau/gateload/workload"
)

func createSession(admin *api.SyncGatewayClient, user workload.User, config workload.Config) http.Cookie {
	userMeta := api.UserAuth{Name: user.Name, Password: "password", AdminChannels: []string{user.Channel}}
	admin.AddUser(user.Name, userMeta)

	session := api.Session{Name: user.Name, TTL: 2592000} // 1 month
	return admin.CreateSession(user.Name, session)
}

func runUser(user workload.User, config workload.Config, cookie http.Cookie, wg *sync.WaitGroup) {
	c := api.SyncGatewayClient{}
	c.Init(config.Hostname, config.Database)
	c.AddCookie(&cookie)

	log.Printf("Starting new %s (%s)", user.Type, user.Name)
	if user.Type == "pusher" {
		go workload.RunPusher(&c, user.Channel, config.DocSize, user.SeqId, config.SleepTimeMs, wg)
	} else {
		go workload.RunPuller(&c, user.Channel, user.Name, wg)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	var config workload.Config
	workload.ReadConfig(&config)

	admin := api.SyncGatewayClient{}
	admin.Init(config.Hostname, config.Database)

	users := [][]interface{}{}
	for user := range workload.UserIterator(config.NumPullers, config.NumPushers) {
		cookie := createSession(&admin, user, config)
		users = append(users, []interface{}{user, cookie})
	}

	rampUpDelay := config.RampUpIntervalMs / (config.NumPullers + config.NumPushers)
	rampUpDelayMs := time.Duration(rampUpDelay) * time.Millisecond
	wg := sync.WaitGroup{}
	for _, user := range users {
		wg := sync.WaitGroup{}
		go runUser(user[0].(workload.User), config, user[1].(http.Cookie), &wg)
		wg.Add(1)
		time.Sleep(rampUpDelayMs)
	}

	if config.RunTimeMs > 0 {
		time.Sleep(time.Duration(config.RunTimeMs-config.RampUpIntervalMs) * time.Millisecond)
		log.Println("Shutting down clients")
	} else {
		wg.Wait()
	}
}
