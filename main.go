package main

import (
	"log"
	"net/http"
	"sync"
	"time"
)

type UserAuth struct {
	Name          string   `json:"name"`
	Password      string   `json:"password"`
	AdminChannels []string `json:"admin_channels"`
}

type Session struct {
	Name string `json:"name"`
	TTL  int    `json:"ttl"`
}

func createSession(admin *SyncGatewayClient, user User, config Config) http.Cookie {
	userMeta := UserAuth{Name: user.Name, Password: "password", AdminChannels: []string{user.Channel}}
	admin.AddUser(user.Name, userMeta)

	session := Session{Name: user.Name, TTL: 2592000} // 1 month
	return admin.CreateSession(user.Name, session)
}

func runUser(user User, config Config, cookie http.Cookie, wg *sync.WaitGroup) {
	c := SyncGatewayClient{}
	c.Init(config.Hostname, config.Database)
	c.AddCookie(&cookie)

	log.Printf("Starting new %s", user.Type)
	if user.Type == "pusher" {
		go RunPusher(&c, user.Channel, config.DocSize, user.SeqId, config.SleepTimeMs, wg)
	} else {
		go RunPuller(&c, user.Channel, user.Name, wg)
	}
}

func main() {
	var config Config
	ReadConfig(&config)

	admin := SyncGatewayClient{}
	admin.Init(config.Hostname, config.Database)

	rampUpDelay := config.RampUpIntervalMs / (config.NumPullers + config.NumPushers)
	rampUpDelayMs := time.Duration(rampUpDelay) * time.Millisecond

	wg := sync.WaitGroup{}
	for user := range UserIterator(config.NumPullers, config.NumPushers) {
		t0 := time.Now()
		cookie := createSession(&admin, user, config)
		t1 := time.Now()

		go runUser(user, config, cookie, &wg)
		wg.Add(1)
		time.Sleep(rampUpDelayMs - t1.Sub(t0))
	}
	if config.RunTimeMs > 0 {
		time.Sleep(time.Duration(config.RunTimeMs-config.RampUpIntervalMs) * time.Millisecond)
		log.Println("Shutting down clients")
	} else {
		wg.Wait()
	}
}
