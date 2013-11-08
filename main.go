package main

import (
	"log"
	"sync"
	"time"
)

func runUser(user User, config Config, wg *sync.WaitGroup) {
	c := SyncGatewayClient{}
	c.Init(config.Hostname, config.Database)

	userMeta := map[string]interface{}{
		"name":           user.Name,
		"password":       "password",
		"admin_channels": []string{user.Channel},
	}
	c.AddUser(userMeta["name"].(string), userMeta)

	sessionMeta := map[string]interface{}{
		"name": user.Name,
		"ttl":  2592000, // 1 month
	}
	cookie := c.CreateSession(userMeta["name"].(string), sessionMeta)
	c.AddCookie(&cookie)

	log.Printf("Starting new %s", user.Type)
	if user.Type == "pusher" {
		go RunPusher(&c, user.Channel, config.DocSize, user.SeqId, config.SleepTimeMs, wg)
	} else {
		go RunPuller(&c, user.Channel, wg)
	}
}

func main() {
	var config Config
	ReadConfig(&config)

	rampUpDelay := config.RampUpIntervalMs / (config.NumPullers + config.NumPushers)

	wg := sync.WaitGroup{}
	for user := range UserIterator(config.NumPullers, config.NumPushers) {
		go runUser(user, config, &wg)
		wg.Add(1)
		time.Sleep(time.Duration(rampUpDelay) * time.Millisecond)
	}
	wg.Wait()
}
