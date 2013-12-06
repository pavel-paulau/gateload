package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type User struct {
	SeqId               int
	Type, Name, Channel string
}

const ChannelQuota = 40

func UserIterator(NumPullers, NumPushers int) <-chan User {
	numUsers := NumPullers + NumPushers
	usersTypes := make([]string, 0, numUsers)
	for i := 0; i < NumPullers; i++ {
		usersTypes = append(usersTypes, "puller")
	}
	for i := 0; i < NumPushers; i++ {
		usersTypes = append(usersTypes, "pusher")
	}
	randSeq := rand.Perm(numUsers)

	ch := make(chan User)
	go func() {
		for currUser := 0; currUser < numUsers; currUser++ {
			currChannel := currUser / ChannelQuota
			ch <- User{
				SeqId:   currUser,
				Type:    usersTypes[randSeq[currUser]],
				Name:    fmt.Sprintf("user-%v", currUser),
				Channel: fmt.Sprintf("channel-%v", currChannel),
			}
		}
		close(ch)
	}()
	return ch
}

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

	wg := sync.WaitGroup{}
	for user := range UserIterator(config.NumPullers, config.NumPushers) {
		cookie := createSession(&admin, user, config)
		go runUser(user, config, cookie, &wg)
		wg.Add(1)
		time.Sleep(time.Duration(rampUpDelay) * time.Millisecond)
	}
	if config.RunTimeMs > 0 {
		time.Sleep(time.Duration(config.RunTimeMs-config.RampUpIntervalMs) * time.Millisecond)
		log.Println("Shutting down clients")
	} else {
		wg.Wait()
	}
}
