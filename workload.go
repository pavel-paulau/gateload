package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

func Hash(inString string) string {
	h := md5.New()
	h.Write([]byte(inString))
	return hex.EncodeToString(h.Sum(nil))
}

func RandString(key string, expectedLength int) string {
	var randString string
	if expectedLength > 64 {
		baseString := RandString(key, expectedLength/2)
		randString = baseString + baseString
	} else {
		randString = (Hash(key) + Hash(key[:len(key)-1]))[:expectedLength]
	}
	return randString
}

func DocIterator(start, end int, size int, channel string) <-chan map[string]interface{} {
	ch := make(chan map[string]interface{})
	go func() {
		for i := start; i < end; i++ {
			docid := Hash(strconv.FormatInt(int64(i), 10))
			doc := map[string]interface{}{
				"_id":      docid,
				"channels": []string{channel},
				"data": map[string]string{
					docid: RandString(docid, size),
				},
			}
			ch <- doc
		}
		close(ch)
	}()
	return ch
}

const ChannelQuota = 40

type User struct {
	SeqId               int
	Type, Name, Channel string
}

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
				currUser,
				usersTypes[randSeq[currUser]],
				fmt.Sprintf("user-%v", currUser),
				fmt.Sprintf("channel-%v", currChannel),
			}
		}
		close(ch)
	}()
	return ch
}

const DocsPerUser = 1000000

func RunPusher(c *SyncGatewayClient, channel string, size, seqId, sleepTime int, wg *sync.WaitGroup) {
	defer wg.Done()

	for doc := range DocIterator(seqId*DocsPerUser, (seqId+1)*DocsPerUser, size, channel) {
		c.PutSingleDoc(doc["_id"].(string), doc)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
}

func RunPuller(c *SyncGatewayClient, channel string, wg *sync.WaitGroup) {
	defer wg.Done()

	lastSeq := fmt.Sprintf("%s:%s", channel, c.GetLastSeq())
	for {
		feed := c.GetChangesFeed(lastSeq)
		lastSeq = feed["last_seq"].(string)

		ids := []string{}
		for _, doc := range feed["results"].([]interface{}) {
			ids = append(ids, doc.(map[string]interface{})["id"].(string))
		}
		if len(ids) == 1 {
			go c.GetSingleDoc(ids[0])
		} else {
			docs := []map[string]string{}
			for _, id := range ids {
				docs = append(docs, map[string]string{"id": id})
			}
			c.GetBulkDocs(map[string][]map[string]string{"docs": docs})
		}
	}
}
