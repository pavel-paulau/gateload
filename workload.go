package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math"
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

type Doc struct {
	Id       string            `json:"_id"`
	Channels []string          `json:"channels"`
	Data     map[string]string `json:"data"`
}

func DocIterator(start, end int, size int, channel string) <-chan Doc {
	ch := make(chan Doc)
	go func() {
		for i := start; i < end; i++ {
			docid := Hash(strconv.FormatInt(int64(i), 10))
			doc := Doc{
				Id:       docid,
				Channels: []string{channel},
				Data:     map[string]string{docid: RandString(docid, size)},
			}
			ch <- doc
		}
		close(ch)
	}()
	return ch
}

const DocsPerUser = 1000000

func RunPusher(c *SyncGatewayClient, channel string, size, seqId, sleepTime int, wg *sync.WaitGroup) {
	defer wg.Done()

	for doc := range DocIterator(seqId*DocsPerUser, (seqId+1)*DocsPerUser, size, channel) {
		c.PutSingleDoc(doc.Id, doc)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
}

const MaxRevsToGetInBulk = 50

func RevsIterator(ids []string) <-chan map[string][]map[string]string {
	ch := make(chan map[string][]map[string]string)

	numRevsToGetInBulk := float64(len(ids))
	numRevsGotten := 0
	go func() {
		for numRevsToGetInBulk > 0 {
			bulkSize := int(math.Min(numRevsToGetInBulk, MaxRevsToGetInBulk))
			docs := []map[string]string{}
			for _, id := range ids[numRevsGotten : numRevsGotten+bulkSize] {
				docs = append(docs, map[string]string{"id": id})
			}
			ch <- map[string][]map[string]string{"docs": docs}

			numRevsGotten += bulkSize
			numRevsToGetInBulk -= float64(bulkSize)
		}
		close(ch)
	}()
	return ch
}

func RunPuller(c *SyncGatewayClient, channel string, wg *sync.WaitGroup) {
	defer wg.Done()

	lastSeq := fmt.Sprintf("%s:%s", channel, c.GetLastSeq())
	for {
		feed := c.GetChangesFeed("longpoll", lastSeq)
		lastSeq = feed["last_seq"].(string)

		ids := []string{}
		for _, doc := range feed["results"].([]interface{}) {
			ids = append(ids, doc.(map[string]interface{})["id"].(string))
		}
		if len(ids) == 1 {
			go c.GetSingleDoc(ids[0])
		} else {
			for docs := range RevsIterator(ids) {
				c.GetBulkDocs(docs)
			}
		}
	}
}
