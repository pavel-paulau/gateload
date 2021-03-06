package workload

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/pavel-paulau/gateload/api"
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

func DocIterator(start, end int, size int, channel string) <-chan api.Doc {
	ch := make(chan api.Doc)
	go func() {
		for i := start; i < end; i++ {
			docid := Hash(strconv.FormatInt(int64(i), 10))
			rev := Hash(strconv.FormatInt(int64(i*i), 10))
			doc := api.Doc{
				Id:        docid,
				Rev:       fmt.Sprintf("1-%s", rev),
				Channels:  []string{channel},
				Data:      map[string]string{docid: RandString(docid, size)},
				Revisions: map[string]interface{}{"ids": []string{rev}, "start": 1},
			}
			ch <- doc
		}
		close(ch)
	}()
	return ch
}

const DocsPerUser = 1000000

func RunPusher(c *api.SyncGatewayClient, channel string, size, seqId, sleepTime int, wg *sync.WaitGroup) {
	defer wg.Done()

	for doc := range DocIterator(seqId*DocsPerUser, (seqId+1)*DocsPerUser, size, channel) {
		revsDiff := map[string][]string{
			doc.Id: []string{doc.Rev},
		}
		c.PostRevsDiff(revsDiff)
		docs := map[string]interface{}{
			"docs":      []api.Doc{doc},
			"new_edits": false,
		}
		c.PostBulkDocs(docs)
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

const MaxFirstFetch = 200

func readFeed(c *api.SyncGatewayClient, feedType, lastSeq string) string {
	feed := c.GetChangesFeed(feedType, lastSeq)

	ids := []string{}
	for _, doc := range feed["results"].([]interface{}) {
		ids = append(ids, doc.(map[string]interface{})["id"].(string))
	}
	if len(ids) == 1 {
		c.GetSingleDoc(ids[0])
	} else {
		for docs := range RevsIterator(ids) {
			c.GetBulkDocs(docs)
		}
	}

	return feed["last_seq"].(string)
}

const CheckpointInverval = time.Duration(5000) * time.Millisecond

func RunPuller(c *api.SyncGatewayClient, channel, name string, wg *sync.WaitGroup) {
	defer wg.Done()

	lastSeq := fmt.Sprintf("%s:%d", channel, int(math.Max(c.GetLastSeq()-MaxFirstFetch, 0)))
	lastSeq = readFeed(c, "normal", lastSeq)

	checkpointSeqId := int64(0)
	for {
		timer := time.AfterFunc(CheckpointInverval, func() {
			checkpoint := api.Checkpoint{LastSequence: lastSeq}
			chechpointHash := fmt.Sprintf("%s-%s", name, Hash(strconv.FormatInt(checkpointSeqId, 10)))
			c.SaveCheckpoint(chechpointHash, checkpoint)
			checkpointSeqId += 1
		})
		lastSeq = readFeed(c, "longpoll", lastSeq)
		timer.Stop()
	}
}
