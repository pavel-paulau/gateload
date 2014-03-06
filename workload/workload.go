package workload

import (
	"crypto/md5"
	"encoding/hex"
	"expvar"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/couchbaselabs/gateload/api"

	"github.com/samuel/go-metrics/metrics"
)

var glExpvars = expvar.NewMap("gateload")

var (
	opshistos = map[string]metrics.Histogram{}
	histosMu  = sync.Mutex{}

	expOpsHistos *expvar.Map
)

func init() {
	api.OperationCallback = recordHTTPClientStat

	expOpsHistos = &expvar.Map{}
	expOpsHistos.Init()
	glExpvars.Set("ops", expOpsHistos)
}

func Log(fmt string, args ...interface{}) {
	if Verbose {
		log.Printf(fmt, args...)
	}
}

type User struct {
	SeqId               int
	Type, Name, Channel string
	Cookie              http.Cookie
	Schedule            RunSchedule
}

func UserIterator(NumPullers, NumPushers, UserOffset, ChannelActiveUsers, ChannelConcurrentUsers, MinUserOffTimeMs, MaxUserOffTimeMs, RampUpDelay, RunTimeMs int) <-chan *User {
	numUsers := NumPullers + NumPushers
	usersTypes := make([]string, 0, numUsers)
	for i := 0; i < NumPullers; i++ {
		usersTypes = append(usersTypes, "puller")
	}
	for i := 0; i < NumPushers; i++ {
		usersTypes = append(usersTypes, "pusher")
	}
	randSeq := rand.Perm(numUsers)

	ch := make(chan *User)
	go func() {
		lastChannel := -1
		channelUserNum := 0
		var schedules []RunSchedule
		for currUser := UserOffset; currUser < numUsers+UserOffset; currUser++ {
			currChannel := currUser / ChannelActiveUsers
			if currChannel != lastChannel {
				scheduleBuilder := NewScheduleBuilder(ChannelActiveUsers, ChannelConcurrentUsers, time.Duration(RampUpDelay)*time.Millisecond, time.Duration(MinUserOffTimeMs)*time.Millisecond, time.Duration(MaxUserOffTimeMs)*time.Millisecond, time.Duration(RunTimeMs)*time.Millisecond)
				schedules = scheduleBuilder.BuildSchedules()

				lastChannel = currChannel
				channelUserNum = 0
			}
			ch <- &User{
				SeqId:    currUser,
				Type:     usersTypes[randSeq[currUser-UserOffset]],
				Name:     fmt.Sprintf("user-%v", currUser),
				Channel:  fmt.Sprintf("channel-%v", currChannel),
				Schedule: schedules[channelUserNum],
			}
			channelUserNum++
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

const DocsPerUser = 1000000

func RunScheduleFollower(schedule RunSchedule, name string, wg *sync.WaitGroup) {
	defer wg.Done()

	online := false
	scheduleIndex := 0
	start := time.Now()
	timer := time.NewTimer(schedule[scheduleIndex].start)

	for {
		select {
		case <-timer.C:
			// timer went off, transition modes
			timeOffset := time.Since(start)
			if online {
				online = false
				scheduleIndex++
				if scheduleIndex < len(schedule) {
					timer = time.NewTimer(schedule[scheduleIndex].start - timeOffset)
				}
			} else {
				online = true
				if schedule[scheduleIndex].end != -1 {
					timer = time.NewTimer(schedule[scheduleIndex].end - timeOffset)
				}
			}
		default:
			//log.Printf("client %s, online: %v", name, online)
			time.Sleep(500 * time.Millisecond)
		}
	}

}

func RunNewPusher(schedule RunSchedule, name string, c *api.SyncGatewayClient, channel string, size int, dist DocSizeDistribution, seqId, sleepTime int, wg *sync.WaitGroup) {
	defer wg.Done()

	glExpvars.Add("user_active", 1)
	// if config contains DocSize, always generate this fixed document size
	if size != 0 {
		dist = DocSizeDistribution{
			&DocSizeDistributionElement{
				Prob:    100,
				MinSize: size,
				MaxSize: size,
			},
		}
	}

	docSizeGenerator, err := NewDocSizeGenerator(dist)
	if err != nil {
		Log("Error starting docuemnt pusher: %v", err)
		return
	}

	docIterator := DocIterator(seqId*DocsPerUser, (seqId+1)*DocsPerUser, docSizeGenerator, channel)
	docsToSend := 0

	online := false
	scheduleIndex := 0
	start := time.Now()
	timer := time.NewTimer(schedule[scheduleIndex].start)
	var lastSend time.Time

	for {
		select {
		case <-timer.C:
			// timer went off, transition modes
			timeOffset := time.Since(start)
			if online {
				glExpvars.Add("user_awake", -1)
				online = false
				scheduleIndex++
				if scheduleIndex < len(schedule) {
					nextOnIn := schedule[scheduleIndex].start - timeOffset
					timer = time.NewTimer(nextOnIn)
					Log("Pusher %s going offline, next on at %v", name, nextOnIn)
					if nextOnIn < 0 {
						log.Printf("WARNING: pusher %s negative timer nextOnTime, exiting", name)
						return
					}
				}
			} else {
				glExpvars.Add("user_awake", 1)
				online = true
				if schedule[scheduleIndex].end != -1 {
					nextOffIn := schedule[scheduleIndex].end - timeOffset
					timer = time.NewTimer(nextOffIn)
					Log("Pusher %s going online, next off at %v", name, nextOffIn)
					if nextOffIn < 0 {
						log.Printf("WARNING: pusher %s negative timer nextOffTime, exiting", name)
						glExpvars.Add("user_awake", -1)
						return
					}
				}
			}
		default:

			if online {
				if lastSend.IsZero() {
					docsToSend = 1
				} else {
					//log.Printf("time since last %v", time.Since(lastSend))
					//log.Printf("durration: %v", (time.Duration(sleepTime) * time.Millisecond))
					docsToSend = int(time.Since(lastSend) / (time.Duration(sleepTime) * time.Millisecond))
					//log.Printf("docs to send: %v", docsToSend)
				}
				if docsToSend > 0 {
					Log("Pusher online sending %d", docsToSend)
					// generage docs
					docs := make([]api.Doc, docsToSend)
					for i := 0; i < docsToSend; i++ {
						nextDoc := <-docIterator
						docs[i] = nextDoc
					}
					// send revs diff
					revsDiff := map[string][]string{}
					for _, doc := range docs {
						revsDiff[doc.Id] = []string{doc.Rev}
					}

					c.PostRevsDiff(revsDiff)
					// set the creation time in doc id
					nowString := "_" + strconv.Itoa(int(time.Now().UnixNano()/1e6)) // time since epoch in ms as string
					for i, doc := range docs {
						doc.Id = doc.Id + nowString
						docs[i] = doc
					}
					// send bulk docs
					bulkDocs := map[string]interface{}{
						"docs":      docs,
						"new_edits": false,
					}
					c.PostBulkDocs(bulkDocs)
					Log("Pusher #%d saved %d docs", seqId, docsToSend)
					docsToSend = 0
					lastSend = time.Now()
				}
			}
			time.Sleep(time.Millisecond)
		}
	}

	glExpvars.Add("user_active", -1)

}

// Max number of old revisions to pull when a user's puller first starts.
const MaxFirstFetch = 200

// Given a set of changes, downloads the associated revisions.
func pullChanges(c *api.SyncGatewayClient, changes []*api.Change, wakeup time.Time) (int, interface{}) {
	docs := []api.BulkDocsEntry{}
	var newLastSeq interface{}
	for _, change := range changes {
		newLastSeq = change.Seq
		for _, changeItem := range change.Changes {
			bulk := api.BulkDocsEntry{ID: change.ID, Rev: changeItem.Rev}
			docs = append(docs, bulk)
		}
	}
	if len(docs) == 1 {
		if !c.GetSingleDoc(docs[0].ID, docs[0].Rev, wakeup) {
			docs = nil
		}
	} else {
		if !c.GetBulkDocs(docs, wakeup) {
			docs = nil
		}
	}
	return len(docs), newLastSeq
}

// Delay between receiving first change and GETting the doc(s), to allow for batching.
const FetchDelay = time.Duration(1000) * time.Millisecond

// Delay after saving docs before saving a checkpoint to the server.
const CheckpointInterval = time.Duration(5000) * time.Millisecond

func RunNewPuller(schedule RunSchedule, c *api.SyncGatewayClient, channel, name, feedType string, wg *sync.WaitGroup) {
	defer wg.Done()

	glExpvars.Add("user_active", 1)
	var wakeupTime = time.Now()

	var lastSeq interface{}
	if c.GetLastSeq() > MaxFirstFetch {
		//FIX: This generates a sequence ID using internal knowledge of the gateway's sequence format.
		lastSeq = fmt.Sprintf("%s:%d", channel, int(math.Max(c.GetLastSeq()-MaxFirstFetch, 0)))
		//lastSeq = c.GetLastSeq() - MaxFirstFetch	// (for use with simple_sequences branch)
	}
	var changesFeed <-chan *api.Change
	var changesResponse *http.Response
	var cancelChangesFeed *bool

	var pendingChanges []*api.Change
	var fetchTimer <-chan time.Time

	var checkpointSeqId int64 = 0
	var checkpointTimer <-chan time.Time

	online := false
	scheduleIndex := 0
	start := time.Now()
	timer := time.NewTimer(schedule[scheduleIndex].start)
	Log("Puller %s first transition at %v", name, schedule[scheduleIndex].start)

outer:
	for {
		select {
		case <-timer.C:
			// timer went off, transition modes
			timeOffset := time.Since(start)
			if online {
				glExpvars.Add("user_awake", -1)
				online = false
				scheduleIndex++
				if scheduleIndex < len(schedule) {
					nextOnIn := schedule[scheduleIndex].start - timeOffset
					timer = time.NewTimer(nextOnIn)
					Log("Puller %s going offline, next on at %v", name, nextOnIn)
					if nextOnIn < 0 {
						log.Printf("WARNING: puller negative timer, exiting")
						return
					}
				} else {
					Log("Puller %s going offline, for good", name)
				}

				// transitioning off, cancel the changes feed, nil our changes feed channel
				*cancelChangesFeed = false
				if changesResponse != nil {
					changesResponse.Body.Close()
				}
				changesFeed = nil
				fetchTimer = nil
				checkpointTimer = nil
				pendingChanges = nil
			} else {
				glExpvars.Add("user_awake", 1)
				online = true
				if schedule[scheduleIndex].end != -1 {
					nextOffIn := schedule[scheduleIndex].end - timeOffset
					timer = time.NewTimer(nextOffIn)
					Log("Puller %s going online, next off at %v", name, nextOffIn)
					if nextOffIn < 0 {
						log.Printf("WARNING: puller negative timer, exiting")
						glExpvars.Add("user_awake", -1)
						return
					}
				} else {
					Log("Puller %s going online, for good", name)
				}

				// reset our wakeupTime to now
				wakeupTime = time.Now()
				Log("new wakeup time %v", wakeupTime)

				// transitioning on, start a changes feed
				changesFeed, cancelChangesFeed, changesResponse = c.GetChangesFeed(feedType, lastSeq)
				Log("** Puller %s watching changes using %s feed...", name, feedType)
			}
		case change, ok := <-changesFeed:
			// Received a change from the feed:
			if !ok {
				break outer
			}
			Log("Puller %s received %+v", name, *change)
			pendingChanges = append(pendingChanges, change)
			if fetchTimer == nil {
				fetchTimer = time.NewTimer(FetchDelay).C
			}
		case <-fetchTimer:
			// Time to get documents from the server:
			fetchTimer = nil
			var nDocs int
			nDocs, lastSeq = pullChanges(c, pendingChanges, wakeupTime)
			pendingChanges = nil
			Log("Puller %s read %d docs", name, nDocs)
			if nDocs > 0 && checkpointTimer == nil {
				checkpointTimer = time.NewTimer(CheckpointInterval).C
			}
		case <-checkpointTimer:
			// Time to save a checkpoint:
			checkpointTimer = nil
			checkpoint := api.Checkpoint{LastSequence: lastSeq}
			checkpointHash := fmt.Sprintf("%s-%s", name, Hash(strconv.FormatInt(checkpointSeqId, 10)))
			// save checkpoint asynchronously
			go c.SaveCheckpoint(checkpointHash, checkpoint)
			checkpointSeqId += 1
			Log("Puller %s saved remote checkpoint", name)
		}
	}

}

func clientHTTPHisto(name string) metrics.Histogram {
	histosMu.Lock()
	defer histosMu.Unlock()
	rv, ok := opshistos[name]
	if !ok {
		rv = metrics.NewBiasedHistogram()
		opshistos[name] = rv

		expOpsHistos.Set(name, &metrics.HistogramExport{rv,
			[]float64{0.25, 0.5, 0.75, 0.90, 0.95, 0.99},
			[]string{"p25", "p50", "p75", "p90", "p95", "p99"}})
	}
	return rv
}

func recordHTTPClientStat(opname string, start time.Time, err error) {
	duration := time.Since(start)
	histo := clientHTTPHisto(opname)
	histo.Update(int64(duration))
}
