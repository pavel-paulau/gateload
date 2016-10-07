package workload

import (
	"crypto/md5"
	"encoding/hex"
	"expvar"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/couchbaselabs/gateload/api"
	"github.com/samuel/go-metrics/metrics"
	"gopkg.in/alexcesaro/statsd.v2"
)

var kStatsPercentiles = []float64{0.5, 0.75, 0.9, 0.95, 0.99}
var kPercentileNames = []string{"p50", "p75", "p90", "p95", "p99"}

var glExpvars = expvar.NewMap("gateload")

var (
	GlConfig  *Config
	statsdQuitChannel chan struct{}
	opshistos = map[string]metrics.Histogram{}
	histosMu  = sync.Mutex{}

	expOpsHistos *expvar.Map

	// keep track of pushers and pullers
	Pullers = []User{}
	Pushers = []User{}
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
	SeqId    int
	Type     string
	Name     string
	Channel  string
	Cookie   http.Cookie
	Schedule RunSchedule
}

// Generates all the users that will be used by this gateload run.  Each new user is added
// to the chan that is returned.
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
				scheduleBuilder := NewScheduleBuilder(
					ChannelActiveUsers,
					ChannelConcurrentUsers,
					time.Duration(RampUpDelay)*time.Millisecond,
					time.Duration(MinUserOffTimeMs)*time.Millisecond,
					time.Duration(MaxUserOffTimeMs)*time.Millisecond,
					time.Duration(RunTimeMs)*time.Millisecond,
				)
				schedules = scheduleBuilder.BuildSchedules()

				lastChannel = currChannel
				channelUserNum = 0
			}
			user := &User{
				SeqId:    currUser,
				Type:     usersTypes[randSeq[currUser-UserOffset]],
				Name:     fmt.Sprintf("user-%v", currUser),
				Channel:  fmt.Sprintf("channel-%v", currChannel),
				Schedule: schedules[channelUserNum],
			}
			ch <- user
			saveUser(user)
			channelUserNum++
		}
		close(ch)
	}()
	return ch
}

func saveUser(user *User) {
	switch user.Type {
	case "pusher":
		Pushers = append(Pushers, *user)
	case "puller":
		Pullers = append(Pullers, *user)
	default:
		panic("Unknown user type")
	}
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

func RunNewPusher(schedule RunSchedule, name string, c *api.SyncGatewayClient, channel string, size int, sendAttachment bool, dist DocSizeDistribution, seqId, sleepTime int, wg *sync.WaitGroup, addTargetUser bool) {
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
		Log("Error starting document pusher: %v", err)
		return
	}

	docIterator := DocIterator(
		seqId*DocsPerUser,
		(seqId+1)*DocsPerUser,
		docSizeGenerator,
		channel,
		sendAttachment,
		addTargetUser,
	)

	docsToSend := 0

	online := false
	scheduleIndex := 0
	start := time.Now()
	timer := time.NewTimer(schedule[scheduleIndex].start)
	var lastSend time.Time
	var pushTimer <-chan time.Time

	for {
		select {
		case <-timer.C:
			// timer went off, transition modes
			timeOffset := time.Since(start)
			if online {
				glExpvars.Add("user_awake", -1)
				online = false
				pushTimer = nil
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
				pushTimer = time.NewTimer(0).C
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
		case <-pushTimer:

			if lastSend.IsZero() {
				docsToSend = 1
			} else {
				//log.Printf("time since last %v", time.Since(lastSend))
				//log.Printf("durration: %v", (time.Duration(sleepTime) * time.Millisecond))
				docsToSend = int(time.Since(lastSend) / (time.Duration(sleepTime) * time.Millisecond))
				//log.Printf("docs to send: %v", docsToSend)
			}
			if docsToSend > 0 {
				Log("Pusher online sending %d docs", docsToSend)
				// generate docs
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
				Log("Pusher #%d saved %d docs", seqId, docsToSend)
				if c.PostBulkDocs(bulkDocs) {
					glExpvars.Add("total_doc_pushed", int64(docsToSend))
				} else {
					glExpvars.Add("total_doc_failed_to_push", int64(docsToSend))
				}
				docsToSend = 0
				lastSend = time.Now()
				// reset the timer
				pushTimer = time.NewTimer(time.Duration(sleepTime) * time.Millisecond).C
			}
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
			glExpvars.Add("total_doc_failed_to_pull", 1)
		} else {
			glExpvars.Add("total_doc_pulled", 1)
		}
	} else {
		if !c.GetBulkDocs(docs, wakeup) {
			docs = nil
			glExpvars.Add("total_doc_failed_to_pull", int64(len(docs)))
		} else {
			glExpvars.Add("total_doc_pulled", int64(len(docs)))
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

	// lastSeqFloat := c.GetLastSeq()
	// harcode to 0 due to https://github.com/couchbase/sync_gateway/issues/1159#issuecomment-142731185

	lastSeqFloat := 0

	if lastSeqFloat < 0 {
		Log("Puller, unable to establish last sequence number, exiting")
		return
	}
	lastSeq = lastSeqFloat
	if lastSeqFloat > MaxFirstFetch {
		//FIX: This generates a sequence ID using internal knowledge of the gateway's sequence format.
		lastSeq = lastSeqFloat - MaxFirstFetch // (for use with simple_sequences branch)
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
			Log("Puller %s done reading %d docs", name, nDocs)
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
	if err != nil {
		histo := clientHTTPHisto(opname + "Errors")
		histo.Update(int64(duration))
	} else {
		histo := clientHTTPHisto(opname)
		histo.Update(int64(duration))
	}
}

func ValidateExpvars() {

	totalDocsPulled := glExpvars.Get("total_doc_pulled")
	if totalDocsPulled == nil {
		log.Fatalf("FATAL ERROR: No docs were pulled.  Test failed")
	}

	totalDocsFailedToPush := glExpvars.Get("total_doc_failed_to_push")
	if totalDocsFailedToPush != nil {
		log.Fatalf("FATAL ERROR: %v docs failed to push.  Test failed", totalDocsFailedToPush)
	}

}

func StartStatsdClient() {
	log.Printf("startStatsdClient() called")

	if GlConfig.StatsdEnabled {
		// statsClient *should* be safe to be shared among multiple
		// goroutines, based on fact that connection returned from Dial
		log.Printf("Creating statsd client")

		c, err := statsd.New(statsd.Prefix("gateload_stats"), statsd.Address(GlConfig.StatsdEndpoint))
		if err != nil {
			// If nothing is listening on the target port, an error is returned and
			// the returned client does nothing but is still usable. So we can
			// just log the error and go on.
			log.Print(err)
		}
		defer c.Close()

		ticker := time.NewTicker(1 * time.Second)
		statsdQuitChannel := make(chan struct{})
		//Iterate over expvars and write to statsd client
		go func() {
			for {
				select {
				case <-ticker.C:
					glExpvars.Do(func(f expvar.KeyValue) {
						log.Printf("Processing glExpvar key: %v, for Type %T", f.Key, f.Value)
						switch v := f.Value.(type) {
						case *expvar.Int:
							c.Gauge(f.Key,v)
						case *expvar.Map:
							v.Do(func(g expvar.KeyValue) {
								log.Printf("Processing expvar.Map key: %v, for Type %T", g.Key, g.Value)

								switch w := g.Value.(type) {
								case *metrics.HistogramExport:
									perc := w.Histogram.Percentiles(kStatsPercentiles)
									for i, p := range perc {
										log.Printf("Processing percentile key: %v, vale: %v", f.Key+"."+g.Key+"."+kPercentileNames[i], p)
										c.Gauge(kPercentileNames[i], p)
									}
								}
							})
						}
					})
				case <-statsdQuitChannel:
					ticker.Stop()
					return
				}
			}
		}()
	}
}

func StopStatsdClient() {
	close(statsdQuitChannel)
}