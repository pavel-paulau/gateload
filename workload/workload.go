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
}

const ChannelQuota = 40

func UserIterator(NumPullers, NumPushers, UserOffset int) <-chan *User {
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
		for currUser := UserOffset; currUser < numUsers+UserOffset; currUser++ {
			currChannel := currUser / ChannelQuota
			ch <- &User{
				SeqId:   currUser,
				Type:    usersTypes[randSeq[currUser-UserOffset]],
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

const DocsPerUser = 1000000

func RunPusher(c *api.SyncGatewayClient, channel string, size int, dist DocSizeDistribution, seqId, sleepTime int, wg *sync.WaitGroup) {
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

	for doc := range DocIterator(seqId*DocsPerUser, (seqId+1)*DocsPerUser, docSizeGenerator, channel) {
		revsDiff := map[string][]string{
			doc.Id: []string{doc.Rev},
		}
		c.PostRevsDiff(revsDiff)
		doc.Created = time.Now()
		docs := map[string]interface{}{
			"docs":      []api.Doc{doc},
			"new_edits": false,
		}
		c.PostBulkDocs(docs)
		Log("Pusher #%d saved doc %q", seqId, doc.Id)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
	glExpvars.Add("user_active", -1)
}

// Max number of old revisions to pull when a user's puller first starts.
const MaxFirstFetch = 200

// Given a set of changes, downloads the associated revisions.
func pullChanges(c *api.SyncGatewayClient, changes []*api.Change) (int, interface{}) {
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
		if !c.GetSingleDoc(docs[0].ID, docs[0].Rev) {
			docs = nil
		}
	} else {
		if !c.GetBulkDocs(docs) {
			docs = nil
		}
	}
	return len(docs), newLastSeq
}

// Delay between receiving first change and GETting the doc(s), to allow for batching.
const FetchDelay = time.Duration(1000) * time.Millisecond

// Delay after saving docs before saving a checkpoint to the server.
const CheckpointInterval = time.Duration(5000) * time.Millisecond

func RunPuller(c *api.SyncGatewayClient, channel, name, feedType string, wg *sync.WaitGroup) {
	defer wg.Done()

	glExpvars.Add("user_active", 1)

	var lastSeq interface{} = fmt.Sprintf("%s:%d", channel, int(math.Max(c.GetLastSeq()-MaxFirstFetch, 0)))
	changesFeed := c.GetChangesFeed(feedType, lastSeq)
	Log("** Puller %s watching changes using %s feed...", name, feedType)

	var pendingChanges []*api.Change
	var fetchTimer <-chan time.Time

	var checkpointSeqId int64 = 0
	var checkpointTimer <-chan time.Time

outer:
	for {
		select {
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
			nDocs, lastSeq = pullChanges(c, pendingChanges)
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
			c.SaveCheckpoint(checkpointHash, checkpoint)
			checkpointSeqId += 1
			Log("Puller %s saved remote checkpoint", name)
		}
	}

	glExpvars.Add("user_active", -1)
}

func clientHTTPHisto(name string) metrics.Histogram {
	histosMu.Lock()
	defer histosMu.Unlock()
	rv, ok := opshistos[name]
	if !ok {
		rv = metrics.NewBiasedHistogram()
		opshistos[name] = rv

		expOpsHistos.Set(name, &metrics.HistogramExport{rv,
			[]float64{0.25, 0.5, 0.75, 0.90, 0.99},
			[]string{"p25", "p50", "p75", "p90", "p99"}})
	}
	return rv
}

func recordHTTPClientStat(opname string, start time.Time, err error) {
	duration := time.Since(start)
	histo := clientHTTPHisto(opname)
	histo.Update(int64(duration))
}
