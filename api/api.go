package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type RestClient struct {
	client  *http.Client
	cookie  interface{}
	Verbose bool
}

var OperationCallback func(op string, start time.Time, err error)

var lastSerialNumber uint64

func (c *RestClient) DoRaw(req *http.Request) (resp *http.Response, serialNumber uint64) {
	defer func() {
		if err := recover(); err != nil {
			log.Fatal(err)
		}
	}()

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Language", "en-us")
	//req.Header.Set("Accept-Encoding", "gzip, deflate")
	if c.cookie != nil {
		req.AddCookie(c.cookie.(*http.Cookie))
	}

	serialNumber = atomic.AddUint64(&lastSerialNumber, 1)
	if req.URL.RawQuery != "" {
		req.URL.RawQuery += "&"
	}
	req.URL.RawQuery += fmt.Sprintf("n=%d", serialNumber)
	var start time.Time
	if c.Verbose {
		log.Printf("#%05d: %s %s", serialNumber, req.Method, req.URL)
		start = time.Now()
	}
	resp, err := c.client.Do(req)
	if err != nil {
		log.Printf("WARNING: Network error: %v for %s", err, req.URL)
		return nil, 0
	} else if resp.StatusCode >= 300 {
		log.Printf("WARNING: HTTP error: %s for %s", resp.Status, req.URL)
		return nil, 0
	} else if c.Verbose {
		log.Printf("#%05d:   <--%d (%v)", serialNumber, resp.StatusCode, time.Since(start))
	}
	return
}

func (c *RestClient) Do(req *http.Request) (mresp map[string]interface{}) {
	start := time.Now()
	resp, serialNumber := c.DoRaw(req)
	if resp == nil {
		return
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		log.Panicf("Non-JSON response for %v", req)
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Panicf("Can't read HTTP response: %v", err)
		return
	}

	if err = json.Unmarshal(body, &mresp); err != nil {
		log.Panicf("Can't parse response JSON: %v\nrequest = %v\n%s", err, req, body)
	}
	if c.Verbose {
		log.Printf("#%05d:      finished in %v", serialNumber, time.Since(start))
	}
	return
}

func (c *RestClient) DoAndIgnore(req *http.Request) {
	start := time.Now()
	resp, serialNumber := c.DoRaw(req)
	if resp == nil {
		return
	}
	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)
	if c.Verbose {
		log.Printf("#%05d:      finished in %v", serialNumber, time.Since(start))
	}
}

type SyncGatewayClient struct {
	baseURI      string
	baseAdminURI string
	client       *RestClient
}

const MaxIdleConnsPerHost = 28000

func (c *SyncGatewayClient) Init(hostname, db string, port, adminPort int, verbose bool) {
	c.baseURI = fmt.Sprintf("http://%s:%d/%s", hostname, port, db)
	c.baseAdminURI = fmt.Sprintf("http://%s:%d/%s", hostname, adminPort, db)
	t := &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
	c.client = &RestClient{&http.Client{Transport: t}, nil, verbose}
}

func (c *SyncGatewayClient) Valid() bool {
	req, _ := http.NewRequest("HEAD", c.baseAdminURI, nil)
	resp, _ := c.client.DoRaw(req)
	return resp != nil
}

func (c *SyncGatewayClient) AddCookie(cookie *http.Cookie) {
	c.client.cookie = cookie
}

type Doc struct {
	Id        string                 `json:"_id"`
	Rev       string                 `json:"_rev"`
	Channels  []string               `json:"channels"`
	Data      map[string]string      `json:"data"`
	Revisions map[string]interface{} `json:"_revisions"`
	Created   time.Time              `json:"created"`
}

func (c *SyncGatewayClient) PutSingleDoc(docid string, doc Doc) {

	if OperationCallback != nil {
		defer func(t time.Time) { OperationCallback("PutSingleDoc", t, nil) }(time.Now())
	}

	b, _ := json.Marshal(doc)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/%s?new_edits=false", c.baseURI, docid)
	req, _ := http.NewRequest("PUT", uri, j)

	c.client.Do(req)
}

func (c *SyncGatewayClient) PostRevsDiff(revsDiff map[string][]string) {

	if OperationCallback != nil {
		defer func(t time.Time) { OperationCallback("PostRevsDiff", t, nil) }(time.Now())
	}

	b, _ := json.Marshal(revsDiff)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_revs_diff", c.baseURI)
	req, _ := http.NewRequest("POST", uri, j)
	c.client.DoAndIgnore(req)
}

func (c *SyncGatewayClient) PostBulkDocs(docs map[string]interface{}) {

	if OperationCallback != nil {
		defer func(t time.Time) { OperationCallback("PostBulkDocs", t, nil) }(time.Now())
	}

	b, _ := json.Marshal(docs)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_bulk_docs", c.baseURI)
	req, _ := http.NewRequest("POST", uri, j)
	c.client.DoAndIgnore(req)
}

type BulkDocsEntry struct {
	ID  string `json:"id"`
	Rev string `json:"rev"`
}

func (c *SyncGatewayClient) GetBulkDocs(docs []BulkDocsEntry, wakeup time.Time) bool {
	oldStart := time.Now()

	body := map[string][]BulkDocsEntry{"docs": docs}
	b, _ := json.Marshal(body)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_bulk_get?revs=true&attachments=true", c.baseURI)
	req, _ := http.NewRequest("POST", uri, j)

	start := time.Now()
	resp, serialNumber := c.client.DoRaw(req) // _bulk_get returns MIME multipart, not JSON
	if OperationCallback != nil {
		defer func(t time.Time) {
			OperationCallback("GetBulkDocs", t, nil)
			log.Printf("#%05d:      oldfinished in %v", serialNumber, time.Since(t))
		}(oldStart)
	}
	if resp == nil {
		return false
	}
	defer resp.Body.Close()
	for _, d := range docs {
		createdTime := createdTimeFromDocId(d.ID)
		if createdTime != nil {
			logPushToSubscriberTime(createdTime, wakeup)
		}
	}
	io.Copy(ioutil.Discard, resp.Body)

	if c.client.Verbose {
		log.Printf("#%05d:      finished in %v", serialNumber, time.Since(start))
	}
	return true
}

func (c *SyncGatewayClient) GetSingleDoc(docid string, revid string, wakeup time.Time) bool {
	oldStart := time.Now()

	uri := fmt.Sprintf("%s/%s", c.baseURI, docid)
	if revid != "" {
		uri += "?rev=" + revid
	}
	req, _ := http.NewRequest("GET", uri, nil)
	start := time.Now()
	resp, serialNumber := c.client.DoRaw(req)
	if OperationCallback != nil {
		defer func(t time.Time) {
			OperationCallback("GetSingle", t, nil)
			log.Printf("#%05d:      oldfinished in %v", serialNumber, time.Since(t))
		}(oldStart)
	}
	if resp == nil {
		return false
	}
	createdTime := createdTimeFromDocId(docid)
	if createdTime != nil {
		logPushToSubscriberTime(createdTime, wakeup)
	}
	io.Copy(ioutil.Discard, resp.Body)

	if c.client.Verbose {
		log.Printf("#%05d:      finished in %v", serialNumber, time.Since(start))
	}
	return true
}

func createdTimeFromDocId(docid string) *time.Time {
	parts := strings.SplitN(docid, "_", 2)
	if len(parts) < 2 {
		return nil
	}
	n, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil
	}
	res := time.Unix(n/1e3, (n%1e3)*1e6)
	return &res
}

func logPushToSubscriberTime(createdTime *time.Time, wakeup time.Time) {
	if wakeup.After(*createdTime) {
		OperationCallback("PushToSubscriberBackfill", wakeup, nil)
	} else {
		OperationCallback("PushToSubscriberInteractive", *createdTime, nil)
	}
}

func (c *SyncGatewayClient) GetLastSeq() float64 {
	if OperationCallback != nil {
		defer func(t time.Time) { OperationCallback("GetLastSeq", t, nil) }(time.Now())
	}

	uri := fmt.Sprintf("%s/", c.baseURI)
	req, _ := http.NewRequest("GET", uri, nil)

	resp := c.client.Do(req)
	return resp["committed_update_seq"].(float64)
}

type Change struct {
	Seq     interface{}
	ID      string
	Changes []ChangeRev
}

type ChangeRev struct {
	Rev string
}

func (c *SyncGatewayClient) changesFeedRequest(feedType, since interface{}) *http.Request {
	var uri string
	uri = fmt.Sprintf("%s/_changes?feed=%s&heartbeat=300000&style=all_docs", c.baseURI, feedType)
	if since != nil {
		uri += fmt.Sprintf("&since=%v", since)
	}
	req, _ := http.NewRequest("GET", uri, nil)
	return req
}

func (c *SyncGatewayClient) GetChangesFeed(feedType string, since interface{}) (<-chan *Change, *bool, *http.Response) {
	if feedType == "continuous" {
		return c.runContinuousChangesFeed(since)
	}

	out := make(chan *Change)
	running := true
	go func() {
		defer close(out)
		for running {
			feed := c.client.Do(c.changesFeedRequest(feedType, since))
			results := feed["results"].([]interface{})
			for _, result := range results {
				doc := result.(map[string]interface{})
				var change Change
				change.ID = doc["id"].(string)
				change.Seq = doc["seq"]
				for _, item := range doc["changes"].([]interface{}) {
					dict := item.(map[string]interface{})
					revID := dict["rev"].(string)
					change.Changes = append(change.Changes, ChangeRev{Rev: revID})
				}
				out <- &change
			}
			since = feed["last_seq"]
			if feedType != "longpoll" {
				break
			}
		}
		if !running {
			log.Printf("Changes feed closed at client request")
		}
	}()
	return out, &running, nil
}

func (c *SyncGatewayClient) runContinuousChangesFeed(since interface{}) (<-chan *Change, *bool, *http.Response) {
	running := true
	response, _ := c.client.DoRaw(c.changesFeedRequest("continuous", since))
	if response == nil {
		return nil, &running, nil
	}
	out := make(chan *Change)
	go func() {
		defer close(out)
		defer response.Body.Close()
		scanner := bufio.NewScanner(response.Body)
		for running && scanner.Scan() {
			if line := scanner.Bytes(); len(line) > 0 {
				var change Change
				if err := json.Unmarshal(line, &change); err != nil {
					log.Printf("Warning: Unparseable line from continuous _changes: %s", scanner.Bytes())
					continue
				}
				out <- &change
			}
		}
		if running {
			log.Printf("Warning: Continuous changes feed closed with error %v", scanner.Err())
		}
	}()
	return out, &running, response
}

type Checkpoint struct {
	LastSequence interface{} `json:"lastSequence"`
}

func (c *SyncGatewayClient) SaveCheckpoint(id string, checkpoint Checkpoint) {

	if OperationCallback != nil {
		defer func(t time.Time) { OperationCallback("SaveCheckpoint", t, nil) }(time.Now())
	}

	b, _ := json.Marshal(checkpoint)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_local/%s", c.baseURI, id)
	req, _ := http.NewRequest("PUT", uri, j)

	c.client.Do(req)
}

type UserAuth struct {
	Name          string   `json:"name"`
	Password      string   `json:"password"`
	AdminChannels []string `json:"admin_channels"`
}

func (c *SyncGatewayClient) AddUser(name string, auth UserAuth) {

	if OperationCallback != nil {
		defer func(t time.Time) { OperationCallback("AddUser", t, nil) }(time.Now())
	}

	b, _ := json.Marshal(auth)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_user/%s", c.baseAdminURI, name)
	req, _ := http.NewRequest("PUT", uri, j)

	log.Printf("Adding user %s", name)
	c.client.DoAndIgnore(req)
}

type Session struct {
	Name string `json:"name"`
	TTL  int    `json:"ttl"`
}

func (c *SyncGatewayClient) CreateSession(name string, session Session) http.Cookie {

	if OperationCallback != nil {
		defer func(t time.Time) { OperationCallback("CreateSession", t, nil) }(time.Now())
	}

	b, _ := json.Marshal(session)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_session", c.baseAdminURI)
	req, _ := http.NewRequest("POST", uri, j)

	resp := c.client.Do(req)
	return http.Cookie{
		Name:  resp["cookie_name"].(string),
		Value: resp["session_id"].(string),
	}
}
