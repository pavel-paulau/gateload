package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

type RestClient struct {
	client *http.Client
	cookie interface{}
}

var OperationCallback func(op string, start time.Time, err error)

func (c *RestClient) DoRaw(req *http.Request) *http.Response {
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

	resp, err := c.client.Do(req)
	if err != nil {
		log.Printf("WARNING: Network error: %v for %s", err, req.URL)
		return nil
	} else if resp.StatusCode >= 300 {
		log.Printf("WARNING: HTTP error: %s for %s", resp.Status, req.URL)
		return nil
	}
	return resp
}

func (c *RestClient) Do(req *http.Request) (mresp map[string]interface{}) {
	resp := c.DoRaw(req)
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
	return
}

type SyncGatewayClient struct {
	baseURI      string
	baseAdminURI string
	client       *RestClient
}

const MaxIdleConnsPerHost = 28000

func (c *SyncGatewayClient) Init(hostname, db string) {
	c.baseURI = fmt.Sprintf("http://%s:4984/%s", hostname, db)
	c.baseAdminURI = fmt.Sprintf("http://%s:4985/%s", hostname, db)
	t := &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
	c.client = &RestClient{&http.Client{Transport: t}, nil}
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

	resp := c.client.DoRaw(req) // _revs_diff returns JSON array, not object, so Do can't parse it
	if resp == nil {
		return
	}
	defer resp.Body.Close()
	_, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Panicf("Can't read HTTP response: %v", err)
		return
	}
}

func (c *SyncGatewayClient) PostBulkDocs(docs map[string]interface{}) {

	if OperationCallback != nil {
		defer func(t time.Time) { OperationCallback("PostBulkDocs", t, nil) }(time.Now())
	}

	b, _ := json.Marshal(docs)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_bulk_docs", c.baseURI)
	req, _ := http.NewRequest("POST", uri, j)

	resp := c.client.DoRaw(req) // _bulk_docs returns JSON array, not object, so Do can't parse it
	if resp == nil {
		return
	}
	defer resp.Body.Close()
	_, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Panicf("Can't read HTTP response: %v", err)
		return
	}
}

type BulkDocsEntry struct {
	ID  string `json:"id"`
	Rev string `json:"rev"`
}

func (c *SyncGatewayClient) GetBulkDocs(docs []BulkDocsEntry, wakeup time.Time) bool {
	if OperationCallback != nil {
		defer func(t time.Time) { OperationCallback("GetBulkDocs", t, nil) }(time.Now())
	}

	body := map[string][]BulkDocsEntry{"docs": docs}
	b, _ := json.Marshal(body)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_bulk_get?revs=true&attachments=true", c.baseURI)
	req, _ := http.NewRequest("POST", uri, j)

	resp := c.client.DoRaw(req) // _bulk_get returns MIME multipart, not JSON
	if resp == nil {
		return false
	}
	defer resp.Body.Close()
	_, attrs, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	mr := multipart.NewReader(resp.Body, attrs["boundary"])
	part, err := mr.NextPart()
	for part != nil && err == nil {
		var data map[string]interface{}
		decoder := json.NewDecoder(part)
		err := decoder.Decode(&data)
		if err == nil {
			logPushToSubscriberTime(data, wakeup)
		}
		part, err = mr.NextPart()
	}

	return true
}

func (c *SyncGatewayClient) GetSingleDoc(docid string, revid string, wakeup time.Time) bool {

	if OperationCallback != nil {
		defer func(t time.Time) { OperationCallback("GetSingle", t, nil) }(time.Now())
	}

	uri := fmt.Sprintf("%s/%s", c.baseURI, docid)
	if revid != "" {
		uri += "?rev=" + revid
	}
	req, _ := http.NewRequest("GET", uri, nil)
	res := c.client.Do(req)
	if res != nil {
		logPushToSubscriberTime(res, wakeup)
	}
	return res != nil
}

func logPushToSubscriberTime(doc map[string]interface{}, wakeup time.Time) {
	created, ok := doc["created"]
	if ok {
		createdString, ok := created.(string)
		if ok {
			createdTime, err := time.Parse(time.RFC3339, createdString)
			if err == nil {
				if wakeup.After(createdTime) {
					OperationCallback("PushToSubscriberBackfill", wakeup, nil)
				} else {
					OperationCallback("PushToSubscriberInteractive", createdTime, nil)
				}
			}
		}
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
	uri = fmt.Sprintf("%s/_changes?feed=%s&heartbeat=300000&style=all_docs&since=%s", c.baseURI, feedType, since)
	req, _ := http.NewRequest("GET", uri, nil)
	return req
}

func (c *SyncGatewayClient) GetChangesFeed(feedType string, since interface{}) <-chan *Change {
	if feedType == "continuous" {
		return c.runContinuousChangesFeed(since)
	}

	out := make(chan *Change)
	go func() {
		defer close(out)
		for {
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
	}()
	return out
}

func (c *SyncGatewayClient) runContinuousChangesFeed(since interface{}) <-chan *Change {
	response := c.client.DoRaw(c.changesFeedRequest("continuous", since))
	if response == nil {
		return nil
	}
	out := make(chan *Change)
	go func() {
		defer close(out)
		defer response.Body.Close()
		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			if line := scanner.Bytes(); len(line) > 0 {
				var change Change
				if err := json.Unmarshal(line, &change); err != nil {
					log.Printf("Warning: Unparseable line from continuous _changes: %s", scanner.Bytes())
					continue
				}
				out <- &change
			}
		}
		log.Printf("Warning: Continuous changes feed closed with error %v", scanner.Err())
	}()
	return out
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
	resp := c.client.DoRaw(req)
	if resp == nil {
		return
	}
	defer resp.Body.Close()
	_, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Panicf("Can't read HTTP response: %v", err)
		return
	}
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
