package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type RestClient struct {
	client *http.Client
	cookie interface{}
}

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
		log.Printf("WARNING: Network error: %v", err)
		return nil
	} else if resp.StatusCode >= 300 {
		log.Printf("WARNING: HTTP error: Status %d %s", resp.StatusCode, resp.Status)
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
}

func (c *SyncGatewayClient) PutSingleDoc(docid string, doc Doc) {
	b, _ := json.Marshal(doc)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/%s?new_edits=false", c.baseURI, docid)
	req, _ := http.NewRequest("PUT", uri, j)

	c.client.Do(req)
}

func (c *SyncGatewayClient) PostRevsDiff(revsDiff map[string][]string) {
	b, _ := json.Marshal(revsDiff)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_revs_diff", c.baseURI)
	req, _ := http.NewRequest("POST", uri, j)

	resp := c.client.DoRaw(req) // _revs_diff returns JSON array, not object, so Do can't parse it
	defer resp.Body.Close()
	_, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Panicf("Can't read HTTP response: %v", err)
		return
	}
}

func (c *SyncGatewayClient) PostBulkDocs(docs map[string]interface{}) {
	b, _ := json.Marshal(docs)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_bulk_docs", c.baseURI)
	req, _ := http.NewRequest("POST", uri, j)

	resp := c.client.DoRaw(req) // _bulk_docs returns JSON array, not object, so Do can't parse it
	defer resp.Body.Close()
	_, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Panicf("Can't read HTTP response: %v", err)
		return
	}
}

type BulkDocsEntry struct {
	ID  string
	Rev string
}

func (c *SyncGatewayClient) GetBulkDocs(docs []BulkDocsEntry) {
	body := map[string][]BulkDocsEntry{"docs": docs}
	b, _ := json.Marshal(body)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_bulk_get?revs=true&attachments=true", c.baseURI)
	req, _ := http.NewRequest("POST", uri, j)

	resp := c.client.DoRaw(req) // _bulk_get returns MIME multipart, not JSON
	defer resp.Body.Close()
	_, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Panicf("Can't read HTTP response: %v", err)
		return
	}
}

func (c *SyncGatewayClient) GetSingleDoc(docid string, revid string) {
	uri := fmt.Sprintf("%s/%s", c.baseURI, docid)
	if revid != "" {
		uri += "?rev=" + revid
	}
	req, _ := http.NewRequest("GET", uri, nil)
	c.client.Do(req)
}

func (c *SyncGatewayClient) GetLastSeq() float64 {
	uri := fmt.Sprintf("%s/", c.baseURI)
	req, _ := http.NewRequest("GET", uri, nil)

	resp := c.client.Do(req)
	return resp["committed_update_seq"].(float64)
}

func (c *SyncGatewayClient) GetChangesFeed(feedType, since string) map[string]interface{} {
	var uri string
	uri = fmt.Sprintf("%s/_changes?feed=%s&heartbeat=300000&style=all_docs&since=%s", c.baseURI, feedType, since)
	req, _ := http.NewRequest("GET", uri, nil)

	return c.client.Do(req)
}

type Checkpoint struct {
	LastSequence string `json:"lastSequence"`
}

func (c *SyncGatewayClient) SaveCheckpoint(id string, checkpoint Checkpoint) {
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
	b, _ := json.Marshal(auth)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_user/%s", c.baseAdminURI, name)
	req, _ := http.NewRequest("PUT", uri, j)

	log.Printf("Adding user %s", name)
	resp := c.client.DoRaw(req)
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
