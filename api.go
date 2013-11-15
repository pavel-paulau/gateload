package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

type RestClient struct {
	client *http.Client
	cookie interface{}
}

func (c *RestClient) Do(req *http.Request) (mresp map[string]interface{}) {
	req.Header.Set("Content-Encoding", "application/json")
	if c.cookie != nil {
		req.AddCookie(c.cookie.(*http.Cookie))
	}

	resp, err := c.client.Do(req)
	if err != nil {
		log.Printf("%v", err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("%v", err)
	}
	json.Unmarshal(body, &mresp)
	return
}

type SyncGatewayClient struct {
	baseURI      string
	baseAdminURI string
	client       *RestClient
}

func (c *SyncGatewayClient) Init(hostname, db string) {
	c.baseURI = fmt.Sprintf("http://%s:4984/%s", hostname, db)
	c.baseAdminURI = fmt.Sprintf("http://%s:4985/%s", hostname, db)
	c.client = &RestClient{&http.Client{}, nil}
}

func (c *SyncGatewayClient) AddCookie(cookie *http.Cookie) {
	c.client.cookie = cookie
}

func (c *SyncGatewayClient) PutSingleDoc(docid string, doc Doc) {
	b, _ := json.Marshal(doc)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/%s?new_edits=true", c.baseURI, docid)
	req, _ := http.NewRequest("PUT", uri, j)
	c.client.Do(req)
}

func (c *SyncGatewayClient) GetBulkDocs(docs map[string][]map[string]string) {
	b, _ := json.Marshal(docs)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_bulk_get", c.baseURI)
	req, _ := http.NewRequest("POST", uri, j)

	c.client.Do(req)
}

func (c *SyncGatewayClient) GetSingleDoc(docid string) {
	uri := fmt.Sprintf("%s/%s", c.baseURI, docid)
	req, _ := http.NewRequest("GET", uri, nil)
	c.client.Do(req)
}

func (c *SyncGatewayClient) GetLastSeq() string {
	uri := fmt.Sprintf("%s/", c.baseURI)
	req, _ := http.NewRequest("GET", uri, nil)

	resp := c.client.Do(req)
	return strconv.FormatFloat(resp["committed_update_seq"].(float64), 'f', 0, 64)
}

func (c *SyncGatewayClient) GetChangesFeed(since string) map[string]interface{} {
	uri := fmt.Sprintf("%s/_changes?limit=10&feed=longpoll&since=%s", c.baseURI, since)
	req, _ := http.NewRequest("GET", uri, nil)

	return c.client.Do(req)
}

func (c *SyncGatewayClient) AddUser(name string, auth UserAuth) {
	b, _ := json.Marshal(auth)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s/_user/%s", c.baseAdminURI, name)
	req, _ := http.NewRequest("PUT", uri, j)

	log.Printf("Adding user %s", name)
	c.client.Do(req)
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
