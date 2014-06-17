package workload

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
)

type Config struct {
	Hostname               string
	Port                   int
	AdminPort              int
	Database               string
	ChannelActiveUsers     int
	ChannelConcurrentUsers int
	MinUserOffTimeMs       int
	MaxUserOffTimeMs       int
	NumPullers             int
	NumPushers             int
	UserOffset             int
	DocSize                int
	SendAttachment         bool
	DocSizeDistribution    DocSizeDistribution
	RampUpIntervalMs       int
	SleepTimeMs            int
	RunTimeMs              int
	FeedType               string // Type of _changes feed: "continuous" or "longpoll"
	SerieslyHostname       string
	SerieslyDatabase       string
	Verbose                bool
	LogRequests            bool
}

var DefaultConfig = Config{
	Hostname:               "127.0.0.1",
	Port:                   4984,
	AdminPort:              4985,
	DocSize:                1024,
	SendAttachment:         false,
	RampUpIntervalMs:       60000,
	RunTimeMs:              7200000,
	SleepTimeMs:            10000,
	FeedType:               "continuous",
	Verbose:                false,
	NumPullers:             7,
	NumPushers:             3,
	ChannelActiveUsers:     10,
	ChannelConcurrentUsers: 2,
	MinUserOffTimeMs:       5000,
	MaxUserOffTimeMs:       120000,
}

var Verbose = false

func ReadConfig(config *Config) {
	*config = DefaultConfig

	workload_path := flag.String("workload", "workload.json", "Path to workload configuration")
	flag.Parse()

	workload, err := ioutil.ReadFile(*workload_path)
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(workload, config)
	if err != nil {
		log.Fatal(err)
	}
	Verbose = config.Verbose
}
