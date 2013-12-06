package workload

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
)

type Config struct {
	Hostname         string
	Database         string
	NumPullers       int
	NumPushers       int
	DocSize          int
	RampUpIntervalMs int
	SleepTimeMs      int
	RunTimeMs        int
}

func ReadConfig(config *Config) {
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
}
