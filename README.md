[![Build Status](https://drone.io/github.com/couchbaselabs/gateload/status.png)](https://drone.io/github.com/couchbaselabs/gateload/latest)

gateload
--------

Sync Gateway workload generator

Install
-------

```
$ go get github.com/couchbaselabs/gateload
```

Usage
-----

```
gateload -workload=workload.json
```


Configuration files
-------------------

gateload uses JSON format for workload configuration:

    {
        "Hostname": "127.0.0.1",
        "Database": "sync_gateway",
        "DocSize": 1024,
        "RampUpIntervalMs": 3600000,
        "SleepTimeMs": 5000,
        "NumPullers": 300,
        "NumPushers": 700
    }

to generate random document sizes with a given distribution, replace DocSize with DocSizeDistriubution

        "DocSizeDistribution": [
          {
            "Prob": 20,
            "MinSize": 32,
            "MaxSize": 127
          },
          {
            "Prob": 70,
            "MinSize": 128,
            "MaxSize": 1023
          },
          {
            "Prob": 10,
            "MinSize": 1024,
            "MaxSize": 65536
          }
        ]

* **Hostname** - address of Sync Gateway instance (notice that port 4985 must be accessible as well)
* **Database** - name of Sync Gateway database
* **DocSize** - document body size in bytes
* **DocSizeDistribution** - document body size distributions, specifying Prob (integer probability), MinSize the minimum document size in this distribution, and MaxSize the maximum document size in this distribution.  All probabilities must sum to 100.
* **RampUpIntervalMs** - time to start all users (e.g, with RampUpIntervalMs=10000 and 10 users, each user starts every 1 second)
* **SleepTimeMs** - delay between push (PUT single doc) requests
* **NumPullers** - number of active readers to start
* **NumPushers** - number of active writers to start
