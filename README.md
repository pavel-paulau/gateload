gateload
--------

Sync Gateway workload generator

Usage
-----

./gateload -workload=workload.json

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

* Hostname - address of Sync Gateway instance (notice that port 4985 must be accessible as well)
* Database - name of Sync Gateway database
* DocSize - document body size in bytes
* RampUpIntervalMs - time to start all users (e.g, with RampUpIntervalMs=10000 and 10 users, each user starts every 1 second)
* SleepTimeMs - delay between push (PUT single doc) requests
* NumPullers - number of active readers to start
* NumPushers - number of active writers to start
