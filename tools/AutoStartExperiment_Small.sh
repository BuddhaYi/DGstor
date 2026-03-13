#!/bin/bash
echo "Begin experiment"
rcdir start
sleep 10s
rccli create geo-4M.json
sleep 10s
rccli start Geo1
sleep 60s
cd ../data
sh tracePuts.sh
sleep 90s
#sh traceGets.sh
#sleep 2800s
#cp /usr/local/var/log/Geo1/http.log /home/data/traceGetlog
rccli genParity Geo1
sleep 15s
cp /usr/local/var/log/Geo1/http.log /home/data/genParitylog
sh traceDgets.sh
sleep 10810s
cp /usr/local/var/log/Geo1/http.log /home/data/traceDgetlog
rccli recovery Geo1
sleep 3600s
cp /usr/local/var/log/Geo1/http.log /home/data/recoverylog
echo "Finish experiment"

