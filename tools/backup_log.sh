#!/bin/bash
echo "####begin to backup_log,every a half of minutes exec once####"
while true
do
    docker cp node1:/usr/local/var/log /home/tianyi/Ptools
    echo "backup_log successful!!!"
    sleep 30s
done

