#!/bin/bash
docker cp rcstor-master node1:/home
docker exec -it -e GOPROXY=https://goproxy.io,direct -w /home/rcstor-master node1  make install
for i in {1..16}
do
  docker cp ssh-copy.sh node$i:/home && docker exec -it -w /home node$i sh ssh-copy.sh && docker exec -it node$i yum -y install wget
done
docker exec -it -e GOPROXY=https://goproxy.io,direct -w /home/rcstor-master node1  sh AutoStartExperiment.sh
