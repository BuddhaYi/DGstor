#!/bin/bash
for i in {1..5000}
do
  wget http://node1:8080/get/$i
  sleep 2s
done
