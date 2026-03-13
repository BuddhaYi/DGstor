#!/bin/bash
for i in {1..5000}
do
  wget http://node1:8080/dget/$i
  sleep 2s
done
