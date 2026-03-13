#!/bin/bash
for i in {1..5000}
do
  wget --post-file=file_$i "http://node1:8080/put/$i"
done
