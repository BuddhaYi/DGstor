#!/bin/bash
for i in {1..16}
  do sshpass -p '111' ssh-copy-id -o stricthostkeychecking=no root@node$i
done

