#!/bin/bash
 
LOGTIME=$(date "+%Y-%m-%d %H:%M:%S")
echo "[$LOGTIME] startup run..." >>/root/start_ssh.log
/usr/sbin/sshd >>/root/start_ssh.log
#service mysql start >>/root/star_mysql.log   //其他服务也可这么实现

