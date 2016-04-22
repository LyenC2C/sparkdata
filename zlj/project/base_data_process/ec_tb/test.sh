#!/bin/bash
source /etc/profile;
yesterday=$(date -d '-1 day' '+%Y-%m-%d')
lastmonth=$(date -d '-1 month' '+%Y-%m-%d')


/usr/local/cloud/hive/bin/hive<<EOF


EOF