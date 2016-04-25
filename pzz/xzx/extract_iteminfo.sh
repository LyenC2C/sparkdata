#!/bin/bash
source ~/.bashrc
#eg: cd ~/xzx; sh extract_iteminfo.sh item_id_xzx_20160424.csv /mnt/raid2/gms/item_id_xzx_20160424.csv.info.20160425


#localpath
#$1:input file
input=$1
#$2:output path
output=$2

hadoop fs -rmr xzx/$input
hadoop fs -put $input xzx/
spark-submit --executor-memory 4g --driver-memory 2g  --total-executor-cores 100 /home/gms/xzx/extract_iteminfo.py  -byitemid  xzx/$input xzx/$output.tmp
hadoop fs -cat xzx/$output.tmp/* > $output
hadoop fs -rmr xzx/$output.tmp
