#!/usr/bin/env bash
source ~/.bashrc

#workspace path
workspace_path=/mnt/raid1/pzz/workspace/sparkdata

input_path=$1
csv_path=$2
hfile_path=$3
hadoop fs -rmr $csv_path
hadoop fs -rmr $hfile_path
spark_middle $workspace_path/pzz/record/gen_record_new_format_to_hbase.py $input_path $csv_path

#0		tb_id	string
#1		tage	int
#2		tgender	string
#3		city	string
#4		alipay	int
#5		year	double
#6		buycnt	string
#7		verify_level	int
#8		ac_score_normal	double
#9		sum_level	int
#10		feedrate	double

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
    -Dimporttsv.separator="," \
    -Dimporttsv.columns=HBASE_ROW_KEY,info:age,info:gender,info:city,info:auth,info:year,info:cnt,info:v_level,info:ac_score,info:sum_level,info:f_rate,info:sum \
    -Dimporttsv.bulk.output=$hfile_path \
    basic_tag \
    $csv_path/*

hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles $hfile_path  basic_tag