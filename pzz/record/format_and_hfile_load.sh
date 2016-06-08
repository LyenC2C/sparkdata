#!/usr/bin/env bash
source ~/.bashrc

#workspace path
workspace_path=/mnt/raid1/pzz/workspace/sparkdata

input_path=$1
csv_path=$2
hfile_path=$3
spark_middle $workspace_path/pzz/record/gen_record_new_format_to_hbase.py $input_path $csv_path


hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
    -Dimporttsv.separator="," \
    -Dimporttsv.columns=HBASE_ROW_KEY,info:v,info:item \
    -Dimporttsv.bulk.output=$hfile_path \
    ec_record_new \
    $csv_path/*

hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles $hfile_path  ec_record_new