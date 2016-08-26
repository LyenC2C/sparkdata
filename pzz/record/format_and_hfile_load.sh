#!/usr/bin/env bash
source ~/.bashrc

#workspace path
workspace_path=/mnt/raid1/pzz/workspace/sparkdata

start=$1
end=$2

input_path=/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new/ds=true1/cmt_inc_data_${end}
csv_path=/data/develop/ecportrait/record_csv_hbase.${start}-${end}
hfile_path=/data/develop/ecportrait/record_new_hfile.${start}-${end}

hadoop fs -rmr $csv_path
hadoop fs -rmr $hfile_path
spark_middle $workspace_path/pzz/record/gen_record_new_format_to_hbase.py $input_path $csv_path

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
    -Dimporttsv.separator="," \
    -Dimporttsv.columns=HBASE_ROW_KEY,info:v,info:item \
    -Dimporttsv.bulk.output=$hfile_path \
    ec_record_new \
    $csv_path/*

hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles $hfile_path  ec_record_new