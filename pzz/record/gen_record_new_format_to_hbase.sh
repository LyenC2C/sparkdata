source ~/.bashrc

#eg:sh pzz/workspace/sparkdata/pzz/record/gen_record_new_format_to_hbase.sh /hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new_tmp/* /data/develop/ecportrait/record_csv_hbase.20160409-20160419.dir

#workspace path
workspace_path=/mnt/raid1/pzz/workspace/sparkdata

input_path=$1
output_path=$2
spark_middle $workspace_path/pzz/record/gen_record_new_format_to_hbase.py $input_path $output_path
