source ~/.bashrc

#eg:sh pzz/workspace/sparkdata/pzz/tag/gen_tag_format_to_hbase.sh /hive/warehouse/wlbase_dev.db/t_pzz_tag_basic_info/* /data/develop/ecportrait/basic_tag_csv_hbase.20160409.dir

#workspace path
workspace_path=/mnt/raid1/pzz/workspace/sparkdata

input_path=$1
output_path=$2
spark_middle $workspace_path/pzz/tag/gen_tag_format_to_hbase.py $input_path $output_path
