source ~/.bashrc

#workspace path
workspace_path=/mnt/raid1/pzz/workspace/sparkdata

input_path=$1
output_path=$2
spark_middle $workspace_path/pzz/record/gen_record_new_format_to_hbase.py $input_path $output_path
