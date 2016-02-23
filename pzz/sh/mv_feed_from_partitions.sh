#source /home/yarn/.bashrc
workspace_path=/mnt/raid1/pzz/workspace/sparkdata

input_dir=$1
table=$2
hadoop fs -ls $1 |awk -F '/'  '{if(length($NF)==8) print $NF }' |awk '{if($1 ~/^[0-9]+$/) print $1" '${input_dir}' '${table}'"}' | xargs -n3 -P 20 sh ${workspace_path}/pzz/sh/mv_step.sh
#hadoop fs -ls $1 |awk -F '/'  '{if(length($NF)==8) print $NF }' |awk '{if($1 ~/^[0-9]+$/) print $1\t${input_dir}}'  | xargs -n2 -P 20 -i sh mv_step.sh {}
#echo $cmd