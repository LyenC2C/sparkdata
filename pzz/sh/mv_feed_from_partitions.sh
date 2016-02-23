#source /home/yarn/.bashrc
input_dir=$1
hadoop fs -ls $1 |awk -F '/'  '{if(length($NF)==8) print $NF }' |awk '{if($1 ~/^[0-9]+$/) print $1" '${input_dir}'\"}' | less
#hadoop fs -ls $1 |awk -F '/'  '{if(length($NF)==8) print $NF }' |awk '{if($1 ~/^[0-9]+$/) print $1\t${input_dir}}'  | xargs -n2 -P 20 -i sh mv_step.sh {}
#echo $cmd