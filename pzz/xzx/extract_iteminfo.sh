source ~/.bashrc

input=$1
output=$2

spark-submit --executor-memory 4g --driver-memory 2g  --total-executor-cores 100 /home/gms/xzx/extract_iteminfo.py $input $output
