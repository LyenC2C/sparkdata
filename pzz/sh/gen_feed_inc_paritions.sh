source ~/.bashrc

workspace=/home/yarn/pzz/workspace
end_date=$1
uid_data_input=/data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.${end_date}

hadoop fs -test -e ${uid_data_input}.partitions
if [ $? -eq 0 ] ;then
    hadoop fs -rmr ${uid_data_input}.partitions
if

spark-submit  --master spark://cs220:7077 \
--total-executor-cores  40 \
--executor-memory  4g \
--driver-memory 4g \
--class MultipleText \
${workspace}/sparkdata/pzz/sh/partition_spark.jar \
${uid_data_input} \
${uid_data_input}.partitions