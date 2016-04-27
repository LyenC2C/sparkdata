source ~/.bashrc

input_path=$1
hfile_path=$2

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
    -Dimporttsv.separator="," \
    -Dimporttsv.columns=HBASE_ROW_KEY,info:v,info:item \
    -Dimporttsv.bulk.output=$hfile_path \
    ec_record_new \
    $input_path/*

hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles $hfile_path  ec_record_new