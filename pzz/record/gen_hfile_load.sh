source ~/.bashrc

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
    -Dimporttsv.separator="," \
    -Dimporttsv.columns=HBASE_ROW_KEY,info:v,info:item \
    -Dimporttsv.bulk.output=/data/develop/ecportrait/hfile_record_new.20160408.updated.1\
    ec_record_new \
    /data/develop/ecportrait/record_csv_hbase.20160408.updated.dir/part-01*

hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /data/develop/ecportrait/hfile_record_new.20160408.updated.1  ec_record_new