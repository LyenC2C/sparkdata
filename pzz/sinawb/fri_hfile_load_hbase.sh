source ~/.bashrc

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv  \
    -Dimporttsv.separator="|"  \
    -Dimporttsv.columns=HBASE_ROW_KEY,fri:v  \
    -Dimporttsv.bulk.output=/data/develop/sinawb/rel_fri.json.20160401.hfile \
    snwb_fri   \
    /data/develop/sinawb/rel_fri.json.20160401.tsv.hbase


hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles \
    -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1024 \
    /data/develop/sinawb/rel_fri.json.20160401.hfile \
    snwb_fri