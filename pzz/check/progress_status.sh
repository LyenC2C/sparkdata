source ~/.bashrc

#[1] 原始数据 XXXXXXXX - XXXXXXXX
#[2] cmt新增数据 大小 path
#[3] cmt partition 大小 path
#[4] feed db
#[5] record db
#[6] record_online tsv
#[7] record_online hfile

d=$1

msg=""
#[2] cmt新增数据 大小 path
hadoop fs -test -e /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.$d
if [ $? -eq 0 ] ;then
    s=`hadoop fs -du -s /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.$d | awk -v date=$d '{print $1/1000/1000/1000" GB /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid."date""}'`
	msg=$msg"[1] cmt_inc "$s
else
	msg=$msg"[0] cmt_inc /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid."$d".partitions"
fi


#[3] cmt partition 大小 path
hadoop fs -test -e /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.$d.partitions
if [ $? -eq 0 ] ;then
    s=`hadoop fs -du -s /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.$d.partitions | awk -v date=$d '{print $1/1000/1000/1000" GB /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid."date".partitions"}'`
	msg=$msg"\n[1] cmt_inc_partition "$s
else
	msg=$msg"\n[0] cmt_inc_partition /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.$d.partitions"
fi


#[5] record db
hadoop fs -test -e /hive/warehouse/wl_base.db/t_base_ec_record_dev_new/ds=true/cmt_inc_data_$d
if [ $? -eq 0 ] ;then
    s=`hadoop fs -du -s /hive/warehouse/wl_base.db/t_base_ec_record_dev_new/ds=true/cmt_inc_data_$d | awk -v date=$d '{print $1/1000/1000/1000" GB /hive/warehouse/wl_base.db/t_base_ec_record_dev_new/ds=true/cmt_inc_data_"date}'`
	msg=$msg"\n[1] record_db "$s
else
	msg=$msg"\n[0] record_db /hive/warehouse/wl_base.db/t_base_ec_record_dev_new/ds=true/cmt_inc_data_$d"
fi

#[6] record_online tsv
s=`hadoop fs -du -s  /data/develop/ecportrait/record_csv_hbase.*-$d`
if [ -n "$s" ]; then
    ss=`echo $s | awk -v data=$d '{print $1/1000/1000/1000" GB "$3}'`
    msg=$msg"\n[1] record_online_tsv "$ss
fi
if [ -z "$s" ]; then
    msg=$msg"\n[0] record_online_tsv /data/develop/ecportrait/record_csv_hbase.*-$d"
fi

echo -e $msg