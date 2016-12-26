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
hadoop fs -test -e /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.$d.partitions
if [ $? -eq 0 ] ;then
    s=`hadoop fs -du -s /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.$d.partitions | awk '{print $1/1000/1000/1000" GB /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.$d.partitions"}'`
	msg=$msg"[1] cmt_inc"$s
else
	msg=$msg"[0] cmt_inc /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.$d.partitions"
fi

echo $msg