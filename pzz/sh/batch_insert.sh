all_feedid_1105=/data/develop/ec/tb/cmt_allfeedid.20151105
all_feedid_1106=/data/develop/ec/tb/cmt_allfeedid.20151106
all_feedid_1107=/data/develop/ec/tb/cmt_allfeedid.20151107
all_feedid_1108=/data/develop/ec/tb/cmt_allfeedid.20151108

#1106
sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh $all_feedid_1105/part* /commit/comments/*2015-11-06 $all_feedid_1106 /data/develop/ec/tb/cmt_newfeedid.20151106 /data/develop/ec/tb/cmt_tmpdata.20151106

#1107
sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh $all_feedid_1106/part* /commit/comments/*2015-11-07 $all_feedid_1107 /data/develop/ec/tb/cmt_newfeedid.20151107 /data/develop/ec/tb/cmt_tmpdata.20151107

#1108
sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh $all_feedid_1107/part* /commit/comments/*2015-11-08 $all_feedid_1108 /data/develop/ec/tb/cmt_newfeedid.20151108 /data/develop/ec/tb/cmt_tmpdata.20151108
