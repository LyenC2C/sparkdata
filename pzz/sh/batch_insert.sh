#!/usr/bin/env bash

feed_dir=/data/develop/ec/tb/cmt/feedid
data_dir=/data/develop/ec/tb/cmt/tmpdata
commit_dir=/commit/comments

all_feedid_1105=${feed_dir}/cmt_allfeedid.20151105
all_feedid_1106=${feed_dir}/cmt_allfeedid.20151106
all_feedid_1107=${feed_dir}/cmt_allfeedid.20151107
all_feedid_1108=${feed_dir}/cmt_allfeedid.20151108


#1106
#sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh $all_feedid_1105/part* $commit_dir/*2015-11-06 $all_feedid_1106 $feed_dir/cmt_newfeedid.20151106 $data_dir/cmt_inc_data.20151106

#1107
#sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh $all_feedid_1106/part* $commit_dir/*2015-11-07 $all_feedid_1107 $feed_dir/cmt_newfeedid.20151107 $data_dir/cmt_inc_data.20151107

#1108
sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh $all_feedid_1107/part* $commit_dir/*2015-11-08 $all_feedid_1108 $feed_dir/cmt_newfeedid.20151108 $data_dir/cmt_inc_data.20151108
