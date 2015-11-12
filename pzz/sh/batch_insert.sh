#!/usr/bin/env bash

feed_dir=/data/develop/ec/tb/cmt/feedid
data_dir=/data/develop/ec/tb/cmt/tmpdata
commit_dir=/commit/comments

all_feedid_1103=${feed_dir}/cmt_allfeedid.20151103
all_feedid_1105=${feed_dir}/cmt_allfeedid.20151105
all_feedid_1106=${feed_dir}/cmt_allfeedid.20151106
all_feedid_1107=${feed_dir}/cmt_allfeedid.20151107
all_feedid_1108=${feed_dir}/cmt_allfeedid.20151108
all_feedid_1109=${feed_dir}/cmt_allfeedid.20151109
all_feedid_1110=${feed_dir}/cmt_allfeedid.20151110
all_feedid_1111=${feed_dir}/cmt_allfeedid.20151111

#1105
#sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh $all_feedid_1103/part* $commit_dir/*2015-11-05 $all_feedid_1105 $feed_dir/cmt_newfeedid.20151105 $data_dir/cmt_inc_data.20151105

#1106
#sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh $all_feedid_1105/part* $commit_dir/*2015-11-06 $all_feedid_1106 $feed_dir/cmt_newfeedid.20151106 $data_dir/cmt_inc_data.20151106

#1107
#sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh $all_feedid_1106/part* $commit_dir/*2015-11-07 $all_feedid_1107 $feed_dir/cmt_newfeedid.20151107 $data_dir/cmt_inc_data.20151107

#1108
#sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh $all_feedid_1107/part* $commit_dir/*2015-11-08 $all_feedid_1108 $feed_dir/cmt_newfeedid.20151108 $data_dir/cmt_inc_data.20151108

#1109
#sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh $all_feedid_1108/part* $commit_dir/*2015-11-09 $all_feedid_1109 $feed_dir/cmt_newfeedid.20151109 $data_dir/cmt_inc_data.20151109

#1110
#sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh $all_feedid_1109/part* $commit_dir/*2015-11-10 $all_feedid_1110 $feed_dir/cmt_newfeedid.20151110 $data_dir/cmt_inc_data.20151110

#1111
sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh $all_feedid_1110/part* $commit_dir/*2015-11-11 $all_feedid_1111 $feed_dir/cmt_newfeedid.20151111 $data_dir/cmt_inc_data.20151111
