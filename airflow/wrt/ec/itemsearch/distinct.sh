#!/usr/bin/env bash
source ~/.bashrc

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
load data inpath '/user/wrt/temp/itemsearch' overwrite into table t_base_item_search partition(ds = '0temp');
insert overwrite table wl_base.t_base_item_search partition(ds = '0temp')
SELECT
nid,max(user_id),max(comment_count),max(encryptedUserId),max(nick)
FROM
wl_base.t_base_item_search where ds = '0temp' group by nid;
EOF
