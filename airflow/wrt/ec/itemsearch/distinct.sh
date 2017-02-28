#!/usr/bin/env bash
source ~/.bashrc

hive<<EOF
use wl_base;
insert overwrite table wl_base.t_base_item_search partition(ds = '0temp')
SELECT
nid,max(user_id),max(comment_count),max(encryptedUserId),max(nick)
FROM
wl_base.t_base_item_search where ds = '0temp' group by nid;
EOF