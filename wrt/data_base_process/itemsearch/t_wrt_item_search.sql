create table wlservice.t_wrt_item_search(
nid string COMMENT '商品id',
user_id string COMMENT '用户id',
comment_count string COMMENT '评论数量',
encryptedUserId string COMMENT '用户加密',
nick string comment '昵称'
)
COMMENT '电商商品搜索表'
PARTITIONED BY  (ds STRING )
;



insert overwrite table wlservice.t_wrt_item_search partition(ds = '20170104_20_30_uniq')
SELECT
nid,max(user_id),max(comment_count),max(encryptedUserId),max(nick)
FROM
wlservice.t_wrt_item_search
where ds = '20170104_20_30'
group by nid;

insert overwrite table wlservice.t_wrt_item_search partition(ds = '20170104_else_uniq')
SELECT
nid,max(user_id),max(comment_count),max(encryptedUserId),max(nick)
FROM
wlservice.t_wrt_item_search
where ds = '20170104_else'
group by nid;

insert overwrite table wlservice.t_wrt_item_search partition(ds = '20170105_else_no2030')


