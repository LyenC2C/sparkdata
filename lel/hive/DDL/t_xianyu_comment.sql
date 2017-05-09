CREATE TABLE  if not exists wlbase_dev.t_base_ec_xianyu_item_comment(
itemid String COMMENT '商品id',
commentId String COMMENT '评论id',
content String COMMENT '评论内容',
reportTime String COMMENT '评论时间',
reporterName String COMMENT '评论者昵称',
reporterNick String COMMENT '评论者别名',
ts String COMMENT '时间戳'
)
COMMENT '闲鱼商品评论信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;
