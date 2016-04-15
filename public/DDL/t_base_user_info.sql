CREATE  TABLE  if not exists t_base_user_info_s(
tb_id  string  COMMENT '淘宝 id',
tgender   string  COMMENT '腾讯性别 0男 1女',
tage  int COMMENT '腾讯年龄',
tname  String  COMMENT '腾讯姓名',
tloc  string COMMENT '地址'
)
COMMENT '电商品牌数据'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;
