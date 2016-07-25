DROP   TABLE t_base_ec_tb_xianyu_item;

CREATE TABLE IF NOT EXISTS t_base_ec_tb_xianyu_item (
  id            string COMMENT '商品id',
  title         string COMMENT '商品title',
  categoryId    string COMMENT '类目',
  province      string COMMENT '省',
  city          string COMMENT '市',
  area          string COMMENT '地区',
  auctionType   string COMMENT '商品类型',
  description   string COMMENT '商品描述',
  detailFrom    string COMMENT '来源 Android iPhone',
  favorNum      INT   COMMENT '收藏',
  commentNum    INT   COMMENT '评价数',
  firstModified string COMMENT '上架时间',
  t_from        string COMMENT '',
  gps           string COMMENT 'gps',
  offline       string COMMENT '下架商品数',
  originalPrice DOUBLE COMMENT '原价',
  price         DOUBLE COMMENT '卖价',
  userId        string COMMENT '用户id',
  userNick      string COMMENT '用户昵称' ,
  community_name string COMMENT '小区名字'
)
COMMENT '电商淘宝闲鱼用户信息表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n';