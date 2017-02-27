CREATE TABLE  if not exists wlbase_dev.t_base_ec_xianyu_userinfo(
userid String COMMENT '用户id',
totalcount String COMMENT '商品数目',
gender String COMMENT '用户性别',
idleuserid String COMMENT '账号是否闲置 猜测',
nick String COMMENT '用户昵称',
tradecount int COMMENT '交易次数',
tradeincome Float COMMENT '交易收入',
usernick String COMMENT '淘宝昵称',
constellation String COMMENT '星座',
birthday String COMMENT '出生日期',
city String COMMENT '城市区域代码',
infoPercent String COMMENT '用户信息完整度',
signature String COMMENT '用户个性签名',
ts String COMMENT '时间戳'
)
COMMENT '闲鱼用户信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

CREATE TABLE  if not exists wl_base.t_base_ec_xianyu_userinfo(
userid String COMMENT '用户id',
totalcount String COMMENT '商品数目',
gender String COMMENT '用户性别',
idleuserid String COMMENT '账号是否闲置 猜测',
nick String COMMENT '用户昵称',
tradecount int COMMENT '交易次数',
tradeincome Float COMMENT '交易收入',
usernick String COMMENT '淘宝昵称',
constellation String COMMENT '星座',
birthday String COMMENT '出生日期',
city String COMMENT '城市区域代码',
infoPercent String COMMENT '用户信息完整度',
signature String COMMENT '用户个性签名',
ts String COMMENT '时间戳'
)
COMMENT '闲鱼用户信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;


