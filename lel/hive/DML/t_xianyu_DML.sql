firstModifiedDiff String COMMENT '第一次更改时间间隔',
fishPoolId String COMMENT '鱼塘id'
fishpoolName String COMMENT '鱼塘名'
bar String COMMENT '类似鱼塘名'
barInfo String COMMENT 'bar等级'
abbr String COMMENT '用户标签'
zhima String COMMENT '芝麻信用认证'
shiren String COMMENT '实人认证'
ts String COMMENT '时间戳'



insert OVERWRITE table wlbase_dev.t_base_ec_xianyu_iteminfo PARTITION(ds='20160721_old')
select
id,userId,title,province,city,area,auctiontype,description,detailFrom,favorNum,
commentNum,firstModified,null,t_from,gps,offline,originalPrice,price,userNick,categoryId,
null,null,community_name,community_name,null,null,null,null,null
from wlbase_dev.t_base_ec_tb_xianyu_item

load data inpath "/user/lel/result" into table t_lel_ec_xianyu_item_info;

spark-submit --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 ~/wolong/sparkdata/spark/t_xianyu.py

