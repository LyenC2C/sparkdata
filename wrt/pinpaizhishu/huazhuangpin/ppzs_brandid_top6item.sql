create table wlservice.ppzs_brandid_top6item
(
brand_id string,
item_id string,
title string,
price string,
picurl string,
now_sold string,
rn string
)
COMMENT '品牌指数项目品牌销量top6商品'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

insert overwrite table ppzs_brandid_top6item partition (ds = 20161108)
select brand_id,item_id,title,price,picurl,now_sold,rn
from
(
select
t1.item_id,
t1.brand_id,
t1.title,
t1.price,
t1.picurl,
t2.now_sold,
ROW_NUMBER() OVER (PARTITION BY t1.brand_id ORDER BY t2.now_sold DESC) as rn
from
(select brand_id,item_id,title,price,picurl from wlservice.ppzs_itemid_info where ds = 20161108)t1
JOIN
(
select item_id,max(sold) as now_sold from wlbase_dev.t_base_ec_shopitem_b where ds <= 20161106 and ds >= 20161031
group by item_id
)t2
ON
t1.item_id = t2.item_id
)t
where rn < 7;