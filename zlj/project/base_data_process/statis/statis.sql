

-- 独立用户数
select count(1) from
(SELECT

user_id

from
t_base_ec_item_feed_dev
where ds >20150701
group by user_id
)t ;


-- 独立商品数
select count(1) from
(SELECT

item_id

from
t_base_ec_item_feed_dev
where ds >20150701
group by item_id
)t ;


-- 购买记录数
SELECT

COUNT(1)
from
t_base_ec_item_feed_dev
where ds >20150701 ;


-- 购买记录商品连接数

select count(1) from
t_zlj_ec_userbuy  ;



-- 商品数

select count(1) from t_base_ec_item_dev where ds=20151030