create table t_wrt_shopinfo_datatao(
shop_id String,
shop_name String,
seller_name String,
star String,
credit String,
starts String,
item_count String,
fans_count String,
good_rate_p String,
desc_score String,
service_score String,
wuliu_score String,
location  String
);

insert overwrite table t_wrt_shopinfo_datatao
select  t1.shop_id, t1.shop_name, t1.seller_name, t1.star, t1.credit, t1.starts,t1.item_count, t1.fans_count, t1.good_rate_p, t1.desc_score, t1.service_score, t1.wuliu_score, t1.location from
(select * from t_base_ec_shop_dev where ds = 20160225)t1
join
(select shop_id from t_base_ec_item_dev where ds = 20160225 and root_cat_id = 1801 group by shop_id limit 100)t2
on
t1.shop_id = t2.shop_id;


--查看数量
select count(1) from(
select shop_id from t_base_ec_item_dev where ds = 20160225 and root_cat_id = 1801 and bc_type = 'B' group by shop_id
)t;