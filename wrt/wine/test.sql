
select count(1) from
t_wrt_baijiu
join
t_wrt_0401_baijiu
on
t_wrt_baijiu.item_id = t_wrt_0401_baijiu.item_id;

LOAD DATA  INPATH '/user/wrt/temp/baijiu_0401_itemid' OVERWRITE INTO TABLE t_wrt_0401_baijiu


insert overwrite table t_wrt_wine_itemid
select t1.item_id from
(select item_id from wlbase_dev.t_base_ec_item_sale_dev where ds = 20160316)t1
join
(
select item_id from wlbase_dev.t_base_ec_item_dev
where ds = 20160331 and root_cat_id = 50008141
)t2
on
t1.item_id = t2.item_id;


insert overwrite table t_wrt_wine_itemid
select item_id from wlbase_dev.t_base_ec_item_dev where ds = 20160331 and root_cat_id = 50008141;


insert overwrite table t_wrt_wine_else_itemid
select t_wrt_wine_itemid.item_id from
t_wrt_wine_itemid
left join
t_wrt_wine_0401_itemid
ON
t_wrt_wine_itemid.item_id = t_wrt_wine_0401_itemid.item_id
WHERE
t_wrt_wine_0401_itemid.item_id is NULL;


insert overwrite table t_wrt_iteminfo_shopid_sellerid
select shop_id,seller_id from t_wrt_baijiu_iteminfo group by shop_id,seller_id;

select count(1) from
(select item_id from wlbase_dev.t_base_ec_item_sale_dev where ds = 20160316)t1
join
(
select item_id from wlbase_dev.t_base_ec_item_dev
where ds = 20160331 and (cat_id = 50013052 or cat_id = 50008144) and bc_type = 'B'
)t2
ON
t1.item_id = t2.item_id;

insert overwrite table t_wrt_lzlj_id_title
select item_id,title from t_wrt_baijiu_iteminfo where brand_id = 4537002;