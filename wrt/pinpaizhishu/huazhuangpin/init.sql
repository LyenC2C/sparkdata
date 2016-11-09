create table t_wrt_caiji_ppzs_itemid_20161107 as
select tt2.item_id from
(select t2.shop_id as shop_id from
(select brand_id from t_wrt_tmp_ppzs_brandid)t1
JOIN
(select shop_id,brand_id from wlbase_dev.t_base_ec_item_dev_new where ds=20161104 and bc_type = 'B')t2
ON
t1.brand_id = t2.brand_id
group by
t2.shop_id
)tt1
JOIN
(
select item_id,shop_id from wlbase_dev.t_base_ec_shopitem_b where ds = 20161105
)tt2
ON
tt1.shop_id = tt2.shop_id

--所得item_id为店铺中的item_id，包括各种品牌，后续需要过滤
--iteminfo的历史item_id和brand_id，即便已经下架也不要忘了加进去，