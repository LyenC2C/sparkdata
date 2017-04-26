




create table wlservice.t_wrt_caiji_search_newshopid_b_20170104 as
select shop_id from wl_base.t_base_ec_item_dev_new where ds = 20170103 and bc_type = 'B' group by shop_id;

create table wlservice.t_wrt_caiji_search_newshopid_c_20170104 as
select shop_id from wl_base.t_base_ec_item_dev_new where ds = 20170103 and bc_type = 'C' group by shop_id;

select count(1) from
(
select t1.shop_id
wlservice.t_wrt_caiji_search_newshopid_b_20170104 t1
right join
(select shop_id from wlbase_dev.t_base_ec_shopitem_b)t2
on
t1.shop_id = t2.shop_id
where t2.shop_id is null
)t

