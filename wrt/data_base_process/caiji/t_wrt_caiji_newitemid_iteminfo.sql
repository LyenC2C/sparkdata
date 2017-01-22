create table wlservice.t_wrt_caiji_sitemc_newid_iteminfo_20161230 as
select item_id,sold from
(
select t1.item_id,t1.sold from
(select item_id,sold from wlbase_dev.t_base_ec_shopitem_c where ds = 20161221)t1
left JOIN
(select item_id from wl_base.t_base_ec_item_dev_new where ds = 20161230)t2
ON
t1.item_id = t2.item_id
where t2.item_id is null
)t
order by sold desc;


create table wlservice.t_wrt_caiji_sitemb_newid_iteminfo_20161230 as
select item_id,sold from
(
select t1.item_id,t1.sold from
(select item_id,sold from wlbase_dev.t_base_ec_shopitem_b where ds = 20161229)z
left JOIN
(select item_id from wl_base.t_base_ec_item_dev_new where ds = 20161224)t2
ON
t1.item_id = t2.item_id
where t2.item_id is null
)t
order by sold desc;


