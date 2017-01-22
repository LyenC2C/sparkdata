create table t_wrt_caiji_record_c_20161224 as
select item_id,ds,sold from
(
select
t1.item_id,
case when t2.ds is null then "19760101" else t2.ds end as ds,
t1.sold
from
(select item_id,sold from wlbase_dev.t_base_ec_shopitem_c where ds = 20161221)t1
left join
(select item_id,max(dsn) as ds from wlbase_dev.t_base_ec_record_dev_new where ds = 'true' group by item_id)t2
on
t1.item_id = t2.item_id
)t
order by sold desc;



--20161227tmall全量id

create table wlservice.t_wrt_caiji_record_b_2017 as
select item_id,ds,sold from
(
select
t1.item_id,
case when t2.ds is null then "19760101" else t2.ds end as ds,
t1.sold
from
(select item_id,sold from wlbase_dev.t_base_ec_shopitem_b where ds = 20161226)t1
left join
(select item_id,max(dsn) as ds from wl_base.t_base_ec_record_dev_new where ds = 'true' and bc_type = 'B'
group by item_id)t2
on
t1.item_id = t2.item_id
)t
order by sold desc;





