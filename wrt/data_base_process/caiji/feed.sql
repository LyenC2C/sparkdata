create table t_wrt_tmp_record_c_20161028 as
select item_id,ds,sold from
(
select
t1.item_id,
case when t2.ds is null then "19760101" else t2.ds end as ds,
t1.sold
from
(select item_id,sold from wlbase_dev.t_base_ec_shopitem_c where ds = 20160922)t1
left join
(select item_id,max(dsn) as ds from wlbase_dev.t_base_ec_record_dev_new where ds = 'true' group by item_id)t2
on
t1.item_id = t2.item_id
)t
order by sold desc;


--20161108临时脚本

create table wlservice.t_wrt_tmp_record_c_20161108 as
select
t1.item_id,
case when t2.ds is null then "19760101" else t2.ds end as ds
from
(select a.item_id from
(select item_id from wlbase_dev.t_base_ec_item_dev_new where ds = 20161104 and bc_type = 'C')a
left JOIN
t_wrt_tmp_record_c_20161028 b
ON
a.item_id = b.item_id
WHERE
b.item_id is NULL
)t1
left join
(select item_id,max(dsn) as ds from wlbase_dev.t_base_ec_record_dev_new where ds = 'true' group by item_id)t2
on
t1.item_id = t2.item_id

