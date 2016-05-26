

select
logp,count(1) as num
from
(
select
brand_id, brand_name,CAST(log(avg(price)) as int ) as logp
from  t_base_ec_item_dev where ds=20160333 and root_cat_id='1801'
 and CAST(brand_id as int )>0
group by brand_id ,brand_name
)t   group by logp ;




select
logp,count(1) as num
from
(
select
brand_id, brand_name,CAST(log(avg(price)) as int ) as logp
from  t_base_ec_item_dev where ds=20160333 and root_cat_id='50008141'
 and CAST(brand_id as int )>0
group by brand_id ,brand_name
)t   group by logp ;



insert overwrite table t_base_ec_brand_level partition(ds='wine_root')

SELECT
*
FROM
(
select
* , ROW_NUMBER() OVER (PARTITION BY level_num ORDER BY num  DESC) AS rn
from
(
select
root_cat_id ,brand_id, brand_name,count(1) num,
case
when CAST(log(avg(price)) as int )<=3 then 1
when CAST(log(avg(price)) as int )=4 then 2
when CAST(log(avg(price)) as int )=5 then 3
when CAST(log(avg(price)) as int )=6 then 4
when CAST(log(avg(price)) as int )>6 then 5
else -1 end as level_num

from  t_base_ec_item_dev where ds=20160333 and root_cat_id='50008141' and bc_type='B'
 and CAST(brand_id as int )>0
group by brand_id ,brand_name
)t

)t2   where rn <10  ;