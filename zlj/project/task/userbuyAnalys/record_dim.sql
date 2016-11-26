






-- 三级类目统计数据、如果没有三级，统计二级
Drop table t_zlj_record_tmp_dim234_cate_level3 ;
create table t_zlj_record_tmp_dim234_cate_level3  as
SELECT
tel_index,cate_level3,
round(sum(price),2) price_sum,
round(count(1) ,2)  buy_count,
round(avg(price),2) price_avg,
round(max(price),2) price_max,
round(min(price),2) price_min ,
round(std(price),2) price_std
from
(
SELECT
* ,
COALESCE(cate_level3_id,cate_level2_id) as cate_level3
from t_zlj_record_tmp_dim234 where tel_index is not null
)t group by tel_index,cate_level3;




