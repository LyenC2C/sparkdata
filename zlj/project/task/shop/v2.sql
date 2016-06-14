

create table t_zlj_shop_desc_score_rank_v2 as
SELECT
t2.shop_id,t2.shop_name, (smax-desc_score)/len
FROM
(
SELECT

1 as id ,max(desc_score)-min(desc_score) as len,max(desc_score) as smax

FROM
(
SELECT
shop_id ,shop_name ,5*desc_highgap/100.0 + 2*service_highgap/100.0 +1*wuliu_highgap/100.0  as desc_score
from
t_base_ec_shop_dev_new
where ds=20160613 and bc_type='B'
)t

)t1 join
(
SELECT
1 as id  ,shop_id ,shop_name ,5*desc_highgap/100.0 + 2*service_highgap/100.0 +1*wuliu_highgap/100.0  as desc_score
from
t_base_ec_shop_dev_new
where ds=20160613 and bc_type='B'
)t2 on t1.id =t2.id

limit 10;
