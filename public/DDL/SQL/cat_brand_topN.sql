



-- ROW_NUMBER() ʹ������
-- ȡÿ����Ŀ������ topn  ��Ʒ��

select
 *
from
(
select
brand_id,brand_name,root_cat_id,root_cat_name  ,
     ROW_NUMBER() OVER (PARTITION BY root_cat_id ORDER BY num  DESC) AS rn

from
(
select  brand_id,brand_name,root_cat_id,root_cat_name ,sum(total_sold) as num
from
t_base_ec_sale_iteminfo
-- where  condition
group by  brand_id,brand_name,root_cat_id,root_cat_name
)t

)t1
where rn <10

;

SELECT user_id,cate_level2_id,price
from
(
select user_id,cate_level2_id,price ,ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY price  DESC) AS rn
from
(
	select user_id,cate_level2_id,sum(price) as   price
	from t_base_ec_dim  as t1 join  t_zlj_ec_userbuy as t2 on t2.cat_id=t1.cate_id where cate_level2_id is not null
 group by user_id ,cate_level2_id

)t

)t1
where rn< 12


SELECT * from
(
SELECT
cate_name,  tag1  ,count(1) as num ,  ROW_NUMBER() OVER (PARTITION BY cate_name ORDER BY num  DESC) AS rn
FROM
t_tc_korea_10allusertag
GROUP  cate_name ,tag1

)t where rn <50


