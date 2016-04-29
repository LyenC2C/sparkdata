



-- ROW_NUMBER() ʹ������
-- ȡÿ����Ŀ������ topn  ��Ʒ��

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

where rn <10

;