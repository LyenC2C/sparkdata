

create  table  t_zlj_user_brand_influence as

select user_id,
concat_ws(' ',collect_set(concat_ws('_',brand_id,brand_name,cast(influence_score as string)))) as brand_loyalty
from
(

SELECT
user_id ,brand_id,brand_name,
round(sum(log2(LENGTH(impr))+log2(LENGTH(impr_c))),2) as influence_score
from
t_zlj_feed2015_parse_jion_cat_brand_shop

where LENGTH(brand_id)>0

group by user_id,brand_id,brand_name
)t group by user_id  ;