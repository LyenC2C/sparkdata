-- 网购偏好


DROP  TABLE  IF EXISTS  t_base_credit_consume_perfer;

create TABLE  t_base_credit_consume_perfer as
SELECT
uid as  tb_id,
'*' as esp_perfer ,
brand,
level_ as user_per_level ,
case when LENGTH(cat_tags )>1 then 1 else -1 end as cat_flag ,
case when LENGTH(house )>1 then 1 else -1 end as house_flag

FROM
t_zlj_user_tag_join_t t1
join t_base_credit_consume_per_price t2
on  uid  is not null and t1.uid=t2.user_id
;
