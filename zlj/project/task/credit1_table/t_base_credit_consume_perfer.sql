-- 网购偏好

create TABLE  t_base_credit_consume_perfer as
SELECT
uid as  tb_id,
dim,
'*' as esp_perfer ,
brand,
level_ as user_per_level
FROM
t_zlj_user_tag_join_t t1
join t_base_credit_consume_per_price t2
on  uid  is not null and t1.uid=t2.user_id
;