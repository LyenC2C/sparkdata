
drop table if EXISTS  t_zlj_tmp;

create table t_zlj_tmp AS

SELECT
item_id ,title,root_cat_id,root_cat_name
FROM

t_base_ec_item_dev
where  ds=20151107 and root_cat_id in
(
50011699,
30,
16,
1625,
50011699
) ;