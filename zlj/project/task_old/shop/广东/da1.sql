SELECT

cate_level1_name,count(1)

from

(
	SELECT t2.*
	FROM

	(SELECT shop_id
		from t_base_ec_shop_dev_new
		WHERE  ds=20160622  and location LIKE '%广东%'
	)t1  join
	(

	SELECT shop_id ,item_id ,cat_id
	from
	t_zlj_shop_shop_user_level_verify

	)t2 on t1.shop_id =t2.shop_id
)t3 join t_base_ec_dim t4 on t3.cat_id =t4.cate_id

group by cate_level1_name  ;