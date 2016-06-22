


-- 收藏数
SELECT sum(favor)
  FROM t_base_ec_item_dev_new
      WHERE ds = 20160615  and shop_id='65525181' ;


      -- 类目分布

SELECT shop_id,cat_name,count(1)
from t_base_ec_item_dev_new where ds=20160615 and shop_id in (
'105799295',
'65525181',
'35432231',
'110999350',
'107903881',
'103889467',
'72113206',
'105777081',
'105951398',
'104738351',
'144619358',
'59288165',
'110452617',
'59559926'
)   group by shop_id,cat_name ;

