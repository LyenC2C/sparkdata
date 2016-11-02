


SELECT
shop_id, sum(day_sold),sum(day_price)
from
(
SELECT
shop_id ,day_sold, day_sold_price*day_sold as day_price ,substring(ds,0,6) as m
FROM t_base_ec_item_daysale_dev_new   t1

join
 t_base_ec_item_dev_new t2 on t1.item_id=t2.item_id and t2.ds=20160607
)t group by shop_id
;


