select COUNT(1) from t_base_user_info_s where  ds=20160310 and  LENGTH(tloc)>0 and LENGTH(tgender)>0

t_zlj_korea_sold_itemid_count_new;

create table t_zlj_korea_sold_itemid_count_new_s as
select

t1.item_id,cat_id,root_cat_id,brand_id,brand_name,total_sold,s_price,bc_type
from
(
 select item_id,total_sold,s_price from t_base_ec_item_sale_dev_new where ds=20160316
  )t1 join t_base_ec_item_dev t2 on t2.ds=20160333 and t1.item_id=t2.item_id  ;



t_tc_brand_korea

CREATE  TABLE  if not exists t_tc_brand_korea (
root_cat_name   String COMMENT '',
cat_name  String ,
brand  String
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;



select brand_name,root_cat_id  ,sum(total_sold),sum(total_sold*s_price),bc_type from  t_tc_brand_korea t1
join  t_zlj_korea_sold_itemid_count_new_s t2  on t1.brand=t2.brand_name group by brand_name,root_cat_id,bc_type ;

