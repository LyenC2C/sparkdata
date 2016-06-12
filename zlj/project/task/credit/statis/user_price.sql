create table t_zlj_credit_user_price_statis  as
select  user_id , COUNT(1) as times, sum(price) as sum_p ,  max(price) as max_p, min(price) as min_p ,avg(price) as avg_p
from t_base_ec_record_dev_new_simple  group by user_id ;





select level1 , COUNT(1) from  (select CAST(log2(max_p) as int ) level1  from t_zlj_ec_user_statis )t group by level1
select level1 , COUNT(1) from  (select CAST(log2(min_p) as int ) level1  from t_zlj_ec_user_statis )t group by level1


insert OVERWRITE table t_base_ec_item_dev_new PARTITION(ds='20160401')
select
case when  t2.item_id   is not null then  t2.item_id       else t1.item_id       end ,
case when  t2.item_id   is not null then  t2.title         else t1.title         end ,
case when  t2.item_id   is not null then  t2.cat_id        else t1.cat_id        end ,
case when  t2.item_id   is not null then  t2.cat_name      else t1.cat_name      end ,
case when  t2.item_id   is not null then  t2.root_cat_id   else t1.root_cat_id   end ,
case when  t2.item_id   is not null then  t2.root_cat_name else t1.root_cat_name end ,
case when  t2.item_id   is not null then  t2.brand_id      else t1.brand_id      end ,
case when  t2.item_id   is not null then  t2.brand_name    else t1.brand_name    end ,
case when  t2.item_id   is not null then  t2.bc_type       else t1.bc_type       end ,
case when  t2.item_id   is not null then  t2.price         else t1.price         end ,
case when  t2.item_id   is not null then  t2.price_zone    else t1.price_zone    end ,
case when  t2.item_id   is not null then  t2.is_online     else t1.is_online     end ,
case when  t2.item_id   is not null then  t2.off_time      else t1.off_time      end ,
case when  t2.item_id   is not null then  t2.favor         else t1.favor         end ,
case when  t2.item_id   is not null then  t2.seller_id     else t1.seller_id     end ,
case when  t2.item_id   is not null then  t2.shop_id       else t1.shop_id       end ,
case when  t2.item_id   is not null then  t2.location      else t1.location      end ,
case when  t2.item_id   is not null then  t2.paramap       else t2.paramap       end ,
case when  t2.item_id   is not null then  t2.sku           else t2.sku          end ,
case when  t2.item_id   is not null then  t2.ts            else t1.ts           end

from t_base_ec_item_dev t1  full OUTER join t_base_ec_item_dev_new t2

on t1.item_id =t2.item_id  and t1.ds=20160333 and t2.ds=20160606