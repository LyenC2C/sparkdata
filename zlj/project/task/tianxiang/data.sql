
create table  t_tianxiang_feed_item as

select  /*+ mapjoin(t1)*/

t1.*,
t2.user_id,t2.sku as buy_sku,rate_type,crawl_type,
t2.ds as fds
from

t_wrt_mask_iteminfo t1 join
wlbase_dev.t_base_ec_item_feed_dev t2
on t1.item_id =t2.item_id ;



create table t_tianxiang_feed_item_user as

select  t1.*,t2.tgender,tage,tname,tloc
from t_tianxiang_feed_item t1

join wlbase_dev.t_base_user_info_s t2
on t1.user_id=t2.tb_id and t2.ds=20160310 ;



-- create table  t_tianxiang_feed_item as
--
-- select  /*+ mapjoin(t1)*/
--
-- t1.*,
-- t2.user_id,t2.sku as buy_sku,rate_type,crawl_type,
-- t2.ds as fds
-- from
--
-- t_wrt_mask_iteminfo t1 join
-- wlbase_dev.t_base_ec_item_feed_dev t2
-- on t1.item_id =t2.item_id ;




-- 用户商品和评价关联
t_zlj_feed2016_parse_v1
