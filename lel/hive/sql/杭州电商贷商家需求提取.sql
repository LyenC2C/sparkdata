杭州电商贷商家需求提取

融信通:

城市限制	杭州（商家与店铺所在地均在杭州）
品类限制 非充值、点卡销售、虚拟物品交易类店铺
近一年销售额 >60万

数据输出格式：号码+所在地+店铺+品类+销售额

201609->till now

select A.*,t3.totalincome 
(select A.tb_id ,A.tb_location ,B.shop_id,B.root_cat_name as cate
(select tb_id,tb_location from t_base_user_profile_telindex where tb_location regexp '杭州') A
join
(select shop_id,user_id,root_cat_name,location
from wl_base.t_base_ec_record_dev_new where ds='true' and title  not regexp '充值|点卡销售|虚拟物品' and root_cat_name not regexp '充值|点卡销售|虚拟物品' and  cast(dsn as int) > 20160307
and location regexp '杭州')B
on A.tb_id = B.user_id)C
join
(select 
t1.shop_id as shop_id,sum(t2.day_sold_price) as totalincome
from
(select  item_id,shop_id from wlbase_dev.t_base_ec_shopitem_b  where ds =20170306 ) t1 
join
(select item_id,day_sold_price from wlbase_dev.t_base_ec_item_daysale_dev_new where cast(ds as int ) > 20160306) t2
on t1.item_id=t2.item_id
group by t1.item_id,t1.shop_id) t3
C.shop_id = t3.shop_id


201603->201703
buser&seller:
create table wl_service.t_lel_rongxintong_20170307_c
as 
select t6.tb_id,t6.`location`,t7.shop_name,t6.cate,t6.income
from
(select shop_id,shop_name from t_base_ec_shop_dev_new where ds='20170304') t7
join
(select C.tb_id,C.`location`,C.shop_id,C.cate,t5.income
from
(select A.tb_id as tb_id,A.tb_location as `location`,B.shop_id as shop_id,B.root_cat_name as cate
from
(select tb_id,tb_location from t_base_user_profile_telindex where tb_location regexp '杭州') A
join
(select shop_id,user_id,item_id,root_cat_name,`location`
from wl_base.t_base_ec_record_dev_new where ds='true' and title  not regexp '充值|点卡销售|虚拟物品' and root_cat_name not regexp '充值|点卡销售|虚拟物品' and  cast(dsn as int) > 20160306
and `location` regexp '杭州')B
on A.tb_id = B.user_id)C
join
(select t4.shop_id as shop_id,sum(t3.income) as income
from
(select  item_id,shop_id from wl_base.t_base_ec_shopitem_c  where ds ='20170306' ) t4
join
(select  t1.item_id,t2.income-t1.income as income from
	(select item_id,total*price as income from t_base_ec_item_sold_dev where ds='20160424') t1
	join
	(select item_id,total*price as income from t_base_ec_item_sold_dev where ds='20170306')t2
	on t1.item_id=t2.item_id
) t3
on t3.item_id = t4.item_id
group by shop_id having sum(t3.income) >600000) t5
on t5.shop_id=C.shop_id) t6
on t6.shop_id = t7.shop_id

201603->201703
seller:
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
create table wl_service.t_lel_rongxintong_20170307_cv2
	as
select D.seller_id,D.`location`,D.shop_name,C.cate,C.income
from
(select shop_id,seller_id,shop_name,`location` from t_base_ec_shop_dev_new where ds='20170304' and `location` regexp '杭州') D
join
(select A.shop_id as shop_id,A.root_cat_name as cate,B.income as income
	from
(select shop_id,root_cat_name
from wl_base.t_base_ec_record_dev_new where ds='true' and title  not regexp '充值|点卡销售|虚拟物品' and root_cat_name not regexp '充值|点卡销售|虚拟物品' and  cast(dsn as int) > 20160306
and `location` regexp '杭州' group by shop_id,root_cat_name)A
join 
(select t4.shop_id as shop_id,sum(t3.income) as income
from
(select  item_id,shop_id from wl_base.t_base_ec_shopitem_c  where ds ='20170306' ) t4
join
(select  t1.item_id,t2.income-t1.income as income from
	(select item_id,total*price as income from t_base_ec_item_sold_dev where ds='20160424') t1
	join
	(select item_id,total*price as income from t_base_ec_item_sold_dev where ds='20170306')t2
	on t1.item_id=t2.item_id
) t3
on t3.item_id = t4.item_id
group by t4.shop_id having sum(t3.income) >600000) B
on A.shop_id = B.shop_id
)C
on C.shop_id=D.shop_id

select a.* from wl_service.t_lel_rongxintong_20170307_res a where substr(a.phone,0,7) in (select prefix from wl_base.t_base_mobile_loc where city regexp '杭州')

/^((\d3)|(\d{3}\-))?13[456789]\d{8}|15[89]\d{8}/


awk -F '\001' '$1 ~ /^1[1-9]+$/{if (length($1)==11)print $1"\001"$2}' rongxintong_c.teltbname > rongxintong_c.teltbname_filtered
awk -F '\001' 'NR==FNR{a[$2]=$1}NR!=FNR{if($1 in a)print a[$1]"\t"$2"\t"$3"\t"$4"\t"$5}' rongxintong_c.teltbname_filtered rongxintong_c > c

awk -F '\001' '$1 ~ /^1[1-9]+$/{if (length($1)==11)print $1"\001"$2}' rongxintong_b.teltbname > rongxintong_b.teltbname_filtered
awk -F '\001' 'NR==FNR{a[$2]=$1}NR!=FNR{if($1 in a)print a[$1]"\t"$2"\t"$3"\t"$4"\t"$5}' rongxintong_b.teltbname_filtered rongxintong_b > b




