艾瑞数据需求20170401


naifen:

root_cat_name       root_cat_id
奶粉/辅食/营养品/零食	35          
咖啡/麦片/冲饮	        50026316	
传统滋补营养品         50020275
家庭保健	            122718004

niunai:
咖啡/麦片/冲饮        50026316

niurougan:

咖啡/麦片/冲饮        50050359
水产肉类/新鲜蔬果/熟食  50026316 
零食/坚果/特产		    50002766 




create table wl_service.t_lel_naifen_cat_test
as
select distinct title,root_cat_name,root_cat_id,cat_id from wl_base.t_base_ec_record_dev_new where ds='true' and title regexp '奶粉'



create table wl_service.t_lel_niunai_cat_test
as
select distinct title,root_cat_name,root_cat_id,cat_id from wl_base.t_base_ec_record_dev_new where ds='true' and title regexp '牛奶'

create table wl_service.t_lel_niurougan_cat_test
as
select distinct title,root_cat_name,root_cat_id,cat_id from wl_base.t_base_ec_record_dev_new where ds='true' and title regexp '牛肉干'


饮料 50009864

冲调食品   50009860

保健品  123454001

零食    123224002

select b.item,a.cate_level1_name,concat_ws(",",collect_set(a.cate_level2_name)) as levels
from
(select  cate_level1_id,cate_level1_name,cate_level2_name from t_base_ec_dim where cate_level2_name <> '' )  a
join
(select  cate_level1_id,
case when  cate_id in (50009864) then '饮料'
when  cate_id in (50009860) then '冲调食品  '
when cate_id in (123454001) then '保健品'
when cate_id in (123224002) then '零食'
end as item
from t_base_ec_dim where cate_id in (50009864,50009860,123454001,123224002))b
on a.cate_level1_id=b.cate_level1_id
group by item,cate_level1_name 