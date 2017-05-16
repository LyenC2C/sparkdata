艾瑞数据需求:
1. 商品详情和销量数据的进一步了解，
请您提供以下商品3月以来在天猫的商品详情（含价格，品类，店铺，库存，运费，品牌型号等）和销量（包括累计销量和日销量）的监测数据

【苏宁易购超市】德亚 全脂纯牛奶 200ML*30盒 德国进口
!!! itemid:543795706267 
【新品上市】惠氏启韵孕产妇配方调制乳粉800g 孕哺期孕妇奶粉   543795706267,543796031687,520804075368,521020623256
!!! itemid:543796031687 
【伊利直营】高钙高铁中青年成人配方营养奶粉900*2听
itemid:14303055836 
美赞臣安婴宝A+婴幼儿配方牛奶粉2段900g*2罐 适合6-12个月
itemid:529753287414 
【伊利直营】金领冠 1段新生儿婴儿配方宝宝奶粉900g听
itemid:14314939049 
【天猫超市】蒙牛纯甄常温酸牛奶200g*6盒好酸奶不添加
itemid:530222374693 
安佳 全脂纯牛奶250ml*24盒 新西兰进口
itemid:521244891536 
【天猫超市】雀巢成人奶粉 安骼高钙高铁女士成人奶粉400g无蔗糖
itemid:20896699903 

【天猫超市】美赞臣0段安婴妈妈A+孕妇奶粉900g*1罐 进口奶源
itemid:520804075368
【天猫超市】【第二件半价】蒙牛酸酸乳草莓乳味饮品250ml*24盒
itemid:521020623256

itemids:521020623256,543795706267,543796031687,529753287414,14303055836,14314939049,530222374693,521244891536,20896699903,520804075368
shopids:
	111021164
	104332625
	70405254
	67597230
	128573071


item:
	530222374693
	20896699903
3	543795706267
	14314939049
	14303055836
	521244891536
7	543796031687
	529753287414
sold:
    530222374693
2   520804075368
	20896699903
	14314939049
	14303055836
	521244891536
7	521020623256
	529753287414
	
521020623256,543795706267,543796031687,529753287414,14303055836,14314939049,530222374693,521244891536,20896699903,520804075368
含价格，品类，店铺，库存，运费，品牌型号等）和销量（包括累计销量和日销量
price,root_cat_name,shop_id,shop_name,kucun,brand_name,sold,daysold

create table wl_service.t_lel_airuisi_iteminfo
as
select shop_id,item_id,sold,saleprice,ds from t_base_ec_shopitem_b where cast(ds as int) >= 20170228 and item_id regexp '543795706267|543796031687|14303055836|529753287414|14314939049|520804075368|530222374693|521020623256|521244891536|20896699903'


create table wl_service.t_lel_airuisi_sold_withflag
as
select item_id,price,qu,ds,cp_flag from wl_base.t_base_ec_item_sold_dev where cast(ds as int) >= 20170228 and item_id regexp '543795706267|543796031687|14303055836|529753287414|14314939049|520804075368|530222374693|521020623256|521244891536|20896699903'


drop table wl_service.t_lel_airuisi_8items_withflag;
create table wl_service.t_lel_airuisi_8items_withflag
as
select a.shop_id,a.item_id,a.shop_name,a.sold,a.saleprice,a.qu,a.ds,b.root_cat_name,b.brand_name,a.cp_flag
from
(select t4.shop_name,t3.shop_id,t3.item_id,t3.sold,t3.saleprice,t3.qu,t3.ds,t3.cp_flag
from
(select t1.shop_id,t1.item_id,t1.sold,t1.saleprice,t2.qu,t1.ds,t2.cp_flag
from
(select shop_id,item_id,sold,saleprice,ds from wl_service.t_lel_airuisi_iteminfo)t1
join 
(select item_id,price,qu,ds,cp_flag from wl_service.t_lel_airuisi_sold_withflag)t2
on t1.item_id = t2.item_id and t1.ds=t2.ds)t3
join
(select shop_id,shop_name from wl_base.t_base_ec_shop_dev_new where ds ='20170322' and shop_id regexp '111021164|104332625|70405254|67597230|128573071')t4
on t3.shop_id = t4.shop_id)a
join
(select item_id,root_cat_name,brand_name from wl_base.t_base_ec_item_dev_new where ds = '20170322' and item_id regexp '543795706267|543796031687|14303055836|529753287414|14314939049|520804075368|530222374693|521020623256|521244891536|20896699903')b
on a.item_id = b.item_id

create table t_lel_airuisi_8items_withflag_marked
as
SELECT case when item_id = '543795706267' then '【苏宁易购超市】德亚 全脂纯牛奶 200ML*30盒 德国进口'
when item_id='543796031687' then '【新品上市】惠氏启韵孕产妇配方调制乳粉800g 孕哺期孕妇奶粉'
when item_id='14303055836' then '【伊利直营】高钙高铁中青年成人配方营养奶粉900*2听'
when item_id='529753287414' then '美赞臣安婴宝A+婴幼儿配方牛奶粉2段900g*2罐 适合6-12个月'
when item_id='14314939049' then '【伊利直营】金领冠 1段新生儿婴儿配方宝宝奶粉900g听'
when item_id='530222374693'  then '【天猫超市】蒙牛纯甄常温酸牛奶200g*6盒好酸奶不添加'
when item_id='521244891536' then '安佳 全脂纯牛奶250ml*24盒 新西兰进口'
when item_id='20896699903' then '【天猫超市】雀巢成人奶粉 安骼高钙高铁女士成人奶粉400g无蔗糖'
end as item_name,
shop_name,sold,saleprice,qu,ds,root_cat_name,brand_name,cp_flag FROM t_lel_airuisi_8items_withflag


select item_name,shop_name,sold,saleprice,qu,cp_flag,root_cat_name,brand_name,ds from t_lel_airuisi_8items_withflag_marked distribute by item_name,ds sort by item_name,ds

 




iteminfo
shopinfo

2. 品牌/品类/SKU总销量的校验
请您提供以下品牌在对应品类在天猫的的总销量数据，以便于我们进行校验，


2.1  -> ok
摩恩的厨房水槽，16年10、11、12月销量；
create table wl_service.t_lel_airuisi_moen_chufan_shuicao
as
select distinct item_id,shop_id from t_base_ec_record_dev_new where title regexp '厨房水槽' and brand_name regexp '摩恩' and ds = 'true'

create table  wl_service.t_lel_airuisi_moen_3months_sold_and_income_20170324
as		
select 
sum(t2.day_sold) as totalsold,
sum(t2.day_sold_price) as totalincome,t2.month as month
from
(select a.item_id,a.day_sold,a.day_sold_price,substr(a.ds,0,6) as month from wl_base.t_base_ec_item_daysale_dev_new a  where a.item_id 
  in (select item_id from wl_service.t_lel_airuisi_moen_chufan_shuicao) and (cast(a.ds as int) between 20160930  and 20170101) )t2
group by t2.month

2.2   -> !!!
惠氏的婴幼儿配方奶粉，16年10、11、12月销量；

create table wl_service.t_lel_airuisi_huishi
as
select distinct item_id,shop_id from t_base_ec_record_dev_new where title regexp '婴幼儿配方奶粉' and brand_name regexp '惠氏' and ds = 'true'

create table  wl_service.t_lel_airuisi_huishi_3months_sold_and_income_20170324
as		
select 
sum(t2.day_sold) as totalsold,
sum(t2.day_sold_price) as totalincome,t2.month as month
from
(select a.item_id,a.day_sold,a.day_sold_price,substr(a.ds,0,6) as month from wl_base.t_base_ec_item_daysale_dev_new a  where a.item_id 
  in (select item_id from wl_service.t_lel_airuisi_huisi) and (cast(a.ds as int) between 20160930  and 20170101) )t2
group by t2.month


2.3   -> ok
这两个SKU在17年2月的月销量
http://item.taobao.com/item.htm?id=538704657972  -> 	45434
http://item.taobao.com/item.htm?id=520848254413  -> 	15747

create table wl_service.t_lel_airuisi_2sku_20170324
as
select
t1.item_id,
t1.shop_id,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome
from
(select item_id,shop_id,sold,saleprice  from wl_base.t_base_ec_shopitem_b   where ds = '20170228'  and item_id regexp '538704657972|520848254413') t1
join
(select item_id,shop_id,sold,saleprice from wl_base.t_base_ec_shopitem_b  where ds = '20170204' and item_id regexp '538704657972|520848254413') t2
on
t1.item_id = t2.item_id
group by item_id,shop_id





beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM" -S -e "SELECT count(*) FROM wl_base.t_base_ec_item_dev_new where ds='20170325'"