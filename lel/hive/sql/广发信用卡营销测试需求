1、城市覆盖（有广发网点覆盖的城市）：
广州|深圳|珠海|东莞|汕头|江门|佛山|潮州|惠州|梅州|清远|韶关|阳江|湛江|中山|肇庆|河源|揭阳|茂名|北京|郑州|武汉|上海|昆明|杭州|南京|沈阳|大连|宁波|温州|大庆|新乡|安阳|无锡|玉溪|曲靖|长沙|天津|苏州|
黄石|鞍山|台州|哈尔滨|义乌|营口|丹东|盘锦|本溪|葫芦岛|平顶山|焦作|三门峡|南阳|红河哈尼族彝族自治州|鄂州|宜昌|衡阳|常德|株洲|湘潭|邵阳|绍兴|嘉兴|南通|常州|镇江|淮安|扬州|云浮|牡丹江|齐齐哈尔|济南|济宁|
乌鲁木齐|喀什|成都|福州

2、直接提取确定的有他行信用卡用户 400个 ---辛成提供给恩炼；

select * from t_lel_guangfa_credit_customers_20170224 limit 400 
download as csv 
transfrom to txt 
rename to guangfa_needs_credit_400




3、提取近三个月消费汽车周边商品的用户300个（定向推荐车油主题信用卡）--恩炼提供；
  create table wlservice.t_lel_purchase_car_around_userid_l3m
 as  
 select distinct(t2.user_id) from
 (select cate_level1_id from wlbase_dev.t_base_ec_dim where  commodity_name like '%汽车%' or industry_360_name like '%汽车%' and  cate_level1_id regexp '26|50024971|124354002|124470006')t1
 join 
 (select root_cat_id,user_id from wl_base.t_base_ec_record_dev_new where  ds='true' and (cast(dsn as int) between 20161223 and 20170223))t2
 on t1.cate_level1_id = t2.root_cat_id

 select * from wlservice.t_lel_purchase_car_around_userid_l3m limit 1000 
 hadoop fs -get /trans/lel/car_around_1000_tbid20170224.teltb
 awk -F '\001' '{print $1}' car_around_1000_tbid20170224.teltb | uniq > extractfrom_ec_record_car_around
 cat guangfa_needs_credit_400 wph_wb_phone_300_20170224 > mq

 distinct to others:  
 notice: phone,tb_id (possibly one tb_id 2 more than one phone)
 awk 'NR==FNR{a[$0]=$0}NR!=FNR{if(!($0 in a ))print $0}' mq extractfrom_ec_record_car_around > extractfrom_ec_record_car_around_distincted

 head -300 extractfrom_ec_record_car_around_distincted > car_around_300_20170224
 




4、提取微博关注唯品会的女性粉丝300个（定向推荐广发唯品会信用卡）--恩炼提供；  weiboif->1589698103

set hive.merge.mapredfiles=true;
create table wl_service.t_lel_guangfa_marketing_testneeds_igf_20170224
as
select t1.id as id,t2.gender as gender,t2.fans as fc
(select id 
from wl_base.t_base_weibo_user_fri 
where ds =20161106 and ids regexp '1589698103') t1
join 
(select id,gender,cast(followers_count as int) as fans
from wl_base.t_base_weibo_user_new
where ds=20161123 and gender = 'f' and location regexp '广州|深圳|珠海|东莞|汕头|江门|佛山|潮州|惠州|梅州|清远|韶关|阳江|湛江|中山|肇庆|河源|揭阳|茂名|北京|郑州|武汉|上海|昆明|杭州|南京|沈阳|大连|宁波|温州|大庆|新乡|安阳|无锡|玉溪|曲靖|长沙|天津|苏州|黄石|鞍山|台州|哈尔滨|义乌|营口|丹东|盘锦|本溪|葫芦岛|平顶山|焦作|三门峡|南阳|红河哈尼族彝族自治州|鄂州|宜昌|衡阳|常德|株洲|湘潭|邵阳|绍兴|嘉兴|南通|常州|镇江|淮安|扬州|云浮|牡丹江|齐齐哈尔|济南|济宁|乌鲁木齐|喀什|成都|福州')t2
on t1.id =t2.id
order by t2.fans desc 

awk -F '\001' '{print $1}' t_lel_guangfa_marketing_testneeds_igf_20170224.telwb | uniq > extractfrom_wph_wb
head -300 extractfrom_wph_wb > wph_wb_phone_300_20170224

最后名单分别不要有重复数据，由恩炼进行统一输出. 在本周内输出结果谢谢配合！



