12.12:欣颜需求

商品详情表：
wl_base.t_base_ec_item_dev_new
评论表：
wlbase_dev.t_base_ec_item_feed_dev_new
record表：
wl_base.t_base_ec_record_dev_new
	
1,主推纹绣产品的目标用户群                 

目标数量：150
用户地区：成都
a)近1年购买了3次及以上”眉笔“”眼线笔““眼线液””眉粉“”的用户；
b)用户为微博关注了标签中含“纹绣”的大V粉丝；
c)用户的简介,教育,标签中包含了附件关键字的用户；
优先提取同时满足条件abc的用户群体，若量不足够则依次减少c,b的条件提取进行补足.

输出字段：姓名,性别,电话,购买商品关键词,购买次数；

a -> method -> a.4
b,c -> method -> b&c
 	

a:
 a.1.','final method','

create table if not exists wlservice.t_lel_ec_xinyan_wenxiu_cd_ikbg as 
select t2.id as id,t1.gender as gender,t1.keyword as keyword,cast(t1.buy_times as int) as buy_times from
(select tb_id,xianyu_gender as gender from wlbase_dev.t_base_user_profile_telindex where tb_location regexp '成都') t1
join
(select t.id as id ,concat_ws(',',collect_set(t.keyword)) as keyword,size(collect_list(t.keyword)) as buy_times
from
  (select user_id as id,regexp_extract(title,'眉笔|眼线笔|眼线液|眉粉',0) as keyword
   from wl_base.t_base_ec_record_dev_new 
    where
     (cast(dsn as int) 
      between  cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as int) - 10000 
      and cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as int) )
       and title regexp '眉笔|眼线笔|眼线液|眉粉' 
      ) t group by id) t2
on t1.tb_id = t2.id
where buy_times >= 3 order by buy_times desc


b:
     一行拆分成多行 -> select t.id,fid from t_lel_test t lateral view explode(split(ids,',')) a as fid; 

统计top20 大v用户的粉丝总量
select sum(t.s) from 
(select cast(followers_count as int) as s from
 wl_base.t_base_weibo_user_new 
  where ds=20161123 and name regexp '纹绣' and verified = 'True' order by s desc limit 20) t    


 b.1
select small.id from
(select t.id as id,fid from wl_base.t_base_weibo_user_fri t lateral view explode(split(ids,',')) a as fid where ds=20161106) small
join
(select id from
 wl_base.t_base_weibo_user_new 
  where ds=20161123 and name regexp '纹绣' and verified = 'True') big
on small.fid = big.id

 b.2
select id,cast(followers_count as int) as fans from
 wl_base.t_base_weibo_user_new 
  where ds=20161123 and name regexp '纹绣' and verified = 'True' order by fans desc limit 100;

select id from wl_base.t_base_weibo_user_fri 
where ds=20161106 
and 
ids regexp '1737427071|3740511320|5338673449|2631049313|2812769985|1858297810|5538044128|5087697021|3772281031'

 b.3','final method','

create table if not exists wl_service.t_lel_weibo_xinyan_wenxiu_only_ig as
select t1.id as id,t2.gender as gender from 
(select id from wl_base.t_base_weibo_user_fri 
where ds=20161106 
and 
ids regexp '1737427071|3740511320|2238109202|5338673449|2631049313|2812769985|
            1858297810|5538044128|5087697021|3772281031|2641725961|2271612520|
            5777153233|3500328947|3610720777|5650839702|5640373134|5600624136|3147718552') 
t1
join 
(select id,gender
 from wl_base.t_base_weibo_user_new
  where ds=20161123 ) t2
on t1.id = t2.id

c:
 c.1 ','final method','

select id,gender
 from wl_base.t_base_weibo_user_new
  where ds=20161123 
   and description regexp '戏剧学院|舞蹈学|,电影学院|传媒大学|演员|歌手|模特|话剧|舞蹈|主持|
                           播音|广播学院|广院|艺人|传媒|表演|电视|广告|营销|艺校|主播|网红|外事'


 b&c: ','final method','
 
 select id,cast(followers_count as int) as fans from
 wl_base.t_base_weibo_user_new 
  where ds=20161123 and name regexp '纹绣' and verified = 'True' order by fans desc;

create table if not exists wl_service.t_lel_weibo_xinyan_wenxiu_cd_ig as
select t1.id as id,t2.gender as gender from 
(select id from wl_base.t_base_weibo_user_fri 
where ds=20161106 
and 
ids regexp '1737427071|3740511320|2238109202|5338673449|2631049313|2812769985|
            1858297810|5538044128|5087697021|3772281031|2641725961|2271612520|
            5777153233|3500328947|3610720777|5650839702|5640373134|5600624136|3147718552'
) t1
join 
(select id,gender
 from wl_base.t_base_weibo_user_new
  where ds=20161123  and location regexp '成都'
   and description regexp '戏剧学院|舞蹈学|,电影学院|传媒大学|演员|歌手|模特|话剧|舞蹈|主持|
                           播音|广播学院|广院|艺人|传媒|表演|电视|广告|营销|艺校|主播|网红|外事'
) t2
on t1.id = t2.id



    
2,主推智能美白牙499的目标用户群

目标数量：150
地区：成都
a)近半年购买2次及以上“牙齿 美白”商品的用户；

输出字段：姓名,性别,电话,购买商品关键词,购买次数.

a.1 ','final method','

create table if not exists wlservice.t_lel_ec_xinyan_meibaiya_cd_ig as 
select t2.id as id,t1.gender as gender,'牙齿美白' as keyword,t1.buy_times as buy_times from 
(select tb_id,xianyu_gender as gender from wlbase_dev.t_base_user_profile_telindex where tb_location regexp '成都') t1
join


(select user_id as id ,count(user_id) as buy_times
 from wl_base.t_base_ec_record_dev_new 
 where
 (cast(dsn as int) 
 between  cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as int) - 600 
 and cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as int) )
 and title regexp '牙齿' 
 and title regexp '美白' 
 group by user_id
 ) t2 
on t1.tb_id = t2.id
where buy_times >= 2 order by buy_times desc;






without buy_times:

create table if not exists wlservice.t_lel_ec_xinyan_meibaiya_cd_ikg as 
select t2.id as id,t1.gender as gender,'牙齿美白' as keyword from 
(select tb_id,xianyu_gender as gender from wlbase_dev.t_base_user_profile_telindex where tb_location regexp '成都') t1
join
(select user_id as id
 from wl_base.t_base_ec_record_dev_new 
 where
 (cast(dsn as int) 
 between  cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as int) - 600 
 and cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as int) )
 and title regexp '牙齿' 
 and title regexp '美白' 
 ) t1 
on t1.tb_id = t2.id;









select id,sum(cast(followers_count as int)) from
 wl_base.t_base_weibo_user_new 
  where ds=20161123 and name regexp '纹绣' and verified = 'True' order by fans desc limit 20;
