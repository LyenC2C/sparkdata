1、主推产品：学生贷产品U族
提取数量：5000
限制年龄段：17-22岁
用户关注了微博 
http://weibo.com/572711557       ->3823605032
http://weibo.com/u/3886194772    ->3886194772
（该部分请老彭协调输出）
输出字段：姓名、电话、年龄

	create table if not exists wl_service.t_lel_weibo_ia_fansOf_daikuan as
	select fri.id as id,user_info.age as age
	from
	(select id
	from
	wl_base.t_base_weibo_user_fri
	where
	ds=20161106 and ids regexp '3823605032|3886194772|2597729662|2548607973|3272375467|
	                            5043166698|3871940355|3491750424|5711430440|2960313250|
	                            5269747892|3246077324|3101293107|5835623389|2866475362|
	                            2951123497|1822476447|2128058797'
	)
	fri
	join
	(select id,(2016 - split(birthday,'-')[0]) as age
	from  wl_base.t_base_weibo_user_basic
	where split(birthday,'-')[0] > 1900) user_info 
	on
	fri.id = user_info.id where age >=17 and age <= 22 ;


	create table if not exists wl_service.t_lel_weibo_ia_xueshengdai_2 as
	select fri.id as id,user_info.age as age
	from
	(select id
	from
	wl_base.t_base_weibo_user_fri
	where
	ds=20161106 and ids regexp '3823605032|3886194772' 
	) fri
	join
	(select id,(2016 - split(birthday,'-')[0]) as age
	from  wl_base.t_base_weibo_user_basic
	where split(birthday,'-')[0] > 1900) user_info 
	on
	fri.id = user_info.id;

	
    create table if not exists wl_service.t_lel_weibo_ia_xueshengdai_2 as
    select t1.id,t2.age from 
    (select id
	from
	wl_base.t_base_weibo_user_fri
	where
	ds=20161106 and  ids regexp '3823605032|3886194772') 
	t1
    join 
	(select id,
	case when birthday is null then 0 
	when 17<= (2016 - split(birthday,'-')[0]) <=22 and split(birthday,'-')[0] > 1990 
	then (2016 - split(birthday,'-')[0]) else 0 end as age
	from  wl_base.t_base_weibo_user_basic)
	t2
	on t1.id = t2.id
	order by t2.age desc;


	create table if not exists wl_service.t_lel_weibo_ia_xueshengdai_3 
		as 
		select case when t1.id is null then t2.id else t1.id end as id,case when t2.age is null then 0 else t2.age end as age
		  from 
		   (select id from wl_base.t_base_weibo_user_fri
		    where ds=20161106 and ids regexp '3823605032|3886194772')
		   t1 
		   full join 
		   (select id,age from wl_service.t_lel_weibo_ia_fansof_xueshengdai) t2 
		   on t1.id = t2.id order by age desc; 



select id,cast(followers_count as int ) from wl_base.t_base_weibo_user_new where ds=20161123 and name regexp '贷款' order by cast(followers_count as int)desc;
select id,cast(followers_count as int ) from wl_base.t_base_weibo_user_new where ds=20161123 and name regexp '学生贷' order by cast(followers_count as int)desc;
贷款  top 20

1	2548607973	948043
2	3272375467	819240
3	3823605032	480880
4	5711430440	479557
5	2960313250	303296
6	5269747892	259664
7	5043166698	237895
8	3246077324	198013
9	3101293107	148714
10	2970891212	137997
11	2440011407	135334
12	3871940355	133054
13	5835623389	103069
14	3491750424	102042
15	3886194772	101157
16	2866475362	100662
17	2951123497	75733
18	2597729662	75304
19	1822476447	57252
20	2128058797	53975

学生贷 top 10

1	2548607973	948043
2	3272375467	819240
3	3823605032	480880
4	5711430440	479557
5	5043166698	237895
6	2970891212	137997
7	2440011407	135334
8	3871940355	133054
9	5835623389	103069
10	3491750424	102042




2、主推产品：成人贷产品手机贷	
提取数量：5000
关注微博：宜人贷http://weibo.com/yirendai  ->2604196653 
微博粉丝同时为多平台借贷用户.
(微博粉丝号码请老彭协调输出到郭处，该部分结果请郭协调输出)

输出字段：姓名、电话、借贷平台名称

create table if not exists wl_service.t_lel_weibo_fansOf_2604196653 as
select id
from
wl_base.t_base_weibo_user_fri
where
ds=20161106 and ids regexp "2604196653";


