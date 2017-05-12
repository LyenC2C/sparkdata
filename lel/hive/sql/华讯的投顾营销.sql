华讯的投顾营销
5w
投资理财偏好人群：
1、社交平台关注投资理财人群；
2、电商消费投资理财书籍人群；
3、注册投资理财平台人群；
	
limit_loc:北京、上海、广州、深圳、天津、杭州、苏州、宁波、温州、绍兴、江阴、萧山、东莞、佛山、无锡、常州、南京

1.

create table wl_service.t_lel_huaxun_licai_weibo_idmiddle_20170330
as
select a.id,a.ids,b.`location` from
(select id,ids from wl_base.t_base_weibo_user_fri )a
join
(select id,regexp_extract(`location`,'北京|上海|广州|深圳|天津|杭州|苏州|宁波|温州|绍兴|江阴|萧山|东莞|佛山|无锡|常州|南京',0) as `location` from wl_base.t_base_weibo_user_new where `location` regexp '北京|上海|广州|深圳|天津|杭州|苏州|宁波|温州|绍兴|江阴|萧山|东莞|佛山|无锡|常州|南京' )b
on a.id=b.id


create table wl_service.t_lel_huaxun_licai_weibo_licaiidmiddle_20170330
as
select id from t_base_weibo_user_new where  verified='True' and name regexp '投资|理财'

spark process

t_lel_huaxun_licai_weibo_idloc_20170330

t_lel_huaxun_licai_weibo_phoneid_20170330


t_lel_huaxun_licai_weibo_20170330



	
2.
create table wl_service.t_lel_huaxun_licai_ec_tbidmiddle_20170330
	as
	select a.user_id,b.tel_loc from 
(SELECT distinct(user_id) FROM wl_base.t_base_ec_record_dev_new 
where ds='true' 
and title regexp "投资理财")a
join 
(select tb_id,regexp_extract(tel_loc,'北京|上海|广州|深圳|天津|杭州|苏州|宁波|温州|绍兴|江阴|萧山|东莞|佛山|无锡|常州|南京',0) as tel_loc from wl_base.t_base_user_profile_telindex where tel_loc regexp '北京|上海|广州|深圳|天津|杭州|苏州|宁波|温州|绍兴|江阴|萧山|东莞|佛山|无锡|常州|南京')b
on
a.user_id=b.tb_id

create table wl_service.t_lel_huaxun_licai_ec_20170330
as
select distinct a.phone,b.tel_loc from 
(select phone,tbid from t_lel_huaxun_licai_ec_phonemiddle_20170330)a
join
(select user_id,tel_loc from t_lel_huaxun_licai_ec_tbidmiddle_20170330)b
on a.tbid = b.user_id

3.
create table wl_service.t_lel_huaxun_licai_platform_20170330
as
select distinct a.phone,b.city from
(select phone,substr(phone,1,7) as prefix  from t_base_touzi_licai where flag='True' and ds='20170329')a
join
(select prefix,city from t_base_mobile_loc where city regexp '北京|上海|广州|深圳|天津|杭州|苏州|宁波|温州|绍兴|江阴|萧山|东莞|佛山|无锡|常州|南京')b
on a.prefix=b.prefix



20170329 product: 
create table t_lel_huaxun_licai_ec_20170330_1w 
as
select * from t_lel_huaxun_licai_ec_20170330 order by rand()  where tel_loc <> '' limit 10000

create table t_lel_huaxun_licai_platform_20170330_4w
as
select * from
(select * from t_lel_huaxun_licai_platform_20170330  where city <> '' order by rand())a where a.phone not in (select phone from t_lel_huaxun_licai_ec_20170330_1w) limit 40000

--------------------------
20170405 product:

awk -F '\001' 'NR==FNR{a[$1]=$1}NR!=FNR{if(!($1 in a)) print $0}' ../given ../ec |sort -R|head -10000 > huaxun_ec_1w.txt
awk -F '\001' '{print $1}' huaxun_ec_1w.txt >> ../given
awk -F '\001' 'NR==FNR{a[$1]=$1}NR!=FNR{if(!($1 in a)) print $0}' ../given ../platform|head -20000 > huaxun_platform_2w.txt
awk -F '\001' '{print $1}' huaxun_platform_2w.txt >> ../given
awk -F '\001' 'NR==FNR{a[$1]=$1}NR!=FNR{if(!($1 in a)) print $0}' ../given ../weibo|head -20000 > huaxun_weibo_2w.txt
awk -F '\001' '{print $1}' huaxun_weibo_2w.txt >> ../given
cat huaxun_* | awk -F '\001' '{print $1"\t"$2}' > 投顾_20170405
scp lel@cs105:/home/lel/work/huaxun/20170405/投顾_20170405 /home/lyen/下载/huaxun/
--------------------------
20170410 product:

投顾_20170410

--------------------------

20170417
投顾_20170417
scp lel@cs105:/home/lel/work/huaxun/20170417/投顾_20170417 /home/lyen/下载/huaxun/
--------------------------
20170424
投顾_20170424
scp lel@cs105:/home/lel/work/huaxun/20170424/投顾_20170424 /home/lyen/下载/huaxun/


20170502
投顾_20170502
scp lel@cs105:/home/lel/work/huaxun/20170502/投顾_20170502 /home/lyen/下载/huaxun/

20170508
投顾_20170508
scp lel@cs105:/home/lel/work/huaxun/20170508/投顾_20170508 /home/lyen/下载/huaxun/