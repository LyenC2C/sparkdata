wlbase_dev
t_base_weibo_usertag

create table wc as
select w.word as word,count(1) as count from
 (select explode(split(tags,' ')) as word  from wlbase_dev.t_base_weibo_usertag where ds = 20161115) w
  group by word order by word;

create table wc_2 as
select w.word as word,count(1) as count from
 (select explode(split(id,' ')) as word from wc_text) w
  group by word order by count desc ;

韩饭粉丝营销需求 

http://weibo.com/yinyuetaikoreamusic  ->1830442653
http://weibo.com/fengwotiyu           ->2619577243
的成都地区粉丝200名即可.

输出字段  号码  性别  


create table if not exists wl_service.t_lel_weibo_id_hanfan as
select t1.id as id ,t2.gender as gender from 
(select id
	from
	wl_base.t_base_weibo_user_fri
	where
	ds=20161106 and  ids regexp '1830442653|2619577243') t1
join
(select id,gender,cast(followers_count as int) as fans
 from wl_base.t_base_weibo_user_new
  where ds=20161123 and split(location,' ')[1]=成都 order by fans desc) t2
on t1.id=t2.id
