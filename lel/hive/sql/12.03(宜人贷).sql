2604196653
a)关注微博宜人贷的微博：http://weibo.com/yirendai
b）城市需求：location in (北京,广州,上海,深圳,南京,苏州,无锡,常州,石家庄,大连,济南,青岛,天津,太原,东莞,惠州,武汉,郑州,义乌,荆州,邢台,佛山,西安)

直辖市:北京,上海
c)年龄25-50岁

输出字段：id、年龄、城市

create table wl_service.t_weibo_IAL_info_fansOf_2604196653 as 
select 
new.id as id,info_with_age.age as age,
case when split(location,' ')[0] in ('北京','上海') then split(location,' ')[0] when split(location,' ')[1] in 
('广州','深圳','南京','苏州','无锡','常州','石家庄','大连','济南','青岛','天津','太原','东莞','惠州','武汉','郑州','义乌','荆州','邢台','佛山','西安') 
then split(location,' ')[1] end as location
from
(
select fri.id,user_info.age from
(select id from wl_base.t_base_weibo_user_fri where ds=20161106 and ids like "%2604196653%") fri
join
(select id,(2016 - split(birthday,'-')[0]) as age from  wl_base.t_base_weibo_user_basic where split(birthday,'-')[0] > 1900) user_info
on fri.id=user_info.id 
) info_with_age
join
(select id,location from wl_base.t_base_weibo_user_new where ds=20161123 and split(location,' ')[0] in ('北京','上海') or split(location,' ')[1] in  ('广州','深圳','南京','苏州','无锡','常州','石家庄','大连','济南','青岛','天津','太原','东莞','惠州','武汉','郑州','义乌','荆州','邢台','佛山','西安') ) new
on info_with_age.id=new.id where age between 25 and 50;



select split(city,' ')[0] as province,count(split(city,' ')[0]) as counts  from t_pzz_tag_basic_info group by split(city,' ')[0] ;



create table wl_service.t_weibo_IAL_info as 
select 
new.id as id,user_info.age as age,
case when split(location,' ')[0] in ('北京','上海') then split(location,' ')[0] when split(location,' ')[1] in 
('广州','深圳','南京','苏州','无锡','常州','石家庄','大连','济南','青岛','天津','太原','东莞','惠州','武汉','郑州','义乌','荆州','邢台','佛山','西安') 
then split(location,' ')[1] end as location from
(select id,(2016 - split(birthday,'-')[0]) as age from  wl_base.t_base_weibo_user_basic where split(birthday,'-')[0] > 1900) user_info
join
(select id,location from wl_base.t_base_weibo_user_new where ds=20161123 and split(location,' ')[0] in ('北京','上海') or split(location,' ')[1] in  ('广州','深圳','南京','苏州','无锡','常州','石家庄','大连','济南','青岛','天津','太原','东莞','惠州','武汉','郑州','义乌','荆州','邢台','佛山','西安') ) new
on user_info.id=new.id where age between 25 and 50;


