t_base_weibo_user_fri 
t_base_weibo_user_new 

1.路虎weiboid：2271357522  浙江省

create table wlservice.t_lel_weiboid_zj_2271357522 as
select fri.id from
(select id from wlbase_dev.t_base_weibo_user_fri where ds=20161106 and ids like "%2271357522%") fri 
join 
(select id,location from wlbase_dev.t_base_weibo_user_new where ds=20161123 and split(location,' ')[0]="浙江") new 
on fri.id = new.id;

2.捷豹weiboid：1798775760  浙江省

create table wlservice.t_lel_weiboid_zj_1798775760 as
select fri.id from
(select id from wlbase_dev.t_base_weibo_user_fri where ds=20161106 and ids like "%1798775760%") fri 
join 
(select id,location from wlbase_dev.t_base_weibo_user_new where ds=20161123 and split(location,' ')[0]="浙江") new 
on fri.id = new.id;

3.埃尔法weiboid：1647951825  不限

create table wlservice.t_lel_weiboid_1647951825 as
select fri.id from
(select id from wlbase_dev.t_base_weibo_user_fri where ds=20161106 and ids like "%1647951825%") fri 
join 
(select id from wlbase_dev.t_base_weibo_user_new where ds=20161123) new 
on fri.id = new.id;

4.
select id，case when jiebao.id is null  then "路虎" else "捷豹" end from
(select fri.id,"路虎" from
(select id from wlbase_dev.t_base_weibo_user_fri where ds=20161106 and ids like "%2271357522%") fri 
join 
(select id,location from wlbase_dev.t_base_weibo_user_new where ds=20161123 and split(location,' ')[0]="浙江") new 
on fri.id = new.id) luhu
full join
(select fri.id,"捷豹" from
(select id from wlbase_dev.t_base_weibo_user_fri where ds=20161123 and ids like "%1798775760%") fri 
join 
(select id,location from wlbase_dev.t_base_weibo_user_new where ds=20161123 and split(location,' ')[0]="浙江") new 
on fri.id = new.id) jiebao on luhu.id = jiebao.id;

5.两张表
select case when luhu.id is null then jiebao.id else luhu.id end,case when jiebao.id is null  then "路虎" else "捷豹" end from
wlservice.t_lel_weiboid_zj_2271357522 luhu
full join 
wlservice.t_lel_weiboid_zj_1798775760 jiebao
on luhu.id=jiebao.id
6.三张表

create table wlservice.t_lel_weiboid_luhu_jiebao_aierfa as
select case when luhu_jiebao.id is null then aierfa.id else luhu_jiebao.id end as id,case when luhu_jiebao.id is null then "埃尔法" else luhu_jiebao.name end as name from
(select case when jiebao.id is null then luhu.id else jiebao.id end,case when luhu.id is null then "捷豹" else "路虎" end as name from
wlservice.t_lel_weiboid_zj_2271357522 luhu
full join 
wlservice.t_lel_weiboid_zj_1798775760 jiebao
on luhu.id=jiebao.id) luhu_jiebao
full join
wlservice.t_lel_weiboid_1647951825 aierfa on luhu_jiebao.id = aierfa.id;
