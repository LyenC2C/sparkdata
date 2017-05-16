保险代理

enterprise_verified_or_not:
	verified_type = 2 -> enterprise
	select * from t_base_weibo_user_new where id in(5319331024,1704558113,1955212357)

保险关键词	职位关键词
保险	        职员
人寿         营销员
中国平安      经理（去除总经理）
中国太平洋	理财专家
	        代理
	        理财规划师
	        顾问
	        总监
	        销售
	        商务
	
	
drop table wl_service.t_lel_baoxiandaili_20170503;
create table wl_service.t_lel_baoxiandaili_20170503
as
select id,name,description,verified_reason from t_base_weibo_user_new where 
(name regexp '(保险|人寿|中国平安|中国太平洋).*(职员|营销员|经理|理财专家|代理|理财规划师|顾问|总监|销售|商务)' and name not regexp '总经理' and verified_type <>2)
or
(description regexp '(保险|人寿|中国平安|中国太平洋).*(职员|营销员|经理|理财专家|代理|理财规划师|顾问|总监|销售|商务)' and description not regexp '总经理' and verified_type <>2)
or 
(verified_reason regexp '(保险|人寿|中国平安|中国太平洋).*(职员|营销员|经理|理财专家|代理|理财规划师|顾问|总监|销售|商务)' and verified_reason not regexp '总经理' and verified_type <>2)



create table wl_service.t_lel_baoxiandaili_20170505_res
as
select id,description from t_base_weibo_user_new where 
(name regexp '保险|人寿|中国平安|中国太平洋' and name not regexp '董事长|ceo|CEO|总经理' and verified_type <>2)
or
(description regexp '保险|人寿|中国平安|中国太平洋' and description not regexp '董事长|ceo|CEO|总经理' and verified_type <>2)


create table wl_service.t_lel_baoxiandaili_20170505_res_2000
as
select * from t_lel_baoxiandaili_20170505_res  where description <> ''  order by rand() limit 2000