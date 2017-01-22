create table wlservice.t_wrt_weibo_1yi8_text_qingxi
(
user_id string,
text string
);

load data inpath '/user/wrt/temp/weibo_text_1.8yi_qingxi' overwrite into table wlservice.t_wrt_weibo_1yi8_text_qingxi;

drop table wlservice.t_wrt_weibo_1yi8_text_concat;
create table wlservice.t_wrt_weibo_1yi8_text_concat as
select user_id,concat_ws("\t",collect_set(text)) as text
from wlservice.t_wrt_weibo_1yi8_text_qingxi
group by user_id;

select max(len(text)) from wlservice.t_wrt_weibo_1yi8_text_qingxi



-- t_base_weibo_career
-- t_base_weibo_user_new
-- t_base_weibo_usertag
create table wlservice.t_wrt_tel_weiboid_20161129
(
tel string,
weiboid string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'   LINES TERMINATED BY '\n';


select ttt1.*,ttt2.tags2 from
(
select tt1.tel,tt1.weiboid,tt1.screen_name,tt1.description,tt2.company,tt2.department
FROM
(select t1.tel,t1.weiboid,t2.screen_name,t2.description from
wlservice.t_wrt_tel_weiboid_20161129 t1
left JOIN
t_base_weibo_user_new t2
ON
t1.weiboid = t2.idstr)tt1
left JOIN
(select * from t_base_weibo_career where ds = 20161115)tt2
ON
tt1.weiboid = cast(tt2.id as string)
)ttt1
left JOIN
(select * from t_base_weibo_usertag where ds = 20161115)ttt2
ON
ttt1.weiboid = cast(ttt2.id as string);
