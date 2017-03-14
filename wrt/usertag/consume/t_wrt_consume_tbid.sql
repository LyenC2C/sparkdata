drop table t_wrt_consume_tbid_cat_monthall;
create table wl_usertag.t_wrt_consume_tbid_cat_monthall AS
-- select user_id,concat_ws("\t",collect_list(root_cat_name))
select user_id,group_concat("\t",root_cat_name) as cat_top from
(
select
user_id,root_cat_name,
ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ct DESC) AS rn
from
(
select
user_id,root_cat_name,count(1) as ct
from
wl_analysis.t_base_record_cate_simple_ds
group by
user_id,root_cat_name
)t

)tt
where rn <=10
group by user_id
;

drop table t_wrt_consume_tbid_cat_1month;
create table wl_usertag.t_wrt_consume_tbid_cat_1month AS
-- select user_id,concat_ws("\t",collect_list(root_cat_name))
select user_id,group_concat("\t",root_cat_name) as cat_top from
(
select
user_id,root_cat_name,
ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ct DESC) AS rn
from
(
select
user_id,root_cat_name,count(1) as ct
from
wl_analysis.t_base_record_cate_simple_ds
where
substr(regexp_replace(cast (date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*1) as string),'-','' ),1,6) <= ds
and
substr(regexp_replace(cast(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*1) as string),'-','' ),1,8) <dsn
group by
user_id,root_cat_name
)t
)tt
where rn <=10
group by user_id
;


drop table t_wrt_consume_tbid_cat_3month;
create table wl_usertag.t_wrt_consume_tbid_cat_3month AS
-- select user_id,concat_ws("\t",collect_list(root_cat_name))
select user_id,group_concat("\t",root_cat_name) as cat_top from
(
select
user_id,root_cat_name,
ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ct DESC) AS rn
from
(
select
user_id,root_cat_name,count(1) as ct
from
wl_analysis.t_base_record_cate_simple_ds
where
substr(regexp_replace(cast (date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*3) as string),'-','' ),1,6) <= ds
and
substr(regexp_replace(cast(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*3) as string),'-','' ),1,8) <dsn
group by
user_id,root_cat_name
)t
)tt
where rn <=10
group by user_id
;


drop table t_wrt_consume_tbid_cat_6month;
create table wl_usertag.t_wrt_consume_tbid_cat_6month AS
-- select user_id,concat_ws("\t",collect_list(root_cat_name))
select user_id,group_concat("\t",root_cat_name) as cat_top from
(
select
user_id,root_cat_name,
ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ct DESC) AS rn
from
(
select
user_id,root_cat_name,count(1) as ct
from
wl_analysis.t_base_record_cate_simple_ds
where
substr(regexp_replace(cast (date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*6) as string),'-','' ),1,6) <= ds
and
substr(regexp_replace(cast(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*6) as string),'-','' ),1,8) <dsn
group by
user_id,root_cat_name
)t
)tt
where rn <=10
group by user_id
;




drop table t_wrt_consume_tbid_cat_12month;
create table wl_usertag.t_wrt_consume_tbid_cat_12month AS
-- select user_id,concat_ws("\t",collect_list(root_cat_name))
select user_id,group_concat("\t",root_cat_name) as cat_top from
(
select
user_id,root_cat_name,
ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ct DESC) AS rn
from
(
select
user_id,root_cat_name,count(1) as ct
from
wl_analysis.t_base_record_cate_simple_ds
where
substr(regexp_replace(cast (date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*12) as string),'-','' ),1,6) <= ds
and
substr(regexp_replace(cast(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*12) as string),'-','' ),1,8) <dsn
group by
user_id,root_cat_name
)t
)tt
where rn <=10
group by user_id
;