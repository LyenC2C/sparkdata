use wlservice;
create table t_wrt_znk_upgrad_record
(
item_id string,
dsn string
);
--专门用于给采集提供t_wrt_znk_record已有的item_id和最新评论时间

insert overwrite table t_wrt_znk_upgrad_record
select item_id,max(dsn) from t_wrt_znk_record where ds = 20160829 group by item_id