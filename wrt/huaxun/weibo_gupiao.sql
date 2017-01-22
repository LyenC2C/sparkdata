drop table wlservice.t_wrt_huaxun_weibo_gupiaoV;
create table wlservice.t_wrt_huaxun_weibo_gupiaoV as
select idstr,screen_name,description from wlbase_dev.t_base_weibo_user_new
where (verified = 'True' or verified = 'true') and
(length(regexp_replace(screen_name ,'股民|股票|炒股|财经|证券|A股|B股|港股',''))<>length(screen_name)
or length(regexp_replace(description ,'股民|股票|炒股|财经|证券|A股|B股|港股',''))<>length(description));



 rlike  "股民|股票|炒股|财经|证券|A股|B股|港股"
