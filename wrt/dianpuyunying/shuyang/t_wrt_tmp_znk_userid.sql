insert overwrite table t_wrt_znk_development_data
select user_id from t_wrt_znk_development_data where ds = '20160919' group by user_id;