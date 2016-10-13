DROP  table t_zlj_tmp;
CREATE  TABLE  t_zlj_tmp as
select id  from

(
SELECT orgin_id  as id  from t_zlj_dc_weibodata_next_user_name_id
UNION  ALL
SELECT user_id   as id   from t_zlj_dc_weibodata_next_user_name_id
)t group by  id  ;