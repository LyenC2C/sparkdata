-- 加入微博教育以及公司职位信息

DROP TABLE t_base_user_info_s_tbuserinfo_t_step7;

CREATE TABLE t_base_user_info_s_tbuserinfo_t_step7 AS
  SELECT
  t1.* ,
  t2.weibo_colleges,
  t3.weibo_company
  from
  t_base_user_info_s_tbuserinfo_t_step6 t1

 LEFT join
 (
 SELECT id as weibo_id ,concat_ws('|',collect_set(college )) weibo_colleges from t_base_weiboid_college group by id
  ) t2 on t1.weibo_id=t2.weibo_id
LEFT join
 (
 SELECT id as weibo_id ,concat_ws('|',collect_set(concat_ws('_', company,department ) )) as weibo_company  from
 t_base_weiboid_career group by id
  ) t3 on t1.weibo_id=t3.weibo_id
 ;



