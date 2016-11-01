-- 加入微博教育以及公司职位信息

DROP TABLE t_base_user_info_s_tbuserinfo_t_step8;

CREATE TABLE t_base_user_info_s_tbuserinfo_t_step8 AS
  SELECT
  t1.* ,
  t2.find_schools as qq_find_schools
  from
  t_base_user_info_s_tbuserinfo_t_step7 t1

 LEFT join
 (
 SELECT  id1 as tb_id , find_schools
 from t_zlj_qq_find_qq_school_rst t1 join
 t_base_uid_tmp t2 on t1.qq_id =t2.uid and t2.ds='qtb'
 )t2  on t1.tb_id=t2.tb_id
 ;


--
-- SELECT  COUNT(1)
--  from t_zlj_qq_find_qq_school_rst t1 join
--  t_base_uid_tmp t2 on t1.qq_id =t2.uid ;