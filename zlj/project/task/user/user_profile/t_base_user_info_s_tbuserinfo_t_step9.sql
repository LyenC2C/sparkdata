-- 加入微博教育以及公司职位信息

DROP TABLE t_base_user_info_s_tbuserinfo_t_step9;

CREATE TABLE t_base_user_info_s_tbuserinfo_t_step9 AS
  SELECT
  t1.* ,
  58_tel ,58_nickname
  from
  t_base_user_info_s_tbuserinfo_t_step8 t1

 LEFT join
 (
 SELECT  decrypted_tel as 58_tel, concat_ws('|', collect_set(nickname)) AS  58_nickname ,t2.tb as tb_id
 from  t_base_credit_58_userinfo   t1
  join
 wlrefer.t_zlj_uid_name t2 on t1.decrypted_tel rlike   '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}' AND
  LENGTH(t1.decrypted_tel)=11  and t1.decrypted_tel =t2.tel
  group by decrypted_tel ,t2.tb
 )t2  on t1.tb_id=t2.tb_id
 ;


--
-- SELECT  COUNT(1)
--  from t_zlj_qq_find_qq_school_rst t1 join
--  t_base_uid_tmp t2 on t1.qq_id =t2.uid ;


-- select num ,COUNT(1) freq
--  from
-- (
-- SELECT uid ,count(1) as num  from t_base_uid_tmp where  ds='ttinfo'  and
--   uid rlike   '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}'
--   group by uid
--   )t group by num
--
--
--
--  SELECT uid ,count(1) as num  from t_base_uid_tmp where  ds='ttinfo'  and
--   uid rlike   '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}'
--   group by uid HAVING  COUNT(1)>100 ;