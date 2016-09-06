

-- 加入闲鱼 个人基本信息
-- 加入模型预测性别数据

  Drop table t_base_user_info_s_tbuserinfo_t_step4 ;
  create table t_base_user_info_s_tbuserinfo_t_step4 as
  SELECT
  t1.* ,
  t2.gender as predict_gender

  FROM
  (
  select
  tb_id        ,
  alipay       ,
  buycnt       ,
  verify       ,
  regtime      ,
  tb_nick      ,
  tb_location  ,
  tgender      ,
  tage         ,
  tname        ,
  tloc         ,
  tel_prov     ,
  tel_city     ,
  t3.gender  as xianyu_gender,
  case when t3.birthday like '%19%' then  2016-YEAR(birthday ) else -1 end  as xianyu_birthday ,
  t3.constellation as xianyu_constellation,
  t3.province  as xianyu_province ,
  t3.city  as xianyu_city
  from
  t_base_ec_tb_xianyu_userinfo t3
  RIGHT  join t_base_user_info_s_tbuserinfo_t_step3 t4
  on t3.userid = t4.tb_id and t3.ds=20160721

  )t1 left join t_zlj_model_user_gender t2 on t1.tb_id=t2.id
  ;


