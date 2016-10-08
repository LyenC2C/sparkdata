
SELECT count(1) from t_zlj_credit_valid_api1_step1 ;


create table t_zlj_credit_valid_api1_step1 as
SELECT
  tel,
  qqid,
  real_name,
  weibo_id,
  qqweibo,
  tb_id,
  email,
  tb_score ,
  tb_regyear
FROM
  (

    SELECT
      tel,
      qqid,
      real_name,
      weibo_id,
      qqweibo,
      t1.tb_id,
      email,
      score AS                  tb_score,
      year  AS                  tb_regyear,
      row_number()
      OVER (PARTITION BY t1.tel
        ORDER BY t2.score DESC) rank
    FROM
      (
          SELECT
          uid tel,
          id1 qqid,
          id2 real_name,
          id3 weibo_id,
          id4 qqweibo,
          id5 tb_id,
          id6 email FROM t_base_uid_tmp WHERE ds ='link'
      ) t1

      JOIN
      (
        SELECT
          tb_id,year,
          (CASE WHEN tage IS NULL
            THEN 0
           ELSE 1 END +
           CASE WHEN tgender IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN city IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN alipay IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN year IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN buycnt IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN verify_level IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN ac_score_normal IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN sum_level IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN feedrate IS NULL
             THEN 0
           ELSE 1 END) AS score
from t_pzz_tag_basic_info
      ) t2 ON t1.tb_id = t2.tb_id

  ) t1
WHERE rank = 1 ;




create table t_zlj_credit_valid_api1_step2  as

SELECT  t1.*
,
weibo_created_at  as weibo_reg_time ,
  xianyu_birthday as xianyu_info
from t_zlj_credit_valid_api1_step1  t1 join t_base_user_profile t2 on
  t1.tb_id=t2.tb_id ;


create table wlfinance.t_hx_xinyan_user_tbid as
SELECT tel,tb_id from wlfinance.t_hx_xinyan_user t1 join t_zlj_credit_valid_api1 t2 on t1.phone_no=t2.tel ;
