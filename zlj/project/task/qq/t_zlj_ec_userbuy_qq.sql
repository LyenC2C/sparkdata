USE wlbase_dev;

set hive.exec.reducers.bytes.per.reducer=1000000000;


DROP TABLE  if exists t_zlj_ec_userbuy_qq;

CREATE TABLE t_zlj_ec_userbuy_qq AS
SELECT
  t3.*,
  t4.*
FROM
  (
    SELECT
      t2.birthday,
      t2.gender_id,
      t2.loc,
      t2.shengxiao,
      t2.constel,
      t1.tbuid,
      t1.qq
    FROM
      t_zlj_data_link t1
      JOIN
      t_base_q_user_dev t2
        ON (LENGTH(t2.uin) > 0 AND LENGTH(t1.qq) > 0 AND t1.qq = t2.uin)
  ) t3 JOIN
  t_zlj_ec_userbuy
  t4
    ON (length(t4.user_id)>0 and length(t3.tbuid)>0 and t3.tbuid= t4.user_id);



