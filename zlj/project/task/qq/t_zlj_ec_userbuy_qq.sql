USE wlbase_dev;

set hive.exec.reducers.bytes.per.reducer=1000000000;


DROP TABLE  if exists t_zlj_ec_userbuy_qq;

CREATE TABLE t_zlj_ec_userbuy_qq AS
SELECT
t4.user_id,
t3.uin
t3.birthday
t3.phone
t3.gender_id
t3.college
t3.lnick
t3.loc_id
t3.loc
t3.h_loc_id
t3.h_loc
t3.personal
t3.shengxiao
t3.gender
t3.occupation
t3.constel
t3.blood
t3.url
t3.homepage
t3.nick
t3.email
t3.uin2
t3.mobile
t3.ts
t3.age
FROM
  (
    SELECT
      t2.*
      t1.tbuid,
      t1.qq
    FROM
      t_zlj_data_link t1
      JOIN
      t_base_qq_user_dev t2
        ON (LENGTH(t2.uin) > 0 AND LENGTH(t1.qq) > 0 AND t1.qq = t2.uin)
  ) t3 JOIN
  t_zlj_ec_userbuy
  t4
    ON (length(t4.user_id)>0 and length(t3.tbuid)>0 and t3.tbuid= t4.user_id);



