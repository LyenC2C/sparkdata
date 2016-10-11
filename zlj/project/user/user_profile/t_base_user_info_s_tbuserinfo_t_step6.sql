-- 加入闲鱼地址信息

-- select from_unixtime(unix_timestamp(weibo_created_at ,'EEE MMM dd HH:mm:ss zzz yyyy'),'yyyyMMdd') from t_credit_user_tb_weibo_register  limit 10;
DROP TABLE t_base_user_info_s_tbuserinfo_t_step6;

CREATE TABLE t_base_user_info_s_tbuserinfo_t_step6 AS
  SELECT
    t4.*,
    t3.weiboid         AS weibo_id,
    t3.screen_name     AS weibo_screen_name,
    t3.gender          AS weibo_gender,
    t3.followers_count AS weibo_followers_count,
    t3.friends_count   AS weibo_friends_count,
    t3.statuses_count  AS weibo_statuses_count,
    t3.created_at      AS weibo_created_at,
    t3.location        AS weibo_location,
    t3.verified        AS weibo_verified
  FROM
    (
      SELECT
        weiboid,
        screen_name,
        gender,
        followers_count,
        friends_count,
        statuses_count,
       from_unixtime(unix_timestamp(created_at ,'EEE MMM dd HH:mm:ss zzz yyyy'),'yyyyMMdd') as created_at,
        location,
        verified,
        tb_id
      FROM
        t_base_weibo_user t1 RIGHT JOIN
        (
          SELECT
            t1.id1 AS weiboid,
            t2.id1 AS tb_id
          from
          t_base_uid_tmp t1 JOIN t_base_uid_tmp t2 ON t1.ds='wid' AND t2.ds='ttinfo' AND t1.uid=t2.uid
          group by t1.id1,t2.id1
        ) t2 ON t1.idstr = t2.weiboid
    ) t3
    RIGHT
    JOIN
  t_base_user_info_s_tbuserinfo_t_step5 t4

ON t3.tb_id=t4.tb_id
;





SELECT
count(1)
  from
t_base_weibo_user t1 RIGHT JOIN
        (
          SELECT
            t1.id1 AS weiboid,
            t2.id1 AS tb_id
          from
          t_base_uid_tmp t1 JOIN t_base_uid_tmp t2 ON t1.ds='wid' AND t2.ds='ttinfo' AND t1.uid=t2.uid
          group by t1.id1,t2.id1
        ) t2 ON t1.idstr = t2.weiboid
;















SELECT count(1) from t_base_user_info_s_tbuserinfo_t_step6 where weibo_id is not null;



107589465
SELECT
        count(1)
      FROM
        t_base_weibo_user t1 JOIN
        (
          SELECT
            t1.id1 AS weiboid,
            t2.id1 AS tb_id
          from
          t_base_uid_tmp t1 JOIN t_base_uid_tmp t2 ON t1.ds='wid' AND t2.ds='ttinfo' AND t1.uid=t2.uid
          group by t1.id1,t2.id1
        ) t2 ON t1.idstr = t2.weiboid ;

4.1 yi
SELECT  count(1) from(select 1  from t_base_weibo_user group by id)t ;


-- 202359111
SELECT count(1) from
  ( SELECT 1  from t_base_uid_tmp t1 join t_base_uid_tmp t2 on t1.ds='wid' and t2.ds='ttinfo'  and t1.uid=t2.uid
     group by t1.id1 ,t2.id1)t  ;



107589465
create table t_zlj_tmp_weiboinfo  as

SELECT
        id AS weiboid,
        screen_name,
        gender,
        followers_count,
        friends_count,
        statuses_count,
       from_unixtime(unix_timestamp(created_at ,'EEE MMM dd HH:mm:ss zzz yyyy'),'yyyyMMdd') as created_at,
        location,
        verified,
        tb_id
      FROM
        t_base_weibo_user t1 JOIN
        (
          SELECT
            t1.id1 AS weiboid,
            t2.id1 AS tb_id
          from
          t_base_uid_tmp t1 JOIN t_base_uid_tmp t2 ON t1.ds='wid' AND t2.ds='ttinfo' AND t1.uid=t2.uid
          group by t1.id1,t2.id1
        ) t2 ON t1.idstr = t2.weiboid ;

249008391
select count(1) from (SELECT  uid  from  t_base_uid_tmp where ds='ttinfo' group by uid)t ;

328253366
SELECT  count(1)  from  t_base_uid_tmp where ds='ttinfo' ;


191247689
 select count(1) from (SELECT  uid  from  t_base_uid_tmp where ds='wid' group by uid)t ;