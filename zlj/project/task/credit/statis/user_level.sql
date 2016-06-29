-- 用户各维度等级

-- SELECT  COUNT(1) FROM t_base_user_info_s_tbuserinfo_t where CAST(buycnt as int)>0


CREATE TABLE t_zlj_credit_user_level_info AS
  SELECT
  u1.*,
  u2.buycnt,
  u2.avg_month_buycnt
FROM
  (SELECT
     user_id,
     times,
     sum_p,
     max_p,
     min_p,
     avg_p,
     CASE WHEN levels > 0 AND levels <= 5
       THEN 'level1'
     WHEN levels > 5 AND levels <= 8
       THEN 'level2'
     WHEN levels > 8 AND levels <= 10
       THEN 'level3'
     WHEN levels > 10 AND levels <= 12
       THEN 'level4'
     WHEN levels > 12
       THEN 'level5'
     WHEN levels <= 0
       THEN 'level0' END AS sum_level,
     CASE WHEN levelm > 0 AND levelm <= 4
       THEN 'level1'
     WHEN levelm > 4 AND levelm <= 7
       THEN 'level2'
     WHEN levelm > 7
       THEN 'level3'
     WHEN levelm <= 0
       THEN 'level0' END AS max_level,
     CASE WHEN levela > 0 AND levela <= 5
       THEN 'level1'
     WHEN levela > 5 AND levela <= 7
       THEN 'level2'
     WHEN levela > 7
       THEN 'level3'
     WHEN levela <= 0
       THEN 'level0' END AS avg_level
   FROM
     (SELECT
        t.*,
        log2(sum_p) levels,
        log2(max_p) levelm,
        log2(avg_p) levela
      FROM t_zlj_credit_user_price_statis t) y) u1
  JOIN
  (SELECT
     tb_id,
     buycnt,
     CAST(
         buycnt /
         (12 * (2016 - YEAR(regexp_replace(regtime, '\\.', '-'))) + (7 - MONTH(regexp_replace(regtime, '\\.', '-'))))
         AS INT) avg_month_buycnt
   FROM
     t_base_user_info_s_tbuserinfo_t
   WHERE buycnt IS NOT NULL
  ) u2
    ON u1.user_id = u2.tb_id ;
