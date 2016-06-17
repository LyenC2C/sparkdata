-- 打分排序

DROP TABLE IF EXISTS t_zlj_shop_desc_score_rank_v4;
CREATE TABLE t_zlj_shop_desc_score_rank_v4 AS

  SELECT
    t_origin.*,
    (t_origin.desc_score - t_normal.smin) / t_normal.len AS nor_desc_s

  FROM
    (
      SELECT
        main_cat_name,
        max(desc_score) - min(desc_score) AS len,
        min(desc_score)                   AS smin
      FROM
        (
          SELECT
            t1.shop_id,
            CASE WHEN main_cat_name IS NULL
              THEN '-'
            ELSE main_cat_name END AS main_cat_name,
            t1.shop_name,
            desc_score
          FROM
            (
              SELECT
                shop_id,
                shop_name,
                log2((12 * (2016 - YEAR(starts)) + MONTH(starts)))*(
                  5 * desc_highgap / 100.0 + 2 * service_highgap / 100.0 + 1 * wuliu_highgap / 100.0
                +5*desc_score/5.0+5*service_score/5.0+5*wuliu_score/5.0 ) AS desc_score
              FROM
                t_base_ec_shop_dev_new
              WHERE ds = 20160613 AND desc_highgap < 100 AND service_highgap < 100 AND wuliu_highgap < 100
            ) t1 LEFT JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id

        ) tn
      GROUP BY main_cat_name

    ) t_normal
    JOIN

    (
      SELECT
        t1.shop_id,
        CASE WHEN main_cat_name IS NULL
          THEN '-'
        ELSE main_cat_name END     AS main_cat_name,
        t1.shop_name,
        bc_type,
        desc_score,
        ROW_NUMBER()
        OVER (PARTITION BY main_cat_name
          ORDER BY desc_score ASC) AS rn
      FROM
        (
          SELECT
            shop_id,
            shop_name,
            bc_type,
           log2((12 * (2016 - YEAR(starts)) + MONTH(starts)))*(
                  5 * desc_highgap / 100.0 + 2 * service_highgap / 100.0 + 1 * wuliu_highgap / 100.0
                +5*desc_score/5.0+5*service_score/5.0+5*wuliu_score/5.0 ) AS desc_score
          FROM
            t_base_ec_shop_dev_new
          WHERE ds = 20160613 AND desc_highgap < 100 AND service_highgap < 100 AND wuliu_highgap < 100
        ) t1 LEFT JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id

    ) t_origin
      ON t_normal.main_cat_name = t_origin.main_cat_name;


-- 成长性v3

DROP TABLE IF EXISTS t_zlj_shop_grow_rank_v4;

CREATE TABLE t_zlj_shop_grow_rank_v4 AS
  SELECT
    t_origin.*,
    (t_origin.growing_score - t_normal.smin) / t_normal.len                       AS nor_growing_s,
    (t_origin.credit - t_normal.credit_smin) / t_normal.credit_len                AS nor_credit_s,
    (t_origin.total_month - t_normal.total_month_smin) / t_normal.total_month_len AS nor_total_month_s


  FROM
    (
      SELECT

        main_cat_name,
        max(growing_score) - min(growing_score) AS len,
        min(growing_score)                      AS smin,
        max(credit) - min(credit)               AS credit_len,
        min(credit)                             AS credit_smin,
        max(total_month) - min(total_month)     AS total_month_len,
        min(total_month)                        AS total_month_smin
      FROM

        (
          SELECT
            t1.shop_id,
            CASE WHEN main_cat_name IS NULL
              THEN '-'
            ELSE main_cat_name END AS main_cat_name,
            t1.shop_name,
            credit,
            total_month,
            growing_score
          FROM
            (
              SELECT
                shop_id,
                shop_name,
                log2((12 * (2016 - YEAR(starts)) + MONTH(starts)))*credit * 100                                 AS credit,
                (12 * (2016 - YEAR(starts)) + MONTH(starts)) AS total_month,
                log2((12 * (2016 - YEAR(starts)) + MONTH(starts))) * credit * 100.0 /
                (12 * (2016 - YEAR(starts)) + MONTH(starts)) AS growing_score
              FROM
                t_base_ec_shop_dev_new
              WHERE ds = 20160613 AND desc_highgap < 100 AND service_highgap < 100 AND wuliu_highgap < 100
            ) t1 LEFT JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id
        ) tn
      GROUP BY main_cat_name

    ) t_normal
    JOIN

    (
      SELECT
        t1.shop_id,
        CASE WHEN main_cat_name IS NULL
          THEN '-'
        ELSE main_cat_name END        AS main_cat_name,
        t1.shop_name,
        credit,
        total_month,
        growing_score,
        ROW_NUMBER()
        OVER (PARTITION BY main_cat_name
          ORDER BY growing_score ASC) AS rn
      FROM
        (
          SELECT
            shop_id,
            shop_name,
            log2((12 * (2016 - YEAR(starts)) + MONTH(starts)))*credit * 100                                 AS credit,
            (12 * (2016 - YEAR(starts)) + MONTH(starts)) AS total_month,
            log2((12 * (2016 - YEAR(starts)) + MONTH(starts))) * credit * 100.0 /
            (12 * (2016 - YEAR(starts)) + MONTH(starts)) AS growing_score
          FROM
            t_base_ec_shop_dev_new
          WHERE ds = 20160613 AND desc_highgap < 100 AND service_highgap < 100 AND wuliu_highgap < 100
        ) t1 LEFT JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id

    ) t_origin
      ON t_normal.main_cat_name = t_origin.main_cat_name;


-- 销量排序v3
DROP TABLE IF EXISTS t_zlj_shop_sold_num_rank_v4;

CREATE TABLE t_zlj_shop_sold_num_rank_v4 AS

  SELECT

    t_origin.*,
    (t_origin.tsnu - t_normal.smin) / t_normal.len AS nor_sold_score

  FROM
    (
      SELECT

        main_cat_name,
        max(tsnu) - min(tsnu) AS len,
        min(tsnu)             AS smin
      FROM

        (
          SELECT
            t1.shop_id,
            main_cat_name,
            t1.shop_name,
            tsnu,
            ROW_NUMBER()
            OVER (PARTITION BY main_cat_name
              ORDER BY tsnu ASC) AS rn
          FROM
            wlservice.t_zlj_shop_anay_statis_info_all
            t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id
        ) tn
      GROUP BY main_cat_name

    ) t_normal
    JOIN

    (
      SELECT
        t1.shop_id,
        main_cat_name,
        t1.shop_name,
        tsnu,
        ROW_NUMBER()
        OVER (PARTITION BY main_cat_name
          ORDER BY tsnu ASC) AS rn
      FROM
        wlservice.t_zlj_shop_anay_statis_info_all
        t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id

    ) t_origin
      ON t_normal.main_cat_name = t_origin.main_cat_name;


-- 销售额排序v3
DROP TABLE IF EXISTS t_zlj_shop_sold_price_rank_v4;

CREATE TABLE t_zlj_shop_sold_price_rank_v4 AS

  SELECT

    t_origin.*,
    (t_origin.tsmo - t_normal.smin) / t_normal.len AS nor_sold_price_score

  FROM
    (
      SELECT

        main_cat_name,
        max(tsmo) - min(tsmo) AS len,
        min(tsmo)             AS smin
      FROM

        (
          SELECT
            t1.shop_id,
            main_cat_name,
            t1.shop_name,
            tsmo,
            ROW_NUMBER()
            OVER (PARTITION BY main_cat_name
              ORDER BY tsmo ASC) AS rn
          FROM
            wlservice.t_zlj_shop_anay_statis_info_all
            t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id
        ) tn
      GROUP BY main_cat_name

    ) t_normal
    JOIN

    (
      SELECT
        t1.shop_id,
        main_cat_name,
        t1.shop_name,
        tsmo,
        ROW_NUMBER()
        OVER (PARTITION BY main_cat_name
          ORDER BY tsmo ASC) AS rn
      FROM
        wlservice.t_zlj_shop_anay_statis_info_all
        t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id

    ) t_origin
      ON t_normal.main_cat_name = t_origin.main_cat_name;


-- 库存排序v3
DROP TABLE IF EXISTS t_zlj_shop_quant_rank_v4;

CREATE TABLE t_zlj_shop_quant_rank_v4 AS

  SELECT

    t_origin.*,
    (t_origin.trenu - t_normal.smin) / t_normal.len AS nor_quant_score

  FROM
    (
      SELECT

        main_cat_name,
        max(trenu) - min(trenu) AS len,
        min(trenu)              AS smin
      FROM

        (
          SELECT
            t1.shop_id,
            main_cat_name,
            t1.shop_name,
            trenu,
            ROW_NUMBER()
            OVER (PARTITION BY main_cat_name
              ORDER BY trenu ASC) AS rn
          FROM
            wlservice.t_zlj_shop_anay_statis_info_all
            t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id
        ) tn
      GROUP BY main_cat_name

    ) t_normal
    JOIN

    (
      SELECT
        t1.shop_id,
        main_cat_name,
        t1.shop_name,
        trenu,
        ROW_NUMBER()
        OVER (PARTITION BY main_cat_name
          ORDER BY trenu ASC) AS rn
      FROM
        wlservice.t_zlj_shop_anay_statis_info_all
        t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id

    ) t_origin
      ON t_normal.main_cat_name = t_origin.main_cat_name;


DROP TABLE IF EXISTS t_zlj_shop_itemnum_rank_v4;
CREATE TABLE t_zlj_shop_itemnum_rank_v4 AS
  SELECT
    t_origin.*,
    (t_origin.item_num - t_normal.smin) / t_normal.len AS nor_item_score
  FROM
    (
      SELECT
        main_cat_name,
        max(item_num) - min(item_num) AS len,
        min(item_num)                 AS smin
      FROM
        (
          SELECT
            t1.shop_id,
            main_cat_name,

            item_num,
            ROW_NUMBER()
            OVER (PARTITION BY main_cat_name
              ORDER BY item_num ASC) AS rn
          FROM
            (
              SELECT
                shop_id,
                count(1) AS item_num
              FROM
                t_base_ec_shop_dev_new
              WHERE ds = 20160613 AND desc_highgap < 100 AND service_highgap < 100 AND wuliu_highgap < 100
              GROUP BY shop_id
            )
            t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id
        ) tn
      GROUP BY main_cat_name

    ) t_normal
    JOIN
    (
      SELECT
        t1.shop_id,
        main_cat_name,
        item_num,
        ROW_NUMBER()
        OVER (PARTITION BY main_cat_name
          ORDER BY item_num ASC) AS rn
      FROM
        (
          SELECT
            shop_id,
            count(1) AS item_num
          FROM
            t_base_ec_shop_dev_new
          WHERE ds = 20160613 AND desc_highgap < 100 AND service_highgap < 100 AND wuliu_highgap < 100
          GROUP BY shop_id
        )
        t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id

    ) t_origin
      ON t_normal.main_cat_name = t_origin.main_cat_name;


DROP TABLE IF EXISTS t_zlj_shop_result_rank_v5;
CREATE TABLE t_zlj_shop_result_rank_v5 AS
  SELECT
    *,
    ROW_NUMBER()
    OVER (PARTITION BY main_cat_name
      ORDER BY sum_score DESC) AS rn
  FROM


    (
      SELECT
        *,
        nor_desc_s * 4 + nor_sold_score * 2 + nor_sold_price_score * 3 + nor_quant_score * 1 + nor_growing_s * 3 +
        nor_credit_s * 3 + nor_total_month_s * 0.5+nor_item_score
          AS sum_score
      FROM
        (
          SELECT
            t1.shop_id,
            t1.main_cat_name,
            t1.shop_name,
            desc_score,
            nor_desc_s,
            tsnu,
            CASE WHEN nor_sold_score IS NULL
              THEN 0.0
            ELSE nor_sold_score END       nor_sold_score,
            tsmo,
            CASE WHEN nor_sold_price_score IS NULL
              THEN 0.0
            ELSE nor_sold_price_score END nor_sold_price_score,
            trenu,
            CASE WHEN nor_quant_score IS NULL
              THEN 0.0
            ELSE nor_quant_score END      nor_quant_score,
            growing_score,
            nor_growing_s,
            credit,
            nor_credit_s,
            total_month,
            nor_total_month_s,
            CASE WHEN  t6.nor_item_score IS NULL
              THEN 0.0
            ELSE  t6.nor_item_score END         nor_item_score

          FROM
            (SELECT *
             FROM t_zlj_shop_desc_score_rank_v4) t1

            LEFT JOIN t_zlj_shop_sold_num_rank_v4 t2 ON t1.shop_id = t2.shop_id
            LEFT JOIN t_zlj_shop_sold_price_rank_v4 t3 ON t1.shop_id = t3.shop_id
            LEFT JOIN t_zlj_shop_quant_rank_v4 t4 ON t1.shop_id = t4.shop_id
            LEFT JOIN t_zlj_shop_grow_rank_v4 t5 ON t1.shop_id = t5.shop_id
            LEFT JOIN t_zlj_shop_itemnum_rank_v4 t6 ON t1.shop_id = t6.shop_id

        ) t

    ) t1;


DROP TABLE IF EXISTS t_zlj_shop_result_rank_v3;
CREATE TABLE t_zlj_shop_result_rank_v3 AS
  SELECT
    t1.*,
    ROW_NUMBER()
    OVER (PARTITION BY main_cat_name
      ORDER BY sum_score DESC) AS rn1
  FROM
    t_zlj_shop_result_rank_v5
    t1
    JOIN
    (SELECT shop_id
     FROM
       t_base_ec_shop_dev_new
     WHERE ds = 20160613 AND LOCATION LIKE '%四川%'
           AND desc_highgap < 100 AND service_highgap < 100 AND wuliu_highgap < 100
    )

    t2 ON t1.shop_id = t2.shop_id;


DROP TABLE IF EXISTS t_zlj_shop_result_rank_v3_zj;
CREATE TABLE t_zlj_shop_result_rank_v3_zj AS
  SELECT
    t1.*,
    ROW_NUMBER()
    OVER (PARTITION BY main_cat_name
      ORDER BY sum_score DESC) AS rn1
  FROM
    t_zlj_shop_result_rank_v5
    t1
    JOIN
    (SELECT shop_id
     FROM
       t_base_ec_shop_dev_new
     WHERE ds = 20160613 AND LOCATION LIKE '%浙江%'
           AND desc_highgap < 100 AND service_highgap < 100 AND wuliu_highgap < 100
    )

    t2 ON t1.shop_id = t2.shop_id;



DROP TABLE IF EXISTS t_zlj_shop_result_rank_v3_gz;
CREATE TABLE t_zlj_shop_result_rank_v3_gz
AS
  SELECT
    t1.*,
    ROW_NUMBER()
    OVER (PARTITION BY main_cat_name
      ORDER BY sum_score DESC) AS rn1
  FROM
    t_zlj_shop_result_rank_v5
    t1
    JOIN
    (
      SELECT shop_id
     FROM
       t_base_ec_shop_dev_new
     WHERE ds = 20160613 AND LOCATION LIKE '%广州%'
           AND desc_highgap < 100 AND service_highgap < 100 AND wuliu_highgap < 100
    )
    t2 ON t1.shop_id = t2.shop_id;

	SELECT * FROM         t_base_ec_shop_dev_new      WHERE ds = 20160613 and shop_name='百分之一／城市轻文艺'
-- SELECT
--   user_id,
--   root_cat_id,
--   cate_level2_id,
--   price
-- FROM t_zlj_ec_userbuy t1 JOIN t_base_ec_dim t2 ON t1.cat_id = t2.cate_id
-- WHERE cate_level2_id IS NOT NULL