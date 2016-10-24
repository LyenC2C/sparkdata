CREATE TABLE t_base_shop_major_all AS SELECT
                                        shop_id,
                                        shop_name,
                                        industry_name AS main_cat_name
                                      FROM wlservice.t_lzh_shop_dim
                                      WHERE LENGTH(industry_name) > 2;


-- 打分排序v2

DROP TABLE IF EXISTS t_zlj_shop_desc_score_rank_v2;
CREATE TABLE t_zlj_shop_desc_score_rank_v2 AS

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
            main_cat_name,
            t1.shop_name,
            desc_score,
            ROW_NUMBER()
            OVER (PARTITION BY main_cat_name
              ORDER BY desc_score ASC) AS rn
          FROM
            (
              SELECT
                shop_id,
                shop_name,
                5 * desc_highgap / 100.0 + 2 * service_highgap / 100.0 + 1 * wuliu_highgap / 100.0 AS desc_score
              FROM
                t_base_ec_shop_dev_new
              WHERE ds = 20160613 AND bc_type = 'B'
            ) t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id

        ) tn
      GROUP BY main_cat_name

    ) t_normal
    JOIN

    (
      SELECT
        t1.shop_id,
        main_cat_name,
        t1.shop_name,
        desc_score,
        ROW_NUMBER()
        OVER (PARTITION BY main_cat_name
          ORDER BY desc_score ASC) AS rn
      FROM
        (
          SELECT
            shop_id,
            shop_name,
            5 * desc_highgap / 100.0 + 2 * service_highgap / 100.0 + 1 * wuliu_highgap / 100.0 AS desc_score
          FROM
            t_base_ec_shop_dev_new
          WHERE ds = 20160613 AND bc_type = 'B'
        ) t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id

    ) t_origin
      ON t_normal.main_cat_name = t_origin.main_cat_name;

-- 成长性v2

DROP TABLE IF EXISTS t_zlj_shop_grow_rank_v2;

CREATE TABLE t_zlj_shop_grow_rank_v2 AS
  SELECT

    t_origin.*,
    (t_origin.growing_score - t_normal.smin) / t_normal.len AS nor_growing_s

  FROM
    (
      SELECT

        main_cat_name,
        max(growing_score) - min(growing_score) AS len,
        min(growing_score)                      AS smin
      FROM

        (
          SELECT
            t1.shop_id,
            main_cat_name,
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
                credit * 100                                                  AS credit,
                (12 * (2016 - YEAR(starts)) + MONTH(starts))                  AS total_month,
                credit * 100.0 / (12 * (2016 - YEAR(starts)) + MONTH(starts)) AS growing_score
              FROM
                t_base_ec_shop_dev_new
              WHERE ds = 20160613 AND bc_type = 'B'
            ) t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id
        ) tn
      GROUP BY main_cat_name

    ) t_normal
    JOIN

    (
      SELECT
        t1.shop_id,
        main_cat_name,
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
            credit * 100                                                  AS credit,
            (12 * (2016 - YEAR(starts)) + MONTH(starts))                  AS total_month,
            credit * 100.0 / (12 * (2016 - YEAR(starts)) + MONTH(starts)) AS growing_score
          FROM
            t_base_ec_shop_dev_new
          WHERE ds = 20160613 AND bc_type = 'B'
        ) t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id

    ) t_origin
      ON t_normal.main_cat_name = t_origin.main_cat_name;



-- 销量排序v2
DROP TABLE IF EXISTS t_zlj_shop_sold_num_rank_v2;

CREATE TABLE t_zlj_shop_sold_num_rank_v2 AS

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
            wlservice.t_zlj_shop_anay_statis_info
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
        wlservice.t_zlj_shop_anay_statis_info
        t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id

    ) t_origin
      ON t_normal.main_cat_name = t_origin.main_cat_name;



-- 销售额排序v2
DROP TABLE IF EXISTS t_zlj_shop_sold_num_rank_v2;

CREATE TABLE t_zlj_shop_sold_price_rank_v2 AS

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
            wlservice.t_zlj_shop_anay_statis_info
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
        wlservice.t_zlj_shop_anay_statis_info
        t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id

    ) t_origin
      ON t_normal.main_cat_name = t_origin.main_cat_name;




-- 库存排序v2
DROP TABLE IF EXISTS t_zlj_shop_quant_rank_v2;

CREATE TABLE t_zlj_shop_quant_rank_v2 AS

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
            wlservice.t_zlj_shop_anay_statis_info
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
        wlservice.t_zlj_shop_anay_statis_info
        t1 JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id

    ) t_origin
      ON t_normal.main_cat_name = t_origin.main_cat_name;





DROP TABLE IF EXISTS t_zlj_shop_result_rank_v2;


CREATE TABLE t_zlj_shop_result_rank_v2 AS
  SELECT
    *,
    ROW_NUMBER()
    OVER (PARTITION BY main_cat_name
      ORDER BY sum_score DESC) AS rn
  FROM
    (
      SELECT
        t1.shop_id,
        t1.main_cat_name,
        t1.shop_name,
        nor_desc_s,
        desc_score,
        nor_sold_score,
        tsnu,
        nor_sold_price_score,
        tsmo,
        nor_quant_score,
        trenu,
        nor_growing_s,
        growing_score,
        nor_desc_s * 3 + nor_sold_score * 2 + nor_sold_price_score * 5 + nor_quant_score * 1 + nor_growing_s * 6
          AS sum_score
      FROM
        t_zlj_shop_desc_score_rank_v2 t1 JOIN
        t_zlj_shop_sold_num_rank_v2 t2 ON t1.shop_id = t2.shop_id
        JOIN t_zlj_shop_sold_price_rank_v2 t3 ON t1.shop_id = t3.shop_id
        JOIN t_zlj_shop_quant_rank_v2 t4 ON t1.shop_id = t4.shop_id
        JOIN t_zlj_shop_grow_rank_v2 t5 ON t1.shop_id = t5.shop_id
    ) t;


select user_id,root_cat_id,cate_level2_id,price  from t_zlj_ec_userbuy
                                                      t1 join t_base_ec_dim t2 on t1.cat_id=t2.cate_id
where cate_level2_id is not null