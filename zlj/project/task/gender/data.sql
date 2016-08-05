CREATE TABLE t_base_ec_record_dev_new_simple_user_rootid AS

  SELECT
    user_id,
    root_cat_id,
    count(1)   AS num,
    avg(price) AS avg_price
  FROM t_base_ec_record_dev_new_simple
  GROUP BY user_id, root_cat_id;

--  用户类目特征表
CREATE TABLE t_base_ec_record_dev_new_simple_user_rootid_group
AS
  SELECT
    user_id,
    concat_ws('\t', collect_set(concat_ws('\t', numdata, pricedata)))
  FROM
    (
      SELECT

        user_id,
        concat_ws(':', cast(root_cat_id_index AS string), cast(num AS string))             numdata,
        concat_ws(':', cast(root_cat_id_index + 150 AS string), cast(avg_price AS string)) pricedata
      FROM
        (
          SELECT
            t3.*,
            t2.root_cat_id_index
          FROM
            (
              SELECT
                root_cat_id,
                ROW_NUMBER()
                OVER (PARTITION BY id
                  ORDER BY root_cat_id) AS root_cat_id_index

              FROM
                (
                  SELECT
                    root_cat_id,
                    1 AS id
                  FROM t_base_ec_record_dev_new_simple_user_rootid
                  GROUP BY root_cat_id
                )
                t1
            ) t2
            JOIN t_base_ec_record_dev_new_simple_user_rootid t3

              ON t2.root_cat_id = t3.root_cat_id

        ) t4

    ) tn
  GROUP BY user_id;