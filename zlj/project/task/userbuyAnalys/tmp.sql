SELECT count(1)
FROM
  (
    SELECT item_id
           from (
               SELECT
                   cast(item_id AS bigint) item_id

                   FROM t_base_ec_item_dev
                   WHERE ds = 20151217
           ) t GROUP BY item_id

  ) t1