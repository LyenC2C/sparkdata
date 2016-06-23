-- 创建14个店铺的指定时间的商品日销量表


DROP TABLE  IF EXISTS   t_wrt_tmp_14shop_totalsold;

CREATE TABLE t_wrt_tmp_14shop_totalsold AS
  SELECT
    /*+ mapjoin(t2)*/
    t2.shop_id        AS shop_id,
    t1.item_id        AS item_id,
    t2.title          AS title,
    t1.day_sold       AS sold,
    t1.day_sold_price AS sales,
    t1.ds             AS ds
  FROM
    (
      SELECT
        item_id,
        day_sold,
        day_sold_price,
        ds
      FROM wlbase_dev.t_base_ec_item_daysale_dev_new
      WHERE ds > 20160400 AND ds < 20160600
    ) t1
    JOIN
    (
      SELECT
        item_id,
        shop_id,
        price,
        title,
        ds
      FROM wlbase_dev.t_base_ec_item_dev_new
      WHERE ds = 20160615
    ) t2
      ON
        t1.item_id = t2.item_id;


