


-- 创建一张临时表 方便后面使用

create TABLE  t_base_ec_item_dev_new_job_tmp  AS


SELECT *  FROM t_base_ec_item_dev_new
  WHERE ds = 20160615
  ;