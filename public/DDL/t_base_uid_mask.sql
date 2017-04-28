CREATE TABLE t_base_uid_mask
  AS
    SELECT *
    FROM
      t_base_uid_tmp
    WHERE ds = 'uid_mark';