  CREATE TABLE t_zlj_tmp_t_base_ec_item_title_cut_with_brand_tf
    AS
select user_id,word ,count(1) from
      (
        SELECT
          user_id,
          word
        FROM
          (SELECT
             user_id,
             hmm
           FROM
             (
               SELECT
                 item_id,
                 concat_ws(' ', title_cut, concat(cat_name, '_c'), concat(brand_name, '_b')) AS hmm
               FROM t_base_ec_item_title_cut_with_brand
             ) t1
             JOIN
             (
               SELECT
                 item_id,
                 user_id
               FROM t_base_ec_item_feed_dev_tmp
             ) t2
               ON t1.item_id = t2.item_id
          ) t
        lateral VIEW explode(split(hmm, ' ')) myTable AS word
      )t1
   WHERE  length(word)>1

      GROUP BY user_id, word