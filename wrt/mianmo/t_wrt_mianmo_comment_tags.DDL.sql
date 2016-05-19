CREATE EXTERNAL TABLE  if not exists t_wrt_mianmo_comment_tags(
        item_id string,
        tag_id string,
        count string,
        tag string,
        posi string
)
COMMENT '面膜评论标签表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;