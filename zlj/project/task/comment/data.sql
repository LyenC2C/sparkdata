
create table t_zlj_feed_parse_corpus_2015 as
SELECT
  item_id,
  user_id,
  content
FROM t_base_ec_item_feed_dev
WHERE ds>20150101 and length(trim(content)) > 1 and length(trim(content)) <50  AND NOT content LIKE '%默认好评%' AND NOT content LIKE 'null'
  and not content like '%此用户没有填写评论%'  and not  content like '好评！' and not  content like  '未写初评'