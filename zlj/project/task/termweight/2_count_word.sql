create table t_zlj_item_feed_title_cut_20151226_word_count as
select word ,count(1) as num from
(
select  word
 from t_zlj_item_feed_title_cut_20151226
 LATERAL  view explode(split(title_cut,' '))t1  as word
 )t group by word  ;