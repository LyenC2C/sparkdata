--1) 1913629393  segment item
drop table wl_analysis.t_lt_base_ciyun_item_segment_words;
create table if not exists wl_analysis.t_lt_base_ciyun_item_segment_words(
item_id bigint comment '商品id',
words string comment '商品分词结果'
)
comment '淘宝商品名分词结果表'
partitioned by (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' stored as textfile;

load data INPATH '/user/lt/cleanWords/part*' into TABLE wl_analysis.t_lt_base_item_segment_words PARTITION (ds='20170428');


-- 2)9475989101  user's item words
create table if not exists wl_analysis.t_lt_base_ciyun_user_record_segment_words(
user_id string comment '用户id',
words string comment '商品分词结果',
item_id bigint comment '商品id'
)
comment '用户购买商品分词结果表'
partitioned by (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' stored as textfile;

-- insert INTO table wl_analysis.t_lt_base_ciyun_user_item_segment_words partition(ds='20170428')
-- SELECT * from wl_analysis.t_lt_base_ciyun_user_item_segment_words_temp;

create table wl_analysis.t_lt_base_ciyun_user_record_segment_words as
SELECT
t2.user_id ,words ,t2.item_id from
(select cast(item_id as bigint) item_id ,words
	from wl_analysis.t_lt_base_ciyun_item_segment_words where length(words)>10 )t1
join
(select item_id ,user_id from wl_analysis.t_base_record_cate_simple_ds where  ds>'201601')t2
on t1.item_id=t2.item_id;

--3)group by user
create table if not exists wl_analysis.t_lt_base_ciyun_user_record_segment_words_group(
user_id string comment '用户id',
words string comment '所有商品分词结果'
)
partitioned by (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' stored as textfile;


--4)user ciyun
create table if not exists wl_analysis.t_lt_base_ciyun_user_record_words_weight(
user_id bigint comment '用户id',
item_weight string comment '商品词语权重'
)
comment '淘宝用户商品词语权重表'
partitioned by (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' stored as textfile;


--5)更新用户购买记录表
CREATE TABLE wl_analysis.t_lt_ciyun_user_item_tmp AS
select item_id,user_id from wl_base.t_base_ec_item_feed_dev_new
where ds>'201501' and ds<'201601';

insert into table t_lt_base_ciyun_user_record_segment_words partition(ds='20170504')
SELECT t2.user_id,t1.words,t1.item_id FROM
(select item_id ,words from
wl_analysis.t_lt_base_ciyun_item_segment_words where length(words)>10 and ds='20170428')t1
join
(select cast(item_id as bigint) item_id,user_id from wl_analysis.t_lt_ciyun_user_item_tmp)t2
on t1.item_id=t2.item_id;

insert into table t_lt_base_ciyun_user_record_segment_words_group partition(ds='20170504')
SELECT user_id,concat_ws('_',collect_list(words)) as words
FROM t_lt_base_ciyun_user_record_segment_words
where ds='20170504'
group by user_id;

--合并所有数据
insert into table t_lt_base_ciyun_user_record_segment_words_group partition(ds='20170505')
SELECT user_id,concat_ws('_',collect_list(words)) as words
FROM t_lt_base_ciyun_user_record_segment_words_group
group by user_id;

insert into table t_lt_base_ciyun_user_record_segment_words partition(ds='20170505')
SELECT user_id,words,item_id
FROM t_lt_base_ciyun_user_record_segment_words
group by user_id,words,item_id;

ALTER TABLE t_lt_base_ciyun_user_record_segment_words DROP IF EXISTS PARTITION (ds='20170504');
ALTER TABLE t_lt_base_ciyun_user_record_segment_words DROP IF EXISTS PARTITION (ds='20170428');
ALTER TABLE t_lt_base_ciyun_user_record_segment_words_group DROP IF EXISTS PARTITION (ds='20170504');
ALTER TABLE t_lt_base_ciyun_user_record_segment_words_group DROP IF EXISTS PARTITION (ds='20170428');