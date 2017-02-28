

drop table t_zlj_base_match;


create table t_zlj_base_match(
tel string,
id1 string ,
id2 string ,
id3 string ,
id4 string ,
id5 string ,
id6 string ,
id7 string
)
COMMENT '商务匹配表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\,'   LINES TERMINATED BY '\n'
stored as textfile ;

LOAD DATA   INPATH '/user/zlj/match/data.csv' OVERWRITE INTO TABLE t_zlj_base_match PARTITION (ds='rong360_test_1111') ;
LOAD DATA   INPATH '/user/zlj/match/rong360.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='ygz_part') ;
LOAD DATA   INPATH '/user/zlj/match/data_2k.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='data_2k') ;
LOAD DATA   INPATH '/user/zlj/match/7w.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='7wid') ;
LOAD DATA   INPATH '/user/zlj/match/20161223.zhima.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='zhima_1223') ;
LOAD DATA   INPATH '/user/zlj/match/正负样本.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='yixin_rong360_0103') ;

LOAD DATA   INPATH '/user/zlj/match/20170103_20161223_merge_upload.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='zhima_20170103_20161223_merge') ;

-- 锦城  tel  label ds
LOAD DATA   INPATH '/user/zlj/match/1.txt' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='jingchen_label') ;

LOAD DATA   INPATH '/user/zlj/match/薪时贷.txt' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='jingchen_xinshidai') ;
LOAD DATA   INPATH '/user/zlj/match/学生贷.txt' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='jingchen_xueshengdai') ;
LOAD DATA   INPATH '/user/zlj/match/捷信贷款.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='jingchen_jiexin') ;
LOAD DATA   INPATH '/user/zlj/match/yixin_label.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='yixin_label_ori') ;

LOAD DATA   INPATH '/user/zlj/match/20170110.zhima.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='zhima') ;
LOAD DATA   INPATH '/user/zlj/match/20170114.zhima.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='zhima') ;
LOAD DATA   INPATH '/user/zlj/match/zhima_rs.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='zhima') ;
LOAD DATA   INPATH '/user/zlj/match/中诚信对接测试数据.txt' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='zhongchengxin') ;
LOAD DATA   INPATH '/user/zlj/match/puhui1.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='puhui') ;
LOAD DATA   INPATH '/user/zlj/match/puhui1.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='puhui') ;
LOAD DATA   INPATH '/user/zlj/match/huxia_线下小额测试样本.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='offline_loan') ;
LOAD DATA   INPATH '/user/zlj/match/广发数据.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='guangfa') ;
LOAD DATA   INPATH '/user/zlj/match/手机号.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='guangfa_all') ;


SELECT
t1.tel,t2.tel_index , t2.*  ,
FROM
(
SELECT  tb,t1.tel ,t2.tel_index
from wlfinance.t_zlj_base_match t1
join wlrefer.t_zlj_uid_name t2 on t1.tel =t2.tel
)join t_base_record_cate_simple_ds t2 on t1.tb= t2.userid  and t1.ds='jingchen_label'


-- 广发测试
drop table  wlservice.t_lt_guangfa_tel_snwb;
create table wlservice.t_lt_guangfa_tel_snwb as
SELECT  t1.tel ,t1.id1, snwb,tb
from wlfinance.t_zlj_base_match t1
join wlrefer.t_zlj_uid_name t2 on t1.tel =t2.tel and t1.ds='guangfa_all'




select
COUNT(1)
from wlfinance.t_zlj_base_match t1 join wlrefer.t_zlj_uid_name t2 on t1.ds='offline_loan' and t1.tel=t2.tel ;

drop table wlcredit.t_zlj_zhima_score_loc ;
create table wlcredit.t_zlj_zhima_score_loc as
SELECT
t1.*, tel_loc
from
wlfinance.t_zlj_base_match t1
join
wlbase_dev.t_base_user_profile t2 on t1.tel=t2.tb_id
where t1.ds='zhima'
;
-- 'test'

--中诚信测试

SELECT

t1.*
from
(
 SELECT
 tel name ,
 id1 idcard,
 id2 tel,
 id3 yuqi,
 id4 time_sub ,
 id5 credit
 from
 wlfinance.t_zlj_base_match  where ds='zhongchengxin'
 )t1 join wlrefer.t_zlj_uid_name t2 on t1.tel =t2.tel


-- index,userid,tel,zhima_score

SELECT
 uid,tel_index
from wlrefer.t_zlj_phone_rank_index t1 join wlfinance.t_zlj_base_match t2 on t1.tel_index  =t2.tel;
-- LOAD DATA   INPATH '/user/zlj/match/browse_group1.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='rong360_browse_group1') ;

SELECT
COUNT(1)
 from  t_zlj_base_match t1 join wlbase_dev.t_base_yhhx_model_tel t2 on t1.ds='rong360_test_1111' and t1.tel=t2.uid ;


SELECT
   user_id  ,concat_ws(',', collect_set(concat_ws(':',tview,tcount)))
   from
(
SELECT
tel as user_id ,
id1 as tview,
id2 as view_s ,
id3 as tcount
from wlfinance.t_zlj_base_match where ds='rong360_browse_group1'
)t group by user_id ;



-- LOAD DATA  local   INPATH '/home/zlj/data/360/credit/train/browse_history_train.txt' OVERWRITE
--   INTO TABLE wlfinance.t_zlj_tmp_csv PARTITION (ds='360_browse_history_train') ;
--
-- LOAD DATA  local   INPATH '/home/zlj/data/360/credit/train/bill_detail_train.txt' OVERWRITE
--   INTO TABLE wlfinance.t_zlj_tmp_csv PARTITION (ds='bill_detail_train') ;
--
--   LOAD DATA  local   INPATH '/home/zlj/data/360/credit/train/bank_detail_train.txt' OVERWRITE
--   INTO TABLE wlfinance.t_zlj_tmp_csv PARTITION (ds='bank_detail_train') ;
--
--
--
-- LOAD DATA  local   INPATH '/home/zlj/data/360/credit/browse_history' OVERWRITE
--   INTO TABLE wlfinance.t_zlj_tmp_csv PARTITION (ds='360_browse_history') ;
--
--   LOAD DATA  local   INPATH '/home/zlj/data/360/credit/bill_detail' OVERWRITE
--   INTO TABLE wlfinance.t_zlj_tmp_csv PARTITION (ds='360_bill_detail') ;
--
--   LOAD DATA  local   INPATH '/home/zlj/data/360/credit/bank_detail' OVERWRITE
--   INTO TABLE wlfinance.t_zlj_tmp_csv PARTITION (ds='360_bank_detail') ;
--
--     LOAD DATA  local   INPATH '/home/zlj/data/360/credit/loan_time' OVERWRITE
--   INTO TABLE wlfinance.t_zlj_tmp_csv PARTITION (ds='360_loan_time') ;
--
--
--     LOAD DATA  local   INPATH '/home/zlj/data/360/credit/user_overdue_label' OVERWRITE
--   INTO TABLE wlfinance.t_zlj_tmp_csv PARTITION (ds='360_user_overdue_label') ;
--
--
--
--   LOAD DATA  local   INPATH '/home/zlj/data/360/credit/browse_history_dropdup' OVERWRITE
--   INTO TABLE wlfinance.t_zlj_tmp_csv PARTITION (ds='360_browse_history_dropdup') ;
--
--   LOAD DATA  local   INPATH '/home/zlj/data/360/credit/bill_detail_dropdup' OVERWRITE
--   INTO TABLE wlfinance.t_zlj_tmp_csv PARTITION (ds='360_bill_detail_dropdup') ;
--
--   LOAD DATA  local   INPATH '/home/zlj/data/360/credit/bank_detail_dropdup' OVERWRITE
--   INTO TABLE wlfinance.t_zlj_tmp_csv PARTITION (ds='360_bank_detail_dropdup') ;