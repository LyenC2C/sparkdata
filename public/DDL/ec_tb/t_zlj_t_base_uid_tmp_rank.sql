--  inner join 266352505
-- 去重排序 tel
Drop table t_zlj_t_base_uid_tmp_rank;
create table t_zlj_t_base_uid_tmp_rank as
SELECT

  id1 as id ,uid , ROW_NUMBER() OVER (PARTITION BY uid ORDER BY CAST( regexp_replace(regtime,'.','') as bigint )  DESC) AS rn
from
(
SELECT uid ,id1  from
t_base_uid_tmp where  uid  rlike '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}'
and ds='ttinfo'
)t1
left join t_base_user_profile t2 on t1.id1=t2.tb_id
 ;

 --  手机号生成index
 Drop table t_zlj_phone_rank_index ;
 create table  t_zlj_phone_rank_index as
 SELECT t1.id as tb_id  ,rn ,t1.uid, t2.id1 as  tel_index
  from
t_zlj_t_base_uid_tmp_rank  t1 join t_base_uid_tmp t2 on t2.ds='tel_index' and t1.uid=t2.uid  ;

-- create table t_zlj_dc_tmp as  SELECT DISTINCT  uid from t_zlj_t_base_uid_tmp_rank ;


LOAD DATA  local  INPATH '/mnt/raid1/zlj/dcweibo/1029_1/tels_index' OVERWRITE INTO TABLE t_base_uid_tmp PARTITION (ds='tel_index')


-- 用户画像

create table t_base_yhhx_tel as
 SELECT
buy_month               ,
avg_cnt                 ,
std_cnt                 ,
avg_price               ,
std_price               ,
avg_price_ratio         ,
cnt_ratio_list          ,
price_ratio_list        ,
brand_id_num            ,
root_cat_id_num         ,
b_bc_price_ratio        ,
b_bc_type_num_ratio     ,
brand_effec_price_ratio ,
brand_effec_num_ratio   ,
total_price             ,
b50_ratio               ,
b50_num_ratio           ,
t2.uid as tel ,
t2.tel_tb_num ,
COALESCE(t3.cnt_total,0) as cnt_total
  from
  t_base_yhhx t1 join
  (select uid,tel_index,count(1) as tel_tb_num  from t_zlj_phone_rank_index  group by uid ,tel_index) t2 on t1.tel_index=t2.tel_index
  left join wlfinance.t_hx_taobao_fraud_userinfo t3 on t2.uid=t3.tel
  ;



  SELECT  * from t_base_yhhx_tel where tel_index=20221166