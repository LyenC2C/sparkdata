

DROP  TABLE t_base_user_consum_statis_feature  ;



create table t_base_user_consum_statis_feature_tmp as
select

t1.consume_price           ,
t1.avg_price               ,
t1.max_price               ,
t1.min_price               ,
t1.local_buycnt            ,
t1.cat_id_num              ,
t1.root_cat_id_num         ,
t1.brand_id_num            ,
t1.brand_effec_id_num      ,
t1.brand_no_effec_id_num   ,
t1.annoy_num               ,
t1.no_annoy_num            ,
t1.b_bc_type_num           ,
t1.c_bc_type_num           ,
t1.b50_ratio               ,
t1.a50b100_ratio           ,
t1.a100b500_ratio          ,
t1.a500b1000_ratio         ,
t1.a1000b5000_ratio        ,
t1.a5000b10000_ratio       ,
t1.a10000_raio             ,
t2.tmall_ratio,
t2.beauty_ratio,
t2.game_ratio,
t2.edu_ratio,
t2.medical_ratio,
t2.fraud_cnt ,
t3.std_pay
from t_base_user_consum_statis_data

t1
 JOIN t_base_user_consum_statis_data_title t2  on (t1.user_id=t2.user_id )
left join t_base_user_consum_statis_user_month_stddev t3
on t1.user_id =t2.user_id
;
