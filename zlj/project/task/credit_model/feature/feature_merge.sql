



CREATE  TABLE   if not exists wlcredit.t_credit_feature_merge (
userid   String COMMENT '',
features String ,
id1 String ,
id2 String
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;


  LOAD DATA     INPATH '/user/zlj/temp/all_feature_main_cat/' OVERWRITE
  INTO TABLE wlcredit.t_credit_feature_merge PARTITION (ds='feature_cate123') ;


sc.textFile('/user/wrt/temp/all_feature_main_cat/').map(lambda x:
                                                        '\001'.join([x.split(' ')[0],' '.join(x.split()[1:])]))\
    .saveAsTextFile('/user/zlj/temp/all_feature_main_cat/')
-- SELECT
-- from
-- wlcredit.t_credit_record_cate1_feature t1
-- left  join
-- wlcredit.t_credit_record_cate2_feature t2  on t1.tel_index=t2.tel_index
-- left join
-- wlcredit.t_credit_record_cate3_feature t3  on t1.tel_index=t3.tel_index
-- left join
-- wlcredit.t_credit_record_cate1_3month_feature t4  on t1.tel_index=t4.tel_index
-- left join
-- wlcredit.t_credit_record_cate1_feature t5  on t1.tel_index=t5.tel_index
-- left join
-- wlcredit.t_credit_record_cate1_feature t6  on t1.tel_index=t6.tel_index
-- left join
-- wlcredit.t_credit_record_cate1_feature t2  on t1.tel_index=
--
-- ;



drop table wlcredit.t_zlj_360_label_feature_svm ;
create table wlcredit.t_zlj_360_label_feature_svm as
SELECT
/*+ mapjoin(t1)*/
t1.tel ,class,t1.label ,t2.features
 from

(
SELECT tel ,tel_index,class,label ,gender,age from
(
SELECT  tel,'8000_c' as class , id1 as label,id3 as gender,id4 as age
   from
   wlfinance.t_zlj_base_match where ds='ygz_part'
   union all
    SELECT tel,'2000_c' as class,'' as label ,id1 as gender,id2 as age
   from
   wlfinance.t_zlj_base_match where ds='rong360_test_1111'
    union all
   SELECT  tel,'data_2k' as class , id1 as label,id3 as gender,id4 as age
   from
   wlfinance.t_zlj_base_match where ds='data_2k'
    )t1
    join
wlbase_dev.t_zlj_phone_rank_index t2 on t1.tel =t2.uid
group by tel_index,class,label ,gender,age,t1.tel
)t1
    join
wlcredit.t_credit_feature_merge t2 on t1.tel_index=t2.userid and t2.ds ='feature_cate123'
;