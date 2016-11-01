
Drop table  t_base_ec_record_dev_new_simple_user_rootid;

CREATE TABLE t_base_ec_record_dev_new_simple_user_rootid AS

  SELECT
    user_id,
    root_cat_id,
    count(1)   AS num,
    avg(price) AS avg_price ,
    max(price) as max_price,
    percentile(cast(price as BIGINT),0.5)  mid_price
  FROM t_base_ec_record_dev_new_simple
    where cast(user_id as BIGINT)>0 and  cast(root_cat_id as BIGINT)>0
  GROUP BY user_id, root_cat_id;


CREATE TABLE t_zlj_cat_index AS
  SELECT
    root_cat_id,
    ROW_NUMBER()
    OVER (PARTITION BY id
      ORDER BY root_cat_id) AS root_cat_id_index
  FROM
    (
      SELECT
        root_cat_id,
        1 AS id
      FROM t_base_ec_record_dev_new_simple_user_rootid
      GROUP BY root_cat_id
    )
    t1;

--  用户类目特征表


--  用户类目特征表
 DROP TABLE  t_base_ec_record_dev_new_simple_user_rootid_group_v2 ;

CREATE TABLE t_base_ec_record_dev_new_simple_user_rootid_group_v2
AS
  SELECT
    user_id,
    concat_ws(' ', collect_set(concat_ws(' ', numdata, pricedata,mid_pricedata)))
  FROM
    (
      SELECT

        user_id,
        concat_ws(':', cast(root_cat_id_index AS string), cast(num AS string))             numdata,
        concat_ws(':', cast(root_cat_id_index + 150 AS string), cast(avg_price AS string)) pricedata,
        concat_ws(':', cast(root_cat_id_index + 300 AS string), cast(mid_price AS string)) mid_pricedata

      FROM
        (
          SELECT
            t3.*,
            t2.root_cat_id_index
          FROM
            t_zlj_cat_index  t2
            JOIN t_base_ec_record_dev_new_simple_user_rootid t3

              ON t2.root_cat_id = t3.root_cat_id

        ) t4

    ) tn
  GROUP BY user_id;


DROP TABLE  t_base_ec_record_dev_new_simple_user_rootid_group_v3 ;

CREATE TABLE t_base_ec_record_dev_new_simple_user_rootid_group_v3
AS
  SELECT
    user_id,
    concat_ws(' ', collect_set(concat_ws(' ', numdata, pricedata,nax_pricedata)))
  FROM
    (
      SELECT

        user_id,
        concat_ws(':', cast(root_cat_id_index AS string), cast(num AS string))             numdata,
        concat_ws(':', cast(root_cat_id_index + 150 AS string), cast(avg_price AS string)) pricedata,
        concat_ws(':', cast(root_cat_id_index + 300 AS string), cast(max_price AS string)) nax_pricedata

      FROM
        (
          SELECT
            t3.*,
            t2.root_cat_id_index
          FROM
            t_zlj_cat_index  t2
            JOIN t_base_ec_record_dev_new_simple_user_rootid t3

              ON t2.root_cat_id = t3.root_cat_id

        ) t4

    ) tn
  GROUP BY user_id;


Drop table  t_base_ec_record_dev_new_simple_user_rootid_group_traindata_v3 ;
create TABLE t_base_ec_record_dev_new_simple_user_rootid_group_traindata_v3 as
SELECT  concat_ws(' ',concat( '+',cast(t1.gender as String)), `_c1` )
  from
( select userid ,gender  from t_base_ec_tb_xianyu_userinfo   where  gender  in('0','1') and   rand()<0.3
 group by userid ,gender )
t1 join t_base_ec_record_dev_new_simple_user_rootid_group_v3 t2 on t1.userid =t2.user_id ;





Drop table  t_base_ec_record_dev_new_simple_user_rootid_group_traindata_v2 ;
create TABLE t_base_ec_record_dev_new_simple_user_rootid_group_traindata_v2 as
SELECT  concat_ws(' ',concat( '+',cast(t1.gender as String)), `_c1` )
  from
( select userid ,gender  from t_base_ec_tb_xianyu_userinfo   where  gender  in('0','1') and   rand()<0.3
 group by userid ,gender )
t1 join t_base_ec_record_dev_new_simple_user_rootid_group_v2  t2 on t1.userid =t2.user_id ;


select
brand_id ,brand_name  ,avg(price) as avg_price , count(1) num from t_base_ec_item_dev_new where
   bc_type='B'  and length(brand_id )>1 and 	root_cat_id in
('50005700',
'50496015',
'121454006',
'121474002',
'124628002',
'124632002',
'124646002',
'124648002',
'124656001',
'124688012')
and  ds ='20160731'
group by brand_id ,brand_name