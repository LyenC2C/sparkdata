

create table t_zlj_shop_desc_score_rank_v2 as

SELECT

t_origin.*,t_normal.smin,t_normal.len ,(t_origin.desc_score-t_normal.smin)/t_normal.len

from
(
    SELECT

    main_cat_name,max(desc_score)-min(desc_score) as len,min(desc_score) as smin    FROM

    (
    SELECT
     t1.shop_id ,main_cat_name ,shop_name,desc_score, ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY desc_score  ASC) AS rn
    from
    (
    SELECT
    shop_id ,shop_name ,5*desc_highgap/100.0 + 2*service_highgap/100.0 +1*wuliu_highgap/100.0  as desc_score
    from
    t_base_ec_shop_dev_new
    where ds=20160613 and bc_type='B'
    )t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id

    )tn group by  main_cat_name

)t_normal
join

(
    SELECT
     t1.shop_id ,main_cat_name ,shop_name,desc_score, ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY desc_score  ASC) AS rn
    from
    (
    SELECT
    shop_id ,shop_name ,5*desc_highgap/100.0 + 2*service_highgap/100.0 +1*wuliu_highgap/100.0  as desc_score
    from
    t_base_ec_shop_dev_new
    where ds=20160613 and bc_type='B'
    )t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id

)t_origin
on t_normal.main_cat_name=t_origin.main_cat_name
limit 10
;
