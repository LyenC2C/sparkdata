
-- 打分排序v2
create table t_zlj_shop_desc_score_rank_v2 as

SELECT

t_origin.*,(t_origin.desc_score-t_normal.smin)/t_normal.len

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
;

-- 成长性v2
create table t_zlj_shop_quant_rank_v2 as

SELECT

t_origin.*,(t_origin.growing_score-t_normal.smin)/t_normal.len

from
(
    SELECT

    main_cat_name,max(growing_score)-min(growing_score) as len,min(growing_score) as smin    FROM

    (
        SELECT
        t1.shop_id ,main_cat_name ,shop_name,credit,total_month,growing_score,ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY growing_score  ASC) AS rn
        from
        (
        SELECT
        shop_id ,shop_name ,credit*100 as  credit ,(12*(2016-YEAR(starts))+MONTH(starts)) as total_month, credit*100.0/(12*(2016-YEAR(starts))+MONTH(starts)) as growing_score
        from
        t_base_ec_shop_dev_new
        where ds=20160613 and bc_type='B'
        )t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id
    )tn group by  main_cat_name

)t_normal
join

(
    SELECT
    t1.shop_id ,main_cat_name ,shop_name,credit,total_month, growing_score,ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY growing_score  ASC) AS rn
    from
    (
    SELECT
    shop_id ,shop_name ,credit*100 as  credit ,(12*(2016-YEAR(starts))+MONTH(starts)) as total_month, credit*100.0/(12*(2016-YEAR(starts))+MONTH(starts)) as growing_score
    from
    t_base_ec_shop_dev_new
    where ds=20160613 and bc_type='B'
    )t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id

)t_origin
on t_normal.main_cat_name=t_origin.main_cat_name;

-- 销量排序v2
create table t_zlj_shop_sold_num_rank_v2 as

SELECT

t_origin.*,(t_origin.tsnu-t_normal.smin)/t_normal.len

from
(
    SELECT

    main_cat_name,max(tsnu)-min(tsnu) as len,min(tsnu) as smin    FROM

    (
        SELECT
         t1.shop_id ,main_cat_name  ,shop_name,tsnu, ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY tsnu  ASC) AS rn
        from
        wlservice.t_zlj_shop_anay_statis_info
        t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id
    )tn group by  main_cat_name

)t_normal
join

(
    SELECT
     t1.shop_id ,main_cat_name  ,shop_name,tsnu, ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY tsnu  ASC) AS rn
    from
    wlservice.t_zlj_shop_anay_statis_info
    t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id

)t_origin
on t_normal.main_cat_name=t_origin.main_cat_name;

-- 销售额排序v2
create table t_zlj_shop_sold_price_rank_v2 as

SELECT

t_origin.*,(t_origin.tsmo-t_normal.smin)/t_normal.len

from
(
    SELECT

    main_cat_name,max(tsmo)-min(tsmo) as len,min(tsmo) as smin    FROM

    (
        SELECT
         t1.shop_id ,main_cat_name  ,shop_name ,tsmo, ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY tsmo  ASC) AS rn
        from
        wlservice.t_zlj_shop_anay_statis_info
        t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id
    )tn group by  main_cat_name

)t_normal
join

(
    SELECT
     t1.shop_id ,main_cat_name  ,shop_name ,tsmo, ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY tsmo  ASC) AS rn
    from
    wlservice.t_zlj_shop_anay_statis_info
    t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id

)t_origin
on t_normal.main_cat_name=t_origin.main_cat_name;


-- 库存排序v2

create table t_zlj_shop_quant_rank_v2 as

SELECT

t_origin.*,(t_origin.trenu-t_normal.smin)/t_normal.len

from
(
    SELECT

    main_cat_name,max(trenu)-min(trenu) as len,min(trenu) as smin    FROM

    (
        SELECT
         t1.shop_id ,main_cat_name  ,shop_name,trenu, ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY trenu  ASC) AS rn
        from
        wlservice.t_zlj_shop_anay_statis_info
        t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id
    )tn group by  main_cat_name

)t_normal
join

(
    SELECT
     t1.shop_id ,main_cat_name  ,shop_name,trenu, ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY trenu  ASC) AS rn
    from
    wlservice.t_zlj_shop_anay_statis_info
    t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id

)t_origin
on t_normal.main_cat_name=t_origin.main_cat_name;