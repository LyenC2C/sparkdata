


-- 江哥 店铺  2016.6.22 by 子昊




---------店铺描述分:	与同行业相比:


select y1.*,y2.total_rn
from
(SELECT  * FROM
	(
	SELECT  main_cat_name,shop_id ,shop_name ,
	  ROW_NUMBER()
		OVER (PARTITION BY main_cat_name
		  ORDER BY desc_score DESC ) AS rn
	from
	t_zlj_shop_join_major

	)t where shop_id in ('104820621','103569798','57299948')
)y1
join
(select main_cat_name,count(1) as total_rn
from t_zlj_shop_join_major
group by main_cat_name)y2
on y1.main_cat_name=y2.main_cat_name
 ;

---------店铺服务分:	与同行业相比:-


select y1.*,y2.total_rn
from
(SELECT  * FROM
	(
	SELECT  main_cat_name,shop_id ,shop_name ,
	  ROW_NUMBER()
		OVER (PARTITION BY main_cat_name
		  ORDER BY  service_score DESC ) AS rn
	from
	t_zlj_shop_join_major

	)t where shop_id in ('104820621','103569798','57299948')
)y1
join
(select main_cat_name,count(1) as total_rn
from t_zlj_shop_join_major
group by main_cat_name)y2
on y1.main_cat_name=y2.main_cat_name


----------店铺物流分:	与同行业相比:-


select y1.*,y2.total_rn
from
(SELECT  * FROM
	(
	SELECT  main_cat_name,shop_id ,shop_name ,
	  ROW_NUMBER()
		OVER (PARTITION BY main_cat_name
		  ORDER BY wuliu_score DESC ) AS rn
	from
	t_zlj_shop_join_major

	)t where shop_id in ('104820621','103569798','57299948')
)y1
join
(select main_cat_name,count(1) as total_rn
from t_zlj_shop_join_major
group by main_cat_name)y2
on y1.main_cat_name=y2.main_cat_name


-----------信用等级:	同行业排名:



select y1.*,y2.total_rn
from
(SELECT  * FROM
	(
	SELECT  main_cat_name,shop_id ,shop_name ,
	  ROW_NUMBER()
		OVER (PARTITION BY main_cat_name
		  ORDER BY credit DESC ) AS rn
	from
	t_zlj_shop_join_major

	)t where shop_id in ('104820621','103569798','57299948')
)y1
join
(select main_cat_name,count(1) as total_rn
from t_zlj_shop_join_major
group by main_cat_name)y2
on y1.main_cat_name=y2.main_cat_name


-----------信用等级:	同地域排名:



select y1.*,y2.total_rn
from
(SELECT  * FROM
	(
	SELECT  location,shop_id ,shop_name ,
	  ROW_NUMBER()
		OVER (PARTITION BY location
		  ORDER BY credit DESC ) AS rn
	from
	t_zlj_shop_join_major

	)t where shop_id in ('104820621','103569798','57299948')
)y1
join
(select location,count(1) as total_rn
from t_zlj_shop_join_major
group by location)y2
on y1.location=y2.location


-----------店龄:	同行业排名:


select y1.*,y2.total_rn
from
(SELECT  * FROM
	(SELECT  main_cat_name,shop_id ,shop_name ,
	  ROW_NUMBER()
		OVER (PARTITION BY main_cat_name
		  ORDER BY shopn DESC ) AS rn
	from
		(select r.*,cast((12 * (2017 - YEAR(starts)) - MONTH(starts)) as float) shopn
		from
		t_zlj_shop_join_major r
	
		)r1
	)t where shop_id in ('104820621','103569798','57299948')
)y1
join
(select main_cat_name,count(1) as total_rn
from t_zlj_shop_join_major
group by main_cat_name)y2
on y1.main_cat_name=y2.main_cat_name


-----------粉丝数:	同行业排名:



select y1.*,y2.total_rn
from
(SELECT  * FROM
	(
	SELECT  main_cat_name,shop_id ,shop_name ,
	  ROW_NUMBER()
		OVER (PARTITION BY main_cat_name
		  ORDER BY  fans_count DESC ) AS rn
	from
	t_zlj_shop_join_major

	)t where shop_id in ('104820621','103569798','57299948')
)y1
join
(select main_cat_name,count(1) as total_rn
from t_zlj_shop_join_major
group by main_cat_name)y2
on y1.main_cat_name=y2.main_cat_name
;



--------------------
SELECT
 u1.main_cat_name,
 u1.vervalue,
 u2.verall,
 cast(u1.verify_num / u2.verify_all as float) verpercent
FROM
 (
   SELECT
     main_cat_name,
     cast(substr(verify, 6, 1) AS FLOAT) vervalue,
     vernum
   FROM
     (SELECT
        main_cat_name,
        verify,
        COUNT(1) vernum
      FROM
        (SELECT
           t1.shop_id,
           t2.main_cat_name,
           t1.verify
         FROM t_zlj_shop_shop_user_level_verify t1
           JOIN t_base_shop_major_all t2
             ON t1.shop_id = t2.shop_id) y
      GROUP BY main_cat_name, verify) r
 ) u1
 JOIN
 (SELECT
    main_cat_name,
    COUNT(1) verall
  FROM
    (SELECT
       t1.shop_id,
       t2.main_cat_name,
       t1.verify
     FROM t_zlj_shop_shop_user_level_verify t1
       JOIN t_base_shop_major_all t2
         ON t1.shop_id = t2.shop_id) y
  GROUP BY main_cat_name) u2
   ON u1.main_cat_name = u2.main_cat_name
