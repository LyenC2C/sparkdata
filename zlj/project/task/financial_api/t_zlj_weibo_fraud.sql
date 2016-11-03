

ADD FILE hdfs://10.3.4.220:9600/user/zlj/udf/udf_fraud.py;


-- 征信微博API
ADD FILE hdfs://10.3.4.220:9600/user/zlj/udf/udf_fraud.py;
Drop table t_zlj_api_weibo_fraud;

create table t_zlj_api_weibo_fraud as
SELECT weibo_id , description ,desc_fraud_score,desc_keywords  ,screen_name ,nick_fraud_score,nick_keywords
from
(
SELECT
id as weibo_id , description ,screen_name , desc_fraud_score,desc_keywords  ,nick_fraud_score,nick_keywords
from
(
SELECT
TRANSFORM(id , description ,desc_fraud_score,desc_keywords ,screen_name )
USING 'python udf_fraud.py'
as (id , description ,desc_fraud_score,desc_keywords ,screen_name, nick_fraud_score,nick_keywords)
from
(
select
TRANSFORM(id , screen_name ,description)
USING 'python udf_fraud.py'
as (id ,screen_name ,description, desc_fraud_score,desc_keywords)
from
t_zlj_api_weibo_fraud_step2
)t1
)t2
where desc_keywords is not NULL)t3 where desc_fraud_score>0 or nick_fraud_score>0
;


SELECT *  from t_zlj_api_weibo_fraud where  (desc_fraud_score+ nick_fraud_score)>3 limit 100;
--
SELECT  weibo_id , description ,desc_fraud_score,desc_keywords  ,screen_name ,nick_fraud_score,nick_keywords
from
(
select * from t_zlj_api_weibo_fraud
where  desc_keywords is not NULL )t  where (desc_fraud_score+ nick_fraud_score)>3 limit 100;

SELECT  weibo_id , description ,desc_fraud_score,desc_keywords  ,screen_name ,nick_fraud_score,nick_keywords
from
(
select * from t_zlj_api_weibo_fraud
where  desc_keywords is not NULL )t  where (desc_fraud_score+ nick_fraud_score)>3 limit 100;


--
create table t_zlj_api_weibo_fraud_step2 as
SELECT  t1.id , screen_name ,description
from
(SELECT id from t_base_weibo_user_fri_tel  )t1
join  t_base_weibo_user  t2 on  t1.id=t2.id and t2.ds=20160829 ;


SELECT  weibo_id , description ,desc_fraud_score,desc_keywords  ,screen_name ,nick_fraud_score,nick_keywords from t_zlj_api_weibo_fraud
where  desc_keywords is not NULL and  desc_fraud_score>0 or nick_fraud_score>0 limit 100;


--


ADD FILE hdfs://10.3.4.220:9600/user/zlj/udf/udf_fraud.py;
Drop table t_zlj_api_weibo_fraud_step1;
create table t_zlj_api_weibo_fraud_step1  as



Drop table t_zlj_api_weibo_fraud_step1;
create table  t_zlj_api_weibo_fraud_step1 as

SELECT
 id ,screen_name ,description, desc_keywords,desc_fraud_score
from  (
select
TRANSFORM(t1.id , screen_name ,description)
USING 'python udf_fraud.py'
as (id ,screen_name ,description, desc_fraud_score,desc_keywords)
from
(SELECT id from t_base_weibo_user_fri_tel limit 100000  )t1
join  t_base_weibo_user  t2 on  t1.id=t2.id and t2.ds =20160829

)t