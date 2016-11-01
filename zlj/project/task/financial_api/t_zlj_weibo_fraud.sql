

ADD FILE hdfs://10.3.4.220:9600/user/zlj/udf/udf_fraud.py;


-- 征信微博API
create table t_zlj_api_weibo_fraud as
SELECT
id as weibo_id , description ,desc_fraud_score,desc_keywords  ,nick_fraud_score,nick_keywords
from
(
SELECT
TRANSFORM(id , description ,desc_fraud_score,desc_keywords ,screen_name )
USING 'python udf_fraud.py'
as (id , description ,desc_fraud_score,desc_keywords ,screen_name,nick_flag_1, nick_kw_1, nick_flag_2, nick_kw_2,nick_fraud_score,nick_keywords)

from
(
select
TRANSFORM(t1.id , screen_name ,description)
USING 'python udf_fraud.py'
as (id ,screen_name ,description,desc_flag_1, desc_kw_1, desc_flag_2, desc_kw_2,desc_fraud_score,desc_keywords)
from
(SELECT id from t_base_weibo_user_fri_tel )t1
join  t_base_weibo_user  t2 on  t1.id=t2.id and t2.ds =20160829
)t1

)t2