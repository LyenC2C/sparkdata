hive<<EOF
Drop table t_zlj_api_weibo_fraud ;
create table t_zlj_api_weibo_fraud as
SELECT  /*+ mapjoin(t4)*/
COALESCE(t3.weibo_id ,t4.id) as weibo_id  , description ,screen_name , desc_fraud_score,desc_keywords  ,nick_fraud_score,nick_keywords ,
t4.follow_ids as finance_weiboids,
t4.follow_names as finance_weibonames
from t_zlj_api_weibo_fraud_step1 t3 full join
(
SELECT
t2.id ,
concat_ws('|',collect_set( concat_ws(':',fid,screen_name )) as follow_ids,
concat_ws('|',collect_set(  screen_name )) as follow_names
from
(
	SELECT screen_name ,id
from t_base_weibo_user where ds ='20161104' and screen_name  in (
       '微贷网',
       '宜人贷',
       '拍拍贷',
       '翼龙贷',
       '积木盒子',
       '和信贷',
       '恒易融',
       '口碑贷',
       '招商贷',
       '手机贷',
       '点融网',
       '北银消费金融',
       '中银消费金融',
       '锦程消费金融',
       '捷信',
       '招联金融',
       '海尔消费金融',
       '苏宁消费金融',
       '马上消费金融官微',
       '贷款易',
       '分期乐'
       )
)
t1 join
(
SELECT id ,fid
from t_base_weibo_user_fri_tel
lateral view explode(split(ids,',')) tt as fid
) t2 on t1.id =t2.fid
group by t2.id
)t4 on t3.weibo_id =t4.id
;



EOF