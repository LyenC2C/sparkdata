



-- API 1. 检验是否注册以及注册时间
SELECT case when t1.tel is  not null then t1.tel else t2.tel end as tel ,
t1.tel,t1.year, t2.weibo_id
from
(
SELECT t1.tb_id,t1.year ,t2.uid as tel

from
(
SELECT tb_id
,
(case when tage              is null then 0 else 1 end +
case when tgender           is null then 0 else 1 end +
case when city              is null then 0 else 1 end +
case when alipay            is null then 0 else 1 end +
case when year              is null then 0 else 1 end +
case when buycnt            is null then 0 else 1 end +
case when verify_level      is null then 0 else 1 end +
case when ac_score_normal   is null then 0 else 1 end +
case when sum_level         is null then 0 else 1 end +
case when feedrate          is null then 0 else 1 end ) as score
from t_pzz_tag_basic_info
)t1

join t_base_uid_tmp  t2 on t2.ds='ttinfo' and t1.tb_id =t2.id1
) t1

-- full join (select uid as tel, id1 as weibo_id from t_base_uid_tmp where ds='wid') t2 on  t1.tel=t2.uid



SELECT  COUNT(1)  from t_base_uid_tmp where ds='wid';