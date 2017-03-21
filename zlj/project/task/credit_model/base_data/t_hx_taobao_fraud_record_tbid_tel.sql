ADD FILE /home/zlj/udf_fraud.py;
drop table wl_analysis.t_hx_taobao_fraud_record_tmp;
create table wl_analysis.t_hx_taobao_fraud_record_tmp as
select
 user_id,
 dsn,
 feed_id,title,keywords
from 
(
SELECT
TRANSFORM(item_id, feed_id, user_id, dsn, root_cat_id, root_cat_name, shop_id, location,ds, title)
USING 'python udf_fraud.py'
AS (item_id, feed_id, user_id, dsn, root_cat_id, root_cat_name, shop_id, location,ds, title,flag_1, kw_1, flag_2, kw_2,fraud_score,keywords)
FROM wl_base.t_base_ec_record_dev_new
where root_cat_name  in ('生活电器','3C数码配件','本地化生活服务','网店/网络服务/软件','个性定制/设计服务/DIY','办公设备/耗材/相关服务','网络店铺代金/优惠券','网络设备/网络相关','智能设备')
and length(regexp_replace(title ,'IC卡|读卡器|考勤|留学|网站建站|网站建设|天翼|恒昌隆|腊肠|考试|t现货|律诗|食堂|考勤|指纹采集仪镜片|师傅|吊顶|简装|家暖|米线|资格考试|代购|套现货|套现代|提现货|套现做|手提现做|信用卡名片夹|黑户外|翼支付|电信|移动|联通|信用卡贴|信用卡式|信用卡包|信用卡大小|卡片优盘|名片夹|名片盒|卡片盒|卡套|卡盒|卡包|卡袋|卡夹|卡夾|钱包|钱夹|支持信用卡|卡式手机|户外|碎信用卡|卡片刀|名片刀|卡刀|信用卡折叠水果刀',''))=length(title)
)t  where fraud_score>=2  ;




drop table wl_analysis.t_hx_taobao_fraud_record_tbid_tel;
create table wl_analysis.t_hx_taobao_fraud_record_tbid_tel as
SELECT
t2.tel ,
 user_id,
 dsn,
 feed_id,title,keywords
from  wl_analysis.t_hx_taobao_fraud_record_tmp t1
join 
wl_link.t_zlj_uid_name t2 on t1.user_id =t2.tb  where tel is not null
;



-- select * from t_hx_taobao_fraud_record_tbid_tel limit 10;
--
--
-- select count(1)  from t_hx_taobao_fraud_record_tbid_tel