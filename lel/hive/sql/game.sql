game

create table t_lel_game_20170322
as
select a.user_id from
(select distinct(user_id) as user_id from t_base_ec_record_dev_new where ds='true' and root_cat_name regexp '动漫|游戏' or title regexp '游戏点卡' and cast(dsn as int) > 20170301)a
left semi join 
(select tb_id from wl_base.t_base_user_profile_telindex where xianyu_birthday between 20 and 29)b
on a.user_id = b.tb_id
select * from t_base_ec_record_dev_new where ds='true' and root_cat_name regexp '动漫|游戏' or title regexp '游戏点卡' and cast(dsn as int) > 20170301



company:有爱互动、掌趣科技、网易游戏、腾讯游戏、触控科技

select * from wl_base.t_base_ec_record_dev_new where ds='true' and title regexp '手游.*阴阳师' and root_cat_name regexp '网游装备' limit 100








set hive.execution.engine=spark;
set hive.merge.sparkfiles=true;
create table wl_service.t_lel_game_withcompany_20170324
as
select distinct user_id,
regexp_extract(title,'全民挂机|萌萌爱消除|阿狸泡泡龙|我欲封灭|塔防先锋|豆比大作战|全民奇迹MU|不良人|大掌门|穿越吧！主公|封神小鲜肉|3D终极车神2|盟军防线|崩坏学院2|三国霸业|冠军足球|热血成吉思汗online|忍者公司|阴阳师|梦幻西游|大话西游3|百万亚瑟王|迷雾世界|大富翁9|元气战姬学院|功夫熊猫3|魂之幻影|大唐游仙记|异次元战姬|网易农场|混沌传奇|飞刀又见飞刀|天天酷跑|王者荣耀|龙之谷手游|猎鱼达人|天天德州|传奇世界手游|街头篮球|欢乐斗地主|星际火线|王牌NBA|捕鱼达人|大圣归来|秦时明月|亡灵杀手|全民天团|爱丽丝快跑|万千回忆|你好英雄|疾风勇者传|武尊|梦想城镇|舞动乾坤|乱斗堂|最游记物语|冰与火online|雷霆少女|天之劫|热血高校',0) as game,
case when title regexp '全民挂机|萌萌爱消除|阿狸泡泡龙|我欲封灭|塔防先锋|豆比大作战' then '有爱互动'
when title regexp '全民奇迹MU|不良人|大掌门|穿越吧！主公|封神小鲜肉|3D终极车神2|盟军防线|崩坏学院2|三国霸业|冠军足球|热血成吉思汗online|忍者公司' then '掌趣科技'
when title regexp '阴阳师|梦幻西游|大话西游3|百万亚瑟王|迷雾世界|大富翁9|元气战姬学院|功夫熊猫3|魂之幻影|大唐游仙记|异次元战姬|网易农场|混沌传奇|飞刀又见飞刀' then '网易游戏'
when title regexp '天天酷跑|王者荣耀|龙之谷手游|猎鱼达人|天天德州|传奇世界手游|街头篮球|欢乐斗地主|星际火线|王牌NBA' then '腾讯游戏'
when title regexp '捕鱼达人|大圣归来|秦时明月|亡灵杀手|全民天团|爱丽丝快跑|万千回忆|你好英雄|疾风勇者传|武尊|梦想城镇|舞动乾坤|乱斗堂|最游记物语|冰与火online|雷霆少女|天之劫|热血高校' then '触控科技'
end as company
from wl_base.t_base_ec_record_dev_new where ds='true' 
and root_cat_name regexp '网游装备' 
and cast(dsn as int) > 20161222
and title regexp '手游.*(全民挂机|萌萌爱消除|阿狸泡泡龙|我欲封灭|塔防先锋|豆比大作战|全民奇迹MU|不良人|大掌门|穿越吧！主公|封神小鲜肉|3D终极车神2|盟军防线|崩坏学院2|三国霸业|冠军足球|热血成吉思汗online|忍者公司|阴阳师|梦幻西游|大话西游3|百万亚瑟王|迷雾世界|大富翁9|元气战姬学院|功夫熊猫3|魂之幻影|大唐游仙记|异次元战姬|网易农场|混沌传奇|飞刀又见飞刀|天天酷跑|王者荣耀|龙之谷手游|猎鱼达人|天天德州|传奇世界手游|街头篮球|欢乐斗地主|星际火线|王牌NBA|捕鱼达人|大圣归来|秦时明月|亡灵杀手|全民天团|爱丽丝快跑|万千回忆|你好英雄|疾风勇者传|武尊|梦想城镇|舞动乾坤|乱斗堂|最游记物语|冰与火online|雷霆少女|天之劫|热血高校)' 


select t.user_id,concat_ws(',',collect_list(t.company)) as company,t.game from
( select  user_id,company,concat_ws(',',collect_list(game)) as game from t_lel_game_withcompany_20170324 group by user_id,company)t
 group by t.user_id,t.game


 