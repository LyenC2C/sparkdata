


-- 8385696

SELECT  count(1) from t_qqweibo_user_info where background1='大学'  and LENGTH(school1)>0  ;



-- 乳霜状  101388
-- 其他/other      549276
-- 睡眠免洗式      2931607
-- 水洗式  4611380
-- 冻胶状  135630
-- 撕拉式  1282975
-- 膏泥状  168845
-- 贴片式  1897530
select para,count(1) from (select paramap["面膜分类"]  as para  from  t_tianxiang_feed_item_tmp_id_map)t group by para


-- 混合型肤质      184189
-- 敏感性肤质      238125
-- 油性肤质        65431
-- 油性及混合性肤质        513579
-- 中性肤质        33593
-- 任何肤质        25088703
-- 干性、中性及混合性肌肤  93496
-- 干性、混合性及油性缺水皮肤      337237
-- 肌肤有暗疮、破损或炎症现象，请慎用.     2038
-- 中性、干性、油性及混合性肌肤，尤其适合于补水、干燥肌肤  142955
-- 中性、干性、油性及混合性肌肤，尤其适合缺水、干燥肌肤。  250643
-- 其它肤质        21519
-- 缺水干燥及需要补水的肌肤        45863
-- 一般肌肤、干燥缺水及粗糙暗沉肌肤        7845
-- 干性肤质        169301
-- 中性及干性肤质  541255
-- 干性及混合性肤质        19489
-- 晒后及肤色暗沉肌肤      3699
-- 中性、油性肤质  85951
-- 中性、干性、油性及混合性肌肤，肌肤暗黄、需要提亮肤色的肌肤      130932
-- 肤色暗沉干燥及需要提亮肤色的肌肤        41822
select para,count(1) from (select paramap["适合肤质"]  as para  from  t_tianxiang_feed_item_tmp_id_map)t group by para



SELECT * from
(
select para,count(1) as num
 from (select paramap["化妆品净含量"]  as para  from  t_tianxiang_feed_item_tmp_id_map)t group by para
 ) t2 order by num desc limit 100;