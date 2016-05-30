
注册时间统计
1999    544
2000    3039
2001    6356
2002    11113
2003    86083
2004    1050620
2005    2585137
2006    4789702
2007    6071697
2008    10252632
2009    13515717
2010    19084003
2011    26518059
2012    37101293
2013    45785487
2014    50789132
2015    46739253
2016    1911634


select tm,count(1) as t  from (select split(regtime,'\\.')[0] as tm  from wlbase_dev.t_base_ec_tb_userinfo ) t group by tm ;


0       73221090
1       193080411

select alipay ,count(1) from t_base_ec_tb_userinfo group by alipay;



        235701730
内蒙古  314996
贵州    273125
广东    3889066
西藏    27248
台湾    29470
江西    699406
浙江    2867667
陕西    650537
海南    146039
上海    1908984
安徽    905974
湖南    934243
江苏    2845395
福建    1323332
河南    1131904
新疆    203799
甘肃    260385
辽宁    944247
宁夏    128359
山西    585204
黑龙江  599729
澳门    920
香港    8540
广西    706775
重庆    617672
青海    39967
四川    1184089
天津    514648
山东    1810005
湖北    1033405
云南    364541
吉林    404272
北京    2080815
河北    1165013
select loc,count(1) as t  from (select split(location,'\\s+')[0] as loc  from wlbase_dev.t_base_ec_tb_userinfo ) t group by loc ;



VIP等级0        124246189
VIP等级1        46219034
VIP等级2        56005734
VIP等级3        24985600
VIP等级4        12057040
VIP等级5        2642319
VIP等级6        145585

select verify,count(1) as t  from  wlbase_dev.t_base_ec_tb_userinfo   group by verify ;



NULL	613746
0	2444003
1	5240475
2	11825146
3	24029020
4	41231387
5	52281070
6	50175457
7	38397098
8	24722696
9	11415863
10	3338871
11	529331
12	49002
13	6357
14	1416
15	437
16	103
17	16
18	7
select loc,count(1) as t  from (select  CAST (log2(buycnt) as int) as loc  from wlbase_dev.t_base_ec_tb_userinfo ) t group by loc ;


CREATE TABLE wlservice.t_lzh_wine_yangjiusalemon
  AS
    SELECT
      y.item_id,
      y.title,
      SUBSTRING(y.ds, 0, 6) date,
      sum(y.day_sold) AS    num
    FROM
      (SELECT *
       FROM
         (SELECT
            item_id,
            brand_id,
            brand_name
          FROM t_base_ec_item_dev
          WHERE cat_id = '50008144') t1
         LEFT JOIN t_base_ec_item_daysale_dev t2
           ON t1.item_id = t2.item_id) y
    GROUP BY y.item_id, y.title, SUBSTRING(y.ds, 0, 6)
    ORDER BY brand_id DESC