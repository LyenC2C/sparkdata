
注册时间统计

1999    788
2000    4427
2001    9240
2002    16263
2003    155749
2004    1903755
2005    4658765
2006    8775658
2007    11173746
2008    19022550
2009    24986334
2010    36135384
2011    49749215
2012    52924072
2013    82925560
2014    77751008
2015    69651814
select tm,count(1) as t  from (select split(regtime,'\\.')[0] as tm  from wlbase_dev.t_base_ec_tb_userinfo where ds=20160530 ) t group by tm ;



0       126294034
1       313550299

select alipay ,count(1) from t_base_ec_tb_userinfo  where ds=20160530 group by alipay;



        384301033
台湾    50365
安徽    1633274
广西    1277271
吉林    729035
云南    664170
海南    265526
澳门    1706
香港    15685
福建    2396378
宁夏    231354
北京    3781184
浙江    5252489
广东    7001249
贵州    494924
湖北    1879097
陕西    1179811
江西    1261029
江苏    5208176
新疆    371446
山西    1051518
河北    2086289
辽宁    1723157
青海    72539
天津    940920
重庆    1122886
湖南    1689462
黑龙江  1087111
四川    2149341
甘肃    470080
河南    2025348
上海    3536991
西藏    48943
内蒙古  567621
山东    3276921
select loc,count(1) as t  from (select split(location,'\\s+')[0] as loc  from wlbase_dev.t_base_ec_tb_userinfo where ds=20160530 ) t group by loc ;



VIP等级0        187221425
VIP等级1        77572911
VIP等级2        100048017
VIP等级3        46609978
VIP等级4        23042921
VIP等级5        5074175
VIP等级6        274901

select verify,count(1) as t  from  wlbase_dev.t_base_ec_tb_userinfo  where ds=20160530   group by verify ;




NULL    1145999
0       2164666
1       5006111
2       13015271
3       31619207
4       63815988
5       88878206
6       88695176
7       69684736
8       46287897
9       21920048
10      6473953
11      1026695
12      94700
13      12033
14      2657
15      777
16      176
17      28
18      14
select loc,count(1) as t  from (select  CAST (log2(buycnt) as int) as loc  from wlbase_dev.t_base_ec_tb_userinfo where ds=20160530 ) t group by loc ;


