
ע��ʱ��ͳ��

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
̨��    50365
����    1633274
����    1277271
����    729035
����    664170
����    265526
����    1706
���    15685
����    2396378
����    231354
����    3781184
�㽭    5252489
�㶫    7001249
����    494924
����    1879097
����    1179811
����    1261029
����    5208176
�½�    371446
ɽ��    1051518
�ӱ�    2086289
����    1723157
�ຣ    72539
���    940920
����    1122886
����    1689462
������  1087111
�Ĵ�    2149341
����    470080
����    2025348
�Ϻ�    3536991
����    48943
���ɹ�  567621
ɽ��    3276921
select loc,count(1) as t  from (select split(location,'\\s+')[0] as loc  from wlbase_dev.t_base_ec_tb_userinfo where ds=20160530 ) t group by loc ;


# ���ѵȼ�

VIP�ȼ�0        187221425
VIP�ȼ�1        77572911
VIP�ȼ�2        100048017
VIP�ȼ�3        46609978
VIP�ȼ�4        23042921
VIP�ȼ�5        5074175
VIP�ȼ�6        274901

select verify,count(1) as t  from  wlbase_dev.t_base_ec_tb_userinfo  where ds=20160530   group by verify ;



# �������
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


select * from t_zlj_user_tag_join_t where length(cat_tags)>20 and  cat_tags like "%�г�%"   limit  1000;