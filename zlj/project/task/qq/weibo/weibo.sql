


-- 8385696

SELECT  count(1) from t_qqweibo_user_info where background1='��ѧ'  and LENGTH(school1)>0  ;



-- ��˪״  101388
-- ����/other      549276
-- ˯����ϴʽ      2931607
-- ˮϴʽ  4611380
-- ����״  135630
-- ˺��ʽ  1282975
-- ����״  168845
-- ��Ƭʽ  1897530
select para,count(1) from (select paramap["��Ĥ����"]  as para  from  t_tianxiang_feed_item_tmp_id_map)t group by para


-- ����ͷ���      184189
-- �����Է���      238125
-- ���Է���        65431
-- ���Լ�����Է���        513579
-- ���Է���        33593
-- �κη���        25088703
-- ���ԡ����Լ�����Լ���  93496
-- ���ԡ�����Լ�����ȱˮƤ��      337237
-- �����а������������֢����������.     2038
-- ���ԡ����ԡ����Լ�����Լ����������ʺ��ڲ�ˮ�����Ａ��  142955
-- ���ԡ����ԡ����Լ�����Լ����������ʺ�ȱˮ�����Ａ����  250643
-- ��������        21519
-- ȱˮ���Ｐ��Ҫ��ˮ�ļ���        45863
-- һ�㼡��������ȱˮ���ֲڰ�������        7845
-- ���Է���        169301
-- ���Լ����Է���  541255
-- ���Լ�����Է���        19489
-- ɹ�󼰷�ɫ��������      3699
-- ���ԡ����Է���  85951
-- ���ԡ����ԡ����Լ�����Լ������������ơ���Ҫ������ɫ�ļ���      130932
-- ��ɫ�������Ｐ��Ҫ������ɫ�ļ���        41822
select para,count(1) from (select paramap["�ʺϷ���"]  as para  from  t_tianxiang_feed_item_tmp_id_map)t group by para



SELECT * from
(
select para,count(1) as num
 from (select paramap["��ױƷ������"]  as para  from  t_tianxiang_feed_item_tmp_id_map)t group by para
 ) t2 order by num desc limit 100;