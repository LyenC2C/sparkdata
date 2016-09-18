
-- 关系数据中 相互专注并且都打通手机号的人
create table t_base_weibo_user_fri_check_18kw_data as
  SELECT t3.id1 ,t3.id2
  from
      t_base_uid_tmp   t4
     join  (
          SELECT  /*+ mapjoin(t2)*/ t1.id1 ,t1.id2
            from   t_base_uid_tmp t2 join
          t_base_weibo_user_fri_bi_friends  t1
              on t1.id1 =t2.id1  and  t2.ds='wid'

    )t3  on t3.id2 =t4.id1 and  t4.ds='wid'
;