LOAD DATA   INPATH '/user/zlj/match/browse_group1.csv' OVERWRITE INTO TABLE wlfinance.t_zlj_base_match PARTITION (ds='rong360_browse_group1') ;

SELECT
COUNT(1)
 from  t_zlj_base_match t1 join wlbase_dev.t_base_yhhx_model_tel t2 on t1.ds='rong360_test_1111' and t1.tel=t2.uid ;


SELECT
   user_id  ,concat_ws(',', collect_set(concat_ws(':',tview,tcount)))
   from
(
SELECT
tel as user_id ,
id1 as tview,
id2 as view_s ,
id3 as tcount
from wlfinance.t_zlj_base_match where ds='rong360_browse_group1'
)t group by user_id ;