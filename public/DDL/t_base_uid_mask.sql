
create table t_base_uid_mask as
SELECT
*
from
t_base_uid_tmp where ds='uid_mark' ;