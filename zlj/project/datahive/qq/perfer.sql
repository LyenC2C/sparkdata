SELECT *

FROM t_zlj_ec_userbuy_qq
WHERE length(shengxiao) > 1
GROUP BY
  shengxiao;