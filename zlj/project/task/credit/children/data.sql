


CREATE TABLE t_zlj_credit_children_feed_data AS
SELECT *
FROM t_base_ec_item_feed_dev
WHERE content LIKE '%����%' or content LIKE '%����%'  or content LIKE '%Ů��%'  or   content LIKE '%Ӥ��%';