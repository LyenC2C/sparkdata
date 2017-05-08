#coding:utf-8

import sys

def transform_df_fileds(content):
    if isinstance(content, unicode):
        res = content.encode("utf-8").decode("latin-1").encode("iso-8859-1").decode("utf-8")
    elif isinstance(content, str):
        res = content.decode("latin-1").encode("iso-8859-1").decode("utf-8")
    else:
        res = str(content)
    return res

for line in sys.stdin:
    print transform_df_fileds(line.strip())
