__author__ = 'wrt'
# coding
import sys
import json

for line in sys.stdin:
    ss = line.strip().split('\t')
    item_id = ss[1]
    zhengwen = ""
    for ln in ss[2:]:
        zhengwen += ln
    ob = json.loads(zhengwen)
    location = ob["itemInfoModel"]["location"].encode('utf-8')
    chandi = "-"
    if ob.has_key("props"):
        for ln in ob["props"]:
            if ln["name"] == "产地":
                chandi = ln["value"].encode('utf-8')
    print item_id + "\001" + location + "\001" + chandi