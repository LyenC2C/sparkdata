#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
# from pyquery import PyQuery as pq

sc = SparkContext(appName="weibo_user_info")

def f():
    ob = json.loads(line.strip())
    id = ob.get("id")
    idstr = ob.get("idstr")
    class_r = ob.get("class")
    screen_name = ob.get("screen_name")
    name = ob.get("name")
    province = ob.get("province")
    city = ob.get("city")
    location = ob.get("location")
    description = ob.get("description")
    profile_url = ob.get("profile_url")
    weihao = ob.get("weihao")
    gender = ob.get("gender")
    followers_count = ob.get("followers_count")
    friends_count = ob.get("friends_count")
    pagefriends_count = ob.get("pagefriends_count")
    statuses_count = ob.get("statuses_count")
    favourites_count = ob.get("favourites_count")
    created_at = ob.get("created_at")
    allow_all_act_msg = ob.get("allow_all_act_msg")
    geo_enabled = ob.get("geo_enabled")
    verified = ob.get("verified")
    verified_type = ob.get("verified_type")
    remark = ob.get("remark")
    status = ob.get("status") #用户的最近一条微博信息字段
    txt_created_at = status.get("created_at")
    txt_id = status.get("id")
    txt_mid = status.get("mid")
    txt_idstr = status.get("idstr")
    txt_text = status.get("text")
    txt_original_pic = status.get("original_pic")
    txt_geo = status.get("geo")
    txt_reposts_count = status.get("reposts_count")
    txt_comments_count = status.get("comments_count")
    txt_attitudes_count = status.get("attitudes_count")
    txt_source = status.get("source")



