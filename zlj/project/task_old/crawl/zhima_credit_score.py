#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')


#!/usr/bin/python
# encoding:utf8
# author@bzy
# 2016.7.20

import json
import re
import requests
import sys
import threading
import time
from Queue import Queue
from pyquery import PyQuery as pq

reload(sys)
sys.setdefaultencoding('utf-8')

THREAD_NUM = 30
JSON_PATH = 'zhima_credit_score.txt'

end = False
pid_queue = Queue()
store_queue = Queue()


def crawl_zhima_credit_score(pid):

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Cache-Control': 'no-cache',
        'Content-Length': '16',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Cookie': 'smzdm_user_source=370BEA96DF98E3D97AC1C39D1D3BE72B; _ga=GA1.3.1827196057.1444552489; right_top_pop_describe_box=1; __jsluid=971ffbcd394ddca7ee236f7fb2d13d98; smzdm_wordpress_360d4e510beef4fe51293184b8908074=user%3A6718416034%7C1472692084%7Cd8946a35ca56bb3a4ab238ed508e0099; smzdm_wordpress_logged_in_360d4e510beef4fe51293184b8908074=user%3A6718416034%7C1472692084%7Cb4c6ddbc41d97619730160759e79db97; user-role-smzdm=subscriber; sess=M2E4OWR8MTQ3MjY5MjA4NHw2NzE4NDE2MDM0fGU5NzIwNGMyY2FkMDhjNTE3ZWY3YmU5Mjc0NmI5ZmI0; user=user%3A6718416034%7C6718416034; smzdm_user_view=310D9514DD6BD0D93057F32175B706AB; PHPSESSID=h1v8bg7f5c1vjfmlg4g3fuodm7; web_ab=A; wt3_eid=%3B999768690672041%7C2144464332500969538%232147001736700411622; wt3_sid=%3B999768690672041; _gat_UA-27058866-1=1; crtg_rta=criteo_D_728*90%3D1%3Bcriteo_300600zy03%3D1%3Bcriteo_300250zy02%3D1%3Bcriteo_300250zy01%3D1%3Bcriteo_300250fx04%3D1%3Bcriteo_300250fx03%3D1%3Bcriteo_300250fx02%3D1%3Bcriteo_300250fx01%3D1%3B; Hm_lvt_9b7ac3d38f30fe89ff0b8a0546904e58=1468308235,1468463280,1468563435,1469760254; Hm_lpvt_9b7ac3d38f30fe89ff0b8a0546904e58=1470022477; userId=6718416034; _ga=GA1.2.1827196057.1444552489',
        'Host': '2.smzdm.com',
        'Origin': 'http://2.smzdm.com',
        'Pragma': 'no-cache',
        'Proxy-Connection': 'keep-alive',
        'Referer': 'http://2.smzdm.com/p/' + pid + '/',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }

    data = {
        'second_id': pid
    }
    url = 'http://2.smzdm.com/details/initialize'

    try:
        r = requests.post(url, headers=headers, data=data, timeout=5)
    except Exception, e:
        return False, None

    return True, r.text


class MakePidThread(threading.Thread):
    global pid_queue, end

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        """
        Put all pages into page_queue.
        """
        for i in range(24000):
            if end:
                break

            pid_queue.put(str(i))


class GrabThread(threading.Thread):
    global pid_queue, end

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            if end:
                break

            one_pid = pid_queue.get()

            print threading.current_thread().name, ' ', one_pid

            result = (crawl_zhima_credit_score(one_pid))

            if result[0]:
                store_queue.put(result[1])
            else:
                pid_queue.put(one_pid)


class StoreThread(threading.Thread):
    global store_queue, JSON_PATH, end

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        f = open(JSON_PATH, 'a')
        while True:
            if store_queue.empty() and end:
                break

            item = store_queue.get()

            f.write(item)
            f.write('\n')

            f.flush()

        f.close()
        print 'Saving finished...\n'


if __name__ == '__main__':

    # result = crawl_zhima_credit_score('23517')
    # print result
    # exit(0)

    grab_th_list = []

    make_pid_th = MakePidThread()
    for i in range(THREAD_NUM):
        grab_th_list.append(GrabThread())
    store_th = StoreThread()

    make_pid_th.start()
    for thd_item in grab_th_list:
        thd_item.start()
    store_th.start()

    while True:
        key_in = raw_input("'exit' to stop:\n")
        if key_in == 'exit':
            end = True
            break
    print 'Waiting for all threads finishing...'
