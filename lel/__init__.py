import datetime
def timestamp2string(timeStamp):
    try:
        d = datetime.datetime.fromtimestamp(timeStamp)
        str1 = d.strftime("%Y%m%d")
        return str1
    except Exception as e:
        print e
        return ''
print timestamp2string(float(u'1490767197'))