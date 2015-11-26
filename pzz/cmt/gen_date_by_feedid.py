#coding:utf-8
import sys

def BinarySearch(array,t):
    #print array
    low = 1
    height = len(array)
    while low < height-1:
        mid = (low+height)/2
        print height,low,mid
        if array[mid] < t:
            low = mid
        elif array[mid] > t:
            height = mid
        else:
            return mid
    return (low + height)/2


def gen_date(data_dic,value_dic,feedid):
    t = int(feedid)
    i = BinarySearch(value_dic,t)
    print "1:" + str(i)
    print str(len(value_dic))
    #return i
    if i == 0:
        return data_dic[0]
    if i == len(value_dic):
        return data_dic[len(value_dic)]
    else:
        a = abs(value_dic[i]-t)
        b = abs(value_dic[i+1]-t)
        #c = abs(value_dic[i+1]-t)
        dic = {
            a:i,
            b:i+1
            #c:i+1
        }
        #print dic
        return value_dic[dic[min(a,b)]]

#f_in:feedid \t date

def format_dic(f_in):
    data_dic = {}
    value_dic = {}
    i = 1
    for line in open(f_in,"r"):
        ls = line.strip().split("\t")
        data_dic[i] = ls[0]
        value_dic[i] = int(ls[1])
        i += 1
    return data_dic,value_dic

if __name__ == "__main__":
    data_dic,value_dic = format_dic("time.feedid.sort")
    feed = sys.argv[1]
    print "2:" + str(gen_date(data_dic,value_dic,feed))

'''
1111    20130101
2222    20130102
2224    20130103
3333    20130104
3555    20130105
4444    20130106
'''