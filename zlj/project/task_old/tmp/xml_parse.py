#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')


def node_text(node):
    result = ""
    for text in node.itertext():
        result = result + text
    return result


from lxml import etree
# tree = etree.parse("E:\\fsimg_0201.xml")#将xml解析为树结构
# root = tree.getroot()#获得该树的树根


import xml.etree.cElementTree as ET
tree = ET.ElementTree(file='E:\\fsimg_0201.xml')
root = tree.getroot()#获得该树的树根

for tag in root:
    for elem in root[0]:print elem.tag, elem.text


# for elem in tree.iter():
#     print elem.tag, elem.attrib



# for article in root:#这样便可以遍历根元素的所有子元素(这里是article元素)
#     print "元素名称：",article.tag#用.tag得到该子元素的名称
#     for field in article:#遍历article元素的所有子元素(这里是指article的author，title，volume，year等)
#         # print type(field.tag),type(root)
#         if 1==1:
#             for field in article:#遍历article元素的所有子元素(这里是指article的author，title，volume，year等)
#                  print field.tag,":",field.text#同样地，用.tag可以得到元素的名称，而.text可以得到元素的内容
#         else :
#             print field.tag,":",field.text#同样地，用.tag可以得到元素的名称，而.text可以得到元素的内容