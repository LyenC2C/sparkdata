#coding:utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark import SparkContext

sc = SparkContext(appName="Bigram")
clean_words = sc.broadcast([u'~',u'`',u'〈',u'—',u'：',u'!',u'！',u'@',u'#',u'$',u'%',u'^',u'&',u'*',u'(',u')',u'_',u'-',u'+',u'=',u'[',u']',u'{',u'}',u'\\',u'|',u'｜',u'"',u'．',u'·',u'•',u'●',u'◆',u'▪',u'❤',u'↑',u'↓',u'★',u'♢',u'「',u'」',u'✚',u'1.',u'..',u'......',u'、',u'\'',u'”',u'“',u'‘',u'’',u'；',u'：',u':',u';',u'/',u'／',u'?',u'？',u'？',u'。',u'.',u'>',u'<',u'《',u'》',u'，',u',u',u'【',u'】',u'（',u'）',u'＂',u'＂',u'［',u'',u'——',u'--',u'...',u'。。。',u'…',u'，',u'▲',u'1',u'2',u'3',u'4',u'5',u'6',u'7',u'8',u'9',u'0']).value

def parse(line):
    if line.strip()=='':
        return None
    items = line.strip().split('\001')
    title = items[-1].replace(' ','')
    title = [w for w in title if w not in clean_words]
    bi_words = []
    if len(title)>2:
        for i in xrange(len(title)-1):
            bi_words.append(title[i]+title[i+1])
    else: bi_words.append(''.join(title))
    items.append(' '.join(bi_words))
    return '\t'.join([items[0],items[1],items[-1]])
    # return '\t'.join(items)

spam = sc.textFile("/hive/warehouse/wl_analysis.db/t_lt_record_base_title_all_label/000000_0")
spam.map(lambda x:parse(x)).filter(lambda x:x != None).saveAsTextFile("/user/lt/BigramAll")