#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')


comm='---'
def clean(s): #整理一下数据，有些不规范的地方
    if u'“/s' not in s:
        return s.replace(u' ”/s', comm)
    elif u'”/s' not in s:
        return s.replace(u'“/s ', comm)
    elif u'‘/s' not in s:
        return s.replace(u' ’/s', comm)
    elif u'’/s' not in s:
        return s.replace(u'‘/s ', comm)
    else:
        return s


u'。|，|、|；|：|？|！|…… '

rs=[]
import numpy as np
import pandas as pd
import  re
from itertools import chain
from keras.utils import np_utils
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
from keras.layers.embeddings import Embedding
from keras.layers.recurrent import LSTM
from sklearn.cross_validation import train_test_split
'''
line '本/S  报/S  南/B  昌/E  讯/S  记/B  者/E  鄢/B  卫/M  华/E  报/B  道/E  ：/S  １/B  ７/E  日/S  上/B  午/E'

return [ [u'/S', u'\uff11/B', u'\uff17/E', u'\u65e5/S', u'\u4e0a/B', u'\u5348/E'] ]
'''
def clean(line):
    rs=[]
    for i in re.split(u'。|，|、|；|：|？|！|…',line):
        rs.append(i.split())
    return rs
s=open('msr_train.txt').read().decode('utf-8')
s=s.split(u'\n')

char_corpus=[]
for line in s:
    char_corpus.extend(clean(line))

char_corpus_fiter=filter(lambda x:len(x)>3 and len(x)<maxlen,char_corpus)
char_txt=[]
char_lable=[]
char_txt=filter ( lambda x: len(x)>0 , map(lambda x: [w.split('/')[0] for w in x if  len(w.split('/')[0])>0], char_corpus_fiter ))
char_lable=filter ( lambda x: len(x)>0 , map(lambda x: [w.split('/')[1].upper() for w in x if  len(w.split('/')[0])>0], char_corpus_fiter ))

d = pd.DataFrame(index=range(len(char_txt)))
d['data'] = char_txt
d['label'] = char_lable
maxlen=32
tag = pd.Series({'S':0, 'B':1, 'M':2, 'E':3})

chars = [w for line in char_txt for w in line ]
chars = pd.Series(chars).value_counts()
chars[:] = range(1, len(chars)+1)

from keras.utils import np_utils
d['x'] = d['data'].apply(lambda x: np.array(chars[x].tolist()+[0]*(maxlen-len(x))))
d['y'] = d['label'].apply(lambda x: np.array(map(lambda y:np_utils.to_categorical(y,4), tag[x].reshape((-1,1)))+[np.array([[0,0,0,0]])]*(maxlen-len(x))))

# d['label'].tail(1).apply(lambda x: np.array(map(lambda y:np_utils.to_categorical(y,4), tag[x].reshape((-1,1)))+[np.array([[0,0,0,0]])]*(maxlen-len(x))))
#
# d['label'].tail(1).apply(lambda x:   tag[x] ).reshape((-1,1)))

word_size = 128
maxlen = 32
from keras.layers import Dense, Embedding, LSTM, TimeDistributed, Input, Bidirectional
from keras.models import Model

train_x=np.array(d['x'].tolist())
train_y=np.array(d['y'].tolist()).reshape((-1,maxlen,4))
batch_size = 1024
def bulid_lstm_model() :
    sequence = Input(shape=(maxlen,), dtype='int32')
    embedded = Embedding(input_dim=len(chars)+1, output_dim=word_size, input_length=maxlen, mask_zero=True)(sequence)
    blstm = Bidirectional(LSTM(64, return_sequences=True), merge_mode='sum')(embedded)
    blstm1 = Bidirectional(LSTM(64, return_sequences=True), merge_mode='sum')(blstm)
    output = TimeDistributed(Dense(4, activation='softmax'))(blstm1)
    model = Model(input=sequence, output=output)
    model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
    history = model.fit(train_x ,train_y, batch_size=batch_size, nb_epoch=1)



# sequence = Input(shape=(maxlen,), dtype='int32')
# embedded = Embedding(input_dim=len(chars)+1, output_dim=word_size, input_length=maxlen, mask_zero=True)(sequence)
# lstm1 =  LSTM(100, return_sequences=True)(embedded)
# lstm2=LSTM(100, return_sequences=True)(lstm1)
# output = TimeDistributed(Dense(4, activation='softmax'))(lstm2)
# model = Model(input=sequence, output=output)
# model.compile(loss='categorical_crossentropy', optimizer='adamax', metrics=['accuracy'])
# history = model.fit(train_x ,train_y, batch_size=batch_size, nb_epoch=3)

from keras.models import model_from_json

with open('/home/hadoop/code/data/icwb2-data/training/zlj_lstm_seg.model', 'r') as json_file:
    model_json = json_file.read()
    model = model_from_json(model_json)
    model.load_weights("/home/hadoop/code/data/icwb2-data/training/zlj_lstm_seg.weight")
#转移概率，单纯用了等概率
zy = {'be':0.5,
      'bm':0.5,
      'eb':0.5,
      'es':0.5,
      'me':0.5,
      'mm':0.5,
      'sb':0.5,
      'ss':0.5
     }


zy = {i:np.log(zy[i]) for i in zy.keys()}

def viterbi(nodes):
    paths = {'b':nodes[0]['b'], 's':nodes[0]['s']}
    for l in range(1,len(nodes)):
        paths_ = paths.copy()
        paths = {}
        for i in nodes[l].keys():
            nows = {}
            for j in paths_.keys():
                if j[-1]+i in zy.keys():
                    nows[j+i]= paths_[j]+nodes[l][i]+zy[j[-1]+i]
            # print i,nows.values()
            k = np.argmax(nows.values())
            paths[nows.keys()[k]] = nows.values()[k]
    return paths.keys()[np.argmax(paths.values())]
def simple_cut(s):
    if s:
        r = model.predict(np.array([chars[[i for i in s]].tolist()+[0]*(maxlen-len(s))]), verbose=False)[0][:len(s)]
        r = np.log(r)
        nodes = [dict(zip(['s','b','m','e'], i[:4])) for i in r]
        t = viterbi(nodes)
        words = []
        for i in range(len(s)):
            if t[i] in ['s', 'b']:
                words.append(s[i])
            else:
                words[-1] += s[i]
        return words
    else:
        return []

not_cuts = re.compile(u'([\da-zA-Z ]+)|[。，、？！\.\?,!]')
def cut_word(s):
    result = []
    j = 0
    for i in not_cuts.finditer(s):
        result.extend(simple_cut(s[j:i.start()]))
        result.append(s[i.start():i.end()])
        j = i.end()
    result.extend(simple_cut(s[j:]))
    return result

for line in open('/home/hadoop/code/data/icwb2-data/gold/msr_test.utf8'):
    words=cut_word(line)
    print '\t'.join(words)


# model_seg_v2 = Sequential()
# model_seg_v2.add(Embedding(input_dim=len(chars)+1, output_dim=word_size, input_length=maxlen, mask_zero=True))
# model_seg_v2.add(LSTM(output_dim=64, return_sequences=True))   # 中间层 lstm
# model_seg_v2.add(LSTM(output_dim=64, return_sequences=False))
# model_seg_v2.add(Dropout(0.5))
# model_seg_v2.add(Dense(4))    # 输出 4 个结果对应 BMES
# model_seg_v2.add(Activation('softmax'))
# model_seg_v2.compile(loss='categorical_crossentropy', optimizer='adam', metrics=["accuracy"])
# history = model_seg_v2.fit(train_x ,train_y, batch_size=batch_size, nb_epoch=1)
#
# x=[ w.tolist()  for line in train_x for w in line ]
# y=[ w.tolist()  for line in train_y for w in line ]
# history = model_seg_v2.fit(x ,y, batch_size=batch_size, nb_epoch=1)
