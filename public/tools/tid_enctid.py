#coding:utf-8
#author@shibin
#2016.04.20

from itertools import product
from itertools import chain
import sys

table = {
    0:["vm", "vF", "vC", "vG", "Mm", "MF", "MC", "MG", "Om", "OF"],
    1:["N", "H", "I", "v", "Q", "8", "x", "c", "g", "k"],
    2:["W", "Y", "y", "G", "0", "b", "L", "u", "4", "S"],
    -1:["vN", "vQ", "vg", "vW", "MN", "MQ", "Mg", "MW",  "ON", "OQ"]
}

chunksf = lambda data,n: [data[i:i+n] for i in range(0, len(data), n)]


def dec_chunk(chunk):
    chunk = map(int,chunk)    
    clen = len(chunk)

    r = ''
    for i in range(clen):
        c = chunk[i]
        index = i
        if clen == 1:
            index = -1
        r += table[index][c]

    pad = (3 - clen)*'T'
    unit = r + pad
    return unit


def load_reflect():
    chrs = map(str,range(10))
    chrtids = reduce(lambda x,y:chain(x,y),[list(product(chrs,repeat = i)) for i in range(1,4)])
    tids = map(lambda x:''.join(x),chrtids)

    ref = {}
    for tid in tids:
        enc = dec_chunk(tid)
        ref[enc] = tid
        ref[tid] = enc
    return ref


reflect = load_reflect()


def get_enctid(tid,head = 'U'):
    global reflect    
    tids = chunksf(tid,3)
    enc = ''.join(map(lambda t:reflect[t],tids))
    ucode = head + enc
    return ucode


def dec_enctid(enctid):
    global reflect
    encs = chunksf(enctid[1:],4)
    tid = ''.join(map(lambda p:reflect[p],encs))
    return tid    

        
if __name__ == "__main__":
    if len(sys.argv) == 1:
        enctid = 'UvCIYOmxLvGcGvWTT'
        tid = dec_enctid(enctid)
        print(tid,enctid)
        tid = '2218663733'
        enctid = get_enctid(tid)
        print(tid,enctid)
    if len(sys.argv) == 2:        
        instr = sys.argv[1]
        if instr.isdigit():
            tid = instr
            enctid = get_enctid(tid)
            print(tid,enctid)
        if instr.startswith('U'):
            enctid = instr
            tid = dec_enctid(enctid)
            print(tid,enctid)
    
