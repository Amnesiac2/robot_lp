# -*- coding： utf-8 -*-

'''
随机抽取一个文本文件中的1000行数据，每一行被抽中的概率相同
'''


import numpy as np
import random

file = open("text.txt", "r")
text = []

def read(file):       # 蓄水池算法
    for i in range(1000):
        text[i] = file.readline()
    count = 1001
    while(1):
        line = file.readline()
        if not line:
            break
        else:
            randin = random.random()
            if randin > 1000/count:
                randout = random.randrange(0, 1000, 1)
                text[randout] = line
        count += 1
    return text



