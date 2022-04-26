import os
from multiprocessing import Process, Lock, freeze_support, Manager
from multiprocessing.managers import DictProxy
import h5py as h5
import numpy as np

class Title:
    def __init__(self, target:h5.File, titles):
        g = target['Titles']
        arr = g.create_dataset('idToTitle', data=np.full(70355200, '???',dtype=object))
        f = open(titles, 'r', encoding='UTF-8')
        lines = f.read().split('\n')
        linesSize = len(lines) - 1
        d = dict()
        for i in range(linesSize):
            print("\rProcess : 1/2 : p1title : %.4f%%" % ((float(i)/linesSize) * 100), end="")
            lineTemp = lines[i].split('=')
            id = int(lineTemp[1].split('_')[0])
            title = lineTemp[4].encode('utf-8')
            arr[id] = title
            d[title.encode('utf-8')] = id
        g.create_dataset('titleToId', data=np.array([str(d)] ,dtype=object))
        f.close()
        return