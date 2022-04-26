import os
from multiprocessing import Process, Lock, freeze_support, Manager
from multiprocessing.managers import DictProxy
import h5py as h5
import numpy as np

class Anker:
    def __init__(self, target:h5.File, titles):
        self.anker = anker
        self.
        self.run()

    def run()
        g = anker['Anker']
        arr = g.create_dataset('idToTitle', data=np.full(70352000, '?', dtype=np.string_))
        f = open(titles, 'r', encoding='UTF-8')
        lines = f.read().split('<')
        linesSize = len(lines) - 1
        for i in range(linesSize):
            print("\rcomplete %.4f" % ((float(i)/linesSize) * 100), end="")
            lineTemp = lines[i].split('=')
            id = int(lineTemp[1].split('_')[0])
            title = lineTemp[4].encode('utf-8')
            arr[id] = title
        f.close()
        return