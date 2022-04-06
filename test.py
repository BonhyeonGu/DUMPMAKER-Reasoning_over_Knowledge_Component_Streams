import os
from multiprocessing import Process, Lock, freeze_support, Manager
from multiprocessing.managers import DictProxy
import h5py as h5
import ast
from p1title import Title

target = h5.File('./Dump0401.hdf5', 'r')
data = target['/Titles/idToTitle']
print(data[70355007].decode('utf-8'))
data = target['/Titles/titleToId']
print(data)
d = ast.literal_eval(data)