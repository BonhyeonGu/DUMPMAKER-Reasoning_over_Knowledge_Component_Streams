import h5py as h5
import numpy as np
MAXID = 70355200
print("Start..")
target = h5.File("./Dump0406.hdf5", 'w')
target.create_group('Titles')
g = target['Titles']
arr = g.create_dataset('idToTitle', data=np.full(MAXID, '???',dtype=object))
f = open("./Raw/01Titles", 'r', encoding='UTF-8')
lines = f.read().split('\n')
print("Page split complite..")
linesSize = len(lines) - 1
d = dict()
for i in range(linesSize):
    print("\rProcess : p1 : %.4f%%" % ((float(i)/linesSize) * 100), end="")
    lineTemp = lines[i].split('=')
    id = int(lineTemp[1].split('_')[0])
    title = lineTemp[4].encode('utf-8')
    arr[id] = title
    d[title] = id
#g.create_dataset('titleToId', data=np.array([str(d)] ,dtype=object))
g.attrs.create('titleToId', data=str(d))
f.close()