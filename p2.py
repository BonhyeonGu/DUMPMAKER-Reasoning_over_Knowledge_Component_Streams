import h5py as h5
import numpy as np

target = h5.File("./Dump0406.hdf5", 'w')
target.create_group('Titles')
g = target['Titles']
arr = g.create_dataset('idToTitle', data=np.full(70355200, '???',dtype=object))
f = open("./Raw/02Ankers", 'r', encoding='UTF-8')
pages = f.read().split('<')
for i in range(1, len(pages)):
    print("\rProcess : 1/2 : p1title : %.4f%%" % ((float(i)/linesSize) * 100), end="")
    
#g.create_dataset('titleToId', data=np.array([str(d)] ,dtype=object))
g.attrs.create('titleToId', data=str(d))
f.close()