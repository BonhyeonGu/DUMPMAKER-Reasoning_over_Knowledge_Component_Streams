import h5py as h5
import numpy as np

MAXINDEX = 16331570
MAXID = 70355180
print("Start..")
target = h5.File("./Dump0413.hdf5", 'w')
target.create_group('Titles')
data = target['/Titles/idToTitle']

emptyArrIndex = 0
emptyArr = np.full(MAXINDEX, -1, dtype=np.int32)
for i in range(0, MAXID):
    if data[i].decode('utf-8') == '???':
        emptyArr[emptyArrIndex] = i
        emptyArrIndex += 1
np.save('03EmptyIDs.npy')
target.close()