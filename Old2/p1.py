import h5py as h5
import numpy as np
MAXINDEX = 16331570
MAXID = 70355180
print("Start..")
target = h5.File("./Dump0413.hdf5", 'w')
target.create_group('Titles')
g = target['Titles']

#emptyArr = np.full(MAXINDEX, -1, dtype=np.int32)
#emptyArrIndex = 0
arr = g.create_dataset('idToTitle', data=np.full(MAXID, '???',dtype=object))

f = open("./Raw/01Titles", 'r', encoding='UTF-8')
lines = f.read().split('\n')
f.close()
print("Page split complite..")

linesSize = len(lines) - 1
d = dict()

lastID = -1
emptyArrIndex = 0
emptyArr = np.full(MAXID, -1, dtype=np.int32)
for i in range(linesSize):
    print("\rProcess : p1 : %.4f%%" % ((float(i)/linesSize) * 100), end="")

    lineTemp = lines[i].split('=')
    id = int(lineTemp[1].split('_')[0])
    title = lineTemp[4].encode('utf-8')

    for j in range(lastID + 1, id):
        emptyArr[emptyArrIndex] = id
        emptyArrIndex += 1
    lastID = id

    arr[id] = title
    d[title] = id
#g.create_dataset('titleToId', data=np.array([str(d)] ,dtype=object))
g.attrs.create('titleToId', data=str(d))
np.save('Raw/03EmptyIDs.npy', emptyArr)
target.create_group('Ankers')
target.close()
input("All complite..")