import numpy as np
import pickle
MAXINDEX = 16331570
MAXID = 70355180
print("Start..")

f = open("./Raw/01Titles", 'r', encoding='UTF-8')
lines = f.read().split('\n')
f.close()

print("Page split complite..")

linesSize = len(lines) - 1

titleToId = dict()
IdToTitle = np.full(MAXID, '???', dtype=object)

lastID = -1
emptyArrIndex = 0
emptyArr = np.full(MAXID, -1, dtype=np.int32)

for i in range(linesSize):
    print("\rProcess : p1 : %.4f%%" % ((float(i)/linesSize) * 100), end="")

    lineTemp = lines[i].split('=')
    id = int(lineTemp[1].split('_')[0])
    title = lineTemp[4].encode('utf-8')

    for j in range(lastID + 1, id):
        emptyArr[emptyArrIndex] = j
        emptyArrIndex += 1

    lastID = id
    IdToTitle[id] = title
    titleToId[title] = id
#g.create_dataset('titleToId', data=np.array([str(d)] ,dtype=object))

np.save('Raw/99IdToTitle.npy', IdToTitle)

with open("./Raw/99TitleToId.pkl", 'wb') as f:
        pickle.dump(titleToId, f)
np.save('Raw/99EmptyIDs.npy', emptyArr)
input("\nAll complite..")