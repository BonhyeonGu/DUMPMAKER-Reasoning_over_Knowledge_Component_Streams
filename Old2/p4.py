import h5py as h5
import ast
import numpy as np

target = h5.File("./Dump0413.hdf5", 'r+')
titleToId_dict = ast.literal_eval(target['/Titles'].attrs['titleToId'])
idToTitle = target['/Titles/idToTitle']

fEmptyMap = open("./Raw/04EmptyMap", 'r', encoding='UTF-8')
emptyMap = ast.literal_eval(fEmptyMap.read())
fEmptyMap.close()

fEmptySize = open("./Raw/04EmptySize", 'r', encoding='UTF-8')
emptySize = int(fEmptySize.read())
fEmptySize.close()

emptyIDs = np.load('03EmptyIDs.npy')

print("Load complite..")
for i in range(0, emptySize):
    addID = emptyIDs[i]
    addTitle = emptyMap[addID]
    titleToId_dict[addTitle] = addID
target.close()