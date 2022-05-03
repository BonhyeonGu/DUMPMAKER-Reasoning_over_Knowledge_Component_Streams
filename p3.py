import pickle
import ast
import numpy as np

idToTitle = np.load('./99IdToTitle.npy', allow_pickle=True)

with open('./99TitleToId.pkl','rb') as f:
    titleToId = pickle.load(f)

with open('./99EmptyIdToTitle.pkl','rb') as f:
    emptyIdToTitle = pickle.load(f)
c = 0
for i in range(len(idToTitle)):
    if idToTitle[i] == '???':
        if i in emptyIdToTitle:
            b = emptyIdToTitle[i]
            idToTitle[i] = b
            titleToId[b] = i
            c +=1
        else:
            break
print(c)
print(titleToId[b'Gundam_0080'])
np.save('./99ComIdToTitle.npy', idToTitle)
with open('./99ComTittleToId.pkl', 'wb') as f:
        pickle.dump(titleToId, f)