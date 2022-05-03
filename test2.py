import pickle
import numpy as np

idToTitle = np.load('./99ComIdToTitle.npy', allow_pickle=True)

c = 0
for title in range(len(idToTitle), 0, -1):
    if title != '???':
        print(len(idToTitle)-c)
        break
    c += 1