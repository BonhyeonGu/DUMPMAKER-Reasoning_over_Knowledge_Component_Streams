import numpy as np
import pickle
from multiprocessing import Process, freeze_support, Manager, Lock
from multiprocessing.managers import DictProxy, ListProxy
import gc

def splitList(lis, splitCount):
    if len(lis) <= splitCount:
        return lis
    ret = []
    size = len(lis) // splitCount
    tmp = []
    idx = 0
    for e in lis:
        tmp.append(e)
        idx += 1
        if idx % size == 0:#0
            ret.append(tmp)
            tmp = []
    if len(tmp) != 0:
        ret.append(tmp)
    return ret

def unproxy_dict(dict_proxy):
        return {k: (dict(v) if isinstance(v, DictProxy) else v)
            for k, v in dict_proxy.items()}

def unproxy_list(list_proxy):
        return {(list(v) if isinstance(v, ListProxy) else v)
            for  v in list_proxy}

def sub(ids, arr:list, arrRevDict:dict, arrRevDictLock):
    for id in ids:
        ret = []
        for x in arr:
            if x[1] == id:
                ret.append(x[2])
        if len(ret) != 0:
            arrRevDictLock.acquire()
            arrRevDict[id] = ret
            arrRevDictLock.release()
    return

if __name__ == '__main__':
    freeze_support()
    m = Manager()
    arrRevDict = m.dict()
    arrRevDictLock = Lock()
    with open('./99Arr.pkl','rb') as f:
        arr = pickle.load(f)
    titleToId = np.load('./99ComTittleToId.pkl', allow_pickle=True)
    ids = list(titleToId.values())
    del(titleToId)
    #ids = idToTitle[:100]
    idss = splitList(ids, 2)
    del(ids)
    print("Load complite..")
    gc.collect()
    print("GC complite..")

    print("Start MultiProcess")
    pros = []
    i = 1
    for ids in idss:
        print("\rProcess : p4 : %.4f%%" % ((float(i)/len(idss)) * 100), end="")
        pro = Process(target=sub, args=(ids, arr, arrRevDict, arrRevDictLock, ))
        pro.daemon = True
        pro.start()
        pros.append(pro)
        i += 1

    print('\nWait MultiProcess')
    i = 1
    for pro in pros:
        pro.join()
        print("\rProcess : p4 : %.4f%%" % ((float(i)/len(pros)) * 100), end="")
        i += 1

    print('\nUnproxy..')
    arrRevDict = unproxy_dict(arrRevDict)
    print('Save..')
    with open('./99ArrRevDict', 'wb') as f:
        pickle.dump(arrRevDict, f)
    input("All Complite..")