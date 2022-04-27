from calendar import LocaleHTMLCalendar
import numpy as np
import os
from pkg_resources import compatible_platforms
from multiprocessing import Process, freeze_support, Manager, Lock
from multiprocessing.managers import DictProxy, ListProxy
import pickle
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

def sub(pages:str, emptyIDs:list, emptyIDsIdx, emptyLock, emptyTitleToId:dict, emptyIdToTitle:dict, arr:list, arrLock, ):
    for i in range(1, len(pages)):
        #print("\rProcess : p2 : %.4f%%" % ((float(i)/len(pages)) * 100), end="")
        lines = pages[i].split('\n')
        titleId = lines[0]#문자열임
        lineCount = len(lines) - 2#파일을 읽을때 읽어야 할 줄 개수, 햇갈리니까 주의

        for j in range(1, lineCount):
            words = lines[j].split('\t\t')
            ankerTitle = words[1].encode('utf-8')
            ankerLink_id = words[0]
            if ankerLink_id == 'CanNotFoundTitle':#문제사항!
                continue
            if ankerLink_id == 'ID_ERROR':#문제사항!!
                if ankerTitle in emptyTitleToId:
                    ankerLink_id = emptyTitleToId[ankerTitle]#이미 아래작업을 해준적이 있으면 그냥 꺼내온다
                else:
                    emptyLock.acquire()
                    popId = emptyIDs[emptyIDsIdx[0]]#배열에서 빼서 아이디 확보(정수)
                    emptyTitleToId[ankerTitle] = popId#확보된 아이디를 키값으로 타이틀 저장
                    emptyIdToTitle[popId] = ankerTitle
                    ankerLink_id = str(popId)#확보된 아이디를 목표 아이디로 결정
                    emptyIDsIdx[0] += 1
                    emptyLock.release()
            arrLock.acquire()
            arr.append((ankerTitle, ankerLink_id, titleId))
            arrLock.release()
        pages[i] = ''
    return

if __name__ == '__main__':
    freeze_support()
    LOCAL = './'
    print("Start..")
    f1 = open(LOCAL + '02Ankers_Merge_Plus', 'r', encoding='UTF-8')
    pages = f1.read().split('<')
    f1.close()
    pages = pages[1:]#0번째는 항상 버린다.
    pagess = splitList(pages, 12)
    print("Load f1 complite..")
    emptyIDs = np.load(LOCAL + '99EmptyIDs.npy')

    print("Start MultiProcess")
    pros = []
    m = Manager()
    arr = m.list()
    arrLock = Lock()
    emptyIDsIdx = m.list()
    emptyIDsIdx.append(0)
    emptyLock = Lock()
    emptyTitleToId = m.dict()
    emptyIdToTitle = m.dict()

    i = 1
    for pages in pagess:
        print("\rProcess : p2 : %.4f%%" % ((float(i)/len(pagess)) * 100), end="")
        pro = Process(target=sub, args=(pages, emptyIDs, emptyIDsIdx, emptyLock, emptyTitleToId, emptyIdToTitle, arr, arrLock, ))
        pro.daemon = True
        pro.start()
        pros.append(pro)
        i += 1
    print('\nWait MultiProcess')
    i = 1
    for pro in pros:
        pro.join()
        print("\rProcess : p2 : %.4f%%" % ((float(i)/len(pros)) * 100), end="")
        i += 1
    
    del(pagess)
    del(pages)
    gc.collect()
    print('\nUnproxy..')
    arr = unproxy_list(arr)
    emptyTitleToId = unproxy_dict(emptyTitleToId)
    with open(LOCAL + '99EmptyTitleToId.pkl', 'wb') as f:
        pickle.dump(emptyTitleToId, f)
    f.close()
    del(emptyTitleToId)
    emptyIdToTitle = unproxy_dict(emptyIdToTitle)
    with open(LOCAL + '99EmptyIdToTitle.pkl', 'wb') as f:
        pickle.dump(emptyIdToTitle, f)
    del(emptyIdToTitle)
    gc.collect()
    print('\nSort..')
    arr = sorted(arr, key=lambda arr: (arr[0], arr[2]))
    print('Save..')
    with open(LOCAL + 'arr.pkl', 'wb') as f:
        pickle.dump(arr, f)
    input("All Complite..")