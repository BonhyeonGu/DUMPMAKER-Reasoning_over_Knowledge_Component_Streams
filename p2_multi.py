import numpy as np
import os
from pkg_resources import compatible_platforms
from multiprocessing import Process, freeze_support, Manager, Lock
from multiprocessing.managers import DictProxy, ListProxy
import gc
#from mpi4py import MPI

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

def sub(pages:str, emptyIDs:list, emptyIDsIdx, emptyLock, emptyMap:dict):
    for i in range(1, len(pages)):
        #print("\rProcess : p2 : %.4f%%" % ((float(i)/len(pages)) * 100), end="")
        lines = pages[i].split('\n')
        id = lines[0]#문자열임

        lineCount = len(lines) - 2#파일을 읽을때 읽어야 할 줄 개수, 햇갈리니까 주의
        nowSize = lineCount - 1#원소의 개수
        direcotryName = 'D:\\Ankers3\\' + str(id)

        if nowSize == 0:
            os.makedirs(direcotryName)
            f = open(direcotryName + '/size', 'w', encoding='UTF-8')
            f.write('0')
            f.close()
            continue
        anker_texts = np.full(nowSize, '?',dtype=object)
        anker_ids = np.full(nowSize, -1, dtype=np.int32)
        for j in range(1, lineCount):
            words = lines[j].split('\t\t')
            anker_texts[j - 1] = words[1].encode('utf-8')
            des_id = words[0]
            if des_id == 'CanNotFoundTitle':#문제사항!
                nowSize -= 1
                continue
            if des_id == 'ID_ERROR':#문제사항!!
                if words[1].encode('utf-8') in emptyMap:
                    des_id = emptyMap[words[1].encode('utf-8')]#이미 아래작업을 해준적이 있으면 그냥 꺼내온다
                else:
                    emptyLock.acquire()
                    nowId = emptyIDs[emptyIDsIdx[0]]#배열에서 빼서 아이디 확보(정수)
                    emptyMap[nowId] = words[1].encode('utf-8')#확보된 아이디를 키값으로 타이틀 저장
                    des_id = nowId#확보된 아이디를 목표 아이디로 결정
                    emptyIDsIdx[0] += 1
                    #print(str(des_id) + ' ' + words[1] + '\n')
                    emptyLock.release()
            des_id = int(des_id)  
            anker_ids[j - 1] = des_id
        
        
        os.makedirs(direcotryName)
        f = open(direcotryName + '/size', 'w', encoding='UTF-8')
        f.write(str(nowSize))
        f.close()
        np.save(direcotryName + '/anker_texts', anker_texts)
        np.save(direcotryName + '/anker_ids', anker_ids)
        pages[i] = ''
    return

if __name__ == '__main__':
    freeze_support()
    print("Start..")
    #arr = g.create_dataset('idToTitle', data=np.full(MAXID, '???',dtype=object))

    f1 = open("C:\py\\02Ankers_Merge_Plus", 'r', encoding='UTF-8')
    pages = f1.read().split('<')
    f1.close()
    print("Load f1 complite..")

    emptyIDs = np.load('C:\py\99EmptyIDs.npy')

    print("Start MultiProcess")
    pages = pages[1:]#0번째는 항상 버린다.
    pagess = splitList(pages, 12)
    pros = []

    m = Manager()
    emptyIDsIdx = m.list()
    emptyIDsIdx.append(0)

    emptyLock = Lock()
    emptyMap = m.dict()

    i = 0
    for pages in pagess:
        print("\rProcess : p2 : %.4f%%" % ((float(i)/len(pagess)) * 100), end="")
        pro = Process(target=sub, args=(pages, emptyIDs, emptyIDsIdx, emptyLock, emptyMap, ))
        pro.daemon = True
        pro.start()
        pros.append(pro)
        i += 1

    print('\n\n')

    i = 0
    for pro in pros:
        pro.join()
        print("\rProcess : p2 : %.4f%%" % ((float(i)/len(pros)) * 100), end="")
        i += 1
    
    del(emptyIDs)
    del(pagess)
    del(pages)
    gc.collect()

    print('\nUnproxy..')
    emptyMap = unproxy_dict(emptyMap)
    #emptyIDsIdx = unproxy_list(emptyIDsIdx)

    print('\nAdded ID size 0 add')
    for emptyID in emptyMap:
        direcotryName = 'D:\\Ankers3\\' + str(emptyID)
        os.makedirs(direcotryName)
        f = open(direcotryName + '/size', 'w', encoding='UTF-8')
        f.write('0')
        f.close()
        continue

    print('\nMerge and writing..')

    emp = open("C:\py\99EmptyMap", 'w', encoding='UTF-8')
    emp.write(str(emptyMap))
    emp.close()
    #emp = open("C:\py\99EmptySize", 'w', encoding='UTF-8')
    #emp.write(str(emptyIDsIdx[0]))
    #emp.close()

    input("\nAll complite..")