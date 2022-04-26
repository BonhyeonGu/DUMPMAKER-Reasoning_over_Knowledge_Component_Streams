import h5py as h5
import numpy as np
import ast
from pkg_resources import compatible_platforms
import requests
from bs4 import BeautifulSoup
from multiprocessing import Process, freeze_support, Manager, Lock
from multiprocessing.managers import DictProxy, ListProxy
import gc
#from mpi4py import MPI

def urlToTitle(keyword):
    while(True):
        try:
            req = requests.get('https://en.wikipedia.org/wiki/' + keyword)
            soup = BeautifulSoup(req.text, 'lxml')
            #존재하지 않는 검색어라는 메세지 발생시 -1을 리턴
            tag = soup.select_one('#noarticletext > tbody > tr > td > b')
            if tag is not None:
                return -1
            tag = soup.select_one('#firstHeading')
            return tag.text
        except Exception as e:
            #print("\n" + keyword + " error")
            e
    return -1

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
        ret.append(tmp)
        return ret

def unproxy_dict(dict_proxy):
        return {k: (dict(v) if isinstance(v, DictProxy) else v)
            for k, v in dict_proxy.items()}

def unproxy_list(list_proxy):
        return {(list(v) if isinstance(v, ListProxy) else v)
            for  v in list_proxy}

def sub(pages:str, emptyIDs:list, emptyIDsIdx, emptyLock, emptyMap:dict, input_ids:list, ret_texts:dict, ret_ids:dict, ret_size:dict, titleToId_dict:dict):
    for i in range(1, len(pages)):
        #print("\rProcess : p2 : %.4f%%" % ((float(i)/len(pages)) * 100), end="")
        lines = pages[i].split('\n')
        id = lines[0]#문자열임
        input_ids.append(int(id))

        lineCount = len(lines) - 2#파일을 읽을때 읽어야 할 줄 개수, 햇갈리니까 주의
        nowSize = lineCount - 1#원소의 개수
        
        if nowSize == 0:
            ret_size[int(id)] = 0
            continue

        anker_texts = np.full(nowSize, '?',dtype=object)
        anker_ids = np.full(nowSize, -1, dtype=np.int32)
        for j in range(1, lineCount):
            words = lines[j].split('\t\t')
            anker_texts[j - 1] = words[1].encode('utf-8')
            des_id = words[0]
            if des_id == 'CanNotFoundTitle':
                nowSize -= 1
                continue
                des_id = urlToTitle(words[1])
                if des_id == -1:#존재하지 않으면
                    nowSize -= 1
                    continue
                des_id = des_id.replace(' ', '_')
                des_id = des_id[0].capitalize() + des_id [1:]
                if des_id == 'Bad_title':#!!
                    nowSize -= 1
                    continue
                des_id = des_id.encode('utf-8')
                if des_id not in titleToId_dict:#!!
                    nowSize -= 1
                    continue
                des_id = titleToId_dict[des_id]#문제사항!
            if des_id == 'ID_ERROR':#문제사항!
                if words[1].encode('utf-8') in emptyMap:
                    des_id = emptyMap[words[1].encode('utf-8')]#이미 아래작업을 해준적이 있으면 그냥 꺼내온다
                else:
                    emptyLock.acquire()
                    nowId = emptyIDs[emptyIDsIdx[0]]#배열에서 빼서 아이디 확보(정수)
                    emptyMap[nowId] = words[1].encode('utf-8')#확보된 아이디를 키값으로 타이틀 저장
                    des_id = nowId#확보된 아이디를 목표 아이디로 결정
                    emptyIDsIdx[0] += 1
                    emptyLock.release()
            des_id = int(des_id)
            # if des_id in redirect_dict:
            #     des_id = redirect_dict[des_id]#여기 des_id는 타이틀
            #     if des_id in titleToId_dict:
            #         des_id = titleToId_dict[des_id]
            #     else:#문제사항!!
            #         emptyLock.acquire()
            #         nowId = emptyIDs[emptyIDsIdx]
            #         emptyMap[nowId] = des_id.encode('utf-8')
            #         des_id = nowId
            #         emptyIDsIdx += 1
            #         emptyLock.release()
            # #####        
            anker_ids[j - 1] = des_id
        np.save('./n/' + str(id) + '_anker_texts', anker_texts)
        np.save('./n/' + str(id) + 'ret_ids', anker_ids)
        #ret_texts[int(id)] = anker_texts
        #ret_ids[int(id)] = anker_ids
        #ret_size[int(id)] = nowSize
        pages[i] = ''
    return

if __name__ == '__main__':
    freeze_support()
    print("Start..")
    #arr = g.create_dataset('idToTitle', data=np.full(MAXID, '???',dtype=object))

    target = h5.File("D:\Dump0413.hdf5", 'r')
    titleToId_dict = ast.literal_eval(target['/Titles'].attrs['titleToId'])
    target.close()
    print("Load Target complite..")

    f1 = open("./Raw/03Ankers_Merge_plus", 'r', encoding='UTF-8')
    pages = f1.read().split('<')
    f1.close()
    print("Load f1 complite..")

    emptyIDs = np.load('./Raw/03EmptyIDs.npy')

    print("Start MultiProcess")
    pages = pages[1:1633157]#0번째는 항상 버린다.
    pagess = splitList(pages, 12)
    pros = []

    m = Manager()
    listInIds = m.list()
    dictTexts = m.dict()
    dictIds = m.dict()
    dictSizes = m.dict()

    emptyIDsIdx = [0]
    emptyLock = Lock()
    emptyMap = m.dict()

    i = 0
    for pages in pagess:
        print("\rProcess : p3 : %.4f%%" % ((float(i)/len(pagess)) * 100), end="")
        pro = Process(target=sub, args=(pages, emptyIDs, emptyIDsIdx, emptyLock, emptyMap, listInIds, dictTexts, dictIds, dictSizes, titleToId_dict, ))
        pro.daemon = True
        pro.start()
        pros.append(pro)
        i += 1

    i = 0
    for pro in pros:
        pro.join()
        print("\rProcess : p3 : %.4f%%" % ((float(i)/len(pros)) * 100), end="")
        i += 1
    
    del(titleToId_dict)
    del(emptyIDs)
    del(pagess)
    del(pages)
    gc.collect()

    print('\nUnproxy..')
    listInIds = unproxy_list(listInIds)
    dictTexts = unproxy_dict(dictTexts)
    dictIds = unproxy_dict(dictIds)
    dictSizes = unproxy_dict(dictSizes)
    emptyMap = unproxy_dict(emptyMap)
    emptyIDsIdx = unproxy_dict(emptyIDsIdx)
    print('\nMerge and writing..')

    emp = open("./Raw/04EmptyMap", 'w', encoding='UTF-8')
    emp.write(str(emptyMap))
    emp.close()
    emp = open("./Raw/04EmptySize", 'w', encoding='UTF-8')
    emp.write(str(emptyIDsIdx[0]))
    emp.close()

    del(emptyMap)
    del(emptyIDsIdx)
    gc.collect()

    target = h5.File("D:\Dump0413.hdf5", 'r+')
    #target.create_group('Ankers')
    g = target['Ankers']

    print('\nWrite start..\n')
    i = 0
    for id in listInIds:
        print("\rProcess : p3 : %.4f%%" % ((float(i)/len(listInIds)) * 100), end="")
        
        now = g.create_group(str(id))#가상경로
        now.attrs['size'] = dictSizes[id]
        #now.attrs.create('size', data=dictSizes[id])
        if dictSizes[id]  == 0:
            continue

        ##????
        si = dictSizes[id]
        anker_texts = now.create_dataset('anker_texts', data=np.full(si, '?',dtype=object))
        anker_ids = now.create_dataset('anker_urls', data=np.full(si, -1, dtype=np.int32))
        a1 = dictTexts[id]
        a2 = dictIds[id]
        for j in range(0, si):
            anker_texts[j] = a1[j]
            anker_ids[j] = a2[j]
        #now.create_dataset('anker_texts', data=dictTexts[id], dtype=object)
        #now.create_dataset('anker_ids', data=dictIds[id], dtype=np.int32)
        
        del(dictTexts[id])
        del(dictIds[id])
        del(dictSizes[id])
        #gc.collect()
        i += 1
    target.close()
    input("\nAll complite..")