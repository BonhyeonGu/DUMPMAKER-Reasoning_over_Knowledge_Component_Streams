import h5py as h5
import numpy as np
import ast
import requests
from bs4 import BeautifulSoup
from multiprocessing import Process, freeze_support, Manager
from multiprocessing.managers import DictProxy, ListProxy
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

def sub(pages:str, input_ids:list, ret_texts:dict, ret_ids:dict, ret_size:dict, titleToId_dict:dict, redirect_dict:dict):
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
            anker_texts[j - 1] = words[2].encode('utf-8')
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
            des_id = int(des_id)
            if des_id in redirect_dict:
                des_id = titleToId_dict[redirect_dict[des_id]]
            anker_ids[j - 1] = des_id
        ret_texts[int(id)] = anker_texts
        ret_ids[int(id)] = anker_ids
        ret_size[int(id)] = nowSize
        pages[i] = ''
    return

if __name__ == '__main__':
    freeze_support()
    print("Start..")
    #arr = g.create_dataset('idToTitle', data=np.full(MAXID, '???',dtype=object))

    target = h5.File("./Dump0410.hdf5", 'r')
    titleToId_dict = ast.literal_eval(target['/Titles'].attrs['titleToId'])
    target.close()
    print("Load Target complite..")

    f1 = open("./Raw/02Ankers_Merge", 'r', encoding='UTF-8')
    pages = f1.read().split('<')
    f1.close()
    print("Load f1 complite..")

    f2 = open("./Raw/00Redirects_dict", 'r', encoding='UTF-8')
    redirect_dict = ast.literal_eval(f2.read())
    f2.close()
    print("Load f2 complite..")

    print("Start MultiProcess")
    pages = pages[0:20000]
    pagess = splitList(pages, 24)
    pros = []

    m = Manager()
    listInIds = m.list()
    dictTexts = m.dict()
    dictIds = m.dict()
    dictSizes = m.dict()

    for pages in pagess:
        pro = Process(target=sub, args=(pages, listInIds, dictTexts, dictIds, dictSizes, titleToId_dict, redirect_dict,))
        pro.daemon = True
        pro.start()
        pros.append(pro)

    i = 0
    for pro in pros:
        pro.join()
        print("\rProcess : p2 : %.4f%%" % ((float(i)/len(pros)) * 100), end="")
        i += 1
    
    print('\nUnproxy..')
    listInIds = unproxy_list(listInIds)
    dictTexts = unproxy_dict(dictTexts)
    dictIds = unproxy_dict(dictIds)
    dictSizes = unproxy_dict(dictSizes)
    print('\nMerge and writing..')

    target = h5.File("./Dump0410.hdf5", 'r+')
    target.create_group('Ankers')
    g = target['Ankers']
    for id in listInIds:
        now = g.create_group(str(id))#가상경로
        now.attrs.create('size', data=dictSizes[id])
        if dictSizes[id]  == 0:
            continue
        now.create_dataset('anker_texts', data=dictTexts[id])
        del(dictTexts[id])
        now.create_dataset('anker_ids', data=dictIds[id])
        del(dictIds[id])
    target.close()
    input("\nAll complite..")