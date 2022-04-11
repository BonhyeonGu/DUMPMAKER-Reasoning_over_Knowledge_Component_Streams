import h5py as h5
import numpy as np
from urllib import parse
import ast
import requests
from bs4 import BeautifulSoup

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
            print("\n" + keyword + " error")
    return -1

def splitList(lis, splitCount):
        if len(lis) <= splitCount:
            return [lis]
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


MAXID = 70355200
print("Start..")
#arr = g.create_dataset('idToTitle', data=np.full(MAXID, '???',dtype=object))

target = h5.File("./Dump0410.hdf5", 'r+')
target.create_group('Ankers')
g = target['Ankers']
titleToId_dict = ast.literal_eval(target['/Titles'].attrs['titleToId'])
print("Load Target complite..")

f1 = open("./Raw/02Ankers", 'r', encoding='UTF-8')
pages = f1.read().split('<')
f1.close()
print("Load f1 complite..")

f2 = open("./Raw/03Redirects_dict", 'r', encoding='UTF-8')
redirect_dict = ast.literal_eval(f2.read())
f2.close()
print("Load f2 complite..")

f3 = open("./Raw/02Ankers_Title_ID", 'r', encoding='UTF-8')
for i in range(1, len(pages)):
    i_per = (float(i)/len(pages)) * 100

    lines = pages[i].split('\n')
    id = lines[0].split('=')[1].split('_')[0]#문자열임
    now = g.create_group(id)#가상경로

    lineCount = len(lines) - 2#파일을 읽을때 읽어야 할 줄 개수, 햇갈리니까 주의
    nowSize = lineCount - 1#원소의 개수
    
    if nowSize == 0:
        now.attrs.create('size', data=nowSize)
        continue
    anker_texts = now.create_dataset('anker_texts', data=np.full(nowSize, '?',dtype=object))
    anker_ids = now.create_dataset('anker_urls', data=np.full(nowSize, -1, dtype=np.int32))

    for j in range(1, lineCount):
        print("\rProcess : p2 : %.4f%% : %.4f%%" % (i_per, (float(j)/lineCount) * 100), end="")
        words = lines[j].split('\t\t')
        anker_texts[j - 1] = words[1].encode('utf-8')
        a = f3.readline().split('\t\t')
        if a[1] != (words[1]+'\n'):
            print('!!!!!!!!!')
        des_id = a[0]

        if des_id == 'CanNotFoundTitle':
            des_id = urlToTitle(words[0])
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
            print('\n'+ ' CrawOK ' + des_id.decode('utf-8') + ' ' + words[0])
            des_id = titleToId_dict[des_id]#문제사항!
        else:
            des_id = int(des_id)
        if des_id in redirect_dict:
            des_id = titleToId_dict[redirect_dict[des_id]]
        anker_ids[j - 1] = des_id
    now.attrs.create('size', data=nowSize)

target.close()
f3.close()
input("\nAll complite..")