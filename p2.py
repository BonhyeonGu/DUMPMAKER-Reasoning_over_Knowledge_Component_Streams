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
            print(keyword + " error")
    return -1

MAXID = 70355200
print("Start..")
target = h5.File("./Dump0410.hdf5", 'r+')
target.create_group('Ankers')
g = target['Ankers']
#arr = g.create_dataset('idToTitle', data=np.full(MAXID, '???',dtype=object))
f1 = open("./Raw/02Ankers", 'r', encoding='UTF-8')
f2 = open("./Raw/03Redirects_dict", 'r', encoding='UTF-8')
f3 = open("./Raw/02Ankers_Title_ID", 'r', encoding='UTF-8')
titleToId_dict = ast.literal_eval(target['/Titles'].attrs['titleToId'])
print("Load f1 complite..")
redirect_dict = ast.literal_eval(f2.read())
print("Load f2 complite..")
f2.close()

pages = f1.read().split('<')
print("Page split complite..")
for i in range(1, 11):
    i_per = (float(i)/len(pages)) * 100
    lines = pages[i].split('\n')
    
    lineCount = len(lines) - 2#마지막 공백 라인 제외한 줄 갯수
    id = lines[0].split('=')[1].split('_')[0]#문자열임
    now = g.create_group(id)#가상경로
    nowSize = lineCount - 1#제목까지 제외한 줄 갯수 == 엥커 갯수
    
    if nowSize == 0:
        now.attrs.create('size', data=nowSize)
        continue
    anker_texts = now.create_dataset('anker_texts', data=np.full(nowSize, '?',dtype=object))
    anker_ids = now.create_dataset('anker_urls', data=np.full(nowSize, -1, dtype=np.int32))

    for j in range(1, lineCount):
        print("\rProcess : p2 : %.4f%% : %.4f%%" % (i_per, (float(j)/len(lineCount)) * 100), end="")
        words = lines[j].split('\t\t')
        anker_texts[j - 1] = words[1].encode('utf-8')
        des_id = f3.readline().split('\t\t')[0]
        if des_id == 'CanNotFoundTitle':
            des_id = urlToTitle(words[0])
            if des_id == -1:#존재하지 않으면
                nowSize -= 1
                continue
            des_id = des_id.replace(' ', '_').encode('utf-8')
            des_id = titleToId_dict[des_id]
        else:
            des_id = int(des_id)
        if des_id in redirect_dict:
            des_id = titleToId_dict[redirect_dict[des_id]]
    now.attrs.create('size', data=nowSize)
target.close()
f1.close()
f3.close()
input("All complite..")