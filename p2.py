import h5py as h5
import numpy as np
from urllib import parse
import ast
MAXID = 70355200
print('open..')
target = h5.File("./Dump0406.hdf5", 'r+')
target.create_group('Ankers')
g = target['Ankers']
#arr = g.create_dataset('idToTitle', data=np.full(MAXID, '???',dtype=object))
f1 = open("./Raw/02Ankers", 'r', encoding='UTF-8')
f2 = open("./Raw/03Redirects_dict", 'r', encoding='UTF-8')
titleToId_dict = ast.literal_eval(target['/Titles'].attrs['titleToId'])
redirect_dict = ast.literal_eval(f2.read())
f2.close()

pages = f1.read().split('<')
for i in range(1, len(pages)):
    print("\rProcess : p2 : %.4f%%" % ((float(i)/len(pages)) * 100), end="")
    lines = pages[i].split('\n')
    
    lineCount = len(lines) - 2#마지막 공백 라인 제외한 줄 갯수
    id = lines[0].split('=')[1].split('_')[0]#문자열임
    g.create_group(id)#가상경로
    now = target['/Ankers/'+str(id)]
    now.attrs.create['size'] = lineCount - 1#제목까지 제외한 줄 갯수 == 엥커 갯수
    if lineCount - 1 == 0:
        continue
    anker_texts = now.create_dataset('anker_texts', data=np.full(lineCount - 1, '?',dtype=object))
    anker_ids = now.create_dataset('anker_urls', data=np.full(lineCount - 1, '?',dtype=np.int32))
    for i in range(1, lineCount):        
        words = lines[i].split('\t\t')
        anker_texts[id] = words[1]
        title = (parse.unquote(words[0])).replace(' ', '_')
        des_id = titleToId_dict[title]
        if des_id in redirect_dict:
            des_id = titleToId_dict[redirect_dict[des_id]]
f1.close()