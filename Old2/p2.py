import h5py as h5
import numpy as np
import ast

f1 = open("./Raw/03Ankers_Merge", 'r', encoding='UTF-8')
pages = f1.read().split('<')
f1.close()
print("Load f1 complite..")

f2 = open("./Raw/03Redirects_dict", 'r', encoding='UTF-8')
redirect_dict = ast.literal_eval(f2.read())
f2.close()
print("Load f2 complite..")

target = h5.File("./Dump0413.hdf5", 'r')
titleToId_dict = ast.literal_eval(target['/Titles'].attrs['titleToId'])
target.close()
print("Load Target complite..")

f3 = open("./Raw/03Ankers_Merge_plus", 'w', encoding='UTF-8')
for i in range(1, len(pages)):
    print("\rProcess : p2 : %.4f%%" % ((float(i)/len(pages)) * 100), end="")
    lines = pages[i].split('\n')
    id = lines[0]#문자열임
    f3.write('<' + id+ '\n')

    lineCount = len(lines) - 2#파일을 읽을때 읽어야 할 줄 개수, 햇갈리니까 주의
    for j in range(1, lineCount):
        words = lines[j].split('\t\t')
        des_id = words[0]
        if des_id != 'CanNotFoundTitle' and int(des_id) in redirect_dict:
            des_id = redirect_dict[int(des_id)]
            if des_id in titleToId_dict:
                des_id = titleToId_dict[des_id]
            else:
                des_id = 'ID_ERROR'
        f3.write(str(des_id) + '\t\t' + words[2] + '\n')
    f3.write('\n')
f3.close()