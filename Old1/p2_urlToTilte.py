import requests
from bs4 import BeautifulSoup
import ast

def urlToId(keyword):
    while(True):
        try:
            req = requests.get('https://en.wikipedia.org/wiki/' + keyword)
            soup = BeautifulSoup(req.text, 'lxml')
            tag = soup.select_one('#firstHeading')
            return tag.text
        except Exception as e:
            print(keyword + " error")
    return -1

f3 = open("./Raw/02Ankers_Title_dict", 'r', encoding='UTF-8')
d = ast.literal_eval(f3.read())
#d = dict()
f3.close()

print("Start..")
f1 = open("./Raw/02Ankers", 'r', encoding='UTF-8')
f2 = open("./Raw/02Ankers_Title", 'a', encoding='UTF-8')

pages = f1.read().split('<')
print("Page split complite..")

#for i in range(1, len(pages)):
for i in range(100, 400):
    print("\rProcess : %d : %.12f%%" % (i, (float(i)/len(pages)) * 100), end="")
    lines = pages[i].split('\n')
    lineCount = len(lines) - 2#마지막 공백 라인 제외한 줄 갯수
    if lineCount - 1 == 0:
        continue
    for i in range(1, lineCount):        
        words = lines[i].split('\t\t')
        if words[0] in d:
            tit = d[words[0]]
        else:
            tit = urlToId(words[0])
            d[words[0]] = tit
        f2.write(tit+'\n')
    f2.write('')
    
f3 = open("./Raw/02Ankers_Title_dict", 'w', encoding='UTF-8')
f3.write(str(d))
f3.close()

f2.close()
f1.close()