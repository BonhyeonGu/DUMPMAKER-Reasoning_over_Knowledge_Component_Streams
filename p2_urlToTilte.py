import requests
from bs4 import BeautifulSoup

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

print("Start..")
f1 = open("./Raw/02Ankers", 'r', encoding='UTF-8')
f2 = open("./Raw/02Ankers_Title", 'w', encoding='UTF-8')
pages = f1.read().split('<')
print("Page split complite..")
for i in range(1, len(pages)):
    print("\rProcess : p2 : %.12f%%" % ((float(i)/len(pages)) * 100), end="")
    lines = pages[i].split('\n')
    lineCount = len(lines) - 2#마지막 공백 라인 제외한 줄 갯수
    if lineCount - 1 == 0:
        continue
    for i in range(1, lineCount):        
        words = lines[i].split('\t\t')
        tit = urlToId(words[0])
        f2.write(tit+'\n')
f1.close()
f2.close()