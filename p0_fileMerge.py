f1 = open("./Raw/02Ankers", 'r', encoding='UTF-8')
pages = f1.read().split('<')
f1.close()
print("Load f1 complite..")
f2 = open("./Raw/02Ankers_Title_ID", 'r', encoding='UTF-8')

f4 = open("./Raw/02Ankers_Merge", 'w', encoding='UTF-8')
for i in range(1, len(pages)):
    lines = pages[i].split('\n')
    lineCount = len(lines) - 2#파일을 읽을때 읽어야 할 줄 개수, 햇갈리니까 주의
    nowSize = lineCount - 1#원소의 개수
    id = lines[0].split('=')[1].split('_')[0]#문자열임
    f4.write('<' + id + '\n')
    for j in range(1, lineCount):
        words = lines[j].split('\t\t')
        a = f2.readline().split('\t\t')
        if a[1] != (words[1]+'\n'):
            print('!!!!!!!!!')
        f4.write(a[0] + '\t\t' + lines[j] + '\n')
    f4.write('\n')
f1.close()
f2.close()
f4.close()