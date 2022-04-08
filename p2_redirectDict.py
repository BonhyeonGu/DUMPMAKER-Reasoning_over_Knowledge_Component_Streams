print("Start..")
f1 = open("./Raw/03Redirects", 'r', encoding='UTF-8')
f2 = open("./Raw/03Redirects_dict", 'w', encoding='UTF-8')
ret = dict()
lines = f1.read().split('\n')
lineSize = len(lines) - 1
for i in range(lineSize):
    #if i == 2652117 : continue
    x = lines[i].split(':')
    id = int(x[1].split('_')[1])
    title = x[3].strip('_') 
    ret[id] = title.encode('utf-8')
f1.close()
f2.write(str(ret))
f2.close()
print("All complite..")