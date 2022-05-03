import pickle
with open('./99Arr.pkl','rb') as f:
    arr = pickle.load(f)
size = len(arr)
for i in range(0, 200):
    print(arr[i][0].decode('utf-8')+ " " + str(arr[i][1]) + " " + str(arr[i][2]))

