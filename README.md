## 0:전처리

### p0_fileMerge 파일 합침(없어질 수 있음)  
in : 00Ankers, 00Ankers_Title_ID  
out : 03Ankers_Merge

### p0_redirectDict 리디렉트를 딕셔너리로)  
in : 00Redirects
out : 03Redirects_dict

## 1:ID와 타이틀 맵핑, 딕셔너리와 배열

### p1  
in : 01Titles  
out : Dump

## 2:주인없는 ID가 나열된 배열

### p2
in : Dump
out : 03EmptyIDs.npy

## 3:ID별로 앵커들을 기록, 오류 ID 발견시 주인없는 ID를 사용 후 빈페이지 취급

### p3_multi
in : Dump, 03Ankers_Merge, 03Redirects_dict, 03EmptyIDs.npy
out : 04EmptyMap, 04EmptySize
edit : Dump

## 4:빈 페이지 사이즈 속성 0을 추가
in : Dump, 04EmptyMap, 04EmptySize
out : 04EmptyMap, 04EmptySize
edit : Dump
