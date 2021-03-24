## 파일 저장공간을 (C:/data/db)로 설정해야합니다. 
(리눅스나 맥의 경우 db_data_read.py 13라인 os.chdir('C:/data/db') 을 저장공간의 주소로 바꿔줘야 합니다)   
---
### 파일 실행순서 

## 1. db_config.py
 - postgres 연동을 위해 host, port, database, user, password를 설정해야 합니다. 
--- 
## 2.db_create_table.py
 - 테이블 생성을 해줍니다 
--- 
## 3.db_data_read.py
 - 파일로부터 데이터를 읽어옵니다
---
## 4.problems_py
 - 각 문제에 따른 답을 함수형태로 구현하여 실행시 쿼리결과가 산출됩니다. 
 - 각 문제에 대한 설명은 코드내 주석으로 달았습니다
