
# Project 

## 0. 실행환경
- 파일 저장공간을 (C:/data/db)로 설정해야합니다. 
- 리눅스나 macOS의 경우 db_data_read.py 13라인 os.chdir('C:/data/db')을 파일 저장공간의 주소로 바꿔줘야 합니다
- psycopg2 및 pandas 라이브러리가 필요합니다
--- 
## 1. db_config.py
 - PostgresSQL 연동을 위해 host, port, database, user, password를 설정해야 합니다
--- 
## 2.db_create_table.py
 - 데이터 구조에 따른 테이블 생성을 해줍니다 
--- 
## 3.db_data_read.py
 - 파일로부터 데이터를 읽어옵니다
---
## 4.problems_.py
 - 각 문제에 따른 SQL쿼리문을 Python 내의 함수형태로 구현하여 실행시 쿼리결과가 산출됩니다
 - 각 문제에 대한 코드 설명은 코드 내 주석으로 달았습니다


