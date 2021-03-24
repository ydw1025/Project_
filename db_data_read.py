import psycopg2
from psycopg2 import connect 
import os 
import pandas as pd
import csv
from db_config import db_config 

# 테이블에 데이터 삽입하는 메소드

def read_data(conn) :
    
    # 데이터베이스 공간 설정
    os.chdir('C:/data/db')
    
    # condition_occurrence 테이블로 데이터 삽입 
    query_condition_occurrence = """
                                    copy condition_occurrence from stdin delimiter ',' csv header;
                                 """
    try:
        cur = conn.cursor()
        with open('condition_occurrence.csv', 'r', encoding="UTF8") as f:
            cur.copy_expert(query_condition_occurrence, f)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)

    # person 테이블로 데이터 삽입 
    query_person = """
                    copy person from stdin delimiter ',' csv header;
                   """
    try:
        cur = conn.cursor()
        with open('person.csv', 'r', encoding="UTF8") as f:
            cur.copy_expert(query_person, f)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)
    
    # death 테이블로 데이터 삽입  
    query_death = """
                    copy death from stdin delimiter ',' csv header;
                  """
    try:
        cur = conn.cursor()
        with open('death.csv', 'r', encoding="UTF8") as f:
            cur.copy_expert(query_death, f)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)
    
    # visit_occurrence 테이블로 데이터 삽입 
    query_visit_occurrence = """
                                copy visit_occurrence from stdin delimiter ',' csv header;
                             """
    try:
        cur = conn.cursor()
        with open('visit_occurrence.csv', 'r', encoding="UTF8") as f:
            cur.copy_expert(query_visit_occurrence, f)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)

    # drug_exposure 테이블로 데이터 삽입 
    query_drug_exposure = """
                                copy drug_exposure from stdin delimiter ',' csv header;
                             """
    try:
        cur = conn.cursor()
        with open('drug_exposure.csv', 'r', encoding="UTF8") as f:
            cur.copy_expert(query_drug_exposure, f)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)
    
    # concept 테이블로 데이터 삽입 
    query_concept = """
                        copy concept from stdin delimiter ',' csv header;
                    """
    try:
        cur = conn.cursor()
        with open('concept.csv', 'r', encoding="UTF8") as f:
            cur.copy_expert(query_concept, f)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)

if __name__ == '__main__':
    
    # postgresql 연동

    param = db_config()
    conn = connect(**param)
    read_data(conn)
    conn.close()