import psycopg2
from psycopg2 import connect
import pandas as pd  
from db_config import db_config 

def problem_2(conn) : 
    
    try:
        cur = conn.cursor()
        query = """
                    select max(D."총내원일수") as "최대내원일", count(D."총내원일수") as "해당환자수"
                    from (
                        select C.person_id,"총입원일","총외래및응급일",  "총입원일" + "총외래및응급일" as "총내원일수"
                        from(
                            with "외래응급" as (
                                select person_id, sum(visit_end_date - visit_start_date + 1) "총외래및응급일" 
                                from visit_occurrence 
                                where visit_concept_id in ('9202', '9203')
                                group by person_id
                            )

                            select "입원".person_id, sum("입원일") as "총입원일", max("총외래및응급일") as "총외래및응급일"
                            from "외래응급",
                            (
                                select A.person_id, "입원시작일", "입원종료일", "입원종료일" - "입원시작일" + 1 as "입원일"  
                                from visit_occurrence B,
                                (
                                    select person_id, min(visit_start_date) as "입원시작일", max(visit_end_date) as "입원종료일"
                                    from visit_occurrence 
                                    where visit_concept_id ='9201'
                                    group by person_id, visit_end_date
                                )A
                                where A.person_id = B.person_id
                                group by A.person_id,"입원시작일","입원종료일"
                            )"입원"
                            where "입원".person_id = "외래응급".person_id
                            group by "입원".person_id
                        )C
                        order by "총내원일수" desc
                    )D
                    group by D."총내원일수"
                    order by "최대내원일" desc

                """
                #1. 입원 / 응급및외래 를 구분하여 총내원일수 산출
                  # - 입원의 경우 visit_end_date가 같은 건끼리 묶어서 동일 입원건을 판단하고 첫입원일과 최종입원일을 산출 
                #2. person_id 별로 그룹화 하여 환자별로 총입원일수와 총외래및응급일수를 산출
                #3. 총입원일과 총외래및응급일을 합하여 총내원일수를 산출
                #4. 총내원일수별로 그룹화(max) 하여 환자들이 가지는 총내원일수의 분포를 산출하고, 이 내원일수에 따른 환자수를 산출 
                  
        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        records = cur.fetchall()
        records = pd.DataFrame(records, columns= colnames)
        return records

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

def problem_3(conn) : 
    
    try:
        cur = conn.cursor()
        query = """
                    select distinct concept_name
                    from condition_occurrence 
                    left join concept 
                    on concept.concept_id = condition_occurrence.condition_concept_id
                    where concept_name ~* '^(a|b|c|d|e)' and concept_name ~* '(heart)' 
                """

                #1. 정규표현식을 사용하여 a,b,c,d,e로 시작하는 상병명을 추출 ( ~* 는 대소문자 구분 무시)
                #2. 같은 방법으로 중간에 대소문자 구분 없이 heart를 가지는 상병명을 추출 
                #3. 두가지 조건을 병합하여 결과 산출  

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        records = cur.fetchall()
        records = pd.DataFrame(records, columns= colnames)
        return records

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

def problem_4(conn) : 
    
    try:
        cur = conn.cursor()
        query = """
                    select drug_concept_id, 
                    min(drug_exposure_start_date) as drug_exposure_begin, 
                    max(drug_exposure_end_date) as drug_exposure_last,
                    max(drug_exposure_end_date) - min(drug_exposure_start_date ) + 1 as drug_exposure_days
                    from drug_exposure 
                    where person_id='1891866'
                    group by drug_concept_id
                    order by drug_exposure_days desc; 
                """
                #1. 처방약 별로 최초투약일(min date) , 최종투약일(max date)을 구하고 이 차이를 투약일(drug_exposure_days)로 정의  
                #2. 조건에 따라 결과 산출 

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        records = cur.fetchall()
        records = pd.DataFrame(records, columns= colnames)
        return records

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

def problem_5(conn) : 
    
    try:
        cur = conn.cursor()
        query = """
                        create temporary table drug_pair (drug_concept_id1 int, drug_concept_id2 int);
                        insert into drug_pair values (40213154,19078106),
                        (19078106,40213154),
                        (19009384,19030765),
                        (40224172,40213154),
                        (19127663,19009384),
                        (1511248,40169216),
                        (40169216,1511248),
                        (1539463,19030765),
                        (19126352,1539411),
                        (1539411,19126352),
                        (1332419,19126352),
                        (40163924,19078106),
                        (19030765,19009384),
                        (19106768,40213154),
                        (19075601,19126352);

                        with drug_list as (
                            select distinct drug_concept_id, concept_name, count(*) as cnt from
                            drug_exposure de
                            join concept
                            on drug_concept_id = concept_id
                            where concept_id in (
                            40213154,19078106,19009384,40224172,19127663,1511248,40169216,1539463,19126352,1539411,1332419,40163924,19030765,19106768,19075601)
                            group by drug_concept_id,concept_name
                            order by count(*) desc
                        )

                        select A.concept_name as d1_concept_name, A.cnt as d1_cnt
                        from drug_list B, 
                        (
                            select *
                            from drug_list
                            join drug_pair 
                            on drug_pair.drug_concept_id1 = drug_list.drug_concept_id 
                        )A
                        where A.drug_concept_id2 = B.drug_concept_id
                        and A.cnt < B.cnt
                        order by A.cnt desc

                   """
                   # 1. drug_list에서 pair의 각 id에 따라 각각 한번씩 join을 수행하여 같은 행에 pair별 count값을 가져옴 
                   # 2. pair별 count값의 차이에 대해 pair의 두번쨰요소가 첫번째요소보다 큰 경우를 산출
                   # 3. 해당 조건일 경우 첫번쨰요소의 약이름과 처방횟수를 산출하여 처방건수기준 내림차순정렬 
        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        records = cur.fetchall()
        records = pd.DataFrame(records, columns= colnames)
        return records

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

def problem_6(conn) : 
    
    try:
        cur = conn.cursor()
        query = """
                    select count(F.person_id) as "# of patient"
                    from person F, 
                    (
                        select C.person_id, sum(D.drug_exposure_days) as tot_drug_exposure_days 
                        from person C, 
                        (
                            select A.person_id, B.cosd, A.drug_exposure_start_date, A.drug_exposure_end_date,
                            case when A.drug_exposure_start_date >= B.cosd and A.drug_exposure_end_date >= B.cosd
                                    then A.drug_exposure_end_date - A.drug_exposure_start_date +1 
                                when A.drug_exposure_start_date < B.cosd and A.drug_exposure_end_date < B.cosd
                                    then 0
                                when A.drug_exposure_start_date < B.cosd and A.drug_exposure_end_date >= B.cosd
                                    then A.drug_exposure_end_date - B.cosd +1 
                                else NULL
                            end as drug_exposure_days 
                            from drug_exposure A, 
                            (
                                select person_id, min(condition_start_date) as cosd
                                from condition_occurrence 
                                where condition_concept_id in (3191208,36684827,3194332,3193274,43531010,4130162,45766052,
                                45757474,4099651,4129519,4063043,4230254,4193704,4304377,201826,3194082,3192767) 
                                group by person_id, condition_concept_id 
                            )B 
                            where A.person_id = B.person_id
                            and A.drug_concept_id = '40163924'
                        )D
                        where C.person_id = D.person_id
                        group by C.person_id
                    )E
                    where F.person_id = E.person_id
                    and E.tot_drug_exposure_days >= 90
                    and extract(year from AGE(NOW(), birth_datetime)) >= 18;

                """
                   # 1. 2형 당뇨병 대상자추출 및 drug_concept_id 40163924 추출하여 병합
                   # 2. 진료일자가 투약시작일과 투약종료일에 대해 모두 큰 경우는 건별 투약일수를 0 으로 산출
                   # 3. 진료일자가 투약시작일과 투약종료일에 대해 모두 같거나 작다면 투약종료일-투약시작일+1의 값으로 건별 투약일 산출
                   # 4. 진료일자가 투약시작일보다 크면서 투약종료일보다는 작은경우 (즉 투약기간중에 진료가 포함되어 있는 경우)는 투약종료일-진료일+1의 값으로 건별 투약일 산출 
                   # 5. 해당 경우의 수에 따라 총투약일수를 산출하고 조건에따라 복용 90일 이상 환자 건만 포함 
                   # 6. person table의 birth_datetime을 활용하여 환자의 나이를 구하고 환자 18세 이상 건만 포함
                   # 7. 총 환자수 산출 
     
        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        records = cur.fetchall()
        records = pd.DataFrame(records, columns= colnames)
        return records

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)        

def problem_7(conn) : 
    
    try:
        cur = conn.cursor()
        query = """
                    select G.person_id, sum(G.sign)-1 as "pattern changed"
                    from (
                        select distinct E.person_id, E.drug_exposure_start_date, E.grp,
                        case when lag(E.grp,1) over (partition by E.person_id) = E.grp 
                        then 0 else 1
                        end as sign 
                        from drug_exposure F, 
                        (
                            select C.person_id, C.drug_exposure_start_date, array_agg(distinct C.drug_concept_id order by C.drug_concept_id) as grp
                            from drug_exposure D, 
                            (
                                    select B.person_id, A.drug_concept_id, A.drug_exposure_start_date
                                    from drug_exposure A, 
                                    (
                                        select person_id
                                        from condition_occurrence 
                                        where condition_concept_id in (3191208,36684827,3194332,3193274,43531010,4130162,45766052,
                                        45757474,4099651,4129519,4063043,4230254,4193704,4304377,201826,3194082,3192767) 
                                        group by person_id
                                    )B 
                                    where B.person_id = A.person_id
                                    and A.drug_concept_id in (19018935,1539411,1539463,19075601,1115171)
                                    group by B.person_id, A.drug_concept_id, A.drug_exposure_start_date
                                    order by B.person_id , A.drug_exposure_start_date asc
                            )C
                            where C.person_id = D.person_id
                            group by C.person_id, C.drug_exposure_start_date
                            order by C.person_id, C.drug_exposure_start_date
                        )E
                        where E.person_id = F.person_id
                        order by E.person_id, E.drug_exposure_start_date, sign desc
                    )G
                    group by G.person_id
                    order by "pattern changed" desc
                """

                   # 1. 2형 당뇨병 환자군과 (19018935,1539411,1539463,19075601,1115171) 의약품에 대한 건만 선 추출
                   # 2. 이때 각 환자와 투약시기, 의약품에 대해 서로 중복되지않는 테이블 산출 
                   # 3. Array_agg 함수를 사용하여 특정 환자의 투약기간의 리스트에 대해 투약한 약제의 조합을 산출  
                   # 4. 환자별 PARTITION 및 LAG 1 함수를 사용하여 특정 환자에 대해 투약시기가 다르면서 약제 처방이 달라진 경우 1, 같은경우는 0으로 산출
                   # 5. 환자가 처음 어떠한 내용으로 처방받았을때 변동사항이 1로 기록되어 1만큼을 차감하여 합산한 환자별 총 변동내역 산출 
        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        records = cur.fetchall()
        records = pd.DataFrame(records, columns= colnames)
        return records

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)  

if __name__ == '__main__':
    
    param = db_config()
    conn = connect(**param)

    print("######################################### p2 #############################################")
    
    p2 = problem_2(conn)
    print(p2)

    print("######################################### p3 #############################################")
    
    p3 = problem_3(conn)
    print(p3)

    print("######################################### p4 ##############################################")
    
    p4 = problem_4(conn)
    print(p4)

    print("######################################### p5 ##############################################")
    
    p5 = problem_5(conn)
    print(p5)

    print("######################################### p6 ##############################################")
    
    p6 = problem_6(conn)
    print(p6)

    print("######################################### p7 ##############################################")
    
    p7 = problem_7(conn)
    print(p7)

    print("###################################### Thank you ##########################################")