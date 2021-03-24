import psycopg2
from psycopg2 import connect 
import os 
import pandas as pd
import csv
from db_config import db_config 

# 테이블 생성 메소드 

def create_tables(conn):
    
    # 테이블 구성 
    # 참고자료 : https://github.com/OHDSI/CommonDataModel/tree/master/PostgreSQL
    commands = (
        
        """
        DROP TABLE IF EXISTS person CASCADE;
        CREATE TABLE person (
            person_id					BIGINT	  	NOT NULL , 
            gender_concept_id			INTEGER	  	NOT NULL ,
            year_of_birth				INTEGER	  	NOT NULL ,
            month_of_birth				INTEGER	  	NULL,
            day_of_birth				INTEGER	  	NULL,
            birth_datetime				TIMESTAMP	NULL,
            race_concept_id				INTEGER		NOT NULL,
            ethnicity_concept_id		INTEGER	  	NOT NULL,
            location_id					BIGINT		NULL,
            provider_id					BIGINT		NULL,
            care_site_id				BIGINT		NULL,
            person_source_value			VARCHAR(50)	NULL,
            gender_source_value			VARCHAR(50) NULL,
            gender_source_concept_id	INTEGER	    NOT NULL,
            race_source_value			VARCHAR(50) NULL,
            race_source_concept_id		INTEGER		NOT NULL,
            ethnicity_source_value		VARCHAR(50) NULL,
            ethnicity_source_concept_id	INTEGER		NOT NULL
        );
        """,

        """
        DROP TABLE IF EXISTS concept CASCADE;
        CREATE TABLE concept (                                    
            concept_id			INTEGER			NOT NULL ,
            concept_name		VARCHAR(255)	NOT NULL ,
            domain_id			VARCHAR(20)		NOT NULL ,
            vocabulary_id		VARCHAR(20)		NOT NULL ,
            concept_class_id	VARCHAR(20)		NOT NULL ,
            standard_concept	VARCHAR(1)		NULL ,
            concept_code		VARCHAR(50)		NOT NULL ,
            valid_start_date	DATE			NOT NULL ,
            valid_end_date		DATE			NOT NULL ,
            invalid_reason		VARCHAR(1)		NULL
        );

        """,

        """ 
        DROP TABLE IF EXISTS visit_occurrence CASCADE;
        CREATE TABLE visit_occurrence(
            visit_occurrence_id			BIGINT			NOT NULL ,
            person_id					BIGINT			NOT NULL ,
            visit_concept_id			INTEGER			NOT NULL ,
            visit_start_date			DATE			NULL ,
            visit_start_datetime		TIMESTAMP		NOT NULL ,
            visit_end_date				DATE			NULL ,
            visit_end_datetime			TIMESTAMP		NOT NULL ,
            visit_type_concept_id		INTEGER			NOT NULL ,
            provider_id					BIGINT			NULL,
            care_site_id				BIGINT			NULL,
            visit_source_value			VARCHAR(50)		NULL,
            visit_source_concept_id		INTEGER			NOT NULL ,
            admitting_source_concept_id INTEGER     	NOT NULL ,   
            admitting_source_value      VARCHAR(50) 	NULL ,
            discharge_to_concept_id		INTEGER   		NOT NULL ,
            discharge_to_source_value	VARCHAR(50)		NULL ,
            preceding_visit_occurrence_id   BIGINT 		NULL
        );
        """,

        """
        DROP TABLE IF EXISTS drug_exposure CASCADE;
        CREATE TABLE drug_exposure(
            drug_exposure_id			BIGINT			 	NOT NULL ,
            person_id					BIGINT			 	NOT NULL ,
            drug_concept_id				INTEGER			  	NOT NULL ,
            drug_exposure_start_date	DATE			    NULL ,
            drug_exposure_start_datetime    TIMESTAMP		NOT NULL ,
            drug_exposure_end_date		DATE			    NULL ,
            drug_exposure_end_datetime	TIMESTAMP		  	NOT NULL ,
            verbatim_end_date			DATE			    NULL ,
            drug_type_concept_id		INTEGER			  	NOT NULL ,
            stop_reason					VARCHAR(20)			NULL ,
            refills						INTEGER		  		NULL ,
            quantity					NUMERIC			    NULL ,
            days_supply					INTEGER		  		NULL ,
            sig							TEXT				NULL ,
            route_concept_id			INTEGER				NOT NULL ,
            lot_number					VARCHAR(50)	 		NULL ,
            provider_id					BIGINT			  	NULL ,
            visit_occurrence_id			BIGINT			  	NOT NULL ,
            visit_detail_id             BIGINT       		NOT NULL ,
            drug_source_value			VARCHAR(50)	  		NULL ,
            drug_source_concept_id		INTEGER			  	NOT NULL ,
            route_source_value			VARCHAR(50)	  		NULL ,
            dose_unit_source_value		VARCHAR(50)	  		NULL
        );
        """,

        """
        DROP TABLE IF EXISTS condition_occurrence CASCADE;
        CREATE TABLE condition_occurrence(
            condition_occurrence_id		BIGINT			NOT NULL ,
            person_id					BIGINT			NOT NULL ,
            condition_concept_id		INTEGER			NOT NULL ,
            condition_start_date		DATE			NULL ,
            condition_start_datetime	TIMESTAMP		NULL ,
            condition_end_date			DATE			NULL ,
            condition_end_datetime		TIMESTAMP		NULL ,
            condition_type_concept_id	INTEGER			NOT NULL ,
            stop_reason					VARCHAR(20)		NULL ,
            provider_id					BIGINT			NULL ,
            visit_occurrence_id			BIGINT			NOT NULL ,
            visit_detail_id             BIGINT	     	NOT NULL ,
            condition_source_value		VARCHAR(50)		NULL ,
            condition_source_concept_id	    INTEGER		NOT NULL ,
            condition_status_source_value	VARCHAR(50)		NULL ,
            condition_status_concept_id	    INTEGER		NOT NULL
        );
        """,
        """
        DROP TABLE IF EXISTS death CASCADE;
        CREATE TABLE death (
            person_id					BIGINT	  	NOT NULL , 
            death_date                  DATE        NULL,
            death_datetime				TIMESTAMP	NULL,
            death_type_concept_id		INTEGER		NOT NULL,
            cause_concept_id		    INTEGER	  	NOT NULL,
            cause_source_value			VARCHAR(50) NULL,
            cause_source_concept_id	    INTEGER		NOT NULL
        );
        """
        ,

        # 테이블 기본키 설정 
        """

        /*concept pk constraint*/ 
        ALTER TABLE concept ADD CONSTRAINT xpk_concept PRIMARY KEY (concept_id);

        /*person pk constraint*/
        ALTER TABLE person ADD CONSTRAINT xpk_person PRIMARY KEY ( person_id );

        /*visit_occurrence pk constraint*/
        ALTER TABLE visit_occurrence ADD CONSTRAINT xpk_visit_occurrence PRIMARY KEY ( visit_occurrence_id );

        /*drug_exposure pk constraint*/
        ALTER TABLE drug_exposure ADD CONSTRAINT xpk_drug_exposure PRIMARY KEY ( drug_exposure_id );
        
        /*condition_occurrence pk constraint*/
        ALTER TABLE condition_occurrence ADD CONSTRAINT xpk_condition_occurrence PRIMARY KEY ( condition_occurrence_id );
        
        /*death pk constraint*/
        ALTER TABLE death ADD CONSTRAINT xpk_death PRIMARY KEY ( person_id );
        """
        
        )
    try:
        # 커서선언
        cur = conn.cursor()
        
        # 테이블 생성
        for command in commands:
            cur.execute(command)

        # 커서종료 
        cur.close()

        # 변동사항 커밋
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)

if __name__ == '__main__':
    
    # postgresql 연동
    
    param = db_config()
    conn = connect(**param)
    create_tables(conn)
    conn.close()