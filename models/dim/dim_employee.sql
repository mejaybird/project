{{
 config(
 materialized = 'table',
 )
}}

with l as (SELECT distinct EMPLOYEE_NUMBER, EMAIL_ADDRESS, PERSON_DISPLAY_NAME,
                          PERSON_TYPE, WORKER_TYPE,DATE_OF_BIRTH,
                           AGE, L1_LEADER from 
{{ref("combined_data")}}
)

SELECT EMPLOYEE_NUMBER, EMAIL_ADDRESS, PERSON_DISPLAY_NAME , PERSON_TYPE, WORKER_TYPE,DATE_OF_BIRTH,
                           AGE, L1_LEADER from l