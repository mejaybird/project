{{
 config(
 materialized = 'table',
 )
}}

SELECT distinct FTV_ID AS EMPLOYEE_NUMBER, EMAIL_ADDRESS, PERSON_DISPLAY_NAME,
                          PERSON_TYPE, WORKER_TYPE,DATE_OF_BIRTH,
                           AGE, L1_LEADER from 
{{ref("active_employees_cleaned")}} order by EMPLOYEE_NUMBER
