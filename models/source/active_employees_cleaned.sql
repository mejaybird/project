WITH ACTIVE_EMPLOYEES AS (
  SELECT *
  FROM {{source('JAY','active_employees')}}
)

            SELECT
                    FTV_ID,
                    PERSON_DISPLAY_NAME,
                    EMAIL_ADDRESS,
                    PERSON_TYPE,
                    WORKER_TYPE,
                    DATE_OF_BIRTH,
                    AGE,
                    ASSIGNMENT_STATUS,
                    ASSIGNMENT_CURRENT_EFFECTIVE_DATE,
                    ASSIGNMENT_CURRENT_CHG_ACTION,
                    ASSIGNMENT_CURRENT_CHG_REASON,
                    ASSIGNMENT_EMPLOYMENT_CATEGORY,
                    SEGMENT,
                    OPCO,
                    SUB_OPCO,
                    LEGAL_ENTITY_NAME,
                    BUSINESS_UNIT_NAME,
                    DEPARTMENT_NAME,
                    ASSIGNMENT_NAME,
                    JOB_TITLE,
                    JOB_FUNCTION,
                    JOB_FAMILY,
                    NVL(EEO_CATEGORY, 'NA') AS EEO_CATEGORY,
                    CAREER_BAND,
                    CAREER_LEVEL,
                    GRADE_NAME,
                    HOURLY_SALARIED AS HOURLY_SALARY,
                    FTE,
                    ORIGINAL_HIRE_DATE,
                    LATEST_HIRE_DATE,
                    ADJUSTED_SERVICE_DATE,
                    LENGTH_OF_SERVICE,
                    L1_LEADER,
                    MANAGER_FTV_ID,
                    NVL(WORK_REGION, '-')  AS WORK_REGION,
                    NVL(WORK_COUNTRY, '-') AS  WORK_COUNTRY,
                    NVL(WORK_STATE, '-') AS WORK_STATE,
                    NVL(WORK_CITY, '-') AS WORK_CITY,
                    NVL(WORK_LOCATION, '-') WORK_LOCATION,
                    HEADCOUNT,
                    SALARY,
                    CURRENCY_CODE,
                    PERFORMANCE_RATING,
                    POTENTIAL_RATING
                                            FROM ACTIVE_EMPLOYEES



  