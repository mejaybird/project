{{
  config(materialized='incremental',
    unique_key='enrollment_id',
    merge_exclude_columns = ['inserted_at'])
}}


WITH FACT AS (
  SELECT    *,
            TO_CHAR(CERTIFICATE_DATE, 'YYYYMMDD')::NUMBER(8) AS CERTIFICATE_DATE_KEY,
            TO_CHAR(DATE_COMPLETED, 'YYYYMMDD')::NUMBER(8) AS DATE_COMPLETED_KEY,
            TO_CHAR(DATE_EDITED, 'YYYYMMDD')::NUMBER(8) AS DATE_EDITED_KEY,
            TO_CHAR(DATE_ENROLLED, 'YYYYMMDD')::NUMBER(8) AS DATE_ENROLLED_KEY,
            TO_CHAR(DATE_EXPIRES, 'YYYYMMDD')::NUMBER(8) AS DATE_EXPIRES_KEY,
            TO_CHAR(LAST_LOGGEDIN, 'YYYYMMDD')::NUMBER(8) AS LAST_LOGGED_IN_KEY,
            TO_CHAR(ORIGINAL_HIRE_DATE, 'YYYYMMDD')::NUMBER(8) AS ORIGINAL_HIRE_DATE_KEY,
            TO_CHAR(LATEST_HIRE_DATE, 'YYYYMMDD')::NUMBER(8) AS LATEST_HIRE_DATE_KEY,
            TO_CHAR(ADJUSTED_SERVICE_DATE, 'YYYYMMDD')::NUMBER(8) AS ADJUSTED_SERVICE_DATE_KEY
  FROM {{ref("combined_data")}}
)

                      SELECT
                      F.ENROLLMENT_ID,
                      R.RATING_KEY,
                      C.COURSE_KEY,
                      LOC.LOCATION_KEY,
                      SEG.SEGMENT_KEY,
                      B.ENTITY_KEY,
                      ASS.ASSIGNMENT_KEY,
                      J.JOB_KEY,
                      EN.ENROLL_KEY,
                      F.EMPLOYEE_NUMBER,
                      NVL(CERTIFICATE_DATE_KEY, -999) AS CERTIFICATE_DATE_KEY,
                      NVL(DATE_COMPLETED_KEY, -999) AS COMPLETED_DATE_KEY,
                      NVL(DATE_EDITED_KEY, -999) AS EDITED_DATE_KEY,
                      NVL(DATE_ENROLLED_KEY, -999) AS ENROLLED_DATE_KEY,
                      NVL(DATE_EXPIRES_KEY, -999) AS EXPIRES_DATE_KEY,
                      NVL(LAST_LOGGED_IN_KEY, -999) AS LAST_LOGGED_IN_DATE_KEY,
                      NVL(ORIGINAL_HIRE_DATE_KEY, -999) AS ORIGINAL_HIRE_DATE_KEY,
                      NVL(LATEST_HIRE_DATE_KEY, -999) AS LATEST_HIRE_DATE_KEY,
                      NVL(ADJUSTED_SERVICE_DATE_KEY, -999) AS ADJUSTED_SERVICE_DATE_KEY,
                      -- F.DAYS_UNTIL_DUE,
                      F.SALARY,
                      F.CURRENCY_CODE,
                      F.LENGTH_OF_SERVICE,
                      F.HEADCOUNT,
                      F.PROGRESS,
                      F.SCORE,
                      F.STATUS,
                      F.TIMESPENT_MIN,
                      F.FTE,
                      NVL(F.MANAGER_FTV_ID, 0) AS MANAGER_EMPLOYEE_NUMBER,
                      F.INSERTED_AT,
                      F.UPDATED_AT
                                             FROM FACT F

                                              LEFT OUTER JOIN {{ref('dim_rating')}} R
                                                ON F.PERFORMANCE_RATING = R.PERFORMANCE_RATING
                                                AND F.POTENTIAL_RATING = R.POTENTIAL_RATING

                                              LEFT OUTER JOIN {{ref("dim_location")}} LOC
                                                ON F.WORK_LOCATION = LOC.WORK_LOCATION
                                                AND F.WORK_CITY = LOC.WORK_CITY
                                                AND F.WORK_STATE = LOC.WORK_STATE
                                                AND F.WORK_COUNTRY = LOC.WORK_COUNTRY
                                                AND F.WORK_REGION = LOC.WORK_REGION

                                              LEFT OUTER JOIN {{ref("dim_job")}} J
                                                ON F.JOB_TITLE = J.JOB_TITLE
                                                AND F.JOB_FUNCTION = J.JOB_FUNCTION
                                                AND F.JOB_FAMILY = J.JOB_FAMILY
                                                AND F.EEO_CATEGORY = J.EEO_CATEGORY
                                                AND F.CAREER_BAND = J.CAREER_BAND
                                                AND F.CAREER_LEVEL = J.CAREER_LEVEL

                                              LEFT OUTER JOIN {{ref("dim_assignment")}} ASS
                                                ON F.ASSIGNMENT_NAME = ASS.ASSIGNMENT_NAME
                                                AND F.ASSIGNMENT_EMPLOYMENT_CATEGORY = ASS.ASSIGNMENT_EMPLOYMENT_CATEGORY
                                                AND F.ASSIGNMENT_CURRENT_CHG_ACTION = ASS.ASSIGNMENT_CURRENT_CHG_ACTION
                                                AND F.ASSIGNMENT_CURRENT_CHG_REASON = ASS.ASSIGNMENT_CURRENT_CHG_REASON
                                                AND F.ASSIGNMENT_STATUS = ASS.ASSIGNMENT_STATUS
                                                AND F.HOURlY_SALARY = ASS.HOURlY_SALARY
                                                AND F.ASSIGNMENT_CURRENT_EFFECTIVE_DATE = ASS.ASSIGNMENT_CURRENT_EFFECTIVE_DATE
                                                AND F.GRADE_NAME = ASS.GRADE_NAME

                                              LEFT OUTER JOIN {{ref("dim_segment")}} SEG
                                                ON F.SEGMENT = SEG.SEGMENT
                                                AND F.OPCO = SEG.OPCO
                                                AND F.SUB_OPCO = SEG.SUB_OPCO
                                                AND F.DEPARTMENT_NAME = SEG.DEPARTMENT_NAME

                                              LEFT OUTER JOIN {{ref("dim_entity")}} B
                                                ON F.BUSINESS_UNIT_NAME = B.BUSINESS_UNIT_NAME
                                                AND F.LEGAL_ENTITY_NAME = B.LEGAL_ENTITY_NAME

                                              LEFT OUTER JOIN {{ref("dim_course")}} C
                                                ON F.COURSE = C.COURSE
                                                AND F.COURSE_NAME = C.COURSE_NAME
                                                AND F.COURSE_TYPE = C.COURSE_TYPE
                                                AND F.COURSE_VERSION = C.COURSE_VERSION
                                                AND F.VENDOR = C.VENDOR
                                                AND F.LANGUAGE = C.LANGUAGE

                                              LEFT OUTER JOIN {{ref("dim_enrollment")}} EN
                                                ON F.IS_ENROLLED = EN.IS_ENROLLED
                                                AND F.ENROLLMENT_METHOD = EN.ENROLLMENT_METHOD
                                                AND F.ATTAINED_CERTIFICATE = EN.ATTAINED_CERTIFICATE
                                                AND F.IS_DELETED = EN.IS_DELETED
                                                AND F.IS_ACTIVE = EN.IS_ACTIVE
