{{
  config
  (materialized='incremental',
    unique_key='enrollment_id',
    merge_exclude_columns = ['inserted_at'])
}}

with course_data_raw as (
      select * from {{ source('JAY','incremental_load') }}
)
select  NVL(ATTAINED_CERTIFICATE, 'NA') AS ATTAINED_CERTIFICATE,
CERTIFICATE_DATE,
COURSE,
COURSE_NAME,
COURSE_TYPE,
NVL(COURSE_VERSION, 'NA') AS COURSE_VERSION,
DATE_COMPLETED,
DATE_EDITED,
DATE_ENROLLED,
DATE_EXPIRES,
DATE_HIRED,
DATE_STARTED,
-- DAYS_UNTIL_DUE,
EMPLOYEE_NUMBER,
ENROLLMENT_ID,
NVL(ENROLLMENT_METHOD, 'NA') AS ENROLLMENT_METHOD,
NVL(IS_ENROLLED, 'NA') AS IS_ENROLLED,
NVL(IS_ACTIVE, 'NA') AS IS_ACTIVE,
CASE
    WHEN IS_DELETED = 'TRUE' THEN 'True'
    WHEN IS_DELETED = 'FALSE' THEN 'False'
    WHEN IS_DELETED IS NULL THEN 'NA'
ELSE IS_DELETED
END AS IS_DELETED,
LANGUAGE,
LAST_LOGGEDIN,
PROGRESS,
SCORE,
CASE
    WHEN STATUS = 'NotComplete' THEN 'Not Complete'
    WHEN STATUS = 'NotStarted' THEN 'Not Started'
    WHEN STATUS = 'InProgress' THEN 'In Progress'
    WHEN STATUS = 'PendingApproval' THEN 'Pending Approval'
    WHEN STATUS = 'PendingEvaluationRequired' THEN 'Pending Evaluation Required'
    WHEN STATUS = 'NotApplicable' THEN 'Not Applicable'
    WHEN STATUS = 'N/A' THEN 'Not Applicable'
    ELSE STATUS
  END AS STATUS,
TIMESPENT_MIN,
NVL(VENDOR, 'NA') AS VENDOR,
CURRENT_TIMESTAMP() as INSERTED_AT,
CURRENT_TIMESTAMP() as UPDATED_AT
          from course_data_raw

--           {% if is_incremental() %}

-- where
--   updated_at > (select max(updated_at) from {{ this }})

-- {% endif %}
