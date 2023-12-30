with course_data_raw as (
      select * from {{ source('JAY','course_data') }}
)
select  ATTAINED_CERTIFICATE,
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
DAYS_UNTIL_DUE,
EMPLOYEE_NUMBER,
ENROLLMENT_ID,
ENROLLMENT_METHOD,
IS_ENROLLED,
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
NVL(VENDOR, 'NA') AS VENDOR from course_data_raw
  