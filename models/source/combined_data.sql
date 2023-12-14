WITH
A AS (
  SELECT * FROM {{ ref('active_employees_cleaned') }}
),
C AS (
  SELECT * FROM {{ ref('course_data_raw') }}
)

SELECT * FROM C
JOIN A ON A.FTV_ID = C.EMPLOYEE_NUMBER
