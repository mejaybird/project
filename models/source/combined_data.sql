WITH
a AS ( SELECT * FROM {{ ref('active_employees_cleaned') }}
),

c AS ( SELECT *  FROM {{ ref('course_data_raw') }}
)

select * from c  join a on a.ftv_id = c.employeenumber