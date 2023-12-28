{{
 config(
 materialized = 'table',
 )
}}


  SELECT DISTINCT   md5(SEGMENT || OPCO || SUB_OPCO || DEPARTMENT_NAME) AS SEGMENT_KEY,

    SEGMENT,
    OPCO,
    SUB_OPCO,
    DEPARTMENT_NAME
  FROM {{ref("active_employees_cleaned")}}

