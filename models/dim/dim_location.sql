{{
 config(
 materialized = 'table',
 )
}}


  SELECT DISTINCT   md5(WORK_LOCATION || WORK_CITY || WORK_STATE || WORK_COUNTRY || WORK_REGION) AS LOCATION_KEY,
    WORK_LOCATION,
    WORK_CITY,
    WORK_STATE,
    WORK_COUNTRY,
    WORK_REGION
  FROM {{ref("active_employees_cleaned")}}


