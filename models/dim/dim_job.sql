{{
    config(
        materialized= 'table'
    )
}}
 
 
 
  SELECT DISTINCT   md5(JOB_TITLE || JOB_FUNCTION || JOB_FAMILY || EEO_CATEGORY || CAREER_BAND || CAREER_LEVEL) AS JOB_KEY,
    JOB_TITLE,
    JOB_FUNCTION,
    JOB_FAMILY,
    EEO_CATEGORY,
    CAREER_BAND,
    CAREER_LEVEL
  FROM {{ref("active_employees_cleaned")}}
