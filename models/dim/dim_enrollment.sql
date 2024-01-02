{{
  config(materialized='incremental',
    unique_key='ENROLL_KEY',
    incremental_strategy='delete+insert')
}}


    SELECT distinct md5(ATTAINED_CERTIFICATE || IS_ACTIVE || IS_DELETED || ENROLLMENT_METHOD || IS_ENROLLED) AS ENROLL_KEY,
             ATTAINED_CERTIFICATE,IS_ACTIVE,IS_DELETED, ENROLLMENT_METHOD, IS_ENROLLED
 from 
{{ref("combined_data")}}
