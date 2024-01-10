{{
 config(
 materialized = 'table',
 )
}}

  SELECT DISTINCT   md5(BUSINESS_UNIT_NAME || LEGAL_ENTITY_NAME) AS ENTITY_KEY, BUSINESS_UNIT_NAME, LEGAL_ENTITY_NAME
  FROM {{ref("active_employees_cleaned")}}

 