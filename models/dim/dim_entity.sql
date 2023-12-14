{{
 config(
 materialized = 'table',
 )
}}

WITH L AS (
  SELECT DISTINCT BUSINESS_UNIT_NAME, LEGAL_ENTITY_NAME
  FROM {{ref("combined_data")}}
)

SELECT
  ROW_NUMBER() OVER (ORDER BY BUSINESS_UNIT_NAME, LEGAL_ENTITY_NAME) AS ENTITY_KEY,
  *
FROM  L


