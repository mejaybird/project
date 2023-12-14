{{
 config(
 materialized = 'table',
 )
}}


WITH L AS (
  SELECT DISTINCT
    SEGMENT,
    OPCO,
    SUB_OPCO,
    DEPARTMENT_NAME
  FROM {{ref("combined_data")}}
)

SELECT
  ROW_NUMBER() OVER (ORDER BY SEGMENT, OPCO, SUB_OPCO, DEPARTMENT_NAME) AS SEGMENT_KEY,
  *
FROM L
