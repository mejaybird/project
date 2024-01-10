{{
  config(materialized='table')
}}

select distinct EMAIL_ADDRESS, LEGAL_ENTITY_NAME from active_employees_cleaned