{{
  config(materialized='table')
}}

select distinct EMAIL_ADDRESS, OPCO from active_employees_cleaned