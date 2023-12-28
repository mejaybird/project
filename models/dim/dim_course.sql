{{
 config(
 materialized = 'table',
 )
}}

    SELECT distinct md5(COURSE || COURSE_NAME || COURSE_TYPE || COURSE_VERSION || LANGUAGE || VENDOR) as course_key,
COURSE,
COURSE_NAME,
COURSE_TYPE,
COURSE_VERSION,
LANGUAGE,
VENDOR

 FROM 
{{ref("combined_data")}}