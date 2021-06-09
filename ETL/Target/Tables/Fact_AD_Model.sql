-- depends_on: {{ ref('Temp_GA_ADs') }}
-- depends_on: {{ ref('Temp_Facebook') }}
-- depends_on: {{ ref('Temp_Linkedin') }}


select * from {{ ref('Temp_GA_ADs')}}
union all
select * from {{ ref('Temp_Facebook')}}
union all
select * from {{ ref('Temp_Linkedin')}}
