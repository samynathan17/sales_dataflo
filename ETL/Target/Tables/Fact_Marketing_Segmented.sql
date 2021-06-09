-- depends_on: {{ ref('Temp_GA_segmented') }}
-- depends_on: {{ ref('Temp_GSC_Segmented') }}


select * from {{ ref('Temp_GA_segmented')}}
union all
select * from {{ ref('Temp_GSC_Segmented')}}
