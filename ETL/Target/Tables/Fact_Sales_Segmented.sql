--depends_on: {{ ref('Temp_Sales_Segmented') }}
-- depends_on: {{ ref('Dim_TimeFrame') }}

{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE in('SF','HS')", "ENTITY_DATASORUCE_NAME||'#'||HISTORY_LOAD||'#'||TO_VARCHAR(nvl(HISTORY_START_DATE,HISTORY_ACTUAL_START_DATE)::DATE, 'DD/MM/YYYY')||'#'||TO_VARCHAR(HISTORY_END_DATE::DATE, 'DD/MM/YYYY')") %}
{% set pits = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".TEMP_SALES_SEGMENTED", "METRIC_ID||'#'||POINT_IN_TIME") %}

{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{{
    config(
        materialized='incremental',
        unique_key= 'Report_Date1'
      )
	  
}}

{% for V_SF_Schema in results %}
{% set entity_name, hist_load, hist_strt_dt, hist_end_dt= V_SF_Schema.split('#') %}
    {% if  hist_load  == 'true' %} 
		   {% for V_PIT in pits %}
		   {% set metricid, pit= V_PIT.split('#') %}
					{% if  pit  == 'TRUE' %} 
							{%- for metrics in [(fact_table_pit_segmented_hist(entity_name,metricid,hist_strt_dt,hist_end_dt))                
								  ]  %}
								  (
								  {{ metrics }} 
								  )
							  
								{% if not loop.last -%}
									union all
								{% endif -%}
							{%- endfor -%}  
					{% else -%}
							{%- for metrics in [(fact_table_segmented_hist(entity_name,metricid,hist_strt_dt,hist_end_dt))                
								  ]  %}
								  (
								  {{ metrics }}
								  )
							  
								{% if not loop.last -%}
									union all
								{% endif -%}
							{%- endfor -%}  
					{% endif -%}
			    {% if not loop.last -%}
					union all
				{% endif -%}
			{%- endfor -%} 
    {%- else -%}
        {% for V_PIT in pit %}
        {% set metricid, pit= V_PIT.split('#') %}
					{% if  pit  == 'TRUE' %} 
						{%- for metrics in [(fact_table_pit_segmented(entity_name,metricid))                
								  ]  %}
							  (
							  {{ metrics }}
							  )
						  
							{% if not loop.last -%}
								union all
							{% endif -%}
						{%- endfor -%}  
					{% else -%}
						{%- for metrics in [(fact_table_segmented(entity_name,metricid))                
								  ]  %}
								  (
								  {{ metrics }}
								  )
							  
							{% if not loop.last -%}
								union all
							{% endif -%}
						{%- endfor -%}  
					{% endif -%}	
		    {% if not loop.last -%}
				union all
			{% endif -%}
		{%- endfor -%}	   
    {% endif -%} 
  {% if not loop.last -%}
		union all
	{% endif -%}	
{%- endfor -%}