{% for i in range(10) %}
with recursive Emp_Hier as (
 select * from 
 (
 select distinct 
 a.entity_id,
 b.source_Emp_id as employee_id, 
 b.emp_position_level, 
 b.emp_role_id,
 a.mngr_emp_id
 a.mngr_position_level,
 a.mngr_role_id
 from 
 {{ ref('Dim_Employee') }} a
 join {{ ref('Dim_Employee') }} b
 on a.source_Emp_id = b.mngr_emp_id
 ) rslt
 where not exists 
 (Select 1 from {{ ref('Dim_Employee') }} e where rslt.employee_id = e.source_Emp_id and rslt.mngr_emp_id = e.mngr_emp_id)
 )
 {% if not loop.last %} union all {% endif %}
{% endfor %}

 select * from Emp_Hier