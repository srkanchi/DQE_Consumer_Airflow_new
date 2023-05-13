select distinct 
document_type.decode1, 
tdr.td_key as parent_td,
ft.tpt_id_key,
concat(status.code_value,': ',status.decode1) as status
from scout_internal.field_testing ft 
left join scout_internal.td_reference tdr on tdr.field_testing_id = ft.field_testing_id
left join 
(
select distinct 
ft.tpt_id_key,
status.code_Value as status_code
from scout_internal.field_testing ft 
left join scout_internal.master_code status on status.code_id = ft.status_code_id
where ft.field_testing_type in ('1') and status.code_value in ('0','1') and 
substring(ft.tpt_id_key,10,12) in ('ADR') and ft.field_year in ('2022') 
) adr on adr.tpt_id_key = tdr.td_key 
left join scout_internal.master_code status on status.code_id = ft.status_code_id
left join scout_internal.master_code document_type on document_type.code_id = ft.field_testing_type_code_id
where ft.field_testing_type in ('3') and tdr.td_key in (adr.tpt_id_key)