INSERT INTO STV2023121143__DWH.s_group_name(hk_group_id, group_name,load_dt,load_src)
select hg.hk_group_id,
sg.group_name,
now() as load_dt,
's3' as load_src
from STV2023121143__DWH.h_groups as hg
left join STV2023121143__STAGING.groups as sg on hg.group_id = sg.id
where hg.hk_group_id not in (select hk_group_id from STV2023121143__DWH.s_group_name);
