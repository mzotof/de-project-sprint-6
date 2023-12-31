INSERT INTO STV2023121143__DWH.l_groups_dialogs (hk_l_groups_dialogs,hk_message_id,hk_group_id,load_dt,load_src)
select
hash(hd.hk_message_id,hg.hk_group_id),
hd.hk_message_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from STV2023121143__STAGING.dialogs as d
inner join STV2023121143__DWH.h_dialogs as hd on d.message_id = hd.message_id
inner join STV2023121143__DWH.h_groups as hg on d.message_group = hg.group_id
where hash(hd.hk_message_id,hg.hk_group_id) not in (select hk_l_admin_id from STV2023121143__DWH.l_admins);
