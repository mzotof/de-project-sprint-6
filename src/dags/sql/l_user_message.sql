INSERT INTO STV2023121143__DWH.l_user_message (hk_l_user_message,hk_user_id,hk_message_id,load_dt,load_src)
select
hash(hu.hk_user_id,hd.hk_message_id),
hu.hk_user_id,
hd.hk_message_id,
now() as load_dt,
's3' as load_src
from STV2023121143__STAGING.dialogs as d
inner join STV2023121143__DWH.h_dialogs as hd on d.message_id = hd.message_id
inner join STV2023121143__DWH.h_users as hu on d.message_from = hu.user_id
where hash(hu.hk_user_id,hd.hk_message_id) not in (select hk_l_admin_id from STV2023121143__DWH.l_admins);
INSERT INTO STV2023121143__DWH.l_user_message (hk_l_user_message,hk_user_id,hk_message_id,load_dt,load_src)
select
hash(hu.hk_user_id,hd.hk_message_id),
hu.hk_user_id,
hd.hk_message_id,
now() as load_dt,
's3' as load_src
from STV2023121143__STAGING.dialogs as d
inner join STV2023121143__DWH.h_dialogs as hd on d.message_id = hd.message_id
inner join STV2023121143__DWH.h_users as hu on d.message_to = hu.user_id
where hash(hu.hk_user_id,hd.hk_message_id) not in (select hk_l_admin_id from STV2023121143__DWH.l_admins);