INSERT INTO STV2023121143__DWH.s_dialog_info(hk_message_id, message,message_from,message_to,load_dt,load_src)
select hd.hk_message_id,
sd.message,
sd.message_from,
sd.message_to,
now() as load_dt,
's3' as load_src
from STV2023121143__DWH.h_dialogs as hd
left join STV2023121143__STAGING.dialogs as sd on hd.message_id = sd.message_id
where hd.hk_message_id not in (select hk_message_id from STV2023121143__DWH.s_dialog_info);
