INSERT INTO STV2023121143__DWH.h_dialogs(hk_message_id, message_id,message_ts,load_dt,load_src)
select
       hash(message_id) as  hk_message_id,
       message_id as message_id,
       message_ts,
       now() as load_dt,
       's3' as load_src
       from STV2023121143__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from STV2023121143__DWH.h_dialogs);
