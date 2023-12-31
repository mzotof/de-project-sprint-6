INSERT INTO STV2023121143__DWH.s_user_socdem(hk_user_id,country,age,load_dt,load_src)
select hu.hk_user_id,
su.country,
su.age,
now() as load_dt,
's3' as load_src
from STV2023121143__DWH.h_users as hu
left join STV2023121143__STAGING.users as su on hu.user_id = su.id
where hu.hk_user_id not in (select hk_user_id from STV2023121143__DWH.s_user_socdem);
