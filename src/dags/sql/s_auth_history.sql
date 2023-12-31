INSERT INTO STV2023121143__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)
select luga.hk_l_user_group_activity,
gl.user_id_from,
gl.event,
gl.datetime,
now() as load_dt,
's3' as load_src
from STV2023121143__DWH.l_user_group_activity as luga
left join STV2023121143__DWH.h_users as hu on luga.hk_user_id = hu.hk_user_id
left join STV2023121143__DWH.h_groups as hg on luga.hk_group_id = hg.hk_group_id
left join STV2023121143__STAGING.group_log as gl
    on hu.user_id = gl.user_id
    and hg.group_id = gl.group_id
where luga.hk_l_user_group_activity not in (select hk_l_user_group_activity from STV2023121143__DWH.s_auth_history);
