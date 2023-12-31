with oldest_groups as (
    select hk_group_id
    from STV2023121143__DWH.h_groups
    order by registration_dt limit 10
), groups_members_cnt as (
    select g.hk_group_id, count(distinct a.hk_user_id) as cnt
    from oldest_groups g
    inner join STV2023121143__DWH.l_user_group_activity a
        on g.hk_group_id = a.hk_group_id
    inner join STV2023121143__DWH.s_auth_history e
        on a.hk_l_user_group_activity = e.hk_l_user_group_activity
        and lower(e.event) = 'add'
    group by g.hk_group_id
), groups_members_with_messages_cnt as (
    select g.hk_group_id, count(distinct ud.hk_user_id) as cnt
    from oldest_groups g
    inner join STV2023121143__DWH.l_groups_dialogs gd
        on g.hk_group_id = gd.hk_group_id
    inner join STV2023121143__DWH.l_user_message ud
        on ud.hk_message_id = gd.hk_message_id
    group by g.hk_group_id
)
select
    a.hk_group_id,
    a.cnt as cnt_added_users,
    coalesce(m.cnt, 0) as cnt_users_in_group_with_messages,
    (coalesce(m.cnt, 0) / a.cnt * 100)::decimal(5, 2) as group_conversion
from groups_members_cnt a
left join groups_members_with_messages_cnt m
on a.hk_group_id = m.hk_group_id;
