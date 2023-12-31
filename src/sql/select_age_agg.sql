select su.age, count(1)
from STV2023121143__DWH.l_user_message lu
inner join STV2023121143__DWH.s_user_socdem su
on lu.hk_user_id = su.hk_user_id
where hk_message_id in (
    select hk_message_id
    from STV2023121143__DWH.l_groups_dialogs
    where hk_group_id in (
        select hk_group_id
        from STV2023121143__DWH.h_groups
        order by registration_dt limit 10
    )
)
group by su.age
order by 2 desc;
