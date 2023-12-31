drop table if exists STV2023121143__DWH.s_auth_history;
create table STV2023121143__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES STV2023121143__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from bigint,
event varchar(10),
event_dt timestamp,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
