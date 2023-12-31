drop table if exists STV2023121143__DWH.s_group_name;
create table STV2023121143__DWH.s_group_name
(
hk_group_id bigint not null CONSTRAINT fk_s_group_name_h_groups REFERENCES STV2023121143__DWH.h_groups (hk_group_id),
group_name varchar(100),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
