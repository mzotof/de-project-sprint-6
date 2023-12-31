drop table if exists STV2023121143__DWH.s_user_socdem;
create table STV2023121143__DWH.s_user_socdem
(
hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_h_users REFERENCES STV2023121143__DWH.h_users (hk_user_id),
country varchar(200),
age int,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
