drop table if exists stv2023121143__staging.groups cascade;
create table stv2023121143__staging.groups (
    id int not null,
    admin_id int,
    group_name varchar(100),
    registration_dt timestamp,
    is_private boolean
)
order by id, admin_id
SEGMENTED BY HASH(id) ALL NODES
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);
