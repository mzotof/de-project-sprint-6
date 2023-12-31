drop table if exists stv2023121143__staging.group_log cascade;
create table stv2023121143__staging.group_log (
    group_id int,
    user_id int,
    user_id_from int,
    event varchar(10),
    datetime timestamp
)
order by group_id, user_id
segmented by hash(user_id) all nodes
partition by datetime::date
group by calendar_hierarchy_day(datetime::date, 3, 2);
