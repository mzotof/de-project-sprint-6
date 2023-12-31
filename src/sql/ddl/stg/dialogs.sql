drop table if exists stv2023121143__staging.dialogs cascade;
create table stv2023121143__staging.dialogs (
    message_id int not null,
    message_ts timestamp,
    message_from int,
    message_to int,
    message varchar(1000),
    message_group int
)
order by message_id
segmented by hash(message_id) all nodes
partition by message_ts::date
group by calendar_hierarchy_day(message_ts::date, 3, 2);
