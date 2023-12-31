drop table if exists stv2023121143__staging.users cascade;
create table stv2023121143__staging.users (
    id int not null,
    chat_name varchar(200),
    registration_dt timestamp,
    country varchar(200),
    age int
)
order by id
segmented by hash(id) all nodes;
