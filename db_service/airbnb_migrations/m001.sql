create schema airbnb;
set search_path to airbnb;

create table users(
    id serial primary key,
    name text not null
);

create table villas(
    id serial primary key,
    rate int not null,
    city text not null
);

create table uservillas(
  userid int references users(id) on delete cascade,
  villaid int references villas(id) on delete cascade
);

create table overbookings(
    villaid int references villas(id) on delete cascade,
    timestamp timestamp not null
);