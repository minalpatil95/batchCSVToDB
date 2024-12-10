show databases;
create database springboot;
use springboot;
create table product(
                        id int NOT NULL PRIMARY KEY,
                        name varchar(20),
                        description varchar(20),
                        price decimal
);

drop table product;
select * from product;

insert into product(name, description, price) values("Pen", "Good", 20);
insert into product(name, description, price) values("Car", "Super", 50);
insert into product(name, description, price) values("Book", "Awsome", 60);