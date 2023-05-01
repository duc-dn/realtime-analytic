create database myshop;
use myshop;

create table categories
(
    id int not null primary key auto_increment,
    category_name varchar(255) not null,
    description text,
    created_at int 
);

create table brands
(
    id int not null primary key auto_increment,
    brand_name varchar(255) not null,
    description text,
    created_at int
);

create table products
(
    id int not null primary key auto_increment,
    product_name varchar(255) not null,
    price double not null, 
    category_id int not null,
    brand_id int not null,
    created_at int not null,
    description text
);

create table inventory
(
    id int not null primary key auto_increment,
    product_id int not null unique,
    quantity int not null,
    updated_at int not null
);

create table users
(
    id int not null primary key auto_increment,
    username varchar(100) not null,
    fullname varchar(255) not null,
    email varchar(255) not null,
    address varchar(255) not null,
    phone_number varchar(12) not null,
    created_at int not null
);

create table orders
(
    id int not null primary key auto_increment,
    user_id int not null,
    payment varchar(100) not null,
    status_id int not null,
    created_at int not null
);

create table order_status 
(
    id int not null primary key auto_increment,
    status_name varchar(100) not null
);

create table order_detail
(
    order_id int not null,
    product_id int not null,
    quantity int not null,
    item_price double not null,
    created_at int not null,
    primary key (order_id, product_id)
);

ALTER TABLE products ADD CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES categories(id);
ALTER TABLE products ADD CONSTRAINT fk_brand FOREIGN KEY (brand_id) REFERENCES brands(id);
ALTER TABLE inventory ADD CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES products(id);
ALTER TABLE order_detail ADD CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id);
ALTER TABLE order_detail ADD CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id);
ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);
ALTER TABLE orders ADD CONSTRAINT fk_order_status FOREIGN KEY (status_id) REFERENCES order_status(id);
