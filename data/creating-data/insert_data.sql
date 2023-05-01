insert into products values (product_name, price, quantity, category_id, brand_id, create_at)
("bread1", 12, 100, 1, 1, 1680531785),
("bread2", 10, 200, 2, 1, 1680531785),
("bread3", 9, 300, 3, 2, 1680531785),
("bread4", 8, 200, 4, 3, 1680531785);


insert into users values (username, fulllname, email, address, phone_number)
('user1', "van user1", "user1@gmail.com", "000011112222"),
('user2', "van user2", "user2@gmail.com", "000011112222"),
('user3', "van user3", "user3@gmail.com", "000011112222"),
('user4', "van user4", "user4@gmail.com", "000011112222"),
('user5', "van user5", "user5@gmail.com", "000011112222");


insert into orders values (user_id, payment, status)
(1, 'carsh', 'confirmed'),
(2, 'master card', 'confirmed'),
(3, 'visa', 'confirmed'),
(4, 'visa', 'confirmed');


insert into orders values (user_id, payment, status)
(1, 'carsh', 'confirmed'),
(2, 'master card', 'confirmed'),
(3, 'visa', 'confirmed'),
(4, 'visa', 'confirmed');
