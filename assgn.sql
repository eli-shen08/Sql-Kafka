-- select database();

create table if not exists products1(
id int primary key auto_increment,
name varchar(255) not null,
category varchar(255) not null,
price float not null,
last_updated timestamp default current_timestamp on update current_timestamp
);

desc products;

INSERT INTO products1 (name, category, price) VALUES
('Phone A1', 'Electronics', 299.99),
('Phone A2', 'Electronics', 349.99),
('Laptop B1', 'Electronics', 899.50),
('Laptop B2', 'Electronics', 999.00),
('Headphones X1', 'Electronics', 59.99),
('Headphones X2', 'Electronics', 79.49),
('Smartwatch Z1', 'Electronics', 149.99),
('Smartwatch Z2', 'Electronics', 199.99),
('Camera C1', 'Electronics', 450.00),
('Camera C2', 'Electronics', 699.99),
('T-Shirt Blue', 'Apparel', 19.99),
('T-Shirt Black', 'Apparel', 21.49),
('T-Shirt White', 'Apparel', 18.99),
('Jeans Slim Fit', 'Apparel', 45.00),
('Jeans Regular Fit', 'Apparel', 49.99),
('Jacket Leather', 'Apparel', 120.00),
('Jacket Denim', 'Apparel', 85.50),
('Shoes Running', 'Apparel', 65.00),
('Shoes Casual', 'Apparel', 59.99),
('Shoes Formal', 'Apparel', 89.99);

select * from products1;

INSERT INTO products1 (name, category, price) VALUES
('Mixer 500W', 'Home', 89.99),
('Mixer 750W', 'Home', 119.99),
('Microwave 20L', 'Home', 130.00),
('Microwave 30L', 'Home', 180.00),
('Refrigerator 190L', 'Home', 350.00),
('Refrigerator 250L', 'Home', 499.00),
('Washing Machine 6kg', 'Home', 299.99),
('Washing Machine 8kg', 'Home', 450.00),
('Vacuum Cleaner Basic', 'Home', 120.00),
('Vacuum Cleaner Pro', 'Home', 200.00);

insert into products1 (name, category,price) values
('Book - Fiction 1', 'Books', 9.99),
('Book - Fiction 2', 'Books', 11.99),
('Book - Fiction 3', 'Books', 13.50),
('Book - Non-Fiction 1', 'Books', 15.00),
('Book - Non-Fiction 2', 'Books', 20.00),
('Book - Science', 'Books', 25.00),
('Book - Programming', 'Books', 35.00),
('Book - AI Basics', 'Books', 40.00),
('Book - Data Science', 'Books', 45.00),
('Book - Machine Learning', 'Books', 50.00);

