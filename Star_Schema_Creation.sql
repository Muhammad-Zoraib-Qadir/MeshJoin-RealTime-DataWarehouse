CREATE DATABASE IF NOT EXISTS Metro;
CREATE USER IF NOT EXISTS 'Admin'@'localhost' IDENTIFIED BY 'datawarehouse';
GRANT ALL PRIVILEGES ON Metro.* TO 'Admin'@'localhost';
FLUSH PRIVILEGES;
USE Metro;
-- Drop existing tables if they exist
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS dates;
DROP TABLE IF EXISTS times;
-- Create Customers Table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    gender VARCHAR(10) NOT NULL
);
-- Create Products Table
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_price DECIMAL(10, 2) NOT NULL,
    supplier_id INT NOT NULL,
    supplier_name VARCHAR(255) NOT NULL,
    store_id INT NOT NULL,
    store_name VARCHAR(255) NOT NULL
);
-- Create Dates Table
CREATE TABLE dates (
    date_id INT PRIMARY KEY AUTO_INCREMENT,
    date DATE NOT NULL,
    day_of_week VARCHAR(50) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL
);
-- Create Times Table
CREATE TABLE times (
    time_id INT PRIMARY KEY AUTO_INCREMENT,
    hour INT NOT NULL,
    minute INT NOT NULL,
    second INT NOT NULL
);
-- Create Sales Table
CREATE TABLE sales (
    order_id INT PRIMARY KEY,
    date_id INT NOT NULL,
    time_id INT NOT NULL,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    quantity_ordered INT NOT NULL,
    total_sale DECIMAL(10, 2),
    FOREIGN KEY (date_id) REFERENCES dates(date_id),
    FOREIGN KEY (time_id) REFERENCES times(time_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
