create database manager;
use manager; 

CREATE TABLE customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(15),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    sku VARCHAR(50) UNIQUE NOT NULL,
    category VARCHAR(50),
    base_price DECIMAL(10,2) CHECK (base_price > 0),
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE sales_channels (
    id INT AUTO_INCREMENT PRIMARY KEY,
    channel_name VARCHAR(50) NOT NULL UNIQUE,
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE payment_methods (
    id INT AUTO_INCREMENT PRIMARY KEY,
    method_name VARCHAR(50) NOT NULL UNIQUE,
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE sales (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    sale_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    payment_method_id INT NOT NULL,
    sales_channel_id INT NOT NULL,
    total_amount DECIMAL(12,2),
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (payment_method_id) REFERENCES payment_methods(id),
    FOREIGN KEY (sales_channel_id) REFERENCES sales_channels(id)
);

CREATE TABLE sale_line_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sale_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT CHECK (quantity > 0),
    unit_price DECIMAL(10,2) CHECK (unit_price >= 0),
    line_total DECIMAL(12,2),
    FOREIGN KEY (sale_id) REFERENCES sales(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

#Integrity using Trigger
DELIMITER //
CREATE TRIGGER trg_line_total_before_insert
BEFORE INSERT ON sale_line_items
FOR EACH ROW
BEGIN
    SET NEW.line_total = NEW.quantity * NEW.unit_price;
END;
//
DELIMITER ;

INSERT INTO customers (name, email, phone, created_at) VALUES
('Alice Johnson', 'alice.johnson@example.com', '9876543210', '2025-01-15 10:30:00'),
('Bob Smith', 'bob.smith@example.com', '9123456780', '2025-02-10 14:45:00'),
('Charlie Brown', 'charlie.brown@example.com', '9988776655', '2025-03-05 09:20:00'),
('Diana Prince', 'diana.prince@example.com', '9871203456', '2025-03-25 16:40:00'),
('Ethan Hunt', 'ethan.hunt@example.com', '9765432109', '2025-04-01 11:05:00');

INSERT INTO products (name, sku, category, base_price, active) VALUES
('Wireless Mouse', 'SKU1001', 'Electronics', 800.00, TRUE),
('Gaming Keyboard', 'SKU1002', 'Electronics', 2500.00, TRUE),
('Office Chair', 'SKU1003', 'Furniture', 4500.00, TRUE),
('Water Bottle', 'SKU1004', 'Accessories', 300.00, TRUE),
('Bluetooth Speaker', 'SKU1005', 'Electronics', 1500.00, TRUE);

INSERT INTO sales_channels (channel_name, active) VALUES
('Online Store', TRUE),
('Retail Outlet', TRUE),
('Mobile App', TRUE),
('Wholesale', TRUE),
('Partner Store', TRUE);

INSERT INTO payment_methods (method_name, active) VALUES
('Credit Card', TRUE),
('Debit Card', TRUE),
('UPI', TRUE),
('Cash', TRUE),
('Net Banking', TRUE);

INSERT INTO sales (customer_id, sale_date, payment_method_id, sales_channel_id, total_amount) VALUES
(1, '2025-03-10 12:15:00', 1, 1, 3300.00), -- Alice via Credit Card, Online
(2, '2025-03-15 15:45:00', 2, 2, 5000.00), -- Bob via Debit Card, Retail
(3, '2025-04-01 10:30:00', 3, 3, 800.00),  -- Charlie via UPI, Mobile App
(4, '2025-04-05 18:20:00', 4, 2, 4800.00), -- Diana via Cash, Retail
(5, '2025-04-10 09:50:00', 5, 1, 1500.00); -- Ethan via Net Banking, Online

INSERT INTO sale_line_items (sale_id, product_id, quantity, unit_price, line_total) VALUES
(1, 1, 2, 800.00, 1600.00),   -- Wireless Mouse x2
(1, 5, 1, 1700.00, 1700.00),  -- Bluetooth Speaker
(2, 3, 1, 5000.00, 5000.00),  -- Office Chair
(3, 1, 1, 800.00, 800.00),    -- Wireless Mouse
(4, 2, 1, 2500.00, 2500.00),  -- Gaming Keyboard
(4, 4, 2, 1150.00, 2300.00),  -- Water Bottle (special price)
(5, 5, 1, 1500.00, 1500.00);  -- Bluetooth Speaker

Select * from customers;
Select * from products;
Select * from sales_channels;
Select * from payment_methods;
Select * from sales;
Select * from sale_line_items;

SELECT DATE_FORMAT(sale_date, '%Y-%m') AS month,
       SUM(total_amount) AS total_revenue
FROM sales
GROUP BY month
ORDER BY month;

SELECT DATE_FORMAT(sale_date, '%Y-%m') AS month,
       AVG(total_amount) AS avg_order_value
FROM sales
GROUP BY month
ORDER BY month;

SELECT p.name,
       sli.unit_price,
       p.base_price,
       (sli.unit_price - p.base_price) AS deviation
FROM sale_line_items sli
JOIN products p ON sli.product_id = p.id;

SELECT sc.channel_name,
       SUM(s.total_amount) AS revenue
FROM sales s
JOIN sales_channels sc ON s.sales_channel_id = sc.id
GROUP BY sc.channel_name;

SELECT pm.method_name,
       SUM(s.total_amount) AS revenue
FROM sales s
JOIN payment_methods pm ON s.payment_method_id = pm.id
GROUP BY pm.method_name;







