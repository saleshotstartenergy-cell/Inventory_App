-- ----------------------
-- STOCK MOVEMENTS
-- ----------------------
DROP TABLE IF EXISTS stock_movements;

CREATE TABLE stock_movements (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    voucher_no VARCHAR(100),
    company VARCHAR(255),        
    item VARCHAR(255),
    qty DECIMAL(20,4),
    rate DECIMAL(20,4),
    amount DECIMAL(20,4),
    movement_type VARCHAR(10)
);

-- ----------------------
-- STOCK ITEMS
-- ----------------------
DROP TABLE IF EXISTS stock_items;

CREATE TABLE stock_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) UNIQUE,     -- ✅ matches ETL
    category VARCHAR(255),
    base_unit VARCHAR(100),
    opening_qty DECIMAL(20,4),
    opening_rate DECIMAL(20,4)
);

-- ----------------------
-- SALES (optional, for reporting)
-- ----------------------
DROP TABLE IF EXISTS sales;

CREATE TABLE sales (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    voucher_no VARCHAR(100),
    company VARCHAR(255),        -- ✅ keep company for consistency
    item VARCHAR(255),
    qty DECIMAL(20,4),
    rate DECIMAL(20,4),
    amount DECIMAL(20,4)
);
CREATE TABLE stock_reservations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    item VARCHAR(255) NOT NULL,
    reserved_by VARCHAR(255) NOT NULL,
    qty DECIMAL(10,2) NOT NULL,
    start_date DATE NOT NULL DEFAULT (CURRENT_DATE),
    end_date DATE NOT NULL,
    status ENUM('ACTIVE','EXPIRED','CANCELLED') DEFAULT 'ACTIVE',
    remarks VARCHAR(255)
);

-- ----------------------
-- USERS 
-- ----------------------
CREATE TABLE IF NOT EXISTS users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  username VARCHAR(100) NOT NULL UNIQUE,
  password_hash VARCHAR(255) NOT NULL,
  role ENUM('admin','sales') NOT NULL DEFAULT 'sales',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
