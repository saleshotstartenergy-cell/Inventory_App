-- Schema export for database `railway`
-- Generated: 2025-11-12T13:13:37.352768

SET FOREIGN_KEY_CHECKS = 0;

-- --------------------------------------------------
-- Table: etl_state
-- --------------------------------------------------
CREATE TABLE `etl_state` (
  `id` varchar(64) NOT NULL,
  `last_sync` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------
-- Table: movement_hashes
-- --------------------------------------------------
CREATE TABLE `movement_hashes` (
  `id` int NOT NULL AUTO_INCREMENT,
  `dedupe_key` char(40) NOT NULL,
  `movement_id` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `dedupe_key` (`dedupe_key`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------
-- Table: sales
-- --------------------------------------------------
CREATE TABLE `sales` (
  `id` int NOT NULL AUTO_INCREMENT,
  `date` date DEFAULT NULL,
  `voucher_no` varchar(100) DEFAULT NULL,
  `company` varchar(255) DEFAULT NULL,
  `item` varchar(255) DEFAULT NULL,
  `qty` decimal(20,4) DEFAULT NULL,
  `rate` decimal(20,4) DEFAULT NULL,
  `amount` decimal(20,4) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------
-- Table: stock_items
-- --------------------------------------------------
CREATE TABLE `stock_items` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `category` varchar(255) DEFAULT NULL,
  `base_unit` varchar(100) DEFAULT NULL,
  `opening_qty` decimal(20,4) DEFAULT NULL,
  `opening_rate` decimal(20,4) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  UNIQUE KEY `ux_stock_items_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------
-- Table: stock_movements
-- --------------------------------------------------
CREATE TABLE `stock_movements` (
  `id` int NOT NULL AUTO_INCREMENT,
  `date` date DEFAULT NULL,
  `voucher_no` varchar(100) DEFAULT NULL,
  `company` varchar(255) DEFAULT NULL,
  `item` varchar(255) DEFAULT NULL,
  `qty` decimal(20,4) DEFAULT NULL,
  `rate` decimal(20,4) DEFAULT NULL,
  `amount` decimal(20,4) DEFAULT NULL,
  `movement_type` varchar(10) DEFAULT NULL,
  `movement_hash` varchar(64) DEFAULT NULL,
  `source` varchar(50) DEFAULT 'tally',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_movements_hash` (`movement_hash`),
  KEY `idx_movements_item_date` (`item`,`date`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------
-- Table: stock_reservations
-- --------------------------------------------------
CREATE TABLE `stock_reservations` (
  `id` int NOT NULL AUTO_INCREMENT,
  `item` varchar(255) NOT NULL,
  `reserved_by` varchar(255) NOT NULL,
  `qty` decimal(10,2) NOT NULL,
  `start_date` date NOT NULL DEFAULT (curdate()),
  `end_date` date NOT NULL,
  `status` enum('ACTIVE','EXPIRED','CANCELLED') DEFAULT 'ACTIVE',
  `remarks` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_res_item_status_enddate` (`item`,`status`,`end_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------
-- Table: users
-- --------------------------------------------------
CREATE TABLE `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `username` varchar(100) NOT NULL,
  `password_hash` varchar(255) NOT NULL,
  `role` enum('admin','sales','customer') NOT NULL DEFAULT 'sales',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SET FOREIGN_KEY_CHECKS = 1;
