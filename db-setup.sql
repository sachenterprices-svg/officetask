-- BSNL Proposal Generator Database Setup
-- Run this after MySQL is installed

CREATE DATABASE IF NOT EXISTS proposal_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE proposal_db;

CREATE TABLE IF NOT EXISTS proposals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    customer_category VARCHAR(100),
    circle VARCHAR(100),
    oa VARCHAR(100),
    sender_name VARCHAR(255),
    sender_designation VARCHAR(255),
    sender_mobile VARCHAR(20),
    proposal_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Sample data for testing
INSERT INTO proposals (customer_name, customer_category, circle, oa, sender_name, sender_designation, sender_mobile, proposal_data)
VALUES ('Test Hospital', 'Hospital', 'UP East', 'Lucknow', 'Test User', 'JTO', '9876543210', '{"channels": 10, "plan": "SIP Trunk"}');

SELECT 'Database and table created successfully!' AS status;
