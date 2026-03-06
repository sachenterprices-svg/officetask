USE proposal_db;

-- 1. Users Table
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    role ENUM('admin', 'user') DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default admin user (Password: admin123 - will be hashed in Node, but for direct SQL we need a pre-hashed string or handle it in Node startup. 
-- We will handle default user creation in server.js to ensure proper bcrypt hashing.)

-- 2. Categories Table (Inventory)
CREATE TABLE IF NOT EXISTS categories (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Products Table (Inventory)
CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    category_id INT NOT NULL,
    model_name VARCHAR(255) NOT NULL,
    default_price DECIMAL(10, 2) DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
);

-- 4. Alter Proposals Table to support new CRM features
-- Safely add columns using stored procedure (since MySQL doesn't have ADD COLUMN IF NOT EXISTS in all versions)
DROP PROCEDURE IF EXISTS upgrade_proposals;
DELIMITER //
CREATE PROCEDURE upgrade_proposals()
BEGIN
    DECLARE col_exists INT;
    
    -- Check for user_id
    SELECT COUNT(*) INTO col_exists FROM information_schema.columns 
    WHERE table_schema = 'proposal_db' AND table_name = 'proposals' AND column_name = 'user_id';
    IF col_exists = 0 THEN
        ALTER TABLE proposals ADD COLUMN user_id INT;
        -- Can't add exact foreign key yet as older proposals might have NULL user_id, 
        -- but we link logically.
    END IF;

    -- Check for quotation_items
    SELECT COUNT(*) INTO col_exists FROM information_schema.columns 
    WHERE table_schema = 'proposal_db' AND table_name = 'proposals' AND column_name = 'quotation_items';
    IF col_exists = 0 THEN
        ALTER TABLE proposals ADD COLUMN quotation_items JSON;
    END IF;
    
    -- Check for direct_sale
    SELECT COUNT(*) INTO col_exists FROM information_schema.columns 
    WHERE table_schema = 'proposal_db' AND table_name = 'proposals' AND column_name = 'direct_sale';
    IF col_exists = 0 THEN
        ALTER TABLE proposals ADD COLUMN direct_sale BOOLEAN DEFAULT FALSE;
    END IF;

END //
DELIMITER ;

CALL upgrade_proposals();
DROP PROCEDURE upgrade_proposals;
