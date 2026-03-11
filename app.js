const express = require('express');
const cors = require('cors');
const path = require('path');
const mysql = require('mysql2/promise');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 8080;
const JWT_SECRET = process.env.JWT_SECRET || 'super-secret-coral-bsnl-key-2026';

// --- MIDDLEWARE ---
// MUST BE AT THE TOP to parse bodies before hitting routes!
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Serve static files from the 'public' directory (CRM)
app.use(express.static(path.join(__dirname, 'public')));

// Serve static files from the 'website' directory
app.use('/site', express.static(path.join(__dirname, 'website')));

// Route for Website Home
app.get('/website-home', (req, res) => {
    res.sendFile(path.join(__dirname, 'website', 'index.html'));
});

// --- DATABASE CONNECTION ---
// MySQL Connection Pool
const pool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    port: parseInt(process.env.DB_PORT) || 3306,
    waitForConnections: true,
    connectionLimit: 5,
    queueLimit: 0,
    connectTimeout: 30000,
    ssl: false
});

// Upgrade Users Table for Advanced Features
async function upgradeUsersTable() {
    try {
        const columns = [
            { name: 'name', type: 'VARCHAR(100)' },
            { name: 'mobile', type: 'VARCHAR(20)' },
            { name: 'email', type: 'VARCHAR(150)' },
            { name: 'allowed_circle', type: 'VARCHAR(100)' },
            { name: 'allowed_oa', type: 'VARCHAR(100)' },
            { name: 'allowed_customers', type: 'TEXT' },
            { name: 'permissions', type: 'JSON' },
            { name: 'reports_to', type: 'INT' },
            { name: 'backdate_rights', type: 'BOOLEAN DEFAULT FALSE' }
        ];

        for (const col of columns) {
            const [rows] = await pool.query(`
                SELECT COUNT(*) as count 
                FROM information_schema.columns 
                WHERE table_schema = ? AND table_name = 'users' AND column_name = ?
            `, [process.env.DB_NAME, col.name]);

            if (rows[0].count === 0) {
                await pool.query(`ALTER TABLE users ADD COLUMN ${col.name} ${col.type}`);
                console.log(`✅ Added column ${col.name} to users table.`);
            }
        }
    } catch (err) {
        console.error('⚠️ Could not upgrade users table:', err.message);
    }
}

// Upgrade Proposals Table for CRM features
async function upgradeProposalsTable() {
    try {
        const columns = [
            { name: 'user_id', type: 'INT' },
            { name: 'direct_sale', type: 'BOOLEAN DEFAULT FALSE' },
            { name: 'quotation_items', type: 'JSON' },
            { name: 'next_followup_date', type: 'DATE' },
            { name: 'followup_notes', type: 'TEXT' }
        ];

        for (const col of columns) {
            const [rows] = await pool.query(`
                SELECT COUNT(*) as count 
                FROM information_schema.columns 
                WHERE table_schema = ? AND table_name = 'proposals' AND column_name = ?
            `, [process.env.DB_NAME, col.name]);

            if (rows[0].count === 0) {
                await pool.query(`ALTER TABLE proposals ADD COLUMN ${col.name} ${col.type}`);
                console.log(`✅ Added column ${col.name} to proposals table.`);
            }
        }
    } catch (err) {
        console.error('⚠️ Could not upgrade proposals table:', err.message);
    }
}

// Initialize Complaints Table
async function initializeComplaintsTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS complaints (
                id INT AUTO_INCREMENT PRIMARY KEY,
                complaint_no VARCHAR(30),
                customer_name VARCHAR(255) NOT NULL,
                complainee_name VARCHAR(255),
                mobile VARCHAR(20),
                email VARCHAR(150),
                std_code VARCHAR(10),
                telephone_number VARCHAR(20),
                circle VARCHAR(100),
                oa_name VARCHAR(100),
                issue_type VARCHAR(100),
                description TEXT,
                priority ENUM('Low','Medium','High') DEFAULT 'Low',
                status VARCHAR(50) DEFAULT 'Pending',
                fault_at VARCHAR(150),
                remark TEXT,
                assigner_comments TEXT,
                assigned_to INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        // Add new columns if they don't exist (for existing installs)
        const newCols = [
            { name: 'complaint_no', type: 'VARCHAR(30)' },
            { name: 'complainee_name', type: 'VARCHAR(255)' },
            { name: 'circle', type: 'VARCHAR(100)' },
            { name: 'oa_name', type: 'VARCHAR(100)' },
            { name: 'priority', type: "ENUM('Low','Medium','High') DEFAULT 'Low'" },
            { name: 'fault_at', type: 'VARCHAR(150)' },
            { name: 'remark', type: 'TEXT' },
            { name: 'assigner_comments', type: 'TEXT' }
        ];
        for (const col of newCols) {
            try {
                await pool.query(`ALTER TABLE complaints ADD COLUMN ${col.name} ${col.type}`);
            } catch (e) { /* column already exists */ }
        }
        console.log('✅ Complaints table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize complaints table:', err.message);
    }
}

// Generate complaint number: e.g. HR260309FDB007
async function generateComplaintNo(circle, oa_name) {
    try {
        const circleCodeMap = {
            'haryana': 'HR', 'rajasthan': 'RJ', 'bihar': 'BH', 'punjab': 'PB',
            'himachal pradesh': 'HP', 'maharashtra': 'MH', 'uttar pradesh (east)': 'UE',
            'uttar pradesh (west)': 'UW', 'gujarat': 'GJ', 'madhya pradesh': 'MP',
            'andhra pradesh': 'AP', 'karnataka': 'KK', 'tamil nadu': 'TN',
            'kerala': 'KL', 'west bengal': 'WB', 'odisha': 'OD', 'jharkhand': 'JK',
            'chhattisgarh': 'CG', 'uttarakhand': 'UK', 'delhi': 'DL'
        };
        const circleKey = (circle || '').toLowerCase().trim();
        const circleCode = circleCodeMap[circleKey] || (circle || 'XX').toUpperCase().substring(0, 2);
        const oaCode = (oa_name || 'GEN').replace(/[^a-zA-Z]/g, '').toUpperCase().substring(0, 3).padEnd(3, 'X');
        const now = new Date();
        const yy = String(now.getFullYear()).slice(2);
        const mm = String(now.getMonth() + 1).padStart(2, '0');
        const dd = String(now.getDate()).padStart(2, '0');
        const dateStr = `${yy}${mm}${dd}`;
        const prefix = `${circleCode}${dateStr}${oaCode}`;
        const [[{ cnt }]] = await pool.query(
            `SELECT COUNT(*) as cnt FROM complaints WHERE complaint_no LIKE ?`,
            [`${prefix}%`]
        );
        const seq = String(cnt + 1).padStart(3, '0');
        return `${prefix}${seq}`;
    } catch (e) {
        return `XX${Date.now()}`;
    }
}

// Initialize Master Customer Database (Excel-based)
async function initializeCustomersTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS customers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                circle VARCHAR(100),
                ssa VARCHAR(100),
                oa_name VARCHAR(100),
                customer_code VARCHAR(100) UNIQUE,
                customer_name VARCHAR(255) NOT NULL,
                order_date DATE,
                contact_person VARCHAR(255),
                mobile_no VARCHAR(20),
                email_id VARCHAR(150),
                backdate_rights BOOLEAN DEFAULT FALSE,
                
                -- Product Details
                analog_line INT DEFAULT 0,
                digital_line INT DEFAULT 0,
                vas_line INT DEFAULT 0,
                ip_line INT DEFAULT 0,
                analog_rent DECIMAL(10,2) DEFAULT 0,
                digital_rent DECIMAL(10,2) DEFAULT 0,
                vas_rent DECIMAL(10,2) DEFAULT 0,
                ip_rent DECIMAL(10,2) DEFAULT 0,
                rg_port INT DEFAULT 0,
                rg_rent DECIMAL(10,2) DEFAULT 0,
                plan_charge DECIMAL(10,2) DEFAULT 0,
                total_line INT DEFAULT 0,
                product_plan VARCHAR(255),
                monthly_rent DECIMAL(10,2) DEFAULT 0,
                channels INT DEFAULT 0,
                
                -- New Product Fields
                revenue_level VARCHAR(100),
                epabx_model VARCHAR(100),
                product_start_date DATE,
                
                -- Telephone Line Details
                telephone_number VARCHAR(100),
                line_type VARCHAR(100),
                sip_no VARCHAR(100),
                start_date DATE,
                telephone_code VARCHAR(100),
                billing_account VARCHAR(100),
                crm_customer_id VARCHAR(100),
                submit_at_fms VARCHAR(10) DEFAULT 'NO',
                fms_submit_date DATE,
                is_closed VARCHAR(10) DEFAULT 'NO',
                closed_date DATE,
                
                -- Advanced Detail Fields
                acc_person_name VARCHAR(255),
                acc_person_mobile VARCHAR(20),
                acc_person_email VARCHAR(150),
                tech_person_name VARCHAR(255),
                tech_person_mobile VARCHAR(20),
                tech_person_email VARCHAR(150),
                customer_status VARCHAR(50) DEFAULT 'OPEN',
                customer_closed_date DATE,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ Master Customer table initialized.');
        await upgradeCustomersTable();
        await upgradeCustomersTableAdvanced();
        await initializeCustomerLinesTable();
        await initializeCustomerOrdersTable();
        await initializeBsnlTables();
    } catch (err) {
        console.error('⚠️ Could not initialize customers table:', err.message);
    }
}

async function initializeBsnlTables() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS bsnl_zones (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE,
                code VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS bsnl_circles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE,
                code VARCHAR(5) NOT NULL UNIQUE,
                vendor_code VARCHAR(50),
                zone_id INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (zone_id) REFERENCES bsnl_zones(id) ON DELETE SET NULL
            ) ENGINE=InnoDB
        `);
        // Add zone_id column if not exists (for existing tables)
        await pool.query(`ALTER TABLE bsnl_circles ADD COLUMN IF NOT EXISTS zone_id INT`).catch(() => {});

        await pool.query(`
            CREATE TABLE IF NOT EXISTS bsnl_oas (
                id INT AUTO_INCREMENT PRIMARY KEY,
                circle_id INT NOT NULL,
                name VARCHAR(100) NOT NULL,
                code VARCHAR(3) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_oa (circle_id, name),
                UNIQUE KEY unique_oa_code (circle_id, code),
                FOREIGN KEY (circle_id) REFERENCES bsnl_circles(id) ON DELETE CASCADE
            ) ENGINE=InnoDB
        `);
        console.log('✅ BSNL Circles and OAs tables initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize BSNL tables:', err.message);
    }
}

async function initializeCustomerLinesTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS customer_lines (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT NOT NULL,
                telephone_number VARCHAR(100),
                line_type VARCHAR(100),
                sip_no VARCHAR(100),
                start_date DATE,
                telephone_code VARCHAR(100),
                billing_account VARCHAR(100),
                crm_customer_id VARCHAR(100),
                submit_at_fms VARCHAR(10) DEFAULT 'NO',
                fms_submit_date DATE,
                is_closed VARCHAR(10) DEFAULT 'NO',
                closed_date DATE,
                telephone_code_2 VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
            ) ENGINE=InnoDB
        `);
        console.log('✅ Customer Lines table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize customer_lines table:', err.message);
    }
}

async function initializeCustomerOrdersTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS customer_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT NOT NULL,
                product_plan VARCHAR(255),
                monthly_rent DECIMAL(10,2) DEFAULT 0,
                channels INT DEFAULT 0,
                analog_line INT DEFAULT 0,
                digital_line INT DEFAULT 0,
                vas_line INT DEFAULT 0,
                ip_line INT DEFAULT 0,
                analog_rent DECIMAL(10,2) DEFAULT 0,
                digital_rent DECIMAL(10,2) DEFAULT 0,
                vas_rent DECIMAL(10,2) DEFAULT 0,
                ip_rent DECIMAL(10,2) DEFAULT 0,
                rg_port INT DEFAULT 0,
                rg_rent DECIMAL(10,2) DEFAULT 0,
                plan_charge DECIMAL(10,2) DEFAULT 0,
                revenue_level VARCHAR(100),
                epabx_model VARCHAR(100),
                product_start_date DATE,
                order_date DATE,
                status VARCHAR(50) DEFAULT 'ACTIVE',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
            ) ENGINE=InnoDB
        `);
        console.log('✅ Customer Orders table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize customer_orders table:', err.message);
    }
}

async function initializeWebsiteContentTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS website_content (
                key_name VARCHAR(100) PRIMARY KEY,
                content_value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ Website Content table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize website_content table:', err.message);
    }
}

async function initializeTasksTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS tasks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                description TEXT,
                assigned_to VARCHAR(100),
                created_by VARCHAR(100),
                status ENUM('PENDING', 'IN PROGRESS', 'COMPLETED') DEFAULT 'PENDING',
                due_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ Tasks table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize tasks table:', err.message);
    }
}

async function initializeRecurringTasksTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS recurring_tasks_templates (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                description TEXT,
                assigned_to VARCHAR(100),
                created_by VARCHAR(100),
                frequency VARCHAR(50) DEFAULT 'MONTHLY',
                next_run_date DATE,
                status VARCHAR(50) DEFAULT 'ACTIVE',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ Recurring Tasks table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize recurring tasks table:', err.message);
    }
}

async function initializeSystemLogsTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS system_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                log_type VARCHAR(50), 
                message TEXT,
                details JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ System Logs table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize system_logs table:', err.message);
    }
}

async function initializeAnalyticsTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS website_analytics (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ip_address VARCHAR(50),
                user_agent TEXT,
                page_url VARCHAR(255),
                action_type VARCHAR(50),
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ Website Analytics table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize website_analytics table:', err.message);
    }
}

async function upgradeCustomersTable() {
    try {
        const [rows] = await pool.query(`
            SELECT COUNT(*) as count 
            FROM information_schema.columns 
            WHERE table_schema = ? AND table_name = 'customers' AND column_name = 'is_closed'
        `, [process.env.DB_NAME]);

        if (rows[0].count === 0) {
            await pool.query("ALTER TABLE customers ADD COLUMN is_closed VARCHAR(10) DEFAULT 'NO' AFTER fms_submit_date");
            console.log("✅ Added column is_closed to customers table.");
        }
    } catch (err) {
        console.error('⚠️ Could not upgrade customers table:', err.message);
    }
}

async function upgradeCustomersTableAdvanced() {
    try {
        const columns = [
            { name: 'acc_person_name', type: 'VARCHAR(255)' },
            { name: 'acc_person_mobile', type: 'VARCHAR(20)' },
            { name: 'acc_person_email', type: 'VARCHAR(150)' },
            { name: 'tech_person_name', type: 'VARCHAR(255)' },
            { name: 'tech_person_mobile', type: 'VARCHAR(20)' },
            { name: 'tech_person_email', type: 'VARCHAR(150)' },
            { name: 'customer_status', type: 'VARCHAR(50) DEFAULT "OPEN"' },
            { name: 'customer_closed_date', type: 'DATE' },
            { name: 'oa_name', type: 'VARCHAR(100)' },
            { name: 'revenue_level', type: 'VARCHAR(100)' },
            { name: 'epabx_model', type: 'VARCHAR(100)' },
            { name: 'product_start_date', type: 'DATE' },
            { name: 'product_plan', type: 'VARCHAR(255)' },
            { name: 'monthly_rent', type: 'DECIMAL(10,2) DEFAULT 0' },
            { name: 'channels', type: 'INT DEFAULT 0' }
        ];

        for (const col of columns) {
            const [rows] = await pool.query(`
                SELECT COUNT(*) as count 
                FROM information_schema.columns 
                WHERE table_schema = ? AND table_name = 'customers' AND column_name = ?
            `, [process.env.DB_NAME, col.name]);

            if (rows[0].count === 0) {
                await pool.query(`ALTER TABLE customers ADD COLUMN ${col.name} ${col.type}`);
                console.log(`✅ Added column ${col.name} to customers table.`);
            }
        }
    } catch (err) {
        console.error('⚠️ Advanced upgrade failed:', err.message);
    }
}

// Create Default Admin User if none exists
async function initializeAdmin() {
    try {
        const [rows] = await pool.query('SELECT * FROM users WHERE username = ?', ['admin']);
        if (rows.length === 0) {
            const hash = await bcrypt.hash('admin123', 10);
            await pool.query('INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)', ['admin', hash, 'admin']);
            console.log('✅ Default Admin created: admin / admin123');
        }
    } catch (err) {
        console.error('⚠️ Could not initialize default admin:', err.message);
    }
}

// --- AUTH MIDDLEWARE ---
const authenticateToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    if (token == null) return res.status(401).json({ error: 'Unauthorized. Please log in.' });

    jwt.verify(token, JWT_SECRET, (err, user) => {
        if (err) return res.status(403).json({ error: 'Session expired or invalid token.' });
        req.user = user;
        next();
    });
};

const isAdmin = (req, res, next) => {
    if (req.user && req.user.role === 'admin') {
        next();
    } else {
        res.status(403).json({ error: 'Access denied. Admin privileges required.' });
    }
};

// --- AUTH ROUTES ---
app.post('/api/login', async (req, res) => {
    const { username, password } = req.body;
    try {
        const [rows] = await pool.query('SELECT * FROM users WHERE username = ?', [username]);
        if (rows.length === 0) return res.status(401).json({ error: 'Invalid username or password' });

        const user = rows[0];
        const validPassword = await bcrypt.compare(password, user.password_hash);
        if (!validPassword) return res.status(401).json({ error: 'Invalid username or password' });

        // Create JWT token
        const token = jwt.sign({ id: user.id, username: user.username, role: user.role }, JWT_SECRET, { expiresIn: '12h' });

        res.json({ token, user: { id: user.id, username: user.username, role: user.role } });
    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ error: 'Server error during login', details: error.message });
    }
});

// --- USERS ROUTES (Admin Only) ---
app.get('/api/users', authenticateToken, isAdmin, async (req, res) => {
    try {
        const [rows] = await pool.query(`
            SELECT u1.id, u1.username, u1.role, u1.name, u1.mobile, u1.email, 
                   u1.allowed_circle, u1.allowed_oa, u1.permissions, u1.reports_to, u1.created_at,
                   u2.name as manager_name, u2.username as manager_username
            FROM users u1
            LEFT JOIN users u2 ON u1.reports_to = u2.id
        `);
        res.json(rows);
    } catch (err) {
        console.error('Fetch users error:', err);
        res.status(500).json({ error: 'Failed to fetch users' });
    }
});

app.post('/api/users', authenticateToken, isAdmin, async (req, res) => {
    const { username, password, role, name, mobile, email, allowed_circle, allowed_oa, permissions, reports_to } = req.body;
    try {
        const hash = await bcrypt.hash(password, 10);
        const [result] = await pool.query(
            'INSERT INTO users (username, password_hash, role, name, mobile, email, allowed_circle, allowed_oa, permissions, reports_to, backdate_rights) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [username, hash, role || 'user', name || null, mobile || null, email || null, allowed_circle || null, allowed_oa || null, JSON.stringify(permissions || []), reports_to || null, req.body.backdate_rights || false]
        );
        res.status(201).json({ id: result.insertId, username, role });
    } catch (err) {
        console.error('Create user error:', err);
        res.status(500).json({ error: 'Failed to create user' });
    }
});

app.put('/api/users/:id', authenticateToken, isAdmin, async (req, res) => {
    const { role, name, mobile, email, allowed_circle, allowed_oa, permissions, reports_to } = req.body;
    try {
        await pool.query(
            'UPDATE users SET role=?, name=?, mobile=?, email=?, allowed_circle=?, allowed_oa=?, permissions=?, reports_to=?, backdate_rights=? WHERE id=?',
            [role, name || null, mobile || null, email || null, allowed_circle || null, allowed_oa || null, JSON.stringify(permissions || []), reports_to || null, req.body.backdate_rights || false, req.params.id]
        );
        res.json({ message: 'User updated successfully' });
    } catch (err) {
        console.error('Edit user error:', err);
        res.status(500).json({ error: 'Failed to update user' });
    }
});

// Admin changes another user's password OR User changes their own
app.put('/api/users/:id/password', authenticateToken, async (req, res) => {
    const { new_password } = req.body;
    const targetUserId = parseInt(req.params.id);

    // Authorization: User can only change their own password, unless they are an admin
    if (req.user.id !== targetUserId && req.user.role !== 'admin') {
        return res.status(403).json({ error: 'Access denied.' });
    }

    try {
        const hash = await bcrypt.hash(new_password, 10);
        await pool.query('UPDATE users SET password_hash=? WHERE id=?', [hash, targetUserId]);
        res.json({ message: 'Password changed successfully' });
    } catch (err) {
        console.error('Password change error:', err);
        res.status(500).json({ error: 'Failed to change password' });
    }
});

app.delete('/api/users/:id', authenticateToken, isAdmin, async (req, res) => {
    try {
        await pool.query('DELETE FROM users WHERE id = ?', [req.params.id]);
        res.json({ message: 'User deleted successfully' });
    } catch (err) {
        console.error('Delete user error:', err);
        res.status(500).json({ error: 'Failed to delete user. They may have related data (proposals/tasks).' });
    }
});

// --- CATALOG ROUTES ---
app.get('/api/categories', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM categories ORDER BY name ASC');
        res.json(rows);
    } catch (err) {
        console.error('Fetch categories error:', err);
        res.status(500).json({ error: 'Failed to fetch categories' });
    }
});

app.post('/api/categories', authenticateToken, isAdmin, async (req, res) => {
    const { name } = req.body;
    try {
        const [result] = await pool.query('INSERT INTO categories (name) VALUES (?)', [name]);
        res.status(201).json({ id: result.insertId, name });
    } catch (err) {
        console.error('Create category error:', err);
        res.status(500).json({ error: 'Failed to create category' });
    }
});

app.get('/api/products', authenticateToken, async (req, res) => {
    const category_id = req.query.category_id;
    try {
        let query = 'SELECT p.*, c.name as category_name FROM products p JOIN categories c ON p.category_id = c.id';
        const params = [];
        if (category_id) {
            query += ' WHERE p.category_id = ?';
            params.push(category_id);
        }
        query += ' ORDER BY p.model_name ASC';

        const [rows] = await pool.query(query, params);
        res.json(rows);
    } catch (err) {
        console.error('Fetch products error:', err);
        res.status(500).json({ error: 'Failed to fetch products' });
    }
});

app.post('/api/products', authenticateToken, isAdmin, async (req, res) => {
    const { category_id, model_name, default_price } = req.body;
    try {
        const [result] = await pool.query('INSERT INTO products (category_id, model_name, default_price) VALUES (?, ?, ?)', [category_id, model_name, default_price || 0.00]);
        res.status(201).json({ id: result.insertId, category_id, model_name, default_price });
    } catch (err) {
        console.error('Create product error:', err);
        res.status(500).json({ error: 'Failed to create product' });
    }
});

// --- PROPOSALS API ---
app.get('/api/proposals', authenticateToken, async (req, res) => {
    try {
        // Build query based on role. Admins see all, users see their own.
        let query = 'SELECT * FROM proposals ORDER BY created_at DESC';
        const params = [];
        if (req.user.role !== 'admin') {
            query = 'SELECT * FROM proposals WHERE user_id = ? ORDER BY created_at DESC';
            params.push(req.user.id);
        }
        const [rows] = await pool.query(query, params);
        res.json(rows);
    } catch (error) {
        console.error('Fetch proposals error:', error);
        res.status(500).json({ error: 'Failed to fetch proposals' });
    }
});

app.post('/api/proposals', authenticateToken, async (req, res) => {
    const { customer_name, customer_category, proposal_data, direct_sale, quotation_items, next_followup_date } = req.body;
    try {
        const [result] = await pool.query(
            'INSERT INTO proposals (customer_name, customer_category, proposal_data, user_id, direct_sale, quotation_items, next_followup_date) VALUES (?, ?, ?, ?, ?, ?, ?)',
            [customer_name, customer_category, JSON.stringify(proposal_data), req.user.id, direct_sale || false, JSON.stringify(quotation_items || []), next_followup_date || null]
        );
        res.status(201).json({ id: result.insertId, message: 'Proposal created successfully' });
    } catch (error) {
        console.error('Create proposal error:', error);
        res.status(500).json({ error: 'Failed to create proposal' });
    }
});

app.patch('/api/proposals/:id/followup', authenticateToken, async (req, res) => {
    const { next_followup_date } = req.body;
    const { id } = req.params;
    try {
        await pool.query(
            'UPDATE proposals SET next_followup_date = ? WHERE id = ?',
            [next_followup_date || null, id]
        );
        res.json({ message: 'Follow-up date updated successfully' });
    } catch (error) {
        console.error('Update followup error:', error);
        res.status(500).json({ error: 'Failed to update follow-up date' });
    }
});

// --- COMPLAINTS ROUTES ---
app.get('/api/complaints', authenticateToken, async (req, res) => {
    try {
        const user = req.user;
        let query = `
            SELECT c.*, u.name as assigned_username, u.username as assigned_user
            FROM complaints c
            LEFT JOIN users u ON c.assigned_to = u.id
        `;
        const params = [];
        // Engineers only see their assigned complaints
        if (user.role !== 'admin') {
            query += ' WHERE c.assigned_to = ?';
            params.push(user.id);
        }
        query += ' ORDER BY c.created_at DESC';
        const [rows] = await pool.query(query, params);
        res.json(rows);
    } catch (err) {
        console.error('Fetch complaints error:', err);
        res.status(500).json({ error: 'Failed to fetch complaints' });
    }
});

// Complaints Report with filters
app.get('/api/complaints/report', authenticateToken, async (req, res) => {
    try {
        const { circle, oa, status, engineer, search, start, end, limit = 40, offset = 0 } = req.query;
        let where = [];
        const params = [];
        if (circle) { where.push('c.circle = ?'); params.push(circle); }
        if (oa) { where.push('c.oa_name = ?'); params.push(oa); }
        if (status && status !== '--All--') { where.push('c.status = ?'); params.push(status); }
        if (engineer && engineer !== '--All--') { where.push('u.name = ?'); params.push(engineer); }
        if (search) { where.push('(c.customer_name LIKE ? OR c.complaint_no LIKE ? OR c.telephone_number LIKE ?)'); params.push(`%${search}%`, `%${search}%`, `%${search}%`); }
        if (start) { where.push('DATE(c.created_at) >= ?'); params.push(start); }
        if (end) { where.push('DATE(c.created_at) <= ?'); params.push(end); }
        const whereStr = where.length ? 'WHERE ' + where.join(' AND ') : '';
        const [rows] = await pool.query(`
            SELECT c.*, u.name as engineer_name
            FROM complaints c
            LEFT JOIN users u ON c.assigned_to = u.id
            ${whereStr}
            ORDER BY c.created_at DESC
            LIMIT ? OFFSET ?
        `, [...params, parseInt(limit), parseInt(offset)]);
        const [[{ total }]] = await pool.query(`SELECT COUNT(*) as total FROM complaints c LEFT JOIN users u ON c.assigned_to = u.id ${whereStr}`, params);
        res.json({ rows, total });
    } catch (err) {
        console.error('Complaints report error:', err);
        res.status(500).json({ error: 'Failed to fetch report' });
    }
});

// Update complaint (engineer action)
app.put('/api/complaints/:id', authenticateToken, async (req, res) => {
    const { fault_at, status, remark, assigner_comments, priority, assigned_to } = req.body;
    try {
        const fields = [];
        const params = [];
        if (fault_at !== undefined) { fields.push('fault_at=?'); params.push(fault_at); }
        if (status !== undefined) { fields.push('status=?'); params.push(status); }
        if (remark !== undefined) { fields.push('remark=?'); params.push(remark); }
        if (assigner_comments !== undefined) { fields.push('assigner_comments=?'); params.push(assigner_comments); }
        if (priority !== undefined) { fields.push('priority=?'); params.push(priority); }
        if (assigned_to !== undefined) { fields.push('assigned_to=?'); params.push(assigned_to); }
        if (fields.length === 0) return res.status(400).json({ error: 'No fields to update' });
        params.push(req.params.id);
        await pool.query(`UPDATE complaints SET ${fields.join(',')} WHERE id=?`, params);
        res.json({ message: 'Complaint updated' });
    } catch (err) {
        console.error('Update complaint error:', err);
        res.status(500).json({ error: 'Failed to update complaint' });
    }
});

app.post('/api/complaints', async (req, res) => {
    const { customer_name, complainee_name, mobile, email, issue_type, description, std_code, telephone_number } = req.body;
    try {
        // Lookup circle and OA from customers table
        let circle = '', oa_name = '';
        try {
            const [custRows] = await pool.query(
                'SELECT circle, oa_name FROM customers WHERE telephone_number=? AND telephone_code=? LIMIT 1',
                [telephone_number, std_code]
            );
            if (custRows.length > 0) { circle = custRows[0].circle || ''; oa_name = custRows[0].oa_name || ''; }
        } catch (e) { /* ignore */ }

        const complaint_no = await generateComplaintNo(circle, oa_name);
        const [result] = await pool.query(
            'INSERT INTO complaints (complaint_no, customer_name, complainee_name, mobile, email, issue_type, description, std_code, telephone_number, circle, oa_name, assigner_comments) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [complaint_no, customer_name, complainee_name || customer_name, mobile, email, issue_type || 'Auto-Support', description, std_code, telephone_number, circle, oa_name, 'System Auto Assign']
        );

        const ticketId = `CRM-${String(result.insertId).padStart(4, '0')}`;
        const complaintData = { ticket_id: ticketId, complaint_no, customer_name, mobile, email, issue_type, description };

        sendEmailNotification(complaintData).catch(err => console.error('Email Notify Error:', err.message));
        sendWhatsAppNotification(complaintData).catch(err => console.error('WhatsApp Notify Error:', err.message));

        res.status(201).json({
            message: 'Complaint registered successfully',
            ticket_id: ticketId,
            complaint_no
        });
    } catch (err) {
        console.error('Submit complaint error:', err);
        res.status(500).json({ error: 'Failed to submit complaint' });
    }
});

// --- BSNL ZONE/CIRCLE/OA ROUTES ---
app.get('/api/bsnl/zones', authenticateToken, async (req, res) => {
    try {
        const [zones] = await pool.query('SELECT * FROM bsnl_zones ORDER BY name ASC');
        res.json(zones);
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch zones' });
    }
});

app.post('/api/bsnl/zones', authenticateToken, isAdmin, async (req, res) => {
    const { id, name, code } = req.body;
    try {
        if (id) {
            await pool.query('UPDATE bsnl_zones SET name=?, code=? WHERE id=?', [name, code || null, id]);
            res.json({ message: 'Zone updated' });
        } else {
            const [result] = await pool.query('INSERT INTO bsnl_zones (name, code) VALUES (?, ?)', [name, code || null]);
            res.status(201).json({ id: result.insertId, message: 'Zone added' });
        }
    } catch (err) {
        res.status(500).json({ error: 'Failed to save zone' });
    }
});

app.delete('/api/bsnl/zones/:id', authenticateToken, isAdmin, async (req, res) => {
    try {
        const [inUse] = await pool.query('SELECT id FROM bsnl_circles WHERE zone_id = ? LIMIT 1', [req.params.id]);
        if (inUse.length > 0) return res.status(400).json({ error: 'Zone in use by circles' });
        await pool.query('DELETE FROM bsnl_zones WHERE id = ?', [req.params.id]);
        res.json({ message: 'Zone deleted' });
    } catch (err) {
        res.status(500).json({ error: 'Delete failed' });
    }
});

app.get('/api/bsnl/circles', authenticateToken, async (req, res) => {
    try {
        const [circles] = await pool.query(`
            SELECT c.*, z.name as zone_name, z.code as zone_code
            FROM bsnl_circles c
            LEFT JOIN bsnl_zones z ON c.zone_id = z.id
            ORDER BY c.name ASC
        `);
        for (let circle of circles) {
            const [oas] = await pool.query('SELECT * FROM bsnl_oas WHERE circle_id = ? ORDER BY name ASC', [circle.id]);
            circle.oas = oas;
        }
        res.json(circles);
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch BSNL data' });
    }
});

app.post('/api/bsnl/circles', authenticateToken, isAdmin, async (req, res) => {
    const { id, name, code, vendor_code, zone_id } = req.body;
    try {
        if (id) {
            await pool.query('UPDATE bsnl_circles SET name=?, code=?, vendor_code=?, zone_id=? WHERE id=?', [name, code, vendor_code || null, zone_id || null, id]);
            res.json({ message: 'Circle updated' });
        } else {
            const [result] = await pool.query('INSERT INTO bsnl_circles (name, code, vendor_code, zone_id) VALUES (?, ?, ?, ?)', [name, code, vendor_code || null, zone_id || null]);
            res.status(201).json({ id: result.insertId, message: 'Circle added' });
        }
    } catch (err) {
        res.status(500).json({ error: 'Failed to save circle' });
    }
});

app.post('/api/bsnl/oas', authenticateToken, isAdmin, async (req, res) => {
    const { id, circle_id, name, code } = req.body;
    try {
        if (id) {
            await pool.query('UPDATE bsnl_oas SET circle_id=?, name=?, code=? WHERE id=?', [circle_id, name, code, id]);
            res.json({ message: 'OA updated' });
        } else {
            const [result] = await pool.query('INSERT INTO bsnl_oas (circle_id, name, code) VALUES (?, ?, ?)', [circle_id, name, code]);
            res.status(201).json({ id: result.insertId, message: 'OA added' });
        }
    } catch (err) {
        res.status(500).json({ error: 'Failed to save OA' });
    }
});

app.delete('/api/bsnl/circles/:id', authenticateToken, isAdmin, async (req, res) => {
    try {
        // Check if in use
        const [inUse] = await pool.query('SELECT id FROM customers WHERE circle = (SELECT name FROM bsnl_circles WHERE id = ?) LIMIT 1', [req.params.id]);
        if (inUse.length > 0) return res.status(400).json({ error: 'Circle in use by customers' });
        
        await pool.query('DELETE FROM bsnl_circles WHERE id = ?', [req.params.id]);
        res.json({ message: 'Circle deleted' });
    } catch (err) {
        res.status(500).json({ error: 'Delete failed' });
    }
});

app.delete('/api/bsnl/oas/:id', authenticateToken, isAdmin, async (req, res) => {
    try {
        const [inUse] = await pool.query('SELECT id FROM customers WHERE oa_name = (SELECT name FROM bsnl_oas WHERE id = ?) LIMIT 1', [req.params.id]);
        if (inUse.length > 0) return res.status(400).json({ error: 'OA in use by customers' });

        await pool.query('DELETE FROM bsnl_oas WHERE id = ?', [req.params.id]);
        res.json({ message: 'OA deleted' });
    } catch (err) {
        res.status(500).json({ error: 'Delete failed' });
    }
});

// --- NOTIFICATION HELPERS (Placeholders) ---
async function sendEmailNotification(data) {
    console.log(`[NOTIFY] Sending Email to ${data.email} with Ticket ID: ${data.ticket_id}`);
    // TODO: Implement nodemailer logic when credentials provided
}

async function sendWhatsAppNotification(data) {
    console.log(`[NOTIFY] Sending WhatsApp to ${data.mobile} with Ticket ID: ${data.ticket_id}`);
    // TODO: Implement WhatsApp API logic when credentials provided
}

// --- CUSTOMERS ROUTES ---
app.get('/api/customers', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM customers ORDER BY customer_name ASC');
        res.json(rows);
    } catch (err) {
        console.error('Fetch customers error:', err);
        res.status(500).json({ error: 'Failed to fetch customers' });
    }
});

app.get('/api/customers/search', async (req, res) => {
    const { phone, std_code, telephone_number } = req.query;
    try {
        let query = 'SELECT customer_name, circle, ssa, customer_code, mobile_no, email_id FROM customers WHERE 1=0';
        const params = [];

        if (phone) {
            query += ' OR mobile_no = ? OR telephone_number = ? OR id IN (SELECT customer_id FROM customer_lines WHERE telephone_number = ?)';
            params.push(phone, phone, phone);
        }

        if (std_code && telephone_number) {
            query += ' OR (telephone_code = ? AND telephone_number = ?) OR id IN (SELECT customer_id FROM customer_lines WHERE telephone_code = ? AND telephone_number = ?)';
            params.push(std_code, telephone_number, std_code, telephone_number);
        }

        const [rows] = await pool.query(query, params);
        if (rows.length > 0) {
            res.json(rows[0]);
        } else {
            res.status(404).json({ error: 'Customer not found' });
        }
    } catch (err) {
        console.error('Search error:', err);
        res.status(500).json({ error: 'Search failed' });
    }
});

app.post('/api/customers', authenticateToken, async (req, res) => {
    const data = req.body;
    const connection = await pool.getConnection();
    try {
        await connection.beginTransaction();

        // 0. Auto-Generate Customer Code if not provided (Edit mode usually provides it)
        if (!data.customer_code) {
            const [circleData] = await connection.query('SELECT code FROM bsnl_circles WHERE name = ?', [data.circle]);
            const [oaData] = await connection.query('SELECT id, code FROM bsnl_oas WHERE name = ? AND circle_id = (SELECT id FROM bsnl_circles WHERE name = ?)', [data.oa_name, data.circle]);
            
            if (circleData.length > 0 && oaData.length > 0) {
                const cCode = circleData[0].code;
                const oCode = oaData[0].code;
                const prefix = `${cCode}${oCode}`;
                
                const [countRows] = await connection.query('SELECT COUNT(*) as count FROM customers WHERE customer_code LIKE ?', [`${prefix}%`]);
                const nextSeq = String(countRows[0].count + 1).padStart(3, '0');
                data.customer_code = `${prefix}${nextSeq}`;
            } else {
                // Fallback or error if circle/oa not found in new tables
                throw new Error('Circle or OA code not found for auto-generation');
            }
        }

        // --- DUPLICATE VALIDATION ---
        if (data.customer_name) {
            const [existingCust] = await connection.query('SELECT id FROM customers WHERE LOWER(customer_name) = LOWER(?)', [data.customer_name.trim()]);
            if (existingCust.length > 0) {
                await connection.rollback();
                connection.release();
                return res.status(400).json({ error: 'A customer with this name already exists in the database.' });
            }
        }
        // --- END DUPLICATE VALIDATION ---

        // 1. Insert Main Customer Record
        const [customerResult] = await connection.query(
            `INSERT INTO customers (
                circle, ssa, oa_name, customer_code, customer_name, order_date, contact_person, mobile_no, email_id,
                analog_line, digital_line, vas_line, ip_line, analog_rent, digital_rent, vas_rent, ip_rent,
                rg_port, rg_rent, plan_charge, total_line,
                telephone_number, telephone_code,
                acc_person_name, acc_person_mobile, acc_person_email,
                tech_person_name, tech_person_mobile, tech_person_email,
                customer_status, customer_closed_date,
                revenue_level, epabx_model, product_start_date
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            [
                data.circle, data.ssa || '', data.oa_name, data.customer_code, data.customer_name, data.order_date || null, data.contact_person, data.mobile_no, data.email_id,
                data.analog_line || 0, data.digital_line || 0, data.vas_line || 0, data.ip_line || 0,
                data.analog_rent || 0, data.digital_rent || 0, data.vas_rent || 0, data.ip_rent || 0,
                data.rg_port || 0, data.rg_rent || 0, data.plan_charge || 0, data.total_line || 0,
                data.lines && data.lines.length > 0 ? data.lines[0].telephone_number : null,
                data.lines && data.lines.length > 0 ? data.lines[0].telephone_code : null,
                data.acc_person_name, data.acc_person_mobile, data.acc_person_email,
                data.tech_person_name, data.tech_person_mobile, data.tech_person_email,
                data.customer_status || 'OPEN', data.customer_closed_date || null,
                data.revenue_level, data.epabx_model, data.product_start_date || null
            ]
        );

        const customerId = customerResult.insertId;

        // 2. Insert Initial Order into customer_orders
        await connection.query(
            `INSERT INTO customer_orders (
                customer_id, product_plan, monthly_rent, channels, 
                analog_line, digital_line, vas_line, ip_line,
                analog_rent, digital_rent, vas_rent, ip_rent,
                rg_port, rg_rent, plan_charge, revenue_level, epabx_model,
                product_start_date, order_date
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            [
                customerId, data.product_plan, data.monthly_rent || 0, data.channels || 0,
                data.analog_line || 0, data.digital_line || 0, data.vas_line || 0, data.ip_line || 0,
                data.analog_rent || 0, data.digital_rent || 0, data.vas_rent || 0, data.ip_rent || 0,
                data.rg_port || 0, data.rg_rent || 0, data.plan_charge || 0,
                data.revenue_level, data.epabx_model, data.product_start_date || null, data.order_date || null
            ]
        );

        // 3. Insert Multiple Telephone Lines with Auto-Code
        if (data.lines && Array.isArray(data.lines)) {
            let lineSeq = 1;
            for (const line of data.lines) {
                const lineCode = `${data.customer_code}${String(lineSeq).padStart(3, '0')}`;
                await connection.query(
                    `INSERT INTO customer_lines (
                        customer_id, telephone_number, line_type, sip_no, start_date,
                        telephone_code, billing_account, crm_customer_id, submit_at_fms, 
                        fms_submit_date, is_closed, closed_date, telephone_code_2
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
                    [
                        customerId, line.telephone_number, line.line_type, line.sip_no, line.start_date || null,
                        lineCode, line.billing_account, line.crm_customer_id, line.submit_at_fms || 'NO',
                        line.fms_submit_date || null, line.is_closed || 'NO', line.closed_date || null, line.telephone_code_2
                    ]
                );
                lineSeq++;
            }
        }

        await connection.commit();
        res.status(201).json({ id: customerId, customer_code: data.customer_code, message: 'Customer added successfully' });
    } catch (err) {
        await connection.rollback();
        console.error('Create customer error:', err);
        res.status(500).json({
            error: err.code === 'ER_DUP_ENTRY' ? 'Duplicate entry found' : 'Failed to add customer',
            details: err.message
        });
    } finally {
        connection.release();
    }
});

// --- IMPORT API (Batch) ---
app.post('/api/import/customers', authenticateToken, async (req, res) => {
    const { customers } = req.body;
    if (!customers || !Array.isArray(customers)) return res.status(400).json({ error: 'Invalid data' });

    const connection = await pool.getConnection();
    try {
        await connection.beginTransaction();
        for (const c of customers) {
            await connection.query(
                `INSERT INTO customers (
                    circle, ssa, oa_name, customer_code, customer_name, order_date, contact_person, mobile_no, email_id,
                    acc_person_name, acc_person_mobile, tech_person_name, customer_status,
                    product_plan, monthly_rent, epabx_model, revenue_level, telephone_number
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON DUPLICATE KEY UPDATE 
                    customer_name=VALUES(customer_name), circle=VALUES(circle), ssa=VALUES(ssa), oa_name=VALUES(oa_name),
                    mobile_no=VALUES(mobile_no), product_plan=VALUES(product_plan), monthly_rent=VALUES(monthly_rent)`,
                [
                    c.circle, c.ssa, c.oa_name, c.customer_code, c.customer_name, c.order_date || null, c.contact_person, c.mobile_no, c.email_id,
                    c.acc_person_name, c.acc_person_mobile, c.tech_person_name, c.customer_status || 'OPEN',
                    c.product_plan, c.monthly_rent || 0, c.epabx_model, c.revenue_level, c.telephone_number
                ]
            );
        }
        await connection.commit();
        res.json({ message: `Successfully imported ${customers.length} customers` });
    } catch (err) {
        await connection.rollback();
        console.error(err);
        res.status(500).json({ error: 'Import failed' });
    } finally {
        connection.release();
    }
});

app.post('/api/import/lines', authenticateToken, async (req, res) => {
    const { lines } = req.body;
    if (!lines || !Array.isArray(lines)) return res.status(400).json({ error: 'Invalid data' });

    const connection = await pool.getConnection();
    try {
        await connection.beginTransaction();
        for (const l of lines) {
            // Find customer id by code
            const [cust] = await connection.query('SELECT id FROM customers WHERE customer_code = ?', [l.customer_code]);
            if (cust.length > 0) {
                await connection.query(
                    `INSERT INTO customer_lines (
                        customer_id, telephone_number, line_type, sip_no, start_date,
                        billing_account, crm_customer_id, submit_at_fms, is_closed
                    ) VALUES (?,?,?,?,?,?,?,?,?)`,
                    [
                        cust[0].id, l.telephone_number, l.line_type, l.sip_no, l.start_date || null,
                        l.billing_account, l.crm_customer_id, l.submit_at_fms || 'NO', l.is_closed || 'NO'
                    ]
                );
            }
        }
        await connection.commit();
        res.json({ message: `Successfully imported ${lines.length} lines` });
    } catch (err) {
        await connection.rollback();
        console.error(err);
        res.status(500).json({ error: 'Import failed' });
    } finally {
        connection.release();
    }
});

// Search Customer by Exact Code or ID (V2)
app.get('/api/customers/search/v2', authenticateToken, async (req, res) => {
    const { customerCode, id } = req.query;
    if (!customerCode && !id) return res.status(400).json({ error: 'Customer code or ID required' });

    try {
        let query = 'SELECT * FROM customers WHERE ';
        let params = [];
        if (id) {
            query += 'id = ?';
            params.push(id);
        } else {
            query += 'customer_code = ?';
            params.push(customerCode);
        }

        const [rows] = await pool.query(query, params);
        if (rows.length === 0) return res.status(404).json({ error: 'Customer not found' });

        const customer = rows[0];
        // Fetch telephone lines for this customer
        const [lines] = await pool.query('SELECT * FROM customer_lines WHERE customer_id = ?', [customer.id]);
        customer.lines = lines;

        res.json(customer);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Update Customer (Master Data)
app.put('/api/customers/:id', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const data = req.body;
    let connection;

    try {
        connection = await pool.getConnection();
        await connection.beginTransaction();

        // --- DUPLICATE VALIDATION ---
        if (data.customer_name) {
            const [existingCust] = await connection.query('SELECT id FROM customers WHERE LOWER(customer_name) = LOWER(?) AND id != ?', [data.customer_name.trim(), id]);
            if (existingCust.length > 0) {
                await connection.rollback();
                connection.release();
                return res.status(400).json({ error: 'Another customer with this name already exists in the database. Please use a different name.' });
            }
        }

        if (data.lines && Array.isArray(data.lines)) {
            const numbers = data.lines.filter(l => l.telephone_number).map(l => l.telephone_number);
            if (numbers.length > 0) {
                const [existingLines] = await connection.query('SELECT telephone_number FROM customer_lines WHERE telephone_number IN (?) AND customer_id != ?', [numbers, id]);
                if (existingLines.length > 0) {
                    await connection.rollback();
                    connection.release();
                    const dups = existingLines.map(l => l.telephone_number).join(', ');
                    return res.status(400).json({ error: `The following telephone numbers already exist for another customer: ${dups}` });
                }
            }
        }
        // --- END DUPLICATE VALIDATION ---

        await connection.query(
            `UPDATE customers SET 
                circle = ?, ssa = ?, oa_name = ?, customer_name = ?, order_date = ?,
                acc_person_name = ?, acc_person_mobile = ?, acc_person_email = ?,
                tech_person_name = ?, tech_person_mobile = ?, tech_person_email = ?,
                customer_status = ?, customer_closed_date = ?,
                product_plan = ?, monthly_rent = ?, channels = ?,
                analog_line = ?, digital_line = ?, vas_line = ?, ip_line = ?,
                analog_rent = ?, digital_rent = ?, vas_rent = ?, ip_rent = ?,
                rg_port = ?, rg_rent = ?, plan_charge = ?,
                revenue_level = ?, epabx_model = ?, product_start_date = ?
            WHERE id = ?`,
            [
                data.circle, data.ssa, data.oa_name, data.customer_name, data.order_date || null,
                data.acc_person_name, data.acc_person_mobile, data.acc_person_email,
                data.tech_person_name, data.tech_person_mobile, data.tech_person_email,
                data.customer_status, data.customer_closed_date || null,
                data.product_plan, data.monthly_rent || 0, data.channels || 0,
                data.analog_line || 0, data.digital_line || 0, data.vas_line || 0, data.ip_line || 0,
                data.analog_rent || 0, data.digital_rent || 0, data.vas_rent || 0, data.ip_rent || 0,
                data.rg_port || 0, data.rg_rent || 0, data.plan_charge || 0,
                data.revenue_level, data.epabx_model, data.product_start_date || null,
                id
            ]
        );

        // If 'is_extension' is true, create a NEW row in customer_orders
        if (data.is_extension) {
            await connection.query(
                `INSERT INTO customer_orders (
                    customer_id, product_plan, monthly_rent, channels, 
                    analog_line, digital_line, vas_line, ip_line,
                    analog_rent, digital_rent, vas_rent, ip_rent,
                    rg_port, rg_rent, plan_charge, revenue_level, epabx_model,
                    product_start_date, order_date
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
                [
                    id, data.product_plan, data.monthly_rent || 0, data.channels || 0,
                    data.analog_line || 0, data.digital_line || 0, data.vas_line || 0, data.ip_line || 0,
                    data.analog_rent || 0, data.digital_rent || 0, data.vas_rent || 0, data.ip_rent || 0,
                    data.rg_port || 0, data.rg_rent || 0, data.plan_charge || 0,
                    data.revenue_level, data.epabx_model, data.product_start_date || null, data.order_date || null
                ]
            );
        } else if (data.order_id) {
            // Otherwise, if we are specifically editing an existing order, update it
            await connection.query(
                `UPDATE customer_orders SET 
                    product_plan=?, monthly_rent=?, channels=?, 
                    analog_line=?, digital_line=?, vas_line=?, ip_line=?,
                    analog_rent=?, digital_rent=?, vas_rent=?, ip_rent=?,
                    rg_port=?, rg_rent=?, plan_charge=?, revenue_level=?, epabx_model=?,
                    product_start_date=?, order_date=?
                WHERE id = ? AND customer_id = ?`,
                [
                    data.product_plan, data.monthly_rent || 0, data.channels || 0,
                    data.analog_line || 0, data.digital_line || 0, data.vas_line || 0, data.ip_line || 0,
                    data.analog_rent || 0, data.digital_rent || 0, data.vas_rent || 0, data.ip_rent || 0,
                    data.rg_port || 0, data.rg_rent || 0, data.plan_charge || 0,
                    data.revenue_level, data.epabx_model, data.product_start_date || null, data.order_date || null,
                    data.order_id, id
                ]
            );
        }

        // Handle Telephone Lines
        if (data.lines && Array.isArray(data.lines) && data.lines.length > 0) {
            const [existingLines] = await connection.query('SELECT telephone_number FROM customer_lines WHERE customer_id = ?', [id]);
            const existingNumbers = existingLines.map(l => l.telephone_number);

            for (const line of data.lines) {
                if (!line.telephone_number) continue;

                if (existingNumbers.includes(line.telephone_number)) {
                    await connection.query(
                        `UPDATE customer_lines SET 
                            telephone_code=?, line_type=?, sip_no=?, start_date=?, 
                            billing_account=?, crm_customer_id=?, submit_at_fms=?, 
                            fms_submit_date=?, is_closed=?, closed_date=?
                         WHERE customer_id = ? AND telephone_number = ?`,
                        [
                            line.telephone_code, line.line_type, line.sip_no, line.start_date || null,
                            line.billing_account, line.crm_customer_id, line.submit_at_fms || 'NO',
                            line.fms_submit_date || null, line.is_closed || 'NO', line.closed_date || null,
                            id, line.telephone_number
                        ]
                    );
                } else {
                    await connection.query(
                        `INSERT INTO customer_lines (
                            customer_id, telephone_number, telephone_code, line_type, sip_no, 
                            start_date, billing_account, crm_customer_id, submit_at_fms, 
                            fms_submit_date, is_closed, closed_date
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
                        [
                            id, line.telephone_number, line.telephone_code, line.line_type, line.sip_no,
                            line.start_date || null, line.billing_account, line.crm_customer_id, line.submit_at_fms || 'NO',
                            line.fms_submit_date || null, line.is_closed || 'NO', line.closed_date || null
                        ]
                    );
                }
            }
        }

        await connection.commit();
        res.json({ message: 'Customer updated successfully' });
    } catch (err) {
        if (connection) await connection.rollback();
        console.error(err);
        res.status(500).json({ error: 'Update failed' });
    } finally {
        if (connection) connection.release();
    }
});

// --- REPORT ENDPOINTS (SECTION-WISE) ---

// 1. Customer Detail Report (Only Customer Data)
app.get('/api/reports/customers', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query(`
            SELECT c.id, c.circle, c.ssa, c.oa_name, c.customer_code, c.customer_name, c.order_date, c.contact_person, c.mobile_no, c.email_id,
                   c.analog_line, c.digital_line, c.vas_line, c.ip_line, c.analog_rent, c.digital_rent, c.vas_rent, c.ip_rent,
                   c.rg_port, c.rg_rent, c.plan_charge, c.total_line, c.product_plan, c.monthly_rent, c.channels,
                   c.revenue_level, c.epabx_model, c.product_start_date,
                   c.acc_person_name, c.acc_person_mobile, c.acc_person_email, c.tech_person_name, c.tech_person_mobile, c.tech_person_email,
                   c.customer_status, c.customer_closed_date, c.created_at, c.updated_at,
                   (SELECT COUNT(*) FROM customer_lines cl WHERE cl.customer_id = c.id) as line_count,
                   (SELECT COUNT(*) FROM customer_orders co WHERE co.customer_id = c.id) as order_count
            FROM customers c
            ORDER BY c.created_at DESC
        `);
        res.json(rows);
    } catch (err) {
        console.error('Customer report error:', err);
        res.status(500).json({ error: 'Failed to fetch customer report' });
    }
});

// 2. Product & Commercials Report (Customer Context + Orders)
// Note: We use customer_orders because that tracks all the plan history/extensions.
app.get('/api/reports/products', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query(`
            SELECT c.id as customer_id, c.circle, c.ssa, c.oa_name, c.customer_name, c.customer_code,
                   o.id as order_id, o.product_plan, o.monthly_rent, o.channels,
                   o.analog_line, o.digital_line, o.vas_line, o.ip_line,
                   o.analog_rent, o.digital_rent, o.vas_rent, o.ip_rent,
                   o.rg_port, o.rg_rent, o.plan_charge, o.revenue_level, o.epabx_model, 
                   o.product_start_date, o.order_date
            FROM customers c
            JOIN customer_orders o ON o.customer_id = c.id
            ORDER BY o.created_at DESC, c.id DESC
        `);
        res.json(rows);
    } catch (err) {
        console.error('Product report error:', err);
        res.status(500).json({ error: 'Failed to fetch product report', details: err.message });
    }
});

// 3. Telephone Lines Report (Customer Context + Lines)
app.get('/api/reports/lines', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query(`
            SELECT c.id as customer_id, c.circle, c.ssa, c.oa_name, c.customer_name, c.customer_code, 
                   l.telephone_number, l.line_type, l.sip_no, l.start_date, l.telephone_code, l.telephone_code_2,
                   l.billing_account, l.crm_customer_id, l.submit_at_fms, l.fms_submit_date, l.is_closed, l.closed_date
            FROM customers c
            JOIN customer_lines l ON l.customer_id = c.id
            ORDER BY l.created_at DESC, c.id DESC
        `);
        res.json(rows);
    } catch (err) {
        console.error('Lines report error:', err);
        res.status(500).json({ error: 'Failed to fetch lines report', details: err.message });
    }
});

// 4. Consolidated Report (Everything combined via LEFT JOIN on the most recent order/main line - flattened view)
// For a true "Consolidated" view, we keep it simple as it was, showing master customer rollups.
app.get('/api/reports/consolidated', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query(`
            SELECT c.id, c.customer_code, c.customer_name, c.circle, c.ssa, c.oa_name,
                   c.product_plan, c.monthly_rent as total_bill, c.customer_status,
                   c.epabx_model, c.revenue_level, 
                   (SELECT telephone_number FROM customer_lines cl WHERE cl.customer_id = c.id LIMIT 1) as primary_line,
                   (SELECT COUNT(*) FROM customer_lines cl WHERE cl.customer_id = c.id) as line_count
            FROM customers c
            ORDER BY c.created_at DESC
        `);
        res.json(rows);
    } catch (err) {
        console.error('Consolidated report error:', err);
        res.status(500).json({ error: 'Failed to fetch consolidated report', details: err.message });
    }
});

app.get('/api/dashboard/stats', authenticateToken, async (req, res) => {
    try {
        const [[{ count: pendingComplaints }]] = await pool.query("SELECT COUNT(*) as count FROM complaints WHERE status = 'Pending'");
        const [[{ count: totalCustomers }]] = await pool.query("SELECT COUNT(*) as count FROM customers");

        res.json({
            pendingComplaints,
            totalCustomers
        });
    } catch (err) {
        console.error('Stats fetch error:', err);
        res.status(500).json({ error: 'Failed' });
    }
});


app.get('/api/dashboard/pending-tasks', authenticateToken, async (req, res) => {
    try {
        const user = req.user;
        let whereClause = 'WHERE 1=1';
        let params = [];

        // Permission filtering (similar to how reports/customers might eventually work)
        if (user.role !== 'admin') {
            const [userData] = await pool.query('SELECT allowed_circle, allowed_oa, allowed_customers FROM users WHERE id = ?', [user.id]);
            const u = userData[0];

            if (u.allowed_circle) {
                whereClause += ' AND circle = ?';
                params.push(u.allowed_circle);
            }
            if (u.allowed_oa) {
                whereClause += ' AND oa_name = ?';
                params.push(u.allowed_oa);
            }
            if (u.allowed_customers) {
                const custs = u.allowed_customers.split(',').map(c => c.trim()).filter(c => c !== '');
                if (custs.length > 0) {
                    whereClause += ' AND (customer_code IN (?) OR customer_name IN (?))';
                    params.push(custs, custs);
                }
            }
        }

        // Pending Products
        const [pendingProducts] = await pool.query(`
            SELECT COUNT(*) as count 
            FROM customers c
            ${whereClause} AND ((SELECT COUNT(*) FROM customer_orders WHERE customer_id = c.id) = 0
               OR c.product_plan IS NULL OR c.product_plan = '')
        `, params);

        // Pending Lines
        const [pendingLines] = await pool.query(`
            SELECT COUNT(*) as count 
            FROM customers c
            ${whereClause} AND (SELECT COUNT(*) FROM customer_lines WHERE customer_id = c.id) = 0
        `, params);

        // Pending Complaints - Simple Status Pending
        // (Complaints might also need circle/oa filtering if that's added to complaints table)
        const [pendingComplaints] = await pool.query(`
            SELECT COUNT(*) as count FROM complaints WHERE status = 'Pending'
        `);

        res.json({
            products: pendingProducts[0].count,
            lines: pendingLines[0].count,
            complaints: pendingComplaints[0].count
        });
    } catch (err) {
        console.error('Pending tasks error:', err);
        res.status(500).json({ error: 'Failed' });
    }
});

// --- WEBSITE ANALYTICS ROUTES ---
// Public endpoint for tracking
app.post('/api/analytics/track', async (req, res) => {
    const { page_url, action_type, details } = req.body;
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
    const userAgent = req.headers['user-agent'];

    try {
        await pool.query(
            'INSERT INTO website_analytics (ip_address, user_agent, page_url, action_type, details) VALUES (?, ?, ?, ?, ?)',
            [ip, userAgent, page_url, action_type || 'visit', details || null]
        );
        res.status(200).end();
    } catch (err) {
        console.error('Track error:', err);
        res.status(500).end();
    }
});

// Admin endpoint for report
app.get('/api/analytics/report', authenticateToken, isAdmin, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM website_analytics ORDER BY created_at DESC LIMIT 500');
        res.json(rows);
    } catch (err) {
        console.error('Analytics fetch error:', err);
        res.status(500).json({ error: 'Failed to fetch analytics' });
    }
});

// --- WEBSITE CMS ROUTES ---
app.get('/api/website/content', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM website_content');
        const contentMap = {};
        rows.forEach(row => contentMap[row.key_name] = row.content_value);
        res.json(contentMap);
    } catch (err) {
        res.status(500).json({ error: 'CMS Fetch failed' });
    }
});

app.post('/api/website/content', authenticateToken, isAdmin, async (req, res) => {
    const content = req.body; // Expecting { key: value }
    try {
        for (const [key, value] of Object.entries(content)) {
            await pool.query(
                'INSERT INTO website_content (key_name, content_value) VALUES (?, ?) ON DUPLICATE KEY UPDATE content_value = ?',
                [key, value, value]
            );
        }
        res.json({ message: 'Website content updated live!' });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'CMS Update failed' });
    }
});

// --- TASK MANAGEMENT ROUTES ---
app.get('/api/tasks', authenticateToken, async (req, res) => {
    try {
        const userRole = req.user.role;
        let query = 'SELECT * FROM tasks ORDER BY due_date ASC';
        let params = [];

        // If not admin, only show tasks assigned to them, created by them, or assigned to their subordinates
        if (userRole !== 'admin') {
            query = `
                SELECT t.*, u.name as assignee_name 
                FROM tasks t
                LEFT JOIN users u ON t.assigned_to = u.username
                WHERE t.assigned_to = ? 
                   OR t.created_by = ? 
                   OR u.reports_to = ?
                ORDER BY t.due_date ASC
            `;
            params = [req.user.username, req.user.username, req.user.id];
        } else {
            query = `
                SELECT t.*, u.name as assignee_name 
                FROM tasks t
                LEFT JOIN users u ON t.assigned_to = u.username
                ORDER BY t.due_date ASC
            `;
        }

        const [rows] = await pool.query(query, params);
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Failed to fetch tasks' });
    }
});

app.post('/api/tasks', authenticateToken, async (req, res) => {
    const { title, description, assigned_to, due_date } = req.body;
    if (!title) return res.status(400).json({ error: 'Title is required' });

    try {
        await pool.query(
            'INSERT INTO tasks (title, description, assigned_to, created_by, due_date) VALUES (?, ?, ?, ?, ?)',
            [title, description, assigned_to, req.user.username, due_date || null]
        );
        res.json({ message: 'Task created successfully' });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Failed to create task' });
    }
});

app.put('/api/tasks/:id', authenticateToken, async (req, res) => {
    const taskId = req.params.id;
    const { status } = req.body;
    try {
        // Simple update allowing assignee to update status
        await pool.query('UPDATE tasks SET status = ? WHERE id = ?', [status, taskId]);
        res.json({ message: 'Task updated successfully' });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Failed to update task' });
    }
});

async function initializeTaskReportsTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS task_reports (
                id INT AUTO_INCREMENT PRIMARY KEY,
                task_id INT NOT NULL,
                user_id INT NOT NULL,
                submission_date DATE NOT NULL,
                report_content TEXT,
                submitted_by_id INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            ) ENGINE=InnoDB
        `);
        console.log('✅ Task Reports table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize task_reports table:', err.message);
    }
}

// --- TASK REPORTS API ---
app.post('/api/task-reports', authenticateToken, async (req, res) => {
    const { task_id, submission_date, report_content } = req.body;
    const userId = req.user.id; // From token

    try {
        // 1. Get user info for backdate check
        const [userRows] = await pool.query('SELECT backdate_rights FROM users WHERE id = ?', [userId]);
        const canBackdate = userRows[0].backdate_rights;

        const today = new Date().toISOString().split('T')[0];
        if (submission_date < today && !canBackdate) {
            return res.status(403).json({ error: 'You do not have permission to submit reports for back-dates.' });
        }

        // 2. Submit report
        await pool.query(
            'INSERT INTO task_reports (task_id, user_id, submission_date, report_content, submitted_by_id) VALUES (?, ?, ?, ?, ?)',
            [task_id, userId, submission_date, report_content, userId]
        );

        // 3. Mark task as COMPLETED
        await pool.query('UPDATE tasks SET status = "COMPLETED" WHERE id = ?', [task_id]);

        res.json({ message: 'Report submitted successfully' });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Failed to submit report' });
    }
});

app.get('/api/task-analytics', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const isAdmin = req.user.role === 'admin';

        let query = `
            SELECT 
                DATE_FORMAT(r.submission_date, '%Y-%m') as month,
                COUNT(*) as completed_tasks
            FROM task_reports r
            JOIN tasks t ON r.task_id = t.id
        `;
        let params = [];

        if (!isAdmin) {
            query += ' JOIN users u ON r.user_id = u.id WHERE (r.user_id = ? OR u.reports_to = ?)';
            params = [userId, userId];
        }

        query += ' GROUP BY month ORDER BY month DESC LIMIT 12';

        const [rows] = await pool.query(query, params);
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Failed to fetch analytics' });
    }
});

// --- RECURRING TASK TEMPLATES API ---
app.get('/api/recurring-tasks', authenticateToken, isAdmin, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM recurring_tasks_templates ORDER BY created_at DESC');
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Failed to fetch templates' });
    }
});

app.post('/api/recurring-tasks', authenticateToken, isAdmin, async (req, res) => {
    const { title, description, assigned_to, frequency, next_run_date } = req.body;
    if (!title || !frequency || !next_run_date) return res.status(400).json({ error: 'Required fields missing' });

    try {
        await pool.query(
            'INSERT INTO recurring_tasks_templates (title, description, assigned_to, created_by, frequency, next_run_date) VALUES (?, ?, ?, ?, ?, ?)',
            [title, description, assigned_to, req.user.username, frequency, next_run_date]
        );
        res.json({ message: 'Recurring task template created' });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Failed to create template' });
    }
});

app.delete('/api/recurring-tasks/:id', authenticateToken, isAdmin, async (req, res) => {
    try {
        await pool.query('DELETE FROM recurring_tasks_templates WHERE id = ?', [req.params.id]);
        res.json({ message: 'Template removed' });
    } catch (err) {
        res.status(500).json({ error: 'Failed to delete template' });
    }
});

// Basic Health Check Endpoint
app.get('/api/health', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT 1');
        res.status(200).json({ status: 'OK', database: 'Connected', message: 'Server is running normally' });
    } catch (err) {
        res.status(200).json({ status: 'OK', database: 'Disconnected', message: 'Server running but DB not connected', error: err.message });
    }
});

// --- AUTOMATED MAINTENANCE SYSTEM ---
let isMaintenanceRunning = false;
global.lastMaintenanceDate = null;

async function runNightlyMaintenance() {
    if (isMaintenanceRunning) return;

    const now = new Date();
    const hour = now.getHours();

    // Run automatically between 1:00 AM and 5:00 AM once per day
    if (hour >= 1 && hour < 5) {
        const dateStr = now.toDateString();
        // Prevent multiple runs in same day
        if (global.lastMaintenanceDate === dateStr) return;

        isMaintenanceRunning = true;

        try {
            console.log(`[${now.toISOString()}] Starting Nightly Maintenance...`);

            // 1. Health check the DB
            await pool.query('SELECT 1');

            // 2. Clear old system logs (older than 30 days) to prevent DB bloat
            await pool.query('DELETE FROM system_logs WHERE created_at < DATE_SUB(NOW(), INTERVAL 30 DAY)');

            // 3. Optional: Clear very old resolved tasks (older than 1 year)
            await pool.query('DELETE FROM tasks WHERE status = "COMPLETED" AND updated_at < DATE_SUB(NOW(), INTERVAL 1 YEAR)');

            // 4. 🔥 RECURRING TASKS GENERATOR 🔥
            const dateStrFormatted = now.toISOString().split('T')[0];
            const [templates] = await pool.query('SELECT * FROM recurring_tasks_templates WHERE status = "ACTIVE" AND next_run_date <= ?', [dateStrFormatted]);

            let tasksSpawned = 0;
            for (const t of templates) {
                // Determine target due date depending on frequency
                let dueDays = 1;
                if (t.frequency === 'WEEKLY') dueDays = 3;
                if (t.frequency === 'MONTHLY') dueDays = 7;

                const dueDate = new Date();
                dueDate.setDate(dueDate.getDate() + dueDays);

                // Spawn the new active task
                await pool.query(
                    'INSERT INTO tasks (title, description, assigned_to, created_by, due_date) VALUES (?, ?, ?, ?, ?)',
                    [t.title, t.description, t.assigned_to, 'SYSTEM (Auto)', dueDate.toISOString().split('T')[0]]
                );

                // Update next_run_date for the template
                const nextRun = new Date(t.next_run_date);
                if (t.frequency === 'DAILY') nextRun.setDate(nextRun.getDate() + 1);
                else if (t.frequency === 'WEEKLY') nextRun.setDate(nextRun.getDate() + 7);
                else if (t.frequency === 'MONTHLY') nextRun.setMonth(nextRun.getMonth() + 1);
                else if (t.frequency === 'YEARLY') nextRun.setFullYear(nextRun.getFullYear() + 1);

                await pool.query('UPDATE recurring_tasks_templates SET next_run_date = ? WHERE id = ?', [nextRun.toISOString().split('T')[0], t.id]);
                tasksSpawned++;
            }

            // Log success
            await pool.query(
                "INSERT INTO system_logs (log_type, message, details) VALUES (?, ?, ?)",
                ['MAINTENANCE', 'Nightly maintenance completed successfully', JSON.stringify({ timestamp: now.toISOString() })]
            );

            global.lastMaintenanceDate = dateStr;
            console.log(`✅ Nightly Maintenance Completed. Spawned ${tasksSpawned} recurring task(s).`);
        } catch (err) {
            console.error('Failed during nightly maintenance:', err);
            try {
                await pool.query(
                    "INSERT INTO system_logs (log_type, message, details) VALUES (?, ?, ?)",
                    ['ERROR', 'Nightly maintenance failed', JSON.stringify({ error: err.message })]
                );
            } catch (e) { } // fail silently if DB is fully down
        } finally {
            isMaintenanceRunning = false;
        }
    }
}

// Check every hour (3600000 ms) automatically without user interference
setInterval(runNightlyMaintenance, 1000 * 60 * 60);

// Endpoint for admin to view maintenance history
app.get('/api/admin/maintenance', authenticateToken, isAdmin, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM system_logs ORDER BY created_at DESC LIMIT 100');
        res.json(rows);
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch maintenance logs' });
    }
});

// Manual trigger for maintenance
app.post('/api/admin/maintenance/run', authenticateToken, isAdmin, async (req, res) => {
    try {
        // Force reset the last maintenance date to allow manual run
        global.lastMaintenanceDate = null;
        // Mock the hour to bypassing 1-5am check for manual run
        const originalGetHours = Date.prototype.getHours;
        Date.prototype.getHours = () => 2;

        await runNightlyMaintenance();

        Date.prototype.getHours = originalGetHours;
        res.json({ message: 'Maintenance triggered successfully' });
    } catch (err) {
        res.status(500).json({ error: 'Manual maintenance failed', details: err.message });
    }
});

// Catch-all route to serve the frontend
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Test database connection on startup
async function startupChecks() {
    try {
        const connection = await pool.getConnection();
        console.log('✅ MySQL database connected successfully!');
        connection.release();
    } catch (err) {
        console.error('❌ MySQL connection failed:', err.message);
        console.log('⚠️  Server will run but database features won\'t work.');
        console.log('    Make sure MySQL is running and .env settings are correct.');
    }

    await upgradeUsersTable();
    await upgradeProposalsTable();
    await initializeComplaintsTable();
    await initializeCustomersTable();
    await initializeTasksTable();
    await initializeRecurringTasksTable();
    await initializeTaskReportsTable();
    await initializeAnalyticsTable();
    await initializeSystemLogsTable();
    await initializeWebsiteContentTable();
    await initializeAdmin();
    console.log('🚀 [READY] All startup checks complete. Server is fully ready!');
}

// --- ADMIN DATA IMPORT ROUTES ---

// Step 1: Clear all test data (TRUNCATE tables in FK-safe order)
app.post('/api/admin/clear-test-data', authenticateToken, async (req, res) => {
    const conn = await pool.getConnection();
    try {
        await conn.query('SET FOREIGN_KEY_CHECKS = 0');
        await conn.query('TRUNCATE TABLE customer_lines');
        await conn.query('TRUNCATE TABLE customer_orders');
        await conn.query('TRUNCATE TABLE customers');
        await conn.query('TRUNCATE TABLE bsnl_oas');
        await conn.query('TRUNCATE TABLE bsnl_circles');
        await conn.query('TRUNCATE TABLE bsnl_zones');
        await conn.query('SET FOREIGN_KEY_CHECKS = 1');
        res.json({ success: true, message: 'Test data cleared successfully' });
    } catch (err) {
        console.error('Clear test data error:', err);
        res.status(500).json({ success: false, error: err.message });
    } finally {
        conn.release();
    }
});

// Step 2: Execute SQL import (receives raw SQL text in body)
app.post('/api/admin/import-sql',
    authenticateToken,
    express.text({ type: '*/*', limit: '10mb' }),
    async (req, res) => {
        const sqlContent = req.body;
        if (!sqlContent || typeof sqlContent !== 'string') {
            return res.status(400).json({ success: false, error: 'No SQL content received' });
        }

        // Split SQL into individual statements (handle multi-line statements)
        const statements = sqlContent
            .split(/;\s*\r?\n/)
            .map(s => s.trim())
            .filter(s => s.length > 0 && !s.startsWith('--'));

        const conn = await pool.getConnection();
        let ok = 0;
        const errors = [];

        try {
            for (const stmt of statements) {
                try {
                    await conn.query(stmt);
                    ok++;
                } catch (e) {
                    // Skip duplicate key warnings, only log real errors
                    if (!e.message.includes('Duplicate entry')) {
                        errors.push(e.message.substring(0, 150));
                    } else {
                        ok++;
                    }
                }
            }
            res.json({
                success: true,
                executed: ok,
                total: statements.length,
                errors: errors.slice(0, 10)
            });
        } catch (err) {
            res.status(500).json({ success: false, error: err.message });
        } finally {
            conn.release();
        }
    }
);

// Support for both local development and Vercel Serverless Functions
if (require.main === module) {
    app.listen(PORT, '0.0.0.0', async () => {
        console.log(`\n🚀 Server is running on port ${PORT} `);
        console.log(`🌐 Accessible at: http://localhost:${PORT}`);
        await startupChecks();
    });
} else {
    // Export the express app so Vercel can run it as a serverless function
    // Run startup checks asynchronously (may delay first request slightly on cold start)
    startupChecks().catch(console.error);
    module.exports = app;
}
