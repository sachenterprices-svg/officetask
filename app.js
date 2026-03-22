const express = require('express');
const cors = require('cors');
const path = require('path');
const mysql = require('mysql2/promise');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const nodemailer = require('nodemailer');
const multer = require('multer');
const xlsx = require('xlsx');
const archiver = require('archiver');
let pdfParse;
try { pdfParse = require('pdf-parse'); } catch(e) { console.warn('pdf-parse not available:', e.message); }
const os = require('os');
const fs = require('fs');
const http = require('http');
const https = require('https');
require('dotenv').config();
const OpenAI = require('openai');
const openaiClient = new OpenAI({ apiKey: process.env.OPENAI_API_KEY || 'sk-placeholder' });

// ── BULK PDF JOB STORE ────────────────────────────────────────────────────────
// jobId -> { zipPath, createdAt }
const bulkPdfJobs = new Map();

// ── BILLING MAILER (SMTP via Gmail SSL) ───────────────────────────────────────
const billMailTransporter = nodemailer.createTransport({
    host: 'smtp.gmail.com',
    port: 465,
    secure: true,
    auth: { user: 'bsnlpribill@gmail.com', pass: 'aqpb qoqz eziq xwld' }
});

// ── SUPPORT / COMPLAINT MAILER ─────────────────────────────────────────────────
const supportMailTransporter = nodemailer.createTransport({
    host: 'smtp.gmail.com',
    port: 465,
    secure: true,
    auth: { user: 'bsnlpbxhelp@gmail.com', pass: 'yfpp apmh dnds lytr' }
});

// ── WEBSITE INQUIRY MAILER ─────────────────────────────────────────────────────
const inquiryMailTransporter = nodemailer.createTransport({
    host: 'smtp.gmail.com',
    port: 465,
    secure: true,
    auth: { user: 'info@coralinfratel.com', pass: 'afib kkds uqcy htbc' }
});

// multer: memory storage for PDF attachments (max 10 MB per file)
const pdfUpload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

const app = express();
const PORT = process.env.PORT || 8080;
const JWT_SECRET = process.env.JWT_SECRET || 'super-secret-coral-bsnl-key-2026';

// Trust proxy (Vercel / Cloudflare) — correct client IP via X-Forwarded-For
app.set('trust proxy', true);

// --- GEO LOOKUP UTILITY (with fallback) ---
async function getGeoData(ip) {
    if (!ip || ip === '127.0.0.1' || ip === '::1' || ip === 'localhost') return { city: '', region: '', country: '', isp: '' };
    // Try ip-api.com first (45 req/min free, HTTP only)
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 5000);
        const res = await fetch('http://ip-api.com/json/' + ip + '?fields=city,regionName,country,isp,status', { signal: controller.signal });
        clearTimeout(timeout);
        if (res.ok) {
            const geo = await res.json();
            if (geo.status === 'success') return { city: geo.city || '', region: geo.regionName || '', country: geo.country || '', isp: geo.isp || '' };
        }
    } catch(e) {}
    // Fallback: ipapi.co (1000 req/day, HTTPS)
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 5000);
        const res = await fetch('https://ipapi.co/' + ip + '/json/', { signal: controller.signal });
        clearTimeout(timeout);
        if (res.ok) {
            const geo = await res.json();
            if (!geo.error) return { city: geo.city || '', region: geo.region || '', country: geo.country_name || '', isp: geo.org || '' };
        }
    } catch(e) {}
    return { city: '', region: '', country: '', isp: '' };
}

// --- MIDDLEWARE ---
// MUST BE AT THE TOP to parse bodies before hitting routes!
app.use(cors());
app.use(express.json({ limit: '25mb' }));
app.use(express.urlencoded({ extended: true, limit: '25mb' }));

// Serve static files — NO CACHE for HTML/JS/CSS (always get latest)
app.use((req, res, next) => {
    if (req.path.endsWith('.html') || req.path.endsWith('.js') || req.path.endsWith('.css')) {
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
    }
    next();
});
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
    ssl: false,
    timezone: '+05:30'
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
            { name: 'backdate_rights', type: 'BOOLEAN DEFAULT FALSE' },
            { name: 'department', type: "VARCHAR(50) DEFAULT 'Technical'" },
            { name: 'work_types', type: 'JSON' },
            { name: 'allowed_circles', type: 'TEXT' },   // JSON array of circle names (multi-select, for engineers)
            { name: 'allowed_oas', type: 'TEXT' },       // JSON array of OA names (multi-select, for engineers)
            { name: 'view_circles', type: 'TEXT' },      // JSON array of circle names (for view purpose)
            { name: 'view_oas', type: 'TEXT' },          // JSON array of OA names (for view purpose)
            { name: 'purpose_technical', type: 'BOOLEAN DEFAULT FALSE' },  // Technical purpose (complaints, fault mgmt)
            { name: 'purpose_clerical', type: 'BOOLEAN DEFAULT FALSE' }    // Clerical purpose (billing, customer data)
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
            { name: 'assigner_comments', type: 'TEXT' },
            { name: 'verification_status', type: "VARCHAR(50) DEFAULT NULL" },
            { name: 'resolved_by', type: 'VARCHAR(100) DEFAULT NULL' },
            { name: 'verified_by', type: 'VARCHAR(100) DEFAULT NULL' }
        ];
        for (const col of newCols) {
            try {
                await pool.query(`ALTER TABLE complaints ADD COLUMN ${col.name} ${col.type}`);
            } catch (e) { /* column already exists */ }
        }
        // Complaint reassignment tracking table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS complaint_reassignments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                from_user_id INT NOT NULL,
                to_user_id INT NOT NULL,
                from_name VARCHAR(100),
                to_name VARCHAR(100),
                leave_from DATE,
                leave_to DATE,
                complaint_ids TEXT,
                complaint_count INT DEFAULT 0,
                status ENUM('active','cancelled') DEFAULT 'active',
                reassigned_by INT,
                reassigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                cancelled_at TIMESTAMP NULL
            ) ENGINE=InnoDB
        `);
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
async function initializeAreaEngineerMappingTables() {
    try {
        // Circle-level mapping
        await pool.query(`
            CREATE TABLE IF NOT EXISTS circle_engineer_mapping (
                id INT AUTO_INCREMENT PRIMARY KEY,
                circle VARCHAR(100) NOT NULL,
                engineer_id INT NOT NULL,
                UNIQUE KEY uniq_circle (circle)
            ) ENGINE=InnoDB
        `);
        // OA-level mapping (overrides circle)
        await pool.query(`
            CREATE TABLE IF NOT EXISTS oa_engineer_mapping (
                id INT AUTO_INCREMENT PRIMARY KEY,
                circle VARCHAR(100) NOT NULL,
                oa_name VARCHAR(100) NOT NULL,
                engineer_id INT NOT NULL,
                UNIQUE KEY uniq_oa (circle, oa_name)
            ) ENGINE=InnoDB
        `);
        // Customer-specific (overrides OA & Circle) - column on customers table
        try { await pool.query('ALTER TABLE customers ADD COLUMN assigned_engineer_id INT DEFAULT NULL'); } catch(e) {}
        console.log('✅ Area engineer mapping tables initialized.');
    } catch (err) {
        console.error('⚠️ Area mapping init error:', err.message);
    }
}

// Helper: auto-assign engineer based on priority
// Priority: Customer-specific > OA mapping table > Circle mapping table
//           > users.allowed_oas (multi-select) > users.allowed_circles (multi-select)
async function autoAssignEngineer(circle, oa_name, telephone_number, std_code) {
    try {
        // Priority 1: specific customer assignment (use telephone_code column, std_code is complaint field)
        const [custRows] = await pool.query(
            `SELECT assigned_engineer_id FROM customers
             WHERE telephone_number=? AND telephone_code=? AND assigned_engineer_id IS NOT NULL LIMIT 1`,
            [telephone_number, std_code]
        ).catch(() => [[]]);
        if (custRows.length && custRows[0].assigned_engineer_id) return custRows[0].assigned_engineer_id;

        // Priority 2: OA mapping table (engineer_area_assignment.html)
        if (oa_name) {
            const [oaRows] = await pool.query(
                'SELECT engineer_id FROM oa_engineer_mapping WHERE circle=? AND oa_name=? LIMIT 1',
                [circle, oa_name]
            );
            if (oaRows.length) return oaRows[0].engineer_id;
        }

        // Priority 3: Circle mapping table
        if (circle) {
            const [circleRows] = await pool.query(
                'SELECT engineer_id FROM circle_engineer_mapping WHERE circle=? LIMIT 1',
                [circle]
            );
            if (circleRows.length) return circleRows[0].engineer_id;
        }

        // Priority 4: users.allowed_oas JSON column (set via users.html multi-select)
        if (oa_name && circle) {
            const [uOaRows] = await pool.query(
                `SELECT id FROM users
                 WHERE work_types LIKE '%engineer%'
                   AND JSON_CONTAINS(IFNULL(allowed_oas,'[]'), JSON_QUOTE(?))
                   AND JSON_CONTAINS(IFNULL(allowed_circles,'[]'), JSON_QUOTE(?))
                 ORDER BY id LIMIT 1`,
                [oa_name, circle]
            );
            if (uOaRows.length) return uOaRows[0].id;
        }

        // Priority 5: users.allowed_circles only (circle-level fallback)
        if (circle) {
            const [uCirRows] = await pool.query(
                `SELECT id FROM users
                 WHERE work_types LIKE '%engineer%'
                   AND JSON_CONTAINS(IFNULL(allowed_circles,'[]'), JSON_QUOTE(?))
                 ORDER BY id LIMIT 1`,
                [circle]
            );
            if (uCirRows.length) return uCirRows[0].id;
        }

        return null; // Unassigned
    } catch(e) {
        console.error('[autoAssignEngineer] error:', e.message);
        return null;
    }
}

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

async function initializeUserManagersTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS user_managers (
                user_id INT NOT NULL,
                manager_id INT NOT NULL,
                PRIMARY KEY (user_id, manager_id),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (manager_id) REFERENCES users(id) ON DELETE CASCADE
            ) ENGINE=InnoDB
        `);
        console.log('✅ User Managers table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize user_managers table:', err.message);
    }
}

async function initializeTaskCategoriesTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS task_categories (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        // Insert defaults if empty
        const [rows] = await pool.query('SELECT COUNT(*) as cnt FROM task_categories');
        if (rows[0].cnt === 0) {
            await pool.query(`INSERT INTO task_categories (name) VALUES ('General'),('Accounts'),('Sales'),('Clerical'),('Technical'),('Other')`);
        }
        console.log('✅ Task Categories table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize task_categories table:', err.message);
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

async function initializeActivityTables() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS activity_templates (
                id INT AUTO_INCREMENT PRIMARY KEY,
                department ENUM('Technical','Clerical','Accounts','Sales','Admin','HR','PR','Marketing','All') NOT NULL DEFAULT 'All',
                category ENUM('Daily','Week1','Week2','Week3','Week4','Monthly','Quarterly') NOT NULL DEFAULT 'Daily',
                task_name TEXT NOT NULL,
                input_type ENUM('status','number','text') NOT NULL DEFAULT 'status',
                sort_order INT DEFAULT 0,
                is_active TINYINT(1) DEFAULT 1,
                created_by INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        // Safe ALTER — add columns if not exist (ignore errors if already present)
        try { await pool.query(`ALTER TABLE activity_templates ADD COLUMN input_type ENUM('status','number','text') NOT NULL DEFAULT 'status' AFTER task_name`); } catch(e) {}
        try { await pool.query(`ALTER TABLE activity_templates MODIFY department ENUM('Technical','Clerical','Accounts','Sales','Admin','HR','PR','Marketing','All') NOT NULL DEFAULT 'All'`); } catch(e) {}
        try { await pool.query(`ALTER TABLE activity_templates MODIFY category ENUM('Daily','Week1','Week2','Week3','Week4','Monthly','Quarterly') NOT NULL DEFAULT 'Daily'`); } catch(e) {}

        await pool.query(`
            CREATE TABLE IF NOT EXISTS activity_user_tasks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                template_id INT NOT NULL,
                assigned_by INT,
                assigned_date DATE,
                is_active TINYINT(1) DEFAULT 1,
                UNIQUE KEY unique_user_template (user_id, template_id)
            ) ENGINE=InnoDB
        `);
        await pool.query(`
            CREATE TABLE IF NOT EXISTS activity_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                template_id INT,
                custom_task VARCHAR(500),
                log_date DATE NOT NULL,
                status ENUM('done','not_done','partial','leave','na') DEFAULT 'not_done',
                value VARCHAR(500) DEFAULT NULL,
                remarks TEXT,
                submitted_late TINYINT(1) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_user_task_date (user_id, template_id, log_date)
            ) ENGINE=InnoDB
        `);
        // Safe ALTER — add new columns if not exist
        try { await pool.query(`ALTER TABLE activity_logs ADD COLUMN value VARCHAR(500) DEFAULT NULL AFTER status`); } catch(e) {}
        try { await pool.query(`ALTER TABLE activity_logs ADD COLUMN submitted_late TINYINT(1) DEFAULT 0 AFTER remarks`); } catch(e) {}

        // Performance scores cache table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS performance_scores (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                period_type ENUM('monthly','quarterly','half_yearly','annual') NOT NULL,
                period_start DATE NOT NULL,
                period_end DATE NOT NULL,
                total_tasks INT DEFAULT 0,
                completed_on_time INT DEFAULT 0,
                completed_late INT DEFAULT 0,
                partial_count INT DEFAULT 0,
                not_done_count INT DEFAULT 0,
                missed_days INT DEFAULT 0,
                total_working_days INT DEFAULT 0,
                submission_rate DECIMAL(5,2) DEFAULT 0,
                score DECIMAL(5,2) DEFAULT 0,
                grade VARCHAR(5) DEFAULT 'F',
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_user_period (user_id, period_type, period_start)
            ) ENGINE=InnoDB
        `);
        console.log('✅ Activity tables initialized (enhanced).');
    } catch (err) {
        console.error('⚠️ Could not initialize activity tables:', err.message);
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
        // Add geolocation columns if missing
        const [cols] = await pool.query(`SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'website_analytics' AND COLUMN_NAME = 'city'`);
        if (cols.length === 0) {
            await pool.query(`ALTER TABLE website_analytics ADD COLUMN city VARCHAR(100) DEFAULT NULL, ADD COLUMN region VARCHAR(100) DEFAULT NULL, ADD COLUMN country VARCHAR(100) DEFAULT NULL, ADD COLUMN isp VARCHAR(200) DEFAULT NULL`);
        }
        console.log('✅ Website Analytics table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize website_analytics table:', err.message);
    }
}

// ── BILL PDFS TABLE (Bulk bill download & email system) ──
async function initBillPdfsTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS bill_pdfs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                batch_id VARCHAR(100),
                telephone_number VARCHAR(100),
                pdf_link TEXT,
                customer_id INT DEFAULT NULL,
                customer_name VARCHAR(255) DEFAULT NULL,
                circle VARCHAR(100) DEFAULT NULL,
                oa_name VARCHAR(100) DEFAULT NULL,
                email VARCHAR(150) DEFAULT NULL,
                renamed_filename VARCHAR(500),
                pdf_data LONGBLOB,
                status ENUM('pending','downloading','matched','not_found','error') DEFAULT 'pending',
                email_sent BOOLEAN DEFAULT FALSE,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ Bill PDFs table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize bill_pdfs table:', err.message);
    }
}

// ── BILL TASKS TABLE (Customer Bill Task / Claim form) ──
async function initBillTasksTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS bill_tasks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                bsnl_bill_no VARCHAR(100),
                bill_for_month VARCHAR(50),
                customer_name VARCHAR(255),
                customer_ph_no VARCHAR(100),
                from_date DATE DEFAULT NULL,
                to_date DATE DEFAULT NULL,
                rent_ip DECIMAL(12,2) DEFAULT 0,
                rent_vas DECIMAL(12,2) DEFAULT 0,
                rent_voice DECIMAL(12,2) DEFAULT 0,
                rg_charges DECIMAL(12,2) DEFAULT 0,
                fixed_charges DECIMAL(12,2) DEFAULT 0,
                plan_charges DECIMAL(12,2) DEFAULT 0,
                call_charges DECIMAL(12,2) DEFAULT 0,
                other_debit_with_st DECIMAL(12,2) DEFAULT 0,
                other_debit_wo_st DECIMAL(12,2) DEFAULT 0,
                other_credit_with_st DECIMAL(12,2) DEFAULT 0,
                other_credit_wo_st DECIMAL(12,2) DEFAULT 0,
                total_bill DECIMAL(12,2) DEFAULT 0,
                value_as_per_bill DECIMAL(12,2) DEFAULT 0,
                gst DECIMAL(12,2) DEFAULT 0,
                customer_id_field VARCHAR(100),
                claim_no VARCHAR(100),
                phone_no VARCHAR(100),
                net_bill_amount DECIMAL(12,2) DEFAULT 0,
                days_of_month INT DEFAULT 0,
                days_of_plan INT DEFAULT 0,
                rental_share_claim DECIMAL(12,2) DEFAULT 0,
                rg_share_claim DECIMAL(12,2) DEFAULT 0,
                call_charges_revenue DECIMAL(12,2) DEFAULT 0,
                modem_rental DECIMAL(12,2) DEFAULT 0,
                claim_fixed_charges DECIMAL(12,2) DEFAULT 0,
                total_charges DECIMAL(12,2) DEFAULT 0,
                remarks TEXT,
                rev_fixed_per_month DECIMAL(12,2) DEFAULT 0,
                rev_fixed_per_line DECIMAL(12,2) DEFAULT 0,
                rev_fixed_per_vas DECIMAL(12,2) DEFAULT 0,
                rev_rent_share_pct DECIMAL(8,4) DEFAULT 0,
                rev_calling_share_pct DECIMAL(8,4) DEFAULT 0,
                rev_rg_share_pct DECIMAL(8,4) DEFAULT 0,
                gst_id VARCHAR(100) DEFAULT '',
                cgst DECIMAL(12,2) DEFAULT 0,
                sgst DECIMAL(12,2) DEFAULT 0,
                igst DECIMAL(12,2) DEFAULT 0,
                created_by INT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ Bill Tasks table initialized.');
        // Add revenue columns if they don't exist (for existing tables)
        const revCols = [
            ['rev_fixed_per_month', 'DECIMAL(12,2) DEFAULT 0'],
            ['rev_fixed_per_line', 'DECIMAL(12,2) DEFAULT 0'],
            ['rev_fixed_per_vas', 'DECIMAL(12,2) DEFAULT 0'],
            ['rev_rent_share_pct', 'DECIMAL(8,4) DEFAULT 0'],
            ['rev_calling_share_pct', 'DECIMAL(8,4) DEFAULT 0'],
            ['rev_rg_share_pct', 'DECIMAL(8,4) DEFAULT 0'],
            ['gst_id', "VARCHAR(100) DEFAULT ''"],
            ['cgst', 'DECIMAL(12,2) DEFAULT 0'],
            ['sgst', 'DECIMAL(12,2) DEFAULT 0'],
            ['igst', 'DECIMAL(12,2) DEFAULT 0'],
            ['circle_id', 'INT DEFAULT NULL'],
            ['oa_id', 'INT DEFAULT NULL']
        ];
        for (const [col, def] of revCols) {
            try {
                const [[exists]] = await pool.query(
                    `SELECT COUNT(*) as c FROM information_schema.columns WHERE table_schema=? AND table_name='bill_tasks' AND column_name=?`,
                    [process.env.DB_NAME, col]
                );
                if (!exists.c) await pool.query(`ALTER TABLE bill_tasks ADD COLUMN ${col} ${def}`);
            } catch(e) {}
        }
    } catch (err) {
        console.error('⚠️ Could not initialize bill_tasks table:', err.message);
    }
}

// ── GST MASTER TABLE ──
async function initGstMasterTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS gst_master (
                id INT AUTO_INCREMENT PRIMARY KEY,
                gst_id VARCHAR(100) NOT NULL,
                cgst DECIMAL(12,2) DEFAULT 0,
                sgst DECIMAL(12,2) DEFAULT 0,
                igst DECIMAL(12,2) DEFAULT 0,
                created_by INT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ GST Master table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize gst_master table:', err.message);
    }
}

async function initRevenueLevelsTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS revenue_levels (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                fixed_per_month DECIMAL(12,2) DEFAULT 0,
                fixed_per_line DECIMAL(12,2) DEFAULT 0,
                fixed_per_vas DECIMAL(12,2) DEFAULT 0,
                rent_share_pct DECIMAL(8,4) DEFAULT 0,
                calling_share_pct DECIMAL(8,4) DEFAULT 0,
                rg_share_pct DECIMAL(8,4) DEFAULT 0,
                created_by INT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ Revenue Levels table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize revenue_levels table:', err.message);
    }
}

async function initModemSchemesTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS modem_schemes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                modem_rental DECIMAL(12,2) DEFAULT 0,
                revenue_per_month DECIMAL(12,2) DEFAULT 0,
                remarks TEXT DEFAULT NULL,
                created_by INT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ Modem Schemes table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize modem_schemes table:', err.message);
    }
}

async function initBaTspMasterTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ba_tsp_master (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                type ENUM('BA','TSP') NOT NULL DEFAULT 'BA',
                contact_person VARCHAR(255) DEFAULT NULL,
                phone VARCHAR(50) DEFAULT NULL,
                email VARCHAR(255) DEFAULT NULL,
                gst_no VARCHAR(15) DEFAULT NULL,
                pan_no VARCHAR(10) DEFAULT NULL,
                address TEXT DEFAULT NULL,
                created_by INT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ BA/TSP Master table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize ba_tsp_master table:', err.message);
    }
}

async function initBsnlStaffContactsTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS bsnl_staff_contacts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                designation VARCHAR(100) DEFAULT NULL,
                section VARCHAR(100) DEFAULT NULL,
                circle VARCHAR(100) DEFAULT NULL,
                oa VARCHAR(100) DEFAULT NULL,
                mobile VARCHAR(50) DEFAULT NULL,
                landline VARCHAR(50) DEFAULT NULL,
                email VARCHAR(255) DEFAULT NULL,
                email2 VARCHAR(255) DEFAULT NULL,
                dob DATE DEFAULT NULL,
                anniversary DATE DEFAULT NULL,
                remarks TEXT DEFAULT NULL,
                created_by INT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ BSNL Staff Contacts table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize bsnl_staff_contacts table:', err.message);
    }
}

async function initManualCollectionsTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS manual_collections (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_name VARCHAR(255) NOT NULL,
                collection_date DATE NOT NULL,
                payment_mode VARCHAR(50) NOT NULL,
                amount DECIMAL(14,2) NOT NULL DEFAULT 0,
                receipt_no VARCHAR(100) DEFAULT NULL,
                cheque_no VARCHAR(100) DEFAULT NULL,
                bank_name VARCHAR(255) DEFAULT NULL,
                remarks TEXT DEFAULT NULL,
                created_by INT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ Manual Collections table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize manual_collections table:', err.message);
    }
}

async function initBaLevelsTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ba_levels (
                id INT AUTO_INCREMENT PRIMARY KEY,
                level_name VARCHAR(100) NOT NULL,
                revenue_share DECIMAL(8,4) DEFAULT 0,
                revenue_share_rent DECIMAL(8,4) DEFAULT 0,
                revenue_share_call DECIMAL(8,4) DEFAULT 0,
                revenue_share_rng DECIMAL(8,4) DEFAULT 0,
                revenue_share_modem DECIMAL(8,4) DEFAULT 0,
                revenue_share_fixed DECIMAL(8,4) DEFAULT 0,
                created_by INT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
        console.log('✅ BA Levels table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize ba_levels table:', err.message);
    }
}

async function initBillEmailLog() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS bill_email_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT NOT NULL,
                billing_month VARCHAR(20) NOT NULL,
                billing_year VARCHAR(10) NOT NULL,
                email_to VARCHAR(255),
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sent_by INT DEFAULT NULL,
                source ENUM('individual','bulk') DEFAULT 'individual',
                INDEX idx_cust_month (customer_id, billing_month, billing_year)
            ) ENGINE=InnoDB
        `);
        console.log('✅ Bill email log table initialized.');
    } catch (err) {
        console.error('⚠️ Could not initialize bill_email_log table:', err.message);
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
            { name: 'channels', type: 'INT DEFAULT 0' },
            { name: 'default_complaint_priority', type: "VARCHAR(10) DEFAULT 'Low'" }
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

// ── Login Logs Table ──────────────────────────────────────────
async function initLoginLogsTable() {
    try {
        await pool.query(`CREATE TABLE IF NOT EXISTS login_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            username VARCHAR(100),
            user_name VARCHAR(150),
            ip_address VARCHAR(50),
            city VARCHAR(100),
            region VARCHAR(100),
            country VARCHAR(100),
            isp VARCHAR(200),
            user_agent TEXT,
            login_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user (user_id),
            INDEX idx_login_at (login_at)
        )`);
    } catch (err) {
        console.error('Login logs table init error:', err.message);
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
        if (err) return res.status(401).json({ error: 'Session expired or invalid token.' });
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

        // Parse permissions from DB
        let permissions = [];
        if (user.permissions) {
            try { permissions = typeof user.permissions === 'string' ? JSON.parse(user.permissions) : user.permissions; } catch (e) { permissions = []; }
        }

        // Create JWT token
        const token = jwt.sign({ id: user.id, username: user.username, role: user.role }, JWT_SECRET, { expiresIn: '12h' });

        // Parse work_types
        let work_types = [];
        if (user.work_types) {
            try { work_types = typeof user.work_types === 'string' ? JSON.parse(user.work_types) : user.work_types; } catch(e) { work_types = []; }
        }
        // Parse allowed_circles / allowed_oas
        let allowed_circles = [], allowed_oas = [];
        try { allowed_circles = JSON.parse(user.allowed_circles || '[]'); } catch(e) {}
        try { allowed_oas = JSON.parse(user.allowed_oas || '[]'); } catch(e) {}

        // ── Log login (IP + geolocation) — fire-and-forget ──────
        const clientIP = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.ip || req.socket?.remoteAddress || '';
        const userAgent = req.headers['user-agent'] || '';
        const cleanIP = clientIP.replace('::ffff:', '');
        (async () => {
            const geo = await getGeoData(cleanIP);
            const { city, region, country, isp } = geo;
            // Always insert the login log, even without geo data
            try {
                await pool.query(
                    `INSERT INTO login_logs (user_id, username, user_name, ip_address, city, region, country, isp, user_agent)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [user.id, user.username, user.name, cleanIP, city, region, country, isp, userAgent]
                );
            } catch (dbErr) {
                console.error('Login log DB insert error:', dbErr.message);
            }
        })();

        res.json({ token, user: {
            id: user.id,
            username: user.username,
            role: user.role,
            name: user.name,
            permissions: permissions,
            work_types: work_types,
            allowed_circles: allowed_circles,
            allowed_oas: allowed_oas,
            allowed_circle: user.allowed_circle,
            allowed_oa: user.allowed_oa,
            backdate_rights: user.backdate_rights,
            purpose_technical: !!user.purpose_technical,
            purpose_clerical: !!user.purpose_clerical
        }});
    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ error: 'Server error during login', details: error.message });
    }
});

// --- LOGIN LOGS API (admin only) ---
app.get('/api/login-logs', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
    try {
        const { user_id, start, end, limit = 100, offset = 0 } = req.query;
        let where = [];
        const params = [];
        if (user_id) { where.push('l.user_id = ?'); params.push(user_id); }
        if (start) { where.push('DATE(l.login_at) >= ?'); params.push(start); }
        if (end) { where.push('DATE(l.login_at) <= ?'); params.push(end); }
        const whereStr = where.length ? 'WHERE ' + where.join(' AND ') : '';
        const [rows] = await pool.query(`
            SELECT l.* FROM login_logs l ${whereStr}
            ORDER BY l.login_at DESC LIMIT ? OFFSET ?
        `, [...params, parseInt(limit), parseInt(offset)]);
        const [[{ total }]] = await pool.query(
            `SELECT COUNT(*) as total FROM login_logs l ${whereStr}`, params
        );
        res.json({ rows, total });
    } catch (err) {
        console.error('Login logs error:', err);
        res.status(500).json({ error: 'Failed to fetch login logs' });
    }
});

// --- TASK CATEGORIES ROUTES ---
app.get('/api/task-categories', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM task_categories ORDER BY name');
        res.json(rows);
    } catch (err) { res.status(500).json({ error: 'Failed to fetch task categories' }); }
});

app.post('/api/task-categories', authenticateToken, isAdmin, async (req, res) => {
    const { name } = req.body;
    if (!name) return res.status(400).json({ error: 'Name required' });
    try {
        const [result] = await pool.query('INSERT INTO task_categories (name) VALUES (?)', [name.trim()]);
        res.status(201).json({ id: result.insertId, name: name.trim() });
    } catch (err) {
        if (err.code === 'ER_DUP_ENTRY') return res.status(409).json({ error: 'Category already exists' });
        res.status(500).json({ error: 'Failed to create category' });
    }
});

app.delete('/api/task-categories/:id', authenticateToken, isAdmin, async (req, res) => {
    try {
        await pool.query('DELETE FROM task_categories WHERE id = ?', [req.params.id]);
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed to delete category' }); }
});

// --- ASSIGNABLE USERS (Hierarchy-based via user_managers table, excludes self) ---
// Optional ?work_type=engineer to return only users with that work type
app.get('/api/users/assignable', authenticateToken, async (req, res) => {
    try {
        const currentUser = req.user;
        const filterWorkType = req.query.work_type || null;
        let rows;
        if (currentUser.role === 'admin') {
            [rows] = await pool.query(
                'SELECT id, username, name, role, work_types FROM users WHERE username != ? ORDER BY name',
                [currentUser.username]
            );
        } else {
            const [meRow] = await pool.query('SELECT id FROM users WHERE username = ?', [currentUser.username]);
            const myId = meRow[0]?.id;
            [rows] = await pool.query(
                `SELECT u.id, u.username, u.name, u.role, u.work_types
                 FROM users u
                 JOIN user_managers um ON u.id = um.user_id
                 WHERE um.manager_id = ? ORDER BY u.name`,
                [myId]
            );
        }
        // Filter by work_type if requested
        if (filterWorkType) {
            rows = rows.filter(u => {
                let wt = u.work_types;
                if (typeof wt === 'string') { try { wt = JSON.parse(wt); } catch(e) { wt = []; } }
                return Array.isArray(wt) && wt.includes(filterWorkType);
            });
        }
        res.json(rows);
    } catch (err) {
        console.error('Assignable users error:', err);
        res.status(500).json({ error: 'Failed to fetch assignable users' });
    }
});

// --- USER MANAGERS (Multiple managers per user) ---
// Get all managers for a specific user
app.get('/api/user-managers/:userId', authenticateToken, isAdmin, async (req, res) => {
    try {
        const [rows] = await pool.query(
            'SELECT manager_id FROM user_managers WHERE user_id = ?',
            [req.params.userId]
        );
        res.json(rows.map(r => r.manager_id));
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch managers' });
    }
});

// Set managers for a user (replaces all existing)
app.post('/api/user-managers/:userId', authenticateToken, isAdmin, async (req, res) => {
    const { manager_ids } = req.body; // array of manager IDs
    const userId = req.params.userId;
    const conn = await pool.getConnection();
    try {
        await conn.beginTransaction();
        await conn.query('DELETE FROM user_managers WHERE user_id = ?', [userId]);
        if (manager_ids && manager_ids.length > 0) {
            const values = manager_ids.map(mid => [userId, mid]);
            await conn.query('INSERT INTO user_managers (user_id, manager_id) VALUES ?', [values]);
        }
        await conn.commit();
        res.json({ success: true });
    } catch (err) {
        await conn.rollback();
        console.error('Set managers error:', err);
        res.status(500).json({ error: 'Failed to set managers' });
    } finally {
        conn.release();
    }
});

// --- USERS LIST (lightweight, admin only - for dropdowns) ---
app.get('/api/users/list', authenticateToken, isAdmin, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT id, username, name, role FROM users ORDER BY name');
        res.json(rows);
    } catch(err) {
        res.status(500).json({ error: 'Failed to fetch users list' });
    }
});

// --- USERS ROUTES (Admin Only) ---
app.get('/api/users', authenticateToken, isAdmin, async (req, res) => {
    try {
        const [rows] = await pool.query(`
            SELECT u1.id, u1.username, u1.role, u1.name, u1.mobile, u1.email,
                   u1.allowed_circle, u1.allowed_oa, u1.allowed_circles, u1.allowed_oas,
                   u1.view_circles, u1.view_oas,
                   u1.permissions, u1.reports_to, u1.created_at,
                   u1.department, u1.work_types, u1.allowed_customers,
                   u1.backdate_rights, u1.purpose_technical, u1.purpose_clerical,
                   u2.name as manager_name, u2.username as manager_username
            FROM users u1
            LEFT JOIN users u2 ON u1.reports_to = u2.id
        `);
        // Batch fetch all manager mappings in ONE query instead of N+1
        const [allMgrs] = await pool.query('SELECT user_id, manager_id FROM user_managers');
        const mgrMap = {};
        allMgrs.forEach(m => {
            if (!mgrMap[m.user_id]) mgrMap[m.user_id] = [];
            mgrMap[m.user_id].push(m.manager_id);
        });
        rows.forEach(row => { row.manager_ids = mgrMap[row.id] || []; });
        res.json(rows);
    } catch (err) {
        console.error('Fetch users error:', err);
        res.status(500).json({ error: 'Failed to fetch users' });
    }
});

app.post('/api/users', authenticateToken, isAdmin, async (req, res) => {
    const { username, password, role, name, mobile, email, permissions, reports_to, manager_ids, department, work_types, allowed_circles, allowed_oas, view_circles, view_oas } = req.body;
    try {
        const hash = await bcrypt.hash(password, 10);
        const [result] = await pool.query(
            'INSERT INTO users (username, password_hash, role, name, mobile, email, permissions, reports_to, backdate_rights, department, work_types, allowed_circles, allowed_oas, view_circles, view_oas) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [username, hash, role || 'user', name || null, mobile || null, email || null, JSON.stringify(permissions || []), reports_to || null, req.body.backdate_rights || false, department || 'Technical', JSON.stringify(work_types || []), JSON.stringify(allowed_circles || []), JSON.stringify(allowed_oas || []), JSON.stringify(view_circles || []), JSON.stringify(view_oas || [])]
        );
        // Save multiple managers in user_managers table
        const newUserId = result.insertId;
        const mgrIds = Array.isArray(manager_ids) ? manager_ids : (reports_to ? [reports_to] : []);
        if (mgrIds.length > 0) {
            const mgrValues = mgrIds.map(mid => [newUserId, mid]);
            await pool.query('INSERT INTO user_managers (user_id, manager_id) VALUES ?', [mgrValues]);
        }
        res.status(201).json({ id: newUserId, username, role });
    } catch (err) {
        console.error('Create user error:', err);
        res.status(500).json({ error: 'Failed to create user' });
    }
});

app.put('/api/users/:id', authenticateToken, isAdmin, async (req, res) => {
    const { role, name, mobile, email, permissions, reports_to, manager_ids, department, work_types, allowed_circles, allowed_oas, view_circles, view_oas, allowed_customers, purpose_technical, purpose_clerical } = req.body;
    try {
        await pool.query(
            'UPDATE users SET role=?, name=?, mobile=?, email=?, permissions=?, reports_to=?, backdate_rights=?, department=?, work_types=?, allowed_circles=?, allowed_oas=?, view_circles=?, view_oas=?, allowed_customers=?, purpose_technical=?, purpose_clerical=? WHERE id=?',
            [role, name || null, mobile || null, email || null, JSON.stringify(permissions || []), reports_to || null, req.body.backdate_rights || false, department || 'Technical', JSON.stringify(work_types || []), JSON.stringify(allowed_circles || []), JSON.stringify(allowed_oas || []), JSON.stringify(view_circles || []), JSON.stringify(view_oas || []), allowed_customers || null, purpose_technical ? 1 : 0, purpose_clerical ? 1 : 0, req.params.id]
        );
        // Update multiple managers in user_managers table
        const userId = parseInt(req.params.id);
        await pool.query('DELETE FROM user_managers WHERE user_id = ?', [userId]);
        const mgrIds = Array.isArray(manager_ids) ? manager_ids : (reports_to ? [reports_to] : []);
        if (mgrIds.length > 0) {
            const mgrValues = mgrIds.map(mid => [userId, parseInt(mid)]);
            await pool.query('INSERT INTO user_managers (user_id, manager_id) VALUES ?', [mgrValues]);
        }
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

// Public check: does an active complaint exist for this number?
app.get('/api/complaints/check', async (req, res) => {
    const { std_code, telephone_number } = req.query;
    try {
        const [rows] = await pool.query(
            `SELECT id, complaint_no, status FROM complaints
             WHERE telephone_number = ? AND std_code = ?
             AND status NOT IN ('Resolved', 'Cancelled')
             ORDER BY created_at DESC LIMIT 1`,
            [telephone_number, std_code]
        );
        if (rows.length > 0) {
            const ticketId = `CRM-${String(rows[0].id).padStart(4, '0')}`;
            res.json({ exists: true, ticket_id: ticketId, complaint_no: rows[0].complaint_no, status: rows[0].status });
        } else {
            res.json({ exists: false });
        }
    } catch (err) {
        res.json({ exists: false });
    }
});

app.get('/api/complaints', authenticateToken, async (req, res) => {
    try {
        const user = req.user;
        let query, params = [];

        if (user.role === 'admin') {
            // Admin sees all complaints
            query = `SELECT c.*, u.name as assigned_username, u.username as assigned_user
                     FROM complaints c LEFT JOIN users u ON c.assigned_to = u.id
                     ORDER BY c.created_at DESC`;
        } else {
            // Get user's own DB row (for allowed_circles + view_circles/oas + allowed_customers)
            const [[meRow]] = await pool.query(
                'SELECT id, allowed_circles, allowed_oas, view_circles, view_oas, allowed_customers FROM users WHERE id = ?', [user.id]
            );
            const myId = meRow?.id || user.id;

            // Parse circle/OA arrays
            let myCircles = [], myOAs = [], viewCircles = [], viewOAs = [], myCustomers = [];
            try { myCircles = JSON.parse(meRow?.allowed_circles || '[]'); } catch (e) {}
            try { myOAs = JSON.parse(meRow?.allowed_oas || '[]'); } catch (e) {}
            try { viewCircles = JSON.parse(meRow?.view_circles || '[]'); } catch (e) {}
            try { viewOAs = JSON.parse(meRow?.view_oas || '[]'); } catch (e) {}
            try { myCustomers = JSON.parse(meRow?.allowed_customers || '[]'); } catch (e) {}

            // Get subordinates (engineers managed by this user)
            const [subs] = await pool.query(
                'SELECT user_id FROM user_managers WHERE manager_id = ?', [myId]
            );
            const subIds = subs.map(s => s.user_id);
            const allIds = [myId, ...subIds]; // self + subordinates

            // WHERE: assigned to self or subordinates
            const idPlaceholders = allIds.map(() => '?').join(',');
            let where = `c.assigned_to IN (${idPlaceholders})`;
            params = [...allIds];

            // Also: unassigned complaints from user's assigned circles
            if (myCircles.length > 0) {
                const circlePH = myCircles.map(() => '?').join(',');
                where += ` OR (c.assigned_to IS NULL AND c.circle IN (${circlePH}))`;
                params.push(...myCircles);
            }

            // View purpose: show complaints from view_circles
            if (viewCircles.length > 0) {
                const vcPH = viewCircles.map(() => '?').join(',');
                where += ` OR c.circle IN (${vcPH})`;
                params.push(...viewCircles);
            }

            // View purpose: show complaints from view_oas
            if (viewOAs.length > 0) {
                const voPH = viewOAs.map(() => '?').join(',');
                where += ` OR c.oa_name IN (${voPH})`;
                params.push(...viewOAs);
            }

            query = `SELECT c.*, u.name as assigned_username, u.username as assigned_user
                     FROM complaints c LEFT JOIN users u ON c.assigned_to = u.id
                     WHERE (${where})
                     ORDER BY c.created_at DESC`;
        }

        const [rows] = await pool.query(query, params);
        res.json(rows);
    } catch (err) {
        console.error('Fetch complaints error:', err);
        res.status(500).json({ error: 'Failed to fetch complaints' });
    }
});

// Complaints Report with filters
app.get('/api/complaints/report', authenticateToken, async (req, res) => {
    const currentUser = req.user;
    try {
        const { circle, oa, status, engineer, search, start, end, limit = 40, offset = 0 } = req.query;
        let where = [];
        const params = [];

        // ── Access control: non-admin sees only own area ───────────────────
        if (currentUser.role !== 'admin') {
            const [[meRow]] = await pool.query(
                'SELECT id, allowed_circles, allowed_oas, view_circles, view_oas FROM users WHERE id = ?', [currentUser.id]
            );
            const myId = meRow?.id || currentUser.id;

            // Subordinates
            const [subs] = await pool.query(
                'SELECT user_id FROM user_managers WHERE manager_id = ?', [myId]
            );
            const subIds = subs.map(r => r.user_id);
            const allIds = [myId, ...subIds];

            // Allowed circles + view circles/OAs
            let myCircles = [], viewCircles = [], viewOAs = [];
            try { myCircles = JSON.parse(meRow?.allowed_circles || '[]'); } catch(e) {}
            try { viewCircles = JSON.parse(meRow?.view_circles || '[]'); } catch(e) {}
            try { viewOAs = JSON.parse(meRow?.view_oas || '[]'); } catch(e) {}

            const idPH = allIds.map(() => '?').join(',');
            let accessParts = [`c.assigned_to IN (${idPH})`];
            params.push(...allIds);

            if (myCircles.length > 0) {
                const cirPH = myCircles.map(() => '?').join(',');
                accessParts.push(`(c.assigned_to IS NULL AND c.circle IN (${cirPH}))`);
                params.push(...myCircles);
            }
            if (viewCircles.length > 0) {
                const vcPH = viewCircles.map(() => '?').join(',');
                accessParts.push(`c.circle IN (${vcPH})`);
                params.push(...viewCircles);
            }
            if (viewOAs.length > 0) {
                const voPH = viewOAs.map(() => '?').join(',');
                accessParts.push(`c.oa IN (${voPH})`);
                params.push(...viewOAs);
            }
            where.push(`(${accessParts.join(' OR ')})`);
        }
        // ──────────────────────────────────────────────────────────────────

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
        const [[{ total }]] = await pool.query(
            `SELECT COUNT(*) as total FROM complaints c LEFT JOIN users u ON c.assigned_to = u.id ${whereStr}`,
            params
        );
        res.json({ rows, total });
    } catch (err) {
        console.error('Complaints report error:', err);
        res.status(500).json({ error: 'Failed to fetch report' });
    }
});

// Update complaint (engineer action)
app.put('/api/complaints/:id', authenticateToken, async (req, res) => {
    const { fault_at, status, remark, assigner_comments, priority, assigned_to } = req.body;
    const currentUser = req.user;
    try {
        // ── Server-side validation: fault_at + remark are mandatory ────────
        if (status && status !== 'Forward to Senior') {
            if (!fault_at || String(fault_at).trim() === '') {
                return res.status(400).json({ error: 'Fault At is required before saving.' });
            }
            if (!remark || String(remark).trim() === '') {
                return res.status(400).json({ error: 'Engineer Remark is required before saving.' });
            }
        } else if (status === 'Forward to Senior') {
            if (!remark || String(remark).trim() === '') {
                return res.status(400).json({ error: 'Remarks are required when forwarding to senior.' });
            }
        }
        // ──────────────────────────────────────────────────────────────────

        const fields = [];
        const params = [];

        // ── Forward to Senior ──────────────────────────────────────────────
        if (status === 'Forward to Senior') {
            // Find current user's manager
            const [[meRow]] = await pool.query('SELECT id FROM users WHERE id = ?', [currentUser.id]);
            const myId = meRow?.id || currentUser.id;
            const [managers] = await pool.query(
                'SELECT manager_id FROM user_managers WHERE user_id = ? LIMIT 1', [myId]
            );
            if (!managers.length) {
                return res.status(400).json({ error: 'No senior/manager assigned to your account. Contact admin.' });
            }
            const managerId = managers[0].manager_id;
            fields.push('status=?');        params.push('Forward to Senior');
            fields.push('assigned_to=?');   params.push(managerId);
            fields.push('remark=?');        params.push(remark || '');
            if (assigner_comments !== undefined) { fields.push('assigner_comments=?'); params.push(assigner_comments); }
        } else {
            // ── Normal update ────────────────────────────────────────────
            if (fault_at !== undefined) { fields.push('fault_at=?'); params.push(fault_at); }
            if (status !== undefined) {
                fields.push('status=?'); params.push(status);
                if (status === 'Resolved' || status === 'Cancelled') {
                    fields.push('resolved_by=?'); params.push(currentUser.name || currentUser.username);
                    // Only set Pending Verification if resolver is a subordinate (not admin/senior)
                    // Senior = has at least one subordinate in user_managers
                    const [[subCnt]] = await pool.query(
                        'SELECT COUNT(*) as cnt FROM user_managers WHERE manager_id = ?', [currentUser.id]
                    );
                    const resolverIsSeniorOrAdmin = currentUser.role === 'admin' || subCnt.cnt > 0;
                    if (!resolverIsSeniorOrAdmin) {
                        fields.push('verification_status=?'); params.push('Pending Verification');
                    } else {
                        // Senior/Admin resolves directly → mark as auto-verified
                        fields.push('verification_status=?'); params.push('Verified');
                        fields.push('verified_by=?'); params.push(currentUser.name || currentUser.username);
                    }
                }
            }
            if (remark !== undefined) { fields.push('remark=?'); params.push(remark); }
            if (assigner_comments !== undefined) { fields.push('assigner_comments=?'); params.push(assigner_comments); }
            if (priority !== undefined) { fields.push('priority=?'); params.push(priority); }
            if (assigned_to !== undefined) { fields.push('assigned_to=?'); params.push(assigned_to); }
        }

        if (fields.length === 0) return res.status(400).json({ error: 'No fields to update' });
        params.push(req.params.id);
        await pool.query(`UPDATE complaints SET ${fields.join(',')} WHERE id=?`, params);

        // ── Auto-create diary task when complaint is assigned to an engineer ──
        if (assigned_to !== undefined && assigned_to) {
            try {
                const [[complaintData]] = await pool.query('SELECT complaint_no, customer_name, complainee_name, telephone_number, description FROM complaints WHERE id=?', [req.params.id]);
                if (complaintData) {
                    const engineerUserId = parseInt(assigned_to);
                    const complaintId = req.params.id;
                    const subject = (complaintData.customer_name || '') + ' - ' + (complaintData.telephone_number || '');
                    const desc = 'Complaint #' + (complaintData.complaint_no || complaintId) + ' | Customer: ' + (complaintData.customer_name || 'N/A') + ' | Phone: ' + (complaintData.telephone_number || 'N/A') + (complaintData.description ? ' | ' + complaintData.description : '');
                    const adminName = currentUser.name || currentUser.username;
                    // Check if a diary task already exists for this complaint
                    const [existing] = await pool.query('SELECT id FROM diary_tasks WHERE source_type = ? AND source_task_id = ? AND user_id = ?', ['complaint', parseInt(complaintId), engineerUserId]);
                    if (!existing.length) {
                        const [dtResult] = await pool.query(
                            `INSERT INTO diary_tasks (user_id, title, description, category, priority, task_type, due_date, source_type, source_task_id, assigned_by, status)
                             VALUES (?, ?, ?, 'Complaints', 'High', 'Work', CURDATE(), 'complaint', ?, ?, 'Pending')`,
                            [engineerUserId, 'Complaint #' + (complaintData.complaint_no || complaintId) + ' - ' + subject, desc, parseInt(complaintId), adminName]
                        );
                        await pool.query('INSERT INTO diary_task_history (task_id, previous_status, new_status, comment, changed_by) VALUES (?,?,?,?,?)',
                            [dtResult.insertId, null, 'Pending', 'Auto-created from complaint assignment', currentUser.id]);
                    }
                }
            } catch(autoTaskErr) {
                console.error('Auto diary task creation error (non-fatal):', autoTaskErr.message);
            }
        }

        // ── Send resolution email if senior/admin directly resolves (no verification queue) ──
        if (status === 'Resolved') {
            const [[subCnt]] = await pool.query('SELECT COUNT(*) as cnt FROM user_managers WHERE manager_id = ?', [currentUser.id]);
            const resolverIsSeniorOrAdmin = currentUser.role === 'admin' || subCnt.cnt > 0;
            if (resolverIsSeniorOrAdmin) {
                // Fetch complaint details for email
                const [[comp]] = await pool.query('SELECT * FROM complaints WHERE id=?', [req.params.id]);
                if (comp && comp.email) {
                    sendResolutionEmail({
                        email: comp.email,
                        complaint_no: comp.complaint_no,
                        customer_name: comp.customer_name,
                        complainee_name: comp.complainee_name,
                        telephone_number: comp.telephone_number,
                        std_code: comp.std_code,
                        company_name: comp.customer_name
                    }).catch(err => console.error('Resolution mail error:', err.message));
                }
            }
        }

        res.json({ message: 'Complaint updated' });
    } catch (err) {
        console.error('Update complaint error:', err);
        res.status(500).json({ error: 'Failed to update complaint' });
    }
});

// GET complaints pending verification (for seniors/managers)
app.get('/api/complaints/pending-verification', authenticateToken, async (req, res) => {
    const currentUser = req.user;
    try {
        let rows;
        if (currentUser.role === 'admin') {
            // Admin sees all pending verification
            [rows] = await pool.query(
                `SELECT c.*, u.name as assigned_username FROM complaints c
                 LEFT JOIN users u ON c.assigned_to = u.id
                 WHERE c.verification_status = 'Pending Verification'
                 ORDER BY c.updated_at DESC`
            );
        } else {
            // Non-admin: see complaints resolved by their subordinates
            const [meRow] = await pool.query('SELECT id FROM users WHERE username=?', [currentUser.username]);
            const myId = meRow[0]?.id;
            [rows] = await pool.query(
                `SELECT c.*, u.name as assigned_username FROM complaints c
                 LEFT JOIN users u ON c.assigned_to = u.id
                 LEFT JOIN users resolver ON resolver.name = c.resolved_by OR resolver.username = c.resolved_by
                 LEFT JOIN user_managers um ON um.user_id = resolver.id
                 WHERE c.verification_status = 'Pending Verification' AND um.manager_id = ?
                 ORDER BY c.updated_at DESC`,
                [myId]
            );
        }
        res.json(rows);
    } catch (err) {
        console.error('Pending verification error:', err);
        res.status(500).json({ error: 'Failed to fetch' });
    }
});

// PUT verify/reject a complaint
app.put('/api/complaints/:id/verify', authenticateToken, async (req, res) => {
    const { action, send_email, verif_remarks } = req.body; // 'Verified' or 'Rejected'
    const currentUser = req.user;
    try {
        if (action === 'Verified') {
            await pool.query(
                `UPDATE complaints SET verification_status='Verified', verified_by=? WHERE id=?`,
                [currentUser.name || currentUser.username, req.params.id]
            );
            // Send resolution email if send_email is not explicitly false
            if (send_email !== false) {
                const [[comp]] = await pool.query('SELECT * FROM complaints WHERE id=?', [req.params.id]);
                if (comp && comp.email) {
                    sendResolutionEmail({
                        email: comp.email,
                        complaint_no: comp.complaint_no,
                        customer_name: comp.customer_name,
                        complainee_name: comp.complainee_name,
                        telephone_number: comp.telephone_number,
                        std_code: comp.std_code,
                        company_name: comp.customer_name,
                        verif_remarks: verif_remarks || ''
                    }).catch(err => console.error('Resolution mail (verify) error:', err.message));
                }
            }
        } else if (action === 'Rejected') {
            // Reject → back to In Progress, clear verification
            await pool.query(
                `UPDATE complaints SET verification_status='Rejected', status='In Progress', resolved_by=NULL WHERE id=?`,
                [req.params.id]
            );
        }
        res.json({ message: `Complaint ${action}` });
    } catch (err) {
        console.error('Verify error:', err);
        res.status(500).json({ error: 'Failed to verify' });
    }
});

// Bulk reassign active complaints from one engineer to another (leave/absence)
app.post('/api/complaints/bulk-reassign', authenticateToken, async (req, res) => {
    const currentUser = req.user;
    try {
        const [[subCnt]] = await pool.query('SELECT COUNT(*) as cnt FROM user_managers WHERE manager_id = ?', [currentUser.id]);
        const isSeniorOrAdmin = currentUser.role === 'admin' || subCnt.cnt > 0;
        if (!isSeniorOrAdmin) return res.status(403).json({ error: 'Only admin or senior engineer can reassign complaints.' });

        const { from_id, to_id, leave_from, leave_to } = req.body;
        if (!from_id || !to_id) return res.status(400).json({ error: 'from_id and to_id are required.' });
        if (String(from_id) === String(to_id)) return res.status(400).json({ error: 'From and To engineer cannot be the same.' });

        // Get names
        const [[fromUser]] = await pool.query('SELECT name FROM users WHERE id = ?', [from_id]);
        const [[toUser]] = await pool.query('SELECT name FROM users WHERE id = ?', [to_id]);

        // Get complaint IDs before reassign (for tracking)
        const [affectedComplaints] = await pool.query(
            `SELECT id FROM complaints WHERE assigned_to = ? AND status NOT IN ('Resolved', 'Cancelled')`,
            [from_id]
        );
        const complaintIds = affectedComplaints.map(c => c.id);

        // Reassign only active (non-resolved/non-cancelled) complaints
        const [result] = await pool.query(
            `UPDATE complaints SET assigned_to = ? WHERE assigned_to = ? AND status NOT IN ('Resolved', 'Cancelled')`,
            [to_id, from_id]
        );
        const count = result.affectedRows;

        // Track reassignment history
        if (count > 0) {
            await pool.query(
                `INSERT INTO complaint_reassignments (from_user_id, to_user_id, from_name, to_name, leave_from, leave_to, complaint_ids, complaint_count, reassigned_by)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                [from_id, to_id, fromUser?.name || '', toUser?.name || '', leave_from || null, leave_to || null, JSON.stringify(complaintIds), count, currentUser.id]
            );
        }

        res.json({
            success: true,
            reassigned: count,
            message: `${count} active complaint(s) reassigned from ${fromUser?.name} to ${toUser?.name}.`
        });
    } catch (err) {
        console.error('Bulk reassign error:', err);
        res.status(500).json({ error: 'Bulk reassign failed: ' + err.message });
    }
});

// GET active reassignments list
app.get('/api/complaints/reassignments', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') {
            const [[subCnt]] = await pool.query('SELECT COUNT(*) as cnt FROM user_managers WHERE manager_id = ?', [req.user.id]);
            if (subCnt.cnt === 0) return res.status(403).json({ error: 'Unauthorized' });
        }
        const [rows] = await pool.query(
            `SELECT r.*, rb.name as reassigned_by_name
             FROM complaint_reassignments r
             LEFT JOIN users rb ON r.reassigned_by = rb.id
             ORDER BY r.reassigned_at DESC LIMIT 50`
        );
        res.json(rows);
    } catch (err) {
        console.error('Get reassignments error:', err);
        res.status(500).json({ error: 'Failed to fetch reassignments' });
    }
});

// Cancel/Revert a reassignment — move complaints back to original engineer
app.post('/api/complaints/cancel-reassignment', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') {
            const [[subCnt]] = await pool.query('SELECT COUNT(*) as cnt FROM user_managers WHERE manager_id = ?', [req.user.id]);
            if (subCnt.cnt === 0) return res.status(403).json({ error: 'Unauthorized' });
        }
        const { reassignment_id } = req.body;
        if (!reassignment_id) return res.status(400).json({ error: 'reassignment_id required' });

        // Get reassignment record
        const [[record]] = await pool.query('SELECT * FROM complaint_reassignments WHERE id = ? AND status = "active"', [reassignment_id]);
        if (!record) return res.status(404).json({ error: 'Active reassignment not found' });

        const complaintIds = JSON.parse(record.complaint_ids || '[]');
        if (complaintIds.length === 0) return res.status(400).json({ error: 'No complaints to revert' });

        // Revert: move complaints back — only those still assigned to to_user and still active
        const placeholders = complaintIds.map(() => '?').join(',');
        const [result] = await pool.query(
            `UPDATE complaints SET assigned_to = ?
             WHERE id IN (${placeholders}) AND assigned_to = ? AND status NOT IN ('Resolved', 'Cancelled')`,
            [record.from_user_id, ...complaintIds, record.to_user_id]
        );

        // Mark reassignment as cancelled
        await pool.query(
            'UPDATE complaint_reassignments SET status = "cancelled", cancelled_at = NOW() WHERE id = ?',
            [reassignment_id]
        );

        res.json({
            success: true,
            reverted: result.affectedRows,
            message: `${result.affectedRows} complaint(s) reverted back to ${record.from_name}.`
        });
    } catch (err) {
        console.error('Cancel reassignment error:', err);
        res.status(500).json({ error: 'Cancel failed: ' + err.message });
    }
});

// Bulk auto-assign all unassigned complaints based on circle/OA mapping
app.post('/api/complaints/bulk-auto-assign', authenticateToken, isAdmin, async (req, res) => {
    try {
        const [unassigned] = await pool.query(
            `SELECT id, circle, oa_name, telephone_number, std_code
             FROM complaints WHERE assigned_to IS NULL`
        );
        let assigned = 0, skipped = 0;
        for (const c of unassigned) {
            const engineerId = await autoAssignEngineer(c.circle, c.oa_name, c.telephone_number, c.std_code);
            if (engineerId) {
                await pool.query('UPDATE complaints SET assigned_to=? WHERE id=?', [engineerId, c.id]);
                assigned++;
            } else {
                skipped++;
            }
        }
        res.json({
            success: true,
            total: unassigned.length,
            assigned,
            skipped,
            message: `${assigned} complaints auto-assigned, ${skipped} could not be matched.`
        });
    } catch (err) {
        console.error('Bulk auto-assign error:', err);
        res.status(500).json({ error: 'Bulk auto-assign failed: ' + err.message });
    }
});

// ══════════════════════════════════════════════════════════════════════════
// COMPLAINT VIEW REPORT — Senior Engineer / Admin monitoring endpoints
// ══════════════════════════════════════════════════════════════════════════

// Helper: check if user is senior (has subordinates) or admin
async function isSeniorOrAdmin(userId, role) {
    if (role === 'admin') return true;
    const [[{ cnt }]] = await pool.query('SELECT COUNT(*) as cnt FROM user_managers WHERE manager_id = ?', [userId]);
    return cnt > 0;
}

// GET /api/complaints/view-report/summary — per-engineer complaint summary
app.get('/api/complaints/view-report/summary', authenticateToken, async (req, res) => {
    const currentUser = req.user;
    try {
        const senior = await isSeniorOrAdmin(currentUser.id, currentUser.role);
        if (!senior) return res.status(403).json({ error: 'Access denied. Only seniors and admins can access this report.' });

        let where = [];
        const params = [];

        // Non-admin: scope by view_circles + view_oas + allowed_customers + subordinates
        if (currentUser.role !== 'admin') {
            const [[meRow]] = await pool.query('SELECT view_circles, view_oas, allowed_circles, allowed_oas, allowed_customers FROM users WHERE id = ?', [currentUser.id]);
            // Prefer view_circles/view_oas; fallback to allowed_circles/allowed_oas
            let myCircles = []; try { myCircles = JSON.parse(meRow?.view_circles || '[]'); } catch(e) {}
            if (!myCircles.length) try { myCircles = JSON.parse(meRow?.allowed_circles || '[]'); } catch(e) {}
            let myOAs = []; try { myOAs = JSON.parse(meRow?.view_oas || '[]'); } catch(e) {}
            if (!myOAs.length) try { myOAs = JSON.parse(meRow?.allowed_oas || '[]'); } catch(e) {}
            let myCust = (meRow?.allowed_customers || '').split(',').map(s => s.trim()).filter(Boolean);

            const orConds = [];
            if (myCircles.length) { orConds.push(`c.circle IN (${myCircles.map(()=>'?').join(',')})`); params.push(...myCircles); }
            if (myOAs.length) { orConds.push(`c.oa_name IN (${myOAs.map(()=>'?').join(',')})`); params.push(...myOAs); }
            if (myCust.length) { orConds.push(`c.customer_name IN (${myCust.map(()=>'?').join(',')})`); params.push(...myCust); }
            // Also include complaints assigned to self or subordinates
            const [subs] = await pool.query('SELECT user_id FROM user_managers WHERE manager_id = ?', [currentUser.id]);
            const allIds = [currentUser.id, ...subs.map(r => r.user_id)];
            orConds.push(`c.assigned_to IN (${allIds.map(()=>'?').join(',')})`);
            params.push(...allIds);

            if (orConds.length) where.push(`(${orConds.join(' OR ')})`);
        }

        // Apply date filters if provided
        const { start, end } = req.query;
        if (start) { where.push('DATE(c.created_at) >= ?'); params.push(start); }
        if (end) { where.push('DATE(c.created_at) <= ?'); params.push(end); }

        const whereStr = where.length ? 'WHERE ' + where.join(' AND ') : '';

        const [rows] = await pool.query(`
            SELECT
                c.assigned_to as engineer_id,
                COALESCE(u.name, u.username, 'Unassigned') as engineer_name,
                COUNT(*) as total,
                SUM(CASE WHEN c.status = 'Pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN c.status = 'In Progress' THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN c.status = 'Forward to Senior' THEN 1 ELSE 0 END) as forwarded,
                SUM(CASE WHEN c.status = 'Resolved' THEN 1 ELSE 0 END) as resolved,
                SUM(CASE WHEN c.status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled
            FROM complaints c
            LEFT JOIN users u ON c.assigned_to = u.id
            ${whereStr}
            GROUP BY c.assigned_to, u.name, u.username
            ORDER BY total DESC
        `, params);

        // Totals
        const totals = { total: 0, pending: 0, in_progress: 0, forwarded: 0, resolved: 0, cancelled: 0 };
        rows.forEach(r => { totals.total += r.total; totals.pending += r.pending; totals.in_progress += r.in_progress; totals.forwarded += r.forwarded; totals.resolved += r.resolved; totals.cancelled += r.cancelled; });

        res.json({ engineers: rows, totals });
    } catch (err) {
        console.error('View report summary error:', err);
        res.status(500).json({ error: 'Failed to fetch summary' });
    }
});

// GET /api/complaints/view-report — detailed complaints list for view report
app.get('/api/complaints/view-report', authenticateToken, async (req, res) => {
    const currentUser = req.user;
    try {
        const senior = await isSeniorOrAdmin(currentUser.id, currentUser.role);
        if (!senior) return res.status(403).json({ error: 'Access denied.' });

        const { circle, oa, status, engineer_id, search, start, end, limit = 40, offset = 0 } = req.query;
        let where = [];
        const params = [];

        // Non-admin: scope by view_circles/view_oas (fallback to allowed_circles/allowed_oas)
        if (currentUser.role !== 'admin') {
            const [[meRow]] = await pool.query('SELECT view_circles, view_oas, allowed_circles, allowed_oas, allowed_customers FROM users WHERE id = ?', [currentUser.id]);
            let myCircles = []; try { myCircles = JSON.parse(meRow?.view_circles || '[]'); } catch(e) {}
            if (!myCircles.length) try { myCircles = JSON.parse(meRow?.allowed_circles || '[]'); } catch(e) {}
            let myOAs = []; try { myOAs = JSON.parse(meRow?.view_oas || '[]'); } catch(e) {}
            if (!myOAs.length) try { myOAs = JSON.parse(meRow?.allowed_oas || '[]'); } catch(e) {}
            let myCust = (meRow?.allowed_customers || '').split(',').map(s => s.trim()).filter(Boolean);

            const orConds = [];
            if (myCircles.length) { orConds.push(`c.circle IN (${myCircles.map(()=>'?').join(',')})`); params.push(...myCircles); }
            if (myOAs.length) { orConds.push(`c.oa_name IN (${myOAs.map(()=>'?').join(',')})`); params.push(...myOAs); }
            if (myCust.length) { orConds.push(`c.customer_name IN (${myCust.map(()=>'?').join(',')})`); params.push(...myCust); }
            const [subs] = await pool.query('SELECT user_id FROM user_managers WHERE manager_id = ?', [currentUser.id]);
            const allIds = [currentUser.id, ...subs.map(r => r.user_id)];
            orConds.push(`c.assigned_to IN (${allIds.map(()=>'?').join(',')})`);
            params.push(...allIds);
            if (orConds.length) where.push(`(${orConds.join(' OR ')})`);
        }

        if (circle) { where.push('c.circle = ?'); params.push(circle); }
        if (oa) { where.push('c.oa_name = ?'); params.push(oa); }
        if (status) { where.push('c.status = ?'); params.push(status); }
        if (engineer_id) { where.push('c.assigned_to = ?'); params.push(parseInt(engineer_id)); }
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

        const [[{ total }]] = await pool.query(
            `SELECT COUNT(*) as total FROM complaints c LEFT JOIN users u ON c.assigned_to = u.id ${whereStr}`, params
        );

        res.json({ rows, total });
    } catch (err) {
        console.error('View report error:', err);
        res.status(500).json({ error: 'Failed to fetch view report' });
    }
});

// PUT /api/complaints/:id/resolve-on-behalf — Senior resolves complaint for another engineer
app.put('/api/complaints/:id/resolve-on-behalf', authenticateToken, async (req, res) => {
    const currentUser = req.user;
    try {
        const senior = await isSeniorOrAdmin(currentUser.id, currentUser.role);
        if (!senior) return res.status(403).json({ error: 'Only seniors/admins can resolve on behalf.' });

        const { fault_at, remark } = req.body;
        if (!fault_at || !String(fault_at).trim()) return res.status(400).json({ error: 'Fault At is required.' });
        if (!remark || !String(remark).trim()) return res.status(400).json({ error: 'Remark is required.' });

        const resolverName = currentUser.name || currentUser.username;
        await pool.query(`
            UPDATE complaints SET
                status = 'Resolved',
                fault_at = ?,
                remark = ?,
                resolved_by = ?,
                verification_status = 'Verified',
                verified_by = ?
            WHERE id = ?
        `, [fault_at, remark, resolverName, resolverName, req.params.id]);

        // Send resolution email
        const [[comp]] = await pool.query('SELECT * FROM complaints WHERE id=?', [req.params.id]);
        if (comp && comp.email) {
            sendResolutionEmail({
                email: comp.email,
                complaint_no: comp.complaint_no,
                customer_name: comp.customer_name,
                complainee_name: comp.complainee_name,
                telephone_number: comp.telephone_number,
                std_code: comp.std_code,
                company_name: comp.customer_name
            }).catch(err => console.error('Resolution mail error:', err.message));
        }

        res.json({ message: 'Complaint resolved successfully by ' + resolverName });
    } catch (err) {
        console.error('Resolve on behalf error:', err);
        res.status(500).json({ error: 'Failed to resolve complaint' });
    }
});

// ══════════════════════════════════════════════════════════════════════════

app.post('/api/complaints', async (req, res) => {
    const { customer_name, complainee_name, mobile, email, issue_type, description, std_code, telephone_number,
            circle: circleFromBody, oa_name: oaFromBody } = req.body;
    try {
        // Check for existing active complaint for this telephone number
        const [existing] = await pool.query(
            `SELECT id FROM complaints
             WHERE telephone_number = ? AND std_code = ?
             AND status NOT IN ('Resolved', 'Cancelled')
             ORDER BY created_at DESC LIMIT 1`,
            [telephone_number, std_code]
        );
        if (existing.length > 0) {
            const existingTicketId = `CRM-${String(existing[0].id).padStart(4, '0')}`;
            return res.status(409).json({
                duplicate: true,
                ticket_id: existingTicketId,
                complaint_no: existing[0].complaint_no,
                message: 'Your complaint is already registered with us.'
            });
        }

        // Lookup circle and OA from customers table (try multiple formats)
        // Use values from request body as fallback if customer record not found
        let circle = circleFromBody || '', oa_name = oaFromBody || '', company_name = '';
        try {
            const fullNumber = `${std_code}-${telephone_number}`;
            const [custRows] = await pool.query(
                `SELECT c.circle, c.oa_name, c.customer_name as company_name FROM customers c
                 WHERE (c.telephone_number=? AND c.telephone_code=?)
                 OR c.id IN (SELECT customer_id FROM customer_lines WHERE telephone_number = ? OR telephone_number = ?)
                 LIMIT 1`,
                [telephone_number, std_code, fullNumber, `${std_code}${telephone_number}`]
            );
            if (custRows.length > 0) {
                circle = custRows[0].circle || circle;
                oa_name = custRows[0].oa_name || oa_name;
                company_name = custRows[0].company_name || '';
            }
        } catch (e) { /* ignore */ }

        const complaint_no = await generateComplaintNo(circle, oa_name);
        // Auto-assign engineer (Customer > OA > Circle priority)
        const autoEngineerId = await autoAssignEngineer(circle, oa_name, telephone_number, std_code);

        // Fetch customer's default complaint priority
        let autoPriority = 'Low';
        try {
            const [[custPriRow]] = await pool.query(
                `SELECT default_complaint_priority FROM customers
                 WHERE (telephone_number=? AND telephone_code=?)
                 OR id IN (SELECT customer_id FROM customer_lines WHERE telephone_number=? OR telephone_number=?)
                 LIMIT 1`,
                [telephone_number, std_code, telephone_number, `${std_code}-${telephone_number}`]
            ).catch(() => [[null]]);
            if (custPriRow && custPriRow.default_complaint_priority) {
                autoPriority = custPriRow.default_complaint_priority;
            }
        } catch(e) {}

        const [result] = await pool.query(
            'INSERT INTO complaints (complaint_no, customer_name, complainee_name, mobile, email, issue_type, description, std_code, telephone_number, circle, oa_name, assigner_comments, assigned_to, priority) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [complaint_no, customer_name, complainee_name || customer_name, mobile, email, issue_type || 'Auto-Support', description, std_code, telephone_number, circle, oa_name, 'System Auto Assign', autoEngineerId, autoPriority]
        );

        const ticketId = `CRM-${String(result.insertId).padStart(4, '0')}`;
        const complaintData = {
            ticket_id: ticketId,
            complaint_no,
            customer_name,
            complainee_name: complainee_name || customer_name,
            company_name: company_name || customer_name,
            mobile,
            email,
            issue_type,
            description,
            std_code,
            telephone_number
        };

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

// --- NOTIFICATION HELPERS ---
async function sendEmailNotification(data) {
    if (!data.email || !data.email.trim()) return;
    const telephone = data.std_code ? `${data.std_code}-${data.telephone_number}` : (data.telephone_number || '');
    await supportMailTransporter.sendMail({
        from: '"BSNL EPABX SUPPORT" <bsnlpbxhelp@gmail.com>',
        to: data.email.trim(),
        subject: `Complaint Registration Confirmation — Ticket ${data.complaint_no}`,
        html: `<div style="font-family:Arial,sans-serif;max-width:620px;padding:20px;color:#222;">
            <h2 style="color:#002d72;margin-bottom:4px;">Complaint Registration Confirmation</h2>
            <p style="color:#555;margin:0 0 18px;">Ticket: <strong>${data.complaint_no}</strong></p>
            <p>Dear ${data.complainee_name},</p>
            <p>Thank you for reaching out to us. We would like to inform you that the complaint registered under <strong>${data.company_name}</strong> for telephone number <strong>${telephone}</strong> has been successfully received and is now being processed by our support team.</p>
            <div style="background:#eff6ff;border-left:4px solid #002d72;padding:12px 18px;margin:18px 0;border-radius:4px;">
                <strong style="color:#002d72;">Your Complaint Ticket Number: ${data.complaint_no}</strong>
            </div>
            <p>Please keep this ticket number for future reference. Our team will review your complaint and get back to you at the earliest. You may also use this ticket number to track the status of your complaint on our website.</p>
            <p>If you have any further queries, please do not hesitate to contact our support team.</p>
            <br>
            <p style="margin:0;">Warm regards,<br><strong>Customer Support Team</strong></p>
            <hr style="border:none;border-top:1px solid #e2e8f0;margin:18px 0;">
            <p style="font-size:0.78rem;color:#94a3b8;">This is an auto-generated email. Please do not reply directly to this message.</p>
        </div>`
    });
    console.log(`[SUPPORT MAIL] Sent confirmation to ${data.email} — Ticket: ${data.complaint_no}`);
}

async function sendWhatsAppNotification(data) {
    console.log(`[NOTIFY] Sending WhatsApp to ${data.mobile} with Ticket ID: ${data.ticket_id}`);
    // TODO: Implement WhatsApp API logic when credentials provided
}

// Send resolution email to customer when complaint is resolved/verified
async function sendResolutionEmail(data) {
    if (!data.email || !data.email.trim()) return;
    const telephone = data.std_code ? `${data.std_code}-${data.telephone_number}` : (data.telephone_number || '');
    const delaySection = (data.verif_remarks && data.verif_remarks.trim()) ? `
        <div style="background:#fefce8;border-left:4px solid #f59e0b;padding:12px 18px;margin:18px 0;border-radius:4px;">
            <strong style="color:#92400e;">Resolution Note from Support Team:</strong>
            <p style="margin:8px 0 0;color:#78350f;">${data.verif_remarks}</p>
        </div>` : '';
    await supportMailTransporter.sendMail({
        from: '"BSNL EPABX SUPPORT" <bsnlpbxhelp@gmail.com>',
        to: data.email.trim(),
        subject: `Complaint Resolved — Ticket ${data.complaint_no}`,
        html: `<div style="font-family:Arial,sans-serif;max-width:620px;padding:20px;color:#222;">
            <h2 style="color:#16a34a;margin-bottom:4px;">Your Complaint Has Been Resolved</h2>
            <p style="color:#555;margin:0 0 18px;">Ticket: <strong>${data.complaint_no}</strong></p>
            <p>Dear ${data.complainee_name || data.customer_name},</p>
            <p>We are pleased to inform you that your complaint registered under <strong>${data.company_name || data.customer_name}</strong> for telephone number <strong>${telephone}</strong> has been successfully resolved.</p>
            <div style="background:#f0fdf4;border-left:4px solid #16a34a;padding:12px 18px;margin:18px 0;border-radius:4px;">
                <strong style="color:#16a34a;">Ticket Number: ${data.complaint_no} — Resolved ✅</strong>
            </div>
            ${delaySection}
            <p>If you face any further issues, please feel free to register a new complaint or contact our support team. Thank you for your patience and cooperation.</p>
            <br>
            <p style="margin:0;">Warm regards,<br><strong>Customer Support Team</strong></p>
            <hr style="border:none;border-top:1px solid #e2e8f0;margin:18px 0;">
            <p style="font-size:0.78rem;color:#94a3b8;">This is an auto-generated email. Please do not reply directly to this message.</p>
        </div>`
    });
    console.log(`[RESOLUTION MAIL] Sent to ${data.email} — Ticket: ${data.complaint_no}`);
}

// --- CUSTOMERS ROUTES ---
app.get('/api/customers', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query(`
            SELECT c.*, u.id as assigned_engineer_id, u.name as assigned_engineer_name, u.username as assigned_engineer_username
            FROM customers c
            LEFT JOIN users u ON c.assigned_engineer_id = u.id
            ORDER BY c.customer_name ASC
        `);
        res.json(rows);
    } catch (err) {
        console.error('Fetch customers error:', err);
        res.status(500).json({ error: 'Failed to fetch customers' });
    }
});

// Bulk assign engineer to multiple customers
app.post('/api/customers/bulk-assign', authenticateToken, isAdmin, async (req, res) => {
    const { engineer_id, customer_ids } = req.body;
    if (!engineer_id || !Array.isArray(customer_ids) || !customer_ids.length) {
        return res.status(400).json({ error: 'engineer_id and customer_ids[] required' });
    }
    try {
        const placeholders = customer_ids.map(() => '?').join(',');
        const [result] = await pool.query(
            `UPDATE customers SET assigned_engineer_id=? WHERE id IN (${placeholders})`,
            [engineer_id, ...customer_ids]
        );
        res.json({ message: `${result.affectedRows} customers assigned`, count: result.affectedRows });
    } catch (err) {
        console.error('Bulk assign error:', err);
        res.status(500).json({ error: 'Bulk assign failed' });
    }
});

// Assign engineer to customer
app.put('/api/customers/:id/assign-engineer', authenticateToken, async (req, res) => {
    const { engineer_id } = req.body;
    try {
        // Add column if not exists
        try { await pool.query('ALTER TABLE customers ADD COLUMN assigned_engineer_id INT DEFAULT NULL'); } catch(e) {}
        await pool.query('UPDATE customers SET assigned_engineer_id=? WHERE id=?', [engineer_id, req.params.id]);
        res.json({ message: 'Engineer assigned' });
    } catch (err) {
        console.error('Assign engineer error:', err);
        res.status(500).json({ error: 'Failed to assign' });
    }
});

// ── AREA ENGINEER MAPPING APIs ────────────────────────────────────

// GET all circle mappings
app.get('/api/area-assignment/circles', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query(
            `SELECT cem.*, u.name as engineer_name, u.username
             FROM circle_engineer_mapping cem
             LEFT JOIN users u ON cem.engineer_id = u.id
             ORDER BY cem.circle`
        );
        res.json(rows);
    } catch(e) { res.status(500).json({ error: 'Failed' }); }
});

// Save circle → engineer mapping
app.post('/api/area-assignment/circles', authenticateToken, async (req, res) => {
    const { circle, engineer_id } = req.body;
    try {
        await pool.query(
            `INSERT INTO circle_engineer_mapping (circle, engineer_id)
             VALUES (?, ?) ON DUPLICATE KEY UPDATE engineer_id=?`,
            [circle, engineer_id, engineer_id]
        );
        res.json({ message: 'Circle mapping saved' });
    } catch(e) { res.status(500).json({ error: 'Failed' }); }
});

app.delete('/api/area-assignment/circles/:circle', authenticateToken, async (req, res) => {
    try {
        await pool.query('DELETE FROM circle_engineer_mapping WHERE circle=?', [decodeURIComponent(req.params.circle)]);
        res.json({ message: 'Deleted' });
    } catch(e) { res.status(500).json({ error: 'Failed' }); }
});

// GET all OA mappings
app.get('/api/area-assignment/oas', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query(
            `SELECT oem.*, u.name as engineer_name, u.username
             FROM oa_engineer_mapping oem
             LEFT JOIN users u ON oem.engineer_id = u.id
             ORDER BY oem.circle, oem.oa_name`
        );
        res.json(rows);
    } catch(e) { res.status(500).json({ error: 'Failed' }); }
});

// Save OA → engineer mapping
app.post('/api/area-assignment/oas', authenticateToken, async (req, res) => {
    const { circle, oa_name, engineer_id } = req.body;
    try {
        await pool.query(
            `INSERT INTO oa_engineer_mapping (circle, oa_name, engineer_id)
             VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE engineer_id=?`,
            [circle, oa_name, engineer_id, engineer_id]
        );
        res.json({ message: 'OA mapping saved' });
    } catch(e) { res.status(500).json({ error: 'Failed' }); }
});

app.delete('/api/area-assignment/oas/:id', authenticateToken, async (req, res) => {
    try {
        await pool.query('DELETE FROM oa_engineer_mapping WHERE id=?', [req.params.id]);
        res.json({ message: 'Deleted' });
    } catch(e) { res.status(500).json({ error: 'Failed' }); }
});

// TEMPORARY REASSIGNMENT: Engineer on leave → reassign all their area to another engineer
app.post('/api/area-assignment/temp-reassign', authenticateToken, async (req, res) => {
    const { from_engineer_id, to_engineer_id, scope } = req.body;
    // scope: 'all' (all active complaints) or 'area' (update mappings temporarily)
    try {
        let updated = 0;
        if (scope === 'complaints') {
            // Reassign all active complaints currently assigned to from_engineer
            const [result] = await pool.query(
                `UPDATE complaints SET assigned_to=?
                 WHERE assigned_to=? AND status NOT IN ('Resolved','Cancelled')`,
                [to_engineer_id, from_engineer_id]
            );
            updated = result.affectedRows;
        } else if (scope === 'area') {
            // Update circle & OA mappings
            await pool.query('UPDATE circle_engineer_mapping SET engineer_id=? WHERE engineer_id=?', [to_engineer_id, from_engineer_id]);
            await pool.query('UPDATE oa_engineer_mapping SET engineer_id=? WHERE engineer_id=?', [to_engineer_id, from_engineer_id]);
            await pool.query('UPDATE customers SET assigned_engineer_id=? WHERE assigned_engineer_id=?', [to_engineer_id, from_engineer_id]);
            updated = 1;
        }
        res.json({ message: 'Reassigned successfully', updated });
    } catch(e) { res.status(500).json({ error: 'Failed' }); }
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
            const fullNumber = `${std_code}-${telephone_number}`;
            query += ' OR (telephone_code = ? AND telephone_number = ?) OR id IN (SELECT customer_id FROM customer_lines WHERE (telephone_code = ? AND telephone_number = ?) OR telephone_number = ? OR telephone_number = ?)';
            params.push(std_code, telephone_number, std_code, telephone_number, fullNumber, `${std_code}${telephone_number}`);
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

        // Fetch product/order history for this customer
        const [orders] = await pool.query('SELECT * FROM customer_orders WHERE customer_id = ? ORDER BY created_at ASC', [customer.id]);
        customer.orders = orders;

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
                revenue_level = ?, epabx_model = ?, product_start_date = ?,
                default_complaint_priority = ?
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
                data.default_complaint_priority || 'Low',
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

// ── BILLING ENDPOINTS ────────────────────────────────────────────────────────

// ── BILLING: distinct circles with active lines (lightweight) ─────────────────
app.get('/api/bills/circles', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query(`
            SELECT DISTINCT c.circle
            FROM customers c
            WHERE c.circle IS NOT NULL AND c.circle != ''
              AND EXISTS (
                SELECT 1 FROM customer_lines cl
                WHERE cl.customer_id = c.id
                  AND (cl.is_closed IS NULL OR cl.is_closed = '' OR UPPER(cl.is_closed) NOT IN ('YES','CLOSED'))
              )
            ORDER BY c.circle
        `);
        res.json(rows.map(r => r.circle));
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ── BILLING: distinct OAs for a circle (lightweight) ──────────────────────────
app.get('/api/bills/oas', authenticateToken, async (req, res) => {
    try {
        const { circle } = req.query;
        const params = [];
        let extra = '';
        if (circle) { extra = ' AND c.circle = ?'; params.push(circle); }
        const [rows] = await pool.query(`
            SELECT DISTINCT c.oa_name
            FROM customers c
            WHERE c.oa_name IS NOT NULL AND c.oa_name != ''
              AND EXISTS (
                SELECT 1 FROM customer_lines cl
                WHERE cl.customer_id = c.id
                  AND (cl.is_closed IS NULL OR cl.is_closed = '' OR UPPER(cl.is_closed) NOT IN ('YES','CLOSED'))
              )${extra}
            ORDER BY c.oa_name
        `, params);
        res.json(rows.map(r => r.oa_name));
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ── BILLING: customers with active lines (server-side filter + pagination) ────
app.get('/api/bills/active-customers', authenticateToken, async (req, res) => {
    try {
        const { circle, oa, search, no_email } = req.query;
        const limitN  = Math.min(Math.max(parseInt(req.query.limit)  || 100, 1), 5000);
        const offsetN = Math.max(parseInt(req.query.offset) || 0, 0);

        const baseWhere = `EXISTS (
            SELECT 1 FROM customer_lines cl
            WHERE cl.customer_id = c.id
              AND (cl.is_closed IS NULL OR cl.is_closed = '' OR UPPER(cl.is_closed) NOT IN ('YES','CLOSED'))
        )`;
        const conditions = [baseWhere];
        const params = [];

        if (circle)   { conditions.push('c.circle = ?');                          params.push(circle); }
        if (oa)       { conditions.push('c.oa_name = ?');                         params.push(oa); }
        if (search)   { conditions.push('c.customer_name LIKE ?');                params.push(`%${search}%`); }
        if (no_email === '1') {
            conditions.push('(c.acc_person_email IS NULL OR c.acc_person_email = \'\')');
        }

        const where = 'WHERE ' + conditions.join(' AND ');

        const [[{ total }]] = await pool.query(
            `SELECT COUNT(DISTINCT c.id) AS total FROM customers c ${where}`, params
        );

        const [rows] = await pool.query(`
            SELECT DISTINCT
                c.id, c.circle, c.oa_name,
                c.customer_name, c.customer_code,
                c.acc_person_email, c.contact_person,
                (SELECT GROUP_CONCAT(
                    cl2.telephone_number
                    ORDER BY cl2.id SEPARATOR '\n'
                 ) FROM customer_lines cl2
                 WHERE cl2.customer_id = c.id
                   AND (cl2.is_closed IS NULL OR cl2.is_closed = '' OR UPPER(cl2.is_closed) NOT IN ('YES','CLOSED'))
                ) AS active_lines
            FROM customers c ${where}
            ORDER BY c.circle, c.oa_name, c.customer_name
            LIMIT ? OFFSET ?
        `, [...params, limitN, offsetN]);

        res.json({ total, rows, limit: limitN, offset: offsetN });
    } catch (err) {
        console.error('Billing active customers error:', err);
        res.status(500).json({ error: 'Failed to fetch billing customers' });
    }
});

// Send bill emails (with optional per-customer PDF attachments)
// Send bill emails — accepts JSON body with base64-encoded PDFs (Vercel-compatible, no multer)
app.post('/api/bills/send-email', authenticateToken, async (req, res) => {
    try {
        const { billing_month, billing_year, subject, message, customer_ids, pdfs } = req.body;
        // DEBUG: log what we received
        console.log('[send-email] body keys:', Object.keys(req.body || {}));
        console.log('[send-email] customer_ids:', JSON.stringify(customer_ids), 'type:', typeof customer_ids, 'isArray:', Array.isArray(customer_ids));
        // customer_ids: array of ints
        // pdfs: { "<custId>": { name: "filename.pdf", data: "<base64>" }, ... }
        const customerIds = Array.isArray(customer_ids) ? customer_ids : JSON.parse(customer_ids || '[]');
        if (!customerIds.length) return res.status(400).json({ error: 'No customers specified' });

        const [customers] = await pool.query(
            'SELECT id, customer_name, customer_code, acc_person_email FROM customers WHERE id IN (?)',
            [customerIds]
        );
        const withEmail = customers.filter(c => c.acc_person_email && c.acc_person_email.trim());
        if (!withEmail.length) return res.status(400).json({ error: 'None of the selected customers have an email address' });

        const pdfMap = pdfs || {};  // { "123": { name: "bill.pdf", data: "<base64>" } }

        const billingPeriod = (billing_month && billing_year)
            ? `${billing_month} ${billing_year}` : 'Current Period';
        const emailSubject = subject || `BSNL PBX Bill — ${billingPeriod}`;

        // Send all emails in PARALLEL
        const results = await Promise.allSettled(withEmail.map(cust => {
            const attachments = [];
            const pdfEntry = pdfMap[String(cust.id)];
            if (pdfEntry && pdfEntry.data) {
                attachments.push({
                    filename: pdfEntry.name || `bill_${cust.customer_code}_${billingPeriod}.pdf`,
                    content: Buffer.from(pdfEntry.data, 'base64'),
                    contentType: 'application/pdf'
                });
            }
            return billMailTransporter.sendMail({
                from: '"BSNL PBX BILL" <bsnlpribill@gmail.com>',
                to: cust.acc_person_email.trim(),
                subject: emailSubject,
                html: `<div style="font-family:Arial,sans-serif;max-width:600px;padding:20px;">
                    <h2 style="color:#002d72;margin-bottom:6px;">BSNL PBX Bill</h2>
                    <p style="color:#555;margin:0 0 16px;">Billing Period: <strong>${billingPeriod}</strong></p>
                    <p>Dear ${cust.customer_name},</p>
                    <p>Please find your BSNL PBX bill for <strong>${billingPeriod}</strong>.${
                        attachments.length ? ' The bill is attached as a PDF.' : ''
                    }</p>
                    ${message ? `<p style="white-space:pre-line;">${message}</p>` : ''}
                    <hr style="border:none;border-top:1px solid #e2e8f0;margin:20px 0;">
                    <p style="font-size:0.82em;color:#94a3b8;">Customer Code: ${cust.customer_code || '—'}</p>
                    <p style="font-size:0.82em;color:#94a3b8;">This is an automated email. Please do not reply.</p>
                </div>`,
                attachments
            }).catch(e => {
                console.error(`Bill email failed for ${cust.acc_person_email}:`, e.message);
                throw e;
            });
        }));

        const sent   = results.filter(r => r.status === 'fulfilled').length;
        const failed = results.filter(r => r.status === 'rejected').length;

        // Log successful sends to bill_email_log
        for (let i = 0; i < results.length; i++) {
            if (results[i].status === 'fulfilled') {
                try {
                    await pool.query(
                        `INSERT INTO bill_email_log (customer_id, billing_month, billing_year, email_to, sent_by, source) VALUES (?, ?, ?, ?, ?, 'individual')`,
                        [withEmail[i].id, billing_month || '', billing_year || '', withEmail[i].acc_person_email.trim(), req.user.id]
                    );
                } catch(logErr) { console.error('Email log insert error:', logErr.message); }
            }
        }

        res.json({ sent, failed, total: withEmail.length });
    } catch (err) {
        console.error('Send bill email error:', err);
        res.status(500).json({ error: 'Email send failed: ' + err.message });
    }
});

// --- Bill Email Sent Status API ---
app.get('/api/bills/email-sent-status', authenticateToken, async (req, res) => {
    try {
        const { billing_month, billing_year, customer_ids } = req.query;
        if (!billing_month || !billing_year) return res.json({ sent_map: {} });

        let custIds = [];
        if (customer_ids) {
            try { custIds = JSON.parse(customer_ids); } catch(e) { custIds = []; }
        }
        if (!custIds.length) return res.json({ sent_map: {} });

        const [rows] = await pool.query(
            `SELECT customer_id, MAX(sent_at) as last_sent
             FROM bill_email_log
             WHERE billing_month = ? AND billing_year = ? AND customer_id IN (?)
             GROUP BY customer_id`,
            [billing_month, billing_year, custIds]
        );

        const sent_map = {};
        rows.forEach(r => {
            sent_map[r.customer_id] = r.last_sent;
        });
        res.json({ sent_map });
    } catch(err) {
        console.error('Email sent status error:', err);
        res.status(500).json({ error: err.message });
    }
});

// --- One-time migration: backfill bill_email_log from bill_pdfs ---
app.post('/api/bills/migrate-email-log', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const { billing_month, billing_year } = req.body;
        if (!billing_month || !billing_year) return res.status(400).json({ error: 'billing_month and billing_year required' });

        const [rows] = await pool.query(
            `SELECT customer_id, email, created_at FROM bill_pdfs WHERE email_sent = true AND customer_id IS NOT NULL AND email IS NOT NULL AND email != ''`
        );
        let inserted = 0;
        for (const r of rows) {
            // Check if already exists
            const [[existing]] = await pool.query(
                `SELECT id FROM bill_email_log WHERE customer_id = ? AND billing_month = ? AND billing_year = ? LIMIT 1`,
                [r.customer_id, billing_month, billing_year]
            );
            if (!existing) {
                await pool.query(
                    `INSERT INTO bill_email_log (customer_id, billing_month, billing_year, email_to, sent_at, sent_by, source) VALUES (?, ?, ?, ?, ?, ?, 'bulk')`,
                    [r.customer_id, billing_month, billing_year, r.email, r.created_at, req.user.id]
                );
                inserted++;
            }
        }
        res.json({ migrated: inserted, total_found: rows.length });
    } catch(err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/dashboard/stats', authenticateToken, async (req, res) => {
    try {
        const user = req.user;
        let cWhere = "WHERE status = 'Pending'";
        let cParams = [];
        let custWhere = '';
        let custParams = [];

        if (user.role !== 'admin') {
            const [[meRow]] = await pool.query('SELECT id, allowed_circles, allowed_oas FROM users WHERE id = ?', [user.id]);
            let myCircles = [];
            try { myCircles = JSON.parse(meRow?.allowed_circles || '[]'); } catch(e) {}

            const [subs] = await pool.query('SELECT user_id FROM user_managers WHERE manager_id = ?', [user.id]);
            const allIds = [user.id, ...subs.map(s => s.user_id)];
            const idPH = allIds.map(() => '?').join(',');

            if (myCircles.length > 0) {
                const cirPH = myCircles.map(() => '?').join(',');
                cWhere += ` AND (assigned_to IN (${idPH}) OR (assigned_to IS NULL AND circle IN (${cirPH})))`;
                cParams = [...allIds, ...myCircles];
                custWhere = `WHERE circle IN (${cirPH})`;
                custParams = [...myCircles];
            } else {
                cWhere += ` AND assigned_to IN (${idPH})`;
                cParams = [...allIds];
            }
        }

        const [[{ count: pendingComplaints }]] = await pool.query(
            `SELECT COUNT(*) as count FROM complaints ${cWhere}`, cParams
        );
        const [[{ count: totalCustomers }]] = await pool.query(
            `SELECT COUNT(*) as count FROM customers ${custWhere}`, custParams
        );

        res.json({ pendingComplaints, totalCustomers });
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
            const [userData] = await pool.query('SELECT allowed_circle, allowed_oa, allowed_circles, allowed_oas, allowed_customers FROM users WHERE id = ?', [user.id]);
            const u = userData[0];

            // Multi-circle filter (new) — falls back to old single-value if new is empty
            const parseArr = v => { try { const a = JSON.parse(v); return Array.isArray(a) ? a.filter(Boolean) : []; } catch(e) { return []; } };
            const circles = parseArr(u.allowed_circles);
            const oas     = parseArr(u.allowed_oas);
            if (circles.length > 0) {
                whereClause += ` AND circle IN (${circles.map(() => '?').join(',')})`;
                params.push(...circles);
            } else if (u.allowed_circle) {
                whereClause += ' AND circle = ?';
                params.push(u.allowed_circle);
            }
            if (oas.length > 0) {
                whereClause += ` AND oa_name IN (${oas.map(() => '?').join(',')})`;
                params.push(...oas);
            } else if (u.allowed_oa) {
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

        // Pending Complaints — area filtered for non-admin
        let complaintWhere = "WHERE status = 'Pending'";
        let complaintParams = [];
        if (user.role !== 'admin') {
            const parseArr2 = v => { try { const a = JSON.parse(v); return Array.isArray(a) ? a.filter(Boolean) : []; } catch(e) { return []; } };
            const [uData2] = await pool.query('SELECT allowed_circles FROM users WHERE id = ?', [user.id]);
            const cCircles = parseArr2(uData2[0]?.allowed_circles);
            // Get subordinates
            const [subs2] = await pool.query('SELECT user_id FROM user_managers WHERE manager_id = ?', [user.id]);
            const subIds2 = subs2.map(s => s.user_id);
            const allIds2 = [user.id, ...subIds2];
            const idPH2 = allIds2.map(() => '?').join(',');
            if (cCircles.length > 0) {
                const cirPH2 = cCircles.map(() => '?').join(',');
                complaintWhere += ` AND (assigned_to IN (${idPH2}) OR (assigned_to IS NULL AND circle IN (${cirPH2})))`;
                complaintParams = [...allIds2, ...cCircles];
            } else {
                complaintWhere += ` AND assigned_to IN (${idPH2})`;
                complaintParams = [...allIds2];
            }
        }
        const [pendingComplaints] = await pool.query(
            `SELECT COUNT(*) as count FROM complaints ${complaintWhere}`, complaintParams
        );

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

// --- DASHBOARD V2 STATS (SLA-aware) ---
// Working hours: Mon-Sat 10:00-18:30, SLA = 4 working hours
app.get('/api/dashboard/v2-stats', authenticateToken, async (req, res) => {
    try {
        // Helper: calculate working minutes between two dates
        // Working day = Mon-Sat, hours = 10:00-18:30 (510 min/day)
        function workingMinutesBetween(start, end) {
            if (end <= start) return 0;
            let mins = 0;
            const cur = new Date(start);
            const WORK_START_H = 10, WORK_START_M = 0;
            const WORK_END_H = 18, WORK_END_M = 30;
            const DAY_MINS = (WORK_END_H * 60 + WORK_END_M) - (WORK_START_H * 60 + WORK_START_M); // 510

            // Cap to 60 days max to prevent infinite loop
            const maxEnd = new Date(start.getTime() + 60 * 24 * 60 * 60 * 1000);
            const actualEnd = end < maxEnd ? end : maxEnd;

            while (cur < actualEnd) {
                const dow = cur.getDay(); // 0=Sun
                if (dow >= 1 && dow <= 6) { // Mon-Sat
                    const dayStart = new Date(cur); dayStart.setHours(WORK_START_H, WORK_START_M, 0, 0);
                    const dayEnd = new Date(cur); dayEnd.setHours(WORK_END_H, WORK_END_M, 0, 0);

                    const effStart = cur > dayStart ? cur : dayStart;
                    const effEnd = actualEnd < dayEnd ? actualEnd : dayEnd;

                    if (effStart < effEnd) {
                        mins += (effEnd - effStart) / 60000;
                    }
                }
                // Jump to next day 10:00
                cur.setDate(cur.getDate() + 1);
                cur.setHours(WORK_START_H, WORK_START_M, 0, 0);
            }
            return Math.round(mins);
        }

        const SLA_MINUTES = 4 * 60; // 240 minutes = 4 working hours
        const now = new Date();
        const user = req.user;

        // ── Area-based access control ─────────────────────────────
        let areaWhere = '';
        let areaParams = [];
        let teamWhere = '';
        let teamParams = [];

        if (user.role !== 'admin') {
            // Get user's allowed circles/oas from DB
            const [[meRow]] = await pool.query(
                'SELECT id, allowed_circles, allowed_oas FROM users WHERE id = ?', [user.id]
            );
            const myId = meRow?.id || user.id;

            let myCircles = [];
            let myOas = [];
            try { myCircles = JSON.parse(meRow?.allowed_circles || '[]'); } catch(e) {}
            try { myOas = JSON.parse(meRow?.allowed_oas || '[]'); } catch(e) {}

            // Get subordinates
            const [subs] = await pool.query(
                'SELECT user_id FROM user_managers WHERE manager_id = ?', [myId]
            );
            const subIds = subs.map(s => s.user_id);
            const allIds = [myId, ...subIds];

            // Complaints filter: assigned to self/subordinates OR unassigned in user's circles
            const idPH = allIds.map(() => '?').join(',');
            if (myCircles.length > 0) {
                const cirPH = myCircles.map(() => '?').join(',');
                areaWhere = `WHERE (c.assigned_to IN (${idPH}) OR (c.assigned_to IS NULL AND c.circle IN (${cirPH})))`;
                areaParams = [...allIds, ...myCircles];
            } else {
                areaWhere = `WHERE c.assigned_to IN (${idPH})`;
                areaParams = [...allIds];
            }

            // Team filter: only show self + subordinates in team chart
            teamWhere = `AND c.assigned_to IN (${idPH})`;
            teamParams = [...allIds];
        }
        // ──────────────────────────────────────────────────────────

        // Complaints filtered by user's area
        const [allComplaints] = await pool.query(
            `SELECT c.id, c.status, c.priority, c.created_at, c.updated_at, c.assigned_to
             FROM complaints c ${areaWhere} ORDER BY c.created_at DESC`,
            areaParams
        );

        // Calculate stats
        let totalActive = 0, totalResolved = 0, totalOverdue = 0, totalEscalated = 0;
        let statusCounts = { Pending: 0, 'In Progress': 0, Resolved: 0, Closed: 0, Escalated: 0 };
        let priorityCounts = { High: 0, Medium: 0, Low: 0 };

        allComplaints.forEach(c => {
            const st = c.status || 'Pending';
            statusCounts[st] = (statusCounts[st] || 0) + 1;
            priorityCounts[c.priority] = (priorityCounts[c.priority] || 0) + 1;

            const isActive = (st === 'Pending' || st === 'In Progress');
            if (isActive) totalActive++;
            if (st === 'Resolved' || st === 'Closed') totalResolved++;
            if (st === 'Escalated') totalEscalated++;

            // Check SLA overdue for active complaints
            if (isActive && c.created_at) {
                const createdAt = new Date(c.created_at);
                const wMins = workingMinutesBetween(createdAt, now);
                if (wMins > SLA_MINUTES) totalOverdue++;
            }
        });

        // 30-day trend: total complaints and overdue per day
        const trend30 = [];
        for (let i = 29; i >= 0; i--) {
            const d = new Date(); d.setDate(d.getDate() - i); d.setHours(0,0,0,0);
            const dEnd = new Date(d); dEnd.setHours(23,59,59,999);
            const dateStr = d.toISOString().split('T')[0];

            let dayTotal = 0, dayOverdue = 0;
            allComplaints.forEach(c => {
                const cDate = new Date(c.created_at);
                if (cDate >= d && cDate <= dEnd) {
                    dayTotal++;
                    const resolvedDate = (c.status === 'Resolved' || c.status === 'Closed') ? new Date(c.updated_at) : now;
                    const wMins = workingMinutesBetween(cDate, resolvedDate);
                    if (wMins > SLA_MINUTES) dayOverdue++;
                }
            });

            trend30.push({ date: dateStr, total: dayTotal, overdue: dayOverdue });
        }

        // Team pending work — filtered by area for non-admin
        const [teamData] = await pool.query(`
            SELECT c.assigned_to, u.name as assignee_name,
                   SUM(CASE WHEN c.status IN ('Pending','In Progress') THEN 1 ELSE 0 END) as active,
                   SUM(CASE WHEN c.status IN ('Resolved','Closed') THEN 1 ELSE 0 END) as resolved,
                   SUM(CASE WHEN c.status = 'Escalated' THEN 1 ELSE 0 END) as escalated
            FROM complaints c
            LEFT JOIN users u ON c.assigned_to = u.id
            WHERE c.assigned_to IS NOT NULL ${teamWhere}
            GROUP BY c.assigned_to, u.name
            ORDER BY active DESC
        `, teamParams);

        // Tasks summary — filtered by user for non-admin
        let taskQuery, taskParams2 = [];
        if (user.role === 'admin') {
            taskQuery = `SELECT COUNT(*) as total,
                SUM(CASE WHEN status != 'COMPLETED' THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN status != 'COMPLETED' AND due_date < CURDATE() THEN 1 ELSE 0 END) as overdue
                FROM tasks`;
        } else {
            taskQuery = `SELECT COUNT(*) as total,
                SUM(CASE WHEN status != 'COMPLETED' THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN status != 'COMPLETED' AND due_date < CURDATE() THEN 1 ELSE 0 END) as overdue
                FROM tasks WHERE assigned_to = ?`;
            taskParams2 = [user.id];
        }
        const [tasksSummary] = await pool.query(taskQuery, taskParams2);

        res.json({
            complaints: {
                total: allComplaints.length,
                active: totalActive,
                resolved: totalResolved,
                overdue: totalOverdue,
                escalated: totalEscalated,
                statusCounts,
                priorityCounts
            },
            trend30,
            teamComplaints: teamData,
            tasks: tasksSummary[0] || { total: 0, active: 0, overdue: 0 },
            sla: { minutes: SLA_MINUTES, workingHours: '10:00-18:30', workingDays: 'Mon-Sat' }
        });
    } catch (err) {
        console.error('Dashboard v2 stats error:', err);
        res.status(500).json({ error: 'Failed to fetch dashboard stats' });
    }
});

// ═══ MODULE DASHBOARD STATS (for dashboard graph boxes) ═══

// --- Work Log Pro Dashboard Stats ---
app.get('/api/diary/dashboard-stats', authenticateToken, async (req, res) => {
    try {
        const today = new Date().toISOString().split('T')[0];
        const isAdmin = req.user.role === 'admin';

        // Query from TASKS table (main task system)
        let taskFilter = '';
        let taskParams = [];
        if (!isAdmin) {
            taskFilter = 'WHERE t.assigned_to = ?';
            taskParams = [req.user.username];
        }

        const [taskStats] = await pool.query(
            `SELECT u.name, u.id as user_id,
                COUNT(*) as total,
                SUM(CASE WHEN t.status != 'COMPLETED' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN t.status = 'COMPLETED' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN t.status != 'COMPLETED' AND t.due_date < ? THEN 1 ELSE 0 END) as overdue
             FROM tasks t
             LEFT JOIN users u ON t.assigned_to = u.username
             ${taskFilter}
             GROUP BY u.name, u.id
             ORDER BY pending DESC`,
            [today, ...taskParams]
        );

        // Also check diary_tasks table
        let diaryFilter = '';
        let diaryParams = [];
        if (!isAdmin) {
            diaryFilter = 'WHERE dt.user_id = ?';
            diaryParams = [req.user.id];
        }

        const [diaryStats] = await pool.query(
            `SELECT u.name, dt.user_id,
                SUM(CASE WHEN dt.status IN ('Pending','In Progress') THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN dt.status = 'Completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN dt.status IN ('Pending','In Progress') AND dt.due_date < ? THEN 1 ELSE 0 END) as overdue
             FROM diary_tasks dt
             LEFT JOIN users u ON dt.user_id = u.id
             ${diaryFilter}
             GROUP BY dt.user_id, u.name`,
            [today, ...diaryParams]
        );

        // Merge both sources
        const merged = {};
        taskStats.forEach(t => {
            const key = t.user_id || t.name;
            merged[key] = { name: t.name, user_id: t.user_id, pending: parseInt(t.pending)||0, completed: parseInt(t.completed)||0, overdue: parseInt(t.overdue)||0 };
        });
        diaryStats.forEach(d => {
            const key = d.user_id || d.name;
            if (merged[key]) {
                merged[key].pending += parseInt(d.pending)||0;
                merged[key].completed += parseInt(d.completed)||0;
                merged[key].overdue += parseInt(d.overdue)||0;
            } else {
                merged[key] = { name: d.name, user_id: d.user_id, pending: parseInt(d.pending)||0, completed: parseInt(d.completed)||0, overdue: parseInt(d.overdue)||0 };
            }
        });
        const userStats = Object.values(merged).sort((a,b) => b.pending - a.pending);

        res.json({ userStats, trend30: [] });
    } catch(e) {
        console.error('Diary dashboard stats error:', e);
        res.status(500).json({ error: e.message });
    }
});

// --- Work Log Pro Drill-down (uses tasks + diary_tasks) ---
app.get('/api/diary/dashboard-drilldown', authenticateToken, async (req, res) => {
    try {
        const today = new Date().toISOString().split('T')[0];
        const { user_id, category } = req.query;

        if (!user_id) return res.status(400).json({ error: 'user_id required' });

        // Get user info
        const [[userInfo]] = await pool.query('SELECT name, username FROM users WHERE id = ?', [user_id]);
        const userName = userInfo ? userInfo.name : 'Unknown';
        const username = userInfo ? userInfo.username : '';

        // Level 3: pending tasks list for a user
        if (category) {
            // From tasks table (main) - only pending
            const [mainTasks] = await pool.query(
                `SELECT id, title, status, 'Medium' as priority, 'Work' as task_type, 'General' as category, due_date,
                    CASE WHEN status != 'COMPLETED' AND due_date < ? THEN 1 ELSE 0 END as is_overdue
                 FROM tasks WHERE assigned_to = ? AND status != 'COMPLETED'
                 ORDER BY due_date ASC`,
                [today, username]
            );
            // From diary_tasks - only pending
            const [diaryTasks] = await pool.query(
                `SELECT id, title, status, priority, task_type, category, due_date,
                    CASE WHEN status IN ('Pending','In Progress') AND due_date < ? THEN 1 ELSE 0 END as is_overdue
                 FROM diary_tasks WHERE user_id = ? AND status IN ('Pending','In Progress')
                 ORDER BY due_date ASC`,
                [today, user_id]
            );
            var allTasks = [...mainTasks, ...diaryTasks];
            return res.json({ level: 3, userName, tasks: allTasks });
        }

        // Level 2: just show pending task count (no categories needed - go directly to task list)
        // From tasks table
        const [[taskCount]] = await pool.query(
            `SELECT COUNT(*) as pending,
                SUM(CASE WHEN due_date < ? THEN 1 ELSE 0 END) as overdue
             FROM tasks WHERE assigned_to = ? AND status != 'COMPLETED'`,
            [today, username]
        );
        // From diary_tasks
        const [[diaryCount]] = await pool.query(
            `SELECT COUNT(*) as pending,
                SUM(CASE WHEN due_date < ? THEN 1 ELSE 0 END) as overdue
             FROM diary_tasks WHERE user_id = ? AND status IN ('Pending','In Progress')`,
            [today, user_id]
        );
        const totalPending = (parseInt(taskCount.pending)||0) + (parseInt(diaryCount.pending)||0);
        const totalOverdue = (parseInt(taskCount.overdue)||0) + (parseInt(diaryCount.overdue)||0);

        res.json({ level: 2, userName, categories: [
            { category: 'All Pending Tasks', total: totalPending, pending: totalPending, overdue: totalOverdue }
        ]});
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- New Business Drill-down ---
app.get('/api/proposals/dashboard-drilldown', authenticateToken, async (req, res) => {
    try {
        const today = new Date().toISOString().split('T')[0];
        const { user_id, head } = req.query;

        if (!user_id) return res.status(400).json({ error: 'user_id required' });
        const [[prUserInfo]] = await pool.query('SELECT name FROM users WHERE id = ?', [user_id]);
        const prUserName = prUserInfo ? prUserInfo.name : 'Unknown';

        // Level 3: specific head's proposals
        if (head) {
            let statusFilter = '';
            if (head === 'overdue_followup') statusFilter = `AND p.next_followup_date IS NOT NULL AND p.next_followup_date < '${today}'`;
            else if (head === 'upcoming_followup') statusFilter = `AND p.next_followup_date IS NOT NULL AND p.next_followup_date >= '${today}'`;
            else if (head === 'direct_sale') statusFilter = 'AND p.direct_sale = 1';

            const [proposals] = await pool.query(
                `SELECT p.id, p.customer_name, p.customer_category, p.next_followup_date, p.direct_sale, p.created_at,
                    CASE WHEN p.next_followup_date IS NOT NULL AND p.next_followup_date < ? THEN 1 ELSE 0 END as is_overdue
                 FROM proposals p WHERE p.user_id = ? ${statusFilter}
                 ORDER BY p.created_at DESC`,
                [today, user_id]
            );
            return res.json({ level: 3, userName: prUserName, proposals });
        }

        // Level 2: head-wise breakdown for a user
        const [[stats]] = await pool.query(
            `SELECT COUNT(*) as total,
                SUM(CASE WHEN next_followup_date IS NOT NULL AND next_followup_date >= ? THEN 1 ELSE 0 END) as upcoming_followup,
                SUM(CASE WHEN next_followup_date IS NOT NULL AND next_followup_date < ? THEN 1 ELSE 0 END) as overdue_followup,
                SUM(CASE WHEN direct_sale = 1 THEN 1 ELSE 0 END) as direct_sale
             FROM proposals WHERE user_id = ?`,
            [today, today, user_id]
        );
        const [[userInfo]] = await pool.query('SELECT name FROM users WHERE id = ?', [user_id]);
        res.json({ level: 2, userName: userInfo ? userInfo.name : 'Unknown', heads: [
            { name: 'Total Proposals', key: 'all', count: stats.total },
            { name: 'Upcoming Followups', key: 'upcoming_followup', count: stats.upcoming_followup },
            { name: 'Overdue Followups', key: 'overdue_followup', count: stats.overdue_followup },
            { name: 'Direct Sales', key: 'direct_sale', count: stats.direct_sale }
        ]});
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- BSNL ERP Drill-down ---
app.get('/api/erp/dashboard-drilldown', authenticateToken, async (req, res) => {
    try {
        const { user_id, head } = req.query;

        if (!user_id) return res.status(400).json({ error: 'user_id required' });

        // Level 3: specific head's bill tasks
        if (head) {
            const [bills] = await pool.query(
                `SELECT bt.id, bt.bsnl_bill_no, bt.customer_name, bt.customer_ph_no, bt.bill_for_month,
                    bt.total_bill, bt.total_charges, bt.claim_no, bt.created_at
                 FROM bill_tasks bt WHERE bt.created_by = ?
                 ORDER BY bt.created_at DESC LIMIT 50`,
                [user_id]
            );
            return res.json({ level: 3, bills });
        }

        // Level 2: summary breakdown for a user
        const [[stats]] = await pool.query(
            `SELECT COUNT(*) as total_bills,
                SUM(total_bill) as total_amount,
                SUM(total_charges) as total_claims,
                COUNT(DISTINCT bill_for_month) as months_covered
             FROM bill_tasks WHERE created_by = ?`,
            [user_id]
        );
        const [[userInfo]] = await pool.query('SELECT name FROM users WHERE id = ?', [user_id]);
        res.json({ level: 2, userName: userInfo ? userInfo.name : 'Unknown', heads: [
            { name: 'Total Bills', key: 'bills', count: stats.total_bills },
            { name: 'Total Amount', key: 'amount', count: '₹' + Math.round(stats.total_amount || 0).toLocaleString() },
            { name: 'Total Claims', key: 'claims', count: '₹' + Math.round(stats.total_claims || 0).toLocaleString() },
            { name: 'Months Covered', key: 'months', count: stats.months_covered }
        ]});
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- Complaints Drill-down ---
app.get('/api/complaints/dashboard-drilldown', authenticateToken, async (req, res) => {
    try {
        const { user_id, head } = req.query;

        if (!user_id) return res.status(400).json({ error: 'user_id required' });
        const [[compUserInfo]] = await pool.query('SELECT name FROM users WHERE id = ?', [user_id]);
        const compUserName = compUserInfo ? compUserInfo.name : 'Unknown';

        // Level 3: specific status complaints
        if (head) {
            let statusFilter = '';
            if (head === 'active') statusFilter = "AND c.status IN ('Pending','In Progress')";
            else if (head === 'resolved') statusFilter = "AND c.status = 'Resolved'";
            else if (head === 'escalated') statusFilter = "AND c.status = 'Escalated'";

            const [complaints] = await pool.query(
                `SELECT c.id, c.complaint_no, c.customer_name, c.status, c.priority, c.circle, c.oa_name, c.created_at
                 FROM complaints c WHERE c.assigned_to = ? ${statusFilter}
                 ORDER BY c.created_at DESC LIMIT 50`,
                [user_id]
            );
            return res.json({ level: 3, userName: compUserName, complaints });
        }

        // Level 2: status-wise breakdown for a user
        const [statuses] = await pool.query(
            `SELECT status, COUNT(*) as count FROM complaints WHERE assigned_to = ? GROUP BY status`,
            [user_id]
        );
        const [[userInfo]] = await pool.query('SELECT name FROM users WHERE id = ?', [user_id]);
        const statusMap = {};
        statuses.forEach(s => { statusMap[s.status] = s.count; });
        res.json({ level: 2, userName: userInfo ? userInfo.name : 'Unknown', heads: [
            { name: 'Active', key: 'active', count: (statusMap['Pending'] || 0) + (statusMap['In Progress'] || 0) },
            { name: 'Resolved', key: 'resolved', count: statusMap['Resolved'] || 0 },
            { name: 'Escalated', key: 'escalated', count: statusMap['Escalated'] || 0 },
            { name: 'Closed', key: 'closed', count: statusMap['Closed'] || 0 }
        ]});
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- New Business Dashboard Stats ---
app.get('/api/proposals/dashboard-stats', authenticateToken, async (req, res) => {
    try {
        const today = new Date().toISOString().split('T')[0];
        const isAdmin = req.user.role === 'admin';

        let userFilter = '';
        let userParams = [];
        if (!isAdmin) {
            userFilter = 'WHERE p.user_id = ?';
            userParams = [req.user.id];
        }

        // User-wise proposal stats
        const [userStats] = await pool.query(
            `SELECT u.name, p.user_id,
                COUNT(*) as total,
                SUM(CASE WHEN p.next_followup_date IS NOT NULL AND p.next_followup_date >= ? THEN 1 ELSE 0 END) as upcoming_followup,
                SUM(CASE WHEN p.next_followup_date IS NOT NULL AND p.next_followup_date < ? THEN 1 ELSE 0 END) as overdue_followup,
                SUM(CASE WHEN p.direct_sale = 1 THEN 1 ELSE 0 END) as direct_sales
             FROM proposals p
             LEFT JOIN users u ON p.user_id = u.id
             ${userFilter}
             GROUP BY p.user_id, u.name
             ORDER BY total DESC`,
            [today, today, ...userParams]
        );

        // Overall counts
        const [[totals]] = await pool.query(
            `SELECT COUNT(*) as total,
                SUM(CASE WHEN p.direct_sale = 1 THEN 1 ELSE 0 END) as direct_sales,
                SUM(CASE WHEN p.next_followup_date IS NOT NULL AND p.next_followup_date < ? THEN 1 ELSE 0 END) as overdue_followups
             FROM proposals p ${userFilter}`,
            [today, ...userParams]
        );

        res.json({ userStats, totals });
    } catch(e) {
        console.error('Proposals dashboard stats error:', e);
        res.status(500).json({ error: e.message });
    }
});

// --- BSNL ERP Dashboard Stats (Pending Products/Lines by Circle) ---
app.get('/api/erp/dashboard-stats', authenticateToken, async (req, res) => {
    try {
        const isAdmin = req.user.role === 'admin';
        let whereClause = 'WHERE 1=1';
        let params = [];

        if (!isAdmin) {
            const [userData] = await pool.query('SELECT allowed_circles, allowed_oas FROM users WHERE id = ?', [req.user.id]);
            const u = userData[0];
            const parseArr = v => { try { const a = JSON.parse(v); return Array.isArray(a) ? a.filter(Boolean) : []; } catch(e) { return []; } };
            const circles = parseArr(u.allowed_circles);
            const oas = parseArr(u.allowed_oas);
            if (circles.length > 0) {
                whereClause += ` AND c.circle IN (${circles.map(() => '?').join(',')})`;
                params.push(...circles);
            }
            if (oas.length > 0) {
                whereClause += ` AND c.oa_name IN (${oas.map(() => '?').join(',')})`;
                params.push(...oas);
            }
        }

        // Circle-wise pending products
        const [circleStats] = await pool.query(
            `SELECT c.circle as name, c.circle as circle_id,
                COUNT(*) as total_customers,
                SUM(CASE WHEN (SELECT COUNT(*) FROM customer_orders WHERE customer_id = c.id) = 0
                    OR c.product_plan IS NULL OR c.product_plan = '' THEN 1 ELSE 0 END) as pending_products,
                SUM(CASE WHEN (SELECT COUNT(*) FROM customer_lines WHERE customer_id = c.id) = 0 THEN 1 ELSE 0 END) as pending_lines
             FROM customers c
             ${whereClause}
             GROUP BY c.circle
             HAVING pending_products > 0 OR pending_lines > 0
             ORDER BY pending_products DESC`,
            params
        );

        // Overall totals
        const [[totals]] = await pool.query(
            `SELECT COUNT(*) as total_customers,
                SUM(CASE WHEN (SELECT COUNT(*) FROM customer_orders WHERE customer_id = c.id) = 0
                    OR c.product_plan IS NULL OR c.product_plan = '' THEN 1 ELSE 0 END) as pending_products,
                SUM(CASE WHEN (SELECT COUNT(*) FROM customer_lines WHERE customer_id = c.id) = 0 THEN 1 ELSE 0 END) as pending_lines
             FROM customers c ${whereClause}`,
            params
        );

        res.json({ userStats: circleStats, totals });
    } catch(e) {
        console.error('ERP dashboard stats error:', e);
        res.status(500).json({ error: e.message });
    }
});

// --- WEBSITE ANALYTICS ROUTES ---
// Public endpoint for tracking
app.post('/api/analytics/track', async (req, res) => {
    const { page_url, action_type, details } = req.body;
    const ip = (req.ip || req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').replace('::ffff:', '');
    const userAgent = req.headers['user-agent'];

    try {
        // Geo lookup + DB insert BEFORE response (Vercel kills function after response)
        const geo = await getGeoData(ip);
        await pool.query(
            'INSERT INTO website_analytics (ip_address, user_agent, page_url, action_type, details, city, region, country, isp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [ip, userAgent, page_url, action_type || 'visit', details || null, geo.city, geo.region, geo.country, geo.isp]
        );
        res.status(200).json({ ok: true });
    } catch (err) {
        console.error('Track error:', err);
        res.status(200).json({ ok: true }); // Still return 200 so website doesn't show errors
    }
});

// One-time backfill geo data for blank records (admin only)
app.post('/api/analytics/backfill-geo', authenticateToken, isAdmin, async (req, res) => {
    try {
        // Get records with blank city from both tables
        const [analytics] = await pool.query("SELECT id, ip_address FROM website_analytics WHERE (city IS NULL OR city = '') AND ip_address != '' AND ip_address != '127.0.0.1' AND ip_address != '::1' LIMIT 100");
        const [logins] = await pool.query("SELECT id, ip_address FROM login_logs WHERE (city IS NULL OR city = '') AND ip_address != '' AND ip_address != '127.0.0.1' AND ip_address != '::1' LIMIT 100");
        let updatedA = 0, updatedL = 0;
        // ip-api.com allows 45 req/min — add delay between calls
        for (const row of analytics) {
            try {
                const geoRes = await fetch('http://ip-api.com/json/' + row.ip_address + '?fields=city,regionName,country,isp,status');
                if (geoRes.ok) {
                    const geo = await geoRes.json();
                    if (geo.status === 'success') {
                        await pool.query('UPDATE website_analytics SET city=?, region=?, country=?, isp=? WHERE id=?',
                            [geo.city||'', geo.regionName||'', geo.country||'', geo.isp||'', row.id]);
                        updatedA++;
                    }
                }
                await new Promise(r => setTimeout(r, 1500)); // 1.5s delay between requests
            } catch(e) {}
        }
        for (const row of logins) {
            try {
                const geoRes = await fetch('http://ip-api.com/json/' + row.ip_address + '?fields=city,regionName,country,isp,status');
                if (geoRes.ok) {
                    const geo = await geoRes.json();
                    if (geo.status === 'success') {
                        await pool.query('UPDATE login_logs SET city=?, region=?, country=?, isp=? WHERE id=?',
                            [geo.city||'', geo.regionName||'', geo.country||'', geo.isp||'', row.id]);
                        updatedL++;
                    }
                }
                await new Promise(r => setTimeout(r, 1500));
            } catch(e) {}
        }
        res.json({ success: true, analytics_updated: updatedA, login_logs_updated: updatedL });
    } catch(err) {
        console.error('Backfill error:', err);
        res.status(500).json({ error: err.message });
    }
});

// Admin endpoint for report (with filters)
app.get('/api/analytics/report', authenticateToken, isAdmin, async (req, res) => {
    try {
        const { start, end, search, limit = 200, offset = 0 } = req.query;
        let where = [];
        const params = [];
        if (start) { where.push('DATE(created_at) >= ?'); params.push(start); }
        if (end) { where.push('DATE(created_at) <= ?'); params.push(end); }
        if (search) { where.push('(ip_address LIKE ? OR page_url LIKE ? OR city LIKE ? OR region LIKE ? OR country LIKE ?)'); params.push('%'+search+'%','%'+search+'%','%'+search+'%','%'+search+'%','%'+search+'%'); }
        const whereStr = where.length ? 'WHERE ' + where.join(' AND ') : '';
        const [rows] = await pool.query(
            'SELECT * FROM website_analytics ' + whereStr + ' ORDER BY created_at DESC LIMIT ? OFFSET ?',
            [...params, parseInt(limit), parseInt(offset)]
        );
        const [[{ total }]] = await pool.query(
            'SELECT COUNT(*) as total FROM website_analytics ' + whereStr, params
        );
        res.json({ rows, total });
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

// ── WEBSITE INQUIRY FORM (public, no auth) ─────────────────────────────────────
app.post('/api/website/inquiry', async (req, res) => {
    const { name, email, mobile, interest, message } = req.body;
    if (!name || !mobile) return res.status(400).json({ error: 'Name and mobile are required' });

    try {
        // Send inquiry email to company
        await inquiryMailTransporter.sendMail({
            from: '"BSNL PBX BILL" <info@coralinfratel.com>',
            to: 'info@coralinfratel.com',
            replyTo: email || undefined,
            subject: `New Website Inquiry from ${name}`,
            html: `
                <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
                    <h2 style="color:#e31837;border-bottom:2px solid #e31837;padding-bottom:10px;">New Website Inquiry</h2>
                    <table style="width:100%;border-collapse:collapse;">
                        <tr><td style="padding:8px;font-weight:bold;width:140px;">Name:</td><td style="padding:8px;">${name}</td></tr>
                        <tr style="background:#f8fafc;"><td style="padding:8px;font-weight:bold;">Email:</td><td style="padding:8px;">${email || 'Not provided'}</td></tr>
                        <tr><td style="padding:8px;font-weight:bold;">Mobile:</td><td style="padding:8px;">${mobile}</td></tr>
                        <tr style="background:#f8fafc;"><td style="padding:8px;font-weight:bold;">Interested In:</td><td style="padding:8px;">${interest || 'Not specified'}</td></tr>
                        <tr><td style="padding:8px;font-weight:bold;">Message:</td><td style="padding:8px;">${message || 'No message'}</td></tr>
                    </table>
                    <p style="color:#64748b;font-size:12px;margin-top:20px;">Sent from Coral Infratel website contact form</p>
                </div>
            `
        });

        res.json({ message: 'Inquiry sent successfully!' });
    } catch (err) {
        console.error('Inquiry mail error:', err);
        res.status(500).json({ error: 'Failed to send inquiry. Please try again.' });
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

// ============================================================
// --- DAILY ACTIVITY MODULE APIs ---
// ============================================================

// GET all templates (admin: all, user: own dept)
app.get('/api/activity/templates', authenticateToken, async (req, res) => {
    try {
        let query, params = [];
        if (req.user.role === 'admin') {
            query = 'SELECT * FROM activity_templates ORDER BY department, category, sort_order, id';
        } else {
            const [uRows] = await pool.query('SELECT department FROM users WHERE id=?', [req.user.id]);
            const dept = uRows[0]?.department || 'All';
            query = 'SELECT * FROM activity_templates WHERE (department=? OR department="All") AND is_active=1 ORDER BY category, sort_order, id';
            params = [dept];
        }
        const [rows] = await pool.query(query, params);
        res.json(rows);
    } catch (err) { res.status(500).json({ error: 'Failed to fetch templates' }); }
});

// POST create template (admin only)
app.post('/api/activity/templates', authenticateToken, isAdmin, async (req, res) => {
    const { department, category, task_name, sort_order, input_type } = req.body;
    if (!task_name) return res.status(400).json({ error: 'task_name required' });
    try {
        await pool.query(
            'INSERT INTO activity_templates (department, category, task_name, input_type, sort_order, created_by) VALUES (?,?,?,?,?,?)',
            [department||'All', category||'Daily', task_name, input_type||'status', sort_order||0, req.user.id]
        );
        res.json({ message: 'Template created' });
    } catch (err) { res.status(500).json({ error: 'Failed to create template' }); }
});

// PUT update template (admin only)
app.put('/api/activity/templates/:id', authenticateToken, isAdmin, async (req, res) => {
    const { department, category, task_name, sort_order, is_active, input_type } = req.body;
    try {
        await pool.query(
            'UPDATE activity_templates SET department=?, category=?, task_name=?, input_type=?, sort_order=?, is_active=? WHERE id=?',
            [department, category, task_name, input_type||'status', sort_order||0, is_active===false?0:1, req.params.id]
        );
        res.json({ message: 'Template updated' });
    } catch (err) { res.status(500).json({ error: 'Failed to update template' }); }
});

// DELETE template (admin only)
app.delete('/api/activity/templates/:id', authenticateToken, isAdmin, async (req, res) => {
    try {
        await pool.query('DELETE FROM activity_templates WHERE id=?', [req.params.id]);
        res.json({ message: 'Template deleted' });
    } catch (err) { res.status(500).json({ error: 'Failed to delete template' }); }
});

// GET user assignments
app.get('/api/activity/assignments', authenticateToken, async (req, res) => {
    try {
        const userId = req.query.user_id || (req.user.role !== 'admin' ? req.user.id : null);
        let query = `SELECT aut.*, at.task_name, at.category, at.department, u.name as user_name, u.username
                     FROM activity_user_tasks aut
                     JOIN activity_templates at ON aut.template_id=at.id
                     JOIN users u ON aut.user_id=u.id
                     WHERE aut.is_active=1`;
        const params = [];
        if (userId) { query += ' AND aut.user_id=?'; params.push(userId); }
        query += ' ORDER BY u.name, at.department, at.category, at.sort_order';
        const [rows] = await pool.query(query, params);
        res.json(rows);
    } catch (err) { res.status(500).json({ error: 'Failed to fetch assignments' }); }
});

// POST assign tasks to user (admin only)
app.post('/api/activity/assignments', authenticateToken, isAdmin, async (req, res) => {
    const { user_id, template_ids } = req.body;
    if (!user_id || !Array.isArray(template_ids)) return res.status(400).json({ error: 'user_id and template_ids[] required' });
    try {
        for (const tid of template_ids) {
            await pool.query(
                'INSERT INTO activity_user_tasks (user_id, template_id, assigned_by, assigned_date) VALUES (?,?,?,CURDATE()) ON DUPLICATE KEY UPDATE is_active=1, assigned_by=?, assigned_date=CURDATE()',
                [user_id, tid, req.user.id, req.user.id]
            );
        }
        res.json({ message: `${template_ids.length} tasks assigned` });
    } catch (err) { res.status(500).json({ error: 'Failed to assign tasks' }); }
});

// DELETE assignment (admin only)
app.delete('/api/activity/assignments/:id', authenticateToken, isAdmin, async (req, res) => {
    try {
        await pool.query('UPDATE activity_user_tasks SET is_active=0 WHERE id=?', [req.params.id]);
        res.json({ message: 'Assignment removed' });
    } catch (err) { res.status(500).json({ error: 'Failed to remove assignment' }); }
});

// GET my tasks for a date (auto-show Daily + current week + monthly on 1st-5th + quarterly on 1st-5th of Q months)
app.get('/api/activity/my-tasks', authenticateToken, async (req, res) => {
    const date = req.query.date || new Date().toISOString().split('T')[0];
    const userId = req.query.user_id || req.user.id;
    if (req.user.role !== 'admin' && String(userId) !== String(req.user.id)) {
        return res.status(403).json({ error: 'Access denied' });
    }
    try {
        const d = new Date(date);
        const day = d.getDate();
        const month = d.getMonth() + 1; // 1-12
        let categories = ['Daily'];
        // Week-based tasks
        if (day >= 1 && day <= 7) categories.push('Week1');
        else if (day >= 8 && day <= 14) categories.push('Week2');
        else if (day >= 15 && day <= 21) categories.push('Week3');
        else categories.push('Week4');
        // Monthly tasks — show on 1st to 5th of every month
        if (day >= 1 && day <= 5) categories.push('Monthly');
        // Quarterly tasks — show on 1st to 5th of Jan, Apr, Jul, Oct
        if (day >= 1 && day <= 5 && [1,4,7,10].includes(month)) categories.push('Quarterly');

        const catPlaceholders = categories.map(() => '?').join(',');
        const [tasks] = await pool.query(
            `SELECT aut.id as assignment_id, at.id as template_id, at.task_name, at.category, at.department,
                    at.input_type, al.status, al.value, al.remarks, al.id as log_id
             FROM activity_user_tasks aut
             JOIN activity_templates at ON aut.template_id=at.id
             LEFT JOIN activity_logs al ON al.template_id=at.id AND al.user_id=? AND al.log_date=?
             WHERE aut.user_id=? AND aut.is_active=1 AND at.is_active=1 AND at.category IN (${catPlaceholders})
             ORDER BY at.category, at.sort_order, at.id`,
            [userId, date, userId, ...categories]
        );
        res.json(tasks);
    } catch (err) { res.status(500).json({ error: 'Failed to fetch tasks' }); }
});

// POST/PUT submit daily log (upsert) — supports value field + late detection
app.post('/api/activity/logs', authenticateToken, async (req, res) => {
    const { logs, log_date } = req.body; // logs: [{template_id, status, value, remarks}]
    if (!Array.isArray(logs) || !log_date) return res.status(400).json({ error: 'logs[] and log_date required' });
    const userId = req.user.id;
    try {
        // Check if submission is late (not same day)
        const today = new Date().toISOString().split('T')[0];
        const isLate = log_date < today ? 1 : 0;
        for (const l of logs) {
            await pool.query(
                `INSERT INTO activity_logs (user_id, template_id, log_date, status, value, remarks, submitted_late)
                 VALUES (?,?,?,?,?,?,?)
                 ON DUPLICATE KEY UPDATE status=VALUES(status), value=VALUES(value), remarks=VALUES(remarks), submitted_late=VALUES(submitted_late), updated_at=NOW()`,
                [userId, l.template_id, log_date, l.status||'not_done', l.value||null, l.remarks||'', isLate]
            );
        }
        res.json({ message: `${logs.length} task(s) saved for ${log_date}`, late: isLate===1 });
    } catch (err) { res.status(500).json({ error: 'Failed to save logs' }); }
});

// GET activity logs (admin: all, user: own) with filters
app.get('/api/activity/logs', authenticateToken, async (req, res) => {
    const { user_id, date_from, date_to, department } = req.query;
    try {
        let where = ['1=1'];
        const params = [];
        if (req.user.role !== 'admin') { where.push('al.user_id=?'); params.push(req.user.id); }
        else if (user_id) { where.push('al.user_id=?'); params.push(user_id); }
        if (date_from) { where.push('al.log_date >= ?'); params.push(date_from); }
        if (date_to) { where.push('al.log_date <= ?'); params.push(date_to); }
        if (department) { where.push('at.department=?'); params.push(department); }

        const [rows] = await pool.query(
            `SELECT al.*, at.task_name, at.category, at.department,
                    u.name as user_name, u.username, u.department as user_dept
             FROM activity_logs al
             LEFT JOIN activity_templates at ON al.template_id=at.id
             JOIN users u ON al.user_id=u.id
             WHERE ${where.join(' AND ')}
             ORDER BY al.log_date DESC, u.name, at.category, at.sort_order`,
            params
        );
        res.json(rows);
    } catch (err) { res.status(500).json({ error: 'Failed to fetch logs' }); }
});

// GET list of users with their task summary for a date (admin dashboard)
app.get('/api/activity/summary', authenticateToken, isAdmin, async (req, res) => {
    const date = req.query.date || new Date().toISOString().split('T')[0];
    try {
        const [rows] = await pool.query(
            `SELECT u.id, u.name, u.username, u.department,
                    COUNT(aut.id) as total_assigned,
                    SUM(CASE WHEN al.status='done' THEN 1 ELSE 0 END) as done_count,
                    SUM(CASE WHEN al.status='not_done' THEN 1 ELSE 0 END) as not_done_count,
                    SUM(CASE WHEN al.status='partial' THEN 1 ELSE 0 END) as partial_count,
                    SUM(CASE WHEN al.status='leave' THEN 1 ELSE 0 END) as leave_count
             FROM users u
             LEFT JOIN activity_user_tasks aut ON aut.user_id=u.id AND aut.is_active=1
             LEFT JOIN activity_logs al ON al.user_id=u.id AND al.log_date=? AND al.template_id=aut.template_id
             WHERE u.role != 'admin' OR u.id = u.id
             GROUP BY u.id
             ORDER BY u.name`,
            [date]
        );
        res.json(rows);
    } catch (err) { res.status(500).json({ error: 'Failed to fetch summary' }); }
});

// ============================================================
// --- PENDENCY & PERFORMANCE APIs ---
// ============================================================

// GET pendency — dates where user has assigned tasks but no/incomplete submissions
app.get('/api/activity/pendency', authenticateToken, async (req, res) => {
    const userId = req.query.user_id || req.user.id;
    if (req.user.role !== 'admin' && String(userId) !== String(req.user.id))
        return res.status(403).json({ error: 'Access denied' });
    try {
        const today = new Date().toISOString().split('T')[0];
        // Get all dates from 30 days ago to yesterday where user had Daily tasks assigned but didn't submit all
        const [rows] = await pool.query(`
            SELECT d.dt as pending_date,
                   COUNT(aut.id) as total_tasks,
                   SUM(CASE WHEN al.id IS NOT NULL THEN 1 ELSE 0 END) as submitted_tasks,
                   COUNT(aut.id) - SUM(CASE WHEN al.id IS NOT NULL THEN 1 ELSE 0 END) as pending_tasks
            FROM (
                SELECT DATE_SUB(?, INTERVAL n DAY) as dt
                FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
                      UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9
                      UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14
                      UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19
                      UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24
                      UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29) nums
            ) d
            JOIN activity_user_tasks aut ON aut.user_id=? AND aut.is_active=1
            JOIN activity_templates at ON aut.template_id=at.id AND at.is_active=1 AND at.category='Daily'
            LEFT JOIN activity_logs al ON al.user_id=? AND al.template_id=at.id AND al.log_date=d.dt
            WHERE d.dt < ? AND d.dt >= DATE_SUB(?, INTERVAL 30 DAY) AND DAYOFWEEK(d.dt) NOT IN (1)
            GROUP BY d.dt
            HAVING pending_tasks > 0
            ORDER BY d.dt DESC
        `, [today, userId, userId, today, today]);
        res.json(rows);
    } catch (err) { res.status(500).json({ error: 'Failed to fetch pendency', detail: err.message }); }
});

// GET performance score for a user
app.get('/api/performance/my-score', authenticateToken, async (req, res) => {
    const userId = req.query.user_id || req.user.id;
    if (req.user.role !== 'admin' && String(userId) !== String(req.user.id))
        return res.status(403).json({ error: 'Access denied' });
    const period = req.query.period || 'monthly'; // monthly, quarterly, half_yearly, annual
    const dateStr = req.query.date || new Date().toISOString().split('T')[0];
    try {
        const d = new Date(dateStr);
        let startDate, endDate;
        if (period === 'monthly') {
            startDate = new Date(d.getFullYear(), d.getMonth(), 1).toISOString().split('T')[0];
            endDate = new Date(d.getFullYear(), d.getMonth() + 1, 0).toISOString().split('T')[0];
        } else if (period === 'quarterly') {
            const q = Math.floor(d.getMonth() / 3) * 3;
            startDate = new Date(d.getFullYear(), q, 1).toISOString().split('T')[0];
            endDate = new Date(d.getFullYear(), q + 3, 0).toISOString().split('T')[0];
        } else if (period === 'half_yearly') {
            const h = d.getMonth() < 6 ? 0 : 6;
            startDate = new Date(d.getFullYear(), h, 1).toISOString().split('T')[0];
            endDate = new Date(d.getFullYear(), h + 6, 0).toISOString().split('T')[0];
        } else { // annual
            startDate = new Date(d.getFullYear(), 0, 1).toISOString().split('T')[0];
            endDate = new Date(d.getFullYear(), 11, 31).toISOString().split('T')[0];
        }
        // Calculate score from activity_logs
        const [stats] = await pool.query(`
            SELECT
                COUNT(*) as total_logs,
                SUM(CASE WHEN al.status='done' AND al.submitted_late=0 THEN 1 ELSE 0 END) as done_on_time,
                SUM(CASE WHEN al.status='done' AND al.submitted_late=1 THEN 1 ELSE 0 END) as done_late,
                SUM(CASE WHEN al.status='partial' THEN 1 ELSE 0 END) as partial_count,
                SUM(CASE WHEN al.status='not_done' THEN 1 ELSE 0 END) as not_done_count,
                SUM(CASE WHEN al.status='leave' THEN 1 ELSE 0 END) as leave_count,
                SUM(CASE WHEN al.status='na' THEN 1 ELSE 0 END) as na_count
            FROM activity_logs al
            WHERE al.user_id=? AND al.log_date BETWEEN ? AND ?
        `, [userId, startDate, endDate]);

        // Count total expected task-days (working days × daily assigned tasks)
        const [assignCount] = await pool.query(
            `SELECT COUNT(*) as cnt FROM activity_user_tasks aut
             JOIN activity_templates at ON aut.template_id=at.id
             WHERE aut.user_id=? AND aut.is_active=1 AND at.is_active=1 AND at.category='Daily'`, [userId]);
        const dailyTaskCount = assignCount[0]?.cnt || 0;

        // Count working days in range (exclude Sundays)
        const [wdRows] = await pool.query(`
            SELECT COUNT(*) as wd FROM (
                SELECT DATE_ADD(?, INTERVAL n DAY) as dt
                FROM (SELECT @row := @row + 1 as n FROM
                    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
                    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
                    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3) c,
                    (SELECT @row := -1) r
                ) nums
                WHERE DATE_ADD(?, INTERVAL n DAY) <= ?
            ) dates WHERE DAYOFWEEK(dt) NOT IN (1) AND dt <= CURDATE()
        `, [startDate, startDate, endDate]);
        const workingDays = wdRows[0]?.wd || 0;
        const totalExpected = dailyTaskCount * workingDays;

        // Count days where user submitted at least one log
        const [subDays] = await pool.query(
            `SELECT COUNT(DISTINCT log_date) as cnt FROM activity_logs WHERE user_id=? AND log_date BETWEEN ? AND ?`,
            [userId, startDate, endDate]);
        const submittedDays = subDays[0]?.cnt || 0;
        const missedDays = Math.max(0, workingDays - submittedDays);

        const s = stats[0] || {};
        const doneOnTime = s.done_on_time || 0;
        const doneLate = s.done_late || 0;
        const partial = s.partial_count || 0;
        const notDone = s.not_done_count || 0;
        const totalLogs = s.total_logs || 0;
        // Scoring: on-time=100%, late=60%, partial=40%, not_done=0%, missed=0%
        const scoreNumerator = (doneOnTime * 100) + (doneLate * 60) + (partial * 40);
        const scoreDenom = totalExpected > 0 ? totalExpected : 1;
        const score = Math.min(100, Math.round((scoreNumerator / scoreDenom) * 100) / 100);
        let grade = 'F';
        if (score >= 95) grade = 'A+';
        else if (score >= 85) grade = 'A';
        else if (score >= 75) grade = 'B+';
        else if (score >= 65) grade = 'B';
        else if (score >= 50) grade = 'C';
        else if (score >= 35) grade = 'D';

        const submissionRate = workingDays > 0 ? Math.round((submittedDays / workingDays) * 100) : 0;

        res.json({
            user_id: parseInt(userId), period, period_start: startDate, period_end: endDate,
            total_expected: totalExpected, total_logs: totalLogs,
            done_on_time: doneOnTime, done_late: doneLate, partial: partial, not_done: notDone,
            working_days: workingDays, submitted_days: submittedDays, missed_days: missedDays,
            submission_rate: submissionRate, score, grade
        });
    } catch (err) { res.status(500).json({ error: 'Failed to calculate score', detail: err.message }); }
});

// GET team performance (admin only) — all users' scores for a period
app.get('/api/performance/team', authenticateToken, isAdmin, async (req, res) => {
    const period = req.query.period || 'monthly';
    const dateStr = req.query.date || new Date().toISOString().split('T')[0];
    try {
        const [users] = await pool.query('SELECT id, name, username, department, role FROM users WHERE is_active=1 ORDER BY name');
        const results = [];
        for (const u of users) {
            // Reuse score logic via internal fetch
            const d = new Date(dateStr);
            let startDate, endDate;
            if (period === 'monthly') {
                startDate = new Date(d.getFullYear(), d.getMonth(), 1).toISOString().split('T')[0];
                endDate = new Date(d.getFullYear(), d.getMonth() + 1, 0).toISOString().split('T')[0];
            } else if (period === 'quarterly') {
                const q = Math.floor(d.getMonth() / 3) * 3;
                startDate = new Date(d.getFullYear(), q, 1).toISOString().split('T')[0];
                endDate = new Date(d.getFullYear(), q + 3, 0).toISOString().split('T')[0];
            } else if (period === 'half_yearly') {
                const h = d.getMonth() < 6 ? 0 : 6;
                startDate = new Date(d.getFullYear(), h, 1).toISOString().split('T')[0];
                endDate = new Date(d.getFullYear(), h + 6, 0).toISOString().split('T')[0];
            } else {
                startDate = new Date(d.getFullYear(), 0, 1).toISOString().split('T')[0];
                endDate = new Date(d.getFullYear(), 11, 31).toISOString().split('T')[0];
            }
            const [stats] = await pool.query(`
                SELECT COUNT(*) as total_logs,
                    SUM(CASE WHEN status='done' AND submitted_late=0 THEN 1 ELSE 0 END) as done_on_time,
                    SUM(CASE WHEN status='done' AND submitted_late=1 THEN 1 ELSE 0 END) as done_late,
                    SUM(CASE WHEN status='partial' THEN 1 ELSE 0 END) as partial_count,
                    SUM(CASE WHEN status='not_done' THEN 1 ELSE 0 END) as not_done_count
                FROM activity_logs WHERE user_id=? AND log_date BETWEEN ? AND ?`, [u.id, startDate, endDate]);
            const [ac] = await pool.query(
                `SELECT COUNT(*) as cnt FROM activity_user_tasks aut JOIN activity_templates at ON aut.template_id=at.id
                 WHERE aut.user_id=? AND aut.is_active=1 AND at.is_active=1 AND at.category='Daily'`, [u.id]);
            const [wdR] = await pool.query(`
                SELECT COUNT(*) as wd FROM (
                    SELECT DATE_ADD(?, INTERVAL n DAY) as dt FROM (
                        SELECT @r:=@r+1 as n FROM
                        (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
                        (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
                        (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3) c,
                        (SELECT @r:=-1) r
                    ) nums WHERE DATE_ADD(?, INTERVAL n DAY) <= ?
                ) dates WHERE DAYOFWEEK(dt) NOT IN (1) AND dt <= CURDATE()
            `, [startDate, startDate, endDate]);
            const [sdR] = await pool.query(
                'SELECT COUNT(DISTINCT log_date) as cnt FROM activity_logs WHERE user_id=? AND log_date BETWEEN ? AND ?',
                [u.id, startDate, endDate]);
            const dailyTasks = ac[0]?.cnt || 0;
            const wd = wdR[0]?.wd || 0;
            const totalExp = dailyTasks * wd;
            const s = stats[0] || {};
            const dOnTime = s.done_on_time || 0;
            const dLate = s.done_late || 0;
            const partial = s.partial_count || 0;
            const notDone = s.not_done_count || 0;
            const scoreN = (dOnTime * 100) + (dLate * 60) + (partial * 40);
            const scoreD = totalExp > 0 ? totalExp : 1;
            const score = Math.min(100, Math.round((scoreN / scoreD) * 100) / 100);
            let grade = 'F';
            if (score >= 95) grade = 'A+'; else if (score >= 85) grade = 'A'; else if (score >= 75) grade = 'B+';
            else if (score >= 65) grade = 'B'; else if (score >= 50) grade = 'C'; else if (score >= 35) grade = 'D';
            const subDays = sdR[0]?.cnt || 0;
            const subRate = wd > 0 ? Math.round((subDays / wd) * 100) : 0;
            results.push({
                user_id: u.id, name: u.name, username: u.username, department: u.department, role: u.role,
                total_expected: totalExp, done_on_time: dOnTime, done_late: dLate, partial, not_done: notDone,
                working_days: wd, submitted_days: subDays, missed_days: Math.max(0, wd - subDays),
                submission_rate: subRate, score, grade
            });
        }
        results.sort((a, b) => b.score - a.score); // Sort by score desc
        res.json({ period, results });
    } catch (err) { res.status(500).json({ error: 'Failed to fetch team performance', detail: err.message }); }
});

// GET monthly trend for a user (last 6 or 12 months)
app.get('/api/performance/trend', authenticateToken, async (req, res) => {
    const userId = req.query.user_id || req.user.id;
    if (req.user.role !== 'admin' && String(userId) !== String(req.user.id))
        return res.status(403).json({ error: 'Access denied' });
    const months = parseInt(req.query.months) || 6;
    try {
        const results = [];
        for (let i = months - 1; i >= 0; i--) {
            const d = new Date();
            d.setMonth(d.getMonth() - i);
            const startDate = new Date(d.getFullYear(), d.getMonth(), 1).toISOString().split('T')[0];
            const endDate = new Date(d.getFullYear(), d.getMonth() + 1, 0).toISOString().split('T')[0];
            const [stats] = await pool.query(`
                SELECT COUNT(*) as total_logs,
                    SUM(CASE WHEN status='done' AND submitted_late=0 THEN 1 ELSE 0 END) as done_on_time,
                    SUM(CASE WHEN status='done' AND submitted_late=1 THEN 1 ELSE 0 END) as done_late,
                    SUM(CASE WHEN status='partial' THEN 1 ELSE 0 END) as partial_count,
                    SUM(CASE WHEN status='not_done' THEN 1 ELSE 0 END) as not_done_count
                FROM activity_logs WHERE user_id=? AND log_date BETWEEN ? AND ?`, [userId, startDate, endDate]);
            const [ac] = await pool.query(
                `SELECT COUNT(*) as cnt FROM activity_user_tasks aut JOIN activity_templates at ON aut.template_id=at.id
                 WHERE aut.user_id=? AND aut.is_active=1 AND at.is_active=1 AND at.category='Daily'`, [userId]);
            const [wdR] = await pool.query(`
                SELECT COUNT(*) as wd FROM (
                    SELECT DATE_ADD(?, INTERVAL n DAY) as dt FROM (
                        SELECT @rr:=@rr+1 as n FROM
                        (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
                        (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
                        (SELECT @rr:=-1) r
                    ) nums WHERE DATE_ADD(?, INTERVAL n DAY) <= ?
                ) dates WHERE DAYOFWEEK(dt) NOT IN (1) AND dt <= CURDATE()
            `, [startDate, startDate, endDate]);
            const dailyTasks = ac[0]?.cnt || 0;
            const wd = wdR[0]?.wd || 0;
            const totalExp = dailyTasks * wd;
            const s = stats[0] || {};
            const scoreN = ((s.done_on_time||0) * 100) + ((s.done_late||0) * 60) + ((s.partial_count||0) * 40);
            const score = totalExp > 0 ? Math.min(100, Math.round((scoreN / totalExp) * 100) / 100) : 0;
            let grade = 'F';
            if (score >= 95) grade = 'A+'; else if (score >= 85) grade = 'A'; else if (score >= 75) grade = 'B+';
            else if (score >= 65) grade = 'B'; else if (score >= 50) grade = 'C'; else if (score >= 35) grade = 'D';
            results.push({
                month: startDate.substring(0, 7), start: startDate, end: endDate,
                score, grade, total_expected: totalExp, total_logs: s.total_logs || 0
            });
        }
        res.json(results);
    } catch (err) { res.status(500).json({ error: 'Failed to fetch trend', detail: err.message }); }
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

// --- KEEP WARM: Self-ping every 5 minutes to prevent Vercel cold starts ---
if (process.env.VERCEL) {
    setInterval(() => {
        fetch('https://officetask-roan.vercel.app/api/health').catch(() => {});
    }, 5 * 60 * 1000); // Every 5 minutes
}

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

// ── BULK PDF DOWNLOAD ─────────────────────────────────────────────────────────
// NOTE: Must be BEFORE the catch-all route below
app.get('/api/bulk-pdf-download/:jobId', authenticateToken, (req, res) => {
    const job = bulkPdfJobs.get(req.params.jobId);
    if (!job) return res.status(404).json({ success: false, error: 'ZIP not found or expired.' });
    if (!fs.existsSync(job.zipPath)) return res.status(410).json({ success: false, error: 'ZIP file no longer available.' });

    const zipFilename = `Coral_Bills_${new Date().toISOString().slice(0,10)}.zip`;
    res.download(job.zipPath, zipFilename, (err) => {
        if (err) console.error('Bulk PDF download error:', err.message);
    });
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
    await initializeAreaEngineerMappingTables();
    await initializeCustomersTable();
    await initializeTasksTable();
    await initializeUserManagersTable();
    await initializeTaskCategoriesTable();
    await initializeRecurringTasksTable();
    await initializeActivityTables();
    await initializeTaskReportsTable();
    await initializeAnalyticsTable();
    await initializeSystemLogsTable();
    await initializeWebsiteContentTable();
    await initBillPdfsTable();
    await initBillTasksTable();
    await initGstMasterTable();
    await initRevenueLevelsTable();
    await initModemSchemesTable();
    await initBaTspMasterTable();
    await initBsnlStaffContactsTable();
    await initManualCollectionsTable();
    await initBaLevelsTable();
    await initBillEmailLog();
    await initDiaryTasksTables();
    await initializeHRTables();
    await initializePRTables();
    await initLoginLogsTable();
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

// ── BULK PDF HELPERS ──────────────────────────────────────────────────────────

/**
 * Download a file from a URL (http or https) to destPath.
 * Follows single-level redirects (301/302).
 */
function downloadFile(url, destPath) {
    return new Promise((resolve, reject) => {
        const doRequest = (targetUrl) => {
            const proto = targetUrl.startsWith('https') ? https : http;
            proto.get(targetUrl, (res) => {
                if (res.statusCode === 301 || res.statusCode === 302) {
                    doRequest(res.headers.location);
                    return;
                }
                if (res.statusCode !== 200) {
                    reject(new Error(`HTTP ${res.statusCode} for ${targetUrl}`));
                    return;
                }
                const out = fs.createWriteStream(destPath);
                res.pipe(out);
                out.on('finish', () => { out.close(); resolve(); });
                out.on('error', reject);
            }).on('error', reject);
        };
        doRequest(url);
    });
}

/**
 * Zip all files in srcDir into destZip using archiver.
 */
function createZip(srcDir, destZip) {
    return new Promise((resolve, reject) => {
        const output = fs.createWriteStream(destZip);
        const archive = archiver('zip', { zlib: { level: 6 } });
        output.on('close', resolve);
        archive.on('error', reject);
        archive.pipe(output);
        archive.directory(srcDir, false);
        archive.finalize();
    });
}

// ── BULK PDF PROCESS ──────────────────────────────────────────────────────────
// Accepts JSON body with base64-encoded Excel (Vercel-compatible, no multer)
app.post('/api/bulk-pdf-process', authenticateToken, async (req, res) => {
    const { excelBase64 } = req.body || {};
    if (!excelBase64) return res.status(400).json({ success: false, error: 'No Excel file uploaded.' });
    const fileBuffer = Buffer.from(excelBase64, 'base64');
    // Shim req.file so the rest of the function works unchanged
    req.file = { buffer: fileBuffer };

    // Clean up old jobs (> 1 hour)
    const ONE_HOUR = 60 * 60 * 1000;
    for (const [jid, job] of bulkPdfJobs.entries()) {
        if (Date.now() - job.createdAt > ONE_HOUR) {
            try { fs.rmSync(path.dirname(job.zipPath), { recursive: true, force: true }); } catch (_) {}
            bulkPdfJobs.delete(jid);
        }
    }

    let workbook;
    try {
        workbook = xlsx.read(req.file.buffer, { type: 'buffer' });
    } catch (e) {
        return res.status(400).json({ success: false, error: 'Could not parse Excel file.' });
    }

    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rows = xlsx.utils.sheet_to_json(sheet, { header: 1 });

    // Skip header row if first cell looks non-numeric (e.g. "Telephone Number")
    const dataRows = rows.filter((r, idx) => {
        if (idx === 0) {
            const firstCell = String(r[0] || '').trim();
            return /^\d/.test(firstCell); // keep if starts with digit
        }
        return r[0] && r[1]; // keep rows with both columns
    });

    if (!dataRows.length) {
        return res.status(400).json({ success: false, error: 'No valid data rows found in Excel.' });
    }

    // Create a temp directory for this job
    const jobId = `bulk_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const tmpDir = path.join(os.tmpdir(), jobId);
    const pdfDir = path.join(tmpDir, 'pdfs');
    fs.mkdirSync(pdfDir, { recursive: true });

    const results = [];
    let matched = 0, notFound = 0, failed = 0, emailed = 0;

    for (const row of dataRows) {
        const telNo = String(row[0] || '').trim();
        const pdfUrl = String(row[1] || '').trim();

        if (!telNo || !pdfUrl) continue;

        let customer = null;
        try {
            // Look up customer via customer_lines table (telephone_number column)
            const [rows2] = await pool.query(
                `SELECT c.id, c.customer_name, c.circle, c.oa_name,
                        COALESCE(c.acc_person_email, c.email_id) AS email
                 FROM customers c
                 INNER JOIN customer_lines cl ON cl.customer_id = c.id
                 WHERE cl.telephone_number = ?
                    OR CONCAT(cl.telephone_code, cl.telephone_number) = ?
                    OR REPLACE(cl.telephone_number, '-', '') = REPLACE(?, '-', '')
                 LIMIT 1`,
                [telNo, telNo, telNo]
            );
            customer = rows2[0] || null;
        } catch (e) {
            results.push({ telNo, status: 'failed', reason: 'DB error: ' + e.message });
            failed++;
            continue;
        }

        if (!customer) {
            results.push({ telNo, status: 'not_found' });
            notFound++;
            continue;
        }

        matched++;

        // Build filename
        const circleAbbr = (customer.circle || 'UNKNW').replace(/\s/g, '').toUpperCase().substring(0, 5);
        const oaAbbr     = (customer.oa_name || 'UNKNW').replace(/\s/g, '').toUpperCase().substring(0, 5);
        const telClean   = telNo.replace(/[/\\?%*:|"<>]/g, '-'); // sanitize for filename
        const filename   = `Coral_${telClean}_${circleAbbr}_${oaAbbr}.pdf`;
        const destPath   = path.join(pdfDir, filename);

        // Download PDF
        try {
            await downloadFile(pdfUrl, destPath);
        } catch (e) {
            results.push({ telNo, customerName: customer.customer_name, status: 'failed', reason: 'Download failed: ' + e.message });
            failed++;
            continue;
        }

        // Email PDF
        let emailedOk = false;
        const recipientEmail = (customer.email || '').trim();
        if (recipientEmail) {
            try {
                await billMailTransporter.sendMail({
                    from: '"Coral Infratel Pvt. Ltd." <bsnlpribill@gmail.com>',
                    to: recipientEmail,
                    subject: `Bill - ${customer.customer_name}`,
                    text: `Dear ${customer.customer_name},\n\nPlease find your bill attached.\n\nRegards,\nCoral Infratel Pvt. Ltd.`,
                    attachments: [{ filename, path: destPath }]
                });
                emailedOk = true;
                emailed++;
            } catch (e) {
                // email failed — file still downloaded; log but don't fail the row
            }
        }

        results.push({
            telNo,
            customerName: customer.customer_name,
            filename,
            status: 'success',
            emailed: emailedOk
        });
    }

    // Create ZIP only if at least one PDF was successfully downloaded
    let zipJobId = null;
    const successCount = results.filter(r => r.status === 'success').length;
    if (successCount > 0) {
        const zipPath = path.join(tmpDir, `Coral_Bills_${new Date().toISOString().slice(0,10)}.zip`);
        try {
            await createZip(pdfDir, zipPath);
            bulkPdfJobs.set(jobId, { zipPath, createdAt: Date.now() });
            zipJobId = jobId;
        } catch (e) {
            console.error('[bulk-pdf] ZIP creation failed:', e.message);
        }
    }

    return res.json({
        success: true,
        jobId: zipJobId, // null if ZIP failed (download button hidden in UI)
        zipAvailable: !!zipJobId,
        total: dataRows.length,
        matched,
        notFound,
        failed,
        emailed,
        results
    });
});

// ── BULK PDF: HELPER — Download PDF to Buffer ────────────────────────────────
// Uses cPanel PHP proxy for BSNL URLs (Vercel IPs are blocked by BSNL)
const PDF_PROXY_URL = 'https://coralinfratel.com/pdf_proxy.php';
const PDF_PROXY_KEY = 'CoralBill2026Proxy';

function downloadPdfBuffer(url) {
    // For BSNL URLs, use cPanel proxy
    if (url.includes('bsnl.co.in')) {
        return downloadViaProxy(url);
    }
    return downloadDirect(url);
}

function downloadViaProxy(originalUrl) {
    return new Promise((resolve, reject) => {
        const proxyUrl = `${PDF_PROXY_URL}?key=${encodeURIComponent(PDF_PROXY_KEY)}&url=${encodeURIComponent(originalUrl)}`;
        const parsedUrl = new URL(proxyUrl);
        const opts = {
            hostname: parsedUrl.hostname,
            path: parsedUrl.pathname + parsedUrl.search,
            timeout: 30000,
            headers: {
                'User-Agent': 'CoralCRM/1.0',
                'Accept': 'application/pdf,*/*'
            }
        };
        const req = https.get(opts, (res) => {
            if (res.statusCode !== 200) {
                const chunks = [];
                res.on('data', c => chunks.push(c));
                res.on('end', () => {
                    const body = Buffer.concat(chunks).toString();
                    reject(new Error(`Proxy HTTP ${res.statusCode}: ${body.substring(0, 200)}`));
                });
                return;
            }
            const chunks = [];
            res.on('data', chunk => chunks.push(chunk));
            res.on('end', () => {
                const buf = Buffer.concat(chunks);
                if (buf.length < 500) {
                    reject(new Error(`Proxy returned too small response (${buf.length} bytes)`));
                } else {
                    resolve(buf);
                }
            });
            res.on('error', reject);
        });
        req.on('error', (e) => reject(new Error(`Proxy error: ${e.message}`)));
        req.on('timeout', () => { req.destroy(); reject(new Error('Proxy timeout 30s')); });
    });
}

function downloadDirect(url, redirectCount = 0) {
    return new Promise((resolve, reject) => {
        if (redirectCount > 5) return reject(new Error('Too many redirects'));
        const parsedUrl = new URL(url);
        const proto = parsedUrl.protocol === 'https:' ? https : http;
        const opts = {
            hostname: parsedUrl.hostname,
            port: parsedUrl.port || (parsedUrl.protocol === 'https:' ? 443 : 80),
            path: parsedUrl.pathname + parsedUrl.search,
            timeout: 45000,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'Accept': 'application/pdf,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive'
            }
        };
        const req = proto.get(opts, (res) => {
            if (res.statusCode === 301 || res.statusCode === 302) {
                res.resume();
                const loc = res.headers.location;
                if (!loc) return reject(new Error('Redirect without location'));
                const nextUrl = loc.startsWith('http') ? loc : new URL(loc, url).href;
                return downloadDirect(nextUrl, redirectCount + 1).then(resolve).catch(reject);
            }
            if (res.statusCode !== 200) {
                res.resume();
                return reject(new Error(`HTTP ${res.statusCode} from ${parsedUrl.hostname}`));
            }
            const chunks = [];
            res.on('data', chunk => chunks.push(chunk));
            res.on('end', () => resolve(Buffer.concat(chunks)));
            res.on('error', reject);
        });
        req.on('error', (e) => reject(new Error(`${e.message} (${parsedUrl.hostname})`)));
        req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout 45s (${parsedUrl.hostname})`)); });
    });
}

// ── BULK PDF: Excel+PDFs OR Auto-Read PDFs → Match → Rename → ZIP ──
const bulkPdfUpload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 10 * 1024 * 1024, files: 510 }
}).fields([
    { name: 'excel', maxCount: 1 },
    { name: 'pdfs', maxCount: 500 }
]);

// Extract telephone numbers from PDF text content
function extractTelFromPdf(text) {
    const patterns = [
        /(?:Telephone\s*(?:No|Number|#)?[:\s]*)([\d-]{7,15})/gi,
        /(?:Tel\s*(?:No|Number)?[:\s]*)([\d-]{7,15})/gi,
        /(?:Phone\s*(?:No|Number)?[:\s]*)([\d-]{7,15})/gi,
        /(?:Service\s*(?:No|Number)?[:\s]*)([\d-]{7,15})/gi,
        /(?:Account\s*(?:No|Number)?[:\s]*)([\d-]{7,15})/gi,
        /\b(0\d{2,4}-\d{6,8})\b/g,
    ];
    const found = [];
    for (const pat of patterns) {
        let m;
        while ((m = pat.exec(text)) !== null) {
            const num = m[1].trim().replace(/\s+/g, '');
            if (num.length >= 7) found.push(num);
        }
    }
    return [...new Set(found)];
}

app.post('/api/bulk-pdf-job/start', authenticateToken, (req, res, next) => {
    bulkPdfUpload(req, res, (err) => {
        if (err) return res.status(400).json({ error: 'Upload error: ' + err.message });
        next();
    });
}, async (req, res) => {
    try {
        const excelFile = req.files && req.files['excel'] && req.files['excel'][0];
        const pdfFiles  = (req.files && req.files['pdfs']) || [];
        if (!pdfFiles.length) return res.status(400).json({ error: 'No PDF files uploaded.' });

        const useExcelMethod = !!excelFile;

        const jobId = `bulk_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
        const job = {
            jobId, total: 0, matched: 0, notFound: 0,
            renamed: 0, noFile: 0,
            zipReady: false, zipPath: null,
            rows: [], errors: []
        };

        if (useExcelMethod) {
            // ── METHOD 1: Excel + PDFs ──
            let workbook;
            try { workbook = xlsx.read(excelFile.buffer, { type: 'buffer' }); }
            catch(e) { return res.status(400).json({ error: 'Invalid Excel file.' }); }

            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const allRows = xlsx.utils.sheet_to_json(sheet, { header: 1 });
            const dataRows = allRows.filter(r => {
                const tel = String(r[0]||'').trim();
                return tel && /^\d/.test(tel);
            });
            job.total = dataRows.length;

            // Build PDF lookup map from filenames
            const pdfMap = new Map();
            for (const pf of pdfFiles) {
                const nameNoExt = (pf.originalname || '').replace(/\.pdf$/i, '');
                const digits = nameNoExt.replace(/[^0-9-]/g, '').replace(/^-+|-+$/g, '');
                if (digits) {
                    pdfMap.set(digits, { buffer: pf.buffer, originalName: pf.originalname });
                    const noDash = digits.replace(/-/g, '');
                    if (noDash !== digits) pdfMap.set(noDash, { buffer: pf.buffer, originalName: pf.originalname });
                }
            }

            // Match each Excel row with DB + find matching PDF
            for (const row of dataRows) {
                const telNo = String(row[0]||'').trim();
                const telNoDash = telNo.replace(/-/g, '');
                try {
                    const [rows2] = await pool.query(
                        `SELECT c.customer_name, COALESCE(c.acc_person_email, c.email_id) AS email
                         FROM customers c
                         INNER JOIN customer_lines cl ON cl.customer_id = c.id
                         WHERE cl.telephone_number = ?
                            OR CONCAT(cl.telephone_code, cl.telephone_number) = ?
                            OR REPLACE(cl.telephone_number, '-', '') = REPLACE(?, '-', '')
                         LIMIT 1`,
                        [telNo, telNo, telNo]
                    );
                    const cust = rows2[0] || null;
                    if (cust) {
                        const nameClean = cust.customer_name.replace(/[/\\?%*:|"<>&]/g,'_').replace(/\s+/g,'_').replace(/_+/g,'_').replace(/^_|_$/g,'');
                        const telClean  = telNo.replace(/[/\\?%*:|"<>&]/g,'-');
                        const pdf = pdfMap.get(telNo) || pdfMap.get(telNoDash);
                        if (pdf) {
                            job.rows.push({
                                telNo, customerName: cust.customer_name, email: cust.email||'',
                                filename: `${nameClean}_${telClean}.pdf`,
                                originalPdf: pdf.originalName,
                                pdfBuffer: pdf.buffer,
                                status: 'matched', fileStatus: 'found'
                            });
                            job.matched++;
                            job.renamed++;
                        } else {
                            job.rows.push({
                                telNo, customerName: cust.customer_name, email: cust.email||'',
                                filename: `${nameClean}_${telClean}.pdf`,
                                status: 'matched', fileStatus: 'no_pdf'
                            });
                            job.matched++;
                            job.noFile++;
                        }
                    } else {
                        job.rows.push({ telNo, status: 'not_found', fileStatus: 'skipped' });
                        job.notFound++;
                    }
                } catch(e) {
                    job.rows.push({ telNo, status: 'error', fileStatus: 'skipped', error: e.message });
                }
            }
        } else {
            // ── METHOD 2: Auto-Read PDFs ──
            if (!pdfParse) return res.status(500).json({ error: 'PDF reading not available on this server. Please use Excel + PDFs method.' });
            job.total = pdfFiles.length;

            for (let i = 0; i < pdfFiles.length; i++) {
                const pf = pdfFiles[i];
                const originalName = pf.originalname || `file_${i}.pdf`;
                try {
                    const parsed = await pdfParse(pf.buffer, { max: 2 });
                    const text = parsed.text || '';
                    const telNumbers = extractTelFromPdf(text);

                    if (!telNumbers.length) {
                        job.rows.push({ originalPdf: originalName, status: 'no_tel', fileStatus: 'no_tel',
                            error: 'No telephone number found in PDF' });
                        job.notFound++;
                        continue;
                    }

                    let matched = false;
                    for (const telNo of telNumbers) {
                        const telNoDash = telNo.replace(/-/g, '');
                        const [rows2] = await pool.query(
                            `SELECT c.customer_name, COALESCE(c.acc_person_email, c.email_id) AS email
                             FROM customers c
                             INNER JOIN customer_lines cl ON cl.customer_id = c.id
                             WHERE cl.telephone_number = ?
                                OR CONCAT(cl.telephone_code, cl.telephone_number) = ?
                                OR REPLACE(cl.telephone_number, '-', '') = ?
                             LIMIT 1`,
                            [telNo, telNo, telNoDash]
                        );
                        if (rows2.length > 0) {
                            const cust = rows2[0];
                            const nameClean = cust.customer_name.replace(/[/\\?%*:|"<>&]/g,'_').replace(/\s+/g,'_').replace(/_+/g,'_').replace(/^_|_$/g,'');
                            const telClean = telNo.replace(/[/\\?%*:|"<>&]/g,'-');
                            job.rows.push({
                                telNo, customerName: cust.customer_name, email: cust.email||'',
                                filename: `${nameClean}_${telClean}.pdf`,
                                originalPdf: originalName,
                                pdfBuffer: pf.buffer,
                                status: 'matched', fileStatus: 'found'
                            });
                            job.matched++;
                            job.renamed++;
                            matched = true;
                            break;
                        }
                    }
                    if (!matched) {
                        job.rows.push({ originalPdf: originalName, telNo: telNumbers[0]||'',
                            status: 'not_in_db', fileStatus: 'not_in_db',
                            error: 'Tel ' + telNumbers[0] + ' not found in customer DB' });
                        job.notFound++;
                    }
                } catch(e) {
                    job.rows.push({ originalPdf: originalName, status: 'error', fileStatus: 'error',
                        error: 'PDF read error: ' + e.message });
                    job.notFound++;
                }
            }
        }

        // Step 2: Create ZIP of renamed PDFs
        const tempDir = path.join(os.tmpdir(), `bulk_${jobId}`);
        fs.mkdirSync(tempDir, { recursive: true });

        const renamedRows = job.rows.filter(r => r.fileStatus === 'found' && r.pdfBuffer);
        if (renamedRows.length > 0) {
            for (const r of renamedRows) {
                fs.writeFileSync(path.join(tempDir, r.filename), r.pdfBuffer);
            }

            const zipPath = path.join(os.tmpdir(), `Coral_Bills_${jobId}.zip`);
            await new Promise((resolve, reject) => {
                const output = fs.createWriteStream(zipPath);
                const archive = archiver('zip', { zlib: { level: 6 } });
                output.on('close', resolve);
                archive.on('error', reject);
                archive.pipe(output);
                for (const r of renamedRows) {
                    archive.file(path.join(tempDir, r.filename), { name: r.filename });
                }
                archive.finalize();
            });
            job.zipPath = zipPath;
            job.zipReady = true;
            bulkPdfJobs.set(jobId, job);
            setTimeout(() => {
                try { fs.rmSync(tempDir, { recursive: true, force: true }); } catch(e) {}
                try { if (job.zipPath) fs.unlinkSync(job.zipPath); } catch(e) {}
                bulkPdfJobs.delete(jobId);
            }, 1800000);
        } else {
            try { fs.rmSync(tempDir, { recursive: true, force: true }); } catch(e) {}
        }

        // Clear pdfBuffer from response (too large)
        const rowSummary = job.rows.map(r => ({
            telNo: r.telNo, customerName: r.customerName||'', filename: r.filename||'',
            email: r.email||'', status: r.status, fileStatus: r.fileStatus,
            originalPdf: r.originalPdf||'', error: r.error||''
        }));
        res.json({
            success: true, jobId, status: 'done',
            total: job.total, matched: job.matched, notFound: job.notFound,
            renamed: job.renamed,
            zipReady: job.zipReady,
            pdfCount: pdfFiles.length,
            rowSummary
        });
    } catch(e) {
        res.status(500).json({ error: 'Job failed: ' + e.message });
    }
});

// ── BULK PDF JOB: DOWNLOAD ZIP (supports ?token= for browser download) ───────
app.get('/api/bulk-pdf-job/:jobId/zip', (req, res, next) => {
    // Allow token via query string for direct browser download (window.open)
    if (!req.headers['authorization'] && req.query.token) {
        req.headers['authorization'] = 'Bearer ' + req.query.token;
    }
    authenticateToken(req, res, next);
}, (req, res) => {
    const job = bulkPdfJobs.get(req.params.jobId);
    if (!job || !job.zipPath || !fs.existsSync(job.zipPath))
        return res.status(404).json({ error: 'ZIP not ready or expired.' });
    const zipName = `Coral_Bills_${new Date().toISOString().slice(0,10)}.zip`;
    res.setHeader('Content-Disposition', `attachment; filename="${zipName}"`);
    res.setHeader('Content-Type', 'application/zip');
    fs.createReadStream(job.zipPath).pipe(res);
});

// ══════════════════════════════════════════════════════════════════════════════
// ── BULK BILL SYSTEM: Excel upload → PDF download → Rename → ZIP → Email ──
// ══════════════════════════════════════════════════════════════════════════════

const bulkBillUpload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } }).single('excel');

// Auto-ensure bill_pdfs table exists before any bulk-bill endpoint
async function ensureBillPdfsTable() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS bill_pdfs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            batch_id VARCHAR(100),
            telephone_number VARCHAR(100),
            pdf_link TEXT,
            customer_id INT DEFAULT NULL,
            customer_name VARCHAR(255) DEFAULT NULL,
            circle VARCHAR(100) DEFAULT NULL,
            oa_name VARCHAR(100) DEFAULT NULL,
            email VARCHAR(150) DEFAULT NULL,
            renamed_filename VARCHAR(500),
            pdf_data LONGBLOB,
            status ENUM('pending','downloading','matched','not_found','error') DEFAULT 'pending',
            email_sent BOOLEAN DEFAULT FALSE,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB
    `);
}

// 1. Upload Excel → Parse → Insert rows into bill_pdfs
app.post('/api/bulk-bill/upload-excel', authenticateToken, (req, res, next) => {
    bulkBillUpload(req, res, (err) => {
        if (err) return res.status(400).json({ error: 'Upload error: ' + err.message });
        next();
    });
}, async (req, res) => {
    try {
        await ensureBillPdfsTable();
        if (!req.file) return res.status(400).json({ error: 'No Excel file uploaded.' });

        let workbook;
        try { workbook = xlsx.read(req.file.buffer, { type: 'buffer' }); }
        catch(e) { return res.status(400).json({ error: 'Invalid Excel file.' }); }

        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const allRows = xlsx.utils.sheet_to_json(sheet, { header: 1 });

        // Skip header row, filter valid rows (col0 = phone, col1 = link)
        const dataRows = [];
        for (let i = 0; i < allRows.length; i++) {
            const r = allRows[i];
            const phone = String(r[0] || '').trim();
            const link = String(r[1] || '').trim();
            if (!phone || !link) continue;
            if (i === 0 && (phone.toUpperCase().includes('PHONE') || phone.toUpperCase().includes('TEL'))) continue;
            if (!link.startsWith('http')) continue;
            dataRows.push({ phone, link });
        }

        if (!dataRows.length) return res.status(400).json({ error: 'No valid rows found in Excel.' });

        const batchId = `bill_${Date.now()}`;

        // Delete old data
        await pool.query('DELETE FROM bill_pdfs');

        // Insert new rows
        const values = dataRows.map(r => [batchId, r.phone, r.link, 'pending']);
        const placeholders = values.map(() => '(?,?,?,?)').join(',');
        const flat = values.flat();
        await pool.query(
            `INSERT INTO bill_pdfs (batch_id, telephone_number, pdf_link, status) VALUES ${placeholders}`,
            flat
        );

        res.json({ success: true, batchId, total: dataRows.length, inserted: dataRows.length });
    } catch(e) {
        res.status(500).json({ error: 'Upload failed: ' + e.message });
    }
});

// 2. Process batch — download PDFs via cPanel proxy, match customer, save BLOB
app.post('/api/bulk-bill/process-batch', authenticateToken, async (req, res) => {
    try {
        // Pick 10 pending rows and process in parallel
        const [pending] = await pool.query(
            `SELECT id, telephone_number, pdf_link FROM bill_pdfs WHERE status='pending' LIMIT 10`
        );
        if (!pending.length) {
            const [counts] = await pool.query(
                `SELECT status, COUNT(*) as cnt FROM bill_pdfs GROUP BY status`
            );
            const stats = {};
            counts.forEach(r => { stats[r.status] = r.cnt; });
            return res.json({ processed: 0, remaining: 0, done: true, stats });
        }

        // Mark all as downloading
        const ids = pending.map(r => r.id);
        await pool.query(`UPDATE bill_pdfs SET status='downloading' WHERE id IN (?)`, [ids]);

        // Process all rows in PARALLEL (simultaneous downloads)
        const results = await Promise.allSettled(pending.map(async (row) => {
            try {
                // Download PDF from BSNL link
                let pdfBuffer;
                try {
                    pdfBuffer = await downloadPdfBuffer(row.pdf_link);
                } catch(dlErr) {
                    await pool.query(
                        `UPDATE bill_pdfs SET status='error', error_message=? WHERE id=?`,
                        ['Download failed: ' + dlErr.message, row.id]
                    );
                    return 'error';
                }

                // Match telephone with customer database
                const telNo = row.telephone_number;
                const telNoDash = telNo.replace(/-/g, '');
                const [custRows] = await pool.query(
                    `SELECT c.id as customer_id, c.customer_name, c.circle, c.oa_name,
                            COALESCE(c.acc_person_email, c.email_id) AS email
                     FROM customers c
                     INNER JOIN customer_lines cl ON cl.customer_id = c.id
                     WHERE cl.telephone_number = ?
                        OR CONCAT(cl.telephone_code, cl.telephone_number) = ?
                        OR REPLACE(cl.telephone_number, '-', '') = ?
                        OR cl.telephone_number = ?
                     LIMIT 1`,
                    [telNo, telNo, telNoDash, telNoDash]
                );

                if (custRows.length > 0) {
                    const c = custRows[0];
                    const nameClean = (c.customer_name || '').replace(/[/\\?%*:|"<>&]/g, '_').replace(/\s+/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '');
                    const telClean = telNo.replace(/[/\\?%*:|"<>&]/g, '-');
                    const circleClean = (c.circle || '').replace(/[/\\?%*:|"<>&]/g, '_').replace(/\s+/g, '_');
                    const oaClean = (c.oa_name || '').replace(/[/\\?%*:|"<>&]/g, '_').replace(/\s+/g, '_');
                    const filename = `${nameClean}_${telClean}_${circleClean}_${oaClean}.pdf`;

                    await pool.query(
                        `UPDATE bill_pdfs SET status='matched', customer_id=?, customer_name=?, circle=?,
                         oa_name=?, email=?, renamed_filename=?, pdf_data=? WHERE id=?`,
                        [c.customer_id, c.customer_name, c.circle, c.oa_name, c.email, filename, pdfBuffer, row.id]
                    );
                } else {
                    const filename = `${telNo.replace(/[/\\?%*:|"<>&]/g, '-')}.pdf`;
                    await pool.query(
                        `UPDATE bill_pdfs SET status='not_found', renamed_filename=?, pdf_data=? WHERE id=?`,
                        [filename, pdfBuffer, row.id]
                    );
                }
                return 'ok';
            } catch(rowErr) {
                await pool.query(
                    `UPDATE bill_pdfs SET status='error', error_message=? WHERE id=?`,
                    ['Process error: ' + rowErr.message, row.id]
                );
                return 'error';
            }
        }));

        const processed = results.filter(r => r.status === 'fulfilled' && r.value === 'ok').length;

        // Get remaining count
        const [[{ cnt: remaining }]] = await pool.query(
            `SELECT COUNT(*) as cnt FROM bill_pdfs WHERE status='pending'`
        );

        res.json({ processed, remaining, done: remaining === 0 });
    } catch(e) {
        res.status(500).json({ error: 'Batch failed: ' + e.message });
    }
});

// 3. Status — progress counts
app.get('/api/bulk-bill/status', authenticateToken, async (req, res) => {
    try {
        const [counts] = await pool.query(
            `SELECT status, COUNT(*) as cnt FROM bill_pdfs GROUP BY status`
        );
        const stats = { total: 0, pending: 0, downloading: 0, matched: 0, not_found: 0, error: 0 };
        counts.forEach(r => { stats[r.status] = r.cnt; stats.total += r.cnt; });

        // Email stats
        const [[emailStats]] = await pool.query(
            `SELECT COUNT(*) as total, SUM(email_sent) as sent
             FROM bill_pdfs WHERE status='matched' AND email IS NOT NULL AND email != ''`
        );

        // Get sample error messages for debugging
        const [errors] = await pool.query(
            `SELECT error_message, COUNT(*) as cnt FROM bill_pdfs WHERE status='error' AND error_message IS NOT NULL GROUP BY error_message LIMIT 5`
        );

        res.json({ ...stats, emailTotal: emailStats.total || 0, emailSent: emailStats.sent || 0, errors });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// 4. Download ZIP — all PDFs (matched + not_found)
app.get('/api/bulk-bill/download-zip', (req, res, next) => {
    if (!req.headers['authorization'] && req.query.token) {
        req.headers['authorization'] = 'Bearer ' + req.query.token;
    }
    authenticateToken(req, res, next);
}, async (req, res) => {
    try {
        const [rows] = await pool.query(
            `SELECT renamed_filename, pdf_data FROM bill_pdfs WHERE pdf_data IS NOT NULL`
        );
        if (!rows.length) return res.status(404).json({ error: 'No PDFs available for download.' });

        const zipName = `Coral_Bills_${new Date().toISOString().slice(0, 10)}.zip`;
        res.setHeader('Content-Disposition', `attachment; filename="${zipName}"`);
        res.setHeader('Content-Type', 'application/zip');

        const archive = archiver('zip', { zlib: { level: 6 } });
        archive.on('error', (err) => { res.status(500).end(); });
        archive.pipe(res);

        for (const r of rows) {
            if (r.pdf_data && r.renamed_filename) {
                archive.append(r.pdf_data, { name: r.renamed_filename });
            }
        }
        archive.finalize();
    } catch(e) {
        res.status(500).json({ error: 'ZIP failed: ' + e.message });
    }
});

// 5. Send email batch — 3 at a time
app.post('/api/bulk-bill/send-email-batch', authenticateToken, async (req, res) => {
    try {
        const billingMonth = req.body.billing_month || new Date().toLocaleString('en', { month: 'long' });
        const billingYear = req.body.billing_year || new Date().getFullYear();
        const subject = req.body.subject || `BSNL PBX Bill — ${billingMonth} ${billingYear}`;
        const customMessage = req.body.message || '';

        const [pending] = await pool.query(
            `SELECT id, customer_name, email, renamed_filename, pdf_data, telephone_number
             FROM bill_pdfs
             WHERE status='matched' AND email_sent=false AND email IS NOT NULL AND email != ''
             LIMIT 3`
        );
        if (!pending.length) {
            return res.json({ sent: 0, remaining: 0, done: true });
        }

        let sent = 0;
        for (const row of pending) {
            try {
                const emails = row.email.split(',').map(e => e.trim()).filter(e => e);
                if (!emails.length) {
                    await pool.query(`UPDATE bill_pdfs SET email_sent=true WHERE id=?`, [row.id]);
                    continue;
                }

                const htmlBody = `
                    <div style="font-family:Arial,sans-serif;max-width:600px;">
                        <h2 style="color:#002d72;">BSNL PBX Bill — ${billingMonth} ${billingYear}</h2>
                        <p>Dear <strong>${row.customer_name || 'Customer'}</strong>,</p>
                        <p>Please find attached your BSNL PBX bill for <strong>${billingMonth} ${billingYear}</strong>
                           (Telephone: ${row.telephone_number}).</p>
                        ${customMessage ? '<p>' + customMessage + '</p>' : ''}
                        <hr style="border:none;border-top:1px solid #e2e8f0;margin:20px 0;">
                        <p style="color:#64748b;font-size:12px;">
                            Coral Infratel Pvt. Ltd.<br>
                            This is an automated email from CRM system.
                        </p>
                    </div>`;

                await billMailTransporter.sendMail({
                    from: '"Coral Infratel" <bsnlpribill@gmail.com>',
                    to: emails.join(','),
                    subject,
                    html: htmlBody,
                    attachments: row.pdf_data ? [{
                        filename: row.renamed_filename || 'bill.pdf',
                        content: row.pdf_data,
                        contentType: 'application/pdf'
                    }] : []
                });

                await pool.query(`UPDATE bill_pdfs SET email_sent=true WHERE id=?`, [row.id]);
                sent++;

                // Log to bill_email_log
                if (row.customer_id) {
                    try {
                        await pool.query(
                            `INSERT INTO bill_email_log (customer_id, billing_month, billing_year, email_to, sent_by, source) VALUES (?, ?, ?, ?, ?, 'bulk')`,
                            [row.customer_id, billingMonth, billingYear, emails.join(','), req.user.id]
                        );
                    } catch(logErr) { console.error('Bulk email log error:', logErr.message); }
                }
            } catch(mailErr) {
                await pool.query(
                    `UPDATE bill_pdfs SET error_message=? WHERE id=?`,
                    ['Email error: ' + mailErr.message, row.id]
                );
            }
        }

        const [[{ cnt: remaining }]] = await pool.query(
            `SELECT COUNT(*) as cnt FROM bill_pdfs
             WHERE status='matched' AND email_sent=false AND email IS NOT NULL AND email != ''`
        );

        res.json({ sent, remaining, done: remaining === 0 });
    } catch(e) {
        res.status(500).json({ error: 'Email batch failed: ' + e.message });
    }
});

// 6. Email status
app.get('/api/bulk-bill/email-status', authenticateToken, async (req, res) => {
    try {
        const [[stats]] = await pool.query(
            `SELECT COUNT(*) as total, SUM(email_sent) as sent
             FROM bill_pdfs WHERE status='matched' AND email IS NOT NULL AND email != ''`
        );
        res.json({ total: stats.total || 0, sent: stats.sent || 0, remaining: (stats.total || 0) - (stats.sent || 0) });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// ══════════════════════════════════════════════════════════════════════════════
// BILL TASKS API (Customer Bill Task / Claim submission)
// ══════════════════════════════════════════════════════════════════════════════

// Create bill task
app.post('/api/bill-tasks', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        const [result] = await pool.query(
            `INSERT INTO bill_tasks (
                bsnl_bill_no, bill_for_month, customer_name, customer_ph_no,
                from_date, to_date, rent_ip, rent_vas, rent_voice, rg_charges,
                fixed_charges, plan_charges, call_charges,
                other_debit_with_st, other_debit_wo_st, other_credit_with_st, other_credit_wo_st,
                total_bill, value_as_per_bill, gst, customer_id_field, claim_no, phone_no,
                net_bill_amount, days_of_month, days_of_plan,
                rental_share_claim, rg_share_claim, call_charges_revenue,
                modem_rental, claim_fixed_charges, total_charges, remarks,
                rev_fixed_per_month, rev_fixed_per_line, rev_fixed_per_vas,
                rev_rent_share_pct, rev_calling_share_pct, rev_rg_share_pct,
                gst_id, cgst, sgst, igst, circle_id, oa_id, created_by
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            [
                b.bsnl_bill_no || '', b.bill_for_month || '', b.customer_name || '', b.customer_ph_no || '',
                b.from_date || null, b.to_date || null,
                b.rent_ip || 0, b.rent_vas || 0, b.rent_voice || 0, b.rg_charges || 0,
                b.fixed_charges || 0, b.plan_charges || 0, b.call_charges || 0,
                b.other_debit_with_st || 0, b.other_debit_wo_st || 0,
                b.other_credit_with_st || 0, b.other_credit_wo_st || 0,
                b.total_bill || 0, b.value_as_per_bill || 0, b.gst || 0,
                b.customer_id_field || '', b.claim_no || '', b.phone_no || '',
                b.net_bill_amount || 0, b.days_of_month || 0, b.days_of_plan || 0,
                b.rental_share_claim || 0, b.rg_share_claim || 0, b.call_charges_revenue || 0,
                b.modem_rental || 0, b.claim_fixed_charges || 0, b.total_charges || 0,
                b.remarks || '',
                b.rev_fixed_per_month || 0, b.rev_fixed_per_line || 0, b.rev_fixed_per_vas || 0,
                b.rev_rent_share_pct || 0, b.rev_calling_share_pct || 0, b.rev_rg_share_pct || 0,
                b.gst_id || '', b.cgst || 0, b.sgst || 0, b.igst || 0,
                b.circle_id || null, b.oa_id || null,
                req.user.id
            ]
        );
        res.json({ success: true, id: result.insertId, message: 'Bill task created successfully.' });
    } catch(e) {
        res.status(500).json({ error: 'Failed to create bill task: ' + e.message });
    }
});

// List bill tasks (with search, sort, pagination)
app.get('/api/bill-tasks', authenticateToken, async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 50;
        const offset = (page - 1) * limit;
        const search = req.query.search || '';
        const sortBy = req.query.sortBy || 'created_at';
        const sortDir = (req.query.sortDir || 'DESC').toUpperCase() === 'ASC' ? 'ASC' : 'DESC';

        const allowedSorts = ['id','bsnl_bill_no','bill_for_month','customer_name','customer_ph_no',
            'total_bill','net_bill_amount','total_charges','created_at','circle_id','oa_id'];
        const safeSort = allowedSorts.includes(sortBy) ? sortBy : 'created_at';

        let where = '1=1';
        const params = [];
        if (search) {
            where += ` AND (bt.customer_name LIKE ? OR bt.bsnl_bill_no LIKE ? OR bt.customer_ph_no LIKE ? OR bt.claim_no LIKE ?)`;
            const s = '%' + search + '%';
            params.push(s, s, s, s);
        }
        if (req.query.circle_id) { where += ` AND bt.circle_id = ?`; params.push(req.query.circle_id); }
        if (req.query.oa_id) { where += ` AND bt.oa_id = ?`; params.push(req.query.oa_id); }
        if (req.query.customer_name) { where += ` AND bt.customer_name LIKE ?`; params.push('%' + req.query.customer_name + '%'); }

        const [[{ total }]] = await pool.query(`SELECT COUNT(*) as total FROM bill_tasks bt WHERE ${where}`, params);
        const [rows] = await pool.query(
            `SELECT bt.*, u.name as created_by_name,
                    bc.name as circle_name, bo.name as oa_name
             FROM bill_tasks bt
             LEFT JOIN users u ON u.id = bt.created_by
             LEFT JOIN bsnl_circles bc ON bc.id = bt.circle_id
             LEFT JOIN bsnl_oas bo ON bo.id = bt.oa_id
             WHERE ${where} ORDER BY bt.${safeSort} ${sortDir} LIMIT ? OFFSET ?`,
            [...params, limit, offset]
        );

        res.json({ data: rows, total, page, limit, totalPages: Math.ceil(total / limit) });
    } catch(e) {
        res.status(500).json({ error: 'Failed to fetch bill tasks: ' + e.message });
    }
});

// Get single bill task
app.get('/api/bill-tasks/:id', authenticateToken, async (req, res) => {
    try {
        const [[row]] = await pool.query(
            `SELECT bt.*, u.name as created_by_name FROM bill_tasks bt
             LEFT JOIN users u ON u.id = bt.created_by WHERE bt.id = ?`, [req.params.id]
        );
        if (!row) return res.status(404).json({ error: 'Bill task not found' });
        res.json(row);
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// Update bill task
app.put('/api/bill-tasks/:id', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        await pool.query(
            `UPDATE bill_tasks SET
                bsnl_bill_no=?, bill_for_month=?, customer_name=?, customer_ph_no=?,
                from_date=?, to_date=?, rent_ip=?, rent_vas=?, rent_voice=?, rg_charges=?,
                fixed_charges=?, plan_charges=?, call_charges=?,
                other_debit_with_st=?, other_debit_wo_st=?, other_credit_with_st=?, other_credit_wo_st=?,
                total_bill=?, value_as_per_bill=?, gst=?, customer_id_field=?, claim_no=?, phone_no=?,
                net_bill_amount=?, days_of_month=?, days_of_plan=?,
                rental_share_claim=?, rg_share_claim=?, call_charges_revenue=?,
                modem_rental=?, claim_fixed_charges=?, total_charges=?, remarks=?,
                rev_fixed_per_month=?, rev_fixed_per_line=?, rev_fixed_per_vas=?,
                rev_rent_share_pct=?, rev_calling_share_pct=?, rev_rg_share_pct=?,
                gst_id=?, cgst=?, sgst=?, igst=?, circle_id=?, oa_id=?
            WHERE id=?`,
            [
                b.bsnl_bill_no || '', b.bill_for_month || '', b.customer_name || '', b.customer_ph_no || '',
                b.from_date || null, b.to_date || null,
                b.rent_ip || 0, b.rent_vas || 0, b.rent_voice || 0, b.rg_charges || 0,
                b.fixed_charges || 0, b.plan_charges || 0, b.call_charges || 0,
                b.other_debit_with_st || 0, b.other_debit_wo_st || 0,
                b.other_credit_with_st || 0, b.other_credit_wo_st || 0,
                b.total_bill || 0, b.value_as_per_bill || 0, b.gst || 0,
                b.customer_id_field || '', b.claim_no || '', b.phone_no || '',
                b.net_bill_amount || 0, b.days_of_month || 0, b.days_of_plan || 0,
                b.rental_share_claim || 0, b.rg_share_claim || 0, b.call_charges_revenue || 0,
                b.modem_rental || 0, b.claim_fixed_charges || 0, b.total_charges || 0,
                b.remarks || '',
                b.rev_fixed_per_month || 0, b.rev_fixed_per_line || 0, b.rev_fixed_per_vas || 0,
                b.rev_rent_share_pct || 0, b.rev_calling_share_pct || 0, b.rev_rg_share_pct || 0,
                b.gst_id || '', b.cgst || 0, b.sgst || 0, b.igst || 0,
                b.circle_id || null, b.oa_id || null,
                req.params.id
            ]
        );
        res.json({ success: true, message: 'Bill task updated.' });
    } catch(e) {
        res.status(500).json({ error: 'Failed to update: ' + e.message });
    }
});

// Delete bill task
app.delete('/api/bill-tasks/:id', authenticateToken, async (req, res) => {
    try {
        await pool.query(`DELETE FROM bill_tasks WHERE id=?`, [req.params.id]);
        res.json({ success: true, message: 'Bill task deleted.' });
    } catch(e) {
        res.status(500).json({ error: 'Failed to delete: ' + e.message });
    }
});

// ══════════════════════════════════════════════════════════════════════════════
// GST MASTER API
// ══════════════════════════════════════════════════════════════════════════════

app.post('/api/gst-master', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        const [result] = await pool.query(
            `INSERT INTO gst_master (gst_id, cgst, sgst, igst, created_by) VALUES (?,?,?,?,?)`,
            [b.gst_id || '', b.cgst || 0, b.sgst || 0, b.igst || 0, req.user.id]
        );
        res.json({ success: true, id: result.insertId, message: 'GST entry created.' });
    } catch(e) {
        res.status(500).json({ error: 'Failed: ' + e.message });
    }
});

app.get('/api/gst-master', authenticateToken, async (req, res) => {
    try {
        const search = req.query.search || '';
        const sortBy = req.query.sortBy || 'created_at';
        const sortDir = (req.query.sortDir || 'DESC').toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
        const allowed = ['id','gst_id','cgst','sgst','igst','created_at'];
        const safeSort = allowed.includes(sortBy) ? sortBy : 'created_at';

        let where = '1=1';
        const params = [];
        if (search) {
            where += ` AND gst_id LIKE ?`;
            params.push('%' + search + '%');
        }

        const [[{ total }]] = await pool.query(`SELECT COUNT(*) as total FROM gst_master WHERE ${where}`, params);
        const [rows] = await pool.query(
            `SELECT g.*, u.name as created_by_name FROM gst_master g
             LEFT JOIN users u ON u.id = g.created_by
             WHERE ${where} ORDER BY ${safeSort} ${sortDir}`, params
        );
        res.json({ data: rows, total });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

app.put('/api/gst-master/:id', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        await pool.query(
            `UPDATE gst_master SET gst_id=?, cgst=?, sgst=?, igst=? WHERE id=?`,
            [b.gst_id || '', b.cgst || 0, b.sgst || 0, b.igst || 0, req.params.id]
        );
        res.json({ success: true, message: 'GST entry updated.' });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

app.delete('/api/gst-master/:id', authenticateToken, async (req, res) => {
    try {
        await pool.query(`DELETE FROM gst_master WHERE id=?`, [req.params.id]);
        res.json({ success: true, message: 'GST entry deleted.' });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// ═══════════════════════════════════════════════════════
// REVENUE LEVELS — Master Module
// ═══════════════════════════════════════════════════════

app.post('/api/revenue-levels', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        if (!b.name || !b.name.trim()) return res.status(400).json({ error: 'Revenue Level Name is required.' });
        const [result] = await pool.query(
            `INSERT INTO revenue_levels (name, fixed_per_month, fixed_per_line, fixed_per_vas, rent_share_pct, calling_share_pct, rg_share_pct, created_by)
             VALUES (?,?,?,?,?,?,?,?)`,
            [b.name.trim(), b.fixed_per_month||0, b.fixed_per_line||0, b.fixed_per_vas||0,
             b.rent_share_pct||0, b.calling_share_pct||0, b.rg_share_pct||0, req.user.id]
        );
        res.json({ success: true, id: result.insertId, message: 'Revenue Level created.' });
    } catch(e) {
        res.status(500).json({ error: 'Failed: ' + e.message });
    }
});

app.get('/api/revenue-levels', authenticateToken, async (req, res) => {
    try {
        const search = req.query.search || '';
        const sortBy = req.query.sortBy || 'created_at';
        const sortDir = (req.query.sortDir || 'DESC').toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
        const allowed = ['id','name','fixed_per_month','fixed_per_line','fixed_per_vas','rent_share_pct','calling_share_pct','rg_share_pct','created_at'];
        const safeSort = allowed.includes(sortBy) ? sortBy : 'created_at';

        let where = '1=1';
        const params = [];
        if (search) {
            where += ` AND name LIKE ?`;
            params.push('%' + search + '%');
        }

        const [[{ total }]] = await pool.query(`SELECT COUNT(*) as total FROM revenue_levels WHERE ${where}`, params);
        const [rows] = await pool.query(
            `SELECT rl.*, u.name as created_by_name FROM revenue_levels rl
             LEFT JOIN users u ON u.id = rl.created_by
             WHERE ${where} ORDER BY ${safeSort} ${sortDir}`, params
        );
        res.json({ data: rows, total });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

app.put('/api/revenue-levels/:id', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        if (!b.name || !b.name.trim()) return res.status(400).json({ error: 'Revenue Level Name is required.' });
        await pool.query(
            `UPDATE revenue_levels SET name=?, fixed_per_month=?, fixed_per_line=?, fixed_per_vas=?,
             rent_share_pct=?, calling_share_pct=?, rg_share_pct=? WHERE id=?`,
            [b.name.trim(), b.fixed_per_month||0, b.fixed_per_line||0, b.fixed_per_vas||0,
             b.rent_share_pct||0, b.calling_share_pct||0, b.rg_share_pct||0, req.params.id]
        );
        res.json({ success: true, message: 'Revenue Level updated.' });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

app.delete('/api/revenue-levels/:id', authenticateToken, async (req, res) => {
    try {
        await pool.query(`DELETE FROM revenue_levels WHERE id=?`, [req.params.id]);
        res.json({ success: true, message: 'Revenue Level deleted.' });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// ═══════════════════════════════════════════════════════
// MODEM SCHEMES — Master Module
// ═══════════════════════════════════════════════════════

app.post('/api/modem-schemes', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        if (!b.name || !b.name.trim()) return res.status(400).json({ error: 'Scheme Name is required.' });
        const [result] = await pool.query(
            `INSERT INTO modem_schemes (name, modem_rental, revenue_per_month, remarks, created_by)
             VALUES (?, ?, ?, ?, ?)`,
            [b.name.trim(), b.modem_rental || 0, b.revenue_per_month || 0, b.remarks || null, req.user.id]
        );
        res.json({ success: true, id: result.insertId, message: 'Modem Scheme created.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/modem-schemes', authenticateToken, async (req, res) => {
    try {
        const { search, sortBy = 'created_at', sortDir = 'DESC' } = req.query;
        const allowed = ['id','name','modem_rental','revenue_per_month','created_at'];
        const col = allowed.includes(sortBy) ? sortBy : 'created_at';
        const dir = sortDir === 'ASC' ? 'ASC' : 'DESC';
        let where = '1=1', params = [];
        if (search) { where = `ms.name LIKE ?`; params.push(`%${search}%`); }
        const [[{ total }]] = await pool.query(`SELECT COUNT(*) as total FROM modem_schemes ms WHERE ${where}`, params);
        const [data] = await pool.query(
            `SELECT ms.*, u.name as created_by_name FROM modem_schemes ms
             LEFT JOIN users u ON ms.created_by = u.id
             WHERE ${where} ORDER BY ms.${col} ${dir}`, params
        );
        res.json({ total, data });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/modem-schemes/:id', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        if (!b.name || !b.name.trim()) return res.status(400).json({ error: 'Scheme Name is required.' });
        await pool.query(
            `UPDATE modem_schemes SET name=?, modem_rental=?, revenue_per_month=?, remarks=? WHERE id=?`,
            [b.name.trim(), b.modem_rental || 0, b.revenue_per_month || 0, b.remarks || null, req.params.id]
        );
        res.json({ success: true, message: 'Modem Scheme updated.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/modem-schemes/:id', authenticateToken, async (req, res) => {
    try {
        await pool.query(`DELETE FROM modem_schemes WHERE id=?`, [req.params.id]);
        res.json({ success: true, message: 'Modem Scheme deleted.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════
// BA LEVELS — BA/TSP Report Module
// ═══════════════════════════════════════════════════════

app.post('/api/ba-levels', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        // Auto-generate level_name: Level 1, Level 2, etc.
        const [[{ maxNum }]] = await pool.query(`SELECT COALESCE(MAX(id), 0) as maxNum FROM ba_levels`);
        const levelName = b.level_name && b.level_name.trim() ? b.level_name.trim() : `Level ${maxNum + 1}`;
        const [result] = await pool.query(
            `INSERT INTO ba_levels (level_name, revenue_share, revenue_share_rent, revenue_share_call, revenue_share_rng, revenue_share_modem, revenue_share_fixed, created_by)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [levelName, b.revenue_share || 0, b.revenue_share_rent || 0, b.revenue_share_call || 0,
             b.revenue_share_rng || 0, b.revenue_share_modem || 0, b.revenue_share_fixed || 0, req.user.id]
        );
        res.json({ success: true, id: result.insertId, level_name: levelName, message: 'BA Level created.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/ba-levels', authenticateToken, async (req, res) => {
    try {
        const { search, sortBy = 'id', sortDir = 'ASC' } = req.query;
        const allowed = ['id','level_name','revenue_share','revenue_share_rent','revenue_share_call','revenue_share_rng','revenue_share_modem','revenue_share_fixed','created_at'];
        const col = allowed.includes(sortBy) ? sortBy : 'id';
        const dir = sortDir === 'ASC' ? 'ASC' : 'DESC';
        let where = '1=1', params = [];
        if (search) { where = `bl.level_name LIKE ?`; params.push(`%${search}%`); }
        const [[{ total }]] = await pool.query(`SELECT COUNT(*) as total FROM ba_levels bl WHERE ${where}`, params);
        const [data] = await pool.query(
            `SELECT bl.*, u.name as created_by_name FROM ba_levels bl
             LEFT JOIN users u ON bl.created_by = u.id
             WHERE ${where} ORDER BY bl.${col} ${dir}`, params
        );
        res.json({ total, data });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/ba-levels/:id', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        await pool.query(
            `UPDATE ba_levels SET level_name=?, revenue_share=?, revenue_share_rent=?, revenue_share_call=?,
             revenue_share_rng=?, revenue_share_modem=?, revenue_share_fixed=? WHERE id=?`,
            [b.level_name, b.revenue_share || 0, b.revenue_share_rent || 0, b.revenue_share_call || 0,
             b.revenue_share_rng || 0, b.revenue_share_modem || 0, b.revenue_share_fixed || 0, req.params.id]
        );
        res.json({ success: true, message: 'BA Level updated.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/ba-levels/:id', authenticateToken, async (req, res) => {
    try {
        await pool.query(`DELETE FROM ba_levels WHERE id=?`, [req.params.id]);
        res.json({ success: true, message: 'BA Level deleted.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════
// BA/TSP MASTER — Add BA/TSP Module
// ═══════════════════════════════════════════════════════

app.post('/api/ba-tsp', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        if (!b.name || !b.name.trim()) return res.status(400).json({ error: 'Name is required.' });
        if (!b.type || !['BA','TSP'].includes(b.type)) return res.status(400).json({ error: 'Type must be BA or TSP.' });
        const [result] = await pool.query(
            `INSERT INTO ba_tsp_master (name, type, contact_person, phone, email, gst_no, pan_no, address, created_by)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [b.name.trim(), b.type, b.contact_person || null, b.phone || null, b.email || null,
             b.gst_no || null, b.pan_no || null, b.address || null, req.user.id]
        );
        res.json({ success: true, id: result.insertId, message: 'BA/TSP created.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/ba-tsp', authenticateToken, async (req, res) => {
    try {
        const { search, type, sortBy = 'created_at', sortDir = 'DESC' } = req.query;
        const allowed = ['id','name','type','contact_person','gst_no','pan_no','created_at'];
        const col = allowed.includes(sortBy) ? sortBy : 'created_at';
        const dir = sortDir === 'ASC' ? 'ASC' : 'DESC';
        let where = '1=1', params = [];
        if (search) { where += ` AND (bt.name LIKE ? OR bt.gst_no LIKE ? OR bt.pan_no LIKE ?)`; params.push(`%${search}%`,`%${search}%`,`%${search}%`); }
        if (type && ['BA','TSP'].includes(type)) { where += ` AND bt.type = ?`; params.push(type); }
        const [[{ total }]] = await pool.query(`SELECT COUNT(*) as total FROM ba_tsp_master bt WHERE ${where}`, params);
        const [[{ ba_count }]] = await pool.query(`SELECT COUNT(*) as ba_count FROM ba_tsp_master WHERE type='BA'`);
        const [[{ tsp_count }]] = await pool.query(`SELECT COUNT(*) as tsp_count FROM ba_tsp_master WHERE type='TSP'`);
        const [data] = await pool.query(
            `SELECT bt.*, u.name as created_by_name FROM ba_tsp_master bt
             LEFT JOIN users u ON bt.created_by = u.id
             WHERE ${where} ORDER BY bt.${col} ${dir}`, params
        );
        res.json({ total, ba_count, tsp_count, data });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/ba-tsp/:id', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        if (!b.name || !b.name.trim()) return res.status(400).json({ error: 'Name is required.' });
        if (!b.type || !['BA','TSP'].includes(b.type)) return res.status(400).json({ error: 'Type must be BA or TSP.' });
        await pool.query(
            `UPDATE ba_tsp_master SET name=?, type=?, contact_person=?, phone=?, email=?, gst_no=?, pan_no=?, address=? WHERE id=?`,
            [b.name.trim(), b.type, b.contact_person || null, b.phone || null, b.email || null,
             b.gst_no || null, b.pan_no || null, b.address || null, req.params.id]
        );
        res.json({ success: true, message: 'BA/TSP updated.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/ba-tsp/:id', authenticateToken, async (req, res) => {
    try {
        await pool.query(`DELETE FROM ba_tsp_master WHERE id=?`, [req.params.id]);
        res.json({ success: true, message: 'BA/TSP deleted.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════
// BSNL STAFF CONTACTS — Task & Other Module
// ═══════════════════════════════════════════════════════

app.post('/api/bsnl-staff-contacts', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        if (!b.name || !b.name.trim()) return res.status(400).json({ error: 'Staff Name is required.' });
        if (!b.designation || !b.designation.trim()) return res.status(400).json({ error: 'Designation is required.' });
        const [result] = await pool.query(
            `INSERT INTO bsnl_staff_contacts (name, designation, section, circle, oa, mobile, landline, email, email2, dob, anniversary, remarks, created_by)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [b.name.trim(), b.designation.trim(), b.section || null, b.circle || null, b.oa || null,
             b.mobile || null, b.landline || null, b.email || null, b.email2 || null,
             b.dob || null, b.anniversary || null, b.remarks || null, req.user.id]
        );
        res.json({ success: true, id: result.insertId, message: 'Staff contact saved.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/bsnl-staff-contacts', authenticateToken, async (req, res) => {
    try {
        const { search, circle, sortBy = 'name', sortDir = 'ASC' } = req.query;
        const allowed = ['id','name','designation','section','circle','oa','dob','anniversary','created_at'];
        const col = allowed.includes(sortBy) ? sortBy : 'name';
        const dir = sortDir === 'ASC' ? 'ASC' : 'DESC';
        let where = '1=1', params = [];
        if (search) { where += ` AND (sc.name LIKE ? OR sc.designation LIKE ? OR sc.section LIKE ? OR sc.mobile LIKE ?)`; params.push(`%${search}%`,`%${search}%`,`%${search}%`,`%${search}%`); }
        if (circle) { where += ` AND sc.circle = ?`; params.push(circle); }
        const [[{ total }]] = await pool.query(`SELECT COUNT(*) as total FROM bsnl_staff_contacts sc WHERE ${where}`, params);
        const [data] = await pool.query(
            `SELECT sc.*, u.name as created_by_name FROM bsnl_staff_contacts sc
             LEFT JOIN users u ON sc.created_by = u.id
             WHERE ${where} ORDER BY sc.${col} ${dir}`, params
        );
        res.json({ total, data });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/bsnl-staff-contacts/:id', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        if (!b.name || !b.name.trim()) return res.status(400).json({ error: 'Staff Name is required.' });
        await pool.query(
            `UPDATE bsnl_staff_contacts SET name=?, designation=?, section=?, circle=?, oa=?, mobile=?, landline=?, email=?, email2=?, dob=?, anniversary=?, remarks=? WHERE id=?`,
            [b.name.trim(), b.designation || null, b.section || null, b.circle || null, b.oa || null,
             b.mobile || null, b.landline || null, b.email || null, b.email2 || null,
             b.dob || null, b.anniversary || null, b.remarks || null, req.params.id]
        );
        res.json({ success: true, message: 'Staff contact updated.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/bsnl-staff-contacts/:id', authenticateToken, async (req, res) => {
    try {
        await pool.query(`DELETE FROM bsnl_staff_contacts WHERE id=?`, [req.params.id]);
        res.json({ success: true, message: 'Staff contact deleted.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════
// MANUAL COLLECTIONS — Sub Master Module
// ═══════════════════════════════════════════════════════

app.post('/api/manual-collections', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        if (!b.customer_name || !b.customer_name.trim()) return res.status(400).json({ error: 'Customer Name is required.' });
        if (!b.collection_date) return res.status(400).json({ error: 'Collection Date is required.' });
        if (!b.payment_mode) return res.status(400).json({ error: 'Payment Mode is required.' });
        if (!b.amount || b.amount <= 0) return res.status(400).json({ error: 'Amount must be greater than 0.' });
        const [result] = await pool.query(
            `INSERT INTO manual_collections (customer_name, collection_date, payment_mode, amount, receipt_no, cheque_no, bank_name, remarks, created_by)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [b.customer_name.trim(), b.collection_date, b.payment_mode, b.amount,
             b.receipt_no || null, b.cheque_no || null, b.bank_name || null, b.remarks || null, req.user.id]
        );
        res.json({ success: true, id: result.insertId, message: 'Collection saved.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/manual-collections', authenticateToken, async (req, res) => {
    try {
        const { search, mode, from, to, sortBy = 'collection_date', sortDir = 'DESC' } = req.query;
        const allowed = ['id','customer_name','collection_date','payment_mode','amount','created_at'];
        const col = allowed.includes(sortBy) ? sortBy : 'collection_date';
        const dir = sortDir === 'ASC' ? 'ASC' : 'DESC';
        let where = '1=1', params = [];
        if (search) { where += ` AND (mc.customer_name LIKE ? OR mc.receipt_no LIKE ?)`; params.push(`%${search}%`,`%${search}%`); }
        if (mode) { where += ` AND mc.payment_mode = ?`; params.push(mode); }
        if (from) { where += ` AND mc.collection_date >= ?`; params.push(from); }
        if (to) { where += ` AND mc.collection_date <= ?`; params.push(to); }
        const [[{ total }]] = await pool.query(`SELECT COUNT(*) as total FROM manual_collections mc WHERE ${where}`, params);
        const [[{ total_amount }]] = await pool.query(`SELECT COALESCE(SUM(amount),0) as total_amount FROM manual_collections mc WHERE ${where}`, params);
        const [data] = await pool.query(
            `SELECT mc.*, u.name as created_by_name FROM manual_collections mc
             LEFT JOIN users u ON mc.created_by = u.id
             WHERE ${where} ORDER BY mc.${col} ${dir}`, params
        );
        res.json({ total, total_amount, data });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/manual-collections/:id', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        if (!b.customer_name || !b.customer_name.trim()) return res.status(400).json({ error: 'Customer Name is required.' });
        await pool.query(
            `UPDATE manual_collections SET customer_name=?, collection_date=?, payment_mode=?, amount=?, receipt_no=?, cheque_no=?, bank_name=?, remarks=? WHERE id=?`,
            [b.customer_name.trim(), b.collection_date, b.payment_mode, b.amount,
             b.receipt_no || null, b.cheque_no || null, b.bank_name || null, b.remarks || null, req.params.id]
        );
        res.json({ success: true, message: 'Collection updated.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/manual-collections/:id', authenticateToken, async (req, res) => {
    try {
        await pool.query(`DELETE FROM manual_collections WHERE id=?`, [req.params.id]);
        res.json({ success: true, message: 'Collection deleted.' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════
// WORK LOG PRO — Personal Diary Task Module
// ═══════════════════════════════════════════════════════

async function initDiaryTasksTables() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS diary_tasks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                title VARCHAR(500) NOT NULL,
                description TEXT,
                category VARCHAR(100) DEFAULT 'General',
                priority ENUM('Low','Medium','High','Critical') DEFAULT 'Medium',
                task_type ENUM('Call','Meeting','Follow Up','Work','Personal','Other') DEFAULT 'Work',
                start_date DATE,
                due_date DATE NOT NULL,
                estimated_time VARCHAR(50),
                status ENUM('Pending','In Progress','Completed','Rescheduled','Cancelled','Closed') DEFAULT 'Pending',
                delay_reason TEXT,
                next_due_date DATE,
                reschedule_notes TEXT,
                notes TEXT,
                attachment_name VARCHAR(255),
                attachment_data LONGBLOB,
                source_type ENUM('manual','assigned') DEFAULT 'manual',
                source_task_id INT,
                assigned_by VARCHAR(255),
                completed_at DATETIME,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_user_date (user_id, due_date),
                INDEX idx_user_status (user_id, status)
            ) ENGINE=InnoDB
        `);
        await pool.query(`
            CREATE TABLE IF NOT EXISTS diary_task_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                task_id INT NOT NULL,
                previous_status VARCHAR(50),
                new_status VARCHAR(50),
                comment TEXT,
                changed_by INT,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_task (task_id)
            ) ENGINE=InnoDB
        `);
        // Add columns if missing (for existing installs)
        const newCols = [
            { name: 'task_type', type: "ENUM('Call','Meeting','Follow Up','Work','Personal','Other') DEFAULT 'Work'" },
            { name: 'reschedule_notes', type: 'TEXT' },
            { name: 'attachment_name', type: 'VARCHAR(255)' },
            { name: 'attachment_data', type: 'LONGBLOB' },
            { name: 'source_type', type: "ENUM('manual','assigned','excel','complaint','auto') DEFAULT 'manual'" },
            { name: 'source_task_id', type: 'INT' },
            { name: 'assigned_by', type: 'VARCHAR(255)' },
            { name: 'completed_at', type: 'DATETIME' }
        ];
        for (const col of newCols) {
            try { await pool.query(`ALTER TABLE diary_tasks ADD COLUMN ${col.name} ${col.type}`); } catch(e) {}
        }
        // Expand source_type ENUM for existing installs
        try { await pool.query(`ALTER TABLE diary_tasks MODIFY COLUMN source_type ENUM('manual','assigned','excel','complaint','auto') DEFAULT 'manual'`); } catch(e) {}
        console.log('✅ Diary tasks tables initialized.');
    } catch(err) {
        console.error('⚠️ Could not initialize diary tasks tables:', err.message);
    }
}

// --- Check pending previous-day diary tasks (for login enforcement) ---
app.get('/api/diary/pending-check', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const today = new Date(); today.setHours(0,0,0,0);
        const todayStr = today.toISOString().split('T')[0];

        // Auto-cancel tasks overdue by more than 2 days to prevent user lockout
        const twoDaysAgo = new Date(today); twoDaysAgo.setDate(twoDaysAgo.getDate() - 2);
        const twoDaysAgoStr = twoDaysAgo.toISOString().split('T')[0];
        await pool.query(
            `UPDATE diary_tasks SET status='Cancelled', delay_reason='Auto-cancelled: overdue by 2+ days'
             WHERE user_id = ? AND due_date < ? AND status IN ('Pending','In Progress')`,
            [userId, twoDaysAgoStr]
        );

        const [rows] = await pool.query(
            `SELECT id, title, due_date, priority, status FROM diary_tasks
             WHERE user_id = ? AND due_date < ? AND status IN ('Pending','In Progress')
             ORDER BY due_date ASC`,
            [userId, todayStr]
        );
        res.json({ hasPending: rows.length > 0, count: rows.length, tasks: rows });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- Bulk resolve pending tasks ---
app.post('/api/diary/bulk-resolve', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { resolutions } = req.body; // [{id, status, delay_reason, next_due_date}]
        if (!resolutions || !resolutions.length) return res.status(400).json({ error: 'No resolutions provided' });

        const conn = await pool.getConnection();
        try {
            await conn.beginTransaction();
            for (const r of resolutions) {
                const [task] = await conn.query('SELECT * FROM diary_tasks WHERE id = ? AND user_id = ?', [r.id, userId]);
                if (!task.length) continue;
                const oldStatus = task[0].status;
                const updates = { status: r.status };
                if (r.delay_reason) updates.delay_reason = r.delay_reason;
                if (r.next_due_date) updates.next_due_date = r.next_due_date;
                if (r.status === 'Completed') updates.completed_at = new Date();

                await conn.query('UPDATE diary_tasks SET ? WHERE id = ? AND user_id = ?', [updates, r.id, userId]);
                await conn.query('INSERT INTO diary_task_history (task_id, previous_status, new_status, comment, changed_by) VALUES (?,?,?,?,?)',
                    [r.id, oldStatus, r.status, r.delay_reason || 'Bulk resolved on login', userId]);

                // If rescheduled, create new task
                if (r.status === 'Rescheduled' && r.next_due_date) {
                    await conn.query(`INSERT INTO diary_tasks (user_id, title, description, category, priority, task_type, start_date, due_date, estimated_time, status, notes, source_type)
                        SELECT user_id, title, description, category, priority, task_type, ?, ?, estimated_time, 'Pending', CONCAT('Rescheduled from #', id), source_type FROM diary_tasks WHERE id = ?`,
                        [r.next_due_date, r.next_due_date, r.id]);
                }
            }
            await conn.commit();
            res.json({ success: true, resolved: resolutions.length });
        } catch(e) { await conn.rollback(); throw e; }
        finally { conn.release(); }
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- Create diary task ---
app.post('/api/diary/tasks', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { title, description, category, priority, task_type, start_date, due_date, estimated_time, notes } = req.body;
        if (!title || !due_date) return res.status(400).json({ error: 'Title and due date required' });

        // Date validation: start_date >= today, due_date >= start_date
        const today = new Date().toISOString().split('T')[0];
        const effectiveStart = start_date || today;
        const finalStart = effectiveStart < today ? today : effectiveStart;
        const finalDue = due_date < finalStart ? finalStart : due_date;

        const [result] = await pool.query(
            `INSERT INTO diary_tasks (user_id, title, description, category, priority, task_type, start_date, due_date, estimated_time, notes, source_type)
             VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
            [userId, title, description || null, category || 'General', priority || 'Medium', task_type || 'Work',
             finalStart, finalDue, estimated_time || null, notes || null, 'manual']
        );
        await pool.query('INSERT INTO diary_task_history (task_id, previous_status, new_status, comment, changed_by) VALUES (?,?,?,?,?)',
            [result.insertId, null, 'Pending', 'Task created', userId]);
        res.status(201).json({ success: true, id: result.insertId });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- Admin: Force-resolve all overdue diary tasks for a user ---
app.post('/api/diary/admin-resolve-overdue', authenticateToken, isAdmin, async (req, res) => {
    try {
        const { user_id } = req.body;
        if (!user_id) return res.status(400).json({ error: 'user_id required' });
        const today = new Date().toISOString().split('T')[0];
        const [rows] = await pool.query(
            `SELECT id, status FROM diary_tasks WHERE user_id = ? AND due_date < ? AND status IN ('Pending','In Progress')`,
            [user_id, today]
        );
        if (!rows.length) return res.json({ message: 'No overdue tasks found', count: 0 });
        for (const r of rows) {
            await pool.query(`UPDATE diary_tasks SET status='Cancelled', delay_reason='Auto-resolved by admin' WHERE id=?`, [r.id]);
            await pool.query('INSERT INTO diary_task_history (task_id, previous_status, new_status, comment, changed_by) VALUES (?,?,?,?,?)',
                [r.id, r.status, 'Cancelled', 'Auto-resolved by admin to unblock user', req.user.id]);
        }
        res.json({ message: `Resolved ${rows.length} overdue tasks`, count: rows.length });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- Get diary tasks (with filters) ---
app.get('/api/diary/tasks', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        let where = 'WHERE user_id = ?';
        let params = [userId];

        if (req.query.status) { where += ' AND status = ?'; params.push(req.query.status); }
        if (req.query.priority) { where += ' AND priority = ?'; params.push(req.query.priority); }
        if (req.query.category) { where += ' AND category = ?'; params.push(req.query.category); }
        if (req.query.task_type) { where += ' AND task_type = ?'; params.push(req.query.task_type); }
        if (req.query.date_from) { where += ' AND due_date >= ?'; params.push(req.query.date_from); }
        if (req.query.date_to) { where += ' AND due_date <= ?'; params.push(req.query.date_to); }
        if (req.query.search) { where += ' AND (title LIKE ? OR description LIKE ?)'; params.push('%'+req.query.search+'%', '%'+req.query.search+'%'); }

        // View modes
        if (req.query.view === 'today') {
            const today = new Date().toISOString().split('T')[0];
            where += ' AND due_date = ?'; params.push(today);
        } else if (req.query.view === 'overdue') {
            const today = new Date().toISOString().split('T')[0];
            where += " AND due_date < ? AND status IN ('Pending','In Progress')"; params.push(today);
        } else if (req.query.view === 'pending') {
            where += " AND status IN ('Pending','In Progress')";
        }

        const [rows] = await pool.query(
            `SELECT id, title, description, category, priority, task_type, start_date, due_date, estimated_time, status, delay_reason, next_due_date, reschedule_notes, notes, source_type, source_task_id, assigned_by, completed_at, created_at, updated_at,
             IF(attachment_data IS NOT NULL, attachment_name, NULL) as attachment_name
             FROM diary_tasks ${where} ORDER BY FIELD(status,'In Progress','Pending','Rescheduled','Completed','Closed','Cancelled'), due_date ASC`, params
        );
        res.json(rows);
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- Get single diary task ---
app.get('/api/diary/tasks/:id', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query(
            `SELECT id, title, description, category, priority, task_type, start_date, due_date, estimated_time, status, delay_reason, next_due_date, reschedule_notes, notes, source_type, source_task_id, assigned_by, completed_at, created_at, updated_at,
             IF(attachment_data IS NOT NULL, attachment_name, NULL) as attachment_name
             FROM diary_tasks WHERE id = ? AND user_id = ?`, [req.params.id, req.user.id]
        );
        if (!rows.length) return res.status(404).json({ error: 'Task not found' });
        res.json(rows[0]);
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- Update diary task ---
app.put('/api/diary/tasks/:id', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const taskId = req.params.id;
        const [existing] = await pool.query('SELECT * FROM diary_tasks WHERE id = ? AND user_id = ?', [taskId, userId]);
        if (!existing.length) return res.status(404).json({ error: 'Task not found' });

        const old = existing[0];
        const fields = ['title','description','category','priority','task_type','start_date','due_date','estimated_time','status','delay_reason','next_due_date','reschedule_notes','notes'];
        const updates = {};
        fields.forEach(f => { if (req.body[f] !== undefined) updates[f] = req.body[f]; });

        // Track status change
        if (updates.status && updates.status !== old.status) {
            if (updates.status === 'Completed') updates.completed_at = new Date();
            await pool.query('INSERT INTO diary_task_history (task_id, previous_status, new_status, comment, changed_by) VALUES (?,?,?,?,?)',
                [taskId, old.status, updates.status, req.body.delay_reason || req.body.notes || 'Status updated', userId]);

            // If rescheduled, create new task
            if (updates.status === 'Rescheduled' && req.body.next_due_date) {
                await pool.query(`INSERT INTO diary_tasks (user_id, title, description, category, priority, task_type, start_date, due_date, estimated_time, status, notes, source_type)
                    VALUES (?,?,?,?,?,?,?,?,'Pending',?,?)`,
                    [userId, old.title, old.description, old.category, old.priority, old.task_type, req.body.next_due_date, req.body.next_due_date, old.estimated_time,
                     'Rescheduled from #' + taskId, old.source_type]);
            }
        }

        if (Object.keys(updates).length > 0) {
            await pool.query('UPDATE diary_tasks SET ? WHERE id = ? AND user_id = ?', [updates, taskId, userId]);
        }
        res.json({ success: true });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- Delete diary task ---
app.delete('/api/diary/tasks/:id', authenticateToken, async (req, res) => {
    try {
        const [result] = await pool.query('DELETE FROM diary_tasks WHERE id = ? AND user_id = ?', [req.params.id, req.user.id]);
        if (result.affectedRows === 0) return res.status(404).json({ error: 'Task not found' });
        await pool.query('DELETE FROM diary_task_history WHERE task_id = ?', [req.params.id]);
        res.json({ success: true });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- Get task history ---
app.get('/api/diary/tasks/:id/history', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query(
            `SELECT h.*, u.name as changed_by_name FROM diary_task_history h LEFT JOIN users u ON h.changed_by = u.id WHERE h.task_id = ? ORDER BY h.changed_at DESC`,
            [req.params.id]
        );
        res.json(rows);
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- Add assigned task to diary ---
app.post('/api/diary/add-from-task/:taskId', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const [tasks] = await pool.query('SELECT * FROM tasks WHERE id = ?', [req.params.taskId]);
        if (!tasks.length) return res.status(404).json({ error: 'Source task not found' });
        const t = tasks[0];

        const [result] = await pool.query(
            `INSERT INTO diary_tasks (user_id, title, description, category, priority, task_type, start_date, due_date, status, source_type, source_task_id, assigned_by)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
            [userId, t.title, t.description, t.category || 'Assigned', t.priority || 'Medium', 'Work',
             new Date().toISOString().split('T')[0], t.due_date || new Date().toISOString().split('T')[0],
             'Pending', 'assigned', t.id, t.creator_name || ('User #' + t.created_by)]
        );
        await pool.query('INSERT INTO diary_task_history (task_id, previous_status, new_status, comment, changed_by) VALUES (?,?,?,?,?)',
            [result.insertId, null, 'Pending', 'Added from assigned task #' + t.id, userId]);
        res.status(201).json({ success: true, id: result.insertId });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// --- Excel Bulk Upload for diary tasks ---
app.post('/api/diary/bulk-upload', authenticateToken, isAdmin, pdfUpload.single('file'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
        const targetUserId = parseInt(req.body.user_id);
        const mode = req.body.mode || 'onetime'; // 'onetime' or 'recurring'
        if (!targetUserId) return res.status(400).json({ error: 'Target user_id is required' });

        // Verify target user exists
        const [userCheck] = await pool.query('SELECT id, name FROM users WHERE id = ?', [targetUserId]);
        if (!userCheck.length) return res.status(404).json({ error: 'Target user not found' });

        const XLSX = require('xlsx');
        const workbook = XLSX.read(req.file.buffer, { type: 'buffer' });
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const data = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });

        // Detect sections by scanning rows for keywords
        let currentSection = 'DAILY';
        const tasks = [];
        for (let i = 0; i < data.length; i++) {
            const row = data[i];
            const rowText = row.map(c => String(c).toUpperCase().trim()).join(' ');

            // Detect section headers (only when col A is NOT a valid serial number)
            const colA = row[0];
            const isDataRow = !isNaN(colA) && parseInt(colA) > 0 && String(row[1] || '').trim().length > 0;
            if (!isDataRow) {
                if (rowText.includes('DAILY') && (rowText.includes('TASK') || rowText.includes('REPORT'))) {
                    currentSection = 'DAILY'; continue;
                }
                if (rowText.includes('WEEKLY') && rowText.includes('TASK')) {
                    currentSection = 'WEEKLY'; continue;
                }
                if (rowText.includes('MONTHLY') && rowText.includes('TASK')) {
                    currentSection = 'MONTHLY'; continue;
                }
                if ((rowText.includes('QUARTERLY') || rowText.includes('QUATERLY')) && rowText.includes('TASK')) {
                    currentSection = 'QUARTERLY'; continue;
                }
            }

            // Skip header rows (Sr.No, WORK, Department...)
            if (rowText.includes('SR') && (rowText.includes('WORK') || rowText.includes('NO'))) continue;

            // Parse task rows: col A = serial number, col B = work text, col C = department
            const serial = row[0];
            const workText = String(row[1] || '').trim();
            const department = String(row[2] || '').trim();

            // Valid task row: has a serial number (numeric) and work text
            if (workText && !isNaN(serial) && parseInt(serial) > 0) {
                tasks.push({
                    title: workText,
                    department: department || 'General',
                    frequency: currentSection
                });
            }
        }

        if (!tasks.length) return res.status(400).json({ error: 'No valid tasks found in the Excel file' });

        // Calculate due dates
        const now = new Date();
        const today = now.toISOString().split('T')[0];
        function getDueDate(freq) {
            const d = new Date();
            if (freq === 'DAILY') return today;
            if (freq === 'WEEKLY') {
                // Next Friday
                const day = d.getDay();
                const daysUntilFri = (5 - day + 7) % 7 || 7;
                d.setDate(d.getDate() + daysUntilFri);
                return d.toISOString().split('T')[0];
            }
            if (freq === 'MONTHLY') {
                // End of current month
                d.setMonth(d.getMonth() + 1, 0);
                return d.toISOString().split('T')[0];
            }
            if (freq === 'QUARTERLY') {
                // End of current quarter
                const qMonth = Math.ceil((d.getMonth() + 1) / 3) * 3;
                const qEnd = new Date(d.getFullYear(), qMonth, 0);
                return qEnd.toISOString().split('T')[0];
            }
            return today;
        }

        const adminName = req.user.name || req.user.username;
        let inserted = 0;

        const conn = await pool.getConnection();
        try {
            await conn.beginTransaction();

            if (mode === 'onetime') {
                for (const t of tasks) {
                    const dueDate = getDueDate(t.frequency);
                    const [result] = await conn.query(
                        `INSERT INTO diary_tasks (user_id, title, description, category, priority, task_type, start_date, due_date, status, source_type, assigned_by)
                         VALUES (?, ?, ?, ?, ?, 'Work', ?, ?, 'Pending', 'excel', ?)`,
                        [targetUserId, t.title, t.frequency + ' task from Excel bulk upload', t.department, 'Medium', today, dueDate, adminName]
                    );
                    await conn.query('INSERT INTO diary_task_history (task_id, previous_status, new_status, comment, changed_by) VALUES (?,?,?,?,?)',
                        [result.insertId, null, 'Pending', 'Bulk uploaded from Excel (' + t.frequency + ')', req.user.id]);
                    inserted++;
                }
            } else {
                // Recurring mode: insert into recurring_tasks_templates + activity_user_tasks
                for (const t of tasks) {
                    const nextRun = getDueDate(t.frequency);
                    const freqMap = { DAILY: 'DAILY', WEEKLY: 'WEEKLY', MONTHLY: 'MONTHLY', QUARTERLY: 'QUARTERLY' };
                    const [result] = await conn.query(
                        `INSERT INTO recurring_tasks_templates (title, description, assigned_to, created_by, frequency, next_run_date, status)
                         VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')`,
                        [t.title, t.department + ' - ' + t.frequency + ' recurring task', userCheck[0].name, adminName, freqMap[t.frequency] || 'MONTHLY', nextRun]
                    );
                    // Also create initial diary task
                    const dueDate = getDueDate(t.frequency);
                    const [diaryResult] = await conn.query(
                        `INSERT INTO diary_tasks (user_id, title, description, category, priority, task_type, start_date, due_date, status, source_type, assigned_by)
                         VALUES (?, ?, ?, ?, ?, 'Work', ?, ?, 'Pending', 'excel', ?)`,
                        [targetUserId, t.title, t.frequency + ' recurring task from Excel', t.department, 'Medium', today, dueDate, adminName]
                    );
                    await conn.query('INSERT INTO diary_task_history (task_id, previous_status, new_status, comment, changed_by) VALUES (?,?,?,?,?)',
                        [diaryResult.insertId, null, 'Pending', 'Recurring task from Excel (' + t.frequency + ')', req.user.id]);
                    inserted++;
                }
            }

            await conn.commit();
            res.json({ success: true, message: inserted + ' tasks imported successfully', count: inserted, tasks: tasks });
        } catch(err) {
            await conn.rollback();
            throw err;
        } finally {
            conn.release();
        }
    } catch(e) {
        console.error('Bulk upload error:', e);
        res.status(500).json({ error: e.message });
    }
});

// --- Consolidated Report (Admin team-wide view) ---
app.get('/api/diary/consolidated-report', authenticateToken, isAdmin, async (req, res) => {
    try {
        const { user_id, start, end, search } = req.query;
        const today = new Date().toISOString().split('T')[0];

        let where = [];
        let params = [];

        if (user_id) { where.push('dt.user_id = ?'); params.push(parseInt(user_id)); }
        if (start) { where.push('dt.due_date >= ?'); params.push(start); }
        if (end) { where.push('dt.due_date <= ?'); params.push(end); }
        if (search) { where.push('(dt.title LIKE ? OR dt.description LIKE ?)'); params.push('%' + search + '%', '%' + search + '%'); }

        const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';

        // Summary totals
        const [[summary]] = await pool.query(
            `SELECT
                COUNT(*) as total,
                SUM(CASE WHEN dt.status IN ('Pending','In Progress') THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN dt.status IN ('Pending','In Progress') AND dt.due_date < ? THEN 1 ELSE 0 END) as overdue,
                SUM(CASE WHEN dt.status = 'Completed' THEN 1 ELSE 0 END) as completed
             FROM diary_tasks dt ${whereClause}`,
            [today, ...params]
        );

        // Per-user breakdown
        const [userRows] = await pool.query(
            `SELECT
                dt.user_id,
                u.name as user_name,
                COUNT(*) as total,
                SUM(CASE WHEN dt.status IN ('Pending','In Progress') THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN dt.status IN ('Pending','In Progress') AND dt.due_date < ? THEN 1 ELSE 0 END) as overdue,
                SUM(CASE WHEN dt.status = 'Completed' THEN 1 ELSE 0 END) as completed
             FROM diary_tasks dt
             LEFT JOIN users u ON dt.user_id = u.id
             ${whereClause}
             GROUP BY dt.user_id, u.name
             ORDER BY overdue DESC, pending DESC`,
            [today, ...params]
        );

        // Fetch tasks per user
        const [allTasks] = await pool.query(
            `SELECT dt.*, u.name as user_name
             FROM diary_tasks dt
             LEFT JOIN users u ON dt.user_id = u.id
             ${whereClause}
             ORDER BY dt.due_date DESC
             LIMIT 500`,
            params
        );

        // Group tasks by user
        const users = userRows.map(ur => ({
            user_id: ur.user_id,
            name: ur.user_name || 'Unknown',
            total: ur.total,
            pending: ur.pending,
            overdue: ur.overdue,
            completed: ur.completed,
            tasks: allTasks.filter(t => t.user_id === ur.user_id)
        }));

        res.json({
            summary: {
                total: summary.total || 0,
                pending: summary.pending || 0,
                overdue: summary.overdue || 0,
                completed: summary.completed || 0
            },
            users
        });
    } catch(e) {
        console.error('Consolidated report error:', e);
        res.status(500).json({ error: e.message });
    }
});

// --- Diary dashboard stats ---
app.get('/api/diary/stats', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const today = new Date().toISOString().split('T')[0];

        const [[todayStats]] = await pool.query(
            `SELECT
                COUNT(*) as total_today,
                SUM(CASE WHEN status IN ('Pending','In Progress') THEN 1 ELSE 0 END) as pending_today,
                SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed_today
             FROM diary_tasks WHERE user_id = ? AND due_date = ?`, [userId, today]
        );
        const [[overdueStats]] = await pool.query(
            `SELECT COUNT(*) as overdue FROM diary_tasks WHERE user_id = ? AND due_date < ? AND status IN ('Pending','In Progress')`, [userId, today]
        );
        const [[weekStats]] = await pool.query(
            `SELECT
                COUNT(*) as week_total,
                SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as week_completed
             FROM diary_tasks WHERE user_id = ? AND due_date >= DATE_SUB(?, INTERVAL 7 DAY) AND due_date <= ?`, [userId, today, today]
        );
        const [categoryBreakdown] = await pool.query(
            `SELECT category, COUNT(*) as count FROM diary_tasks WHERE user_id = ? AND due_date >= DATE_SUB(?, INTERVAL 30 DAY) GROUP BY category ORDER BY count DESC`, [userId, today]
        );
        const [priorityBreakdown] = await pool.query(
            `SELECT priority, COUNT(*) as count FROM diary_tasks WHERE user_id = ? AND status IN ('Pending','In Progress') GROUP BY priority`, [userId]
        );
        // 7-day trend
        const [trend] = await pool.query(
            `SELECT DATE(due_date) as date,
                COUNT(*) as total,
                SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status IN ('Pending','In Progress') AND due_date < ? THEN 1 ELSE 0 END) as overdue
             FROM diary_tasks WHERE user_id = ? AND due_date >= DATE_SUB(?, INTERVAL 7 DAY) AND due_date <= ?
             GROUP BY DATE(due_date) ORDER BY date`, [today, userId, today, today]
        );

        res.json({
            today: todayStats,
            overdue: overdueStats.overdue,
            week: weekStats,
            categoryBreakdown,
            priorityBreakdown,
            trend
        });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// ═══════════════════════════════════════════════════════════════
// ██  HR MODULE — Enterprise Workforce Intelligence System  ██
// ═══════════════════════════════════════════════════════════════

async function initializeHRTables() {
    const conn = await pool.getConnection();
    try {
        // Dynamic configuration table (no hardcoding)
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_config (
            id INT AUTO_INCREMENT PRIMARY KEY,
            config_key VARCHAR(100) NOT NULL,
            config_value JSON,
            category VARCHAR(50) DEFAULT 'general',
            description VARCHAR(255),
            updated_by INT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_key (config_key)
        )`);

        // Employee extended profile
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_employees (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            emp_code VARCHAR(20),
            designation VARCHAR(100),
            department VARCHAR(100),
            date_of_joining DATE,
            date_of_birth DATE,
            gender ENUM('Male','Female','Other'),
            phone VARCHAR(20),
            emergency_contact VARCHAR(20),
            address TEXT,
            city VARCHAR(50),
            state VARCHAR(50),
            blood_group VARCHAR(5),
            pan_number VARCHAR(20),
            aadhar_number VARCHAR(20),
            bank_name VARCHAR(100),
            bank_account VARCHAR(30),
            ifsc_code VARCHAR(15),
            base_salary DECIMAL(12,2) DEFAULT 0,
            status ENUM('Active','Inactive','Terminated','On Leave') DEFAULT 'Active',
            profile_photo VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_user (user_id),
            UNIQUE KEY uk_code (emp_code)
        )`);

        // GPS Logs
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_gps_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            latitude DECIMAL(10,8),
            longitude DECIMAL(11,8),
            accuracy FLOAT,
            address TEXT,
            log_type ENUM('auto','checkin','checkout','field_visit','lunch_start','lunch_end') DEFAULT 'auto',
            captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user_date (user_id, captured_at)
        )`);

        // Attendance
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_attendance (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            attendance_date DATE NOT NULL,
            check_in TIME,
            check_out TIME,
            check_in_lat DECIMAL(10,8),
            check_in_lng DECIMAL(11,8),
            check_in_address VARCHAR(255),
            check_out_lat DECIMAL(10,8),
            check_out_lng DECIMAL(11,8),
            check_out_address VARCHAR(255),
            total_hours DECIMAL(5,2) DEFAULT 0,
            overtime_hours DECIMAL(5,2) DEFAULT 0,
            status ENUM('Present','Absent','Half Day','Late','On Leave','Holiday','Week Off') DEFAULT 'Present',
            source ENUM('system','manual','gps','corrected') DEFAULT 'system',
            remarks VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_user_date (user_id, attendance_date)
        )`);

        // Leave Policy (dynamic)
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_leave_policy (
            id INT AUTO_INCREMENT PRIMARY KEY,
            leave_type VARCHAR(50) NOT NULL,
            total_days INT DEFAULT 0,
            carry_forward TINYINT(1) DEFAULT 0,
            max_carry INT DEFAULT 0,
            applicable_to VARCHAR(100) DEFAULT 'All',
            paid TINYINT(1) DEFAULT 1,
            description VARCHAR(255),
            is_active TINYINT(1) DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Leave Requests
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_leaves (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            leave_type VARCHAR(50),
            from_date DATE,
            to_date DATE,
            total_days DECIMAL(4,1),
            reason TEXT,
            status ENUM('Pending','Approved','Rejected','Cancelled') DEFAULT 'Pending',
            approved_by INT,
            approved_at DATETIME,
            rejection_reason VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user (user_id),
            INDEX idx_status (status)
        )`);

        // Expense Heads (dynamic)
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_expense_heads (
            id INT AUTO_INCREMENT PRIMARY KEY,
            head_name VARCHAR(100) NOT NULL,
            max_limit DECIMAL(10,2) DEFAULT 0,
            requires_receipt TINYINT(1) DEFAULT 1,
            applicable_roles VARCHAR(255) DEFAULT 'All',
            is_active TINYINT(1) DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Expenses
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_expenses (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            expense_date DATE,
            head_id INT,
            amount DECIMAL(10,2),
            description TEXT,
            receipt_url VARCHAR(255),
            task_id INT,
            status ENUM('Pending','Approved','Rejected','Paid') DEFAULT 'Pending',
            approved_by INT,
            approved_at DATETIME,
            rejection_reason VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user (user_id)
        )`);

        // Approval Workflow
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_approvals (
            id INT AUTO_INCREMENT PRIMARY KEY,
            module VARCHAR(50),
            record_id INT,
            level INT DEFAULT 1,
            approver_id INT,
            status ENUM('Pending','Approved','Rejected') DEFAULT 'Pending',
            comments TEXT,
            acted_at DATETIME,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Manual Reports (user-entered)
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_manual_reports (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            report_date DATE,
            check_in_time TIME,
            check_out_time TIME,
            location VARCHAR(255),
            work_summary TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_user_date (user_id, report_date)
        )`);

        // System Reports (auto-generated from GPS)
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_system_reports (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            report_date DATE,
            first_location_time TIME,
            last_location_time TIME,
            total_locations INT DEFAULT 0,
            distance_km DECIMAL(8,2) DEFAULT 0,
            office_time_mins INT DEFAULT 0,
            field_time_mins INT DEFAULT 0,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_user_date (user_id, report_date)
        )`);

        // Comparison Results
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_comparison_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            comparison_date DATE,
            time_mismatch_mins INT DEFAULT 0,
            location_mismatch TINYINT(1) DEFAULT 0,
            severity ENUM('None','Low','Medium','High','Critical') DEFAULT 'None',
            details JSON,
            auto_action VARCHAR(100),
            reviewed_by INT,
            reviewed_at DATETIME,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Error Logs (Self-Healing)
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_error_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            module VARCHAR(50),
            error_type ENUM('logic','data_mismatch','gps_inconsistency','api_failure','validation','workflow_break') DEFAULT 'logic',
            severity ENUM('Low','Medium','High','Critical') DEFAULT 'Medium',
            description TEXT,
            stack_trace TEXT,
            affected_record_id INT,
            affected_user_id INT,
            status ENUM('Detected','Analyzing','Fixing','Fixed','Escalated','Ignored') DEFAULT 'Detected',
            auto_fixable TINYINT(1) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_status (status),
            INDEX idx_module (module)
        )`);

        // Fix Logs (Self-Healing)
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_fix_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            error_id INT,
            fix_type ENUM('auto','suggested','manual','escalated') DEFAULT 'auto',
            fix_rule VARCHAR(100),
            fix_description TEXT,
            before_value TEXT,
            after_value TEXT,
            success TINYINT(1) DEFAULT 0,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (error_id) REFERENCES hr_error_logs(id) ON DELETE CASCADE
        )`);

        // Test Results (Self-Testing)
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_test_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            test_module VARCHAR(50),
            test_name VARCHAR(100),
            test_type ENUM('unit','integration','validation','stress') DEFAULT 'unit',
            status ENUM('Pass','Fail','Warning','Skipped') DEFAULT 'Pass',
            execution_time_ms INT DEFAULT 0,
            details JSON,
            error_message TEXT,
            run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_module (test_module)
        )`);

        // System Health
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_system_health (
            id INT AUTO_INCREMENT PRIMARY KEY,
            metric_name VARCHAR(100),
            metric_value DECIMAL(10,2),
            metric_unit VARCHAR(20),
            status ENUM('Healthy','Warning','Critical') DEFAULT 'Healthy',
            details JSON,
            checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Payroll
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_payroll (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            month INT,
            year INT,
            base_salary DECIMAL(12,2) DEFAULT 0,
            hra DECIMAL(10,2) DEFAULT 0,
            da DECIMAL(10,2) DEFAULT 0,
            special_allowance DECIMAL(10,2) DEFAULT 0,
            bonus DECIMAL(10,2) DEFAULT 0,
            overtime_pay DECIMAL(10,2) DEFAULT 0,
            pf_deduction DECIMAL(10,2) DEFAULT 0,
            esi_deduction DECIMAL(10,2) DEFAULT 0,
            tds DECIMAL(10,2) DEFAULT 0,
            other_deductions DECIMAL(10,2) DEFAULT 0,
            leave_deduction DECIMAL(10,2) DEFAULT 0,
            net_salary DECIMAL(12,2) DEFAULT 0,
            status ENUM('Draft','Processed','Paid') DEFAULT 'Draft',
            paid_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_user_month (user_id, month, year)
        )`);

        // Insert default configs if empty
        const [configCount] = await conn.query('SELECT COUNT(*) as c FROM hr_config');
        if (configCount[0].c === 0) {
            await conn.query(`INSERT IGNORE INTO hr_config (config_key, config_value, category, description) VALUES
                ('office_start_time', '"09:30"', 'attendance', 'Office start time'),
                ('office_end_time', '"18:30"', 'attendance', 'Office end time'),
                ('late_threshold_mins', '15', 'attendance', 'Minutes after start time to mark late'),
                ('half_day_hours', '4', 'attendance', 'Minimum hours for half day'),
                ('full_day_hours', '8', 'attendance', 'Minimum hours for full day'),
                ('overtime_start_hours', '9', 'attendance', 'Hours after which overtime counts'),
                ('week_off_days', '["Sunday"]', 'attendance', 'Weekly off days'),
                ('salary_components', '{"hra_percent":40,"da_percent":10,"pf_percent":12,"esi_percent":1.75}', 'payroll', 'Salary component percentages'),
                ('approval_levels', '{"leave":1,"expense":2,"attendance_correction":1}', 'workflow', 'Approval levels per module'),
                ('gps_capture_interval', '15', 'gps', 'GPS capture interval in minutes'),
                ('office_latitude', '0', 'gps', 'Office GPS latitude'),
                ('office_longitude', '0', 'gps', 'Office GPS longitude'),
                ('office_radius_meters', '200', 'gps', 'Office geo-fence radius')
            `);
        }

        // Insert default leave policies if empty
        const [leaveCount] = await conn.query('SELECT COUNT(*) as c FROM hr_leave_policy');
        if (leaveCount[0].c === 0) {
            await conn.query(`INSERT IGNORE INTO hr_leave_policy (leave_type, total_days, carry_forward, max_carry, paid, description) VALUES
                ('Casual Leave', 12, 0, 0, 1, 'For personal or urgent matters'),
                ('Sick Leave', 12, 1, 6, 1, 'For medical reasons with certificate'),
                ('Earned Leave', 15, 1, 30, 1, 'Privilege leave earned per month'),
                ('Maternity Leave', 180, 0, 0, 1, 'As per Maternity Benefit Act'),
                ('Paternity Leave', 15, 0, 0, 1, 'For new fathers'),
                ('Compensatory Off', 0, 0, 0, 1, 'For working on holidays/weekends'),
                ('Loss of Pay', 0, 0, 0, 0, 'Unpaid leave')
            `);
        }

        // Insert default expense heads if empty
        const [expCount] = await conn.query('SELECT COUNT(*) as c FROM hr_expense_heads');
        if (expCount[0].c === 0) {
            await conn.query(`INSERT IGNORE INTO hr_expense_heads (head_name, max_limit, requires_receipt, applicable_roles) VALUES
                ('Travel - Local', 2000, 1, 'All'),
                ('Travel - Outstation', 10000, 1, 'All'),
                ('Food & Meals', 500, 0, 'All'),
                ('Accommodation', 5000, 1, 'All'),
                ('Office Supplies', 1000, 1, 'All'),
                ('Communication', 500, 0, 'All'),
                ('Client Entertainment', 3000, 1, 'Manager,Admin'),
                ('Fuel & Vehicle', 5000, 1, 'All'),
                ('Medical', 2000, 1, 'All'),
                ('Miscellaneous', 1000, 1, 'All')
            `);
        }

        // ── RBAC: Module Permissions ──
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_module_permissions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            module VARCHAR(50) NOT NULL,
            can_view TINYINT(1) DEFAULT 1,
            can_create TINYINT(1) DEFAULT 0,
            can_edit TINYINT(1) DEFAULT 0,
            can_delete TINYINT(1) DEFAULT 0,
            can_approve TINYINT(1) DEFAULT 0,
            can_export TINYINT(1) DEFAULT 0,
            granted_by INT,
            granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_user_module (user_id, module)
        )`);

        // ── Module Registry (for dynamic module tracking) ──
        await conn.query(`CREATE TABLE IF NOT EXISTS hr_module_registry (
            id INT AUTO_INCREMENT PRIMARY KEY,
            module_key VARCHAR(50) NOT NULL,
            module_name VARCHAR(100),
            module_icon VARCHAR(10) DEFAULT '',
            category VARCHAR(50) DEFAULT 'HR',
            sort_order INT DEFAULT 0,
            is_active TINYINT(1) DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_key (module_key)
        )`);

        // Insert default modules if empty
        const [modCount] = await conn.query('SELECT COUNT(*) as c FROM hr_module_registry');
        if (modCount[0].c === 0) {
            await conn.query(`INSERT IGNORE INTO hr_module_registry (module_key, module_name, module_icon, category, sort_order) VALUES
                ('hr_dashboard', 'HR Dashboard', '📊', 'HR', 1),
                ('hr_employees', 'Employee Management', '👤', 'HR', 2),
                ('hr_attendance', 'Attendance & GPS', '📅', 'HR', 3),
                ('hr_leaves', 'Leave Management', '🌴', 'HR', 4),
                ('hr_expenses', 'Expense & Voucher', '💰', 'HR', 5),
                ('hr_payroll', 'Payroll', '💲', 'HR', 6),
                ('hr_comparison', 'Manual vs System', '🔍', 'HR', 7),
                ('hr_health', 'System Health', '🩺', 'HR', 8),
                ('hr_config', 'Configuration', '⚙', 'HR', 9),
                ('hr_permissions', 'Permissions', '🔐', 'HR', 10),
                ('worklog', 'Work Log Pro', '📓', 'CRM', 11),
                ('tasks', 'Task Manager', '✅', 'CRM', 12),
                ('complaints', 'Complaints', '🛠', 'CRM', 13),
                ('reports', 'Reports', '📊', 'CRM', 14),
                ('daily_activity', 'Work Log Pro', '📝', 'CRM', 15)
            `);
        }

        console.log('✅ HR Module tables initialized (with RBAC)');
    } catch(e) {
        console.error('HR tables init error:', e.message);
    } finally {
        conn.release();
    }
}

// ═══════════════════════════════════════════════════════════════
// ██  PR MODULE — Communication & Outreach Intelligence System ██
// ═══════════════════════════════════════════════════════════════

async function initializePRTables() {
    const conn = await pool.getConnection();
    try {
        // PR Contacts
        await conn.query(`CREATE TABLE IF NOT EXISTS pr_contacts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(150) NOT NULL,
            company VARCHAR(150),
            designation VARCHAR(100),
            email VARCHAR(150),
            phone VARCHAR(20),
            whatsapp VARCHAR(20),
            category ENUM('Client','Vendor','Partner','Media','Government','Internal','VIP','Other') DEFAULT 'Client',
            date_of_birth DATE,
            anniversary DATE,
            address TEXT,
            city VARCHAR(50),
            state VARCHAR(50),
            tags VARCHAR(255),
            engagement_score INT DEFAULT 0,
            consent_whatsapp TINYINT(1) DEFAULT 1,
            consent_email TINYINT(1) DEFAULT 1,
            consent_sms TINYINT(1) DEFAULT 1,
            unsubscribed TINYINT(1) DEFAULT 0,
            last_contacted DATE,
            notes TEXT,
            status ENUM('Active','Inactive','Blocked') DEFAULT 'Active',
            created_by INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_category (category),
            INDEX idx_dob (date_of_birth),
            INDEX idx_anniversary (anniversary)
        )`);

        // PR Campaigns
        await conn.query(`CREATE TABLE IF NOT EXISTS pr_campaigns (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            campaign_type ENUM('Festival','Promotional','Reminder','Birthday','Anniversary','Welcome','Follow-up','Custom') DEFAULT 'Custom',
            channel ENUM('WhatsApp','Email','SMS','Social','Multi-Channel') DEFAULT 'Multi-Channel',
            template_id INT,
            target_audience JSON,
            target_count INT DEFAULT 0,
            scheduled_at DATETIME,
            executed_at DATETIME,
            status ENUM('Draft','Scheduled','Running','Completed','Paused','Failed') DEFAULT 'Draft',
            sent_count INT DEFAULT 0,
            delivered_count INT DEFAULT 0,
            read_count INT DEFAULT 0,
            replied_count INT DEFAULT 0,
            failed_count INT DEFAULT 0,
            created_by INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_status (status),
            INDEX idx_type (campaign_type)
        )`);

        // PR Templates
        await conn.query(`CREATE TABLE IF NOT EXISTS pr_templates (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(150) NOT NULL,
            template_type ENUM('WhatsApp','Email','SMS','Social') DEFAULT 'WhatsApp',
            category ENUM('Greeting','Promotion','Reminder','Follow-up','Festival','Birthday','Anniversary','Welcome','Custom') DEFAULT 'Custom',
            subject VARCHAR(255),
            content TEXT NOT NULL,
            variables JSON,
            media_url VARCHAR(500),
            is_active TINYINT(1) DEFAULT 1,
            usage_count INT DEFAULT 0,
            created_by INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Communication Logs
        await conn.query(`CREATE TABLE IF NOT EXISTS pr_communication_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            contact_id INT,
            campaign_id INT,
            channel ENUM('WhatsApp','Email','SMS','Social','Call') DEFAULT 'WhatsApp',
            direction ENUM('Outbound','Inbound') DEFAULT 'Outbound',
            message_type VARCHAR(50),
            content TEXT,
            status ENUM('Queued','Sent','Delivered','Read','Failed','Bounced') DEFAULT 'Queued',
            sent_at DATETIME,
            delivered_at DATETIME,
            read_at DATETIME,
            error_message VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_contact (contact_id),
            INDEX idx_campaign (campaign_id),
            INDEX idx_status (status),
            INDEX idx_channel (channel)
        )`);

        // Email Logs (detailed)
        await conn.query(`CREATE TABLE IF NOT EXISTS pr_email_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            comm_log_id INT,
            to_email VARCHAR(150),
            subject VARCHAR(255),
            opened TINYINT(1) DEFAULT 0,
            opened_at DATETIME,
            clicked TINYINT(1) DEFAULT 0,
            clicked_at DATETIME,
            bounced TINYINT(1) DEFAULT 0,
            unsubscribed TINYINT(1) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // WhatsApp Logs (detailed)
        await conn.query(`CREATE TABLE IF NOT EXISTS pr_whatsapp_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            comm_log_id INT,
            to_number VARCHAR(20),
            message_id VARCHAR(100),
            delivered TINYINT(1) DEFAULT 0,
            delivered_at DATETIME,
            read_flag TINYINT(1) DEFAULT 0,
            read_at DATETIME,
            replied TINYINT(1) DEFAULT 0,
            reply_text TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Reminders & Events
        await conn.query(`CREATE TABLE IF NOT EXISTS pr_reminders (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(200),
            reminder_type ENUM('Birthday','Anniversary','Festival','Meeting','Follow-up','Gift','Custom') DEFAULT 'Custom',
            contact_id INT,
            reminder_date DATE,
            reminder_time TIME DEFAULT '09:00:00',
            recurrence ENUM('None','Daily','Weekly','Monthly','Yearly') DEFAULT 'None',
            auto_action ENUM('None','WhatsApp','Email','Both') DEFAULT 'None',
            template_id INT,
            status ENUM('Active','Completed','Cancelled') DEFAULT 'Active',
            notes TEXT,
            created_by INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_date (reminder_date)
        )`);

        // Gift & Activity
        await conn.query(`CREATE TABLE IF NOT EXISTS pr_gifts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            contact_id INT,
            occasion VARCHAR(100),
            gift_type VARCHAR(100),
            gift_description TEXT,
            amount DECIMAL(10,2) DEFAULT 0,
            gift_date DATE,
            delivery_status ENUM('Planned','Ordered','Delivered','Acknowledged') DEFAULT 'Planned',
            thank_you_sent TINYINT(1) DEFAULT 0,
            notes TEXT,
            created_by INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Social Media Posts
        await conn.query(`CREATE TABLE IF NOT EXISTS pr_social_posts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            platform ENUM('Facebook','Instagram','LinkedIn','Twitter','YouTube','Other') DEFAULT 'LinkedIn',
            post_type ENUM('Image','Video','Story','Reel','Article','Poll') DEFAULT 'Image',
            content TEXT,
            media_url VARCHAR(500),
            hashtags VARCHAR(500),
            scheduled_at DATETIME,
            published_at DATETIME,
            status ENUM('Draft','Scheduled','Published','Failed') DEFAULT 'Draft',
            likes INT DEFAULT 0,
            comments INT DEFAULT 0,
            shares INT DEFAULT 0,
            reach INT DEFAULT 0,
            created_by INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // PR Config
        await conn.query(`CREATE TABLE IF NOT EXISTS pr_config (
            id INT AUTO_INCREMENT PRIMARY KEY,
            config_key VARCHAR(100) NOT NULL,
            config_value JSON,
            category VARCHAR(50) DEFAULT 'general',
            description VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_key (config_key)
        )`);

        // PR Error Logs (Self-Healing)
        await conn.query(`CREATE TABLE IF NOT EXISTS pr_error_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            module VARCHAR(50),
            error_type VARCHAR(50),
            severity ENUM('Low','Medium','High','Critical') DEFAULT 'Medium',
            description TEXT,
            auto_action VARCHAR(100),
            status ENUM('Detected','Fixed','Escalated') DEFAULT 'Detected',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Seed default configs
        const [cfgCount] = await conn.query('SELECT COUNT(*) as c FROM pr_config');
        if (cfgCount[0].c === 0) {
            await conn.query(`INSERT IGNORE INTO pr_config (config_key, config_value, category, description) VALUES
                ('birthday_auto_wish', 'true', 'automation', 'Auto send birthday wishes'),
                ('anniversary_auto_wish', 'true', 'automation', 'Auto send anniversary wishes'),
                ('birthday_wish_time', '"09:00"', 'automation', 'Time to send birthday wishes'),
                ('default_whatsapp_channel', '"api"', 'whatsapp', 'WhatsApp send method'),
                ('email_from_name', '"Coral Infratel"', 'email', 'From name for emails'),
                ('email_from_address', '"info@coralinfratel.com"', 'email', 'From email address'),
                ('campaign_retry_count', '3', 'campaign', 'Retry failed messages'),
                ('engagement_decay_days', '90', 'engagement', 'Days after which engagement score decays'),
                ('consent_required', 'true', 'compliance', 'Require consent before messaging'),
                ('unsubscribe_link', 'true', 'compliance', 'Include unsubscribe link in emails')
            `);
        }

        // Seed default templates
        const [tmplCount] = await conn.query('SELECT COUNT(*) as c FROM pr_templates');
        if (tmplCount[0].c === 0) {
            await conn.query(`INSERT IGNORE INTO pr_templates (name, template_type, category, subject, content, variables) VALUES
                ('Birthday Wish', 'WhatsApp', 'Birthday', NULL, 'Dear {Name}, wishing you a very Happy Birthday! May this year bring you great success and happiness. Warm regards, Coral Infratel Pvt. Ltd.', '["Name"]'),
                ('Anniversary Wish', 'WhatsApp', 'Anniversary', NULL, 'Dear {Name}, wishing you a Happy Anniversary! May your bond grow stronger every year. Best wishes from Coral Infratel.', '["Name"]'),
                ('Festival Greeting', 'WhatsApp', 'Festival', NULL, 'Dear {Name}, {Festival} ki hardik shubhkamnayein! May this festival bring joy and prosperity. - Coral Infratel', '["Name","Festival"]'),
                ('Welcome Email', 'Email', 'Welcome', 'Welcome to Coral Infratel!', 'Dear {Name},\\n\\nWelcome to Coral Infratel! We are delighted to have you as our valued {Category}.\\n\\nBest regards,\\nCoral Infratel Pvt. Ltd.', '["Name","Category"]'),
                ('Follow-up Email', 'Email', 'Follow-up', 'Following up - {Subject}', 'Dear {Name},\\n\\nThis is a follow-up regarding {Subject}. We would love to hear your feedback.\\n\\nBest regards,\\nCoral Infratel', '["Name","Subject"]'),
                ('Promotional Offer', 'Email', 'Promotion', 'Special Offer for {Company}', 'Dear {Name},\\n\\nWe have an exclusive offer for {Company}. Please connect with us to know more.\\n\\nRegards,\\nCoral Infratel', '["Name","Company"]'),
                ('Social Post Template', 'Social', 'Custom', NULL, '{Caption}\\n\\n#CoralInfratel #Telecom #BSNL {Hashtags}', '["Caption","Hashtags"]'),
                ('Diwali Greeting', 'WhatsApp', 'Festival', NULL, 'Dear {Name}, Happy Diwali! May the festival of lights illuminate your life with happiness and prosperity. - Team Coral Infratel', '["Name"]')
            `);
        }

        console.log('✅ PR Module tables initialized');
    } catch(e) {
        console.error('PR tables init error:', e.message);
    } finally {
        conn.release();
    }
}

// PR Middleware: Ensure tables
let prTablesReady = false;
async function ensurePRTables(req, res, next) {
    if (!prTablesReady) {
        try { await initializePRTables(); prTablesReady = true; } catch(e) { console.error('PR init:', e.message); }
    }
    next();
}

// ── PR API: Dashboard ──
app.get('/api/pr/dashboard', authenticateToken, ensurePRTables, async (req, res) => {
    try {
        const today = new Date().toISOString().split('T')[0];
        const [totalContacts] = await pool.query('SELECT COUNT(*) as c FROM pr_contacts WHERE status="Active"');
        const [totalCampaigns] = await pool.query('SELECT COUNT(*) as c FROM pr_campaigns');
        const [activeCampaigns] = await pool.query('SELECT COUNT(*) as c FROM pr_campaigns WHERE status IN ("Scheduled","Running")');
        const [msgsSent] = await pool.query('SELECT COUNT(*) as c FROM pr_communication_logs WHERE MONTH(created_at)=MONTH(NOW()) AND YEAR(created_at)=YEAR(NOW())');
        const [msgsDelivered] = await pool.query('SELECT COUNT(*) as c FROM pr_communication_logs WHERE status="Delivered" AND MONTH(created_at)=MONTH(NOW())');
        const [todayBirthdays] = await pool.query('SELECT COUNT(*) as c FROM pr_contacts WHERE DATE_FORMAT(date_of_birth,"%m-%d")=DATE_FORMAT(?,"%m-%d") AND status="Active"', [today]);
        const [upcomingBdays] = await pool.query(`SELECT id,name,company,date_of_birth FROM pr_contacts
            WHERE DATE_FORMAT(date_of_birth,'%m-%d') BETWEEN DATE_FORMAT(?,'%m-%d') AND DATE_FORMAT(DATE_ADD(?,INTERVAL 7 DAY),'%m-%d') AND status='Active' ORDER BY DATE_FORMAT(date_of_birth,'%m-%d') LIMIT 10`, [today, today]);
        const [recentCampaigns] = await pool.query('SELECT * FROM pr_campaigns ORDER BY created_at DESC LIMIT 5');
        const [channelStats] = await pool.query(`SELECT channel, COUNT(*) as total,
            SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END) as delivered,
            SUM(CASE WHEN status='Read' THEN 1 ELSE 0 END) as read_count,
            SUM(CASE WHEN status='Failed' THEN 1 ELSE 0 END) as failed
            FROM pr_communication_logs WHERE MONTH(created_at)=MONTH(NOW()) GROUP BY channel`);
        const [templates] = await pool.query('SELECT COUNT(*) as c FROM pr_templates WHERE is_active=1');
        res.json({
            stats: { totalContacts: totalContacts[0].c, totalCampaigns: totalCampaigns[0].c, activeCampaigns: activeCampaigns[0].c,
                msgsSentMonth: msgsSent[0].c, msgsDelivered: msgsDelivered[0].c, todayBirthdays: todayBirthdays[0].c, templates: templates[0].c },
            upcomingBirthdays: upcomingBdays, recentCampaigns, channelStats
        });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── PR API: Contacts CRUD ──
app.get('/api/pr/contacts', authenticateToken, ensurePRTables, async (req, res) => {
    try {
        const { category, search, status } = req.query;
        let sql = 'SELECT * FROM pr_contacts WHERE 1=1';
        const params = [];
        if (category) { sql += ' AND category=?'; params.push(category); }
        if (status) { sql += ' AND status=?'; params.push(status); }
        if (search) { sql += ' AND (name LIKE ? OR company LIKE ? OR email LIKE ? OR phone LIKE ?)'; params.push(`%${search}%`,`%${search}%`,`%${search}%`,`%${search}%`); }
        sql += ' ORDER BY name LIMIT 500';
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/pr/contacts', authenticateToken, ensurePRTables, async (req, res) => {
    try {
        const b = req.body;
        const [result] = await pool.query(`INSERT INTO pr_contacts (name,company,designation,email,phone,whatsapp,category,date_of_birth,anniversary,address,city,state,tags,notes,created_by)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            [b.name,b.company,b.designation,b.email,b.phone,b.whatsapp||b.phone,b.category||'Client',b.date_of_birth||null,b.anniversary||null,b.address,b.city,b.state,b.tags,b.notes,req.user.id]);
        res.json({ success: true, id: result.insertId });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/pr/contacts/:id', authenticateToken, async (req, res) => {
    try {
        const b = req.body; const fields = []; const vals = [];
        const allowed = ['name','company','designation','email','phone','whatsapp','category','date_of_birth','anniversary','address','city','state','tags','notes','status','consent_whatsapp','consent_email','consent_sms','unsubscribed'];
        allowed.forEach(f => { if (b[f] !== undefined) { fields.push(`${f}=?`); vals.push(b[f]); }});
        if (!fields.length) return res.json({ success: true });
        vals.push(req.params.id);
        await pool.query(`UPDATE pr_contacts SET ${fields.join(',')} WHERE id=?`, vals);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── PR API: Templates CRUD ──
app.get('/api/pr/templates', authenticateToken, ensurePRTables, async (req, res) => {
    try {
        const { type, category } = req.query;
        let sql = 'SELECT * FROM pr_templates WHERE is_active=1';
        const params = [];
        if (type) { sql += ' AND template_type=?'; params.push(type); }
        if (category) { sql += ' AND category=?'; params.push(category); }
        sql += ' ORDER BY name';
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/pr/templates', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        const [result] = await pool.query(`INSERT INTO pr_templates (name,template_type,category,subject,content,variables,media_url,created_by)
            VALUES (?,?,?,?,?,?,?,?)`, [b.name,b.template_type,b.category,b.subject,b.content,JSON.stringify(b.variables||[]),b.media_url,req.user.id]);
        res.json({ success: true, id: result.insertId });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/pr/templates/:id', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        await pool.query(`UPDATE pr_templates SET name=?,template_type=?,category=?,subject=?,content=?,variables=?,media_url=?,is_active=? WHERE id=?`,
            [b.name,b.template_type,b.category,b.subject,b.content,JSON.stringify(b.variables||[]),b.media_url,b.is_active!==undefined?(b.is_active?1:0):1,req.params.id]);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── PR API: Campaigns CRUD ──
app.get('/api/pr/campaigns', authenticateToken, ensurePRTables, async (req, res) => {
    try {
        const { status } = req.query;
        let sql = 'SELECT c.*, t.name as template_name FROM pr_campaigns c LEFT JOIN pr_templates t ON c.template_id=t.id WHERE 1=1';
        const params = [];
        if (status) { sql += ' AND c.status=?'; params.push(status); }
        sql += ' ORDER BY c.created_at DESC LIMIT 100';
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/pr/campaigns', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        const [result] = await pool.query(`INSERT INTO pr_campaigns (name,campaign_type,channel,template_id,target_audience,target_count,scheduled_at,status,created_by)
            VALUES (?,?,?,?,?,?,?,?,?)`, [b.name,b.campaign_type,b.channel,b.template_id||null,JSON.stringify(b.target_audience||{}),b.target_count||0,b.scheduled_at||null,b.status||'Draft',req.user.id]);
        res.json({ success: true, id: result.insertId });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/pr/campaigns/:id', authenticateToken, async (req, res) => {
    try {
        const b = req.body; const fields = []; const vals = [];
        ['name','campaign_type','channel','template_id','target_count','scheduled_at','status'].forEach(f => {
            if (b[f] !== undefined) { fields.push(`${f}=?`); vals.push(b[f]); }
        });
        if (b.target_audience) { fields.push('target_audience=?'); vals.push(JSON.stringify(b.target_audience)); }
        vals.push(req.params.id);
        await pool.query(`UPDATE pr_campaigns SET ${fields.join(',')} WHERE id=?`, vals);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── PR API: Communication Logs ──
app.get('/api/pr/comm-logs', authenticateToken, ensurePRTables, async (req, res) => {
    try {
        const { contact_id, channel, campaign_id } = req.query;
        let sql = `SELECT cl.*, c.name as contact_name, c.company FROM pr_communication_logs cl
            LEFT JOIN pr_contacts c ON cl.contact_id=c.id WHERE 1=1`;
        const params = [];
        if (contact_id) { sql += ' AND cl.contact_id=?'; params.push(contact_id); }
        if (channel) { sql += ' AND cl.channel=?'; params.push(channel); }
        if (campaign_id) { sql += ' AND cl.campaign_id=?'; params.push(campaign_id); }
        sql += ' ORDER BY cl.created_at DESC LIMIT 200';
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// Simulate sending message (for demo/testing)
app.post('/api/pr/send-message', authenticateToken, async (req, res) => {
    try {
        const { contact_id, channel, content, campaign_id, template_id } = req.body;
        const now = new Date().toISOString().slice(0,19).replace('T',' ');
        const statuses = ['Delivered','Delivered','Delivered','Read','Sent','Failed'];
        const simStatus = statuses[Math.floor(Math.random()*statuses.length)];

        const [result] = await pool.query(`INSERT INTO pr_communication_logs (contact_id,campaign_id,channel,direction,content,status,sent_at,delivered_at)
            VALUES (?,?,?,?,?,?,?,?)`, [contact_id,campaign_id||null,channel||'WhatsApp','Outbound',content,simStatus,now,simStatus!=='Failed'?now:null]);

        // Update contact last_contacted
        await pool.query('UPDATE pr_contacts SET last_contacted=CURDATE(), engagement_score=LEAST(engagement_score+5,100) WHERE id=?', [contact_id]);

        // Channel-specific log
        if (channel === 'WhatsApp') {
            const [contact] = await pool.query('SELECT whatsapp FROM pr_contacts WHERE id=?', [contact_id]);
            await pool.query('INSERT INTO pr_whatsapp_logs (comm_log_id,to_number,delivered,read_flag) VALUES (?,?,?,?)',
                [result.insertId, contact[0]?.whatsapp||'', simStatus!=='Failed'?1:0, simStatus==='Read'?1:0]);
        } else if (channel === 'Email') {
            const [contact] = await pool.query('SELECT email FROM pr_contacts WHERE id=?', [contact_id]);
            await pool.query('INSERT INTO pr_email_logs (comm_log_id,to_email,subject,opened) VALUES (?,?,?,?)',
                [result.insertId, contact[0]?.email||'', 'Message from Coral Infratel', simStatus==='Read'?1:0]);
        }

        res.json({ success: true, id: result.insertId, status: simStatus });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── PR API: Reminders ──
app.get('/api/pr/reminders', authenticateToken, ensurePRTables, async (req, res) => {
    try {
        const [rows] = await pool.query(`SELECT r.*, c.name as contact_name, c.company FROM pr_reminders r
            LEFT JOIN pr_contacts c ON r.contact_id=c.id WHERE r.status='Active' ORDER BY r.reminder_date LIMIT 50`);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/pr/reminders', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        const [result] = await pool.query(`INSERT INTO pr_reminders (title,reminder_type,contact_id,reminder_date,reminder_time,recurrence,auto_action,template_id,notes,created_by)
            VALUES (?,?,?,?,?,?,?,?,?,?)`, [b.title,b.reminder_type,b.contact_id||null,b.reminder_date,b.reminder_time||'09:00',b.recurrence||'None',b.auto_action||'None',b.template_id||null,b.notes,req.user.id]);
        res.json({ success: true, id: result.insertId });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── PR API: Gifts ──
app.get('/api/pr/gifts', authenticateToken, ensurePRTables, async (req, res) => {
    try {
        const [rows] = await pool.query(`SELECT g.*, c.name as contact_name, c.company FROM pr_gifts g
            LEFT JOIN pr_contacts c ON g.contact_id=c.id ORDER BY g.gift_date DESC LIMIT 100`);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/pr/gifts', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        const [result] = await pool.query(`INSERT INTO pr_gifts (contact_id,occasion,gift_type,gift_description,amount,gift_date,notes,created_by)
            VALUES (?,?,?,?,?,?,?,?)`, [b.contact_id,b.occasion,b.gift_type,b.gift_description,b.amount||0,b.gift_date,b.notes,req.user.id]);
        res.json({ success: true, id: result.insertId });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── PR API: Social Posts ──
app.get('/api/pr/social-posts', authenticateToken, ensurePRTables, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM pr_social_posts ORDER BY created_at DESC LIMIT 50');
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/pr/social-posts', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        const [result] = await pool.query(`INSERT INTO pr_social_posts (platform,post_type,content,media_url,hashtags,scheduled_at,status,created_by)
            VALUES (?,?,?,?,?,?,?,?)`, [b.platform,b.post_type,b.content,b.media_url,b.hashtags,b.scheduled_at||null,b.status||'Draft',req.user.id]);
        res.json({ success: true, id: result.insertId });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── PR API: Config ──
app.get('/api/pr/config', authenticateToken, ensurePRTables, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM pr_config ORDER BY category, config_key');
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/pr/config/:key', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        await pool.query('UPDATE pr_config SET config_value=? WHERE config_key=?', [JSON.stringify(req.body.value), req.params.key]);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── PR API: Analytics ──
app.get('/api/pr/analytics', authenticateToken, ensurePRTables, async (req, res) => {
    try {
        const [channelPerf] = await pool.query(`SELECT channel, COUNT(*) as total,
            SUM(CASE WHEN status IN ('Delivered','Read') THEN 1 ELSE 0 END) as success,
            SUM(CASE WHEN status='Failed' THEN 1 ELSE 0 END) as failed
            FROM pr_communication_logs GROUP BY channel`);
        const [dailyTrend] = await pool.query(`SELECT DATE(created_at) as date, COUNT(*) as total,
            SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END) as delivered
            FROM pr_communication_logs WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY DATE(created_at) ORDER BY date`);
        const [topContacts] = await pool.query(`SELECT c.name, c.company, c.engagement_score, COUNT(cl.id) as msg_count
            FROM pr_contacts c LEFT JOIN pr_communication_logs cl ON c.id=cl.contact_id
            WHERE c.status='Active' GROUP BY c.id ORDER BY c.engagement_score DESC LIMIT 10`);
        const [campaignPerf] = await pool.query(`SELECT name, campaign_type, channel, sent_count, delivered_count, read_count, failed_count, status
            FROM pr_campaigns ORDER BY created_at DESC LIMIT 10`);
        res.json({ channelPerformance: channelPerf, dailyTrend, topContacts, campaignPerformance: campaignPerf });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── PR API: Demo Data Generator ──
app.post('/api/pr/generate-demo', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        await initializePRTables();
        const conn = await pool.getConnection();
        try {
            const names = ['Rajesh Kumar','Priya Sharma','Amit Singh','Neha Gupta','Vikram Patel','Sunita Verma','Karan Mehta','Deepa Nair','Rohit Jain','Anjali Mishra','Suresh Reddy','Kavita Iyer','Manish Agarwal','Pooja Rao','Sanjay Tiwari'];
            const companies = ['TechCorp','InfoSys','Reliance','Tata Group','Wipro','HCL','Bharti Airtel','Adani','Mahindra','Godrej'];
            const categories = ['Client','Vendor','Partner','Media','VIP'];
            const cities = ['Delhi','Mumbai','Bangalore','Chennai','Hyderabad','Pune','Kolkata','Jaipur'];
            let contactCount = 0;
            for (const name of names) {
                const [exists] = await conn.query('SELECT id FROM pr_contacts WHERE name=?', [name]);
                if (exists.length) continue;
                const company = companies[Math.floor(Math.random()*companies.length)];
                const cat = categories[Math.floor(Math.random()*categories.length)];
                const city = cities[Math.floor(Math.random()*cities.length)];
                const month = String(1+Math.floor(Math.random()*12)).padStart(2,'0');
                const day = String(1+Math.floor(Math.random()*28)).padStart(2,'0');
                await conn.query(`INSERT INTO pr_contacts (name,company,designation,email,phone,whatsapp,category,date_of_birth,city,engagement_score,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
                    [name,company,'Manager',name.toLowerCase().replace(/\s/g,'.')+'@'+company.toLowerCase()+'.com',
                     '98'+String(Math.floor(10000000+Math.random()*90000000)),'98'+String(Math.floor(10000000+Math.random()*90000000)),
                     cat,'1985-'+month+'-'+day,city,Math.floor(Math.random()*100),req.user.id]);
                contactCount++;
            }

            // Generate comm logs
            const [contacts] = await conn.query('SELECT id FROM pr_contacts LIMIT 10');
            let commCount = 0;
            const channels = ['WhatsApp','Email','WhatsApp','WhatsApp','Email'];
            const statuses = ['Delivered','Delivered','Read','Sent','Failed','Delivered'];
            for (const c of contacts) {
                for (let i = 0; i < 5; i++) {
                    const ch = channels[Math.floor(Math.random()*channels.length)];
                    const st = statuses[Math.floor(Math.random()*statuses.length)];
                    const daysAgo = Math.floor(Math.random()*30);
                    await conn.query(`INSERT INTO pr_communication_logs (contact_id,channel,direction,content,status,sent_at,created_at)
                        VALUES (?,?,'Outbound','Demo message',?,DATE_SUB(NOW(),INTERVAL ? DAY),DATE_SUB(NOW(),INTERVAL ? DAY))`,
                        [c.id,ch,st,daysAgo,daysAgo]);
                    commCount++;
                }
            }
            res.json({ success: true, generated: { contacts: contactCount, communications: commCount } });
        } finally { conn.release(); }
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR Middleware: Ensure tables exist (Vercel cold start safety) ──
let hrTablesReady = false;
async function ensureHRTables(req, res, next) {
    if (!hrTablesReady) {
        try { await initializeHRTables(); hrTablesReady = true; } catch(e) { console.error('HR init error:', e.message); }
    }
    next();
}

// ── HR API: Dashboard Stats ──
app.get('/api/hr/dashboard', authenticateToken, ensureHRTables, async (req, res) => {
    try {
        const today = new Date().toISOString().split('T')[0];
        const [totalEmp] = await pool.query('SELECT COUNT(*) as c FROM hr_employees WHERE status="Active"');
        const [presentToday] = await pool.query('SELECT COUNT(*) as c FROM hr_attendance WHERE attendance_date=? AND status IN ("Present","Late")', [today]);
        const [onLeave] = await pool.query('SELECT COUNT(*) as c FROM hr_leaves WHERE status="Approved" AND from_date<=? AND to_date>=?', [today, today]);
        const [pendingLeaves] = await pool.query('SELECT COUNT(*) as c FROM hr_leaves WHERE status="Pending"');
        const [pendingExpenses] = await pool.query('SELECT COUNT(*) as c FROM hr_expenses WHERE status="Pending"');
        const [totalExpMonth] = await pool.query('SELECT COALESCE(SUM(amount),0) as total FROM hr_expenses WHERE MONTH(expense_date)=MONTH(NOW()) AND YEAR(expense_date)=YEAR(NOW()) AND status IN ("Approved","Paid")');
        const [errors] = await pool.query('SELECT COUNT(*) as c FROM hr_error_logs WHERE status IN ("Detected","Analyzing")');
        const [healthScore] = await pool.query('SELECT COALESCE(AVG(metric_value),100) as score FROM hr_system_health WHERE checked_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)');

        // Attendance trend (7 days)
        const [attTrend] = await pool.query(`SELECT attendance_date as date,
            SUM(CASE WHEN status IN ('Present','Late') THEN 1 ELSE 0 END) as present,
            SUM(CASE WHEN status='Absent' THEN 1 ELSE 0 END) as absent,
            SUM(CASE WHEN status='Late' THEN 1 ELSE 0 END) as late
            FROM hr_attendance WHERE attendance_date >= DATE_SUB(?, INTERVAL 7 DAY)
            GROUP BY attendance_date ORDER BY attendance_date`, [today]);

        // Department distribution
        const [deptDist] = await pool.query('SELECT department, COUNT(*) as count FROM hr_employees WHERE status="Active" GROUP BY department ORDER BY count DESC');

        // Recent errors
        const [recentErrors] = await pool.query('SELECT id, module, error_type, severity, description, status, created_at FROM hr_error_logs ORDER BY created_at DESC LIMIT 5');

        res.json({
            stats: {
                totalEmployees: totalEmp[0].c,
                presentToday: presentToday[0].c,
                onLeave: onLeave[0].c,
                pendingLeaves: pendingLeaves[0].c,
                pendingExpenses: pendingExpenses[0].c,
                monthlyExpenses: totalExpMonth[0].total,
                activeErrors: errors[0].c,
                healthScore: Math.round(healthScore[0].score)
            },
            attendanceTrend: attTrend,
            departmentDistribution: deptDist,
            recentErrors
        });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Employees ──
app.get('/api/hr/employees', authenticateToken, async (req, res) => {
    try {
        const { status, department, search } = req.query;
        let sql = `SELECT e.*, u.name as user_name, u.username, u.role
                    FROM hr_employees e
                    LEFT JOIN users u ON e.user_id = u.id WHERE 1=1`;
        const params = [];
        if (status) { sql += ' AND e.status=?'; params.push(status); }
        if (department) { sql += ' AND e.department=?'; params.push(department); }
        if (search) { sql += ' AND (u.name LIKE ? OR e.emp_code LIKE ? OR e.designation LIKE ?)'; params.push(`%${search}%`,`%${search}%`,`%${search}%`); }
        sql += ' ORDER BY e.emp_code ASC';
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/hr/employees', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        const [result] = await pool.query(`INSERT INTO hr_employees
            (user_id, emp_code, designation, department, date_of_joining, date_of_birth, gender, phone,
             emergency_contact, address, city, state, blood_group, pan_number, aadhar_number,
             bank_name, bank_account, ifsc_code, base_salary, status)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            [b.user_id, b.emp_code, b.designation, b.department, b.date_of_joining, b.date_of_birth,
             b.gender, b.phone, b.emergency_contact, b.address, b.city, b.state, b.blood_group,
             b.pan_number, b.aadhar_number, b.bank_name, b.bank_account, b.ifsc_code,
             b.base_salary || 0, b.status || 'Active']);
        res.json({ success: true, id: result.insertId });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/hr/employees/:id', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        const fields = [];
        const vals = [];
        const allowed = ['emp_code','designation','department','date_of_joining','date_of_birth','gender','phone',
            'emergency_contact','address','city','state','blood_group','pan_number','aadhar_number',
            'bank_name','bank_account','ifsc_code','base_salary','status'];
        allowed.forEach(f => { if (b[f] !== undefined) { fields.push(`${f}=?`); vals.push(b[f]); }});
        if (!fields.length) return res.json({ success: true });
        vals.push(req.params.id);
        await pool.query(`UPDATE hr_employees SET ${fields.join(',')} WHERE id=?`, vals);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Attendance ──
app.get('/api/hr/attendance', authenticateToken, async (req, res) => {
    try {
        const { user_id, from_date, to_date, status } = req.query;
        let sql = `SELECT a.*, u.name as user_name, e.emp_code, e.department
                    FROM hr_attendance a
                    LEFT JOIN users u ON a.user_id = u.id
                    LEFT JOIN hr_employees e ON a.user_id = e.user_id WHERE 1=1`;
        const params = [];
        if (user_id) { sql += ' AND a.user_id=?'; params.push(user_id); }
        if (from_date) { sql += ' AND a.attendance_date>=?'; params.push(from_date); }
        if (to_date) { sql += ' AND a.attendance_date<=?'; params.push(to_date); }
        if (status) { sql += ' AND a.status=?'; params.push(status); }
        if (req.user.role !== 'admin') { sql += ' AND a.user_id=?'; params.push(req.user.id); }
        sql += ' ORDER BY a.attendance_date DESC, u.name ASC LIMIT 500';
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/hr/attendance/checkin', authenticateToken, async (req, res) => {
    try {
        const today = new Date().toISOString().split('T')[0];
        const now = new Date().toTimeString().split(' ')[0];
        const { latitude, longitude, address } = req.body;

        // Get office start time from config
        const [cfg] = await pool.query("SELECT config_value FROM hr_config WHERE config_key='office_start_time'");
        const startTime = cfg.length ? JSON.parse(cfg[0].config_value) : '09:30';
        const [lateCfg] = await pool.query("SELECT config_value FROM hr_config WHERE config_key='late_threshold_mins'");
        const lateMins = lateCfg.length ? parseInt(lateCfg[0].config_value) : 15;

        // Check if late
        const [sh, sm] = startTime.split(':').map(Number);
        const lateLimit = new Date(); lateLimit.setHours(sh, sm + lateMins, 0);
        const currentTime = new Date(); currentTime.setHours(...now.split(':').map(Number));
        const isLate = currentTime > lateLimit;

        const [existing] = await pool.query('SELECT id FROM hr_attendance WHERE user_id=? AND attendance_date=?', [req.user.id, today]);
        if (existing.length) {
            return res.status(409).json({ error: 'Already checked in today' });
        }

        await pool.query(`INSERT INTO hr_attendance (user_id, attendance_date, check_in, check_in_lat, check_in_lng, check_in_address, status, source)
            VALUES (?,?,?,?,?,?,?,?)`, [req.user.id, today, now, latitude, longitude, address, isLate ? 'Late' : 'Present', 'system']);

        // Log GPS
        if (latitude && longitude) {
            await pool.query('INSERT INTO hr_gps_logs (user_id, latitude, longitude, address, log_type) VALUES (?,?,?,?,?)',
                [req.user.id, latitude, longitude, address, 'checkin']);
        }

        res.json({ success: true, isLate, checkInTime: now });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/hr/attendance/checkout', authenticateToken, async (req, res) => {
    try {
        const today = new Date().toISOString().split('T')[0];
        const now = new Date().toTimeString().split(' ')[0];
        const { latitude, longitude, address } = req.body;

        const [att] = await pool.query('SELECT * FROM hr_attendance WHERE user_id=? AND attendance_date=?', [req.user.id, today]);
        if (!att.length) return res.status(404).json({ error: 'No check-in found for today' });
        if (att[0].check_out) return res.status(409).json({ error: 'Already checked out' });

        // Calculate hours
        const checkIn = att[0].check_in;
        const [h1,m1] = checkIn.split(':').map(Number);
        const [h2,m2] = now.split(':').map(Number);
        const totalHrs = ((h2*60+m2) - (h1*60+m1)) / 60;

        // Get overtime config
        const [otCfg] = await pool.query("SELECT config_value FROM hr_config WHERE config_key='overtime_start_hours'");
        const otHrs = otCfg.length ? parseInt(otCfg[0].config_value) : 9;
        const overtime = Math.max(0, totalHrs - otHrs);

        // Half day check
        const [hdCfg] = await pool.query("SELECT config_value FROM hr_config WHERE config_key='half_day_hours'");
        const hdHrs = hdCfg.length ? parseInt(hdCfg[0].config_value) : 4;
        let status = att[0].status;
        if (totalHrs < hdHrs) status = 'Half Day';

        await pool.query(`UPDATE hr_attendance SET check_out=?, check_out_lat=?, check_out_lng=?, check_out_address=?,
            total_hours=?, overtime_hours=?, status=? WHERE id=?`,
            [now, latitude, longitude, address, totalHrs.toFixed(2), overtime.toFixed(2), status, att[0].id]);

        if (latitude && longitude) {
            await pool.query('INSERT INTO hr_gps_logs (user_id, latitude, longitude, address, log_type) VALUES (?,?,?,?,?)',
                [req.user.id, latitude, longitude, address, 'checkout']);
        }

        res.json({ success: true, totalHours: totalHrs.toFixed(2), overtime: overtime.toFixed(2) });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Leave Management ──
app.get('/api/hr/leave-policy', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM hr_leave_policy WHERE is_active=1 ORDER BY leave_type');
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/hr/leave-policy', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const b = req.body;
        const [result] = await pool.query(`INSERT INTO hr_leave_policy (leave_type, total_days, carry_forward, max_carry, paid, applicable_to, description)
            VALUES (?,?,?,?,?,?,?)`, [b.leave_type, b.total_days, b.carry_forward?1:0, b.max_carry||0, b.paid?1:0, b.applicable_to||'All', b.description]);
        res.json({ success: true, id: result.insertId });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/hr/leave-policy/:id', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const b = req.body;
        await pool.query(`UPDATE hr_leave_policy SET leave_type=?, total_days=?, carry_forward=?, max_carry=?, paid=?, applicable_to=?, description=?, is_active=? WHERE id=?`,
            [b.leave_type, b.total_days, b.carry_forward?1:0, b.max_carry||0, b.paid?1:0, b.applicable_to||'All', b.description, b.is_active?1:0, req.params.id]);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/hr/leaves', authenticateToken, async (req, res) => {
    try {
        const { status, user_id } = req.query;
        let sql = `SELECT l.*, u.name as user_name, e.emp_code, e.department, a.name as approver_name
                    FROM hr_leaves l
                    LEFT JOIN users u ON l.user_id = u.id
                    LEFT JOIN hr_employees e ON l.user_id = e.user_id
                    LEFT JOIN users a ON l.approved_by = a.id WHERE 1=1`;
        const params = [];
        if (status) { sql += ' AND l.status=?'; params.push(status); }
        if (req.user.role !== 'admin') { sql += ' AND l.user_id=?'; params.push(req.user.id); }
        else if (user_id) { sql += ' AND l.user_id=?'; params.push(user_id); }
        sql += ' ORDER BY l.created_at DESC LIMIT 200';
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/hr/leaves', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        const from = new Date(b.from_date); const to = new Date(b.to_date);
        const diffDays = Math.ceil((to - from) / (1000*60*60*24)) + 1;
        const [result] = await pool.query(`INSERT INTO hr_leaves (user_id, leave_type, from_date, to_date, total_days, reason)
            VALUES (?,?,?,?,?,?)`, [req.user.id, b.leave_type, b.from_date, b.to_date, diffDays, b.reason]);
        res.json({ success: true, id: result.insertId });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/hr/leaves/:id/approve', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const { status, rejection_reason } = req.body;
        await pool.query('UPDATE hr_leaves SET status=?, approved_by=?, approved_at=NOW(), rejection_reason=? WHERE id=?',
            [status, req.user.id, rejection_reason || null, req.params.id]);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Leave Balance ──
app.get('/api/hr/leave-balance', authenticateToken, async (req, res) => {
    try {
        const userId = req.query.user_id || req.user.id;
        const year = new Date().getFullYear();
        const [policies] = await pool.query('SELECT * FROM hr_leave_policy WHERE is_active=1');
        const [taken] = await pool.query(`SELECT leave_type, COALESCE(SUM(total_days),0) as used
            FROM hr_leaves WHERE user_id=? AND status='Approved' AND YEAR(from_date)=?
            GROUP BY leave_type`, [userId, year]);
        const takenMap = {};
        taken.forEach(t => takenMap[t.leave_type] = t.used);
        const balance = policies.map(p => ({
            leave_type: p.leave_type, total: p.total_days, used: takenMap[p.leave_type] || 0,
            remaining: p.total_days - (takenMap[p.leave_type] || 0), paid: p.paid
        }));
        res.json(balance);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Expense Heads ──
app.get('/api/hr/expense-heads', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM hr_expense_heads WHERE is_active=1 ORDER BY head_name');
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/hr/expense-heads', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const b = req.body;
        const [result] = await pool.query('INSERT INTO hr_expense_heads (head_name, max_limit, requires_receipt, applicable_roles) VALUES (?,?,?,?)',
            [b.head_name, b.max_limit||0, b.requires_receipt?1:0, b.applicable_roles||'All']);
        res.json({ success: true, id: result.insertId });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Expenses ──
app.get('/api/hr/expenses', authenticateToken, async (req, res) => {
    try {
        const { status, from_date, to_date } = req.query;
        let sql = `SELECT ex.*, u.name as user_name, e.emp_code, h.head_name, a.name as approver_name
                    FROM hr_expenses ex
                    LEFT JOIN users u ON ex.user_id = u.id
                    LEFT JOIN hr_employees e ON ex.user_id = e.user_id
                    LEFT JOIN hr_expense_heads h ON ex.head_id = h.id
                    LEFT JOIN users a ON ex.approved_by = a.id WHERE 1=1`;
        const params = [];
        if (status) { sql += ' AND ex.status=?'; params.push(status); }
        if (from_date) { sql += ' AND ex.expense_date>=?'; params.push(from_date); }
        if (to_date) { sql += ' AND ex.expense_date<=?'; params.push(to_date); }
        if (req.user.role !== 'admin') { sql += ' AND ex.user_id=?'; params.push(req.user.id); }
        sql += ' ORDER BY ex.created_at DESC LIMIT 300';
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/hr/expenses', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        const [result] = await pool.query(`INSERT INTO hr_expenses (user_id, expense_date, head_id, amount, description, task_id)
            VALUES (?,?,?,?,?,?)`, [req.user.id, b.expense_date, b.head_id, b.amount, b.description, b.task_id||null]);
        res.json({ success: true, id: result.insertId });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/hr/expenses/:id/approve', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const { status, rejection_reason } = req.body;
        await pool.query('UPDATE hr_expenses SET status=?, approved_by=?, approved_at=NOW(), rejection_reason=? WHERE id=?',
            [status, req.user.id, rejection_reason || null, req.params.id]);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: GPS Logs ──
app.post('/api/hr/gps-log', authenticateToken, async (req, res) => {
    try {
        const { latitude, longitude, accuracy, address, log_type } = req.body;
        await pool.query('INSERT INTO hr_gps_logs (user_id, latitude, longitude, accuracy, address, log_type) VALUES (?,?,?,?,?,?)',
            [req.user.id, latitude, longitude, accuracy, address, log_type || 'auto']);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/hr/gps-logs', authenticateToken, async (req, res) => {
    try {
        const { user_id, date } = req.query;
        const targetUser = (req.user.role === 'admin' && user_id) ? user_id : req.user.id;
        const targetDate = date || new Date().toISOString().split('T')[0];
        const [rows] = await pool.query(`SELECT * FROM hr_gps_logs WHERE user_id=? AND DATE(captured_at)=? ORDER BY captured_at`,
            [targetUser, targetDate]);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Configuration ──
app.get('/api/hr/config', authenticateToken, async (req, res) => {
    try {
        const { category } = req.query;
        let sql = 'SELECT * FROM hr_config';
        const params = [];
        if (category) { sql += ' WHERE category=?'; params.push(category); }
        sql += ' ORDER BY category, config_key';
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/hr/config/:key', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const { value } = req.body;
        await pool.query('UPDATE hr_config SET config_value=?, updated_by=? WHERE config_key=?',
            [JSON.stringify(value), req.user.id, req.params.key]);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Manual Reports ──
app.post('/api/hr/manual-report', authenticateToken, async (req, res) => {
    try {
        const b = req.body;
        await pool.query(`INSERT INTO hr_manual_reports (user_id, report_date, check_in_time, check_out_time, location, work_summary)
            VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE check_in_time=VALUES(check_in_time), check_out_time=VALUES(check_out_time),
            location=VALUES(location), work_summary=VALUES(work_summary)`,
            [req.user.id, b.report_date, b.check_in_time, b.check_out_time, b.location, b.work_summary]);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Comparison Engine ──
app.get('/api/hr/comparison', authenticateToken, async (req, res) => {
    try {
        const { user_id, from_date, to_date } = req.query;
        let sql = `SELECT c.*, u.name as user_name, e.emp_code
            FROM hr_comparison_results c
            LEFT JOIN users u ON c.user_id = u.id
            LEFT JOIN hr_employees e ON c.user_id = e.user_id WHERE 1=1`;
        const params = [];
        if (user_id) { sql += ' AND c.user_id=?'; params.push(user_id); }
        if (from_date) { sql += ' AND c.comparison_date>=?'; params.push(from_date); }
        if (to_date) { sql += ' AND c.comparison_date<=?'; params.push(to_date); }
        if (req.user.role !== 'admin') { sql += ' AND c.user_id=?'; params.push(req.user.id); }
        sql += ' ORDER BY c.comparison_date DESC LIMIT 200';
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Self-Healing — Error Logs ──
app.get('/api/hr/errors', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const { status, module: mod } = req.query;
        let sql = 'SELECT * FROM hr_error_logs WHERE 1=1';
        const params = [];
        if (status) { sql += ' AND status=?'; params.push(status); }
        if (mod) { sql += ' AND module=?'; params.push(mod); }
        sql += ' ORDER BY created_at DESC LIMIT 100';
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/hr/fix-logs', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const [rows] = await pool.query(`SELECT f.*, e.module, e.error_type, e.severity as error_severity
            FROM hr_fix_logs f LEFT JOIN hr_error_logs e ON f.error_id = e.id ORDER BY f.applied_at DESC LIMIT 100`);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Self-Test Module ──
app.get('/api/hr/test-results', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const [rows] = await pool.query('SELECT * FROM hr_test_results ORDER BY run_at DESC LIMIT 100');
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/hr/run-tests', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const testResults = [];
        const runTest = async (mod, name, type, testFn) => {
            const start = Date.now();
            try {
                const result = await testFn();
                const ms = Date.now() - start;
                const status = result.pass ? 'Pass' : 'Fail';
                await pool.query(`INSERT INTO hr_test_results (test_module, test_name, test_type, status, execution_time_ms, details, error_message)
                    VALUES (?,?,?,?,?,?,?)`, [mod, name, type, status, ms, JSON.stringify(result.details || {}), result.error || null]);
                testResults.push({ module: mod, name, status, ms, details: result.details });
            } catch(e) {
                const ms = Date.now() - start;
                await pool.query(`INSERT INTO hr_test_results (test_module, test_name, test_type, status, execution_time_ms, error_message)
                    VALUES (?,?,?,?,?,?)`, [mod, name, type, 'Fail', ms, e.message]);
                testResults.push({ module: mod, name, status: 'Fail', ms, error: e.message });
            }
        };

        // Test 1: Database connectivity
        await runTest('System', 'Database Connection', 'unit', async () => {
            const [r] = await pool.query('SELECT 1 as ok');
            return { pass: r[0].ok === 1, details: { connected: true } };
        });

        // Test 2: HR Tables exist
        await runTest('System', 'HR Tables Integrity', 'integration', async () => {
            const tables = ['hr_employees','hr_attendance','hr_leaves','hr_expenses','hr_gps_logs','hr_config',
                'hr_error_logs','hr_fix_logs','hr_test_results','hr_system_health'];
            const missing = [];
            for (const t of tables) {
                const [r] = await pool.query(`SELECT COUNT(*) as c FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?`, [t]);
                if (r[0].c === 0) missing.push(t);
            }
            return { pass: missing.length === 0, details: { totalTables: tables.length, missing } };
        });

        // Test 3: Config integrity
        await runTest('Config', 'Configuration Completeness', 'validation', async () => {
            const requiredKeys = ['office_start_time','office_end_time','late_threshold_mins','half_day_hours','full_day_hours'];
            const [configs] = await pool.query('SELECT config_key FROM hr_config');
            const existing = configs.map(c => c.config_key);
            const missing = requiredKeys.filter(k => !existing.includes(k));
            return { pass: missing.length === 0, details: { total: requiredKeys.length, missing } };
        });

        // Test 4: Leave policy check
        await runTest('Leave', 'Leave Policy Exists', 'validation', async () => {
            const [r] = await pool.query('SELECT COUNT(*) as c FROM hr_leave_policy WHERE is_active=1');
            return { pass: r[0].c > 0, details: { activePolicies: r[0].c } };
        });

        // Test 5: Expense heads check
        await runTest('Expense', 'Expense Heads Exist', 'validation', async () => {
            const [r] = await pool.query('SELECT COUNT(*) as c FROM hr_expense_heads WHERE is_active=1');
            return { pass: r[0].c > 0, details: { activeHeads: r[0].c } };
        });

        // Test 6: Attendance data consistency
        await runTest('Attendance', 'Attendance Data Consistency', 'validation', async () => {
            const [bad] = await pool.query('SELECT COUNT(*) as c FROM hr_attendance WHERE check_out IS NOT NULL AND total_hours<=0');
            return { pass: bad[0].c === 0, details: { inconsistentRecords: bad[0].c } };
        });

        // Update system health
        const passCount = testResults.filter(t => t.status === 'Pass').length;
        const healthPct = (passCount / testResults.length) * 100;
        await pool.query(`INSERT INTO hr_system_health (metric_name, metric_value, metric_unit, status, details)
            VALUES ('test_pass_rate', ?, 'percent', ?, ?)`,
            [healthPct, healthPct >= 80 ? 'Healthy' : healthPct >= 50 ? 'Warning' : 'Critical',
             JSON.stringify({ total: testResults.length, passed: passCount })]);

        res.json({ success: true, results: testResults, summary: { total: testResults.length, passed: passCount, failed: testResults.length - passCount } });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: System Health ──
app.get('/api/hr/system-health', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM hr_system_health ORDER BY checked_at DESC LIMIT 50');
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Demo Data Generator ──
app.post('/api/hr/generate-demo', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const conn = await pool.getConnection();
        try {
            const depts = ['Technology','Sales','HR','Finance','Operations','Marketing'];
            const designations = ['Software Engineer','Sr. Engineer','Team Lead','Manager','Executive','Analyst','Coordinator'];
            const cities = ['Delhi','Mumbai','Bangalore','Hyderabad','Chennai','Pune','Kolkata'];
            const bloodGroups = ['A+','A-','B+','B-','O+','O-','AB+','AB-'];

            // Get all users
            const [users] = await conn.query('SELECT id, name FROM users LIMIT 20');
            let empCount = 0;

            for (const u of users) {
                const [exists] = await conn.query('SELECT id FROM hr_employees WHERE user_id=?', [u.id]);
                if (exists.length) continue;

                const dept = depts[Math.floor(Math.random() * depts.length)];
                const desig = designations[Math.floor(Math.random() * designations.length)];
                const city = cities[Math.floor(Math.random() * cities.length)];
                const salary = Math.round((25000 + Math.random() * 75000) / 1000) * 1000;

                await conn.query(`INSERT INTO hr_employees (user_id, emp_code, designation, department, date_of_joining, gender, phone, city, state, blood_group, base_salary, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Active')`,
                    [u.id, 'CI' + String(u.id).padStart(4,'0'), desig, dept,
                     '2024-' + String(1+Math.floor(Math.random()*12)).padStart(2,'0') + '-01',
                     Math.random() > 0.5 ? 'Male' : 'Female',
                     '9' + String(Math.floor(1000000000 + Math.random() * 9000000000)).substring(0,9),
                     city, 'India', bloodGroups[Math.floor(Math.random() * bloodGroups.length)], salary]);
                empCount++;
            }

            // Generate 30 days attendance for all employees
            const [emps] = await conn.query('SELECT user_id FROM hr_employees WHERE status="Active"');
            let attCount = 0;
            for (const emp of emps) {
                for (let d = 29; d >= 0; d--) {
                    const date = new Date(); date.setDate(date.getDate() - d);
                    if (date.getDay() === 0) continue; // Skip Sunday
                    const dateStr = date.toISOString().split('T')[0];
                    const [exists] = await conn.query('SELECT id FROM hr_attendance WHERE user_id=? AND attendance_date=?', [emp.user_id, dateStr]);
                    if (exists.length) continue;

                    const rand = Math.random();
                    if (rand < 0.08) { // 8% absent
                        await conn.query('INSERT INTO hr_attendance (user_id, attendance_date, status, source) VALUES (?,?,?,?)',
                            [emp.user_id, dateStr, 'Absent', 'system']);
                    } else {
                        const lateRand = Math.random();
                        const checkInH = lateRand < 0.15 ? 10 : 9;
                        const checkInM = Math.floor(Math.random() * 30) + (lateRand < 0.15 ? 0 : 15);
                        const checkOutH = 17 + Math.floor(Math.random() * 3);
                        const checkOutM = Math.floor(Math.random() * 60);
                        const totalH = ((checkOutH*60+checkOutM) - (checkInH*60+checkInM)) / 60;
                        const status = lateRand < 0.15 ? 'Late' : 'Present';
                        const lat = 28.6 + (Math.random() * 0.1);
                        const lng = 77.2 + (Math.random() * 0.1);

                        await conn.query(`INSERT INTO hr_attendance (user_id, attendance_date, check_in, check_out, check_in_lat, check_in_lng,
                            total_hours, status, source) VALUES (?,?,?,?,?,?,?,?,?)`,
                            [emp.user_id, dateStr,
                             `${String(checkInH).padStart(2,'0')}:${String(checkInM).padStart(2,'0')}:00`,
                             `${String(checkOutH).padStart(2,'0')}:${String(checkOutM).padStart(2,'0')}:00`,
                             lat, lng, totalH.toFixed(2), status, 'system']);
                    }
                    attCount++;
                }
            }

            // Generate some leave requests
            let leaveCount = 0;
            for (const emp of emps.slice(0, 5)) {
                const leaveTypes = ['Casual Leave','Sick Leave','Earned Leave'];
                const lt = leaveTypes[Math.floor(Math.random() * leaveTypes.length)];
                const fromD = new Date(); fromD.setDate(fromD.getDate() + Math.floor(Math.random()*30));
                const toD = new Date(fromD); toD.setDate(toD.getDate() + Math.floor(Math.random()*3));
                const statuses = ['Pending','Approved','Pending'];
                await conn.query(`INSERT INTO hr_leaves (user_id, leave_type, from_date, to_date, total_days, reason, status)
                    VALUES (?,?,?,?,?,?,?)`,
                    [emp.user_id, lt, fromD.toISOString().split('T')[0], toD.toISOString().split('T')[0],
                     Math.ceil((toD-fromD)/(1000*60*60*24))+1, 'Demo leave request',
                     statuses[Math.floor(Math.random()*statuses.length)]]);
                leaveCount++;
            }

            // Generate some expenses
            let expCount = 0;
            const [heads] = await conn.query('SELECT id FROM hr_expense_heads LIMIT 5');
            for (const emp of emps.slice(0, 8)) {
                for (let i = 0; i < 3; i++) {
                    const head = heads[Math.floor(Math.random() * heads.length)];
                    const amt = Math.round(100 + Math.random() * 4000);
                    const expDate = new Date(); expDate.setDate(expDate.getDate() - Math.floor(Math.random()*30));
                    await conn.query(`INSERT INTO hr_expenses (user_id, expense_date, head_id, amount, description, status)
                        VALUES (?,?,?,?,?,?)`,
                        [emp.user_id, expDate.toISOString().split('T')[0], head.id, amt, 'Demo expense',
                         ['Pending','Approved','Pending'][Math.floor(Math.random()*3)]]);
                    expCount++;
                }
            }

            res.json({ success: true, generated: { employees: empCount, attendance: attCount, leaves: leaveCount, expenses: expCount } });
        } finally { conn.release(); }
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Payroll ──
app.get('/api/hr/payroll', authenticateToken, async (req, res) => {
    try {
        const { month, year } = req.query;
        let sql = `SELECT p.*, u.name as user_name, e.emp_code, e.department, e.designation
            FROM hr_payroll p
            LEFT JOIN users u ON p.user_id = u.id
            LEFT JOIN hr_employees e ON p.user_id = e.user_id WHERE 1=1`;
        const params = [];
        if (month) { sql += ' AND p.month=?'; params.push(month); }
        if (year) { sql += ' AND p.year=?'; params.push(year); }
        if (req.user.role !== 'admin') { sql += ' AND p.user_id=?'; params.push(req.user.id); }
        sql += ' ORDER BY u.name';
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/hr/payroll/generate', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const { month, year } = req.body;
        const [emps] = await pool.query('SELECT * FROM hr_employees WHERE status="Active"');
        const [salaryConfig] = await pool.query("SELECT config_value FROM hr_config WHERE config_key='salary_components'");
        const comp = salaryConfig.length ? JSON.parse(salaryConfig[0].config_value) : { hra_percent:40, da_percent:10, pf_percent:12, esi_percent:1.75 };

        let count = 0;
        for (const emp of emps) {
            const [exists] = await pool.query('SELECT id FROM hr_payroll WHERE user_id=? AND month=? AND year=?', [emp.user_id, month, year]);
            if (exists.length) continue;

            const base = emp.base_salary || 0;
            const hra = base * (comp.hra_percent / 100);
            const da = base * (comp.da_percent / 100);
            const gross = base + hra + da;
            const pf = base * (comp.pf_percent / 100);
            const esi = gross * (comp.esi_percent / 100);

            // Count absent/leave days for deduction
            const [absentDays] = await pool.query(`SELECT COUNT(*) as c FROM hr_attendance
                WHERE user_id=? AND MONTH(attendance_date)=? AND YEAR(attendance_date)=? AND status IN ('Absent')`, [emp.user_id, month, year]);
            const perDaySalary = base / 30;
            const leaveDeduction = absentDays[0].c * perDaySalary;

            const net = gross - pf - esi - leaveDeduction;

            await pool.query(`INSERT INTO hr_payroll (user_id, month, year, base_salary, hra, da, pf_deduction, esi_deduction, leave_deduction, net_salary, status)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
                [emp.user_id, month, year, base, hra.toFixed(2), da.toFixed(2), pf.toFixed(2), esi.toFixed(2), leaveDeduction.toFixed(2), net.toFixed(2), 'Draft']);
            count++;
        }
        res.json({ success: true, generated: count });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Users list (for dropdowns) ──
app.get('/api/hr/users-list', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT id, name, username, role FROM users ORDER BY name');
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Module Registry ──
app.get('/api/hr/modules', authenticateToken, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM hr_module_registry WHERE is_active=1 ORDER BY sort_order');
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Permissions CRUD ──
app.get('/api/hr/permissions', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const { user_id } = req.query;
        let sql = `SELECT p.*, u.name as user_name, u.username, m.module_name, m.module_icon
            FROM hr_module_permissions p
            LEFT JOIN users u ON p.user_id = u.id
            LEFT JOIN hr_module_registry m ON p.module = m.module_key
            WHERE 1=1`;
        const params = [];
        if (user_id) { sql += ' AND p.user_id=?'; params.push(user_id); }
        sql += ' ORDER BY u.name, m.sort_order';
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// Get MY permissions (for frontend access control)
app.get('/api/hr/my-permissions', authenticateToken, async (req, res) => {
    try {
        // Admin gets all permissions automatically
        if (req.user.role === 'admin') {
            const [modules] = await pool.query('SELECT module_key FROM hr_module_registry WHERE is_active=1');
            const perms = {};
            modules.forEach(m => { perms[m.module_key] = { can_view:1, can_create:1, can_edit:1, can_delete:1, can_approve:1, can_export:1 }; });
            return res.json(perms);
        }
        const [rows] = await pool.query('SELECT module, can_view, can_create, can_edit, can_delete, can_approve, can_export FROM hr_module_permissions WHERE user_id=?', [req.user.id]);
        const perms = {};
        rows.forEach(r => { perms[r.module] = r; });
        res.json(perms);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// Set permissions for a user (bulk)
app.post('/api/hr/permissions/set', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const { user_id, permissions } = req.body;
        // permissions = [{ module, can_view, can_create, can_edit, can_delete, can_approve, can_export }]
        if (!user_id || !permissions || !permissions.length) return res.status(400).json({ error: 'user_id and permissions required' });
        const conn = await pool.getConnection();
        try {
            for (const p of permissions) {
                await conn.query(`INSERT INTO hr_module_permissions (user_id, module, can_view, can_create, can_edit, can_delete, can_approve, can_export, granted_by)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ON DUPLICATE KEY UPDATE can_view=VALUES(can_view), can_create=VALUES(can_create), can_edit=VALUES(can_edit),
                    can_delete=VALUES(can_delete), can_approve=VALUES(can_approve), can_export=VALUES(can_export), granted_by=VALUES(granted_by)`,
                    [user_id, p.module, p.can_view?1:0, p.can_create?1:0, p.can_edit?1:0, p.can_delete?1:0, p.can_approve?1:0, p.can_export?1:0, req.user.id]);
            }
            res.json({ success: true });
        } finally { conn.release(); }
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// Delete permissions for a user
app.delete('/api/hr/permissions/:userId', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        await pool.query('DELETE FROM hr_module_permissions WHERE user_id=?', [req.params.userId]);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Expense Heads CRUD (enhanced) ──
app.put('/api/hr/expense-heads/:id', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        const b = req.body;
        await pool.query('UPDATE hr_expense_heads SET head_name=?, max_limit=?, requires_receipt=?, applicable_roles=?, is_active=? WHERE id=?',
            [b.head_name, b.max_limit||0, b.requires_receipt?1:0, b.applicable_roles||'All', b.is_active!==undefined?(b.is_active?1:0):1, req.params.id]);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/hr/expense-heads/:id', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        await pool.query('UPDATE hr_expense_heads SET is_active=0 WHERE id=?', [req.params.id]);
        res.json({ success: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── HR API: Force Initialize Tables (for Vercel cold start) ──
app.post('/api/hr/init-tables', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        await initializeHRTables();
        res.json({ success: true, message: 'All HR tables initialized successfully' });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// Seed default expense heads (if table is empty)
app.post('/api/hr/expense-heads/seed', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
        // Ensure tables exist first (Vercel cold start fix)
        await initializeHRTables();
        const [count] = await pool.query('SELECT COUNT(*) as c FROM hr_expense_heads');
        if (count[0].c > 0) return res.json({ success: true, message: 'Heads already exist', count: count[0].c });
        await pool.query(`INSERT INTO hr_expense_heads (head_name, max_limit, requires_receipt, applicable_roles) VALUES
            ('Travel - Local', 2000, 1, 'All'),
            ('Travel - Outstation', 10000, 1, 'All'),
            ('Food & Meals', 500, 0, 'All'),
            ('Accommodation / Hotel', 5000, 1, 'All'),
            ('Office Supplies', 1000, 1, 'All'),
            ('Communication / Mobile', 500, 0, 'All'),
            ('Client Entertainment', 3000, 1, 'Manager,Admin'),
            ('Fuel & Vehicle', 5000, 1, 'All'),
            ('Medical', 2000, 1, 'All'),
            ('Tour & Travelling', 8000, 1, 'All'),
            ('Courier & Postage', 500, 1, 'All'),
            ('Printing & Stationery', 1000, 1, 'All'),
            ('Internet & Software', 2000, 1, 'All'),
            ('Repair & Maintenance', 3000, 1, 'All'),
            ('Miscellaneous', 1000, 1, 'All')`);
        res.json({ success: true, seeded: 15 });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════════════════════════
// VOICE BOT APIs (Public - No Auth Required)
// ══════════════════════════════════════════════════════════════════════════════

// Voice Transcribe - Whisper STT
const voiceUpload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 25 * 1024 * 1024 } });
app.post('/api/voice/transcribe', voiceUpload.single('audio'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ error: 'No audio file' });
        if (!process.env.OPENAI_API_KEY) return res.status(500).json({ error: 'OpenAI API key not configured' });

        const tempPath = path.join(os.tmpdir(), `voice_${Date.now()}.webm`);
        fs.writeFileSync(tempPath, req.file.buffer);

        const transcription = await openaiClient.audio.transcriptions.create({
            model: 'whisper-1',
            file: fs.createReadStream(tempPath),
            language: undefined, // auto-detect Hindi/English
        });

        fs.unlinkSync(tempPath);
        res.json({ text: transcription.text, language: 'auto' });
    } catch(e) {
        console.error('Voice transcribe error:', e);
        res.status(500).json({ error: e.message });
    }
});

// Voice TTS - Text to Speech
app.post('/api/voice/speak', async (req, res) => {
    try {
        const { text, language } = req.body;
        if (!text) return res.status(400).json({ error: 'No text provided' });
        if (!process.env.OPENAI_API_KEY) return res.status(500).json({ error: 'OpenAI API key not configured' });

        const mp3 = await openaiClient.audio.speech.create({
            model: 'tts-1',
            voice: 'nova',
            input: text,
            speed: 0.95,
        });

        const buffer = Buffer.from(await mp3.arrayBuffer());
        res.set({ 'Content-Type': 'audio/mpeg', 'Content-Length': buffer.length });
        res.send(buffer);
    } catch(e) {
        console.error('Voice speak error:', e);
        res.status(500).json({ error: e.message });
    }
});

// ══════ Gemini AI Helper for Voice Bot ══════
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || 'AIzaSyA8TIlYCt8zIlp64Z7UJbkSFQ6DwPkIrAY';
const GEMINI_MODEL = 'gemini-2.0-flash';
const GEMINI_URL = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}`;

async function callGemini(systemPrompt, userMessage) {
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 8000);
        const res = await fetch(GEMINI_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            signal: controller.signal,
            body: JSON.stringify({
                contents: [{ role: 'user', parts: [{ text: systemPrompt + '\n\nUser message: ' + userMessage }] }],
                generationConfig: { temperature: 0.3, maxOutputTokens: 500, responseMimeType: 'application/json' }
            })
        });
        clearTimeout(timeout);
        const data = await res.json();
        const text = data.candidates?.[0]?.content?.parts?.[0]?.text;
        if (!text) return null;
        return JSON.parse(text);
    } catch (e) {
        console.error('Gemini API error:', e.message);
        return null;
    }
}

// Voice Chat - Gemini AI Powered Complaint Flow
app.post('/api/voice/chat', async (req, res) => {
    try {
        const { message, session } = req.body;
        if (!message) return res.status(400).json({ error: 'No message' });

        const step = (session && session.step) || 'greeting';
        const data = (session && session.data) || {};
        let reply = '';
        let nextStep = step;
        let nextData = { ...data };
        let action = null;
        const lang = (data && data.lang) || 'hi';
        const R = (hi, en) => lang === 'en' ? en : hi;

        // ── Greeting step (no AI needed) ──
        if (step === 'greeting') {
            reply = 'नमस्ते! कोरल इंफ्राटेल सपोर्ट में आपका स्वागत है।\nWelcome to Coral Infratel Support!\n\nकृपया भाषा चुनें / Please choose language:\n1️⃣ हिंदी\n2️⃣ English';
            nextStep = 'choose_lang';
            res.json({ reply, session: { step: nextStep, data: nextData }, action });
            return;
        }

        // ── Submitted / Done steps (no AI needed) ──
        if (step === 'submitted') {
            reply = R(
                'आपकी कंप्लेंट रजिस्टर हो गई है!\n• टिकट आईडी: ' + (data.ticket_id || '') + '\n• कंप्लेंट नंबर: ' + (data.complaint_no || '') + '\n\nआपको ईमेल और व्हाट्सएप पर सूचना मिलेगी। धन्यवाद!',
                'Your complaint has been registered!\n• Ticket ID: ' + (data.ticket_id || '') + '\n• Complaint No: ' + (data.complaint_no || '') + '\n\nYou will receive notification via Email and WhatsApp. Thank you!'
            );
            nextStep = 'done';
            res.json({ reply, session: { step: nextStep, data: nextData }, action });
            return;
        }

        if (step === 'verify_customer') {
            if (data.customer_found) {
                reply = R('कस्टमर का नाम ' + data.customer_name + ' है। क्या यह सही है? हाँ या ना बोलें।', 'Customer name is ' + data.customer_name + '. Is this correct? Say Yes or No.');
                nextStep = 'confirm_customer';
            } else {
                reply = R('इस एसटीडी कोड और टेलीफोन नंबर से कोई कस्टमर नहीं मिला। कृपया दोबारा चेक करें। अपना एसटीडी कोड बताएं।', 'No customer found with this STD Code and Telephone. Please check again. Tell your STD Code.');
                nextStep = 'ask_std';
                nextData = { lang: data.lang };
            }
            res.json({ reply, session: { step: nextStep, data: nextData }, action });
            return;
        }

        // ── Build Gemini system prompt based on current step ──
        const collectedFields = [];
        if (data.lang) collectedFields.push('language: ' + data.lang);
        if (data.std_code) collectedFields.push('std_code: ' + data.std_code);
        if (data.telephone_number) collectedFields.push('telephone: ' + data.telephone_number);
        if (data.customer_name) collectedFields.push('customer_name: ' + data.customer_name);
        if (data.complainee_name) collectedFields.push('complainee_name: ' + data.complainee_name);
        if (data.mobile) collectedFields.push('mobile: ' + data.mobile);
        if (data.email) collectedFields.push('email: ' + data.email);
        if (data.description) collectedFields.push('description: ' + data.description);

        const geminiSystemPrompt = `You are the AI brain of Coral Infratel's customer support chatbot. Your job is to understand what the user means and return structured JSON.

CURRENT STEP: ${step}
LANGUAGE: ${lang} (hi=Hindi, en=English)
COLLECTED DATA: ${collectedFields.length > 0 ? collectedFields.join(', ') : 'none yet'}
${data.has_duplicate ? 'DUPLICATE COMPLAINT EXISTS: ticket ' + data.duplicate_ticket : ''}
${data.customer_found ? 'CUSTOMER VERIFIED: ' + data.customer_name : ''}

CONVERSATION FLOW (steps in order):
1. choose_lang → user picks Hindi(1) or English(2)
2. ask_std → collect STD code (like 0129, 0131, 011). Must be 2+ digits starting with 0
3. ask_phone → collect telephone/landline number (6+ digits)
4. verify_customer → system checks DB (handled separately)
5. confirm_customer → user confirms customer name (yes/no)
6. ask_name → collect complainant's full name
7. ask_mobile → collect 10-digit mobile number
8. ask_email → collect email address (must have @ and .)
9. ask_issue → collect issue/complaint description (5+ chars)
10. confirm_submit → user confirms all details to submit (yes/no)

RULES:
- If user seems to want to change language at any step, set intent to "change_language"
- If user wants to correct/fix a previously entered field, set intent to "correction" and specify which field
- If user says yes/haan/ji/हाँ/हां → intent is "confirm_yes"
- If user says no/nahi/नहीं → intent is "confirm_no"
- For ask_std: STD codes are Indian telephone area codes (0129, 0131, 011, 01onal etc). Single digit "1" or "2" alone is NOT a valid STD code - it's likely a language selection attempt
- For ask_mobile: extract exactly 10 digits
- For ask_email: look for email pattern with @ and .
- Always generate reply_hi in Devanagari Hindi and reply_en in English
- Keep replies short, friendly, and professional (1-2 sentences max)
- If user input is unclear for the current step, ask them to clarify politely

RESPOND WITH THIS EXACT JSON STRUCTURE:
{
  "intent": "provide_data | change_language | correction | confirm_yes | confirm_no | unclear | restart",
  "extracted_value": "the extracted value if any, or empty string",
  "field": "which field: lang | std_code | telephone_number | complainee_name | mobile | email | description | none",
  "correction_field": "if intent is correction, which field to fix: std_code | telephone_number | complainee_name | mobile | email | description | none",
  "new_lang": "if changing language: hi or en, otherwise empty",
  "reply_hi": "Hindi reply in Devanagari script",
  "reply_en": "English reply"
}`;

        // ── Call Gemini AI ──
        const ai = await callGemini(geminiSystemPrompt, message);

        // ── If Gemini fails, use basic fallback ──
        if (!ai) {
            console.log('Gemini failed, using fallback for step:', step);
            const fallback = voiceBotFallback(step, message, data, nextData);
            res.json({ reply: fallback.reply, session: { step: fallback.nextStep, data: fallback.nextData }, action: fallback.action });
            return;
        }

        console.log('Gemini response:', JSON.stringify(ai));

        // ── Process Gemini response ──
        const intent = ai.intent || 'unclear';
        const extracted = (ai.extracted_value || '').trim();
        const replyText = lang === 'en' ? (ai.reply_en || ai.reply_hi || '') : (ai.reply_hi || ai.reply_en || '');

        // Handle language change from any step
        if (intent === 'change_language') {
            const newLang = ai.new_lang || (extracted === '2' || extracted.toLowerCase().includes('eng') ? 'en' : 'hi');
            nextData.lang = newLang;
            reply = newLang === 'en' ? (ai.reply_en || 'Language changed to English.') : (ai.reply_hi || 'भाषा हिंदी में बदल दी गई।');
            // Stay on same step or move to ask_std if on choose_lang
            if (step === 'choose_lang') {
                // Check form data and skip to right step
                if (data.customer_found && data.customer_name && data.std_code && data.telephone_number) {
                    if (data.complainee_name && data.mobile && data.email && data.description) { nextStep = 'confirm_submit'; }
                    else if (data.complainee_name && data.mobile && data.email) { nextStep = 'ask_issue'; }
                    else if (data.complainee_name && data.mobile) { nextStep = 'ask_email'; }
                    else if (data.complainee_name) { nextStep = 'ask_mobile'; }
                    else { nextStep = 'ask_name'; }
                } else if (data.std_code) { nextStep = 'ask_phone'; }
                else { nextStep = 'ask_std'; }
                // Append next step instruction to reply
                const nR = (hi, en) => newLang === 'en' ? en : hi;
                if (nextStep === 'ask_std') reply += nR(' कृपया अपना एसटीडी कोड बताएं, जैसे 0129 या 0131।', ' Please tell your STD Code (like 0129, 0131).');
            }
            // If not choose_lang, stay on current step
            res.json({ reply, session: { step: nextStep, data: nextData }, action });
            return;
        }

        // Handle corrections
        if (intent === 'correction') {
            const fixField = ai.correction_field || ai.field || 'none';
            reply = replyText;
            if (fixField === 'complainee_name' || fixField === 'name') {
                delete nextData.complainee_name; nextStep = 'ask_name';
                reply = reply || R('ठीक है, अपना सही नाम बताएं।', 'Okay, please tell your correct name.');
            } else if (fixField === 'mobile') {
                delete nextData.mobile; nextStep = 'ask_mobile';
                reply = reply || R('ठीक है, अपना सही मोबाइल नंबर बताएं।', 'Okay, please tell your correct mobile number.');
            } else if (fixField === 'email') {
                delete nextData.email; nextStep = 'ask_email';
                reply = reply || R('ठीक है, अपनी सही ईमेल आईडी बताएं।', 'Okay, please tell your correct email address.');
            } else if (fixField === 'description') {
                delete nextData.description; nextStep = 'ask_issue';
                reply = reply || R('ठीक है, अपनी समस्या फिर से बताएं।', 'Okay, please describe your issue again.');
            } else if (fixField === 'std_code') {
                delete nextData.std_code; delete nextData.telephone_number; delete nextData.customer_found; delete nextData.customer_name;
                nextStep = 'ask_std';
                reply = reply || R('ठीक है, अपना सही एसटीडी कोड बताएं।', 'Okay, please tell your correct STD Code.');
            } else if (fixField === 'telephone_number') {
                delete nextData.telephone_number; delete nextData.customer_found; delete nextData.customer_name;
                nextStep = 'ask_phone';
                reply = reply || R('ठीक है, अपना सही टेलीफोन नंबर बताएं।', 'Okay, please tell your correct telephone number.');
            } else {
                reply = reply || R(
                    'क्या ठीक करना है? बताएं: नाम, मोबाइल, ईमेल, समस्या, एसटीडी कोड, या टेलीफोन।',
                    'What do you want to fix? Say: name, mobile, email, issue, STD code, or telephone.'
                );
            }
            res.json({ reply, session: { step: nextStep, data: nextData }, action });
            return;
        }

        // ── Step-specific processing with AI understanding ──
        switch (step) {
            case 'choose_lang': {
                // AI already detected intent, process it
                if (intent === 'confirm_yes' || extracted === '1' || extracted === 'hi' || extracted.includes('हिंदी') || extracted.includes('hindi')) {
                    nextData.lang = 'hi';
                } else {
                    nextData.lang = extracted === '2' || extracted.toLowerCase().includes('eng') ? 'en' : 'hi';
                }
                const L = nextData.lang;
                reply = L === 'en' ? (ai.reply_en || 'Great! You selected English.') : (ai.reply_hi || 'बहुत अच्छा! आपने हिंदी चुनी है।');
                // Check form data and skip to right step
                if (data.customer_found && data.customer_name && data.std_code && data.telephone_number) {
                    if (data.complainee_name && data.mobile && data.email && data.description) { nextStep = 'confirm_submit'; }
                    else if (data.complainee_name && data.mobile && data.email) { nextStep = 'ask_issue'; }
                    else if (data.complainee_name && data.mobile) { nextStep = 'ask_email'; }
                    else if (data.complainee_name) { nextStep = 'ask_mobile'; }
                    else { nextStep = 'ask_name'; }
                } else if (data.std_code) { nextStep = 'ask_phone'; }
                else {
                    nextStep = 'ask_std';
                    const nR = (hi, en) => L === 'en' ? en : hi;
                    reply += nR(' कृपया अपना एसटीडी कोड बताएं, जैसे 0129 या 0131।', ' Please tell your STD Code (like 0129, 0131).');
                }
                break;
            }

            case 'ask_std': {
                const stdCode = extracted.replace(/[^0-9]/g, '');
                if (!stdCode || stdCode.length < 2) {
                    reply = replyText || R('यह सही एसटीडी कोड नहीं है। कृपया सिर्फ नंबर में बताएं, जैसे 0129 या 0131।', 'Invalid STD Code. Please enter only numbers like 0129, 0131.');
                } else {
                    nextData.std_code = stdCode;
                    reply = replyText || R('एसटीडी कोड ' + stdCode + '। अब अपना टेलीफोन नंबर बताएं।', 'STD Code ' + stdCode + '. Now tell your Telephone Number.');
                    nextStep = 'ask_phone';
                }
                break;
            }

            case 'ask_phone': {
                const phone = extracted.replace(/[^0-9]/g, '');
                if (!phone || phone.length < 6) {
                    reply = replyText || R('यह सही टेलीफोन नंबर नहीं है। कृपया सिर्फ नंबर में बताएं।', 'Invalid telephone number. Please enter only numbers.');
                } else {
                    nextData.telephone_number = phone;
                    reply = replyText || R('टेलीफोन नंबर ' + phone + '। आपका अकाउंट वेरीफाई कर रहे हैं।', 'Telephone ' + phone + '. Verifying your account...');
                    nextStep = 'verify_customer';
                    action = 'lookup';
                }
                break;
            }

            case 'confirm_customer': {
                if (intent === 'confirm_yes') {
                    if (data.has_duplicate) {
                        reply = R('आपकी एक कंप्लेंट पहले से रजिस्टर्ड है। टिकट नंबर ' + data.duplicate_ticket + '। नई कंप्लेंट तब तक नहीं हो सकती जब तक पुरानी हल न हो।', 'You already have a registered complaint. Ticket: ' + data.duplicate_ticket + '. New complaint cannot be registered until the previous one is resolved.');
                        nextStep = 'done';
                    } else {
                        reply = replyText || R('बहुत अच्छा! अब अपना पूरा नाम बताएं जो कंप्लेंट में लिखना है।', 'Great! Now tell your full name for the complaint.');
                        nextStep = 'ask_name';
                    }
                } else {
                    reply = replyText || R('ठीक है, फिर से कोशिश करते हैं। अपना एसटीडी कोड बताएं।', 'Okay, let\'s try again. Tell your STD Code.');
                    nextStep = 'ask_std';
                    nextData = { lang: data.lang };
                }
                break;
            }

            case 'ask_name': {
                const name = extracted || message.trim();
                if (name.length < 2) {
                    reply = replyText || R('कृपया अपना पूरा नाम बताएं।', 'Please tell your full name.');
                } else {
                    nextData.complainee_name = name;
                    reply = replyText || R('नाम ' + name + '। अब अपना दस अंकों का मोबाइल नंबर बताएं।', 'Name: ' + name + '. Now tell your 10-digit mobile number.');
                    nextStep = 'ask_mobile';
                }
                break;
            }

            case 'ask_mobile': {
                const mobile = (extracted || message).replace(/[^0-9]/g, '');
                if (!mobile || mobile.length !== 10) {
                    reply = replyText || R('कृपया दस अंकों का मोबाइल नंबर बताएं।', 'Please enter a valid 10-digit mobile number.');
                } else {
                    nextData.mobile = mobile;
                    reply = replyText || R('मोबाइल ' + mobile + '। अब अपनी ईमेल आईडी बताएं।', 'Mobile: ' + mobile + '. Now tell your Email ID.');
                    nextStep = 'ask_email';
                }
                break;
            }

            case 'ask_email': {
                const email = (extracted || message).trim().toLowerCase();
                if (!email.includes('@') || !email.includes('.')) {
                    reply = replyText || R('यह सही ईमेल नहीं लग रहा। कृपया सही ईमेल टाइप करें।', 'This doesn\'t look like a valid email. Please type your correct email.');
                } else {
                    nextData.email = email;
                    reply = replyText || R('ईमेल ' + email + '। अब अपनी समस्या बताएं। क्या परेशानी है?', 'Email: ' + email + '. Now describe your issue. What is the problem?');
                    nextStep = 'ask_issue';
                }
                break;
            }

            case 'ask_issue': {
                const issue = extracted || message.trim();
                if (issue.length < 5) {
                    reply = replyText || R('कृपया अपनी समस्या थोड़ी विस्तार से बताएं।', 'Please describe your issue in more detail.');
                } else {
                    nextData.description = issue;
                    reply = R(
                        'कृपया पुष्टि करें:\n• नाम: ' + nextData.complainee_name + '\n• मोबाइल: ' + nextData.mobile + '\n• ईमेल: ' + nextData.email + '\n• समस्या: ' + issue.substring(0, 100) + '\n\nक्या कंप्लेंट सबमिट करें? हाँ या ना बोलें।',
                        'Please confirm:\n• Name: ' + nextData.complainee_name + '\n• Mobile: ' + nextData.mobile + '\n• Email: ' + nextData.email + '\n• Issue: ' + issue.substring(0, 100) + '\n\nSubmit complaint? Say Yes or No.'
                    );
                    nextStep = 'confirm_submit';
                }
                break;
            }

            case 'confirm_submit': {
                if (intent === 'confirm_yes') {
                    reply = R('कंप्लेंट सबमिट हो रही है...', 'Submitting your complaint...');
                    nextStep = 'submitting';
                    action = 'submit';
                } else {
                    reply = replyText || R('कंप्लेंट रद्द कर दी गई। क्या दोबारा कोशिश करना चाहेंगे?', 'Complaint cancelled. Would you like to try again?');
                    nextStep = 'ask_std';
                    nextData = { lang: data.lang };
                }
                break;
            }

            case 'done': {
                if (intent === 'confirm_yes') {
                    nextStep = 'ask_std';
                    nextData = { lang: data.lang };
                    reply = R('ठीक है! अपना एसटीडी कोड बताएं।', 'Okay! Tell your STD Code.');
                } else {
                    reply = replyText || R('धन्यवाद! कोरल इंफ्राटेल सपोर्ट से बात करने के लिए शुक्रिया।', 'Thank you for contacting Coral Infratel Support!');
                }
                break;
            }

            default:
                reply = R('कुछ गड़बड़ हो गई। फिर से शुरू करते हैं। अपना एसटीडी कोड बताएं।', 'Something went wrong. Let\'s start again. Tell your STD Code.');
                nextStep = 'ask_std';
                nextData = { lang: data.lang };
        }

        res.json({ reply, session: { step: nextStep, data: nextData }, action });
    } catch(e) {
        console.error('Voice chat error:', e);
        res.status(500).json({ error: e.message });
    }
});

// ── Fallback rule-based logic (when Gemini is unavailable) ──
function voiceBotFallback(step, message, data, nextData) {
    const lang = (data && data.lang) || 'hi';
    const R = (hi, en) => lang === 'en' ? en : hi;
    const YES_CHECK = (msg) => {
        const l = msg.toLowerCase().trim();
        return l.includes('haan') || l.includes('han') || l.includes('yes') || l.includes('ha') || l.includes('ji') || l === 'y' || l.includes('हाँ') || l.includes('हां') || l.includes('जी');
    };
    let reply = '', nextStep = step, action = null;
    nextData = { ...data };

    switch (step) {
        case 'choose_lang': {
            const m = message.toLowerCase().trim();
            if (m.includes('eng') || m === '2' || m.includes('english')) { nextData.lang = 'en'; reply = 'Great! You selected English.'; }
            else { nextData.lang = 'hi'; reply = 'बहुत अच्छा! आपने हिंदी चुनी है।'; }
            const L = nextData.lang;
            reply += L === 'en' ? ' Please tell your STD Code (like 0129, 0131).' : ' कृपया अपना एसटीडी कोड बताएं, जैसे 0129 या 0131।';
            nextStep = 'ask_std';
            break;
        }
        case 'ask_std': {
            const stdCode = message.replace(/[^0-9]/g, '').trim();
            if (!stdCode || stdCode.length < 2) { reply = R('यह सही एसटीडी कोड नहीं है। कृपया जैसे 0129, 0131।', 'Invalid STD Code. Please enter like 0129, 0131.'); }
            else { nextData.std_code = stdCode; reply = R('एसटीडी कोड ' + stdCode + '। अब टेलीफोन नंबर बताएं।', 'STD Code ' + stdCode + '. Now tell Telephone Number.'); nextStep = 'ask_phone'; }
            break;
        }
        case 'ask_phone': {
            const phone = message.replace(/[^0-9]/g, '').trim();
            if (!phone || phone.length < 6) { reply = R('सही टेलीफोन नंबर बताएं।', 'Enter valid telephone number.'); }
            else { nextData.telephone_number = phone; reply = R('वेरीफाई कर रहे हैं...', 'Verifying...'); nextStep = 'verify_customer'; action = 'lookup'; }
            break;
        }
        case 'confirm_customer': {
            if (YES_CHECK(message)) {
                if (data.has_duplicate) { reply = R('कंप्लेंट पहले से है। टिकट: ' + data.duplicate_ticket, 'Complaint exists. Ticket: ' + data.duplicate_ticket); nextStep = 'done'; }
                else { reply = R('अपना पूरा नाम बताएं।', 'Tell your full name.'); nextStep = 'ask_name'; }
            } else { reply = R('फिर से कोशिश करें। एसटीडी कोड बताएं।', 'Try again. Tell STD Code.'); nextStep = 'ask_std'; nextData = { lang: data.lang }; }
            break;
        }
        case 'ask_name': {
            if (message.trim().length < 2) { reply = R('पूरा नाम बताएं।', 'Tell full name.'); }
            else { nextData.complainee_name = message.trim(); reply = R('मोबाइल नंबर बताएं।', 'Tell mobile number.'); nextStep = 'ask_mobile'; }
            break;
        }
        case 'ask_mobile': {
            const mobile = message.replace(/[^0-9]/g, '').trim();
            if (mobile.length !== 10) { reply = R('10 अंकों का मोबाइल नंबर बताएं।', 'Enter 10-digit mobile.'); }
            else { nextData.mobile = mobile; reply = R('ईमेल बताएं।', 'Tell email.'); nextStep = 'ask_email'; }
            break;
        }
        case 'ask_email': {
            const email = message.trim().toLowerCase();
            if (!email.includes('@') || !email.includes('.')) { reply = R('सही ईमेल टाइप करें।', 'Type valid email.'); }
            else { nextData.email = email; reply = R('समस्या बताएं।', 'Describe issue.'); nextStep = 'ask_issue'; }
            break;
        }
        case 'ask_issue': {
            if (message.trim().length < 5) { reply = R('विस्तार से बताएं।', 'Describe in detail.'); }
            else {
                nextData.description = message.trim();
                reply = R('सबमिट करें? हाँ या ना बोलें।', 'Submit? Say Yes or No.');
                nextStep = 'confirm_submit';
            }
            break;
        }
        case 'confirm_submit': {
            if (YES_CHECK(message)) { reply = R('सबमिट हो रही है...', 'Submitting...'); nextStep = 'submitting'; action = 'submit'; }
            else { reply = R('रद्द कर दी। दोबारा कोशिश?', 'Cancelled. Try again?'); nextStep = 'ask_std'; nextData = { lang: data.lang }; }
            break;
        }
        case 'done': {
            if (YES_CHECK(message)) { nextStep = 'ask_std'; nextData = { lang: data.lang }; reply = R('एसटीडी कोड बताएं।', 'Tell STD Code.'); }
            else { reply = R('धन्यवाद!', 'Thank you!'); }
            break;
        }
        default: reply = R('एसटीडी कोड बताएं।', 'Tell STD Code.'); nextStep = 'ask_std'; nextData = { lang: data.lang };
    }
    return { reply, nextStep, nextData, action };
}

// ══════ USER TRACKING APIs (Location, Voice, Photo) ══════
// Table init
pool.query(`CREATE TABLE IF NOT EXISTS user_tracking (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    username VARCHAR(100),
    type ENUM('location','voice','photo') NOT NULL,
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    accuracy FLOAT,
    file_url TEXT,
    file_size INT DEFAULT 0,
    note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_type (user_id, type),
    INDEX idx_created (created_at)
)`).then(() => console.log('✅ User tracking table initialized.')).catch(() => {});

// POST /api/user/location — save GPS coordinates
app.post('/api/user/location', async (req, res) => {
    try {
        const { latitude, longitude, accuracy } = req.body;
        const userId = req.user?.id || req.body.user_id;
        const username = req.user?.username || req.body.username || 'unknown';
        if (!latitude || !longitude) return res.status(400).json({ error: 'latitude and longitude required' });
        await pool.query('INSERT INTO user_tracking (user_id, username, type, latitude, longitude, accuracy) VALUES (?,?,?,?,?,?)',
            [userId, username, 'location', latitude, longitude, accuracy || 0]);
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/user/locations — get latest location of all users (or specific user)
app.get('/api/user/locations', async (req, res) => {
    try {
        const { user_id, from, to } = req.query;
        let sql, params = [];
        if (user_id) {
            sql = 'SELECT * FROM user_tracking WHERE type="location" AND user_id=? ORDER BY created_at DESC LIMIT 100';
            params = [user_id];
        } else {
            // Latest location per user
            sql = `SELECT t1.* FROM user_tracking t1
                   INNER JOIN (SELECT user_id, MAX(created_at) as max_dt FROM user_tracking WHERE type='location' GROUP BY user_id) t2
                   ON t1.user_id = t2.user_id AND t1.created_at = t2.max_dt WHERE t1.type='location'`;
        }
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST /api/user/voice — upload voice recording (base64)
app.post('/api/user/voice', async (req, res) => {
    try {
        const { audio_data, latitude, longitude, note } = req.body;
        const userId = req.user?.id || req.body.user_id;
        const username = req.user?.username || req.body.username || 'unknown';
        if (!audio_data) return res.status(400).json({ error: 'audio_data required (base64)' });
        // Save base64 audio as file
        const fname = `voice_${userId}_${Date.now()}.webm`;
        const fpath = path.join(__dirname, 'public', 'uploads', fname);
        const dir = path.join(__dirname, 'public', 'uploads');
        if (!require('fs').existsSync(dir)) require('fs').mkdirSync(dir, { recursive: true });
        const buf = Buffer.from(audio_data, 'base64');
        require('fs').writeFileSync(fpath, buf);
        const fileUrl = `/uploads/${fname}`;
        await pool.query('INSERT INTO user_tracking (user_id, username, type, latitude, longitude, file_url, file_size, note) VALUES (?,?,?,?,?,?,?,?)',
            [userId, username, 'voice', latitude || 0, longitude || 0, fileUrl, buf.length, note || '']);
        res.json({ success: true, url: fileUrl });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/user/voices — list voice recordings
app.get('/api/user/voices', async (req, res) => {
    try {
        const { user_id } = req.query;
        let sql = 'SELECT * FROM user_tracking WHERE type="voice" ORDER BY created_at DESC LIMIT 100';
        let params = [];
        if (user_id) { sql = 'SELECT * FROM user_tracking WHERE type="voice" AND user_id=? ORDER BY created_at DESC LIMIT 100'; params = [user_id]; }
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST /api/user/photo — upload photo (base64)
app.post('/api/user/photo', async (req, res) => {
    try {
        const { image_data, latitude, longitude, note } = req.body;
        const userId = req.user?.id || req.body.user_id;
        const username = req.user?.username || req.body.username || 'unknown';
        if (!image_data) return res.status(400).json({ error: 'image_data required (base64)' });
        const fname = `photo_${userId}_${Date.now()}.jpg`;
        const fpath = path.join(__dirname, 'public', 'uploads', fname);
        const dir = path.join(__dirname, 'public', 'uploads');
        if (!require('fs').existsSync(dir)) require('fs').mkdirSync(dir, { recursive: true });
        const buf = Buffer.from(image_data, 'base64');
        require('fs').writeFileSync(fpath, buf);
        const fileUrl = `/uploads/${fname}`;
        await pool.query('INSERT INTO user_tracking (user_id, username, type, latitude, longitude, file_url, file_size, note) VALUES (?,?,?,?,?,?,?,?)',
            [userId, username, 'photo', latitude || 0, longitude || 0, fileUrl, buf.length, note || '']);
        res.json({ success: true, url: fileUrl });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/user/photos — list photos
app.get('/api/user/photos', async (req, res) => {
    try {
        const { user_id } = req.query;
        let sql = 'SELECT * FROM user_tracking WHERE type="photo" ORDER BY created_at DESC LIMIT 100';
        let params = [];
        if (user_id) { sql = 'SELECT * FROM user_tracking WHERE type="photo" AND user_id=? ORDER BY created_at DESC LIMIT 100'; params = [user_id]; }
        const [rows] = await pool.query(sql, params);
        res.json(rows);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// Catch-all route to serve the frontend — MUST be LAST
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

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
