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

// --- MIDDLEWARE ---
// MUST BE AT THE TOP to parse bodies before hitting routes!
app.use(cors());
app.use(express.json({ limit: '25mb' }));
app.use(express.urlencoded({ extended: true, limit: '25mb' }));

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
            { name: 'allowed_circles', type: 'TEXT' },   // JSON array of circle names (multi-select)
            { name: 'allowed_oas', type: 'TEXT' }        // JSON array of OA names (multi-select)
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
                department ENUM('Technical','Clerical','Accounts','Sales','Admin','All') NOT NULL DEFAULT 'All',
                category ENUM('Daily','Week1','Week2','Week3','Week4') NOT NULL DEFAULT 'Daily',
                task_name TEXT NOT NULL,
                sort_order INT DEFAULT 0,
                is_active TINYINT(1) DEFAULT 1,
                created_by INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        `);
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
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_user_task_date (user_id, template_id, log_date)
            ) ENGINE=InnoDB
        `);
        console.log('✅ Activity tables initialized.');
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

        // Parse permissions from DB
        let permissions = [];
        if (user.permissions) {
            try { permissions = typeof user.permissions === 'string' ? JSON.parse(user.permissions) : user.permissions; } catch (e) { permissions = []; }
        }

        // Create JWT token
        const token = jwt.sign({ id: user.id, username: user.username, role: user.role }, JWT_SECRET, { expiresIn: '12h' });

        res.json({ token, user: {
            id: user.id,
            username: user.username,
            role: user.role,
            name: user.name,
            permissions: permissions,
            allowed_circle: user.allowed_circle,
            allowed_oa: user.allowed_oa,
            backdate_rights: user.backdate_rights
        }});
    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ error: 'Server error during login', details: error.message });
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

// --- USERS ROUTES (Admin Only) ---
app.get('/api/users', authenticateToken, isAdmin, async (req, res) => {
    try {
        const [rows] = await pool.query(`
            SELECT u1.id, u1.username, u1.role, u1.name, u1.mobile, u1.email,
                   u1.allowed_circle, u1.allowed_oa, u1.allowed_circles, u1.allowed_oas,
                   u1.permissions, u1.reports_to, u1.created_at,
                   u1.department, u1.work_types, u1.allowed_customers,
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
    const { username, password, role, name, mobile, email, permissions, reports_to, department, work_types, allowed_circles, allowed_oas } = req.body;
    try {
        const hash = await bcrypt.hash(password, 10);
        const [result] = await pool.query(
            'INSERT INTO users (username, password_hash, role, name, mobile, email, permissions, reports_to, backdate_rights, department, work_types, allowed_circles, allowed_oas) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [username, hash, role || 'user', name || null, mobile || null, email || null, JSON.stringify(permissions || []), reports_to || null, req.body.backdate_rights || false, department || 'Technical', JSON.stringify(work_types || []), JSON.stringify(allowed_circles || []), JSON.stringify(allowed_oas || [])]
        );
        res.status(201).json({ id: result.insertId, username, role });
    } catch (err) {
        console.error('Create user error:', err);
        res.status(500).json({ error: 'Failed to create user' });
    }
});

app.put('/api/users/:id', authenticateToken, isAdmin, async (req, res) => {
    const { role, name, mobile, email, permissions, reports_to, department, work_types, allowed_circles, allowed_oas } = req.body;
    try {
        await pool.query(
            'UPDATE users SET role=?, name=?, mobile=?, email=?, permissions=?, reports_to=?, backdate_rights=?, department=?, work_types=?, allowed_circles=?, allowed_oas=? WHERE id=?',
            [role, name || null, mobile || null, email || null, JSON.stringify(permissions || []), reports_to || null, req.body.backdate_rights || false, department || 'Technical', JSON.stringify(work_types || []), JSON.stringify(allowed_circles || []), JSON.stringify(allowed_oas || []), req.params.id]
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
            // Get user's own DB row (for allowed_circles)
            const [[meRow]] = await pool.query(
                'SELECT id, allowed_circles FROM users WHERE id = ?', [user.id]
            );
            const myId = meRow?.id || user.id;

            // Parse allowed circles
            let myCircles = [];
            try { myCircles = JSON.parse(meRow?.allowed_circles || '[]'); } catch (e) {}

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
                'SELECT id, allowed_circles, allowed_oas FROM users WHERE id = ?', [currentUser.id]
            );
            const myId = meRow?.id || currentUser.id;

            // Subordinates
            const [subs] = await pool.query(
                'SELECT user_id FROM user_managers WHERE manager_id = ?', [myId]
            );
            const subIds = subs.map(r => r.user_id);
            const allIds = [myId, ...subIds];

            // Allowed circles
            let myCircles = [];
            try { myCircles = JSON.parse(meRow?.allowed_circles || '[]'); } catch(e) {}

            const idPH = allIds.map(() => '?').join(',');
            if (myCircles.length > 0) {
                const cirPH = myCircles.map(() => '?').join(',');
                where.push(`(c.assigned_to IN (${idPH}) OR (c.assigned_to IS NULL AND c.circle IN (${cirPH})))`);
                params.push(...allIds, ...myCircles);
            } else {
                where.push(`c.assigned_to IN (${idPH})`);
                params.push(...allIds);
            }
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
        // Only admin or senior (user with subordinates) can do this
        const [[subCnt]] = await pool.query('SELECT COUNT(*) as cnt FROM user_managers WHERE manager_id = ?', [currentUser.id]);
        const isSeniorOrAdmin = currentUser.role === 'admin' || subCnt.cnt > 0;
        if (!isSeniorOrAdmin) return res.status(403).json({ error: 'Only admin or senior engineer can reassign complaints.' });

        const { from_id, to_id } = req.body;
        if (!from_id || !to_id) return res.status(400).json({ error: 'from_id and to_id are required.' });
        if (String(from_id) === String(to_id)) return res.status(400).json({ error: 'From and To engineer cannot be the same.' });

        // Reassign only active (non-resolved/non-cancelled) complaints
        const [result] = await pool.query(
            `UPDATE complaints
             SET assigned_to = ?
             WHERE assigned_to = ?
               AND status NOT IN ('Resolved', 'Cancelled')`,
            [to_id, from_id]
        );
        const count = result.affectedRows;
        res.json({
            success: true,
            reassigned: count,
            message: `${count} active complaint(s) reassigned successfully.`
        });
    } catch (err) {
        console.error('Bulk reassign error:', err);
        res.status(500).json({ error: 'Bulk reassign failed: ' + err.message });
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
        res.json({ sent, failed, total: withEmail.length });
    } catch (err) {
        console.error('Send bill email error:', err);
        res.status(500).json({ error: 'Email send failed: ' + err.message });
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
    const { department, category, task_name, sort_order } = req.body;
    if (!task_name) return res.status(400).json({ error: 'task_name required' });
    try {
        await pool.query(
            'INSERT INTO activity_templates (department, category, task_name, sort_order, created_by) VALUES (?,?,?,?,?)',
            [department||'All', category||'Daily', task_name, sort_order||0, req.user.id]
        );
        res.json({ message: 'Template created' });
    } catch (err) { res.status(500).json({ error: 'Failed to create template' }); }
});

// PUT update template (admin only)
app.put('/api/activity/templates/:id', authenticateToken, isAdmin, async (req, res) => {
    const { department, category, task_name, sort_order, is_active } = req.body;
    try {
        await pool.query(
            'UPDATE activity_templates SET department=?, category=?, task_name=?, sort_order=?, is_active=? WHERE id=?',
            [department, category, task_name, sort_order||0, is_active===false?0:1, req.params.id]
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

// GET my tasks for a date (auto-show Daily + current week tasks)
app.get('/api/activity/my-tasks', authenticateToken, async (req, res) => {
    const date = req.query.date || new Date().toISOString().split('T')[0];
    const userId = req.query.user_id || req.user.id;
    if (req.user.role !== 'admin' && String(userId) !== String(req.user.id)) {
        return res.status(403).json({ error: 'Access denied' });
    }
    try {
        const d = new Date(date);
        const day = d.getDate();
        let weekCategories = ['Daily'];
        if (day >= 1 && day <= 7) weekCategories.push('Week1');
        else if (day >= 8 && day <= 14) weekCategories.push('Week2');
        else if (day >= 15 && day <= 21) weekCategories.push('Week3');
        else weekCategories.push('Week4');

        const catPlaceholders = weekCategories.map(() => '?').join(',');
        const [tasks] = await pool.query(
            `SELECT aut.id as assignment_id, at.id as template_id, at.task_name, at.category, at.department,
                    al.status, al.remarks, al.id as log_id
             FROM activity_user_tasks aut
             JOIN activity_templates at ON aut.template_id=at.id
             LEFT JOIN activity_logs al ON al.template_id=at.id AND al.user_id=? AND al.log_date=?
             WHERE aut.user_id=? AND aut.is_active=1 AND at.is_active=1 AND at.category IN (${catPlaceholders})
             ORDER BY at.category, at.sort_order, at.id`,
            [userId, date, userId, ...weekCategories]
        );
        res.json(tasks);
    } catch (err) { res.status(500).json({ error: 'Failed to fetch tasks' }); }
});

// POST/PUT submit daily log (upsert)
app.post('/api/activity/logs', authenticateToken, async (req, res) => {
    const { logs, log_date } = req.body; // logs: [{template_id, status, remarks}]
    if (!Array.isArray(logs) || !log_date) return res.status(400).json({ error: 'logs[] and log_date required' });
    const userId = req.user.id;
    try {
        for (const l of logs) {
            await pool.query(
                `INSERT INTO activity_logs (user_id, template_id, log_date, status, remarks)
                 VALUES (?,?,?,?,?)
                 ON DUPLICATE KEY UPDATE status=VALUES(status), remarks=VALUES(remarks), updated_at=NOW()`,
                [userId, l.template_id, log_date, l.status||'not_done', l.remarks||'']
            );
        }
        res.json({ message: `${logs.length} task(s) saved for ${log_date}` });
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
function downloadPdfBuffer(url, redirectCount = 0) {
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
                return downloadPdfBuffer(nextUrl, redirectCount + 1).then(resolve).catch(reject);
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

// 2. Process batch — download 5 PDFs, match customer, save BLOB
app.post('/api/bulk-bill/process-batch', authenticateToken, async (req, res) => {
    try {
        // Pick 10 pending rows and process ALL in parallel
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

        res.json({ ...stats, emailTotal: emailStats.total || 0, emailSent: emailStats.sent || 0 });
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
