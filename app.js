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

// Serve static files from the 'public' directory
app.use(express.static(path.join(__dirname, 'public')));

// --- DATABASE CONNECTION ---
// MySQL Connection Pool
const pool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    port: process.env.DB_PORT,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
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
            { name: 'permissions', type: 'JSON' }
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
        res.status(500).json({ error: 'Server error during login' });
    }
});

// --- USERS ROUTES (Admin Only) ---
app.get('/api/users', authenticateToken, isAdmin, async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT id, username, role, name, mobile, email, allowed_circle, allowed_oa, permissions, created_at FROM users');
        res.json(rows);
    } catch (err) {
        console.error('Fetch users error:', err);
        res.status(500).json({ error: 'Failed to fetch users' });
    }
});

app.post('/api/users', authenticateToken, isAdmin, async (req, res) => {
    const { username, password, role, name, mobile, email, allowed_circle, allowed_oa, permissions } = req.body;
    try {
        const hash = await bcrypt.hash(password, 10);
        const [result] = await pool.query(
            'INSERT INTO users (username, password_hash, role, name, mobile, email, allowed_circle, allowed_oa, permissions) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [username, hash, role || 'user', name || null, mobile || null, email || null, allowed_circle || null, allowed_oa || null, JSON.stringify(permissions || [])]
        );
        res.status(201).json({ id: result.insertId, username, role });
    } catch (err) {
        console.error('Create user error:', err);
        res.status(500).json({ error: 'Failed to create user' });
    }
});

app.put('/api/users/:id', authenticateToken, isAdmin, async (req, res) => {
    const { role, name, mobile, email, allowed_circle, allowed_oa, permissions } = req.body;
    try {
        await pool.query(
            'UPDATE users SET role=?, name=?, mobile=?, email=?, allowed_circle=?, allowed_oa=?, permissions=? WHERE id=?',
            [role, name || null, mobile || null, email || null, allowed_circle || null, allowed_oa || null, JSON.stringify(permissions || []), req.params.id]
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

// Basic Health Check Endpoint
app.get('/api/health', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT 1');
        res.status(200).json({ status: 'OK', database: 'Connected', message: 'Server is running normally' });
    } catch (err) {
        res.status(200).json({ status: 'OK', database: 'Disconnected', message: 'Server running but DB not connected' });
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
    await initializeAdmin();
}

// Support for both local development and Vercel Serverless Functions
if (require.main === module) {
    app.listen(PORT, '127.0.0.1', async () => {
        console.log(`\n🚀 Server is running on port ${PORT} (Secure Local-Only Mode)`);
        console.log(`🌐 Open http://localhost:${PORT} in your browser.\n`);
        await startupChecks();
    });
} else {
    // Export the express app so Vercel can run it as a serverless function
    // Run startup checks asynchronously (may delay first request slightly on cold start)
    startupChecks().catch(console.error);
    module.exports = app;
}
