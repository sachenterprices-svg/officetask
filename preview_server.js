const express = require('express');
const path = require('path');
const app = express();
const PORT = 8081;

app.use((req, res, next) => {
    console.log(`[${new Date().toLocaleTimeString()}] ${req.method} ${req.url}`);
    next();
});

// Serve static files from the 'website' directory
app.use(express.static(path.join(__dirname, 'website')));

// Helpful redirect if user tries to open ERP pages on the website port
app.get(['/login.html', '/dashboard.html', '/customers.html', '/tasks.html'], (req, res) => {
    const targetUrl = `http://localhost:8080${req.url}`;
    res.send(`
        <div style="font-family:sans-serif; text-align:center; padding:50px;">
            <h2>⚠️ Wrong Port!</h2>
            <p>You are trying to access the <b>Office Task ERP</b> on the Website Preview port (8081).</p>
            <p>Redirecting you to the correct page now...</p>
            <script>setTimeout(() => { window.location.href = '${targetUrl}'; }, 3000);</script>
            <a href="${targetUrl}" style="color:blue;">Click here if not redirected</a>
        </div>
    `);
});

// Fallback for .html files (allow support instead of support.html)
app.get('/:page', (req, res, next) => {
    if (req.params.page.indexOf('.') === -1) {
        res.sendFile(path.join(__dirname, 'website', `${req.params.page}.html`), err => {
            if (err) next();
        });
    } else {
        next();
    }
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`\n========================================================`);
    console.log(`  CORAL INFRATEL - WEBSITE LOCAL PREVIEWER (Node.js)`);
    console.log(`========================================================\n`);
    console.log(`🚀 Website Preview is running on port ${PORT}`);
    console.log(`🌐 Open http://localhost:${PORT} in your browser.`);
    console.log(`\nPress Ctrl+C to stop the server.`);
});
