// Configuration for Split Deployment
const VERCEL_API_URL = 'https://officetask-roan.vercel.app';

// Automatically detect if running locally vs remote
const API_BASE_URL = (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1')
    ? '' // Use local backend when on localhost
    : VERCEL_API_URL; // Use Vercel when on production (BigRock)

(function checkAuth() {
    // Skip auth check on login and debug pages to avoid redirect loops
    if (window.location.pathname.endsWith('login.html') || window.location.pathname.endsWith('debug.html')) return;

    const token = localStorage.getItem('crm_token');
    const user = JSON.parse(localStorage.getItem('crm_user'));

    if (!token || !user) {
        // Not logged in, redirect to login page
        window.location.href = 'login.html';
    }

    // Attach user info to global window object for easy access
    window.currentUser = user;

    // Show admin-only elements after page load
    document.addEventListener('DOMContentLoaded', () => {
        if (window.currentUser && window.currentUser.role === 'admin') {
            document.querySelectorAll('.admin-only').forEach(el => el.style.display = 'inline-block');
        }
    });

})();

// --- PERMISSION CHECKER ---
window.hasPermission = function (module, action) {
    const user = window.currentUser || JSON.parse(localStorage.getItem('crm_user'));
    if (!user) return false;
    if (user.role === 'admin') return true;

    let perms = user.permissions || [];
    if (typeof perms === 'string') {
        try { perms = JSON.parse(perms); } catch (e) { perms = []; }
    }

    const required = `${module}:${action}`;
    return Array.isArray(perms) ? perms.includes(required) : perms[required] === true;
};

window.hasModuleAccess = function (moduleName) {
    return window.hasPermission(moduleName, 'view');
};

// Helper function for Auth API calls
window.apiFetch = async function (url, options = {}) {
    const token = localStorage.getItem('crm_token');

    // Build absolute URL if API_BASE_URL is set
    const finalUrl = url.startsWith('/') ? (API_BASE_URL + url) : url;

    // Setup headers with Authorization Bearer token
    if (!options.headers) options.headers = {};
    if (token) options.headers['Authorization'] = `Bearer ${token}`;

    if (!options.headers['Content-Type'] && !(options.body instanceof FormData)) {
        options.headers['Content-Type'] = 'application/json';
    }

    const res = await fetch(finalUrl, options);

    // Only force logout on 401 (token expired/invalid), NOT on 403 (permission denied)
    if (res.status === 401) {
        localStorage.removeItem('crm_token');
        localStorage.removeItem('crm_user');
        window.location.href = 'login.html';
    }

    return res;
};

// Global Logout function
window.logout = function () {
    localStorage.removeItem('crm_token');
    localStorage.removeItem('crm_user');
    window.location.href = 'login.html';
};

// --- INACTIVITY AUTO-LOGOUT (5 Minutes) ---
(function () {
    let inactivityTimer;
    const INACTIVITY_LIMIT = 60 * 60 * 1000; // 60 minutes (1 hour)

    function resetInactivityTimer() {
        if (inactivityTimer) clearTimeout(inactivityTimer);
        inactivityTimer = setTimeout(() => {
            console.log("Inactivity limit reached. Logging out...");
            window.logout();
        }, INACTIVITY_LIMIT);
    }

    // Reset timer on user activity
    ['mousemove', 'keydown', 'click', 'scroll'].forEach(evt =>
        window.addEventListener(evt, resetInactivityTimer)
    );

    // Initial start
    resetInactivityTimer();
})();

// --- GLOBAL SIDEBAR & PERMISSION ENFORCEMENT ---
document.addEventListener('DOMContentLoaded', () => {
    const user = JSON.parse(localStorage.getItem('crm_user'));
    if (!user) return;

    // Handle Admin-Only visibility
    if (user.role !== 'admin') {
        document.querySelectorAll('.admin-only').forEach(el => el.style.display = 'none');
    }

    // Handle Module-Specific Sidebar Links — HIDE if no permission
    const sidebarChecks = [
        { id: 'navProposals', perm: 'proposals' },
        { id: 'navComplaints', perm: 'complaints' },
        { id: 'navMasters', perm: 'customers' },
        { id: 'navReconciliation', perm: 'reconciliation' },
        { id: 'navSystem', perm: 'system' },
    ];

    sidebarChecks.forEach(check => {
        const el = document.getElementById(check.id);
        if (el && !window.hasModuleAccess(check.perm)) {
            el.style.display = 'none';
        }
    });

    // Auto-active current page in visual tiles
    const currentPath = window.location.pathname.split('/').pop() || 'dashboard.html';
    document.querySelectorAll('.module-tile').forEach(link => {
        if (link.getAttribute('href') === currentPath) {
            link.classList.add('active');
        } else {
            link.classList.remove('active');
        }
    });
});
