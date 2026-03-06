// auth.js - Include this script on all protected pages

(function checkAuth() {
    const token = localStorage.getItem('crm_token');
    const user = JSON.parse(localStorage.getItem('crm_user'));

    if (!token || !user) {
        // Not logged in, redirect to login page
        window.location.href = '/login.html';
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

// Helper function for Auth API calls
window.apiFetch = async function (url, options = {}) {
    const token = localStorage.getItem('crm_token');

    // Setup headers with Authorization Bearer token
    if (!options.headers) options.headers = {};
    options.headers['Authorization'] = `Bearer ${token}`;
    if (!options.headers['Content-Type'] && !(options.body instanceof FormData)) {
        options.headers['Content-Type'] = 'application/json';
    }

    const res = await fetch(url, options);

    // If 401/403 (Token expired or unauthorized), force logout
    if (res.status === 401 || res.status === 403) {
        localStorage.removeItem('crm_token');
        localStorage.removeItem('crm_user');
        window.location.href = '/login.html';
    }

    return res;
};

// Global Logout function
window.logout = function () {
    localStorage.removeItem('crm_token');
    localStorage.removeItem('crm_user');
    window.location.href = '/login.html';
};

// --- INACTIVITY AUTO-LOGOUT (5 Minutes) ---
(function () {
    let inactivityTimer;
    const INACTIVITY_LIMIT = 5 * 60 * 1000; // 5 minutes

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
