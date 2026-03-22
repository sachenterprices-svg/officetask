// Coral Infratel — Background Location + Voice + Camera Tracking
(function() {
    const API = 'https://officetask-roan.vercel.app/api';

    // Get user info from auth
    function getUser() {
        try {
            const token = localStorage.getItem('token');
            if (!token) return null;
            const payload = JSON.parse(atob(token.split('.')[1]));
            return { id: payload.id || payload.user_id, username: payload.username };
        } catch(e) { return null; }
    }

    // Send location to server
    async function sendLocation(lat, lng, accuracy) {
        const user = getUser();
        if (!user) return;
        try {
            await fetch(API + '/user/location', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + localStorage.getItem('token') },
                body: JSON.stringify({ user_id: user.id, username: user.username, latitude: lat, longitude: lng, accuracy: accuracy })
            });
        } catch(e) { console.log('Location send failed:', e.message); }
    }

    // Start GPS tracking
    function startTracking() {
        if (!navigator.geolocation) return;

        // Immediate first location
        navigator.geolocation.getCurrentPosition(
            pos => sendLocation(pos.coords.latitude, pos.coords.longitude, pos.coords.accuracy),
            () => {},
            { enableHighAccuracy: true, timeout: 10000 }
        );

        // Track every 5 minutes
        setInterval(() => {
            navigator.geolocation.getCurrentPosition(
                pos => sendLocation(pos.coords.latitude, pos.coords.longitude, pos.coords.accuracy),
                () => {},
                { enableHighAccuracy: true, timeout: 10000 }
            );
        }, 5 * 60 * 1000);
    }

    // Auto-start after login
    if (getUser()) {
        startTracking();
    } else {
        // Wait for login then start
        const checkLogin = setInterval(() => {
            if (getUser()) {
                clearInterval(checkLogin);
                startTracking();
            }
        }, 3000);
    }
})();
