document.addEventListener('DOMContentLoaded', () => {
    checkAuth();
    loadAnalytics();

    document.getElementById('refreshBtn').addEventListener('click', loadAnalytics);
});

async function loadAnalytics() {
    const tbody = document.getElementById('analyticsBody');
    const token = localStorage.getItem('token');

    try {
        const res = await fetch(`${API_BASE_URL}/api/analytics/report`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (!res.ok) throw new Error('Failed to fetch data');

        const data = await res.json();
        renderAnalytics(data);
    } catch (err) {
        console.error(err);
        tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; color: red;">Error loading analytics. ${err.message}</td></tr>`;
    }
}

function renderAnalytics(data) {
    const tbody = document.getElementById('analyticsBody');
    if (data.length === 0) {
        tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; padding: 50px;">No visitor data available yet.</td></tr>`;
        return;
    }

    tbody.innerHTML = data.map(item => `
        <tr>
            <td class="badge-time">${new Date(item.created_at).toLocaleString()}</td>
            <td><span class="badge badge-ip">${item.ip_address}</span></td>
            <td style="font-weight: 600;">${item.page_url}</td>
            <td><span class="badge" style="background: #fef3c7; color: #92400e;">${item.action_type}</span></td>
            <td><small>${item.details || '-'}</small></td>
            <td title="${item.user_agent}"><small>${item.user_agent.substring(0, 30)}...</small></td>
        </tr>
    `).join('');
}
