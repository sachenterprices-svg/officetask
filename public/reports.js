document.addEventListener('DOMContentLoaded', () => {
    let allProposals = [];
    const tbody = document.getElementById('reportsTableBody');
    const searchInput = document.getElementById('searchInput');
    const sortSelect = document.getElementById('sortSelect');
    const filterDirect = document.getElementById('filterDirect');

    // Admin check logic can be added here if needed for header links

    async function loadReports() {
        try {
            const res = await window.apiFetch('/api/proposals');
            if (res.ok) {
                allProposals = await res.json();
                applyFilters();
            } else {
                tbody.innerHTML = '<tr><td colspan="8" style="color:red; text-align:center;">Failed to load reports</td></tr>';
            }
        } catch (err) {
            console.error(err);
            tbody.innerHTML = '<tr><td colspan="8" style="color:red; text-align:center;">Network error</td></tr>';
        }
    }

    function applyFilters() {
        let filtered = [...allProposals];

        // Search Filter
        const search = searchInput.value.toLowerCase();
        if (search) {
            filtered = filtered.filter(p =>
                (p.customer_name && p.customer_name.toLowerCase().includes(search)) ||
                (p.customer_category && p.customer_category.toLowerCase().includes(search))
            );
        }

        // Direct Sale Filter
        const direct = filterDirect.value;
        if (direct === 'yes') filtered = filtered.filter(p => p.direct_sale === 1);
        if (direct === 'no') filtered = filtered.filter(p => p.direct_sale === 0);

        // Sorting
        const sort = sortSelect.value;
        filtered.sort((a, b) => {
            if (sort === 'date-desc') return new Date(b.created_at) - new Date(a.created_at);
            if (sort === 'date-asc') return new Date(a.created_at) - new Date(b.created_at);
            if (sort === 'name-asc') return (a.customer_name || '').localeCompare(b.customer_name || '');
            if (sort === 'name-desc') return (b.customer_name || '').localeCompare(a.customer_name || '');
            return 0;
        });

        renderReports(filtered);
    }

    function renderReports(proposals) {
        let overdueCount = 0;
        let todayCount = 0;

        if (!proposals || proposals.length === 0) {
            tbody.innerHTML = '<tr><td colspan="8" style="color:#64748b; text-align:center; padding: 30px;">No matching proposals found.</td></tr>';
            return;
        }

        let html = '';
        proposals.forEach(p => {
            const date = new Date(p.created_at).toLocaleString('en-IN', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' });
            const followupDate = p.next_followup_date ? new Date(p.next_followup_date) : null;
            const today = new Date();
            today.setHours(0, 0, 0, 0);

            let followupClass = '';
            if (followupDate) {
                const fd = new Date(followupDate);
                fd.setHours(0, 0, 0, 0);
                if (fd < today) { followupClass = 'overdue'; overdueCount++; }
                else if (fd.getTime() === today.getTime()) { followupClass = 'upcoming'; todayCount++; }
            }

            const followupStr = followupDate ? followupDate.toLocaleDateString('en-IN') : '-';
            const followupVal = p.next_followup_date ? p.next_followup_date.split('T')[0] : '';

            let data = {};
            try { data = typeof p.proposal_data === 'string' ? JSON.parse(p.proposal_data) : p.proposal_data; } catch (e) { }

            const circle = data.circle || '-';
            const oa = data.oa || '-';
            const isDirectSale = p.direct_sale === 1 ? '<span class="badge-yes">Yes</span>' : '<span class="badge-no">No</span>';

            html += `
                <tr>
                    <td><strong>${p.customer_name || 'Walk-in'}</strong></td>
                    <td>${date}</td>
                    <td><span style="background:#f1f5f9; padding:3px 8px; border-radius:4px;">${p.customer_category}</span></td>
                    <td>${circle} / ${oa}</td>
                    <td style="text-align:center;">${isDirectSale}</td>
                    <td>
                        <div class="followup-control">
                            <span class="${followupClass}">${followupStr}</span>
                            <input type="date" class="followup-input" value="${followupVal}" onchange="updateFollowup(${p.id}, this.value)">
                        </div>
                    </td>
                    <td><button class="btn" style="padding: 4px 10px; font-size: 0.85em; background: #0033a0;" onclick="viewProposal(${p.id})">View</button></td>
                </tr>
            `;
        });

        tbody.innerHTML = html;
        document.getElementById('overdueCount').textContent = overdueCount;
        document.getElementById('todayCount').textContent = todayCount;
    }

    window.viewProposal = (id) => {
        const p = allProposals.find(item => item.id === id);
        if (!p) return;

        let data = {};
        try { data = typeof p.proposal_data === 'string' ? JSON.parse(p.proposal_data) : p.proposal_data; } catch (e) { }

        let quoteItems = [];
        try { quoteItems = typeof p.quotation_items === 'string' ? JSON.parse(p.quotation_items) : (p.quotation_items || []); } catch (e) { }

        let html = `
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                <div>
                    <h4 style="color: #0033a0; border-bottom: 1px solid #e2e8f0; margin-bottom: 10px;">Customer Info</h4>
                    <p><strong>Name:</strong> ${p.customer_name}</p>
                    <p><strong>Category:</strong> ${p.customer_category}</p>
                    <p><strong>Location:</strong> ${data.circle} / ${data.oa}</p>
                </div>
                <div>
                    <h4 style="color: #0033a0; border-bottom: 1px solid #e2e8f0; margin-bottom: 10px;">BSNL Details</h4>
                    <p><strong>Proposed By:</strong> ${data.senderName || '-'}</p>
                    <p><strong>Plan Type:</strong> ${data.planType}</p>
                    <p><strong>Channels:</strong> ${data.channels}</p>
                </div>
            </div>
            
            <h4 style="color: #0033a0; border-bottom: 1px solid #e2e8f0; margin-top: 20px; margin-bottom: 10px;">Annexure A: Recurring Plan</h4>
            <table style="width:100%; font-size: 0.9em; border-collapse: collapse;">
                <tr style="background:#f8fafc;">
                    <th style="padding:8px; border:1px solid #e2e8f0; text-align:left;">Description</th>
                    <th style="padding:8px; border:1px solid #e2e8f0;">Qty</th>
                </tr>
                <tr>
                    <td style="padding:8px; border:1px solid #e2e8f0;">SIP Trunk Channels</td>
                    <td style="padding:8px; border:1px solid #e2e8f0; text-align:center;">${data.channels}</td>
                </tr>
                ${data.ipQty ? `<tr><td style="padding:8px; border:1px solid #e2e8f0;">IP Extensions</td><td style="padding:8px; border:1px solid #e2e8f0; text-align:center;">${data.ipQty}</td></tr>` : ''}
                ${data.analogQty ? `<tr><td style="padding:8px; border:1px solid #e2e8f0;">Analog Extensions</td><td style="padding:8px; border:1px solid #e2e8f0; text-align:center;">${data.analogQty}</td></tr>` : ''}
            </table>
        `;

        if (p.direct_sale && quoteItems.length > 0) {
            html += `
                <h4 style="color: #0033a0; border-bottom: 1px solid #e2e8f0; margin-top: 20px; margin-bottom: 10px;">Annexure B: Equipment Sale</h4>
                <table style="width:100%; font-size: 0.9em; border-collapse: collapse;">
                    <tr style="background:#f8fafc;">
                        <th style="padding:8px; border:1px solid #e2e8f0; text-align:left;">Item</th>
                        <th style="padding:8px; border:1px solid #e2e8f0;">Qty</th>
                        <th style="padding:8px; border:1px solid #e2e8f0; text-align:right;">Rate</th>
                    </tr>
                    ${quoteItems.map(i => `
                        <tr>
                            <td style="padding:8px; border:1px solid #e2e8f0;">${i.name}</td>
                            <td style="padding:8px; border:1px solid #e2e8f0; text-align:center;">${i.qty}</td>
                            <td style="padding:8px; border:1px solid #e2e8f0; text-align:right;">₹${i.rate.toLocaleString()}</td>
                        </tr>
                    `).join('')}
                </table>
            `;
        }

        document.getElementById('modalContent').innerHTML = html;
        document.getElementById('detailsModal').style.display = 'block';
    }

    window.updateFollowup = async (id, date) => {
        try {
            const res = await window.apiFetch(`/api/proposals/${id}/followup`, {
                method: 'PATCH',
                body: JSON.stringify({ next_followup_date: date })
            });
            if (res.ok) {
                loadReports();
            } else {
                alert("Failed to update follow-up date");
            }
        } catch (err) {
            console.error(err);
            alert("Network error updating follow-up");
        }
    };

    searchInput.addEventListener('input', applyFilters);
    sortSelect.addEventListener('change', applyFilters);
    filterDirect.addEventListener('change', applyFilters);

    loadReports();
});
