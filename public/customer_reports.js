document.addEventListener('DOMContentLoaded', () => {
    const urlParams = new URLSearchParams(window.location.search);
    const reportType = urlParams.get('type') || 'customers';

    const titleEl = document.getElementById('reportTitle');
    const subTitleEl = document.getElementById('reportSubTitle');
    const headEl = document.getElementById('reportHead');
    const bodyEl = document.getElementById('reportData');
    const countEl = document.getElementById('recordCount');
    const searchInput = document.getElementById('globalSearch');
    const loadingEl = document.getElementById('loading');

    let rawData = [];
    let currentSort = { col: null, dir: 'asc' };

    const CONFIG = {
        customers: {
            title: 'Customer Detail Report',
            icon: 'fa-id-card',
            cols: [
                { key: 'circle', label: 'Circle' },
                { key: 'oa_name', label: 'OA Name' },
                { key: 'ssa', label: 'SSA Name' },
                { key: 'customer_code', label: 'Code' },
                { key: 'customer_name', label: 'Customer Name' },
                { key: 'mobile_no', label: 'Mobile' },
                { key: 'telephone_number', label: 'Phone' },
                { key: 'order_date', label: 'Order Date', type: 'date' },
                { key: 'acc_person_name', label: 'Accounts Contact' },
                { key: 'acc_person_mobile', label: 'Acc. Mobile' },
                { key: 'tech_person_name', label: 'Tech Contact' },
                { key: 'customer_status', label: 'Status' },
                { key: 'actions', label: 'Actions', noSort: true }
            ]
        },
        products: {
            title: 'Product & Commercials Report',
            icon: 'fa-boxes',
            cols: [
                { key: 'circle', label: 'Circle' },
                { key: 'oa_name', label: 'OA' },
                { key: 'ssa', label: 'SSA' },
                { key: 'customer_name', label: 'Customer' },
                { key: 'product_plan', label: 'Plan' },
                { key: 'monthly_rent', label: 'Monthly Rent' },
                { key: 'channels', label: 'Channels' },
                { key: 'analog_line', label: 'Analog' },
                { key: 'digital_line', label: 'Digital' },
                { key: 'vas_line', label: 'VAS' },
                { key: 'ip_line', label: 'IP' },
                { key: 'plan_charge', label: 'Plan Charge' },
                { key: 'revenue_level', label: 'Rev Level' },
                { key: 'epabx_model', label: 'EPABX Model' },
                { key: 'product_start_date', label: 'Start Date', type: 'date' },
                { key: 'actions', label: 'Actions', noSort: true }
            ]
        },
        lines: {
            title: 'Telephone Lines Report',
            icon: 'fa-phone-alt',
            cols: [
                { key: 'circle', label: 'Circle' },
                { key: 'oa_name', label: 'OA' },
                { key: 'ssa', label: 'SSA' },
                { key: 'customer_name', label: 'Customer' },
                { key: 'telephone_number', label: 'Phone No' },
                { key: 'line_type', label: 'Type' },
                { key: 'sip_no', label: 'SIP No' },
                { key: 'start_date', label: 'Start Date', type: 'date' },
                { key: 'billing_account', label: 'Bill Acc' },
                { key: 'crm_customer_id', label: 'CRM ID' },
                { key: 'submit_at_fms', label: 'FMS Submit' },
                { key: 'is_closed', label: 'Closed' },
                { key: 'actions', label: 'Actions', noSort: true }
            ]
        },
        consolidated: {
            title: 'Registration Mode Report (Consolidated)',
            icon: 'fa-file-alt',
            cols: [
                { key: 'circle', label: 'Circle' },
                { key: 'oa_name', label: 'OA' },
                { key: 'ssa', label: 'SSA' },
                { key: 'customer_code', label: 'Code' },
                { key: 'customer_name', label: 'Customer' },
                { key: 'customer_status', label: 'Status' },
                { key: 'product_plan', label: 'Plan' },
                { key: 'total_bill', label: 'Monthly Rent' },
                { key: 'epabx_model', label: 'EPABX' },
                { key: 'revenue_level', label: 'Rev Level' },
                { key: 'primary_line', label: 'Main Phone' },
                { key: 'line_count', label: 'Total Lines' },
                { key: 'actions', label: 'Actions', noSort: true }
            ]
        }
    };

    async function init() {
        if (!CONFIG[reportType]) return alert('Invalid report type');

        const conf = CONFIG[reportType];
        titleEl.innerHTML = `<i class="fas ${conf.icon}"></i> ${conf.title}`;
        subTitleEl.textContent = `Generated on ${new Date().toLocaleDateString()} at ${new Date().toLocaleTimeString()}`;

        renderHeader(conf.cols);

        // Update import button URL
        const importBtn = document.getElementById('importBtn');
        if (importBtn) {
            importBtn.href = `import_data.html?type=${reportType}`;
        }

        await fetchData();
    }

    function renderHeader(cols) {
        let html = '<tr>';
        cols.forEach(c => {
            if (c.noSort) {
                html += `<th>${c.label}</th>`;
            } else {
                html += `<th data-key="${c.key}">${c.label} <i class="fas fa-sort"></i></th>`;
            }
        });
        html += '</tr>';
        headEl.innerHTML = html;

        headEl.querySelectorAll('th').forEach(th => {
            th.addEventListener('click', () => {
                const key = th.dataset.key;
                const dir = (currentSort.col === key && currentSort.dir === 'asc') ? 'desc' : 'asc';
                currentSort = { col: key, dir };

                // Update UI icons
                headEl.querySelectorAll('i').forEach(i => i.className = 'fas fa-sort');
                th.querySelector('i').className = `fas fa-sort-${dir === 'asc' ? 'up' : 'down'}`;

                sortAndRender();
            });
        });
    }

    async function fetchData() {
        loadingEl.style.display = 'flex';
        bodyEl.innerHTML = '';
        try {
            const res = await window.apiFetch(`/api/reports/${reportType}`);
            if (res.ok) {
                rawData = await res.json();

                // --- Pending Tasks Filtering ---
                const pendingFilter = urlParams.get('pending');
                if (reportType === 'customers' && pendingFilter) {
                    if (pendingFilter === 'products') {
                        // Show customers with no orders or missing plan
                        rawData = rawData.filter(r => (r.order_count === 0 || !r.product_plan));
                    } else if (pendingFilter === 'lines') {
                        // Show customers with 0 telephone lines
                        rawData = rawData.filter(r => r.line_count === 0);
                    }
                }

                sortAndRender();
            } else {
                bodyEl.innerHTML = '<tr><td colspan="100" style="text-align:center; padding:40px;">No records found or access denied.</td></tr>';
            }
        } catch (err) {
            console.error(err);
            bodyEl.innerHTML = '<tr><td colspan="100" style="text-align:center; padding:40px; color:red;">Connection error.</td></tr>';
        } finally {
            loadingEl.style.display = 'none';
        }
    }

    function sortAndRender() {
        const query = searchInput.value.toLowerCase();
        let filtered = rawData.filter(row => {
            return Object.values(row).some(val => String(val).toLowerCase().includes(query));
        });

        if (currentSort.col) {
            filtered.sort((a, b) => {
                let v1 = a[currentSort.col];
                let v2 = b[currentSort.col];

                // Handle nulls
                if (v1 == null) v1 = '';
                if (v2 == null) v2 = '';

                if (v1 < v2) return currentSort.dir === 'asc' ? -1 : 1;
                if (v1 > v2) return currentSort.dir === 'asc' ? 1 : -1;
                return 0;
            });
        }

        renderBody(filtered);
    }

    function renderBody(data) {
        const cols = CONFIG[reportType].cols;
        countEl.textContent = data.length;

        if (data.length === 0) {
            bodyEl.innerHTML = '<tr><td colspan="100" style="text-align:center; padding:20px;">No matches found.</td></tr>';
            return;
        }

        let html = '';
        data.forEach(row => {
            html += '<tr>';
            cols.forEach(c => {
                let val = row[c.key];

                // Formatting
                if (c.type === 'date' && val) {
                    val = new Date(val).toLocaleDateString();
                }
                if (c.key === 'customer_status' || c.key === 'is_closed') {
                    const badgeClass = (val === 'OPEN' || val === 'NO') ? 'status-open' : 'status-closed';
                    val = `<span class="status-badge ${badgeClass}">${val}</span>`;
                }

                if (c.key === 'actions') {
                    const code = row.customer_code || '';
                    const custId = row.customer_id || row.id || '';
                    const orderId = row.order_id || '';
                    let buttons = `<button onclick="editRecord('${custId}', '${code}', '${orderId}')" class="btn-edit" title="Edit Entry"><i class="fas fa-edit"></i> Edit</button>`;

                    if (reportType === 'products') {
                        buttons += `<button onclick="addExtension('${custId}', '${code}')" class="btn-add-ext" title="Add New Order/Extension"><i class="fas fa-plus"></i> Add Ext.</button>`;
                    }
                    val = `<div class="action-cell">${buttons}</div>`;
                }

                html += `<td>${val ?? '-'}</td>`;
            });
            html += '</tr>';
        });
        bodyEl.innerHTML = html;
    }

    searchInput.addEventListener('input', sortAndRender);

    window.exportToExcel = () => {
        if (!rawData || rawData.length === 0) return alert('No data to export');

        const conf = CONFIG[reportType];
        const fileName = `${conf.title.replace(/\s+/g, '_')}_${new Date().toISOString().split('T')[0]}.xlsx`;

        // 1. Prepare Data for SheetJS
        // Map keys to labels for the header row
        const exportData = rawData.map(row => {
            const mappedRow = {};
            conf.cols.forEach(c => {
                let val = row[c.key];
                if (c.type === 'date' && val) {
                    val = new Date(val).toLocaleDateString();
                }
                mappedRow[c.label] = val ?? '-';
            });
            return mappedRow;
        });

        // 2. Create Workbook
        const ws = XLSX.utils.json_to_sheet(exportData);
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, "Report");

        // 3. Trigger Download
        XLSX.writeFile(wb, fileName);
    };

    window.editRecord = (id, code, orderId) => {
        const pendingFilter = urlParams.get('pending');
        let url = `customers.html?id=${id}&code=${code || ''}${orderId ? '&order_id=' + orderId : ''}&mode=edit`;
        if (pendingFilter) {
            url += `&focus=${pendingFilter}`;
        }
        window.location.href = url;
    };

    window.addExtension = (id, code) => {
        window.location.href = `customers.html?id=${id}&code=${code || ''}&mode=add_extension`;
    };

    init();
});
