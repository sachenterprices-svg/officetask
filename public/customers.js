document.addEventListener('DOMContentLoaded', () => {
    // Declare first — used throughout the file
    const selCircle = document.getElementById('custCircle');
    const selOA     = document.getElementById('custOa');
    let bsnlCircles = [];

    async function fetchBsnlData() {
        try {
            const res = await window.apiFetch('/api/bsnl/circles');
            if (res.ok) {
                bsnlCircles = await res.json();
                populateCircles();
            }
        } catch (err) {
            console.error('Failed to fetch BSNL circles:', err);
        }
    }

    function populateCircles() {
        if (!selCircle) return;
        if (bsnlCircles.length === 0) {
            selCircle.innerHTML = '<option value="">No Circles found - Add in Setup</option>';
        } else {
            selCircle.innerHTML = '<option value="">Select Circle...</option>';
            bsnlCircles.forEach(circle => {
                const opt = document.createElement('option');
                opt.value = circle.name;
                opt.textContent = circle.name;
                selCircle.appendChild(opt);
            });
        }
    }

    if (selCircle) {
        selCircle.addEventListener('change', (e) => {
            const circleName = e.target.value;
            const circle = bsnlCircles.find(c => c.name === circleName);
            selOA.innerHTML = '<option value="">Select OA...</option>';
            if (circle && circle.oas && circle.oas.length > 0) {
                circle.oas.sort((a,b) => a.name.localeCompare(b.name)).forEach(oa => {
                    const opt = document.createElement('option');
                    opt.value = oa.name;
                    opt.textContent = oa.name;
                    selOA.appendChild(opt);
                });
                selOA.disabled = false;
            } else {
                selOA.disabled = true;
            }
        });
    }

    fetchBsnlData();

    // --- Dynamic Telephone Lines Logic ---
    const linesContainer = document.getElementById('linesContainer');
    const addBtn = document.getElementById('addNewLineBtn');

    if (addBtn) {
        addBtn.addEventListener('click', () => {
            const firstSection = document.querySelector('.telephone-line-section');
            const newSection = firstSection.cloneNode(true);

            // Clear inputs in cloned section
            newSection.querySelectorAll('input').forEach(i => {
                if (!i.disabled) i.value = '';
            });
            newSection.querySelectorAll('select').forEach(s => s.selectedIndex = 0);

            // Replace the ADD and SAVE buttons with a REMOVE button in clones
            const btnContainer = newSection.querySelector('#addNewLineBtn').parentElement;
            btnContainer.innerHTML = `
                <button type="button" class="remove-line-btn" style="background:#ef4444; color:white; border:none; padding:10px 15px; border-radius:8px; font-weight:700; cursor:pointer; width:100%; transition:0.3s;">
                    REMOVE LINE
                </button>`;

            // Re-apply conditional logic for SIP No to the new section
            attachLineLogic(newSection);

            linesContainer.appendChild(newSection);
            updateLineIndices();
        });
    }

    function attachLineLogic(section) {
        const lineType = section.querySelector('.lineType');
        const sipNo = section.querySelector('.sipNo');
        const removeBtn = section.querySelector('.remove-line-btn');

        function toggle() {
            if (lineType.value === 'SIP TRUNK') {
                sipNo.disabled = false;
            } else {
                sipNo.disabled = true;
                sipNo.value = '';
            }
        }

        lineType.addEventListener('change', toggle);
        toggle(); // Initial state

        if (removeBtn) {
            removeBtn.onclick = () => {
                section.remove();
                updateLineIndices();
            };
        }
    }

    function updateLineIndices() {
        document.querySelectorAll('.telephone-line-section').forEach((sec, idx) => {
            sec.querySelector('h3').textContent = `Telephone Line #${idx + 1}`;
        });
    }

    const initialSection = document.querySelector('.telephone-line-section');
    if (initialSection) attachLineLogic(initialSection);

    function renderLines(lines) {
        if (!lines || lines.length === 0) return;

        // Clear existing lines container (except for the template if needed, but we'll rebuild)
        linesContainer.innerHTML = '';

        lines.forEach((line, idx) => {
            const section = initialSection.cloneNode(true);
            section.querySelector('h3').textContent = `Telephone Line #${idx + 1}`;

            // Populate fields
            section.querySelector('.telStdCode').value = line.telephone_code_std || ''; // Note: mapping might need adjustment if DB schema changed
            section.querySelector('.telNumber').value = line.telephone_number || '';
            section.querySelector('.lineType').value = line.line_type || '';
            section.querySelector('.sipNo').value = line.sip_no || '';
            section.querySelector('.startDate').value = line.start_date ? line.start_date.split('T')[0] : '';
            section.querySelector('.billingAcc').value = line.billing_account || '';
            section.querySelector('.crmCustId').value = line.crm_customer_id || '';
            section.querySelector('.submitFms').value = line.submit_at_fms || 'NO';
            section.querySelector('.fmsDate').value = line.fms_submit_date ? line.fms_submit_date.split('T')[0] : '';
            section.querySelector('.isClosed').value = line.is_closed || 'NO';
            section.querySelector('.closedDate').value = line.closed_date ? line.closed_date.split('T')[0] : '';
            
            const telCodeInput = section.querySelector('.telCode') || section.querySelector('.telCode-generated'); 
            if (telCodeInput) {
                telCodeInput.value = line.telephone_code || 'System Generated';
            }

            // Buttons logic
            const btnContainer = section.querySelector('#addNewLineBtn').parentElement;
            if (idx !== 0) {
                btnContainer.innerHTML = `
                    <button type="button" class="remove-line-btn" style="background:#ef4444; color:white; border:none; padding:10px 15px; border-radius:8px; font-weight:700; cursor:pointer; width:100%; transition:0.3s;">
                        REMOVE LINE
                    </button>`;
            }

            attachLineLogic(section);
            linesContainer.appendChild(section);
        });
    }

    // --- Edit Mode & Search Logic ---
    const editToggle = document.getElementById('editModeToggle');
    const searchContainer = document.getElementById('searchContainer');
    const btnLoad = document.getElementById('btnLoadCustomer');
    const searchInput = document.getElementById('editSearchCode');

    if (editToggle) {
        // Enforce View/Edit Permission for Edit Mode
        if (!window.hasPermission('customers', 'edit')) {
            editToggle.parentElement.style.display = 'none';
        }

        editToggle.addEventListener('change', () => {
            searchContainer.style.display = editToggle.checked ? 'flex' : 'none';
            if (!editToggle.checked) {
                const url = new URL(window.location);
                url.search = '';
                window.history.replaceState({}, '', url);
                window.location.reload();
            }
        });
    }

    // Enforce Add Permission
    const submitBtn = document.querySelector('#customerForm button[type="submit"]');
    if (submitBtn) {
        const isEdit = new URLSearchParams(window.location.search).get('id') || new URLSearchParams(window.location.search).get('code');
        const action = isEdit ? 'edit' : 'add';
        if (!window.hasPermission('customers', action)) {
            submitBtn.disabled = true;
            submitBtn.style.opacity = '0.5';
            submitBtn.title = `You do not have permission to ${action} customers.`;
        }
    }

    // --- Auto-Load from URL Parameters ---
    const urlParams = new URLSearchParams(window.location.search);
    const idFromUrl = urlParams.get('id');
    const codeFromUrl = urlParams.get('code');
    const orderIdFromUrl = urlParams.get('order_id');
    const modeFromUrl = urlParams.get('mode');
    const focusFromUrl = urlParams.get('focus');

    if ((idFromUrl || codeFromUrl) && editToggle) {
        editToggle.checked = true;
        searchContainer.style.display = 'flex';
        searchInput.value = codeFromUrl || idFromUrl;

        if (focusFromUrl) {
            const headerControls = document.querySelector('.header-controls');
            if (headerControls) headerControls.style.display = 'none';
        }

        // Use timeout to ensure basic event listeners are ready
        setTimeout(() => {
            loadCustomerData(idFromUrl, codeFromUrl, orderIdFromUrl, modeFromUrl);
        }, 100);
    }

    if (btnLoad) {
        btnLoad.addEventListener('click', () => {
            const code = searchInput.value.trim();
            if (!code) return alert('Please enter a Customer Code');
            loadCustomerData(null, code);
        });
    }

    async function loadCustomerData(id, code, orderId = '', mode = '') {
        try {
            let url = `/api/customers/search/v2?`;
            if (id) url += `id=${id}`;
            else url += `customerCode=${code}`;

            const res = await window.apiFetch(url);
            if (res.ok) {
                const data = await res.json();
                populateForm(data, orderId, mode);
            } else {
                alert('Customer not found');
            }
        } catch (err) {
            console.error(err);
            alert('Search failed');
        }
    }

    // --- Helper: Fill product form fields from data ---
    function fillProductFields(d) {
        document.getElementById('prodPlan').value = d.product_plan || '';
        document.getElementById('monthlyRent').value = d.monthly_rent || 0;
        document.getElementById('channels').value = d.channels || 0;
        document.getElementById('analogLines').value = d.analog_line || 0;
        document.getElementById('digitalLines').value = d.digital_line || 0;
        document.getElementById('vasLines').value = d.vas_line || 0;
        document.getElementById('ipLines').value = d.ip_line || 0;
        document.getElementById('analogRent').value = d.analog_rent || 0;
        document.getElementById('digitalRent').value = d.digital_rent || 0;
        document.getElementById('vasRent').value = d.vas_rent || 0;
        document.getElementById('ipRent').value = d.ip_rent || 0;
        document.getElementById('rgPort').value = d.rg_port || 0;
        document.getElementById('rgRent').value = d.rg_rent || 0;
        document.getElementById('planCharge').value = d.plan_charge || 0;
        document.getElementById('revLevel').value = d.revenue_level || '';
        document.getElementById('epabxModel').value = d.epabx_model || '';
        document.getElementById('prodStartDate').value = d.product_start_date ? d.product_start_date.split('T')[0] : '';
    }

    // --- Helper: Render Orders History Table ---
    function renderOrdersTable(orders) {
        const tbody = document.getElementById('ordersTableBody');
        const tfoot = document.getElementById('ordersTotalRow');
        const totals = { channels: 0, analog: 0, digital: 0, vas: 0, ip: 0, rent: 0, planChg: 0, rg: 0 };

        tbody.innerHTML = orders.map((o, i) => {
            const dt = o.product_start_date ? new Date(o.product_start_date).toLocaleDateString('en-IN', { day:'2-digit', month:'short', year:'numeric' }) : (o.order_date ? new Date(o.order_date).toLocaleDateString('en-IN', { day:'2-digit', month:'short', year:'numeric' }) : '-');
            totals.channels += Number(o.channels || 0);
            totals.analog += Number(o.analog_line || 0);
            totals.digital += Number(o.digital_line || 0);
            totals.vas += Number(o.vas_line || 0);
            totals.ip += Number(o.ip_line || 0);
            totals.rent += Number(o.monthly_rent || 0);
            totals.planChg += Number(o.plan_charge || 0);
            totals.rg += Number(o.rg_port || 0);
            return `<tr style="border-bottom:1px solid #e2e8f0;">
                <td style="padding:8px 10px; font-weight:700; color:#64748b;">${i + 1}</td>
                <td style="padding:8px 10px; white-space:nowrap; color:#334155; font-weight:600;">${dt}</td>
                <td style="padding:8px 10px; color:#0f172a; font-weight:600;">${o.product_plan || '-'}</td>
                <td style="padding:8px 10px; text-align:center;">${o.channels || 0}</td>
                <td style="padding:8px 10px; text-align:center;">${o.analog_line || 0}</td>
                <td style="padding:8px 10px; text-align:center;">${o.digital_line || 0}</td>
                <td style="padding:8px 10px; text-align:center;">${o.vas_line || 0}</td>
                <td style="padding:8px 10px; text-align:center;">${o.ip_line || 0}</td>
                <td style="padding:8px 10px; text-align:right; font-weight:600;">${Number(o.monthly_rent || 0).toLocaleString('en-IN')}</td>
                <td style="padding:8px 10px; text-align:right;">${Number(o.plan_charge || 0).toLocaleString('en-IN')}</td>
                <td style="padding:8px 10px; text-align:center;">${o.rg_port || 0}</td>
            </tr>`;
        }).join('');

        // Total row
        tfoot.innerHTML = `<tr style="border-top:2px solid #002d72;">
            <td style="padding:10px; font-weight:800;" colspan="3">TOTAL</td>
            <td style="padding:10px; text-align:center; font-weight:800;">${totals.channels}</td>
            <td style="padding:10px; text-align:center; font-weight:800;">${totals.analog}</td>
            <td style="padding:10px; text-align:center; font-weight:800;">${totals.digital}</td>
            <td style="padding:10px; text-align:center; font-weight:800;">${totals.vas}</td>
            <td style="padding:10px; text-align:center; font-weight:800;">${totals.ip}</td>
            <td style="padding:10px; text-align:right; font-weight:800;">${totals.rent.toLocaleString('en-IN')}</td>
            <td style="padding:10px; text-align:right; font-weight:800;">${totals.planChg.toLocaleString('en-IN')}</td>
            <td style="padding:10px; text-align:center; font-weight:800;">${totals.rg}</td>
        </tr>`;
    }

    function populateForm(data, orderId = '', mode = '') {
        if (focusFromUrl) {
            document.getElementById('focusHeader').style.display = 'block';
            document.getElementById('focusCircle').textContent = data.circle || '-';
            document.getElementById('focusOa').textContent = data.oa_name || '-';
            document.getElementById('focusSsa').textContent = data.ssa || '-';
            document.getElementById('focusCustName').textContent = data.customer_name || '-';
            document.getElementById('focusCustCode').textContent = data.customer_code || '-';

            document.getElementById('sectionCustomerDetail').style.display = 'none';
            document.getElementById('custCircle').removeAttribute('required');
            document.getElementById('custOa').removeAttribute('required');
            document.getElementById('custName').removeAttribute('required');

            if (focusFromUrl === 'products') {
                document.getElementById('sectionProductCommercial').style.display = 'block';
                document.getElementById('linesContainer').style.display = 'none';
                document.querySelectorAll('.lineType').forEach(el => el.removeAttribute('required'));
            } else if (focusFromUrl === 'lines') {
                document.getElementById('sectionProductCommercial').style.display = 'none';
                document.getElementById('linesContainer').style.display = 'block';
            }
        }

        selCircle.value = data.circle;
        selCircle.dispatchEvent(new Event('change'));
        setTimeout(() => {
            selOA.value = data.oa_name;
            const ssaEl = document.getElementById('custSsa');
            if (ssaEl) ssaEl.value = data.ssa || '';
        }, 300);

        document.getElementById('custName').value = data.customer_name || '';
        document.getElementById('customerCode').value = data.customer_code || '';
        document.getElementById('custOrderDate').value = data.order_date ? data.order_date.split('T')[0] : '';
        document.getElementById('accName').value = data.acc_person_name || '';
        document.getElementById('accMobile').value = data.acc_person_mobile || '';
        document.getElementById('accEmail').value = data.acc_person_email || '';
        document.getElementById('techName').value = data.tech_person_name || '';
        document.getElementById('techMobile').value = data.tech_person_mobile || '';
        document.getElementById('techEmail').value = data.tech_person_email || '';
        document.getElementById('custStatus').value = data.customer_status || 'OPEN';
        document.getElementById('custClosedDate').value = data.customer_closed_date ? data.customer_closed_date.split('T')[0] : '';

        // Product Details & Order History
        const orders = data.orders || [];
        const ordersSection = document.getElementById('existingOrdersSection');
        const addNewBtn = document.getElementById('addNewProductBtnWrap');
        const productFields = document.getElementById('productFields');

        if (orders.length > 0) {
            // Show order history table (read-only)
            ordersSection.style.display = 'block';
            document.getElementById('ordersTotalBadge').textContent = orders.length + ' Order' + (orders.length > 1 ? 's' : '');
            renderOrdersTable(orders);

            // Show "Add New" button, hide product form initially
            addNewBtn.style.display = 'block';
            productFields.style.display = 'none';

            // Fill product fields with latest order data (for update if needed)
            const latest = orders[orders.length - 1];
            fillProductFields(latest);
            if (orderId) form.dataset.orderId = orderId;

            // "Add New Product" button handler
            document.getElementById('addNewProductBtn').onclick = () => {
                productFields.style.display = 'grid';
                addNewBtn.style.display = 'none';
                // Clear fields for new entry
                document.querySelectorAll('.product-field').forEach(i => i.value = (i.type === 'number' ? '0' : ''));
                form.dataset.isExtension = 'true';
                form.dataset.orderId = '';
            };
        } else if (mode === 'add_extension') {
            document.querySelectorAll('.product-field').forEach(i => i.value = (i.type === 'number' ? '0' : ''));
            form.dataset.isExtension = 'true';
        } else {
            // No orders yet - show product form directly (new customer or no history)
            ordersSection.style.display = 'none';
            addNewBtn.style.display = 'none';
            productFields.style.display = 'grid';
            fillProductFields(data);
            if (orderId) form.dataset.orderId = orderId;
        }

        // Populate Existing Lines
        if (data.lines && data.lines.length > 0) {
            renderLines(data.lines);
        }

        form.dataset.editId = data.id;
    }

    // Helper to safely get value from an element that might be missing
    const getVal = (id, defaultVal = '') => {
        const el = document.getElementById(id);
        if (!el) return defaultVal;
        return el.value;
    };

    // Handle Form Submission
    const form = document.getElementById('customerForm');
    if (form) {
        form.addEventListener('submit', async (e) => {
            e.preventDefault();

            try {
                // Collect Lines
                const lineData = [];
                document.querySelectorAll('.telephone-line-section').forEach(sec => {
                    const getLineVal = (selector) => {
                        const el = sec.querySelector(selector);
                        return el ? el.value : '';
                    };
                    lineData.push({
                        telephone_code: getLineVal('.telStdCode'),
                        telephone_number: getLineVal('.telNumber'),
                        line_type: getLineVal('.lineType'),
                        sip_no: getLineVal('.sipNo'),
                        start_date: getLineVal('.startDate'),
                        billing_account: getLineVal('.billingAcc'),
                        crm_customer_id: getLineVal('.crmCustId'),
                        submit_at_fms: getLineVal('.submitFms') || 'NO',
                        fms_submit_date: getLineVal('.fmsDate'),
                        is_closed: getLineVal('.isClosed') || 'NO',
                        closed_date: getLineVal('.closedDate'),
                        telephone_code_2: ''
                    });
                });

                const formData = {
                    circle: selCircle ? selCircle.value : '',
                    ssa: '', // SSA field removed from UI but kept in API for compatibility
                    oa_name: selOA ? selOA.value : '',
                    customer_name: getVal('custName'),
                    customer_code: getVal('customerCode'),
                    order_date: getVal('custOrderDate'),
                    acc_person_name: getVal('accName'),
                    acc_person_mobile: getVal('accMobile'),
                    acc_person_email: getVal('accEmail'),
                    tech_person_name: getVal('techName'),
                    tech_person_mobile: getVal('techMobile'),
                    tech_person_email: getVal('techEmail'),
                    customer_status: getVal('custStatus', 'OPEN'),
                    customer_closed_date: getVal('custClosedDate'),

                    product_plan: getVal('prodPlan'),
                    monthly_rent: getVal('monthlyRent', 0),
                    channels: getVal('channels', 0),
                    analog_line: getVal('analogLines', 0),
                    digital_line: getVal('digitalLines', 0),
                    vas_line: getVal('vasLines', 0),
                    ip_line: getVal('ipLines', 0),
                    analog_rent: getVal('analogRent', 0),
                    digital_rent: getVal('digitalRent', 0),
                    vas_rent: getVal('vasRent', 0),
                    ip_rent: getVal('ipRent', 0),
                    rg_port: getVal('rgPort', 0),
                    rg_rent: getVal('rgRent', 0),
                    plan_charge: getVal('planCharge', 0),
                    revenue_level: getVal('revLevel'),
                    epabx_model: getVal('epabxModel'),
                    product_start_date: getVal('prodStartDate'),
                    lines: lineData,
                    is_extension: form.dataset.isExtension === 'true',
                    order_id: form.dataset.orderId || null
                };

                const isEdit = form.dataset.editId;
                const url = isEdit ? `/api/customers/${isEdit}` : '/api/customers';
                const method = isEdit ? 'PUT' : 'POST';

                console.log('Registering/Updating customer...', formData);

                const res = await window.apiFetch(url, {
                    method: method,
                    body: JSON.stringify(formData)
                });

                if (res.ok) {
                    const result = await res.json();
                    alert(isEdit ? 'Customer updated successfully!' : 'Customer registered successfully!');
                    console.log('Success:', result);
                    window.location.reload();
                } else {
                    const err = await res.json();
                    alert('Error: ' + (err.error || 'Operation failed'));
                    console.error('API Error:', err);
                }
            } catch (err) {
                console.error('Submission Crash:', err);
                alert('Connection error or script crash. Check console.');
            }
        });
    }
});
