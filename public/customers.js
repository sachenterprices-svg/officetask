document.addEventListener('DOMContentLoaded', () => {
    const bsnlData = {
        "Andaman and Nicobar": ["Port Blair"],
        "Andhra Pradesh": ["Anantapur", "Chittoor", "Cuddapah", "East Godavari", "Guntur", "Krishna", "Kurnool", "Nellore", "Prakasam", "Srikakulam", "Visakhapatnam", "Vizianagaram", "West Godavari"],
        "Assam": ["Bongaigaon", "Cachar", "Dibrugarh", "Jorhat", "Kamrup", "Karbi Anglong", "Lakhimpur", "Nagaon", "Nalbari", "Sibsagar", "Tezpur"],
        "Bihar": ["Ara", "Aurangabad", "Begusarai", "Bhagalpur", "Chapra", "Darbhanga", "Gaya", "Hajipur", "Katihar", "Khagaria", "Munger", "Motihari", "Muzaffarpur", "Patna", "Purnea", "Saharsa", "Samastipur", "Sasaram"],
        "Chhattisgarh": ["Bastar", "Bilaspur", "Durg", "Raigarh", "Raipur", "Surguja"],
        "Chennai Telephones": ["Chennai"],
        "Gujarat": ["Ahmedabad", "Amreli", "Bharuch", "Bhavnagar", "Bhuj", "Godhra", "Himmatnagar", "Jamnagar", "Junagadh", "Kheda", "Mehsana", "Nadiad", "Palanpur", "Rajkot", "Surat", "Surendranagar", "Vadodara", "Valsad"],
        "Haryana": ["Ambala", "Faridabad", "Gurgaon", "Hissar", "Karnal", "Kurukshetra", "Panipat", "Rohtak", "Sirsa", "Sonepat"],
        "Himachal Pradesh": ["Dharamshala", "Hamirpur", "Kullu", "Mandi", "Shimla", "Solan"],
        "Jammu and Kashmir": ["Anantnag", "Baramulla", "Jammu", "Kathua", "Leh", "Rajouri", "Srinagar", "Udhampur"],
        "Jharkhand": ["Bokaro", "Dhanbad", "Dumka", "Hazaribagh", "Jamshedpur", "Ranchi"],
        "Karnataka": ["Bangalore", "Belagavi", "Bellary", "Bidar", "Bijapur", "Chikmagalur", "Dakshina Kannada", "Davanagere", "Dharwad", "Gadag", "Gulbarga", "Hassan", "Karwar", "Kolar", "Koppal", "Mandya", "Mysore", "Raichur", "Shimoga", "Tumkur", "Udupi"],
        "Kerala": ["Alappuzha", "Ernakulam", "Idukki", "Kannur", "Kasaragod", "Kollam", "Kottayam", "Kozhikode", "Malappuram", "Palakkad", "Pathanamthitta", "Thiruvananthapuram", "Thrissur", "Wayanad"],
        "Kolkata Telephones": ["Kolkata"],
        "Madhya Pradesh": ["Balaghat", "Betul", "Bhind", "Bhopal", "Chhatarpur", "Chhindwara", "Damoh", "Dewas", "Dhar", "Guna", "Gwalior", "Hoshangabad", "Indore", "Jabalpur", "Khandwa", "Khargone", "Mandsaur", "Morena", "Narsinghpur", "Neemuch", "Panna", "Raisen", "Rajgarh", "Ratlam", "Rewa", "Sagar", "Satna", "Sehore", "Seoni", "Shahdol", "Shajapur", "Shivpuri", "Sidhi", "Tikamgarh", "Ujjain", "Umaria", "Vidisha"],
        "Maharashtra": ["Ahmednagar", "Akola", "Amravati", "Aurangabad", "Beed", "Bhandara", "Buldhana", "Chandrapur", "Dhule", "Gadchiroli", "Gondia", "Hingoli", "Jalgaon", "Jalna", "Kalyan", "Kolhapur", "Latur", "Nagpur", "Nanded", "Nandurbar", "Nashik", "Osmanabad", "Parbhani", "Pune", "Raigad", "Ratnagiri", "Sangli", "Satara", "Sindhudurg", "Solapur", "Thane", "Wardha", "Washim", "Yavatmal"],
        "North East-I": ["Meghalaya", "Mizoram", "Tripura"],
        "North East-II": ["Arunachal Pradesh", "Manipur", "Nagaland"],
        "Odisha": ["Balasore", "Berhampur", "Bhubaneswar", "Cuttack", "Dhenkanal", "Keonjhar", "Koraput", "Phulbani", "Rourkela", "Sambalpur", "Sundargarh"],
        "Punjab": ["Amritsar", "Bathinda", "Chandigarh", "Ferozepur", "Hoshiarpur", "Jalandhar", "Ludhiana", "Pathankot", "Patiala", "Rupnagar", "Sangrur"],
        "Rajasthan": ["Ajmer", "Alwar", "Banswara", "Barmer", "Bharatpur", "Bhilwara", "Bikaner", "Bundi", "Chittorgarh", "Churu", "Dausa", "Dholpur", "Dungarpur", "Hanumangarh", "Jaipur", "Jaisalmer", "Jalore", "Jhalawar", "Jhunjhunu", "Jodhpur", "Karauli", "Kota", "Nagaur", "Pali", "Pratapgarh", "Rajsamand", "Sawai Madhopur", "Sikar", "Sirohi", "Sri Ganganagar", "Tonk", "Udaipur"],
        "Tamil Nadu": ["Chennai", "Coimbatore", "Cuddalore", "Dharmapuri", "Dindigul", "Erode", "Kanchipuram", "Kanyakumari", "Karur", "Krishnagiri", "Madurai", "Nagapattinam", "Namakkal", "Nilgiris", "Perambalur", "Pudukkottai", "Ramanathapuram", "Salem", "Sivaganga", "Thanjavur", "Theni", "Thoothukudi", "Tiruchirappalli", "Tirunelveli", "Tirupathur", "Tiruvallur", "Tiruvannamalai", "Tiruvarur", "Vellore", "Viluppuram", "Virudhunagar"],
        "Telangana": ["Adilabad", "Hyderabad", "Karimnagar", "Khammam", "Mahabubnagar", "Medak", "Nalgonda", "Nizamabad", "Rangareddy", "Warangal"],
        "Uttar Pradesh (East)": ["Allahabad", "Azamgarh", "Bahraich", "Ballia", "Banda", "Barabanki", "Basti", "Deoria", "Faizabad", "Farrukhabad", "Fatehpur", "Ghazipur", "Gonda", "Gorakhpur", "Hardoi", "Jaunpur", "Kanpur", "Lakhimpur", "Lucknow", "Mau", "Mirzapur", "Orai", "Pratapgarh", "Raebareli", "Shahjahanpur", "Sitapur", "Sultanpur", "Unnao", "Varanasi"],
        "Uttar Pradesh (West)": ["Agra", "Aligarh", "Bareilly", "Bijnor", "Budaun", "Bulandshahr", "Etah", "Etawah", "Ghaziabad", "Hapur", "Mathura", "Meerut", "Moradabad", "Muzaffarnagar", "Noida", "Pilibhit", "Rampur", "Saharanpur"],
        "Uttarakhand": ["Almora", "Dehradun", "Haldwani", "Haridwar", "Nainital", "Pauri", "Srinagar (Garhwal)", "Tehri"],
        "West Bengal": ["Asansol", "Bankura", "Birbhum", "Burdwan", "Cooch Behar", "Darjeeling", "Hooghly", "Howrah", "Jalpaiguri", "Kalyani", "Kharagpur", "Krishnanagar", "Malda", "Murshidabad", "Purulia", "Raiganj", "Suri"]
    };

    const selCircle = document.getElementById('custCircle');
    const selOA = document.getElementById('custOa');

    if (selCircle) {
        const sortedCircles = Object.keys(bsnlData).sort();
        sortedCircles.forEach(circle => {
            const opt = document.createElement('option');
            opt.value = circle;
            opt.textContent = circle;
            selCircle.appendChild(opt);
        });

        selCircle.addEventListener('change', (e) => {
            const circle = e.target.value;
            const oas = bsnlData[circle] || [];
            selOA.innerHTML = '<option value="">Select OA...</option>';
            if (oas.length > 0) {
                oas.sort().forEach(oa => {
                    const opt = document.createElement('option');
                    opt.value = oa;
                    opt.textContent = oa;
                    selOA.appendChild(opt);
                });
                selOA.disabled = false;
            } else {
                selOA.disabled = true;
            }
        });
    }

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

            // Replace the ADD button with a REMOVE button in clones
            const btnContainer = newSection.querySelector('#addNewLineBtn').parentElement;
            btnContainer.innerHTML = `
                <button type="button" class="remove-line-btn" style="background:#ef4444; color:white; border:none; padding:10px 15px; border-radius:8px; font-weight:700; cursor:pointer; width:100%;">
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
            section.querySelector('.telStdCode').value = line.telephone_code || '';
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

            // Buttons logic
            const btnContainer = section.querySelector('#addNewLineBtn').parentElement;
            if (idx === 0) {
                // Keep the ADD button for the first row only, or handle differently
                // Actually, let's make ALL existing rows have REMOVE, and add one empty at the end?
                // Or just the first one has ADD, others have REMOVE.
                // Let's stick to the user's current pattern: 1st has ADD, clones have REMOVE.
            } else {
                btnContainer.innerHTML = `
                    <button type="button" class="remove-line-btn" style="background:#ef4444; color:white; border:none; padding:10px 15px; border-radius:8px; font-weight:700; cursor:pointer; width:100%;">
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
            document.getElementById('custSsa').value = data.ssa || '';
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

        // Product Details
        if (mode === 'add_extension') {
            // Extension Mode: Clear product fields but keep customer info
            document.querySelectorAll('.product-field').forEach(i => i.value = (i.type === 'number' ? 0 : ''));
            form.dataset.isExtension = 'true';
            alert('Extension Mode Active: Please enter the NEW order details.');
        } else {
            document.getElementById('prodPlan').value = data.product_plan || '';
            document.getElementById('monthlyRent').value = data.monthly_rent || 0;
            document.getElementById('channels').value = data.channels || 0;
            document.getElementById('analogLines').value = data.analog_line || 0;
            document.getElementById('digitalLines').value = data.digital_line || 0;
            document.getElementById('vasLines').value = data.vas_line || 0;
            document.getElementById('ipLines').value = data.ip_line || 0;
            document.getElementById('analogRent').value = data.analog_rent || 0;
            document.getElementById('digitalRent').value = data.digital_rent || 0;
            document.getElementById('vasRent').value = data.vas_rent || 0;
            document.getElementById('ipRent').value = data.ip_rent || 0;
            document.getElementById('rgPort').value = data.rg_port || 0;
            document.getElementById('rgRent').value = data.rg_rent || 0;
            document.getElementById('planCharge').value = data.plan_charge || 0;
            document.getElementById('revLevel').value = data.revenue_level || '';
            document.getElementById('epabxModel').value = data.epabx_model || '';
            document.getElementById('prodStartDate').value = data.product_start_date ? data.product_start_date.split('T')[0] : '';

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
