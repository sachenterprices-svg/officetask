document.addEventListener('DOMContentLoaded', () => {
    // Input Fields
    const inputs = {
        bsnlCircle: document.getElementById('bsnlCircle'),
        bsnlOA: document.getElementById('bsnlOA'),
        senderName: document.getElementById('senderName'),
        senderDesignation: document.getElementById('senderDesignation'),
        senderMobile: document.getElementById('senderMobile'),
        senderEmail: document.getElementById('senderEmail'),
        customerName: document.getElementById('customerName'),
        customerEmail: document.getElementById('customerEmail'),
        customerAddress: document.getElementById('customerAddress'),
        contactPerson: document.getElementById('contactPerson'),
        contactMobile: document.getElementById('contactMobile'),
        category: document.getElementById('customerCategory'),
        planType: document.getElementById('planType'),
        businessSegment: document.getElementById('businessSegment'),
        sipCc: document.getElementById('sipCc'),
        pabxPort: document.getElementById('pabxPort'),
        ipExt: document.getElementById('ipExt'),
        analogExt: document.getElementById('analogExt'),
        vasExt: document.getElementById('vasExt'),
        sipPlan: document.getElementById('sipPlan'),
        channels: document.getElementById('channels'),
        freeDid: document.getElementById('freeDid'),
        extraDid: document.getElementById('extraDid'),
        nextFollowup: document.getElementById('nextFollowup'),
        directSaleToggle: document.getElementById('directSaleToggle'),
        quoteCategory: document.getElementById('quoteCategory'),
        quoteItem: document.getElementById('quoteItem'),
        quoteQty: document.getElementById('quoteQty'),
        quoteRate: document.getElementById('quoteRate'),
        addQuoteBtn: document.getElementById('addQuoteBtn'),
        quoteCurrentItems: document.getElementById('quoteCurrentItems')
    };

    // Application State
    let quotationItems = [];
    let catalogProducts = [];

    // UI Elements
    const els = {
        currentDate: document.getElementById('currentDate'),
        tableBody: document.getElementById('tableBody'),
        grandTotal: document.getElementById('grandTotal'),
        categoryFeatures: document.getElementById('categoryFeatures'),
        prevSipPlan: document.getElementById('prevSipPlan'),
        prevChannels: document.getElementById('prevChannels'),
        prevFreeDids: document.getElementById('prevFreeDids'),
        staticIpWarning: document.getElementById('staticIpWarning'),
        staticIpNotePreview: document.getElementById('staticIpNotePreview'),
        customerInfoBox: document.getElementById('customerInfoBox'),
        prevCustomerName: document.getElementById('prevCustomerName'),
        prevCustomerAddress: document.getElementById('prevCustomerAddress'),
        prevContactPerson: document.getElementById('prevContactPerson'),
        prevContactMobile: document.getElementById('prevContactMobile'),
        prevCustomerEmail: document.getElementById('prevCustomerEmail'),
        senderInfoBox: document.getElementById('senderInfoBox'),
        prevSenderName: document.getElementById('prevSenderName'),
        prevDesignation: document.getElementById('prevDesignation'),
        prevCircleOA: document.getElementById('prevCircleOA'),
        prevMobile: document.getElementById('prevMobile'),
        prevSenderEmailHeader: document.getElementById('prevSenderEmailHeader'),
        footerSignature: document.getElementById('footerSignature'),
        exportBtn: document.getElementById('exportBtn'),
        exportStatus: document.getElementById('exportStatus'),
        proposalPreview: document.getElementById('proposalPreview'),
        quotationPreviewSection: document.getElementById('quotationPreviewSection'),
        quotePreviewBody: document.getElementById('quotePreviewBody'),
        quoteGrandTotal: document.getElementById('quoteGrandTotal'),
        comboPlanWrapper: document.getElementById('comboPlanWrapper'),
        sipCcWrapper: document.getElementById('sipCcWrapper'),
        pabxPortWrapper: document.getElementById('pabxPortWrapper'),
        channelsWrapper: document.getElementById('channelsWrapper'),
        ipExtWrapper: document.getElementById('ipExtWrapper'),
        analogExtWrapper: document.getElementById('analogExtWrapper'),
        vasExtWrapper: document.getElementById('vasExtWrapper'),
        previewBtn: document.getElementById('previewBtn'),
        previewModal: document.getElementById('previewModal'),
        modalBody: document.getElementById('modalBody'),
        closePreviewBtn: document.getElementById('closePreviewBtn'),
        modalDownloadBtn: document.getElementById('modalDownloadBtn'),
        quotationBuilder: document.getElementById('quotationBuilder'),
        submitProposalBtn: document.getElementById('submitProposalBtn'),
        exportWordBtn: document.getElementById('exportWordBtn'),
        headerCircleOA: document.getElementById('headerCircleOA')
    };

    // Circular 57/20-21 Data
    const circularData = {
        Small: {
            didRatio: 2,
            sip: { 10: 6000, 20: 12000, 30: 16500, 40: 22000 },
            pabx: {
                "32A-8I": { name: "32 Analog / 8 IP / 4 Data", rate: 6800, analog: 32, ip: 8 },
                "64A-16I": { name: "64 Analog / 16 IP / 4 Data", rate: 9300, analog: 64, ip: 16 },
                "96A-24I": { name: "96 Analog / 24 IP / 4 Data", rate: 10800, analog: 96, ip: 24 }
            }
        },
        Medium: {
            didRatio: 4,
            sip: { 60: 33000, 100: 40000, 120: 48000, 150: 60000 },
            pabx: {
                "112-32": { name: "112 Analog / 32 IP / 24 Data", rate: 16800, analog: 112, ip: 32 },
                "144-32": { name: "144 Analog / 32 IP / 24 Data", rate: 18720, analog: 144, ip: 32 },
                "176-48": { name: "176 Analog / 48 IP / 24 Data", rate: 23520, analog: 176, ip: 48 },
                "208-48": { name: "208 Analog / 48 IP / 24 Data", rate: 25440, analog: 208, ip: 48 },
                "272-64": { name: "272 Analog / 64 IP / 48 Data", rate: 36480, analog: 272, ip: 64 },
                "336-64": { name: "336 Analog / 64 IP / 48 Data", rate: 40320, analog: 336, ip: 64 },
                "400-96": { name: "400 Analog / 96 IP / 48 Data", rate: 49920, analog: 400, ip: 96 },
                "464-96": { name: "464 Analog / 96 IP / 48 Data", rate: 53760, analog: 464, ip: 96 }
            }
        },
        Large: {
            didRatio: 4,
            sip: { 180: 125000, 240: 150000, 300: 175000, 360: 200000 },
            pabxPerPort: { analog: 60, ip: 180, data: 180 }
        }
    };

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
        "West Bengal": ["Asansol", "Bankura", "Birbhum", "Burdwan", "Cooch Behar", "Darjeeling", "Hooghly", "Howrah", "Jalpaiguri", "Kalyani", "Kharagpur", "Krishnanagar", "Malda", "Midnapore", "Murshidabad", "Purulia", "Raiganj", "Suri"]
    };

    // Initialization
    els.currentDate.textContent = new Date().toLocaleDateString('en-IN', { year: 'numeric', month: 'long', day: 'numeric' });
    const sortedCircles = Object.keys(bsnlData).sort();
    sortedCircles.forEach(circle => {
        const opt = document.createElement('option');
        opt.value = circle;
        opt.textContent = circle;
        inputs.bsnlCircle.appendChild(opt);
    });

    // Helper functions
    function getIpVasRate(qty) {
        if (qty === 0) return 0;
        if (qty < 48) return 300;
        if (qty >= 48 && qty <= 96) return 225;
        return 180;
    }
    function getAnalogRate(qty) {
        if (qty === 0) return 0;
        if (qty < 48) return 100;
        if (qty >= 48 && qty <= 96) return 75;
        return 60;
    }
    function getSipTrunkRate(ch) {
        if (ch < 10) return 0;
        if (ch <= 25) return 600;
        if (ch <= 45) return 550;
        if (ch <= 90) return 550;
        if (ch <= 490) return 400;
        if (ch <= 990) return 350;
        if (ch <= 1900) return 300;
        if (ch <= 4900) return 250;
        return 200;
    }
    function getSipFreeMinutes(ch) {
        if (ch < 10) return 0;
        if (ch <= 25) return 850;
        if (ch <= 45) return 800;
        if (ch <= 90) return 600;
        if (ch <= 490) return 450;
        if (ch <= 990) return 400;
        if (ch <= 1900) return 350;
        if (ch <= 4900) return 300;
        return 250;
    }

    function updateSegmentOptions() {
        const isCombo = inputs.planType.value === 'Combo';
        if (!isCombo) return;
        const seg = inputs.businessSegment.value;
        const data = circularData[seg];

        // No longer populating sipCc dropdown as we use dynamic channels input
        els.sipCcWrapper.style.display = 'none';
        els.channelsWrapper.style.display = 'block';

        inputs.pabxPort.innerHTML = '';
        if (data.pabx) {
            Object.keys(data.pabx).forEach(id => {
                const opt = document.createElement('option');
                opt.value = id;
                opt.textContent = data.pabx[id].name;
                inputs.pabxPort.appendChild(opt);
            });
            els.pabxPortWrapper.style.display = 'block';
            els.ipExtWrapper.style.display = 'none';
            els.analogExtWrapper.style.display = 'none';
        } else {
            els.pabxPortWrapper.style.display = 'none';
            els.ipExtWrapper.style.display = 'block';
            els.analogExtWrapper.style.display = 'block';
        }
        calculateDids();
    }

    function calculateDids() {
        try {
            // Unify channel input: always use the numeric 'channels' field
            let val = parseInt(inputs.channels.value) || 10;
            if (val < 10) val = 10;
            // Enforce 5-increment logic for all plans as per user request
            val = Math.round(val / 5) * 5;
            inputs.channels.value = val;

            const ch = val;
            let ip = parseInt(inputs.ipExt.value) || 0;
            let analog = parseInt(inputs.analogExt.value) || 0;

            const isCombo = inputs.planType.value === 'Combo';
            if (isCombo) {
                const data = circularData[inputs.businessSegment.value];
                if (data.pabx && data.pabx[inputs.pabxPort.value]) {
                    ip = data.pabx[inputs.pabxPort.value].ip;
                    analog = data.pabx[inputs.pabxPort.value].analog;
                }
            }

            // --- DID Ratio per Circular 57/20-21 ---
            // < 50 CC = 2x ratio, >= 50 CC = 4x ratio
            const ratio = ch < 50 ? 2 : 4;
            const free = ch * ratio;

            console.log(`[DID Calc] Ch: ${ch}, IP: ${ip}, Analog: ${analog}, Ratio: ${ratio}, Free: ${free}`);

            inputs.freeDid.value = free;
            // Extra DIDs = (Total Extensions) - (Free DIDs)
            inputs.extraDid.value = Math.max(0, (ip + analog) - free);

            inputs.sipPlan.value = ch >= 50 ? 'MPLS' : 'Internet';
            updateProposal();
        } catch (err) {
            console.error("Error in calculateDids:", err);
        }
    }

    function updateProposal() {
        const vals = {
            circle: inputs.bsnlCircle.value,
            oa: inputs.bsnlOA.value,
            senderName: inputs.senderName.value,
            custName: inputs.customerName.value,
            planType: inputs.planType.value,
            segment: inputs.businessSegment.value,
            channels: parseInt(inputs.channels.value) || 0,
            ipQty: parseInt(inputs.ipExt.value) || 0,
            analogQty: parseInt(inputs.analogExt.value) || 0,
            vasQty: parseInt(inputs.vasExt.value) || 0,
            freeDids: parseInt(inputs.freeDid.value) || 0,
            extraDids: parseInt(inputs.extraDid.value) || 0,
            sipPlan: inputs.sipPlan.value
        };

        // UI Previews
        els.prevSipPlan.textContent = `BSNL SIP Trunk - ${vals.sipPlan}`;
        els.prevChannels.textContent = vals.channels;
        els.prevFreeDids.textContent = vals.freeDids;
        els.staticIpWarning.style.display = vals.sipPlan === 'Internet' ? 'block' : 'none';
        els.prevCustomerName.textContent = vals.custName || '________________';
        els.prevSenderName.textContent = vals.senderName || '________________';
        els.headerCircleOA.textContent = vals.circle && vals.oa ? `${vals.circle} / ${vals.oa}` : (vals.circle || vals.oa || '');

        // Show/Hide Info Boxes
        els.customerInfoBox.style.display = vals.custName ? 'block' : 'none';
        els.senderInfoBox.style.display = vals.senderName ? 'block' : 'none';

        // Update other preview fields
        els.prevCustomerAddress.textContent = inputs.customerAddress.value || '';
        els.prevContactPerson.textContent = inputs.contactPerson.value || '';
        els.prevContactMobile.textContent = inputs.contactMobile.value || '';
        els.prevDesignation.textContent = inputs.senderDesignation.value ? `, ${inputs.senderDesignation.value}` : '';
        els.prevMobile.textContent = inputs.senderMobile.value || '';
        els.prevSenderEmailHeader.textContent = inputs.senderEmail.value || '';

        // Update Circle/OA in box
        els.prevCircleOA.textContent = vals.circle && vals.oa ? `${vals.circle} / ${vals.oa}` : (vals.circle || vals.oa || '-');

        // Update Footer Signature
        els.footerSignature.innerHTML = `For BSNL,<br>${vals.senderName || '________________'}<br><span style="font-weight:normal; font-size:0.9em;">${inputs.senderDesignation.value || ''}</span>`;

        // Pricing
        let sipTotal = 0, sipRate = 0, sipFree = 0;
        let pabxTotal = 0, pabxLabel = "";

        if (vals.planType === 'Combo') {
            const data = circularData[vals.segment];
            // Calculate SIP Rent for arbitrary channels
            // Find the best per-channel rate from the defined slabs
            const slabs = Object.keys(data.sip).map(Number).sort((a, b) => a - b);
            let nearestSlab = slabs[0];
            slabs.forEach(s => { if (vals.channels >= s) nearestSlab = s; });

            const baseRate = data.sip[nearestSlab];
            const perChannelRate = baseRate / nearestSlab;
            sipTotal = perChannelRate * vals.channels;
            sipRate = perChannelRate;
            sipFree = vals.segment === 'Small' ? 850 : 450;

            if (vals.segment === 'Large') {
                pabxTotal = (vals.ipQty * 180) + (vals.analogQty * 60);
                pabxLabel = "Large Segment Port Configuration";
            } else {
                const b = data.pabx[inputs.pabxPort.value];
                pabxTotal = b ? b.rate : 0;
                pabxLabel = b ? b.name : "Bundled PABX";
            }
        } else {
            sipRate = getSipTrunkRate(vals.channels);
            sipTotal = vals.channels * sipRate;
            sipFree = getSipFreeMinutes(vals.channels);
        }

        const ipTotal = vals.planType === 'Normal' ? (vals.ipQty * getIpVasRate(vals.ipQty)) : 0;
        const analogTotal = vals.planType === 'Normal' ? (vals.analogQty * getAnalogRate(vals.analogQty)) : 0;
        const vasTotal = vals.vasQty * getIpVasRate(vals.vasQty);
        const extraDidTotal = vals.extraDids * 10;
        const grandTotal = sipTotal + pabxTotal + ipTotal + analogTotal + vasTotal + extraDidTotal;

        // Table
        let rows = `<tr>
            <td><b>SIP Trunk Configuration (${vals.channels} CC)</b><br><small>Includes ${sipFree} free mins/ch.</small></td>
            <td style="text-align:center;">${vals.channels}</td>
            <td style="text-align:right;">${vals.planType === 'Combo' ? 'Bundled' : '₹' + sipRate}</td>
            <td style="text-align:right;">₹${sipTotal.toLocaleString()}</td>
        </tr>`;

        if (pabxTotal > 0) {
            rows += `<tr>
                <td><b>EPABX Subscription (${pabxLabel})</b></td>
                <td style="text-align:center;">-</td><td style="text-align:right;">-</td>
                <td style="text-align:right;">₹${pabxTotal.toLocaleString()}</td>
            </tr>`;
        }

        if (vals.planType === 'Normal') {
            if (ipTotal > 0) rows += `<tr><td>IP User Extensions</td><td style="text-align:center;">${vals.ipQty}</td><td style="text-align:right;">₹${getIpVasRate(vals.ipQty)}</td><td style="text-align:right;">₹${ipTotal.toLocaleString()}</td></tr>`;
            if (analogTotal > 0) rows += `<tr><td>Analog Extensions</td><td style="text-align:center;">${vals.analogQty}</td><td style="text-align:right;">₹${getAnalogRate(vals.analogQty)}</td><td style="text-align:right;">₹${analogTotal.toLocaleString()}</td></tr>`;
        }
        if (vasTotal > 0) rows += `<tr><td>VAS Ports</td><td style="text-align:center;">${vals.vasQty}</td><td style="text-align:right;">₹${getIpVasRate(vals.vasQty)}</td><td style="text-align:right;">₹${vasTotal.toLocaleString()}</td></tr>`;
        if (extraDidTotal > 0) rows += `<tr><td>Extra DIDs</td><td style="text-align:center;">${vals.extraDids}</td><td style="text-align:right;">₹10</td><td style="text-align:right;">₹${extraDidTotal.toLocaleString()}</td></tr>`;

        els.tableBody.innerHTML = rows;
        els.grandTotal.textContent = `₹ ${grandTotal.toLocaleString()}`;
        window.currentProposalPayload = vals;
    }

    function downloadPDF() {
        const el = els.proposalPreview;
        const ow = el.style.width, omw = el.style.minWidth;
        el.style.width = '210mm'; el.style.minWidth = '210mm';
        el.classList.add('pdf-container');

        const opt = {
            margin: 5, filename: `BSNL_Proposal_${new Date().toISOString().slice(0, 10)}.pdf`,
            html2canvas: { scale: 2, useCORS: true, windowWidth: 794 },
            jsPDF: { unit: 'mm', format: 'a4', orientation: 'portrait' }
        };

        els.exportStatus.textContent = "Generating PDF...";
        html2pdf().set(opt).from(el).save().then(() => {
            el.style.width = ow; el.style.minWidth = omw;
            el.classList.remove('pdf-container');
            els.exportStatus.textContent = "✓ PDF Success";
        });
    }

    function downloadWord() {
        const el = els.proposalPreview;
        const ow = el.style.width, omw = el.style.minWidth;
        const clone = el.cloneNode(true);
        clone.style.width = '210mm';

        // Ensure "To" and "From" boxes are visible in Word
        const toBox = clone.querySelector('#customerInfoBox');
        const fromBox = clone.querySelector('#senderInfoBox');
        if (toBox) toBox.style.display = 'block';
        if (fromBox) fromBox.style.display = 'block';

        const header = `
            <html xmlns:o='urn:schemas-microsoft-com:office:office' xmlns:w='urn:schemas-microsoft-com:office:word' xmlns='http://www.w3.org/TR/REC-html40'>
            <head><meta charset='utf-8'><title>BSNL Proposal</title>
            <style>
                body { font-family: 'Inter', sans-serif; line-height: 1.5; color: #334155; }
                table { border-collapse: collapse; width: 100%; border: 1px solid #e2e8f0; margin-bottom: 20px; }
                th, td { border: 1px solid #e2e8f0; padding: 10px; font-size: 10pt; }
                th { background-color: #f1f5f9; color: #475569; font-weight: 600; }
                .proposal-header { border-bottom: 3px solid #0033a0; padding-bottom: 20px; margin-bottom: 30px; }
                .brand-name { font-size: 24pt; font-weight: 800; color: #0033a0; }
                .tagline { font-size: 10pt; color: #e31837; font-weight: 600; }
                h1 { color: #0033a0; font-size: 18pt; margin-bottom: 5px; }
                h2, h3 { color: #0033a0; font-size: 14pt; border-bottom: 2px solid #e2e8f0; padding-bottom: 5px; margin-top: 25px; }
                .info-boxes { margin-bottom: 25px; }
                .customer-info-box, .sender-info-box { background: #f8fafc; border: 1px solid #e2e8f0; padding: 15px; border-radius: 8px; }
                .page-break { page-break-before: always; }
            </style>
            </head><body>`;
        const footer = "</body></html>";
        const html = header + clone.innerHTML + footer;

        const blob = new Blob(['\ufeff', html], { type: 'application/msword' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `BSNL_Proposal_${new Date().toISOString().slice(0, 10)}.doc`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);

        els.exportStatus.style.display = 'block';
        els.exportStatus.textContent = "✓ Doc Success";
    }

    async function submitProposal() {
        if (!window.hasPermission('proposals', 'add')) {
            alert("You do not have permission to create proposals.");
            return;
        }
        const payload = window.currentProposalPayload;
        if (!payload || !payload.custName) {
            alert("Please fill in customer details before submitting.");
            return;
        }

        const body = {
            customer_name: payload.custName,
            customer_category: inputs.category.value,
            proposal_data: payload,
            direct_sale: inputs.directSaleToggle.checked,
            quotation_items: quotationItems,
            next_followup_date: inputs.nextFollowup.value || null
        };

        try {
            els.exportStatus.style.display = 'block';
            els.exportStatus.textContent = "Submitting to CRM...";

            const res = await window.apiFetch('/api/proposals', {
                method: 'POST',
                body: JSON.stringify(body)
            });

            if (res.ok) {
                els.exportStatus.textContent = "✓ Submitted Successfully!";
                alert("Proposal saved to CRM database.");
            } else {
                const errData = await res.json();
                throw new Error(errData.error || "Submission failed");
            }
        } catch (err) {
            console.error(err);
            els.exportStatus.textContent = "❌ Submission Failed";
            alert("Error: " + err.message);
        }
    }

    // Listeners
    inputs.planType.addEventListener('change', (e) => {
        const isCombo = e.target.value === 'Combo';
        els.comboPlanWrapper.style.display = isCombo ? 'block' : 'none';
        els.sipCcWrapper.style.display = 'none'; // Always hide dropdown, we use dynamic input
        els.pabxPortWrapper.style.display = isCombo ? 'block' : 'none';
        els.channelsWrapper.style.display = 'block'; // Always show numeric input

        // Hide/Show manual extensions based on Combo/Normal
        els.ipExtWrapper.style.display = isCombo ? 'none' : 'block';
        els.analogExtWrapper.style.display = isCombo ? 'none' : 'block';

        if (isCombo) updateSegmentOptions(); else updateProposal();
    });

    inputs.bsnlCircle.addEventListener('change', (e) => {
        const circle = e.target.value;
        console.log(`[OA Event] Selection changed to: ${circle}`);

        const oas = bsnlData[circle] || [];
        inputs.bsnlOA.innerHTML = '<option value="">Select OA...</option>';

        if (oas.length > 0) {
            oas.sort().forEach(oa => {
                const opt = document.createElement('option');
                opt.value = oa;
                opt.textContent = oa;
                inputs.bsnlOA.appendChild(opt);
            });
            inputs.bsnlOA.disabled = false;
            console.log(`[OA] Circle: ${circle}, OA Count: ${oas.length}`);
        } else {
            inputs.bsnlOA.disabled = true;
            console.log(`[OA Warning] No OAs found for circle: ${circle}`);
        }
        updateProposal();
    });

    inputs.businessSegment.addEventListener('change', updateSegmentOptions);
    inputs.sipCc.addEventListener('change', calculateDids);
    inputs.sipCc.addEventListener('input', calculateDids);
    inputs.pabxPort.addEventListener('change', updateProposal);
    inputs.channels.addEventListener('change', calculateDids);
    inputs.channels.addEventListener('input', calculateDids);
    [inputs.ipExt, inputs.analogExt, inputs.vasExt].forEach(i => i.addEventListener('input', calculateDids));

    // Preview
    els.previewBtn.addEventListener('click', () => {
        els.modalBody.innerHTML = '';
        const clone = els.proposalPreview.cloneNode(true);
        clone.style.width = '210mm'; clone.classList.add('pdf-container');
        els.modalBody.appendChild(clone);
        els.previewModal.style.display = 'block';
    });

    els.closePreviewBtn.addEventListener('click', () => els.previewModal.style.display = 'none');
    els.modalDownloadBtn.addEventListener('click', downloadPDF);
    els.exportBtn.addEventListener('click', downloadPDF);
    els.exportWordBtn.addEventListener('click', downloadWord);
    els.submitProposalBtn.addEventListener('click', submitProposal);

    // --- QUOTATION BUILDER (ANNEXURE B) ---
    async function loadCatalog() {
        try {
            const [catRes, prodRes] = await Promise.all([
                window.apiFetch('/api/categories'),
                window.apiFetch('/api/products')
            ]);
            if (catRes.ok && prodRes.ok) {
                const categories = await catRes.json();
                catalogProducts = await prodRes.json();

                inputs.quoteCategory.innerHTML = '<option value="">Select Category...</option>';
                categories.forEach(c => {
                    const opt = document.createElement('option');
                    opt.value = c.id;
                    opt.textContent = c.name;
                    inputs.quoteCategory.appendChild(opt);
                });
            }
        } catch (err) { console.error("Error loading catalog:", err); }
    }

    inputs.quoteCategory.addEventListener('change', () => {
        const catId = inputs.quoteCategory.value;
        inputs.quoteItem.innerHTML = '<option value="">Select Item...</option>';
        catalogProducts.filter(p => p.category_id == catId).forEach(p => {
            const opt = document.createElement('option');
            opt.value = p.id;
            opt.textContent = p.model_name;
            opt.dataset.price = p.default_price;
            inputs.quoteItem.appendChild(opt);
        });
    });

    inputs.quoteItem.addEventListener('change', () => {
        const opt = inputs.quoteItem.options[inputs.quoteItem.selectedIndex];
        if (opt && opt.dataset.price) {
            inputs.quoteRate.value = opt.dataset.price;
        }
    });

    inputs.addQuoteBtn.addEventListener('click', () => {
        const itemOpt = inputs.quoteItem.options[inputs.quoteItem.selectedIndex];
        if (!itemOpt.value) return;

        const item = {
            id: Date.now(),
            productId: itemOpt.value,
            name: itemOpt.textContent,
            qty: parseInt(inputs.quoteQty.value) || 1,
            rate: parseFloat(inputs.quoteRate.value) || 0
        };

        quotationItems.push(item);
        renderQuotation();
    });

    function renderQuotation() {
        // UI Builder Table
        if (quotationItems.length === 0) {
            inputs.quoteCurrentItems.innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 15px; color: #64748b;">No items added yet.</td></tr>';
        } else {
            inputs.quoteCurrentItems.innerHTML = quotationItems.map(item => `
                <tr>
                    <td style="padding: 8px; border-bottom: 1px solid #f1f5f9;">${item.name}</td>
                    <td style="padding: 8px; border-bottom: 1px solid #f1f5f9; text-align:center;">${item.qty}</td>
                    <td style="padding: 8px; border-bottom: 1px solid #f1f5f9; text-align:right;">₹${item.rate.toLocaleString()}</td>
                    <td style="padding: 8px; border-bottom: 1px solid #f1f5f9; text-align:right;">₹${(item.qty * item.rate).toLocaleString()}</td>
                    <td style="padding: 8px; border-bottom: 1px solid #f1f5f9; text-align:center;">
                        <button onclick="window.removeQuoteItem(${item.id})" style="color:#ef4444; border:none; background:none; cursor:pointer;">&times;</button>
                    </td>
                </tr>
            `).join('');
        }

        // Preview Section (Annexure B)
        const isDirectSale = inputs.directSaleToggle.checked;
        els.quotationPreviewSection.style.display = (isDirectSale && quotationItems.length > 0) ? 'block' : 'none';

        if (isDirectSale && quotationItems.length > 0) {
            let total = 0;
            els.quotePreviewBody.innerHTML = quotationItems.map(item => {
                const amount = item.qty * item.rate;
                total += amount;
                return `
                    <tr>
                        <td>${item.name}</td>
                        <td style="text-align:center;">${item.qty}</td>
                        <td style="text-align:right;">₹${item.rate.toLocaleString()}</td>
                        <td style="text-align:right;">₹${amount.toLocaleString()}</td>
                    </tr>
                `;
            }).join('');
            els.quoteGrandTotal.textContent = `₹ ${total.toLocaleString()}`;
        }
    }

    window.removeQuoteItem = (id) => {
        quotationItems = quotationItems.filter(i => i.id !== id);
        renderQuotation();
    };

    inputs.directSaleToggle.addEventListener('change', (e) => {
        els.quotationBuilder.style.display = e.target.checked ? 'block' : 'none';
        renderQuotation();
    });

    loadCatalog();
    calculateDids();
});
