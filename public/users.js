const MODULES_CONFIG = [
    { id: "proposals", label: "Proposals", actions: ["view", "add", "edit"] },
    { id: "customers", label: "Customers", actions: ["view", "add", "edit"] },
    { id: "complaints", label: "Complaints", actions: ["view", "add", "edit"] },
    { id: "reconciliation", label: "Reconciliation", actions: ["view", "add", "edit"] },
    { id: "users", label: "User Hierarchy", actions: ["view", "add", "edit"] },
    { id: "system", label: "System Maintenance", actions: ["view"] }
];

let users = [];
let selectedUserId = null;

// Initialize Page
document.addEventListener('DOMContentLoaded', () => {
    loadUsers();
    renderDynamicPermissions();

    // Form Submissions
    document.getElementById('userForm').addEventListener('submit', saveUser);
});

async function loadUsers() {
    try {
        const res = await window.apiFetch('/api/users');
        if (res.ok) {
            users = await res.json();
            renderTable();
            populateManagersDropdown();
        } else {
            console.error("Failed to load users");
        }
    } catch (err) {
        console.error(err);
    }
}

function populateManagersDropdown() {
    const select = document.getElementById('modalReportsTo');
    if (!select) return;

    // Save current value if editing
    const currentVal = select.value;

    select.innerHTML = '<option value="">None (Top Level)</option>';
    users.forEach(u => {
        const opt = document.createElement('option');
        opt.value = u.id;
        opt.textContent = `${u.name || u.username} (${u.role})`;
        select.appendChild(opt);
    });

    select.value = currentVal;
}

function renderTable() {
    const tbody = document.getElementById('usersTableBody');
    if (users.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align: center; padding: 40px;">No users found.</td></tr>';
        return;
    }

    let html = '';
    users.forEach(u => {
        // Summarize rights
        let rights = [];
        try {
            const perms = typeof u.permissions === 'string' ? JSON.parse(u.permissions) : u.permissions;
            rights = (perms || []).map(p => {
                const mod = MODULES_CONFIG.find(m => m.id === p);
                return mod ? mod.id : p;
            });
        } catch (e) { }

        const rightsText = rights.length > 0 ? rights.join(', ') : 'None';

        html += `
            <tr>
                <td style="text-align:center; font-weight:700; color:#64748b;">${u.id}</td>
                <td style="font-weight:700; color:#0f172a;">${u.name || '-'}</td>
                <td>${u.username}</td>
                <td><span style="background:#f1f5f9; padding:4px 10px; border-radius:4px; font-size:0.75rem; font-weight:700; text-transform:uppercase;">${u.role}</span></td>
                <td>${u.manager_name || '<i style="color:#cbd5e1">No Manager</i>'}</td>
                <td style="font-size:0.8rem; max-width: 250px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title="${rightsText}">${rightsText}</td>
                <td style="text-align:center;">
                    <div style="display:flex; gap:10px; justify-content:center;">
                        <button onclick="openEditModal(${u.id})" style="background:none; border:1px solid #e2e8f0; padding:6px; border-radius:6px; cursor:pointer;" title="Edit Profile">&#9998;</button>
                        <button onclick="deleteUser(${u.id})" style="background:none; border:1px solid #fee2e2; padding:6px; border-radius:6px; cursor:pointer; color:#ef4444;" title="Delete User">&#128465;</button>
                    </div>
                </td>
            </tr>
        `;
    });
    tbody.innerHTML = html;
}

// --- DYNAMIC PERMISSIONS RENDERER ---
function renderDynamicPermissions() {
    const container = document.getElementById('dynamicPermissionsContainer');
    // Set grid to 3 columns for View, Add, Edit
    container.style.display = 'block';

    let html = `
        <div style="display:grid; grid-template-columns: 2fr 1fr 1fr 1fr; gap:10px; font-weight:800; font-size:0.75rem; color:#64748b; margin-bottom:10px; text-transform:uppercase;">
            <div>Module</div>
            <div style="text-align:center;">View</div>
            <div style="text-align:center;">Add</div>
            <div style="text-align:center;">Edit</div>
        </div>
    `;

    MODULES_CONFIG.forEach(mod => {
        html += `
            <div style="display:grid; grid-template-columns: 2fr 1fr 1fr 1fr; gap:10px; align-items:center; padding:8px 0; border-bottom:1px solid #f1f5f9;">
                <div style="font-weight:700; color:#334155;">${mod.label}</div>
                <div style="text-align:center;"><input type="checkbox" name="perm_${mod.id}_view" value="${mod.id}:view"></div>
                <div style="text-align:center;">${mod.actions.includes('add') ? `<input type="checkbox" name="perm_${mod.id}_add" value="${mod.id}:add">` : '-'}</div>
                <div style="text-align:center;">${mod.actions.includes('edit') ? `<input type="checkbox" name="perm_${mod.id}_edit" value="${mod.id}:edit">` : '-'}</div>
            </div>
        `;
    });
    container.innerHTML = html;
}

// --- MODAL LOGIC ---
function openAddModal() {
    document.getElementById('userForm').reset();
    document.getElementById('modalUserId').value = '';
    document.getElementById('userModalTitle').innerText = 'Add New User';
    document.getElementById('passwordRow').style.display = 'block';
    document.getElementById('modalPassword').setAttribute('required', 'true');
    document.getElementById('modalUsername').disabled = false;
    document.getElementById('userModal').style.display = 'flex';
}

function openEditModal(id) {
    const user = users.find(u => u.id === id);
    if (!user) return;

    selectedUserId = id;

    document.getElementById('modalUserId').value = user.id;
    document.getElementById('modalName').value = user.name || '';
    document.getElementById('modalUsername').value = user.username || '';
    document.getElementById('modalUsername').disabled = true;
    document.getElementById('modalRole').value = user.role || 'user';
    document.getElementById('modalCircle').value = user.allowed_circle || '';
    document.getElementById('modalOA').value = user.allowed_oa || '';
    document.getElementById('modalCustomers').value = user.allowed_customers || '';
    document.getElementById('modalBackdate').checked = user.backdate_rights || false;

    // Set Permissions
    let perms = [];
    try { perms = typeof user.permissions === 'string' ? JSON.parse(user.permissions) : user.permissions; } catch (e) { }
    const checkboxes = document.querySelectorAll('#dynamicPermissionsContainer input[type="checkbox"]');
    checkboxes.forEach(cb => {
        cb.checked = (perms || []).includes(cb.value);
    });

    // Hide password field during standard edit
    document.getElementById('passwordRow').style.display = 'none';
    document.getElementById('modalPassword').removeAttribute('required');

    document.getElementById('userModalTitle').innerText = 'Edit User Profile';
    document.getElementById('userModal').style.display = 'flex';
}

function closeModals() {
    document.getElementById('userModal').style.display = 'none';
}

window.closeModals = closeModals;

// --- SAVE FUNCTION ---
async function saveUser(e) {
    e.preventDefault();
    const id = document.getElementById('modalUserId').value;
    const isEditing = id !== '';

    // Gather permissions
    const permissions = [];
    document.querySelectorAll('#dynamicPermissionsContainer input[type="checkbox"]:checked').forEach(cb => {
        permissions.push(cb.value);
    });

    const payload = {
        name: document.getElementById('modalName').value,
        username: document.getElementById('modalUsername').value,
        role: document.getElementById('modalRole').value,
        reports_to: document.getElementById('modalReportsTo').value || null,
        allowed_circle: document.getElementById('modalCircle').value || null,
        allowed_oa: document.getElementById('modalOA').value || null,
        allowed_customers: document.getElementById('modalCustomers').value || null,
        permissions: permissions,
        backdate_rights: document.getElementById('modalBackdate').checked
    };

    if (!isEditing) {
        payload.password = document.getElementById('modalPassword').value;
    }

    const url = isEditing ? `/api/users/${id}` : '/api/users';
    const method = isEditing ? 'PUT' : 'POST';

    try {
        const res = await window.apiFetch(url, {
            method: method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (res.ok) {
            closeModals();
            loadUsers();
        } else {
            const err = await res.json();
            alert(err.error || 'Failed to save user');
        }
    } catch (err) {
        console.error(err);
        alert('Network error');
    }
}

async function deleteUser(id) {
    if (!confirm("Are you sure you want to delete this user? This action cannot be undone.")) return;

    try {
        // Note: DELETE endpoint might need to be added to app.js if not already there
        const res = await window.apiFetch(`/api/users/${id}`, { method: 'DELETE' });
        if (res.ok) {
            loadUsers();
        } else {
            alert("Could not delete user. They may have related data (proposals/tasks).");
        }
    } catch (err) { alert('Network error'); }
}
