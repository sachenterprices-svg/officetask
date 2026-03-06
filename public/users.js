// --- DYNAMIC MODULES LIST ---
// Add new modules here in the future. The UI will automatically generate checkboxes for them.
const AVAILABLE_MODULES = [
    { id: "create_proposal", label: "Create Proposals" },
    { id: "view_own_reports", label: "View Own Reports" },
    { id: "view_all_reports", label: "View All Reports (Admin)" },
    { id: "manage_catalog", label: "Manage Catalog" },
    { id: "manage_users", label: "Manage Users" }
];

let users = [];
let selectedUserId = null;

// Initialize Page
document.addEventListener('DOMContentLoaded', () => {
    // Admin Guard Check
    if (!window.currentUser || window.currentUser.role !== 'admin') {
        window.location.href = '/index.html';
        return;
    }

    loadUsers();
    renderDynamicPermissions();

    // Listen for radio button selection in the table
    document.getElementById('usersTableBody').addEventListener('change', (e) => {
        if (e.target.name === 'selectedUser') {
            selectedUserId = parseInt(e.target.value);
        }
    });

    // Form Submissions
    document.getElementById('userForm').addEventListener('submit', saveUser);
    document.getElementById('pwdForm').addEventListener('submit', updatePassword);
    document.getElementById('rightsForm').addEventListener('submit', saveRights);
    document.getElementById('areaForm').addEventListener('submit', saveArea);
});

async function loadUsers() {
    try {
        const res = await window.apiFetch('/api/users');
        if (res.ok) {
            users = await res.json();
            renderTable();
        } else {
            alert("Failed to load users");
        }
    } catch (err) {
        console.error(err);
    }
}

function renderTable() {
    const tbody = document.getElementById('usersTableBody');
    if (users.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" style="text-align: center;">No users found.</td></tr>';
        return;
    }

    let html = '';
    users.forEach(u => {
        html += `
            <tr>
                <td style="text-align:center;">
                    <input type="radio" name="selectedUser" value="${u.id}" ${selectedUserId === u.id ? 'checked' : ''}>
                </td>
                <td>${u.id}</td>
                <td>${u.name || '-'}</td>
                <td>${u.username}</td>
                <td>${u.mobile || '-'}</td>
                <td>${u.email || '-'}</td>
                <td style="text-align:center;">
                    <button class="legacy-btn" onclick="openEditModal(${u.id})" style="padding:2px 5px;">&#9998;</button>
                </td>
                <td style="text-align:center;">
                    <button class="legacy-btn" onclick="deleteUser(${u.id})" style="padding:2px 5px; color:red;">&#128465;</button>
                </td>
            </tr>
        `;
    });
    tbody.innerHTML = html;
}

// --- DYNAMIC PERMISSIONS RENDERER ---
function renderDynamicPermissions() {
    const container = document.getElementById('dynamicPermissionsContainer');
    let html = '';
    AVAILABLE_MODULES.forEach(mod => {
        html += `
            <label style="display:flex; align-items:center; gap:5px; font-size:0.9rem;">
                <input type="checkbox" name="modulePerm" value="${mod.id}"> ${mod.label}
            </label>
        `;
    });
    container.innerHTML = html;
}

// --- ACTION BUTTON HANDLER ---
function triggerAction(actionName) {
    if (!selectedUserId) {
        alert("Please select a user from the list first by clicking the radio button on the left.");
        return;
    }

    const user = users.find(u => u.id === selectedUserId);
    if (!user) return;

    if (actionName === 'changePassword' || actionName === 'resetPassword') {
        document.getElementById('pwdNewPassword').val('');
        document.getElementById('passwordModal').style.display = 'flex';
    } else if (actionName === 'assignRights') {
        openRightsModal(user);
    } else if (actionName === 'assignPermission') {
        openAreaModal(user);
    } else if (actionName === 'assignCustomer') {
        // Future module stub
    }
}

// --- MODAL LOGIC ---
function openAddModal() {
    document.getElementById('userForm').reset();
    document.getElementById('modalUserId').value = '';
    document.getElementById('userModalTitle').innerText = 'Add New User';
    document.getElementById('passwordRow').style.display = 'block';
    document.getElementById('modalPassword').setAttribute('required', 'true');
    document.getElementById('userModal').style.display = 'flex';
}

function openEditModal(id) {
    const user = users.find(u => u.id === id);
    if (!user) return;

    selectedUserId = id; // auto-select this user
    renderTable(); // re-render to check the radio box

    document.getElementById('modalUserId').value = user.id;
    document.getElementById('modalName').value = user.name || '';
    document.getElementById('modalUsername').value = user.username || '';
    document.getElementById('modalUsername').disabled = true; // Cannot edit username
    document.getElementById('modalRole').value = user.role || 'user';
    document.getElementById('modalMobile').value = user.mobile || '';
    document.getElementById('modalEmail').value = user.email || '';

    // Hide password field for standard edit (use action buttons for pwd change)
    document.getElementById('passwordRow').style.display = 'none';
    document.getElementById('modalPassword').removeAttribute('required');

    document.getElementById('userModalTitle').innerText = 'Edit User Profile';
    document.getElementById('userModal').style.display = 'flex';
}

function openRightsModal(user) {
    // Check boxes based on user.permissions
    let perms = [];
    try { perms = typeof user.permissions === 'string' ? JSON.parse(user.permissions) : user.permissions; } catch (e) { }
    if (!perms) perms = [];

    const checkboxes = document.querySelectorAll('input[name="modulePerm"]');
    checkboxes.forEach(cb => {
        cb.checked = perms.includes(cb.value);
    });

    document.getElementById('rightsModal').style.display = 'flex';
}

function openAreaModal(user) {
    document.getElementById('areaCircle').value = user.allowed_circle || '';
    document.getElementById('areaOA').value = user.allowed_oa || '';
    document.getElementById('areaModal').style.display = 'flex';
}

function closeModals() {
    document.querySelectorAll('.modal').forEach(m => m.style.display = 'none');
}

window.closeModals = closeModals; // global for HTML onclicks

// --- SAVE FUNCTIONS (API CALLS) ---

async function saveUser(e) {
    e.preventDefault();
    const id = document.getElementById('modalUserId').value;
    const isEditing = id !== '';

    const payload = {
        name: document.getElementById('modalName').value,
        role: document.getElementById('modalRole').value,
        mobile: document.getElementById('modalMobile').value,
        email: document.getElementById('modalEmail').value
    };

    let url = '/api/users';
    let method = 'POST';

    if (isEditing) {
        url = `/api/users/${id}`;
        method = 'PUT';
        // Keep existing area/perms
        const existing = users.find(u => Math.abs(u.id) === Math.abs(id));
        if (existing) {
            payload.allowed_circle = existing.allowed_circle;
            payload.allowed_oa = existing.allowed_oa;
            payload.permissions = existing.permissions;
        }
    } else {
        payload.username = document.getElementById('modalUsername').value;
        payload.password = document.getElementById('modalPassword').value;
    }

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

async function updatePassword(e) {
    e.preventDefault();
    if (!selectedUserId) return;

    const newPwd = document.getElementById('pwdNewPassword').value;
    try {
        const res = await window.apiFetch(`/api/users/${selectedUserId}/password`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ new_password: newPwd })
        });
        if (res.ok) {
            alert("Password updated successfully.");
            closeModals();
        } else {
            const err = await res.json();
            alert(err.error);
        }
    } catch (err) {
        alert('Network error');
    }
}

async function saveRights(e) {
    e.preventDefault();
    if (!selectedUserId) return;
    const user = users.find(u => u.id === selectedUserId);

    // Gather checked permissions
    const perms = [];
    document.querySelectorAll('input[name="modulePerm"]:checked').forEach(cb => {
        perms.push(cb.value);
    });

    const payload = { ...user, permissions: perms };

    try {
        const res = await window.apiFetch(`/api/users/${selectedUserId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (res.ok) {
            closeModals();
            loadUsers(); // Refresh data
        }
    } catch (err) { alert('Error updating rights'); }
}

async function saveArea(e) {
    e.preventDefault();
    if (!selectedUserId) return;
    const user = users.find(u => u.id === selectedUserId);

    const payload = {
        ...user,
        allowed_circle: document.getElementById('areaCircle').value,
        allowed_oa: document.getElementById('areaOA').value
    };

    try {
        const res = await window.apiFetch(`/api/users/${selectedUserId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (res.ok) {
            closeModals();
            loadUsers();
        }
    } catch (err) { alert('Error updating area coverage'); }
}

async function deleteUser(id) {
    if (confirm("Future feature: Deleting users requires checking dependencies (proposals created). Proceed?")) {
        // Implement DELETE logic when required OR add active/inactive status flag
        alert("Action stubbed for safety. Recommend setting status to 'inactive' instead.");
    }
}
