document.addEventListener('DOMContentLoaded', () => {
    checkAuth();

    const form = document.getElementById('taskForm');
    const container = document.getElementById('tasksContainer');

    // Fetch and render tasks
    async function loadTasks() {
        try {
            const res = await window.apiFetch('/api/tasks');
            if (res.ok) {
                const tasks = await res.json();
                renderTasks(tasks);
            }
        } catch (e) {
            container.innerHTML = `<p style="color:red;">Error loading tasks.</p>`;
        }
    }

    async function loadUsers() {
        try {
            const res = await window.apiFetch('/api/users');
            if (res.ok) {
                const users = await res.json();
                const select = document.getElementById('taskAssign');
                users.forEach(u => {
                    const opt = document.createElement('option');
                    opt.value = u.username;
                    opt.textContent = `${u.name || u.username} (${u.role})`;
                    select.appendChild(opt);
                });
            }
        } catch (e) { console.error('Error loading users'); }
    }

    function renderTasks(tasks) {
        if (tasks.length === 0) {
            container.innerHTML = '<p style="color:#64748b;">No tasks assigned right now. 🎉</p>';
            return;
        }

        const now = new Date();
        now.setHours(0, 0, 0, 0);

        const inTwoDays = new Date(now);
        inTwoDays.setDate(inTwoDays.getDate() + 2);

        let html = '';
        tasks.sort((a, b) => new Date(a.due_date) - new Date(b.due_date)).forEach(task => {
            let statusBadgeClass = task.status === 'PENDING' ? 'badge-pending'
                : task.status === 'IN PROGRESS' ? 'badge-progress' : 'badge-completed';

            let dueStr = 'No Due Date';
            let urgencyBadge = '';

            if (task.due_date) {
                const dueDate = new Date(task.due_date);
                dueDate.setHours(0, 0, 0, 0);
                dueStr = dueDate.toLocaleDateString();

                if (task.status !== 'COMPLETED') {
                    if (dueDate < now) {
                        urgencyBadge = '<span class="badge badge-overdue" style="font-size:0.65rem; padding:2px 6px; margin-left:8px;">OVERDUE</span>';
                    } else if (dueDate <= inTwoDays) {
                        urgencyBadge = '<span class="badge badge-pending" style="font-size:0.65rem; padding:2px 6px; margin-left:8px;">DUE SOON</span>';
                    } else {
                        urgencyBadge = '<span class="badge badge-completed" style="font-size:0.65rem; padding:2px 6px; margin-left:8px;">ON TRACK</span>';
                    }
                }
            }

            const assigneeDisplay = task.assignee_name ? `${task.assignee_name} (${task.assigned_to})` : (task.assigned_to || 'Unassigned');

            html += `
                <div class="task-card">
                    <div style="flex-grow:1;">
                        <h4 class="task-title" style="display:flex; align-items:center;">[${task.category || 'General'}] ${task.title} ${urgencyBadge}</h4>
                        <p class="task-meta">Assigned to: ${assigneeDisplay} | Due: ${dueStr}</p>
                        <p style="font-size: 0.9rem; margin: 10px 0 15px; color:#475569;">${task.description || 'No description provided.'}</p>
                        <span class="badge ${statusBadgeClass}">${task.status}</span>
                    </div>
                    <div style="margin-left:20px;">
                        <select onchange="updateStatus(${task.id}, this.value)" style="padding: 8px; border-radius: 8px; border:1px solid #cbd5e1; font-weight:600; cursor:pointer; background:#f8fafc;">
                            <option value="PENDING" ${task.status === 'PENDING' ? 'selected' : ''}>Pending</option>
                            <option value="IN PROGRESS" ${task.status === 'IN PROGRESS' ? 'selected' : ''}>In Progress</option>
                            <option value="COMPLETED" ${task.status === 'COMPLETED' ? 'selected' : ''}>Completed</option>
                        </select>
                    </div>
                </div>
            `;
        });
        container.innerHTML = html;
    }

    // Create Task
    form.onsubmit = async (e) => {
        e.preventDefault();
        const payload = {
            title: document.getElementById('taskTitle').value,
            description: document.getElementById('taskDesc').value,
            assigned_to: document.getElementById('taskAssign').value,
            due_date: document.getElementById('taskDue').value,
            category: document.getElementById('taskCategory').value
        };

        try {
            const res = await window.apiFetch('/api/tasks', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (res.ok) {
                form.reset();
                loadTasks();
            } else {
                alert('Failed to create task');
            }
        } catch (err) {
            alert('Error creating task');
        }
    };

    // Global update function
    window.updateStatus = async (id, status) => {
        try {
            const res = await window.apiFetch(`/api/tasks/${id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ status })
            });
            if (res.ok) {
                loadTasks();
            }
        } catch (e) {
            alert('Failed to update status');
        }
    };

    // Initial load
    loadTasks();
    loadUsers();
});
