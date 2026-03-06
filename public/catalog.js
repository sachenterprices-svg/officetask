// catalog.js
document.addEventListener('DOMContentLoaded', () => {

    // UI Elements
    const els = {
        catTable: document.getElementById('categoriesTableBody'),
        prodTable: document.getElementById('productsTableBody'),
        addCatForm: document.getElementById('addCategoryForm'),
        addProdForm: document.getElementById('addProductForm'),
        prodCategorySelect: document.getElementById('prodCategory'),
        filterCategorySelect: document.getElementById('filterCategory')
    };

    // Role-based UI logic
    if (window.currentUser && window.currentUser.role === 'admin') {
        const adminEls = document.querySelectorAll('.admin-only, .admin-nav');
        adminEls.forEach(el => el.style.display = 'block');
        // Fix form styling that might be broken by display:block instead of flex
        els.addCatForm.style.display = 'block';
        els.addProdForm.style.display = 'block';
    }

    // Load Data
    async function loadCategories() {
        try {
            const res = await window.apiFetch('/api/categories');
            if (res.ok) {
                const categories = await res.json();
                renderCategories(categories);
                updateCategoryDropdowns(categories);
            }
        } catch (err) {
            console.error('Failed to load categories', err);
        }
    }

    async function loadProducts(categoryId = '') {
        try {
            const res = await window.apiFetch(`/api/products${categoryId ? '?category_id=' + categoryId : ''}`);
            if (res.ok) {
                const products = await res.json();
                renderProducts(products);
            }
        } catch (err) {
            console.error('Failed to load products', err);
        }
    }

    // Render logic
    function renderCategories(cats) {
        if (!cats.length) {
            els.catTable.innerHTML = '<tr><td style="color:#64748b; text-align:center;">No categories found.</td></tr>';
            return;
        }
        let html = '';
        cats.forEach(c => {
            html += `<tr><td><strong>${c.name}</strong></td></tr>`;
        });
        els.catTable.innerHTML = html;
    }

    function renderProducts(prods) {
        if (!prods.length) {
            els.prodTable.innerHTML = '<tr><td colspan="3" style="color:#64748b; text-align:center;">No products found in this category.</td></tr>';
            return;
        }
        let html = '';
        prods.forEach(p => {
            html += `
                <tr>
                    <td><span style="background:#f1f5f9; padding:3px 8px; border-radius:4px; font-size:0.85em; color:#475569;">${p.category_name}</span></td>
                    <td>${p.model_name}</td>
                    <td style="text-align: right; font-weight: 500;">₹ ${parseFloat(p.default_price).toLocaleString()}</td>
                </tr>
            `;
        });
        els.prodTable.innerHTML = html;
    }

    function updateCategoryDropdowns(cats) {
        let options = '<option value="">Select Category...</option>';
        cats.forEach(c => {
            options += `<option value="${c.id}">${c.name}</option>`;
        });

        els.prodCategorySelect.innerHTML = options;

        // Update filter dropdown (keep "All" option)
        els.filterCategorySelect.innerHTML = '<option value="">All Categories</option>' + options.replace('<option value="">Select Category...</option>', '');
    }

    // Event Listeners
    els.filterCategorySelect.addEventListener('change', (e) => {
        loadProducts(e.target.value);
    });

    els.addCatForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const name = document.getElementById('catName').value;
        const btn = els.addCatForm.querySelector('button');
        btn.disabled = true;

        try {
            const res = await window.apiFetch('/api/categories', {
                method: 'POST',
                body: JSON.stringify({ name })
            });

            if (res.ok) {
                els.addCatForm.reset();
                loadCategories();
            } else {
                alert('Failed to add category. Note: Names must be unique.');
            }
        } catch (err) {
            alert('Network error');
        } finally {
            btn.disabled = false;
        }
    });

    els.addProdForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const category_id = els.prodCategorySelect.value;
        const model_name = document.getElementById('prodName').value;
        const default_price = document.getElementById('prodPrice').value || 0;

        const btn = els.addProdForm.querySelector('button');
        btn.disabled = true;

        try {
            const res = await window.apiFetch('/api/products', {
                method: 'POST',
                body: JSON.stringify({ category_id, model_name, default_price })
            });

            if (res.ok) {
                els.addProdForm.reset();
                // refresh products for current filter
                loadProducts(els.filterCategorySelect.value);
            } else {
                alert('Failed to add product');
            }
        } catch (err) {
            alert('Network error');
        } finally {
            btn.disabled = false;
        }
    });

    // Initial Load
    loadCategories();
    loadProducts();
});
