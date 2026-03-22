function logout() {
  localStorage.removeItem('spts_token');
  localStorage.removeItem('spts_role');
  localStorage.removeItem('spts_username');
  globalThis.location.href = '/static/login.html';
}

function backToApp() {
  globalThis.location.href = '/static/index.html';
}

function _setVlkgStatusView(status, fallbackError) {
  const statusMsg = document.getElementById('vlkg-status-msg');
  const collectionEl = document.getElementById('vlkg-collection');
  const countEl = document.getElementById('vlkg-count');
  const pathEl = document.getElementById('vlkg-path');
  const errorEl = document.getElementById('vlkg-error');

  if (!statusMsg || !collectionEl || !countEl || !pathEl || !errorEl) {
    return;
  }

  if (!status) {
    statusMsg.textContent = 'Unavailable';
    collectionEl.textContent = '-';
    countEl.textContent = '-';
    pathEl.textContent = '-';
    errorEl.textContent = fallbackError || 'unknown';
    return;
  }

  const ready = Boolean(status.collection_ready);
  statusMsg.textContent = ready ? 'Ready' : 'Not Ready';
  collectionEl.textContent = status.collection_name || 'spts_vlkg';
  countEl.textContent = String(status.mapping_count ?? 0);
  pathEl.textContent = status.chroma_path || '-';
  errorEl.textContent = status.error || 'none';
}

async function refreshVlkgStatus() {
  const refreshBtn = document.getElementById('refreshVlkgBtn');
  const token = localStorage.getItem('spts_token');

  if (!token) {
    logout();
    return;
  }

  if (refreshBtn) {
    refreshBtn.disabled = true;
    refreshBtn.textContent = 'Refreshing...';
  }

  try {
    const response = await fetch('/admin/vlkg-status', {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });

    if (response.status === 401) {
      logout();
      return;
    }

    if (response.status === 403) {
      _setVlkgStatusView(null, 'admin access required');
      return;
    }

    if (!response.ok) {
      _setVlkgStatusView(null, `request failed (${response.status})`);
      return;
    }

    const payload = await response.json();
    _setVlkgStatusView(payload, null);
  } catch (error) {
    console.error('VLKG status refresh failed:', error);
    _setVlkgStatusView(null, 'unexpected error');
  } finally {
    if (refreshBtn) {
      refreshBtn.disabled = false;
      refreshBtn.textContent = 'Refresh';
    }
  }
}

async function ensureAdmin() {
  const token = localStorage.getItem('spts_token');
  const role = (localStorage.getItem('spts_role') || '').toLowerCase();

  if (!token) {
    logout();
    return false;
  }

  if (role === 'admin') {
    return true;
  }

  try {
    const response = await fetch('/me', {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (response.status === 401) {
      logout();
      return false;
    }

    if (!response.ok) {
      return false;
    }

    const me = await response.json();
    localStorage.setItem('spts_role', me.role || '');
    localStorage.setItem('spts_username', me.username || '');
    return (me.role || '').toLowerCase() === 'admin';
  } catch (error) {
    console.warn('Failed to verify admin role via /me:', error);
    return false;
  }
}

async function createAdminUser() {
  const errorMsg = document.getElementById('error-msg');
  const successMsg = document.getElementById('success-msg');
  const createBtn = document.getElementById('createBtn');

  errorMsg.style.display = 'none';
  successMsg.style.display = 'none';

  const username = document.getElementById('username').value.trim();
  const password = document.getElementById('password').value;
  const role = document.getElementById('role').value;

  if (!username || !password) {
    errorMsg.textContent = 'Please enter username and password.';
    errorMsg.style.display = 'block';
    return;
  }

  if (password.length < 4) {
    errorMsg.textContent = 'Password must be at least 4 characters long.';
    errorMsg.style.display = 'block';
    return;
  }

  const token = localStorage.getItem('spts_token');
  if (!token) {
    logout();
    return;
  }

  createBtn.disabled = true;
  createBtn.textContent = 'Creating...';

  try {
    const response = await fetch('/admin/register', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ username, password, role }),
    });

    if (response.status === 401) {
      logout();
      return;
    }

    const payload = await response.json();

    if (!response.ok) {
      errorMsg.textContent = payload.detail || 'Failed to create user.';
      errorMsg.style.display = 'block';
      return;
    }

    successMsg.textContent = `Created user '${username}' with role '${payload.role}'.`;
    successMsg.style.display = 'block';
    document.getElementById('username').value = '';
    document.getElementById('password').value = '';
  } catch (error) {
    console.error('Admin user creation failed:', error);
    errorMsg.textContent = 'Unexpected error while creating user.';
    errorMsg.style.display = 'block';
  } finally {
    createBtn.disabled = false;
    createBtn.textContent = 'Create User';
  }
}

globalThis.addEventListener('DOMContentLoaded', async () => {
  const allowed = await ensureAdmin();
  if (!allowed) {
    alert('Admin access required.');
    globalThis.location.href = '/static/index.html';
    return;
  }

  await refreshVlkgStatus();
});
