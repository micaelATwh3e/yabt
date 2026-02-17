async function postJson(url, payload) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload || {}),
  });
  return res.json();
}

async function refreshStatus() {
  const res = await fetch('/api/status');
  if (!res.ok) {
    return;
  }
  const data = await res.json();
  const running = new Set(data.queue.running || []);
  const queued = new Set(data.queue.queued || []);

  document.querySelectorAll('[data-profile-id]').forEach((card) => {
    const profileId = Number(card.getAttribute('data-profile-id'));
    const badge = card.querySelector('.status-badge');
    if (!badge) {
      return;
    }
    if (running.has(profileId)) {
      badge.textContent = 'running';
      badge.className = 'badge warn status-badge';
    } else if (queued.has(profileId)) {
      badge.textContent = 'queued';
      badge.className = 'badge idle status-badge';
    } else {
      badge.textContent = 'idle';
      badge.className = 'badge idle status-badge';
    }
  });

  const schedulerBtn = document.querySelector('[data-scheduler-toggle]');
  if (schedulerBtn) {
    schedulerBtn.textContent = data.scheduler_enabled ? 'Scheduler: On' : 'Scheduler: Off';
    schedulerBtn.classList.toggle('secondary', !data.scheduler_enabled);
  }

  document.querySelectorAll('[data-system-run]').forEach((button) => {
    const key = button.getAttribute('data-system-run');
    const running = data.system && data.system[key];
    button.textContent = `${key.toUpperCase()}: ${running ? 'Running' : 'Run now'}`;
    button.disabled = !!running;
  });
}

function wireActions() {
  document.querySelectorAll('[data-run-now]').forEach((button) => {
    button.addEventListener('click', async () => {
      const profileId = button.getAttribute('data-run-now');
      button.disabled = true;
      const result = await postJson(`/api/profiles/${profileId}/run`);
      if (!result.success) {
        alert(result.message || 'Unable to queue run');
      }
      button.disabled = false;
      refreshStatus();
    });
  });

  const schedulerBtn = document.querySelector('[data-scheduler-toggle]');
  if (schedulerBtn) {
    schedulerBtn.addEventListener('click', async () => {
      await postJson('/api/scheduler/toggle');
      refreshStatus();
    });
  }

  document.querySelectorAll('[data-system-run]').forEach((button) => {
    button.addEventListener('click', async () => {
      const key = button.getAttribute('data-system-run');
      button.disabled = true;
      const result = await postJson(`/api/system/${key}/run`);
      if (!result.success) {
        alert(result.message || 'Unable to start task');
      }
      refreshStatus();
    });
  });
}

window.addEventListener('load', () => {
  wireActions();
  refreshStatus();
  setInterval(refreshStatus, 10000);
});
