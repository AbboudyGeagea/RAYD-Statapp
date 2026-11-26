// static/js/report_presets.js — saved filter presets under the shared report panel
// (templates/_report_filter_panel.html report_presets(), routes/report_presets.py; operator,
// 2026-11-26: the Super Report's "Saved presets" on Report 22). A preset is the panel's form as
// it stands (dates, dropdowns, "Apply filter" ticks, Include / Exclude); opening one reloads the
// page with those values, exactly as Generate would.
(function () {
    const box = document.getElementById('rp-presets');
    if (!box) return;
    const url = `/report/${box.dataset.report}/presets`;
    const list = document.getElementById('rp-presets-list');
    const msg = document.getElementById('rp-presets-msg');
    let presets = [];

    const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g,
        c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));

    function say(text, ok) {
        msg.textContent = text;
        msg.style.color = ok ? '#34d399' : '#f87171';
        if (ok) setTimeout(() => { if (msg.textContent === text) msg.textContent = ''; }, 3000);
    }

    // the panel's fields with a value, each as a list (a dropdown sends one per value chosen)
    function panelValues() {
        const out = {};
        new FormData(document.getElementById('mainForm')).forEach((v, k) => {
            if (k === 'csrf_token' || v === '' || typeof v !== 'string') return;
            (out[k] = out[k] || []).push(v);
        });
        return out;
    }

    function draw() {
        if (!presets.length) {
            list.innerHTML = '<p class="text-[10px] text-slate-500 italic">No saved presets</p>';
            return;
        }
        list.innerHTML = presets.map(p => `
            <div class="flex items-center gap-1 group">
                <button type="button" onclick="rpOpen(${p.id})" title="${esc(p.name)} · ${esc(p.updated_at || '')}"
                        class="flex-1 text-left text-[10px] font-bold text-slate-400 hover:text-white truncate transition-colors py-1 px-2 rounded hover:bg-[#1d2522]">
                    <i class="bi bi-bookmark-fill text-[#60a5fa] mr-1" style="font-size:8px;" aria-hidden="true"></i>${esc(p.name)}
                </button>
                <button type="button" onclick="rpDelete(${p.id})" aria-label="Delete ${esc(p.name)}"
                        class="opacity-0 group-hover:opacity-100 focus:opacity-100 text-slate-500 hover:text-red-400 transition-all px-1 text-xs">
                    <i class="bi bi-trash3" aria-hidden="true"></i>
                </button>
            </div>`).join('');
    }

    async function load() {
        try {
            const res = await fetch(url);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            presets = await res.json();
            draw();
        } catch (e) {
            list.innerHTML = '<p class="text-[10px] text-slate-500 italic">Could not load the presets</p>';
        }
    }

    window.rpSave = async function () {
        const name = prompt('Name this preset:');
        if (!name || !name.trim()) return;
        try {
            const res = await fetch(url, {method: 'POST', headers: {'Content-Type': 'application/json'},
                                          body: JSON.stringify({name: name.trim(), filters: panelValues()})});
            const d = await res.json();
            if (!res.ok || !d.success) throw new Error(d.error || `HTTP ${res.status}`);
            say(`Saved "${d.name}"`, true);
            load();
        } catch (e) {
            say('Not saved: ' + e.message, false);
        }
    };

    window.rpOpen = function (id) {
        const p = presets.find(x => x.id === id);
        if (!p) return;
        const qs = new URLSearchParams();
        Object.entries(p.filters || {}).forEach(([k, vs]) => (Array.isArray(vs) ? vs : [vs]).forEach(v => qs.append(k, v)));
        window.location.href = window.location.pathname + '?' + qs.toString();
    };

    window.rpDelete = async function (id) {
        const p = presets.find(x => x.id === id);
        if (!confirm(`Delete the preset "${p ? p.name : id}"?`)) return;
        try {
            const res = await fetch(`${url}/${id}`, {method: 'DELETE'});
            const d = await res.json();
            if (!res.ok || !d.success) throw new Error(d.error || `HTTP ${res.status}`);
            say('Deleted', true);
            load();
        } catch (e) {
            say('Not deleted: ' + e.message, false);
        }
    };

    window.rpClear = function () {
        window.location.href = window.location.pathname;
    };

    load();
})();
