(function () {
  'use strict';

  var POLL_MS = 2000;
  var $ = function (id) { return document.getElementById(id); };
  var rows = {};          // job id -> {tr, cells}
  var latest = {};        // job id -> last job payload
  var timer = null;
  var openJob = null;

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined) e.textContent = text;
    return e;
  }

  function fmtDuration(ms) {
    if (ms < 0) ms = 0;
    if (ms < 1000) return ms + 'ms';
    var s = ms / 1000;
    if (s < 60) return s.toFixed(1) + 's';
    var m = Math.floor(s / 60);
    var rest = Math.floor(s % 60);
    return m + 'm ' + (rest < 10 ? '0' : '') + rest + 's';
  }

  function duration(j) {
    if (!j.started_at) return '—';
    var start = Date.parse(j.started_at);
    if (j.finished_at) return fmtDuration(Date.parse(j.finished_at) - start);
    if (j.status === 'PROCESSING') return fmtDuration(Date.now() - start);
    return '—';
  }

  function fmtNum(n) { return Number(n).toLocaleString(); }

  // ---------- jobs table ----------

  function renderProgress(cell, j) {
    cell.textContent = '';
    var bar = el('div', 'bar');
    var text = el('div', 'ptext');
    if (j.status === 'PROCESSING') {
      bar.classList.add('busy');
      text.textContent = fmtNum(j.total_rows) + ' rows so far';
    } else if (j.status === 'COMPLETED') {
      var total = j.valid_rows + j.invalid_rows;
      var v = el('div', 'valid'), b = el('div', 'invalid');
      v.style.width = total ? (j.valid_rows / total * 100) + '%' : '100%';
      b.style.width = total ? (j.invalid_rows / total * 100) + '%' : '0';
      bar.append(v, b);
      text.textContent = fmtNum(j.valid_rows) + ' valid / ' + fmtNum(j.invalid_rows) + ' invalid';
    } else if (j.status === 'FAILED') {
      bar.classList.add('failed');
      text.classList.add('failed');
      text.textContent = j.error_msg || 'Failed';
      cell.title = j.error_msg || '';
    }
    cell.append(bar, text);
  }

  function renderLinks(cell, j) {
    cell.textContent = '';
    if (j.status !== 'COMPLETED') return;
    var s = el('button', null, 'Summary');
    s.type = 'button';
    s.addEventListener('click', function () { showDetail(j.id); });
    var a = el('a', null, 'errors.csv');
    a.href = '/api/jobs/' + encodeURIComponent(j.id) + '/errors.csv';
    a.download = '';
    cell.append(s, a);
  }

  function upsertRow(j) {
    var r = rows[j.id];
    if (!r) {
      var tr = el('tr');
      r = { tr: tr, name: el('td', 'fname'), status: el('td'), progress: el('td'), attempts: el('td', 'num'),
            dur: el('td', 'num'), links: el('td', 'links'), sig: {} };
      tr.append(r.name, r.status, r.progress, r.attempts, r.dur, r.links);
      rows[j.id] = r;
      $('jobs').appendChild(tr);
    }
    r.name.textContent = j.filename;
    r.name.title = j.filename;
    if (r.sig.status !== j.status) {
      r.status.textContent = '';
      r.status.appendChild(el('span', 'badge ' + j.status, j.status));
    }
    var psig = [j.status, j.total_rows, j.valid_rows, j.invalid_rows, j.error_msg].join('|');
    if (r.sig.progress !== psig) renderProgress(r.progress, j);
    if (r.sig.status !== j.status) renderLinks(r.links, j);
    r.attempts.textContent = j.attempts;
    r.dur.textContent = duration(j);
    r.sig = { status: j.status, progress: psig };
  }

  function renderJobs(list) {
    var seen = {};
    list.forEach(function (j) { seen[j.id] = true; latest[j.id] = j; upsertRow(j); });
    // Keep API order (newest first) without rebuilding rows.
    var body = $('jobs');
    list.forEach(function (j) { body.appendChild(rows[j.id].tr); });
    Object.keys(rows).forEach(function (id) {
      if (!seen[id]) { rows[id].tr.remove(); delete rows[id]; delete latest[id]; }
    });
    $('empty').hidden = list.length > 0;
  }

  function poll() {
    fetch('/api/jobs?limit=50', { cache: 'no-store' })
      .then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .then(function (data) {
        $('jobs-error').hidden = true;
        $('poll').textContent = 'Updated ' + new Date().toLocaleTimeString();
        renderJobs(data.jobs || data);
      })
      .catch(function (e) {
        $('jobs-error').textContent = 'Could not load jobs: ' + e.message;
        $('jobs-error').hidden = false;
      })
      .then(schedule);
  }

  function schedule() {
    clearTimeout(timer);
    timer = null;
    if (!document.hidden) timer = setTimeout(poll, POLL_MS);
  }

  document.addEventListener('visibilitychange', function () {
    if (document.hidden) { clearTimeout(timer); timer = null; }
    else if (!timer) { clearTimeout(timer); poll(); }
  });

  // ---------- detail panel ----------

  function stat(label, value) {
    var d = el('div', 'stat');
    d.append(el('b', null, value), el('span', null, label));
    return d;
  }

  function breakdown(title, counts) {
    var keys = Object.keys(counts || {}).sort(function (a, b) { return counts[b] - counts[a] || (a < b ? -1 : 1); });
    var frag = document.createDocumentFragment();
    frag.appendChild(el('h3', null, title));
    if (!keys.length) { frag.appendChild(el('p', 'hint', 'No errors.')); return frag; }
    var t = el('table');
    var head = el('tr');
    head.append(el('th', null, title.indexOf('rule') >= 0 ? 'Rule' : 'Field'), el('th', 'num', 'Errors'));
    t.appendChild(head);
    keys.forEach(function (k) {
      var tr = el('tr');
      var c = el('td'); c.appendChild(el('code', null, k));
      tr.append(c, el('td', 'num', fmtNum(counts[k])));
      t.appendChild(tr);
    });
    frag.appendChild(t);
    return frag;
  }

  function showDetail(id) {
    openJob = id;
    var j = latest[id] || {};
    $('detail-title').textContent = 'Summary: ' + (j.filename || id);
    var body = $('detail-body');
    body.textContent = 'Loading…';
    $('detail').hidden = false;
    fetch('/api/jobs/' + encodeURIComponent(id) + '/report')
      .then(function (r) {
        if (r.status === 404) throw new Error('This job no longer exists.');
        if (r.status === 409) throw new Error('The report is not available yet.');
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (s) {
        if (openJob !== id) return;
        body.textContent = '';
        var stats = el('div', 'stats');
        stats.append(
          stat('Total rows', fmtNum(s.total_rows)), stat('Valid rows', fmtNum(s.valid_rows)),
          stat('Invalid rows', fmtNum(s.invalid_rows)), stat('Error lines', fmtNum(s.error_count)),
          stat('Duration', fmtDuration(s.duration_ms)), stat('Rows / sec', fmtNum(Math.round(s.rows_per_sec))));
        if (s.errors_truncated) {
          body.appendChild(el('div', 'notice', 'errors.csv lists the first ' + fmtNum(s.error_lines_written) +
            ' errors. The counts below are complete.'));
        }
        body.append(stats, breakdown('Errors by field', s.errors_by_field), breakdown('Errors by rule', s.errors_by_rule));
      })
      .catch(function (e) {
        if (openJob !== id) return;
        body.textContent = '';
        body.appendChild(el('div', 'notice', e.message));
      });
  }

  function closeDetail() { openJob = null; $('detail').hidden = true; }
  $('detail-close').addEventListener('click', closeDetail);
  $('detail').addEventListener('click', function (e) { if (e.target === $('detail')) closeDetail(); });
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape') closeDetail(); });

  // ---------- upload ----------

  var queue = [];
  var uploading = false;

  function enqueue(files) {
    Array.prototype.forEach.call(files, function (f) {
      var li = el('li');
      var row = el('div', 'row');
      var name = el('span', 'name', f.name);
      var status = el('span', null, '');
      row.append(name, status);
      var bar = el('div', 'bar');
      var fill = el('div', 'valid');
      fill.style.width = '0';
      bar.appendChild(fill);
      li.append(row, bar);
      $('uploads').prepend(li);
      if (!/\.csv$/i.test(f.name)) {
        status.textContent = 'Only .csv files are accepted';
        status.className = 'err';
        bar.hidden = true;
        return;
      }
      queue.push({ file: f, li: li, status: status, bar: bar, fill: fill });
    });
    next();
  }

  function next() {
    if (uploading || !queue.length) return;
    uploading = true;
    var item = queue.shift();
    var fd = new FormData();
    fd.append('file', item.file, item.file.name);
    var xhr = new XMLHttpRequest();
    xhr.open('POST', '/api/jobs');
    xhr.upload.onprogress = function (e) {
      if (!e.lengthComputable) return;
      var pct = Math.round(e.loaded / e.total * 100);
      item.fill.style.width = pct + '%';
      item.status.textContent = pct + '%';
    };
    function fail(msg) {
      item.status.textContent = msg;
      item.status.className = 'err';
      item.bar.hidden = true;
    }
    xhr.onload = function () {
      if (xhr.status >= 200 && xhr.status < 300) {
        item.fill.style.width = '100%';
        item.status.textContent = 'Uploaded, queued for processing';
        item.status.className = 'done';
        poll();
      } else {
        var msg = 'Upload failed (HTTP ' + xhr.status + ')';
        try { var body = JSON.parse(xhr.responseText); if (body && body.error) msg = body.error; } catch (_) { /* keep default */ }
        fail(msg);
      }
    };
    xhr.onerror = function () { fail('Network error'); };
    xhr.onloadend = function () { uploading = false; next(); };
    item.status.textContent = '0%';
    xhr.send(fd);
  }

  var drop = $('drop'), input = $('file');
  drop.addEventListener('click', function () { input.click(); });
  drop.addEventListener('keydown', function (e) {
    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); input.click(); }
  });
  input.addEventListener('change', function () { enqueue(input.files); input.value = ''; });
  ['dragenter', 'dragover'].forEach(function (t) {
    drop.addEventListener(t, function (e) { e.preventDefault(); drop.classList.add('over'); });
  });
  ['dragleave', 'drop'].forEach(function (t) {
    drop.addEventListener(t, function (e) { e.preventDefault(); drop.classList.remove('over'); });
  });
  drop.addEventListener('drop', function (e) { enqueue(e.dataTransfer.files); });
  // Stop the browser from navigating to a file dropped outside the zone.
  window.addEventListener('dragover', function (e) { e.preventDefault(); });
  window.addEventListener('drop', function (e) { e.preventDefault(); });

  poll();
})();
