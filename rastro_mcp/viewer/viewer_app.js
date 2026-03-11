(function () {
  "use strict";

  // Viewer bundle and state
  const TABLE_META_FIELDS = new Set(["source_explanations", "source_metadata", "_source_urls", "sources", "web_sources", "variants"]);

  let bundle = null;
  let fieldStats = {};

  const state = {
    search: "",
    scope: "records",
    filters: {},
    selectedRecordId: null,
    drawerOpen: false,
    galleryIndex: 0,
    previewIndexes: {},
    filterPopover: null,
  };

  async function loadBundle() {
    const bundlePath = document.body.dataset.bundlePath || "./bundle.json";
    const response = await fetch(bundlePath, { cache: "no-store" });
    if (!response.ok) {
      throw new Error("Bundle request failed with status " + response.status);
    }

    bundle = await response.json();
    fieldStats = (bundle.field_analytics && bundle.field_analytics.field_stats) || {};

    state.search = "";
    state.scope = bundle.mode === "catalog" && (bundle.variant_records || []).length > 0 ? "products" : "records";
    state.filters = {};
    state.selectedRecordId = null;
    state.drawerOpen = false;
    state.galleryIndex = 0;
    state.previewIndexes = {};
    state.filterPopover = null;
  }

  function renderLoadError(error) {
    console.error("Failed to load local viewer bundle", error);
    const tableMetaLeft = document.getElementById("table-meta-left");
    const tableMetaRight = document.getElementById("table-meta-right");
    const tableMount = document.getElementById("table-mount");
    if (tableMetaLeft) tableMetaLeft.textContent = "Load failed";
    if (tableMetaRight) tableMetaRight.textContent = "";
    if (tableMount) {
      tableMount.innerHTML =
        '<div class="empty-state"><strong>Viewer failed to load.</strong><br />' +
        escapeHtml(error && error.message ? error.message : "Unknown error") +
        "</div>";
    }
  }

  // Generic formatting helpers
  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function titleCase(value) {
    return String(value || "")
      .replace(/[_\.]+/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .replace(/\b\w/g, (match) => match.toUpperCase());
  }

  function isUrl(value) {
    return typeof value === "string" && /^https?:\/\//i.test(value);
  }

  function isImageUrl(value) {
    if (!isUrl(value)) return false;
    return /\.(jpg|jpeg|png|gif|webp|svg|bmp|ico|avif|tif|tiff)(\?|#|$)/i.test(value) || /image|img|photo|picture|thumbnail|media/i.test(value);
  }

  function proxyAssetUrl(value) {
    if (!isUrl(value)) return value;
    if (!/^https?:$/i.test(window.location.protocol)) return value;
    return window.location.origin + "/proxy?url=" + encodeURIComponent(value);
  }

  function isEmptyValue(value) {
    return value === null || value === undefined || value === "" || (Array.isArray(value) && value.length === 0) || (typeof value === "object" && !Array.isArray(value) && Object.keys(value).length === 0);
  }

  function plainText(value) {
    if (value === null || value === undefined) return "";
    if (Array.isArray(value)) return value.map((entry) => (typeof entry === "object" ? JSON.stringify(entry) : String(entry))).join(", ");
    if (typeof value === "object") return JSON.stringify(value);
    return String(value);
  }

  function summarizeUrl(value) {
    try {
      const url = new URL(value);
      const path = (url.pathname || "/").replace(/\/+/g, "/");
      const compact = url.hostname + (path.length > 42 ? path.slice(0, 39) + "..." : path);
      return escapeHtml(compact + (url.search ? " ..." : ""));
    } catch {
      return escapeHtml(value.length > 72 ? value.slice(0, 69) + "..." : value);
    }
  }

  function progressClass(rate) {
    if (rate >= 0.8) return "high";
    if (rate >= 0.5) return "mid";
    return "";
  }

  // Bundle-aware helpers
  function getRecordsForScope() {
    if (!bundle) return [];
    if (bundle.mode === "catalog" && state.scope === "variants") {
      return bundle.variant_records || [];
    }
    return bundle.records || [];
  }

  function getPayload(record) {
    if (!bundle) return {};
    return bundle.mode === "activity" ? record.after_data || {} : record.data || {};
  }

  function getStickyColumns() {
    if (bundle && bundle.mode === "catalog" && state.scope === "products") {
      return [{ key: "__variant_count", label: "Variants" }];
    }
    return [];
  }

  function getValue(record, column) {
    if (column === "__entity_type") return record.entity_type || "";
    if (column === "__variant_count") return record.variant_count || 0;
    return getPayload(record)[column];
  }

  function columnPriority(name) {
    const value = String(name || "").toLowerCase();
    if (/pdf|document|manual|spec|datasheet|brochure|sheet/.test(value)) return 0;
    if (/image|photo|thumbnail|gallery|swatch|media/.test(value)) return 1;
    if (/url|link|website|source/.test(value)) return 2;
    return 10;
  }

  function getColumns(records) {
    if (!bundle) return [];
    const schemaOrder = (bundle.schema && bundle.schema.field_order) || [];
    const fieldStatNames = Object.keys(fieldStats || {});
    const columns = [];
    const seen = new Set();

    function push(name) {
      if (!name || seen.has(name) || TABLE_META_FIELDS.has(name)) return;
      seen.add(name);
      columns.push(name);
    }

    schemaOrder.forEach(push);
    fieldStatNames.forEach(push);
    records.forEach((record) => {
      Object.keys(getPayload(record)).forEach(push);
    });

    return columns
      .map((name, index) => ({ name, index }))
      .sort((left, right) => columnPriority(left.name) - columnPriority(right.name) || left.index - right.index)
      .map((entry) => entry.name);
  }

  function getFieldMeta(column) {
    return (bundle && bundle.schema && bundle.schema.fields && bundle.schema.fields[column]) || {};
  }

  function distinctValues(records, column) {
    const counts = new Map();
    records.forEach((record) => {
      const value = getValue(record, column);
      if (isEmptyValue(value)) return;
      const normalized = plainText(value);
      counts.set(normalized, (counts.get(normalized) || 0) + 1);
    });

    return Array.from(counts.entries())
      .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
      .map(([value, count]) => ({ value, count }));
  }

  // Field rendering helpers
  function previewValue(value) {
    if (isEmptyValue(value)) return '<span class="muted">Empty</span>';
    if (isImageUrl(value)) return '<div class="thumb"><img class="thumb-image" alt="" src="' + escapeHtml(proxyAssetUrl(value)) + '" loading="lazy" /></div>';
    if (Array.isArray(value)) {
      if (value.every((entry) => typeof entry === "string")) {
        const stringValues = value.filter((entry) => !!entry);
        const imageValues = stringValues.filter((entry) => isImageUrl(entry));
        if (imageValues.length) {
          return (
            '<div class="preview-cell"><div class="thumb"><img class="thumb-image" alt="" src="' +
            escapeHtml(proxyAssetUrl(imageValues[0])) +
            '" loading="lazy" /></div><div class="thumb-count">' +
            imageValues.length +
            (imageValues.length === 1 ? " image" : " images") +
            "</div></div>"
          );
        }
        if (stringValues.every((entry) => isUrl(entry))) {
          return '<span class="json-pill">' + stringValues.length + (stringValues.length === 1 ? " link" : " links") + "</span>";
        }
        return '<div class="cell-value truncate">' + escapeHtml(value.join(", ")) + "</div>";
      }
      return '<span class="json-pill">' + value.length + " items</span>";
    }
    if (typeof value === "object") {
      return '<span class="json-pill">' + Object.keys(value).length + " keys</span>";
    }
    const text = String(value);
    return '<div class="cell-value truncate" title="' + escapeHtml(text) + '">' + escapeHtml(text) + "</div>";
  }

  function needsFieldExpansion(value) {
    if (typeof value === "string") return value.length > 180 || value.includes("\n");
    if (Array.isArray(value)) {
      if (value.length > 3) return true;
      return value.some((entry) => typeof entry === "object" || String(entry || "").length > 90);
    }
    if (typeof value === "object" && value !== null) return true;
    return false;
  }

  function renderExpandedFieldValue(value) {
    if (isEmptyValue(value)) return '<span class="muted">Empty</span>';
    if (Array.isArray(value)) {
      if (value.every((entry) => typeof entry === "string" && isUrl(entry))) {
        return '<div class="field-links">' + value.map((entry) => '<a href="' + escapeHtml(proxyAssetUrl(entry)) + '" target="_blank" rel="noreferrer">' + escapeHtml(entry) + "</a>").join("") + "</div>";
      }
      return '<pre class="field-json">' + escapeHtml(JSON.stringify(value, null, 2)) + "</pre>";
    }
    if (typeof value === "object") return '<pre class="field-json">' + escapeHtml(JSON.stringify(value, null, 2)) + "</pre>";
    if (isUrl(value)) return '<a href="' + escapeHtml(proxyAssetUrl(value)) + '" target="_blank" rel="noreferrer">' + escapeHtml(value) + "</a>";
    return '<div class="field-preview">' + escapeHtml(String(value)) + "</div>";
  }

  function renderCompactFieldValue(value) {
    if (isEmptyValue(value)) return '<span class="muted">Empty</span>';
    if (typeof value === "boolean") return value ? "Yes" : "No";
    if (Array.isArray(value)) {
      if (value.every((entry) => typeof entry === "string" && isUrl(entry))) {
        const label = value.length === 1 ? summarizeUrl(value[0]) : escapeHtml(value.length + " links");
        return '<a href="' + escapeHtml(proxyAssetUrl(value[0])) + '" target="_blank" rel="noreferrer">' + label + "</a>";
      }
      if (value.every((entry) => typeof entry !== "object")) {
        const preview = value.slice(0, 3).map((entry) => String(entry)).join(", ");
        return escapeHtml(preview + (value.length > 3 ? " ..." : ""));
      }
      return escapeHtml(value.length + " items");
    }
    if (typeof value === "object") return escapeHtml(Object.keys(value).length + " keys");
    if (isUrl(value)) return '<a href="' + escapeHtml(proxyAssetUrl(value)) + '" target="_blank" rel="noreferrer">' + summarizeUrl(value) + "</a>";
    return escapeHtml(String(value));
  }

  function renderFieldCard(name, value) {
    const label = escapeHtml(getFieldMeta(name).label || titleCase(name));
    const compact = renderCompactFieldValue(value);
    const expandable = needsFieldExpansion(value);
    const expanded = expandable
      ? '<details class="field-expand"><summary>Show full value</summary><div class="field-expanded">' + renderExpandedFieldValue(value) + "</div></details>"
      : "";
    return '<div class="field-card"><label>' + label + '</label><div class="field-preview compact">' + compact + "</div>" + expanded + "</div>";
  }

  // Filtering and selection
  function matchesSearch(record) {
    if (!state.search) return true;
    const haystack = [record.title, record.identifier, record.review_reasoning, JSON.stringify(record.flags || []), JSON.stringify(getPayload(record))]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(state.search.toLowerCase());
  }

  function matchesHeaderFilters(record) {
    return Object.entries(state.filters).every(([column, filter]) => {
      const value = getValue(record, column);
      const text = plainText(value).toLowerCase();
      if (!filter || filter.mode === "any") return true;
      if (filter.mode === "filled") return !isEmptyValue(value);
      if (filter.mode === "empty") return isEmptyValue(value);
      if (filter.mode === "contains") return text.includes(String(filter.value || "").toLowerCase());
      if (filter.mode === "equals") return text === String(filter.value || "").toLowerCase();
      return true;
    });
  }

  function visibleRecords() {
    return getRecordsForScope().filter(matchesSearch).filter(matchesHeaderFilters);
  }

  function activeFilters() {
    const items = [];
    if (state.search) items.push({ key: "search", label: "Search", value: state.search });
    Object.entries(state.filters).forEach(([column, filter]) => {
      if (!filter || filter.mode === "any") return;
      items.push({
        key: "column:" + column,
        label: titleCase(column.replace(/^__/, "")),
        value: filter.mode === "contains" || filter.mode === "equals" ? String(filter.value || "") : filter.mode,
      });
    });
    return items;
  }

  function hasActiveFilter(column) {
    const filter = state.filters[column];
    return !!(filter && filter.mode && filter.mode !== "any");
  }

  function ensureSelection(records) {
    if (!records.length) {
      state.selectedRecordId = null;
      state.drawerOpen = false;
      return;
    }
    if (!state.selectedRecordId || !records.some((record) => record.id === state.selectedRecordId)) {
      state.selectedRecordId = records[0].id;
      state.galleryIndex = 0;
    }
  }

  function selectedRecord() {
    return visibleRecords().find((record) => record.id === state.selectedRecordId) || null;
  }

  function clearFilter(key) {
    if (key === "search") state.search = "";
    else if (key.startsWith("column:")) delete state.filters[key.slice(7)];
  }

  // Rendering
  function filterIconSvg() {
    return (
      '<svg viewBox="0 0 20 20" fill="none" aria-hidden="true">' +
      '<path d="M3 5h14l-5.5 6.2v4.3l-3 1.5v-5.8L3 5Z" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"/>' +
      "</svg>"
    );
  }

  function renderToolbar() {
    const scopeButtons = [];
    if (bundle.mode === "catalog" && (bundle.variant_records || []).length > 0) {
      scopeButtons.push({ key: "products", label: "Products" });
      scopeButtons.push({ key: "variants", label: "Variants" });
    }

    document.getElementById("scope-row").innerHTML = scopeButtons
      .map((button) => '<button class="scope-button ' + (state.scope === button.key ? "active" : "") + '" type="button" data-scope="' + button.key + '">' + button.label + "</button>")
      .join("");

    document.getElementById("active-filters").innerHTML = activeFilters()
      .map((entry) => '<button type="button" class="filter-chip" data-clear-filter="' + escapeHtml(entry.key) + '"><strong>' + escapeHtml(entry.label) + "</strong>" + escapeHtml(entry.value) + " ×</button>")
      .join("");
  }

  function renderHeader(column) {
    const meta = getFieldMeta(column);
    const stats = fieldStats[column];
    const pct = stats ? Math.round(Number(stats.completion_rate || 0) * 100) : null;
    const filterActive = hasActiveFilter(column);
    return (
      '<th class="dynamic-column"><div class="header-cell">' +
      '<div class="header-top"><div class="header-label" title="' +
      escapeHtml((meta.label || titleCase(column)) + (meta.description ? " — " + meta.description : "")) +
      '"><span>' +
      escapeHtml(meta.label || titleCase(column)) +
      '</span></div><div class="header-actions"><button class="' +
      (filterActive ? "active" : "") +
      '" type="button" data-filter-column="' +
      escapeHtml(column) +
      '" title="' +
      (filterActive ? "Filter active" : "Filter column") +
      '">' +
      filterIconSvg() +
      "</button></div></div>" +
      '<div class="header-meta"><span>' +
      escapeHtml(meta.type || "") +
      '</span>' +
      (pct != null ? "<span>" + pct + "%</span>" : "<span></span>") +
      "</div>" +
      (pct != null ? '<div class="progress ' + progressClass(Number(stats.completion_rate || 0)) + '"><span style="width:' + pct + '%"></span></div>' : "") +
      "</div></th>"
    );
  }

  function renderStickyHeader(column, label) {
    const filterActive = hasActiveFilter(column);
    return (
      '<th class="sticky-meta ' +
      (column === getStickyColumns()[0].key ? "meta-a" : "meta-b") +
      '"><div class="header-cell"><div class="header-top"><div class="header-label"><span>' +
      escapeHtml(label) +
      '</span></div><div class="header-actions"><button class="' +
      (filterActive ? "active" : "") +
      '" type="button" data-filter-column="' +
      escapeHtml(column) +
      '" title="' +
      (filterActive ? "Filter active" : "Filter column") +
      '">' +
      filterIconSvg() +
      "</button></div></div></div></th>"
    );
  }

  function renderTable() {
    const loadedRecords = getRecordsForScope();
    const records = visibleRecords();
    ensureSelection(records);
    const columns = getColumns(loadedRecords);

    document.getElementById("table-meta-left").textContent = records.length + " rows";
    document.getElementById("table-meta-right").textContent = columns.length + " fields";

    if (!records.length) {
      document.getElementById("table-mount").innerHTML = '<div class="empty-state"><strong>No rows match the current filters.</strong><br />Clear a few filters or broaden the search.</div>';
      return;
    }

    const stickyHeaders = getStickyColumns();
    const headerHtml =
      "<tr>" +
      '<th class="sticky-preview">Open</th>' +
      '<th class="sticky-record">Record</th>' +
      stickyHeaders.map((column) => renderStickyHeader(column.key, column.label)).join("") +
      columns.map((column) => renderHeader(column)).join("") +
      "</tr>";

    const rowHtml = records
      .map((record) => {
        const selected = state.selectedRecordId === record.id ? " selected" : "";
        const previewImages = record.images || [];
        const previewIndex = Math.max(0, Math.min(previewImages.length - 1, Number(state.previewIndexes[record.id] || 0)));
        const primaryImage = previewImages[previewIndex];
        return (
          '<tr class="' + selected + '" data-row-id="' + escapeHtml(record.id) + '">' +
          '<td class="sticky-preview"><div class="preview-cell"><button class="row-open" type="button" data-open-row="' + escapeHtml(record.id) + '" title="Open details">↗</button>' +
          (primaryImage
            ? '<button class="thumb thumb-button" type="button" data-cycle-preview="' + escapeHtml(record.id) + '" title="' + (previewImages.length > 1 ? "Cycle row images" : "Preview image") + '"><img class="thumb-image" alt="" src="' + escapeHtml(proxyAssetUrl(primaryImage)) + '" loading="lazy" /></button>'
            : '<div class="thumb"><span class="thumb-empty">No img</span></div>') +
          (previewImages.length > 1 ? '<div class="thumb-count">' + (previewIndex + 1) + " / " + previewImages.length + "</div>" : "") +
          "</div></td>" +
          '<td class="sticky-record"><div class="record-cell"><div><h3 class="record-title">' +
          escapeHtml(record.title || "Untitled record") +
          '</h3><div class="record-id">' +
          escapeHtml(record.identifier || record.id) +
          "</div></div></div></td>" +
          stickyHeaders
            .map((column, index) => '<td class="sticky-meta ' + (index === 0 ? "meta-a" : "meta-b") + '">' + previewValue(getValue(record, column.key)) + "</td>")
            .join("") +
          columns.map((column) => '<td class="dynamic-column">' + previewValue(getValue(record, column)) + "</td>").join("") +
          "</tr>"
        );
      })
      .join("");

    document.getElementById("table-mount").innerHTML = "<table><thead>" + headerHtml + "</thead><tbody>" + rowHtml + "</tbody></table>";
  }

  function renderDrawer() {
    const record = selectedRecord();
    const drawer = document.getElementById("drawer");
    const backdrop = document.getElementById("drawer-backdrop");

    if (!state.drawerOpen || !record) {
      drawer.classList.remove("open");
      backdrop.classList.remove("open");
      drawer.innerHTML = "";
      return;
    }

    const records = visibleRecords();
    const index = records.findIndex((entry) => entry.id === record.id);
    const payload = getPayload(record);
    const images = record.images || [];
    const mainImage = images[Math.max(0, Math.min(state.galleryIndex, images.length - 1))];
    const documents = record.documents || [];

    const fieldCards =
      Object.entries(payload)
        .filter(([, value]) => !isEmptyValue(value))
        .map(([name, value]) => renderFieldCard(name, value))
        .join("") || '<div class="note-card">No populated fields.</div>';

    drawer.innerHTML =
      '<div class="drawer-head">' +
      '<div class="drawer-head-top"><div><h2 style="margin:0 0 6px;font-size:30px;line-height:1.1">' +
      escapeHtml(record.title || "Untitled record") +
      '</h2><div class="record-id">' +
      escapeHtml(record.identifier || record.id) +
      '</div></div><button class="drawer-close" type="button" id="drawer-close" aria-label="Close">✕</button></div>' +
      '<div class="drawer-nav-row"><div class="badge-row">' +
      (record.external_url ? '<a class="action" href="' + escapeHtml(record.external_url) + '" target="_blank" rel="noreferrer">Open source</a>' : "") +
      (bundle.activity && bundle.activity.review_url ? '<a class="action" href="' + escapeHtml(bundle.activity.review_url) + '" target="_blank" rel="noreferrer">Open review</a>' : "") +
      '</div><div style="display:flex;gap:8px;align-items:center"><span class="shortcut-hint">↑/↓ rows · ←/→ images · Esc close</span><button class="drawer-nav" type="button" id="drawer-prev" title="Previous row (Up)" ' +
      (index > 0 ? "" : "disabled") +
      '>↑</button><button class="drawer-nav" type="button" id="drawer-next" title="Next row (Down)" ' +
      (index < records.length - 1 ? "" : "disabled") +
      ">↓</button></div></div></div>" +
      '<div class="drawer-body">' +
      (images.length
        ? '<section class="section"><div class="section-head"><h3>Images</h3><p>' + images.length + ' image(s)</p></div><div class="gallery-main">' +
          (mainImage ? '<img class="gallery-image" alt="" src="' + escapeHtml(proxyAssetUrl(mainImage)) + '" />' : "") +
          '</div><div class="gallery-controls"><button class="gallery-nav" type="button" id="gallery-prev" title="Previous image (H)" ' +
          (state.galleryIndex > 0 ? "" : "disabled") +
          '>←</button><div class="gallery-count">' +
          (state.galleryIndex + 1) +
          " / " +
          images.length +
          '</div><button class="gallery-nav" type="button" id="gallery-next" title="Next image (L)" ' +
          (state.galleryIndex < images.length - 1 ? "" : "disabled") +
          '>→</button></div></section>'
        : "") +
      (documents.length
        ? '<section class="section"><div class="section-head"><h3>Documents</h3><p>' + documents.length + ' document(s)</p></div><div class="field-links">' +
          documents.map((url) => '<a href="' + escapeHtml(proxyAssetUrl(url)) + '" target="_blank" rel="noreferrer">' + escapeHtml(url) + "</a>").join("") +
          "</div></section>"
        : "") +
      '<section class="section"><div class="section-head"><h3>Fields</h3><p>' +
      Object.keys(payload).length +
      ' fields</p></div><div class="field-grid">' +
      fieldCards +
      '</div></section><details><summary>Raw JSON</summary><pre>' +
      escapeHtml(JSON.stringify(record, null, 2)) +
      "</pre></details></div>";

    drawer.classList.add("open");
    backdrop.classList.add("open");
  }

  function openFilterPopover(column, anchor) {
    const popover = document.getElementById("filter-popover");
    const rect = anchor.getBoundingClientRect();
    const current = state.filters[column] || { mode: "any", value: "" };
    const values = distinctValues(getRecordsForScope(), column).slice(0, 32);

    state.filterPopover = { column };
    popover.hidden = false;
    popover.style.top = rect.bottom + 8 + "px";
    popover.style.left = Math.min(window.innerWidth - 332, Math.max(12, rect.left - 288 + rect.width)) + "px";
    popover.innerHTML =
      '<div class="popover-head"><strong>' + escapeHtml(titleCase(column.replace(/^__/, ""))) + '</strong><button class="drawer-close" type="button" data-close-popover>✕</button></div>' +
      '<div class="popover-grid"><div class="filter-actions">' +
      [
        { mode: "any", label: "All" },
        { mode: "filled", label: "Filled" },
        { mode: "empty", label: "Empty" },
      ].map((entry) => '<button class="filter-action ' + (current.mode === entry.mode ? "active" : "") + '" type="button" data-filter-mode="' + entry.mode + '">' + entry.label + "</button>").join("") +
      '</div><div><div class="muted" style="margin-bottom:6px">Contains text</div><div style="display:flex;gap:8px"><input class="input" id="filter-contains" type="text" value="' +
      escapeHtml(current.mode === "contains" ? String(current.value || "") : "") +
      '" style="min-width:0;width:100%" placeholder="Contains text..." /></div></div><div><div class="popover-subhead"><div class="muted">Top values</div><button class="action" type="button" id="clear-column-filter">Clear</button></div><div class="value-list">' +
      values
        .map((entry) => '<button class="value-option ' + (current.mode === "equals" && String(current.value || "") === entry.value ? "active" : "") + '" type="button" data-filter-equals="' + escapeHtml(entry.value) + '"><span title="' + escapeHtml(entry.value) + '">' + escapeHtml(entry.value) + '</span><span class="muted">' + entry.count + "</span></button>")
        .join("") +
      '</div></div><div class="popover-foot"><div class="muted">Press Enter to apply text</div><button class="action primary" type="button" id="apply-contains">Apply</button></div></div>';

    const containsInput = document.getElementById("filter-contains");
    if (containsInput) {
      containsInput.focus();
      containsInput.select();
    }
  }

  function render() {
    if (!bundle) return;
    renderToolbar();
    renderTable();
    renderDrawer();
    bindEvents();
  }

  // Navigation and event binding
  function scrollSelectedRowIntoView() {
    const row = document.querySelector('[data-row-id="' + CSS.escape(state.selectedRecordId || "") + '"]');
    if (row) row.scrollIntoView({ block: "nearest", inline: "nearest" });
  }

  function moveSelection(delta, openDrawer) {
    const records = visibleRecords();
    if (!records.length) return;
    let index = records.findIndex((record) => record.id === state.selectedRecordId);
    if (index < 0) index = 0;
    const nextIndex = Math.max(0, Math.min(records.length - 1, index + delta));
    state.selectedRecordId = records[nextIndex].id;
    state.galleryIndex = 0;
    if (openDrawer) state.drawerOpen = true;
    render();
    scrollSelectedRowIntoView();
  }

  function moveGallery(delta) {
    const record = selectedRecord();
    const images = (record && record.images) || [];
    if (images.length <= 1) return;
    state.galleryIndex = Math.max(0, Math.min(images.length - 1, state.galleryIndex + delta));
    renderDrawer();
    bindEvents();
  }

  function bindEvents() {
    const searchInput = document.getElementById("search-input");
    searchInput.value = state.search;
    searchInput.oninput = (event) => {
      state.search = event.target.value || "";
      render();
    };

    document.getElementById("clear-filters").onclick = () => {
      state.search = "";
      state.filters = {};
      render();
    };

    document.querySelectorAll("[data-scope]").forEach((button) => {
      button.onclick = () => {
        state.scope = button.getAttribute("data-scope");
        render();
      };
    });

    document.querySelectorAll("[data-clear-filter]").forEach((button) => {
      button.onclick = () => {
        clearFilter(button.getAttribute("data-clear-filter"));
        render();
      };
    });

    document.querySelectorAll("[data-filter-column]").forEach((button) => {
      button.onclick = (event) => {
        event.stopPropagation();
        openFilterPopover(button.getAttribute("data-filter-column"), event.currentTarget);
        bindEvents();
      };
    });

    document.querySelectorAll("[data-row-id]").forEach((row) => {
      row.onclick = () => {
        state.selectedRecordId = row.getAttribute("data-row-id");
        if (state.drawerOpen) {
          state.galleryIndex = 0;
          render();
        } else {
          renderTable();
          bindEvents();
        }
      };
      row.ondblclick = () => {
        state.selectedRecordId = row.getAttribute("data-row-id");
        state.drawerOpen = true;
        state.galleryIndex = 0;
        render();
      };
    });

    document.querySelectorAll("[data-open-row]").forEach((button) => {
      button.onclick = (event) => {
        event.stopPropagation();
        state.selectedRecordId = button.getAttribute("data-open-row");
        state.drawerOpen = true;
        state.galleryIndex = 0;
        render();
      };
    });

    document.querySelectorAll("[data-cycle-preview]").forEach((button) => {
      button.onclick = (event) => {
        event.stopPropagation();
        const recordId = button.getAttribute("data-cycle-preview");
        const record = getRecordsForScope().find((entry) => entry.id === recordId);
        const images = (record && record.images) || [];
        if (images.length <= 1) return;
        const current = Math.max(0, Math.min(images.length - 1, Number(state.previewIndexes[recordId] || 0)));
        state.previewIndexes[recordId] = (current + 1) % images.length;
        renderTable();
        bindEvents();
      };
    });

    const drawerClose = document.getElementById("drawer-close");
    if (drawerClose) {
      drawerClose.onclick = () => {
        state.drawerOpen = false;
        renderDrawer();
      };
    }

    document.getElementById("drawer-backdrop").onclick = () => {
      state.drawerOpen = false;
      renderDrawer();
    };

    const prev = document.getElementById("drawer-prev");
    const next = document.getElementById("drawer-next");
    if (prev) prev.onclick = () => moveSelection(-1, true);
    if (next) next.onclick = () => moveSelection(1, true);

    const galleryPrev = document.getElementById("gallery-prev");
    const galleryNext = document.getElementById("gallery-next");
    if (galleryPrev) galleryPrev.onclick = () => moveGallery(-1);
    if (galleryNext) galleryNext.onclick = () => moveGallery(1);

    const popover = document.getElementById("filter-popover");
    const closePopover = popover.querySelector("[data-close-popover]");
    if (closePopover) {
      closePopover.onclick = () => {
        state.filterPopover = null;
        popover.hidden = true;
      };
    }

    popover.querySelectorAll("[data-filter-mode]").forEach((button) => {
      button.onclick = () => {
        const column = state.filterPopover.column;
        const mode = button.getAttribute("data-filter-mode");
        if (mode === "any") delete state.filters[column];
        else state.filters[column] = { mode, value: "" };
        state.filterPopover = null;
        render();
      };
    });

    const applyContains = document.getElementById("apply-contains");
    if (applyContains) {
      applyContains.onclick = () => {
        const input = document.getElementById("filter-contains");
        const value = (input.value || "").trim();
        if (!value) {
          delete state.filters[state.filterPopover.column];
        } else {
          state.filters[state.filterPopover.column] = { mode: "contains", value };
        }
        state.filterPopover = null;
        render();
      };

      const input = document.getElementById("filter-contains");
      if (input) {
        input.onkeydown = (event) => {
          if (event.key === "Enter") {
            event.preventDefault();
            applyContains.click();
          }
        };
      }
    }

    const clearColumnFilter = document.getElementById("clear-column-filter");
    if (clearColumnFilter) {
      clearColumnFilter.onclick = () => {
        delete state.filters[state.filterPopover.column];
        state.filterPopover = null;
        render();
      };
    }

    popover.querySelectorAll("[data-filter-equals]").forEach((button) => {
      button.onclick = () => {
        const nextValue = button.getAttribute("data-filter-equals");
        const current = state.filters[state.filterPopover.column];
        if (current && current.mode === "equals" && String(current.value || "") === String(nextValue || "")) {
          delete state.filters[state.filterPopover.column];
        } else {
          state.filters[state.filterPopover.column] = { mode: "equals", value: nextValue };
        }
        state.filterPopover = null;
        render();
      };
    });
  }

  document.addEventListener("mousedown", (event) => {
    if (!state.filterPopover) return;
    const popover = document.getElementById("filter-popover");
    const target = event.target;
    if (popover.contains(target) || (target.closest && target.closest("[data-filter-column]"))) return;
    state.filterPopover = null;
    popover.hidden = true;
  });

  document.addEventListener("keydown", (event) => {
    if (!bundle) return;

    const target = event.target;
    if (target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.tagName === "SELECT" || target.isContentEditable)) {
      if (event.key !== "Escape") return;
    }

    if (event.key === "Escape") {
      if (state.filterPopover) {
        state.filterPopover = null;
        document.getElementById("filter-popover").hidden = true;
        return;
      }
      if (state.drawerOpen) {
        state.drawerOpen = false;
        renderDrawer();
      }
      return;
    }

    if (event.key === "Enter" && !state.drawerOpen) {
      event.preventDefault();
      state.drawerOpen = true;
      render();
      return;
    }

    if (event.key === "j" || event.key === "ArrowDown") {
      event.preventDefault();
      moveSelection(1, state.drawerOpen);
      return;
    }

    if (event.key === "k" || event.key === "ArrowUp") {
      event.preventDefault();
      moveSelection(-1, state.drawerOpen);
      return;
    }

    if ((event.key === "h" || event.key === "ArrowLeft") && state.drawerOpen) {
      event.preventDefault();
      moveGallery(-1);
      return;
    }

    if ((event.key === "l" || event.key === "ArrowRight") && state.drawerOpen) {
      event.preventDefault();
      moveGallery(1);
    }
  });

  async function main() {
    try {
      await loadBundle();
      render();
    } catch (error) {
      renderLoadError(error);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", main, { once: true });
  } else {
    void main();
  }
})();
