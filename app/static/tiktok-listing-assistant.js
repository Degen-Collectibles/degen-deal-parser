(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const base = '/tiktok/products/assistant';
  const config = JSON.parse($('listing-config').textContent);
  const csrf = config.csrf;
  let draft = null, categories = [], presets = [], busy = false, dirty = false;
  const blockedAttempts = new Map();
  const fulfillmentLabels = {sealed: 'Shipped Sealed', rip: 'Live Rip', both: 'Shipped Sealed or Live Rip'};
  function fulfillmentCopy(mode) {
    const sealed = 'Shipped Sealed: You receive the complete product unopened in its original sealed packaging.';
    const rip = 'Live Rip: This product is opened during a Degen Collectibles TikTok livestream. It will not be shipped sealed. Bulk cards: During the stream, our host usually asks whether you would like your bulk cards included.';
    return mode === 'both' ? 'Choose Shipped Sealed or Live Rip when ordering. ' + sealed + ' ' + rip : mode === 'rip' ? rip : sealed;
  }
  function allocationText(f) {
    if ((f.fulfillment_mode || 'sealed') !== 'both') return '';
    const total = Number(f.quantity), sealed = f.sealed_quantity === '' || f.sealed_quantity == null ? Math.ceil(total / 10) : Number(f.sealed_quantity);
    if (f.quantity === '' || !Number.isInteger(total) || total < 0 || total > 999999 || !Number.isInteger(sealed) || sealed < 0 || sealed > total) return 'Enter total stock and a sealed allocation between zero and that total.';
    return `${sealed} Shipped Sealed at $${f.price || '—'} + ${total - sealed} Live Rip at $${f.rip_price || f.price || '—'} = ${total} total. ${total < 2 ? 'Only one option can have stock with fewer than two units.' : ''}`;
  }
  function renderAllocation() {
    const f = fields(), mode = f.fulfillment_mode || 'sealed';
    $('variant-allocation').hidden = mode !== 'both';
    $('allocation-summary').textContent = allocationText(f);
    $('fulfillment-copy').textContent = fulfillmentCopy(mode);
  }
  function notice(text) { $('notice').textContent = text; $('notice').hidden = !text; }
  async function api(path, body, method) {
    const upload = body instanceof FormData;
    const response = await fetch(base + path, {method: method || (body ? 'POST' : 'GET'),
      headers: {'X-CSRF-Token': csrf, ...(!upload && body ? {'Content-Type': 'application/json'} : {})},
      body: body ? (upload ? body : JSON.stringify(body)) : undefined});
    const result = await response.json();
    if (!response.ok) {
      const error = new Error(typeof result.detail === 'string' ? result.detail : result.detail?.message || 'Unable to complete this step. Check the fields and try again.');
      error.code = result.detail?.code;
      throw error;
    }
    return result;
  }
  async function task(fn) {
    if (busy) return;
    busy = true; $('image-recovery').hidden = true; notice('Working…');
    lockState();
    document.querySelectorAll('button').forEach(b => b.disabled = true);
    try { await fn(); notice(''); }
    catch (error) { notice(error.message); }
    finally { busy = false; document.querySelectorAll('button').forEach(b => b.disabled = false); lockState(); }
  }
  const path = suffix => '/drafts/' + draft.id + suffix;
  function fields() {
    const result = {};
    document.querySelectorAll('[data-field]').forEach(e => result[e.dataset.field] = e.type === 'checkbox' ? e.checked : e.value);
    result.attributes = {...(draft?.fields.attributes || {})};
    document.querySelectorAll('[data-attribute]').forEach(e => result.attributes[e.dataset.attribute] = e.value);
    return result;
  }
  function option(select, value, label) {const el = document.createElement('option'); el.value = value; el.textContent = label; select.append(el);}
  function setField(key, value) {
    const el = document.querySelector(`[data-field="${key}"]`);
    if (!el) return;
    if (el.type === 'checkbox') el.checked = value === true;
    else {if (el.tagName === 'SELECT' && value && !Array.from(el.options).some(o => o.value === value)) option(el, value, value); el.value = value || '';}
  }
  function setImage(id, src) {$(id).hidden = !src; if (src) $(id).src = src; else $(id).removeAttribute('src');}
  function hydrate(d) {
    if (draft?.id !== d.id) {$('stock-counts').textContent = ''; $('image-change').value = '';}
    draft = d;
    dirty = false;
    document.querySelectorAll('[data-field]').forEach(e => setField(e.dataset.field, d.fields[e.dataset.field]));
    if (!d.fields.fulfillment_mode) setField('fulfillment_mode', 'sealed');
    $('draft-picker').hidden = true; $('workspace').hidden = false;
    $('save-status').textContent = 'Saved · ' + new Date(d.updated_at).toLocaleTimeString();
    setImage('photo-preview', d.assets.photo); setImage('source-preview', d.assets.source); setImage('designed-preview', d.assets.designed);
    const i = d.identification;
    $('identification').textContent = i ? [i.name, i.language, 'Confidence: ' + i.confidence, ...i.uncertainties].filter(Boolean).join(' · ') : 'Confirm the edition, language and product type before continuing.';
    if (i && !$('search-query').value) { $('search-query').value = i.search_query || i.name; if (i.game) $('search-game').value = i.game; }
    const s = d.selected_product;
    $('source-info').textContent = s ? [s.name, s.source_name].filter(Boolean).join(' · ') : 'No product image selected.';
    if (d.shop_metadata) applyShopFields(d.shop_metadata);
    $('product-languages').replaceChildren();
    (d.language_options || []).forEach(value => option($('product-languages'), value, value));
    setImage('summary-image', d.assets.source);
    $('summary-product').textContent = d.fields.product_name || 'Your product';
    const source = d.defaults?.sources?.shop;
    if (source?.brand_name) {
      const brandOption = Array.from($('brand').options).find(o => o.value === d.fields.brand_id);
      if (brandOption) brandOption.textContent = source.brand_name;
    }
    $('defaults-summary').textContent = [fulfillmentLabels[d.fields.fulfillment_mode || 'sealed'], d.fields.language,
      source ? 'Shop settings from ' + source.title : 'Shop settings need review'].filter(Boolean).join(' · ');
    const market = d.defaults?.sources?.price;
    $('market-reference').replaceChildren();
    if (market) {
      $('market-reference').textContent = `Market reference: $${Number(market.amount).toFixed(2)} · ${market.language} · looked up ${new Date(market.looked_up_at).toLocaleString()}. `;
      const a = document.createElement('a'); a.href = market.url; a.target='_blank'; a.rel='noopener noreferrer'; a.textContent='TCGPlayer'; $('market-reference').append(a);
    } else $('market-reference').textContent = 'No verified market price for this edition. Enter your price in Edit details.';
    $('defaults-warning').textContent = [d.defaults?.warning, d.missing_defaults?.length ? 'Needed: ' + d.missing_defaults.join(', ') + '.' : ''].filter(Boolean).join(' ');
    $('image-history').replaceChildren();
    (d.image_history || []).forEach(entry => {
      const button = document.createElement('button'); button.type='button'; button.className='candidate';
      const img = document.createElement('img'); img.src=d.assets[entry.key]; img.alt='Previous listing image';
      const label = document.createElement('span'); label.textContent='Use this version'; button.append(img,label);
      button.onclick=()=>task(async()=>{hydrate(await api(path('/restore-image'), {version:draft.version,key:entry.key}));});
      $('image-history').append(button);
    });
    $('previous-images').hidden = !(d.image_history || []).length;
    renderCandidates(); renderReview(); renderAllocation(); lockState();
  }
  function lockState() {
    if (!draft) return;
    const locked = draft.status !== 'draft';
    const generating = draft.image_job?.status === 'running';
    document.querySelectorAll('[data-panel] input,[data-panel] select,[data-panel] textarea,[data-panel] button').forEach(e => e.disabled = locked || generating || busy);
    $('save-draft').disabled = locked || generating || busy;
    $('submission-result').hidden = !locked;
    $('stock-transfer').hidden = !config.stockTransfersEnabled || draft.status !== 'submitted' || draft.fields.fulfillment_mode !== 'both';
    const transfer = draft.stock_transfer;
    $('confirm-transfer').hidden = transfer?.status !== 'preview';
    $('confirm-transfer').disabled = busy;
    $('reconcile-transfer').hidden = !['needs_review','in_progress'].includes(transfer?.status);
    $('reconcile-transfer').disabled = busy;
    $('refresh-stock').disabled = busy;
    $('preview-transfer').disabled = busy || ['in_progress','needs_review'].includes(transfer?.status);
    $('transfer-amount').disabled = busy;
    $('transfer-summary').textContent = transfer ? (transfer.status === 'preview' ? `Move ${transfer.amount} units: sealed ${transfer.before.sealed} → ${transfer.after.sealed}; Live Rip ${transfer.before.rip} → ${transfer.after.rip}. Total ${transfer.before.sealed + transfer.before.rip} unchanged. Preview expires after 2 minutes.` : transfer.message || 'Transfer in progress. Do not retry.') : '';
    if (locked) {
      $('result-title').textContent = draft.status === 'submitted' ? 'Sent to TikTok' : 'Check submission status';
      $('result-text').textContent = draft.status === 'submitted' ? `Product ${draft.product_id}. Status: ${draft.tiktok_status || 'awaiting read-back'}. Review: ${draft.audit_status || 'not yet reported'}. Verification: ${draft.verification || 'pending'}.` : (draft.submission_error || 'A submission is in progress. Reload before taking another action.');
      if (draft.verification_issues?.length) $('result-text').textContent += ' Check: ' + draft.verification_issues.join(', ') + '.';
      $('verify-submission').hidden = !draft.product_id;
      $('reconcile-panel').hidden = Boolean(draft.product_id);
    }
  }
  async function save() {const d = await api(path('/save'), {version: draft.version, fields: fields()}); hydrate(d); return d;}
  function step(number) {
    document.querySelectorAll('[data-panel]').forEach(el => el.hidden = Number(el.dataset.panel) !== number);
    document.querySelectorAll('[data-step]').forEach(el => {if (Number(el.dataset.step) === number) el.setAttribute('aria-current', 'step'); else el.removeAttribute('aria-current');});
    if (number === 3) renderReview();
  }
  async function list() {
    const result = await api('/drafts'); $('draft-list').replaceChildren();
    result.drafts.forEach(d => {const b = document.createElement('button'); const title = document.createElement('strong'); title.textContent = d.title; const status = document.createElement('span'); status.textContent = d.status; b.append(title, status); b.onclick = () => task(async () => {hydrate(await api('/drafts/' + d.id)); step(d.status !== 'draft' || draft.assets.designed ? 3 : draft.assets.source ? 2 : 1); if (draft.image_job?.status === 'running') {await waitForDesign(); step(3);}}); $('draft-list').append(b);});
  }
  function renderCandidates() {
    $('candidates').replaceChildren();
    (draft.candidates || []).forEach((p, index) => {const b = document.createElement('button'); b.className = 'candidate';
      const url = new URL(p.image_url || 'https://invalid.example');
      if (url.protocol === 'https:' && ['product-images.tcgplayer.com','tcgplayer-cdn.tcgplayer.com','cdn.tcgtracking.com'].includes(url.hostname)) {const img = document.createElement('img'); img.src = url.href; img.alt = p.name; img.referrerPolicy = 'no-referrer'; b.append(img);}
      const text = document.createElement('span'); text.textContent = [p.name, p.language, p.market_price_source === 'TCGPlayer Market' && p.market_price ? '$' + p.market_price + ' market' : ''].filter(Boolean).join(' · '); b.append(text);
      b.onclick = () => task(async () => {hydrate(await api(path('/select'), {version: draft.version, index})); step(2); notice('Looking up product facts and matching your shop settings…'); await loadDefaults();}); $('candidates').append(b);
    });
    if (draft.search_warning) {const p = document.createElement('p'); p.textContent = draft.search_warning; $('candidates').append(p);}
  }
  function renderReview() {
    const f = draft.fields;
    $('fulfillment-badge').textContent = fulfillmentLabels[f.fulfillment_mode || 'sealed'];
    $('review-fulfillment').textContent = fulfillmentCopy(f.fulfillment_mode || 'sealed');
    $('review-allocation').textContent = allocationText(f);
    $('review-title').textContent = f.title || 'Add a title'; $('review-description').textContent = f.description || '';
    $('review-source').textContent = 'Image source: ' + (draft.selected_product?.source_name || 'not selected');
    const sourceURL = draft.selected_product?.external_url;
    if (sourceURL && sourceURL.startsWith('https://')) {const a = document.createElement('a'); a.href=sourceURL; a.target='_blank'; a.rel='noopener noreferrer'; a.textContent=' · View original product'; $('review-source').append(a);}
    $('review-details').replaceChildren();
    for (const [k,v] of Object.entries({'Price': f.price ? '$' + f.price : 'Missing', 'Quantity': f.quantity || 'Missing', 'Packed weight': (f.weight || '—') + ' lb', 'Packed size': [f.length,f.width,f.height].map(x => x || '—').join(' × ') + ' in', 'Language / edition': f.language || 'Not supplied', 'Category': $('category').selectedOptions[0]?.textContent || f.category_id || 'Missing', 'Warehouse': $('warehouse').selectedOptions[0]?.textContent || f.warehouse_id || 'Missing'})) {const dt = document.createElement('dt'), dd = document.createElement('dd'); dt.textContent = k; dd.textContent = v; $('review-details').append(dt,dd);}
    $('duplicates').hidden = !(draft.duplicates || []).length; $('duplicate-list').replaceChildren();
    (draft.duplicates || []).forEach(d => {const li = document.createElement('li'); li.textContent = `${d.title} (${d.status || 'unknown'}) · ${d.id}`; $('duplicate-list').append(li);});
  }
  function filterCategories() {const current = draft.fields.category_id || ''; const q = $('category-filter').value.toLowerCase(); $('category').replaceChildren(); option($('category'), '', 'Choose category'); categories.filter(c => c.name.toLowerCase().includes(q) || String(c.id) === current).forEach(c => option($('category'), String(c.id), c.name)); setField('category_id', current);}
  function applyShopFields(result) {
    draft.fields.attributes ||= {};
    result.attributes.forEach(a => {if (['language','card language'].includes(String(a.name).toLowerCase())) draft.fields.attributes[a.id] = draft.fields.language || '';});
    categories = result.categories; filterCategories();
    $('warehouse').replaceChildren(); option($('warehouse'), '', 'Choose warehouse'); result.warehouses.forEach(w => option($('warehouse'), String(w.id), w.name)); setField('warehouse_id', draft.fields.warehouse_id);
    $('category-attributes').replaceChildren();
    result.attributes.filter(a => !['language','card language'].includes(String(a.name).toLowerCase())).forEach(a => {const label = document.createElement('label'); label.textContent = a.name + (a.is_required || a.requirement?.is_required ? ' *' : ''); const input = document.createElement('input'); input.dataset.attribute = a.id; input.value = draft.fields.attributes?.[a.id] || ''; input.maxLength = 200; if (a.values?.length) {const list = document.createElement('datalist'); list.id = 'attr-' + a.id; a.values.forEach(v => {const opt = document.createElement('option'); opt.value = v.name; list.append(opt);}); input.setAttribute('list', list.id); label.append(list);} label.append(input); $('category-attributes').append(label);});
    $('shop-warning').textContent = 'Shop settings loaded. You can edit the suggested values here.';
  }
  async function shopFields() {
    const result = await api('/shop-fields?category_id=' + encodeURIComponent(draft.fields.category_id || ''));
    draft.shop_metadata = result;
    applyShopFields(result);
  }
  async function loadDefaults() {
    hydrate(await api(path('/autofill'), {version: draft.version}));
    $('edit-details').open = (draft.missing_defaults || []).some(x => x !== 'Quantity');
  }
  async function upload(input, suffix) {
    if (!input.files[0]) return;
    await save();
    const body = new FormData(); body.append('file', input.files[0]); body.append('version', draft.version);
    if (suffix === '/photo') $('search-query').value = '';
    try {hydrate(await api(path(suffix), body));}
    catch(error) {hydrate(await api(path(''))); throw error;}
    finally {input.value = '';}
  }
  $('new-draft').onclick = () => task(async () => {hydrate(await api('/drafts', {})); $('search-query').value = ''; step(1);});
  $('all-drafts').onclick = () => task(async () => {if (draft.status === 'draft') await save(); await list(); $('workspace').hidden = true; $('draft-picker').hidden = false;});
  $('save-draft').onclick = () => task(save);
  $('photo').onchange = () => task(() => upload($('photo'), '/photo'));
  $('source').onchange = () => task(() => upload($('source'), '/source'));
  $('search-products').onclick = () => task(async () => {await save(); hydrate(await api(path('/search'), {version: draft.version, query: $('search-query').value, game: $('search-game').value || 'Pokemon'}));});
  $('to-details').onclick = () => task(async () => {await save(); if (!draft.assets.source) throw new Error('Choose a product image first.'); step(2); $('edit-details').open = !draft.defaults;});
  $('refresh-defaults').onclick = () => task(async () => {await save(); await loadDefaults();});
  async function waitForDesign() {
    while (draft.image_job?.status === 'running') {
      notice('Generating with GPT Image 2… Your job is saved. You can reopen this draft to check its progress.');
      await new Promise(resolve => setTimeout(resolve, 2000));
      hydrate(await api(path('')));
    }
    if (draft.image_job?.status === 'failed') {
      const error = new Error(draft.image_job.message);
      error.code = draft.image_job.code;
      throw error;
    }
  }
  function generatePreview(revision = '') { return task(async () => {
    await save();
    if (!revision) {
      const f = draft.fields;
      const missing = ['product_name','language','title','description','price','quantity','category_id','warehouse_id','weight','length','width','height'].filter(k => f[k] == null || f[k] === '');
      (draft.shop_metadata?.attributes || []).forEach(a => {if ((a.is_required || a.requirement?.is_required) && !f.attributes?.[a.id]) missing.push(a.name);});
      if (missing.length) {$('edit-details').open = true; step(2); throw new Error('Complete these details before generating: ' + missing.join(', ') + '.');}
      if (!Number.isInteger(Number(f.quantity)) || Number(f.quantity) < 0) throw new Error('Enter a whole-number quantity of zero or more.');
    }
    notice('Generating with GPT Image 2… This can take several minutes if the provider needs one retry.');
    try {
      hydrate(await api(path('/design'), {version: draft.version, revision}));
      await waitForDesign();
      blockedAttempts.delete(draft.id); step(3);
    } catch (error) {
      if (error.code === 'image_blocked') {
        const count = (blockedAttempts.get(draft.id) || 0) + 1;
        blockedAttempts.set(draft.id, count);
        $('image-recovery').hidden = false;
        $('retry-image').hidden = count > 1;
        if (count > 1) error.message += ' It was blocked again. Change the source image or review the product details before generating again.';
      }
      throw error;
    }
  }); }
  $('generate-preview').onclick = () => generatePreview();
  $('regenerate-image').onclick = () => {const revision = $('image-change').value.trim(); if (!revision) {notice('Describe the changes you want first.'); return;} generatePreview(revision);};
  $('use-source-image').onclick = () => task(async () => {await save(); hydrate(await api(path('/use-source-image'), {version:draft.version})); step(3);});
  $('fulfillment-mode').onchange = () => task(async () => {
    const f = fields();
    const title = f.title.replace(/\s*[—–-]\s*(?:Shipped Sealed or Live Rip|Shipped Sealed|Live Rip(?: Only)?)\s*$/i, '');
    setField('title', (title || f.product_name) + ' — ' + fulfillmentLabels[f.fulfillment_mode]);
    // Migrate only the exact boilerplate created by the old assistant.
    if (f.description === 'One ' + f.product_name + '. Supplied unopened.') setField('description', 'One ' + f.product_name + '.');
    setField('review_confirmed', false); dirty = true; renderAllocation();
    await save();
    if (draft.selected_product?.external_id) await loadDefaults();
  });
  document.querySelector('[data-field="language"]').onchange = () => task(async () => {
    const chosen = fields().language;
    if (chosen === draft.fields.language) return;
    await save();
    $('edit-details').open = true;
    throw new Error('Edition changed. Enter its price and upload a matching product photo using Product → upload a clean image. Review the shipping settings for this edition.');
  });
  $('reset-allocation').onclick = () => { setField('sealed_quantity', ''); setField('review_confirmed', false); dirty = true; renderAllocation(); };
  $('retry-image').onclick = () => generatePreview(draft.image_job?.revision || '');
  $('change-image').onclick = () => { $('image-recovery').hidden = true; notice('Review the product image and details before generating again.'); step(1); };
  $('load-shop').onclick = () => task(async () => {await save(); await shopFields();});
  $('find-brand').onclick = () => task(async () => {const result = await api('/brands?q=' + encodeURIComponent($('brand-query').value) + '&category_id=' + encodeURIComponent($('category').value)); $('brand').replaceChildren(); option($('brand'), '', 'No brand selected'); result.brands.forEach(b => option($('brand'), b.id, b.name)); if (!result.brands.length) throw new Error('No matching brand returned by TikTok. Try another name.');});
  $('category-filter').oninput = filterCategories;
  $('category').onchange = () => task(async () => {await save(); await shopFields();});
  document.querySelectorAll('[data-step],[data-back]').forEach(b => b.onclick = () => task(async () => {if (draft.status === 'draft') await save(); step(Number(b.dataset.step || b.dataset.back));}));
  async function submit(mode) {
    const reviewed = fields().review_confirmed;
    ['product_confirmed','image_confirmed','shipping_confirmed'].forEach(k => setField(k,reviewed));
    await save();
    if (!draft.fields.review_confirmed) throw new Error('Review the preview and check its confirmation before submitting.');
    try {hydrate(await api(path('/submit'), {version: draft.version, mode}));}
    catch(error) {hydrate(await api(path(''))); throw error;}
  }
  $('submit-listing').onclick = () => task(() => submit('LISTING'));
  $('tiktok-draft').onclick = () => task(() => submit('AS_DRAFT'));
  $('verify-submission').onclick = () => task(async () => hydrate(await api(path('/verify'), {})));
  $('refresh-stock').onclick = () => task(async () => {const result = await api(path('/stock')); $('stock-counts').textContent = `Available: ${result.stock.variants.sealed.quantity} sealed / ${result.stock.variants.rip.quantity} Live Rip.`;});
  $('preview-transfer').onclick = () => task(async () => {hydrate(await api(path('/stock/preview'), {version:draft.version, amount:Number($('transfer-amount').value)}));});
  $('confirm-transfer').onclick = () => task(async () => {
    try {hydrate(await api(path('/stock/confirm'), {version:draft.version, transfer_id:draft.stock_transfer.id}));}
    catch(error) {hydrate(await api(path(''))); throw error;}
  });
  $('reconcile-transfer').onclick = () => task(async () => {hydrate(await api(path('/stock/reconcile'), {}));});
  $('reconcile-submission').onclick = () => task(async () => hydrate(await api(path('/reconcile'), {product_id: $('reconcile-id').value})));
  async function loadPresets() {presets = (await api('/packaging')).presets; $('packaging-preset').replaceChildren(); option($('packaging-preset'), '', 'Enter measurements'); presets.forEach((p,i) => option($('packaging-preset'), String(i), p.name));}
  $('packaging-preset').onchange = () => {const p = presets[Number($('packaging-preset').value)]; if ($('packaging-preset').value !== '' && p) {['weight','length','width','height'].forEach(k => setField(k,p[k])); setField('shipping_confirmed',false);dirty=true;}};
  $('save-preset').onclick = () => task(async () => {const f = fields(); await api('/packaging', {name:$('preset-name').value, ...Object.fromEntries(['weight','length','width','height'].map(k => [k,f[k]]))}); await loadPresets(); $('preset-name').value = '';});
  document.addEventListener('input', e => {if (e.target.matches('[data-field],[data-attribute]')) {dirty=true; renderAllocation();}});
  window.addEventListener('beforeunload', e => {if (draft?.status === 'draft' && dirty) {e.preventDefault(); e.returnValue = '';}});
  task(async () => {await list(); await loadPresets();});
})();
