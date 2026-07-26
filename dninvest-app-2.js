
// ══════════════════════════════════════════
// ACTIVITY LOG
// ══════════════════════════════════════════
let alViewMode = 'compact'; // 'detail' or 'compact'
function toggleAlView(){
  alViewMode = alViewMode==='detail' ? 'compact' : 'detail';
  const btn = document.getElementById('al-view-toggle');
  if(btn) btn.textContent = alViewMode==='compact' ? '☰ Detailed' : '☰ Compact';
  renderActivityLog();
}

// Formats a log's timestamp for the Activity Log. Edit logs & new call logs carry
// a full ISO timestamp (ts/date with 'T') → date+time. Old call logs only have a
// date (no time) → show date only, so a date never renders as a bogus 05:30 AM.
function alWhen(l, withYear){
  const w = l.ts || l.date;
  if(!w) return '—';
  if(/T\d/.test(w)){
    const o = withYear ? {day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'}
                       : {day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'};
    return new Date(w).toLocaleString('en-IN',o);
  }
  const o = withYear ? {day:'2-digit',month:'short',year:'numeric'} : {day:'2-digit',month:'short'};
  return new Date(w+'T00:00:00').toLocaleDateString('en-IN',o);
}

// ══════════════════════════════════════════════════════════════════
// COLUMN AUTO-FILTER ENGINE (CF)
// Usage: CF.th(tid, col, labelHTML) → <th> string with ▾ filter button
//        CF.applyXxx(data)          → filtered array
//        CF.reset(tid)              → clear all filters
const CF = (function(){
  const _filters = {}; // { tid: { col: Set<string> } }
  let _openDd = null;  // currently open dropdown DOM element

  function _init(tid){ if(!_filters[tid]) _filters[tid]={}; }
  function hasActive(tid,col){ const f=(_filters[tid]||{})[col]; return !!(f&&f.size>0); }

  // Close any open dropdown
  function _closeAll(){
    if(_openDd){ _openDd.remove(); _openDd=null; }
  }

  // Open filter dropdown for a column
  function _openFromBtn(btnEl, tid, col){
    // Toggle: click same btn again → close
    if(_openDd && _openDd.dataset.tid===tid && _openDd.dataset.col===col){
      _closeAll(); return;
    }
    _closeAll();
    _init(tid);

    // Gather all raw values from FULL (unfiltered) dataset
    const allData = _getData(tid);
    const rawVals = allData.map(r=>String(_getVal(tid,col,r)||''));
    const uniqueVals = [...new Set(rawVals)].sort((a,b)=>{
      if(a===''&&b!=='') return 1; if(b===''&&a!=='') return -1;
      return a.localeCompare(b,undefined,{numeric:true,sensitivity:'base'});
    });

    const current = _filters[tid][col] || new Set();

    // Build dropdown
    const dd = document.createElement('div');
    dd.className = 'col-filter-dd';
    dd.dataset.tid = tid;
    dd.dataset.col = col;

    // Search box
    const srch = document.createElement('input');
    srch.className = 'cf-search';
    srch.placeholder = 'Search...';
    srch.addEventListener('input', function(){
      const q = this.value.toLowerCase();
      dd.querySelectorAll('.cf-item:not([data-val="__ALL__"])').forEach(item=>{
        item.style.display = (item.dataset.label||'').toLowerCase().includes(q) ? '' : 'none';
      });
    });
    dd.appendChild(srch);

    // List
    const list = document.createElement('div');
    list.className = 'cf-list';

    // "Select All" item
    const allChecked = current.size===0;
    const allItem = _mkItem('__ALL__', '(Select All)', allChecked);
    list.appendChild(allItem);

    uniqueVals.forEach(v=>{
      const item = _mkItem(v, v||'(blank)', allChecked || current.has(v));
      list.appendChild(item);
    });

    // Sync on any checkbox change
    list.addEventListener('change', ()=>_syncAll(dd));
    // Click on row (not checkbox) toggles checkbox
    list.querySelectorAll('.cf-item').forEach(item=>{
      item.addEventListener('click', function(e){
        if(e.target.tagName==='INPUT') return;
        const cb = this.querySelector('input[type=checkbox]');
        if(cb){ cb.checked=!cb.checked; cb.dispatchEvent(new Event('change',{bubbles:true})); }
      });
    });

    dd.appendChild(list);

    // Footer buttons
    const footer = document.createElement('div');
    footer.className = 'cf-footer';

    const clearBtn = document.createElement('button');
    clearBtn.className = 'cf-clear';
    clearBtn.textContent = '✕ Clear';
    clearBtn.addEventListener('click', function(e){
      e.stopPropagation();
      _init(tid);
      delete _filters[tid][col];
      _closeAll();
      _rerender(tid);
    });

    const applyBtn = document.createElement('button');
    applyBtn.className = 'cf-apply';
    applyBtn.textContent = '✔ Apply';
    applyBtn.addEventListener('click', function(e){
      e.stopPropagation();
      _init(tid);
      const allCb = dd.querySelector('[data-val="__ALL__"] input[type=checkbox]');
      if(allCb && allCb.checked){
        delete _filters[tid][col];
      } else {
        const sel = new Set(
          [...dd.querySelectorAll('.cf-item:not([data-val="__ALL__"])')].filter(i=>{
            const cb=i.querySelector('input[type=checkbox]'); return cb&&cb.checked;
          }).map(i=>i.dataset.val)
        );
        if(sel.size===0) delete _filters[tid][col];
        else _filters[tid][col] = sel;
      }
      _closeAll();
      _rerender(tid);
    });

    footer.appendChild(clearBtn);
    footer.appendChild(applyBtn);
    dd.appendChild(footer);

    // Position: attach to document.body, place near button
    document.body.appendChild(dd);
    _openDd = dd;

    const rect = btnEl.getBoundingClientRect();
    dd.style.position = 'fixed';
    dd.style.zIndex = '9999';
    dd.style.top = (rect.bottom + window.scrollY + 2) + 'px';
    // Check if would overflow right edge
    const ddW = 200;
    let left = rect.left + window.scrollX;
    if(left + ddW > window.innerWidth) left = window.innerWidth - ddW - 8;
    dd.style.left = left + 'px';

    // Close on outside click
    setTimeout(()=>{
      function outsideClick(e){
        if(!dd.contains(e.target) && e.target!==btnEl){
          _closeAll();
          document.removeEventListener('click', outsideClick);
        }
      }
      document.addEventListener('click', outsideClick);
    }, 10);
  }

  function _mkItem(val, label, checked){
    const div = document.createElement('div');
    div.className = 'cf-item' + (checked?' selected':'');
    div.dataset.val = val;
    div.dataset.label = label;
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = checked;
    const sp = document.createElement('span');
    sp.textContent = label;
    div.appendChild(cb);
    div.appendChild(sp);
    return div;
  }

  function _syncAll(dd){
    const allItem = dd.querySelector('[data-val="__ALL__"]');
    const items = [...dd.querySelectorAll('.cf-item:not([data-val="__ALL__"])')].filter(i=>i.style.display!=='none');
    const allChecked = items.every(i=>i.querySelector('input[type=checkbox]').checked);
    if(allItem){
      const cb=allItem.querySelector('input[type=checkbox]');
      if(cb) cb.checked=allChecked;
      allItem.classList.toggle('selected',allChecked);
    }
    items.forEach(i=>i.classList.toggle('selected',i.querySelector('input[type=checkbox]').checked));
  }

  // Build <th> HTML string - labelHTML can contain sort spans
  function th(tid, col, labelHTML){
    const active = hasActive(tid,col);
    const dot = active?'<span class="col-filter-active-dot"></span>':'';
    return `<th style="white-space:nowrap">
      ${labelHTML}${dot}
      <button class="col-filter-btn${active?' active':''}" onclick="event.stopPropagation();CF._openFromBtn(this,'${tid}','${col}')" title="Filter">▾</button>
    </th>`;
  }

  function _rerender(tid){
    if(tid==='eq') renderEqTable();
    else if(tid==='mf') renderMfTable();
    else if(tid==='leads'){ leadsPage=1; renderLeadsTable(); }
    else if(tid==='mftxn') renderMfTxnTable();
    else if(tid==='demat') renderEqDematTable();
    else if(tid==='sip') renderSip();
  }

  function _getData(tid){
    if(tid==='eq') return getMyEqClients();
    if(tid==='mf') return getMyMfClients();
    if(tid==='leads') return Object.values(DB.get('leads')||{});
    if(tid==='mftxn') return getMfBizEntries();
    if(tid==='demat') return getEqDematEntries();
    if(tid==='sip') return (DB.get('sip_data')||[]);
    return [];
  }

  function _getVal(tid,col,r){
    const map = {
      eq:  {code:r.code,name:r.name,mobile:r.mobile,rm:r.rm,status:r.status,followup_status:r.followup_status},
      mf:  {name:r.name,mobile:r.mobile,pan:r.pan,rm:r.rm,status:r.status,followup_status:r.followup_status},
      leads:{name:r.name,mobile:r.mobile,source:r.source,segment:r.segment,rm:r.rm,status:r.status,followup_status:r.followup_status},
      mftxn:{rm:r.rm,type:r.type,fund_name:r.fund_name,status:r.status||'Pending'},
      demat:{rm:r.rm,status:r.status||'Pending'},
      sip:  {rm:r.rm,fund_name:r.fund_name,sip_type:r.sip_type,status:r.status}
    };
    return (map[tid]||{})[col];
  }

  function _applyFilter(tid,data){
    const f = _filters[tid]||{};
    return data.filter(r=>{
      for(const [col,sel] of Object.entries(f)){
        if(!sel||sel.size===0) continue;
        if(!sel.has(String(_getVal(tid,col,r)||''))) return false;
      }
      return true;
    });
  }

  function reset(tid){ _filters[tid]={}; _rerender(tid); }

  return {
    th, _openFromBtn, reset, hasActive,
    applyEq:  d=>_applyFilter('eq',d),
    applyMf:  d=>_applyFilter('mf',d),
    applyLeads:d=>_applyFilter('leads',d),
    applyMftxn:d=>_applyFilter('mftxn',d),
    applyDemat:d=>_applyFilter('demat',d),
    applySip:  d=>_applyFilter('sip',d),
  };
})();

// Short colored segment badge for Activity Log (Equity / MF / Lead / Other Products etc.)
function alSegInfo(seg){
  const s=String(seg||'').trim().toLowerCase();
  if(s==='equity'||s==='eq')   return {label:'EQ',   full:'Equity',        bg:'#dbeafe', color:'#1e40af'};
  if(s==='mf')                 return {label:'MF',   full:'Mutual Fund',   bg:'#d1fae5', color:'#065f46'};
  if(s==='lead'||s==='leads')  return {label:'LEAD', full:'Lead',          bg:'#ede9fe', color:'#6d28d9'};
  if(s==='mftxn')              return {label:'MF-TXN',full:'MF Transaction',bg:'#ccfbf1', color:'#0f766e'};
  if(s==='demat')              return {label:'DEMAT',full:'Demat',         bg:'#fee2e2', color:'#991b1b'};
  if(s==='sip')                return {label:'SIP',  full:'SIP',           bg:'#cffafe', color:'#155e75'};
  if(s==='other'||s==='op'||s==='other_products') return {label:'OP', full:'Other Products', bg:'#fce7f3', color:'#9d174d'};
  if(!s)                       return {label:'—',    full:'—',             bg:'#f1f5f9', color:'#64748b'};
  return {label:s.toUpperCase().slice(0,8), full:seg, bg:'#fef9c3', color:'#854d0e'};
}
function alSegBadge(seg){
  const i=alSegInfo(seg);
  return `<span title="${i.full}" style="background:${i.bg};color:${i.color};padding:1px 6px;border-radius:8px;font-size:.68rem;font-weight:800;letter-spacing:.3px;white-space:nowrap">${i.label}</span>`;
}

function renderActivityLog(){
  const isAdmin = CU && CU.role==='admin';
  const seg = (document.getElementById('al-seg')||{value:''}).value;
  const rmF = (document.getElementById('al-rm')||{value:''}).value;
  const typeF = (document.getElementById('al-type')||{value:''}).value;
  const byF = (document.getElementById('al-by')||{value:''}).value;
  const fromF = (document.getElementById('al-from')||{value:''}).value;
  const toF = (document.getElementById('al-to')||{value:''}).value;
  const search = ((document.getElementById('al-search')||{value:''}).value||'').toLowerCase().trim();

  // Merge call_logs + activity_logs
  // Build client lookup map for enriching old call logs (which may lack rm/client_name)
  const allEqClients = DB.get('eq_clients')||[];
  const allMfClients = DB.get('mf_clients')||[];
  const clientMap = new Map();
  [...allEqClients,...allMfClients].forEach(c=>clientMap.set(c.id, c));

  const callLogs = (DB.get('call_logs')||[]).map(l=>{
    const c = clientMap.get(l.client_id)||{};
    return {
      id: l.id||uid(),
      type: 'call',
      seg: l.seg || (c.pan?'mf':'equity'),
      client_id: l.client_id,
      client_name: l.client_name||l.name||c.name||'—',
      rm: l.rm||c.rm||'—',
      by: l.by||l.rm||c.rm||'—',
      date: l.date||'',
      ts: l.ts||'',
      changes: [],
      call_status: l.status||'',
      next_call: l.next_call||'',
      remarks: l.note||l.remarks||''
    };
  });
  const editLogs = DB.get('activity_logs')||[];
  let all = [...editLogs, ...callLogs].sort((a,b)=>String(b.ts||b.date).localeCompare(String(a.ts||a.date)));

  // Staff only sees their own RM logs
  if(!isAdmin){
    const myRMs = [...new Set([...(CU.eq_dealers||[CU.name]),...(CU.mf_dealers||[CU.name])])].map(d=>d.trim().toUpperCase());
    all = all.filter(l=>myRMs.includes((l.rm||'').trim().toUpperCase()));
  }

  // Populate RM + By dropdowns (rebuild every time)
  const rmEl=document.getElementById('al-rm'), byEl=document.getElementById('al-by');
  if(rmEl){
    const prevRm=rmEl.value;
    rmEl.innerHTML='<option value="">All RMs</option>';
    const rms=[...new Set(all.map(l=>normRm(l.rm)).filter(r=>r&&r!=='—'))].sort();
    rms.forEach(r=>{ const o=document.createElement('option'); o.value=r; o.textContent=r; if(r===prevRm) o.selected=true; rmEl.appendChild(o); });
  }
  if(byEl){
    const prevBy=byEl.value;
    byEl.innerHTML='<option value="">All Users</option>';
    const bys=[...new Set(all.map(l=>l.by).filter(b=>b&&b!=='—'))].sort();
    bys.forEach(b=>{ const o=document.createElement('option'); o.value=b; o.textContent=b; if(b===prevBy) o.selected=true; byEl.appendChild(o); });
  }

  // Apply filters
  if(seg) all=all.filter(l=>alSegInfo(l.seg).label===alSegInfo(seg).label);
  if(rmF) all=all.filter(l=>normRm(l.rm)===rmF);
  if(typeF) all=all.filter(l=>l.type===typeF);
  if(byF) all=all.filter(l=>l.by===byF);
  if(fromF) all=all.filter(l=>l.date&&l.date>=fromF);
  if(toF) all=all.filter(l=>l.date&&l.date<=toF+'T23:59:59');
  if(search) all=all.filter(l=>(l.client_name||'').toLowerCase().includes(search));

  const countEl=document.getElementById('al-count');
  if(countEl) countEl.textContent=`${all.length} records`;

  const tbl=document.getElementById('al-table');
  if(!tbl) return;

  const typeLabel={add:'✅ New Client',edit:'✏️ Edited',call:'📞 Call Log',call_update:'📞 Follow-up Updated',bulk_update:'⇅ Bulk Update',bulk_rm_update:'⇅ Bulk RM Update'};
  const typeBg={add:'#d1fae5',edit:'#fef3c7',call:'#e0f2fe',call_update:'#f0e6ff'};
  const typeColor={add:'#065f46',edit:'#92400e',call:'#0369a1',call_update:'#6d28d9'};

  if(!all.length){
    tbl.innerHTML=`<tr><td colspan="7" style="text-align:center;padding:32px;color:var(--gray)">No logs found. Reset filters.</td></tr>`;
    return;
  }

  // ── COMPACT VIEW ──
  if(alViewMode==='compact'){
    let rows='<thead><tr><th>Date & Time</th><th>Type</th><th>Client</th><th>Seg</th><th>RM</th><th>By</th><th>Summary</th></tr></thead><tbody>';
    all.forEach(l=>{
      const dt = alWhen(l,false);
      const typeBadge=`<span style="background:${typeBg[l.type]||'#f1f5f9'};color:${typeColor[l.type]||'#475569'};padding:1px 6px;border-radius:8px;font-size:.72rem;font-weight:700">${typeLabel[l.type]||l.type}</span>`;
      let summary='';
      if(l.type==='call') summary=`${l.call_status||''}${l.next_call?' → '+fmtDate(l.next_call):''}${l.remarks?' | '+l.remarks:''}`;
      else if(l.type==='add') summary='Added a new client';
      else if(l.changes&&l.changes.length) summary=l.changes.map(ch=>{
        const fLabel={name:'Name',mobile:'Mobile',rm:'RM',status:'Status',code:'Code',asset_value:'AUM',followup_status:'Follow-up',remarks:'Remarks',pan:'PAN',aum:'AUM',next_call:'Next Call'}[ch.field]||ch.field;
        return `${fLabel}: ${ch.old}→${ch.new}`;
      }).join(' | ');
      else summary='—';
      rows+=`<tr style="font-size:.8rem">
        <td style="white-space:nowrap;color:var(--gray)">${dt}</td>
        <td>${typeBadge}</td>
        <td style="font-weight:600">${l.client_name||'—'}</td>
        <td>${alSegBadge(l.seg)}</td>
        <td>${l.rm||'—'}</td>
        <td>${l.by||'—'}</td>
        <td style="color:var(--gray);max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${summary}">${summary}</td>
      </tr>`;
    });
    rows+='</tbody>';
    tbl.innerHTML=rows;
    return;
  }

  // ── DETAILED VIEW (default) ──
  let rows='<thead><tr><th>Date & Time</th><th>Type</th><th>Client</th><th>Segment</th><th>RM</th><th>By</th><th>Changes / Details</th></tr></thead><tbody>';
  all.forEach(l=>{
    const dt = alWhen(l,true);
    let detail='';
    if(l.type==='call'){
      detail=`<span style="color:var(--gray);font-size:.8rem">Status: <b>${l.call_status||'—'}</b>${l.next_call?` | Next Call: <b>${fmtDate(l.next_call)}</b>`:''} ${l.remarks?`| <i>${l.remarks}</i>`:''}</span>`;
    } else if(l.type==='add'){
      detail=`<span style="color:var(--gray);font-size:.8rem">Added a new client</span>`;
    } else if(l.changes && l.changes.length){
      detail=l.changes.map(ch=>{
        const fLabel={name:'Name',mobile:'Mobile',email:'Email',dob:'Date of Birth',rm:'RM',status:'Status',code:'Client Code',asset_value:'Asset Value',revenue:'Revenue',last_trade_date:'Last Trade',last_call_date:'Last Call',next_call:'Next Call',followup_status:'Follow-up Status',remarks:'Remarks',pan:'PAN',client_id:'Client Code',aum:'AUM',sip_amount:'SIP Amount',sip_count:'SIP Count',last_invest_date:'Last Invest'}[ch.field]||ch.field;
        return `<div style="font-size:.78rem;margin-bottom:2px"><b>${fLabel}:</b> <span style="color:#ef4444;text-decoration:line-through">${ch.old}</span> → <span style="color:#16a34a">${ch.new}</span></div>`;
      }).join('');
    } else {
      detail='<span style="color:var(--gray);font-size:.8rem">—</span>';
    }
    rows+=`<tr>
      <td style="font-size:.8rem;white-space:nowrap">${dt}</td>
      <td><span style="background:${typeBg[l.type]||'#f1f5f9'};color:${typeColor[l.type]||'#475569'};padding:2px 8px;border-radius:10px;font-size:.75rem;font-weight:700">${typeLabel[l.type]||l.type}</span></td>
      <td style="font-weight:600">${l.client_name||'—'}</td>
      <td>${alSegBadge(l.seg)}</td>
      <td style="font-size:.8rem">${l.rm||'—'}</td>
      <td style="font-size:.8rem">${l.by||'—'}</td>
      <td>${detail}</td>
    </tr>`;
  });
  rows+='</tbody>';
  tbl.innerHTML=rows;
}

function exportActivityLog(){
  const tbl=document.getElementById('al-table');
  if(!tbl||!tbl.rows.length){ toast('No data to export','error'); return; }
  const isAdmin = CU && CU.role==='admin';
  const callLogs=(DB.get('call_logs')||[]).map(l=>({type:'call',seg:l.seg||'equity',client_name:l.client_name||l.name||'',rm:l.rm||'',by:l.by||l.rm||'',date:l.date||'',ts:l.ts||'',call_status:l.status||'',next_call:l.next_call||'',remarks:l.remarks||'',changes:[]}));
  const editLogs=DB.get('activity_logs')||[];
  let all=[...editLogs,...callLogs].sort((a,b)=>String(b.ts||b.date).localeCompare(String(a.ts||a.date)));
  if(!isAdmin){
    const myRMs=[...(CU.eq_dealers||[CU.name]),...(CU.mf_dealers||[CU.name])].map(d=>d.trim().toUpperCase());
    all=all.filter(l=>myRMs.includes((l.rm||'').trim().toUpperCase()));
  }
  const rows=[];
  all.forEach(l=>{
    const dt=alWhen(l,true);
    if(l.type==='call'){
      rows.push([dt,'Call Log',l.client_name,alSegInfo(l.seg).label,l.rm,l.by,'—','—','—',`Status: ${l.call_status}${l.next_call?' | Next: '+l.next_call:''} ${l.remarks}`]);
    } else if(!l.changes||!l.changes.length){
      rows.push([dt,l.type==='add'?'New Client':'Edited',l.client_name,alSegInfo(l.seg).label,l.rm,l.by,'—','—','—','']);
    } else {
      l.changes.forEach((ch,i)=>{
        rows.push([i===0?dt:'',i===0?'Edited':'',i===0?l.client_name:'',i===0?alSegInfo(l.seg).label:'',i===0?l.rm:'',i===0?l.by:'',ch.field,ch.old,ch.new,'']);
      });
    }
  });
  const typeColor=v=>{ v=String(v||'').toLowerCase();
    if(v==='new client') return {bg:'FFC6EFCE',font:'FF006100'};
    if(v==='edited')     return {bg:'FFBDD7EE',font:'FF1F4E78'};
    if(v==='call log')   return {bg:'FFEDEDED',font:'FF555555'};
    return null; };
  const cols=[
    {header:'Date & Time',width:20},{header:'Type',width:14,align:'center',color:typeColor},
    {header:'Client Name',width:24},{header:'Segment',width:10,align:'center'},
    {header:'RM',width:12},{header:'By',width:12},{header:'Field',width:18},
    {header:'Old Value',width:20},{header:'New Value',width:20},{header:'Remarks',width:30}
  ];
  dnXlsx(`Activity_Log_${new Date().toISOString().slice(0,10)}.xlsx`, 'Activity Log — '+new Date().toLocaleDateString('en-IN'), cols, rows);
  toast('Export done!','success');
}

// ══════════════════════════════════════════
// Indian-format money helper (₹ 1,23,456.78) — kept from the removed
// Brokerage Calculator because MF Transactions & View Client still use it.
function brkFmt(n){ return (n||0).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2}); }

/* ══════════════════════════════════════════════════════════════════════════
   DUPLICATE FINDER  (Admin only)
   Equity → grouped by Client Code · MF → grouped by PAN.
   Keys are normalised (trim + uppercase + inner spaces removed) so that
   "1375210", " 1375210 " and "abcde1234f"/"ABCDE1234F" group together.
   Blank keys are skipped (a Minor's PAN is legitimately empty).
   Deletes go through DB.deleteClient / DB.deleteClientsBulk, so they are
   shard-aware and merge-safe. Every group must keep at least one record.
   ══════════════════════════════════════════════════════════════════════════ */
const DUP = {
  activeTab: 'eq',
  groups: { eq: [], mf: [] },     // [ [key, [recs...]], ... ]
  sel:    { eq: new Set(), mf: new Set() },
  stats:  { eq: {total:0,blank:0}, mf: {total:0,blank:0} },

  KEY(seg){ return seg==='eq' ? 'eq_clients' : 'mf_clients'; },
  norm(s){ return String(s==null?'':s).trim().toUpperCase().replace(/\s+/g,''); },

  scan(){
    ['eq','mf'].forEach(seg=>{
      const list = DB.get(this.KEY(seg)) || [];
      const keyOf = seg==='eq' ? (c=>this.norm(c.code)) : (c=>this.norm(c.pan));
      const groups = {}; let blank = 0;
      list.forEach(c=>{
        // Minors carry their guardian's PAN by design — grouping them here
        // would flag a real father/daughter pair as duplicates and offer to
        // delete one of them. Leave them out of the MF PAN scan entirely.
        if(seg==='mf' && c.is_minor) return;
        const k = keyOf(c);
        if(!k){ blank++; return; }
        (groups[k] = groups[k] || []).push(c);
      });
      this.groups[seg] = Object.entries(groups)
        .filter(([,v])=>v.length>1)
        .sort((a,b)=> b[1].length-a[1].length || a[0].localeCompare(b[0]));
      this.sel[seg].clear();
      this.stats[seg] = { total:list.length, blank };
      this.render(seg, list.length, blank);
    });
    this.renderBar();
  },

  _fmtD(d){ return d ? String(d).split('T')[0] : ''; },

  render(seg, total, blank){
    const gs = this.groups[seg];
    const extra = gs.reduce((s,[,v])=>s+v.length-1, 0);
    const label = seg==='eq' ? 'Client Code' : 'PAN';
    document.getElementById('dup-'+seg+'-summary').innerHTML =
      `Total ${seg==='eq'?'clients':'investors'}: <b>${total}</b> &nbsp;|&nbsp; ` +
      `Duplicate ${label}s: <b style="color:${gs.length?'#e53935':'#2e7d32'}">${gs.length}</b> &nbsp;|&nbsp; ` +
      `Extra records: <b style="color:${extra?'#e53935':'#2e7d32'}">${extra}</b> &nbsp;|&nbsp; ` +
      `<span style="color:#888">Blank ${label} (skipped): ${blank}</span>`;

    const body = document.getElementById('dup-'+seg+'-body');
    if(!gs.length){
      body.innerHTML = `<div style="padding:30px;text-align:center;color:#2e7d32;font-size:1rem">
        ✅ Koi duplicate ${label} nahi mila.</div>`;
      return;
    }

    body.innerHTML = gs.map(([key, recs])=>{
      // newest first, so "Keep newest" = keep row #1
      const sorted = recs.slice().sort((a,b)=>
        String(b.updated||b.created||'').localeCompare(String(a.updated||a.created||'')));
      const rows = sorted.map((c,i)=>{
        const extraCol = seg==='eq'
          ? `<td>${c.status||''}</td><td>${this._fmtD(c.last_trade)}</td>`
          : `<td>${c.status||''}</td><td style="text-align:right">${c.aum?'₹'+brkFmt(c.aum):''}</td>`;
        return `<tr>
          <td style="width:34px"><input type="checkbox" data-seg="${seg}" data-id="${c.id}"
                onchange="DUP.toggle('${seg}','${c.id}',this.checked)"
                ${this.sel[seg].has(c.id)?'checked':''}></td>
          <td style="color:#888">${i+1}</td>
          <td><b>${c.name||'—'}</b></td>
          <td>${c.mobile||''}</td>
          <td>${c.rm||''}</td>
          ${extraCol}
          <td style="color:#888;font-size:.72rem">${this._fmtD(c.updated||c.created)}</td>
          <td><button class="btn btn-outline" style="padding:2px 8px;font-size:.7rem;color:#e53935;border-color:#e53935"
                onclick="DUP.deleteOne('${seg}','${c.id}')">🗑️</button></td>
        </tr>`;
      }).join('');
      const hdr = seg==='eq'
        ? '<th></th><th>#</th><th>Name</th><th>Mobile</th><th>RM</th><th>Status</th><th>Last Trade</th><th>Updated</th><th></th>'
        : '<th></th><th>#</th><th>Name</th><th>Mobile</th><th>RM</th><th>Status</th><th>AUM</th><th>Updated</th><th></th>';
      return `<div style="border:1px solid #e0e0e0;border-radius:8px;margin-bottom:12px;overflow:hidden">
        <div style="background:#f5f7fa;padding:8px 12px;display:flex;align-items:center;gap:12px;flex-wrap:wrap">
          <b style="font-size:.9rem">${label}: ${key}</b>
          <span style="background:#e53935;color:#fff;border-radius:10px;padding:1px 8px;font-size:.72rem">${recs.length} records</span>
          <span style="flex:1"></span>
          <button class="btn btn-outline" style="padding:3px 10px;font-size:.72rem" onclick="DUP.keepFirst('${seg}','${key}')">Keep newest, select rest</button>
          <button class="btn btn-outline" style="padding:3px 10px;font-size:.72rem" onclick="DUP.keepLast('${seg}','${key}')">Keep oldest, select rest</button>
        </div>
        <div class="tbl-wrap"><div class="tbl-scroll"><table style="width:100%">
          <thead><tr>${hdr}</tr></thead><tbody>${rows}</tbody></table></div></div>
      </div>`;
    }).join('');
  },

  _group(seg,key){ const g=this.groups[seg].find(([k])=>k===key); return g?g[1]:[]; },
  _sortedGroup(seg,key){
    return this._group(seg,key).slice().sort((a,b)=>
      String(b.updated||b.created||'').localeCompare(String(a.updated||a.created||'')));
  },

  toggle(seg,id,on){ on ? this.sel[seg].add(id) : this.sel[seg].delete(id); this.renderBar(); },
  clearSel(){ this.sel[this.activeTab].clear(); this.scan(); },

  // select every record in the group EXCEPT the one to keep
  _keep(seg,key,idx){
    const s = this._sortedGroup(seg,key);
    s.forEach((c,i)=>{ i===idx ? this.sel[seg].delete(c.id) : this.sel[seg].add(c.id); });
    // NOTE: must NOT call scan() here — scan() clears sel[] and would wipe the
    // selection we just made. Re-render from the stats captured during scan().
    const st = this.stats[seg];
    this.render(seg, st.total, st.blank);
    this.renderBar();
  },
  keepFirst(seg,key){ this._keep(seg,key,0); },
  keepLast(seg,key){ const s=this._sortedGroup(seg,key); this._keep(seg,key,s.length-1); },

  renderBar(){
    const seg = this.activeTab;
    const n = this.sel[seg].size;
    const bar = document.getElementById('dup-actionbar');
    if(!bar) return;
    bar.style.display = n ? 'flex' : 'none';
    const el = document.getElementById('dup-selcount');
    if(el) el.textContent = `${n} record${n>1?'s':''} selected for delete`;
  },

  // Guard: never let a whole group be wiped out
  _wouldEmpty(seg, ids){
    const bad = [];
    this.groups[seg].forEach(([key,recs])=>{
      if(recs.every(c=>ids.has(c.id))) bad.push(key);
    });
    return bad;
  },

  async deleteOne(seg,id){
    const key = this.KEY(seg);
    const rec = (DB.get(key)||[]).find(c=>c.id===id);
    if(!rec) return;
    const bad = this._wouldEmpty(seg, new Set([id]));
    if(bad.length){ toast('Ye group ka aakhri record hai — delete nahi kar sakte','error'); return; }
    if(!confirm(`Delete "${rec.name}"?\n\n${seg==='eq'?'Code':'PAN'}: ${seg==='eq'?rec.code:rec.pan}\nRM: ${rec.rm||'—'}\n\nYe permanent hai.`)) return;
    await DB.deleteClient(key, id);
    DB.addActivityLog({ id:uid(), type:'delete', seg:seg==='eq'?'equity':'mf', client_id:id,
      client_name:rec.name, rm:rec.rm, by:CU.name, date:new Date().toISOString(),
      changes:[{field:'duplicate_cleanup', old:seg==='eq'?rec.code:rec.pan, new:'deleted'}] });
    toast('Deleted: '+rec.name,'success');
    this.scan(); refreshDash(); updateBadges();
  },

  async deleteSelected(){
    const seg = this.activeTab, key = this.KEY(seg);
    const ids = new Set(this.sel[seg]);
    if(!ids.size) return;
    const bad = this._wouldEmpty(seg, ids);
    if(bad.length){
      toast(`${bad.length} group me saare records selected hain — har group me 1 rakhna zaruri hai`,'error');
      return;
    }
    const list = DB.get(key)||[];
    const recs = list.filter(c=>ids.has(c.id));
    if(!confirm(`${recs.length} records DELETE karein?\n\nYe permanent hai aur wapas nahi aayega.\nBackup liya hai na?`)) return;
    await DB.deleteClientsBulk(key, [...ids]);
    DB.addActivityLog(recs.map(r=>({ id:uid(), type:'delete', seg:seg==='eq'?'equity':'mf',
      client_id:r.id, client_name:r.name, rm:r.rm, by:CU.name, date:new Date().toISOString(),
      changes:[{field:'duplicate_cleanup', old:seg==='eq'?r.code:r.pan, new:'deleted'}] })));
    toast(`✅ ${recs.length} duplicate records delete ho gaye`,'success');
    this.sel[seg].clear();
    this.scan(); refreshDash(); updateBadges();
    if(seg==='eq' && typeof renderEqTable==='function') renderEqTable();
    if(seg==='mf' && typeof renderMfTable==='function') renderMfTable();
  },

  export(){
    if(typeof XLSX==='undefined'){ toast('Excel library load nahi hui','error'); return; }
    const wb = XLSX.utils.book_new();
    let any = false;
    [['eq','Client Code','EQ dup CODE'],['mf','PAN','MF dup PAN']].forEach(([seg,label,sheet])=>{
      const rows = [];
      this.groups[seg].forEach(([k,recs])=>{
        recs.slice().sort((a,b)=>String(b.updated||b.created||'').localeCompare(String(a.updated||a.created||'')))
          .forEach((c,i)=>rows.push(seg==='eq'
            ? {[label]:k, '#':i+1, Name:c.name||'', Mobile:c.mobile||'', RM:c.rm||'', Status:c.status||'', PAN:c.pan||'', 'Last Trade':this._fmtD(c.last_trade), Updated:this._fmtD(c.updated||c.created), ID:c.id}
            : {[label]:k, '#':i+1, Name:c.name||'', Mobile:c.mobile||'', RM:c.rm||'', Status:c.status||'', Minor:c.is_minor?'YES':'', AUM:c.aum||'', Updated:this._fmtD(c.updated||c.created), ID:c.id}));
      });
      if(rows.length){ XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(rows), sheet); any = true; }
    });
    if(!any){ toast('Koi duplicate nahi — export ke liye kuch nahi','error'); return; }
    XLSX.writeFile(wb, 'duplicates_'+today()+'.xlsx');
    toast('Excel download ho gayi','success');
  }
};

/* ══════════════════════════════════════════════════════════════════════════
   INCENTIVE ENGINE — MF Business (% of amount) + Demat (fixed ₹ per a/c).
   Auto global rate with optional per-entry override. CRM side only for now;
   HR Portal sync wired later. Config in crm_data/incentive_config:
   {mf_pct, demat_fixed, mf_types[]}. Per-entry override: inc_mode + inc_value.
   ══════════════════════════════════════════════════════════════════════════ */
const INC = {
  DEFAULTS: { mf_rates:{'Lumpsum':1,'SIP':1,'Additional Buy':1,'SIP Bounce Buy':1,'Switch':1,'STP':1}, demat_fixed:100 },
  MF_TYPES: ['Lumpsum','SIP','Additional Buy','SIP Bounce Buy','Switch','STP','SWP','Redemption','SIP Stop','SIP Pause'],
  cfg(){
    const c = DB.get('incentive_config') || {};
    let rates;
    if(c.mf_rates && typeof c.mf_rates==='object' && !Array.isArray(c.mf_rates)){
      rates = c.mf_rates;
    } else if(c.mf_pct!=null || Array.isArray(c.mf_types)){
      // migrate old single-rate model → per-type
      rates = {};
      const pct = c.mf_pct!=null ? Number(c.mf_pct) : 1;
      (Array.isArray(c.mf_types)?c.mf_types:Object.keys(this.DEFAULTS.mf_rates)).forEach(t=>{ rates[t]=pct; });
    } else {
      rates = Object.assign({}, this.DEFAULTS.mf_rates);
    }
    return {
      mf_rates: rates,
      demat_fixed: (c.demat_fixed!=null ? Number(c.demat_fixed) : this.DEFAULTS.demat_fixed)
    };
  },
  fmt(n){ n=Number(n)||0; return '₹'+n.toLocaleString('en-IN',{maximumFractionDigits:0}); },

  mf(e){
    const cfg=this.cfg();
    // Admin khud ke MF transaction pe incentive nahi kamata — Approve ke baad bhi 0.
    if((e.rm||'').trim().toLowerCase()==='admin') return {amt:0, over:false, label:'admin · nil'};
    if(e.inc_mode==='fixed') return {amt:Number(e.inc_value)||0, over:true, label:'₹ fixed'};
    if(e.inc_mode==='pct')   return {amt:(Number(e.amount)||0)*(Number(e.inc_value)||0)/100, over:true, label:(Number(e.inc_value)||0)+'%'};
    const rate = Number(cfg.mf_rates[e.type]||0);
    if(rate>0) return {amt:(Number(e.amount)||0)*rate/100, over:false, label:rate+'% auto'};
    return {amt:0, over:false, label:'—'};
  },
  demat(e){
    const cfg=this.cfg();
    if(e.inc_mode==='fixed') return {amt:Number(e.inc_value)||0, over:true, label:'₹ fixed'};
    return {amt:cfg.demat_fixed, over:false, label:'auto'};
  },

  isApproved(e){ return (e && (e.status||'Pending'))==='Approved'; },

  cell(kind, e){
    const r = kind==='mf' ? this.mf(e) : this.demat(e);
    const isAdmin = CU && CU.role==='admin';
    const approved = this.isApproved(e);
    const edit = isAdmin
      ? ` <span class="btn-icon" style="cursor:pointer;font-size:.72rem;color:var(--teal)" onclick="INC.openOverride('${kind}','${e.id}')" title="Set incentive override">✏️</span>` : '';
    if(!approved){
      // Commission calculates only AFTER approval — show as not-yet-counted
      return `<td style="text-align:right;white-space:nowrap;color:var(--gray)" title="Counts only after Approve">${this.fmt(r.amt)} <span style="font-size:.62rem;background:#fee2e2;color:#b91c1c;border-radius:6px;padding:0 5px">pending</span>${edit}</td>`;
    }
    const tag = r.over
      ? `<span style="font-size:.62rem;background:#fde68a;color:#92400e;border-radius:6px;padding:0 5px;margin-left:4px" title="Manual override">${r.label}</span>`
      : `<span style="font-size:.62rem;color:var(--gray);margin-left:4px">${r.label}</span>`;
    return `<td style="text-align:right;white-space:nowrap;font-weight:600">${this.fmt(r.amt)}${tag}${edit}</td>`;
  },

  // Totals count ONLY approved entries (commission = approved only)
  total(kind, entries){ return (entries||[]).reduce((s,e)=> s + (this.isApproved(e) ? ((kind==='mf'?this.mf(e):this.demat(e)).amt||0) : 0), 0); },

  // ── per-entry override modal ──
  openOverride(kind, id){
    if(!(CU&&CU.role==='admin')) return;
    const list = kind==='mf' ? getMfBizEntries() : getEqDematEntries();
    const e = list.find(x=>x.id===id); if(!e) return;
    this._editKind=kind; this._editId=id;
    let m=document.getElementById('incOverrideModal');
    if(!m){
      m=document.createElement('div'); m.id='incOverrideModal'; m.className='modal-overlay';
      m.setAttribute('onclick',"if(event.target===this)closeModal('incOverrideModal')");
      m.innerHTML=`<div class="modal" style="width:420px">
        <div class="modal-hdr"><h3 id="incOvTitle">Incentive Override</h3>
          <button class="modal-close" onclick="closeModal('incOverrideModal')">×</button></div>
        <div class="modal-body">
          <div id="incOvInfo" style="font-size:.82rem;color:var(--gray);margin-bottom:12px"></div>
          <div class="form-field"><label>Mode</label>
            <select id="incOvMode" onchange="INC._toggleVal()">
              <option value="">Auto (global rate)</option>
              <option value="pct">Percentage of amount</option>
              <option value="fixed">Fixed ₹</option>
            </select></div>
          <div class="form-field" id="incOvValWrap" style="margin-top:10px;display:none">
            <label id="incOvValLbl">Value</label>
            <input type="number" id="incOvVal" placeholder="e.g. 500" step="0.01"></div>
          <div id="incOvPreview" style="margin-top:12px;font-weight:600"></div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-outline" onclick="closeModal('incOverrideModal')">Cancel</button>
          <button class="btn btn-success" onclick="INC.saveOverride()">Save</button>
        </div></div>`;
      document.body.appendChild(m);
    }
    const isMf = kind==='mf';
    document.getElementById('incOvTitle').textContent = 'Incentive — '+(e.client_name||'');
    document.getElementById('incOvInfo').innerHTML = isMf
      ? `Type: <b>${escapeHtml(e.type||'—')}</b> · Amount: <b>${INC.fmt(e.amount)}</b>`
      : `Demat A/c · Opening RM: <b>${escapeHtml(e.opening_rm||e.rm||'—')}</b>`;
    const modeSel=document.getElementById('incOvMode');
    modeSel.querySelector('option[value="pct"]').style.display = isMf ? '' : 'none';
    modeSel.value = e.inc_mode || '';
    document.getElementById('incOvVal').value = (e.inc_value!=null && e.inc_mode) ? e.inc_value : '';
    this._toggleVal();
    m.classList.add('open');
  },
  _toggleVal(){
    const mode=document.getElementById('incOvMode').value;
    document.getElementById('incOvValWrap').style.display = mode ? '' : 'none';
    const lbl=document.getElementById('incOvValLbl');
    if(lbl) lbl.textContent = mode==='pct' ? 'Percentage (%)' : 'Fixed amount (₹)';
    const inp=document.getElementById('incOvVal'); if(inp) inp.oninput=()=>this._preview();
    this._preview();
  },
  _preview(){
    const kind=this._editKind;
    const list = kind==='mf' ? getMfBizEntries() : getEqDematEntries();
    const e = list.find(x=>x.id===this._editId); if(!e) return;
    const mode=document.getElementById('incOvMode').value;
    const val=Number(document.getElementById('incOvVal').value)||0;
    const probe = Object.assign({}, e, {inc_mode:mode||undefined, inc_value:mode?val:undefined});
    const r = kind==='mf' ? this.mf(probe) : this.demat(probe);
    document.getElementById('incOvPreview').innerHTML = 'Incentive: '+this.fmt(r.amt)+(r.over?' <span style="color:#92400e">(override)</span>':' <span style="color:var(--gray)">(auto)</span>');
  },
  saveOverride(){
    if(!(CU&&CU.role==='admin')) return;
    const kind=this._editKind, id=this._editId;
    const mode=document.getElementById('incOvMode').value;
    const val=Number(document.getElementById('incOvVal').value)||0;
    if(mode && val<0){ toast('Invalid value','error'); return; }
    if(kind==='mf'){
      const entries=getMfBizEntries(); const e=entries.find(x=>x.id===id); if(!e) return;
      if(mode){ e.inc_mode=mode; e.inc_value=val; } else { delete e.inc_mode; delete e.inc_value; }
      setMfBizEntries(entries); renderMfTxnTable();
    } else {
      const entries=getEqDematEntries(); const e=entries.find(x=>x.id===id); if(!e) return;
      if(mode){ e.inc_mode=mode; e.inc_value=val; } else { delete e.inc_mode; delete e.inc_value; }
      setEqDematEntries(entries); renderEqDematTable();
    }
    closeModal('incOverrideModal');
    toast('Incentive override saved','success');
  },

  // ── global rate settings modal (admin) ──
  openSettings(){
    if(!(CU&&CU.role==='admin')){ toast('Admin only','error'); return; }
    const cfg=this.cfg();
    let m=document.getElementById('incSettingsModal');
    if(!m){
      m=document.createElement('div'); m.id='incSettingsModal'; m.className='modal-overlay';
      m.setAttribute('onclick',"if(event.target===this)closeModal('incSettingsModal')");
      m.innerHTML=`<div class="modal" style="width:480px">
        <div class="modal-hdr"><h3>⚙️ Incentive Rates</h3>
          <button class="modal-close" onclick="closeModal('incSettingsModal')">×</button></div>
        <div class="modal-body">
          <div class="form-field" style="max-width:240px"><label>Demat — fixed ₹ per account</label>
            <input type="number" id="incSetDemat" step="1" placeholder="e.g. 100"></div>
          <div style="margin-top:16px"><label style="font-weight:600;font-size:.85rem">MF Business — % per type</label>
            <div style="font-size:.74rem;color:var(--gray);margin:2px 0 8px">Separate rate for each type. 0 or blank = no incentive for that type.</div>
            <div id="incSetRates" style="display:grid;grid-template-columns:1fr 86px 1fr 86px;gap:8px 12px;align-items:center"></div></div>
          <div style="margin-top:12px;font-size:.78rem;color:var(--gray)">Individual entries can be overridden using ✏️.</div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-outline" onclick="closeModal('incSettingsModal')">Cancel</button>
          <button class="btn btn-success" onclick="INC.saveSettings()">Save Rates</button>
        </div></div>`;
      document.body.appendChild(m);
    }
    document.getElementById('incSetDemat').value = cfg.demat_fixed;
    document.getElementById('incSetRates').innerHTML = this.MF_TYPES.map(t=>{
      const v = cfg.mf_rates[t]!=null ? cfg.mf_rates[t] : '';
      return `<label style="font-size:.8rem">${escapeHtml(t)}</label>`
        + `<input type="number" class="incRateInput" data-type="${escapeHtml(t)}" step="0.01" value="${v}" placeholder="0" style="width:80px;padding:5px 7px">`;
    }).join('');
    m.classList.add('open');
  },
  saveSettings(){
    if(!(CU&&CU.role==='admin')) return;
    const demat_fixed=Number(document.getElementById('incSetDemat').value)||0;
    const mf_rates={};
    document.querySelectorAll('.incRateInput').forEach(inp=>{
      const t=inp.getAttribute('data-type'); const v=Number(inp.value)||0;
      if(v>0) mf_rates[t]=v;
    });
    DB.set('incentive_config', {mf_rates, demat_fixed});
    closeModal('incSettingsModal');
    toast('Incentive rates updated','success');
    const pg=getCurrentPageId();
    if(pg==='mf-txns') renderMfTxnTable();
    if(pg==='eq-demat') renderEqDematTable();
  }
};

/* ══════════════════════════════════════════════════════════════════════════
   RM-WISE SALES SUMMARY  (MF Sales ₹ + SIP ₹ + Demat opened count + incentive)
   Admin can Push to HR Portal (writes crm_data/rm_sales_summary that HR reads)
   or Remove. Final HR-side consumption wired when HR file is provided.
   ══════════════════════════════════════════════════════════════════════════ */
const RMSUM = {
  SALES_TYPES: ['Lumpsum','Additional Buy','Switch','STP','SIP Bounce Buy'],
  monthsAvailable(){
    const set=new Set();
    getMfBizEntries().forEach(e=>{ if(e.date) set.add(e.date.slice(0,7)); });
    getEqDematEntries().forEach(e=>{ if(e.date) set.add(e.date.slice(0,7)); });
    return [...set].sort().reverse();
  },
  // incentiveOnly=true → sirf wahi entries figures me jodo jinpe incentive >0 mila.
  // Aise schemes jisme amount hai lekin incentive zero hai, wo HR push me na aaye.
  compute(month, incentiveOnly){
    const inMonth = d => !month || (d||'').slice(0,7)===month;
    const map={};
    const get=rm=>{ rm=normRm(rm||'')||'(No RM)'; if(!map[rm]) map[rm]={rm,mf_sales:0,sip_amount:0,demat_count:0,mf_inc:0,demat_inc:0}; return map[rm]; };
    getMfBizEntries().forEach(e=>{
      if((e.status||'Pending')!=='Approved') return;
      if(!inMonth(e.date)) return;
      const inc=INC.mf(e).amt;
      if(incentiveOnly && !(inc>0)) return; // zero-incentive scheme skip
      const r=get(e.rm); const amt=Number(e.amount)||0;
      if(e.type==='SIP') r.sip_amount+=amt;
      else if(this.SALES_TYPES.includes(e.type)) r.mf_sales+=amt;
      r.mf_inc += inc;
    });
    getEqDematEntries().forEach(e=>{
      if((e.status||'Pending')!=='Approved') return;
      if(!inMonth(e.date)) return;
      const inc=INC.demat(e).amt;
      if(incentiveOnly && !(inc>0)) return; // zero-incentive demat skip
      const r=get(e.opening_rm||e.rm); r.demat_count++; r.demat_inc += inc;
    });
    return Object.values(map).map(r=>({...r,total_inc:r.mf_inc+r.demat_inc}))
      .sort((a,b)=>(b.mf_sales+b.sip_amount)-(a.mf_sales+a.sip_amount));
  },
  open(){
    if(!(CU&&CU.role==='admin')){ toast('Admin only','error'); return; }
    let m=document.getElementById('rmSumModal');
    if(!m){
      m=document.createElement('div'); m.id='rmSumModal'; m.className='modal-overlay';
      m.setAttribute('onclick',"if(event.target===this)closeModal('rmSumModal')");
      m.innerHTML=`<div class="modal" style="width:780px;max-width:96vw">
        <div class="modal-hdr"><h3>📊 RM-wise Sales Summary</h3>
          <button class="modal-close" onclick="closeModal('rmSumModal')">×</button></div>
        <div class="modal-body">
          <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:12px">
            <label style="font-size:.85rem;font-weight:600">Month:</label>
            <select id="rmSumMonth" onchange="RMSUM.render()" style="max-width:150px"></select>
            <button class="btn btn-outline" onclick="RMSUM.exportXlsx()">⬇️ Export</button>
            <span style="flex:1"></span>
            <button class="btn btn-primary" onclick="RMSUM.pushHR()">📤 Push to HR Portal</button>
            <button class="btn btn-outline" style="color:var(--red)" onclick="RMSUM.removeHR()">🗑️ Remove from HR</button>
          </div>
          <div id="rmSumStatus" style="font-size:.78rem;color:var(--gray);margin-bottom:8px"></div>
          <div class="tbl-wrap"><div id="rmSumTable" style="overflow:auto"></div></div>
          <div style="font-size:.72rem;color:var(--gray);margin-top:8px">MF Sales = Lumpsum + Additional Buy + Switch + STP + SIP Bounce Buy. SIP = New SIP. <b>Only Approved entries are counted.</b></div>
        </div>
      </div>`;
      document.body.appendChild(m);
    }
    const months=this.monthsAvailable();
    const cur=new Date().toISOString().slice(0,7);
    const sel=document.getElementById('rmSumMonth');
    sel.innerHTML=`<option value="">All months</option>`+months.map(mo=>`<option value="${mo}" ${mo===cur?'selected':''}>${mo}</option>`).join('');
    if(!months.includes(cur)) sel.value='';
    this.render();
    m.classList.add('open');
  },
  render(){
    const month=document.getElementById('rmSumMonth').value;
    const rows=this.compute(month);
    const tbl=document.getElementById('rmSumTable');
    const f=n=>INC.fmt(n);
    if(!rows.length){ tbl.innerHTML='<div style="padding:24px;text-align:center;color:var(--gray)">No data for this period</div>'; this._showStatus(); return; }
    const tot=rows.reduce((a,r)=>({mf_sales:a.mf_sales+r.mf_sales,sip_amount:a.sip_amount+r.sip_amount,demat_count:a.demat_count+r.demat_count,mf_inc:a.mf_inc+r.mf_inc,demat_inc:a.demat_inc+r.demat_inc,total_inc:a.total_inc+r.total_inc}),{mf_sales:0,sip_amount:0,demat_count:0,mf_inc:0,demat_inc:0,total_inc:0});
    tbl.innerHTML=`<table><thead><tr>
      <th>RM</th><th style="text-align:right">MF Sales</th><th style="text-align:right">SIP Amount</th>
      <th style="text-align:right">Demat Opened</th><th style="text-align:right">MF Incentive</th>
      <th style="text-align:right">Demat Incentive</th><th style="text-align:right">Total Incentive</th></tr></thead><tbody>
      ${rows.map(r=>`<tr>
        <td style="font-weight:600">${escapeHtml(r.rm)}</td>
        <td style="text-align:right">${f(r.mf_sales)}</td>
        <td style="text-align:right">${f(r.sip_amount)}</td>
        <td style="text-align:right">${r.demat_count}</td>
        <td style="text-align:right">${f(r.mf_inc)}</td>
        <td style="text-align:right">${f(r.demat_inc)}</td>
        <td style="text-align:right;font-weight:700">${f(r.total_inc)}</td></tr>`).join('')}
      <tr style="font-weight:700;background:var(--bg,#f6f7fb)">
        <td>TOTAL</td><td style="text-align:right">${f(tot.mf_sales)}</td><td style="text-align:right">${f(tot.sip_amount)}</td>
        <td style="text-align:right">${tot.demat_count}</td><td style="text-align:right">${f(tot.mf_inc)}</td>
        <td style="text-align:right">${f(tot.demat_inc)}</td><td style="text-align:right">${f(tot.total_inc)}</td></tr>
      </tbody></table>`;
    this._showStatus();
  },
  _showStatus(){
    const s=document.getElementById('rmSumStatus'); if(!s) return;
    const hr=DB.get('rm_sales_summary');
    if(hr && Array.isArray(hr.rows) && hr.rows.length){
      s.innerHTML=`✅ HR Portal me pushed: <b>${escapeHtml(hr.month||'all')}</b> · ${hr.rows.length} RMs · ${hr.generated_at?new Date(hr.generated_at).toLocaleString('en-IN'):''} by ${escapeHtml(hr.by||'')}`;
    } else {
      s.innerHTML='Nothing pushed to HR Portal yet.';
    }
  },
  pushHR(){
    if(!(CU&&CU.role==='admin')) return;
    const month=document.getElementById('rmSumMonth').value;
    // Incentive-only figures: zero-incentive scheme ke amount in figures me nahi aate.
    const incMap={};
    this.compute(month, true).forEach(r=>{ incMap[r.rm]=r; });
    // Base list = is period me jis bhi RM ki approved activity hai (Admin chhod ke).
    // NIL incentive wale RM ka naam bhi rahe — figures 0 ke saath.
    const rows=this.compute(month)
      .filter(r=>(r.rm||'').trim().toLowerCase()!=='admin')
      .map(r=>{
        const i=incMap[r.rm]||{mf_sales:0,sip_amount:0,demat_count:0,mf_inc:0,demat_inc:0,total_inc:0};
        return {rm:r.rm,mf_sales:Math.round(i.mf_sales),sip_amount:Math.round(i.sip_amount),demat_count:i.demat_count,mf_incentive:Math.round(i.mf_inc),demat_incentive:Math.round(i.demat_inc),total_incentive:Math.round(i.total_inc)};
      });
    if(!rows.length){ toast('No RM activity in this period','error'); return; }
    DB.set('rm_sales_summary', {month:month||'all', rows, generated_at:new Date().toISOString(), by:CU.name});
    toast('Pushed to HR Portal','success');
    this._showStatus();
  },
  removeHR(){
    if(!(CU&&CU.role==='admin')) return;
    if(!confirm('Remove this summary from HR Portal?')) return;
    DB.set('rm_sales_summary', {month:'', rows:[], cleared_at:new Date().toISOString(), by:CU.name});
    toast('Removed from HR Portal','success');
    this._showStatus();
  },
  exportXlsx(){
    const month=document.getElementById('rmSumMonth').value;
    const rows=this.compute(month);
    if(!rows.length){ toast('No data','error'); return; }
    const cols=[{header:'RM',width:18},{header:'MF Sales',width:16,money:true},{header:'SIP Amount',width:16,money:true},{header:'Demat Opened',width:14,align:'center'},{header:'MF Incentive',width:16,money:true},{header:'Demat Incentive',width:16,money:true},{header:'Total Incentive',width:16,money:true}];
    const data=rows.map(r=>[r.rm,Math.round(r.mf_sales),Math.round(r.sip_amount),r.demat_count,Math.round(r.mf_inc),Math.round(r.demat_inc),Math.round(r.total_inc)]);
    const tot=rows.reduce((a,r)=>[null,a[1]+r.mf_sales,a[2]+r.sip_amount,a[3]+r.demat_count,a[4]+r.mf_inc,a[5]+r.demat_inc,a[6]+r.total_inc],[null,0,0,0,0,0,0]);
    const totalRow=['TOTAL',Math.round(tot[1]),Math.round(tot[2]),tot[3],Math.round(tot[4]),Math.round(tot[5]),Math.round(tot[6])];
    dnXlsx('RM_Sales_Summary_'+(month||'all')+'.xlsx','RM-wise Sales Summary — '+(month||'All months'),cols,data,totalRow);
    toast('Export done!','success');
  }
};

/* ══════════════════════════════════════════════════════════════════════════
   BULK UPLOAD — MF Business (MF Transactions) + Demat Open. Admin only.
   Template download + Excel upload (header-name based mapping, order-flexible).
   Uploaded entries are auto-Approved (admin upload).
   ══════════════════════════════════════════════════════════════════════════ */
const MFUP = {
  TYPES: ['Lumpsum','SIP','SIP Stop','Switch','STP','Redemption','SWP','Additional Buy','SIP Bounce Buy','SIP Pause'],
  cleanRm(v){ return normRm(String(v||'').replace(/\(.*?\)/g,'').trim()); },
  buildColMap(headerRow, spec){
    const map={};
    (headerRow||[]).forEach((cell,ci)=>{
      const h=normHdr(cell);
      for(const key in spec){ if(map[key]===undefined && spec[key].includes(h)) map[key]=ci; }
    });
    return map;
  },

  // ── MF BUSINESS ──
  mfTemplate(){
    const cols=[{header:'Date',width:12},{header:'Type',width:16},{header:'Client Name',width:24},{header:'Mobile',width:14},{header:'PAN',width:14},{header:'Fund Name',width:32},{header:'Amount',width:12,money:true},{header:'RM',width:14},{header:'Remark',width:20}];
    const rows=[
      ['2026-04-01','Lumpsum','RAMESH AGARWAL','9431523242','AAWPA4903C','HDFC Flexi Cap Fund - Regular Plan - Growth',25000,'Komal','SENT'],
      ['2026-04-02','SIP','REKHA RANI','9431793095','CQBPP7850A','TATA India Innovation Fund',5000,'Megha','']
    ];
    dnXlsx('MF_Business_Template.xlsx','MF Business Upload Template', cols, rows);
    toast('MF template downloaded','success');
  },
  mfUpload(input){
    if(CU.role!=='admin'){ toast('Admin only','error'); if(input) input.value=''; return; }
    const file=input&&input.files&&input.files[0]; if(input) input.value='';
    if(!file) return;
    readExcel(file,(err,rows)=>{
      if(err||!rows||!rows.length){ toast('Could not read file','error'); return; }
      const spec={date:['date'],type:['type','txntype','transactiontype'],client_name:['clientname','name','client'],mobile:['mobile','mobileno','phone','contact'],pan:['pan','pancard','panno'],fund_name:['fundname','fund','scheme','schemename'],amount:['amount','amt'],rm:['rm','dealer','relationshipmanager','dealername'],remarks:['remark','remarks','note']};
      let hdrIdx=-1, colMap={};
      for(let i=0;i<Math.min(rows.length,15);i++){
        const map=this.buildColMap(rows[i],spec);
        if(map.client_name!==undefined && (map.date!==undefined||map.amount!==undefined)){ hdrIdx=i; colMap=map; break; }
      }
      if(hdrIdx<0){ toast('Header row not found — use the Template columns','error'); return; }
      const mf=DB.get('mf_clients')||[];
      const findClient=(name,pan,mob)=>{
        pan=(pan||'').trim().toUpperCase(); mob=(mob||'').replace(/\D/g,'').slice(-10); name=(name||'').trim().toLowerCase();
        return mf.find(c=>(pan&&(c.pan||'').trim().toUpperCase()===pan)||(mob&&String(c.mobile||'').replace(/\D/g,'').slice(-10)===mob)||(name&&(c.name||'').trim().toLowerCase()===name));
      };
      const newEntries=[]; let skipped=0;
      for(let i=hdrIdx+1;i<rows.length;i++){
        const r=rows[i]; if(!r) continue;
        const name=colMap.client_name!==undefined?String(r[colMap.client_name]||'').trim():'';
        const date=colMap.date!==undefined?parseExcelDate(r[colMap.date]):'';
        if(!name && !date) continue;
        if(!name){ skipped++; continue; }
        let type=colMap.type!==undefined?String(r[colMap.type]||'').trim():'';
        const tmatch=this.TYPES.find(t=>t.toLowerCase()===type.toLowerCase());
        type=tmatch||type||'Lumpsum';
        const amount=colMap.amount!==undefined?(parseFloat(String(r[colMap.amount]).replace(/[^0-9.\-]/g,''))||0):0;
        const pan=colMap.pan!==undefined?String(r[colMap.pan]||'').trim().toUpperCase():'';
        const mob=colMap.mobile!==undefined?String(r[colMap.mobile]||'').replace(/\D/g,'').slice(-10):'';
        const client=findClient(name,pan,mob);
        newEntries.push({
          id:uid(), client_id:client?client.id:'', client_name:name,
          rm:this.cleanRm(colMap.rm!==undefined?r[colMap.rm]:(client?client.rm:'')),
          type, amount, pan,
          fund_name:colMap.fund_name!==undefined?String(r[colMap.fund_name]||'').trim():'',
          date:date||today(),
          remarks:colMap.remarks!==undefined?String(r[colMap.remarks]||'').trim():'',
          created_by:CU.name, created_by_role:CU.role, created:today(),
          status:'Approved', decline_reason:'', cross_remark:'', cross_remark_by:'', cross_remark_at:''
        });
      }
      if(!newEntries.length){ toast('No valid rows found','error'); return; }
      if(!confirm(newEntries.length+' MF business entries upload hongi (status: Approved)'+(skipped?(', '+skipped+' blank/invalid skip'):'')+'.\nConfirm?')) return;
      const biz=DB.get('mf_business');
      const entries=(Array.isArray(biz)?biz:(biz?.entries||[])).concat(newEntries);
      const eqEntries=Array.isArray(biz)?[]:(biz?.eq_entries||[]);
      DB.set('mf_business',{entries,eq_entries:eqEntries});
      if(typeof renderMfTxnTable==='function') renderMfTxnTable();
      refreshDash&&refreshDash();
      toast(newEntries.length+' MF entries uploaded','success');
    });
  },

  // ── DEMAT OPEN ──
  dematTemplate(){
    const cols=[{header:'Date',width:12},{header:'Client Name',width:24},{header:'Client Code',width:14},{header:'Mobile',width:14},{header:'Trading RM',width:14},{header:'Opening RM',width:14},{header:'Remark',width:20}];
    const rows=[
      ['2026-04-01','RAMESH AGARWAL','ABC1234','9431523242','Komal','Rohit','New a/c'],
      ['2026-04-02','BALRAM DAS','XYZ5678','9470573834','Megha','Megha','']
    ];
    dnXlsx('Demat_Open_Template.xlsx','Demat Open Upload Template', cols, rows);
    toast('Demat template downloaded','success');
  },
  dematUpload(input){
    if(CU.role!=='admin'){ toast('Admin only','error'); if(input) input.value=''; return; }
    const file=input&&input.files&&input.files[0]; if(input) input.value='';
    if(!file) return;
    readExcel(file,(err,rows)=>{
      if(err||!rows||!rows.length){ toast('Could not read file','error'); return; }
      const spec={date:['date'],client_name:['clientname','name','client'],client_code:['clientcode','code','ucc','clientcodeucc'],mobile:['mobile','mobileno','phone','contact'],rm:['rm','tradingrm','dealer','relationshipmanager'],opening_rm:['openingrm','openrm','openingrelationshipmanager','openingdealer'],remarks:['remark','remarks','note']};
      let hdrIdx=-1, colMap={};
      for(let i=0;i<Math.min(rows.length,15);i++){
        const map=this.buildColMap(rows[i],spec);
        if(map.client_name!==undefined && (map.date!==undefined||map.client_code!==undefined)){ hdrIdx=i; colMap=map; break; }
      }
      if(hdrIdx<0){ toast('Header row not found — use the Template columns','error'); return; }
      const eq=DB.get('eq_clients')||[];
      const findClient=(name,code,mob)=>{
        code=(code||'').trim().toUpperCase(); mob=(mob||'').replace(/\D/g,'').slice(-10); name=(name||'').trim().toLowerCase();
        return eq.find(c=>(code&&(c.code||'').trim().toUpperCase()===code)||(mob&&String(c.mobile||'').replace(/\D/g,'').slice(-10)===mob)||(name&&(c.name||'').trim().toLowerCase()===name));
      };
      const newEntries=[]; let skipped=0;
      for(let i=hdrIdx+1;i<rows.length;i++){
        const r=rows[i]; if(!r) continue;
        const name=colMap.client_name!==undefined?String(r[colMap.client_name]||'').trim():'';
        const date=colMap.date!==undefined?parseExcelDate(r[colMap.date]):'';
        if(!name && !date) continue;
        if(!name){ skipped++; continue; }
        const code=colMap.client_code!==undefined?String(r[colMap.client_code]||'').trim().toUpperCase():'';
        const mob=colMap.mobile!==undefined?String(r[colMap.mobile]||'').replace(/\D/g,'').slice(-10):'';
        const client=findClient(name,code,mob);
        const tradeRm=this.cleanRm(colMap.rm!==undefined?r[colMap.rm]:(client?client.rm:''));
        const openRm=this.cleanRm(colMap.opening_rm!==undefined?r[colMap.opening_rm]:'')||tradeRm||CU.name;
        newEntries.push({
          id:uid(), client_id:client?client.id:'', client_name:name, client_code:code,
          rm:tradeRm||(client?client.rm:''), opening_rm:openRm,
          type:'Open Demat Account', date:date||today(),
          remarks:colMap.remarks!==undefined?String(r[colMap.remarks]||'').trim():'',
          mobile:mob, created_by:CU.name, created:today(),
          status:'Approved', decline_reason:''
        });
      }
      if(!newEntries.length){ toast('No valid rows found','error'); return; }
      if(!confirm(newEntries.length+' Demat entries upload hongi (status: Approved)'+(skipped?(', '+skipped+' blank/invalid skip'):'')+'.\nConfirm?')) return;
      setEqDematEntries(getEqDematEntries().concat(newEntries));
      if(typeof renderEqDematTable==='function') renderEqDematTable();
      refreshDash&&refreshDash();
      toast(newEntries.length+' Demat entries uploaded','success');
    });
  }
};

/* ══════════════════════════════════════════════════════════════════════════
   FULL BACKUP & RESTORE (admin) — dumps ALL Firestore data (crm_data + hr_data
   + shared_control) to one JSON file; restore writes it back. Covers both apps.
   ══════════════════════════════════════════════════════════════════════════ */
const BK = {
  COLLECTIONS: ['crm_data','hr_data','shared_control'],
  setStatus(msg){ const el=document.getElementById('bk-status'); if(el) el.textContent=msg||''; },
  async backup(){
    if(!(CU&&CU.role==='admin')){ toast('Admin only','error'); return; }
    if(typeof fdb==='undefined'){ toast('Firebase is not connected','error'); return; }
    this.setStatus('Creating backup…');
    try{
      const out={app:'DNInvest', type:'full_backup', version:1, created:new Date().toISOString(), by:CU.name, data:{}};
      let docCount=0;
      for(const col of this.COLLECTIONS){
        out.data[col]={};
        const snap=await fdb.collection(col).get();
        snap.forEach(doc=>{ out.data[col][doc.id]=doc.data(); docCount++; });
      }
      const blob=new Blob([JSON.stringify(out)],{type:'application/json'});
      const url=URL.createObjectURL(blob);
      const a=document.createElement('a');
      a.href=url; a.download='DNInvest_Backup_'+today()+'_'+Date.now()+'.json';
      document.body.appendChild(a); a.click(); a.remove();
      setTimeout(()=>URL.revokeObjectURL(url), 4000);
      this.setStatus('✅ Backup downloaded — '+docCount+' records · '+new Date().toLocaleString('en-IN'));
      toast('Backup downloaded','success');
    }catch(e){
      this.setStatus('❌ Backup error');
      toast('Backup error: '+(e.message||e),'error');
    }
  },
  async restore(input){
    if(!(CU&&CU.role==='admin')){ toast('Admin only','error'); if(input) input.value=''; return; }
    const file=input && input.files && input.files[0]; if(input) input.value='';
    if(!file) return;
    if(typeof fdb==='undefined'){ toast('Firebase is not connected','error'); return; }
    let parsed;
    try{ parsed=JSON.parse(await file.text()); }catch(e){ toast('Could not read file (invalid JSON)','error'); return; }
    if(!parsed || parsed.type!=='full_backup' || !parsed.data){ toast('This is not a valid DNInvest backup file','error'); return; }
    let n=0; Object.keys(parsed.data).forEach(c=>{ n+=Object.keys(parsed.data[c]||{}).length; });
    const when = parsed.created ? new Date(parsed.created).toLocaleString('en-IN') : 'unknown';
    if(!confirm('RESTORE?\n\nBackup date: '+when+' (by '+(parsed.by||'?')+')\nRecords: '+n+'\n\nThis will REPLACE the CURRENT data with the data in this file.\nIt is better to take a fresh backup first.')) return;
    const typed = prompt('Type "RESTORE" to confirm:');
    if((typed||'').trim().toUpperCase()!=='RESTORE'){ toast('Restore cancel','error'); return; }
    this.setStatus('Restoring… (do not close/refresh the page)');
    try{
      let done=0;
      for(const col of Object.keys(parsed.data)){
        for(const docId of Object.keys(parsed.data[col])){
          await fdb.collection(col).doc(docId).set(parsed.data[col][docId]);
          done++; this.setStatus('Restore: '+done+'/'+n+'…');
        }
      }
      this.setStatus('✅ Restore complete — '+done+' records. Reloading…');
      toast('Restore complete! Reloading…','success');
      setTimeout(()=>location.reload(), 1500);
    }catch(e){
      this.setStatus('❌ Restore error');
      toast('Restore error: '+(e.message||e),'error');
    }
  }
};

/* ══════════════════════════════════════════════════════════════════════════
   ADMIN BULK OPERATIONS  (Equity / MF / Leads)
   Select clients via checkbox → Set RM (map/change) · Unmap RM ·
   Shift segment (Equity↔MF copy, Lead→Equity/MF move) · Delete.
   Admin-only. All ops show a confirmation summary before applying and write
   an activity-log entry. Uses the same merge-on-write DB helpers.
   ══════════════════════════════════════════════════════════════════════════ */
const BULK = {
  sel: { eq:new Set(), mf:new Set(), leads:new Set() },
  KEY: { eq:'eq_clients', mf:'mf_clients', leads:'leads' },
  LBL: { eq:'Equity', mf:'MF', leads:'Lead' },

  isAdmin(){ return CU && CU.role==='admin'; },
  _esc(s){ return String(s==null?'':s).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); },
  filtered(seg){ return seg==='eq'?eqFiltered : seg==='mf'?mfFiltered : leadsFiltered; },

  // ── table cell builders (called from render functions) ──
  th(seg){
    if(!this.isAdmin()) return '';
    return `<th style="text-align:center" onclick="event.stopPropagation()">`
      + `<input type="checkbox" id="bulk-all-${seg}" title="Select all on this page" `
      + `onclick="event.stopPropagation()" onchange="BULK.togglePage('${seg}',this.checked)"></th>`;
  },
  td(seg,id){
    if(!this.isAdmin()) return '';
    const ck=this.sel[seg].has(id)?'checked':'';
    return `<td style="text-align:center" onclick="event.stopPropagation()">`
      + `<input type="checkbox" class="bulk-cb" data-id="${id}" ${ck} `
      + `onclick="event.stopPropagation()" onchange="BULK.toggle('${seg}','${id}',this.checked)"></td>`;
  },

  // ── selection ──
  toggle(seg,id,checked){
    if(checked) this.sel[seg].add(id); else this.sel[seg].delete(id);
    this.syncHeader(seg); this.updateBar(seg);
  },
  togglePage(seg,checked){
    document.querySelectorAll(`#${seg}-table .bulk-cb`).forEach(cb=>{
      cb.checked=checked; const id=cb.getAttribute('data-id');
      if(checked) this.sel[seg].add(id); else this.sel[seg].delete(id);
    });
    this.updateBar(seg);
  },
  selectAllFiltered(seg){
    this.filtered(seg).forEach(c=>this.sel[seg].add(c.id));
    this._rerender(seg); this.updateBar(seg);
  },
  clear(seg){ this.sel[seg].clear(); this._rerender(seg); this.updateBar(seg); },
  clearAll(){ this.sel.eq.clear(); this.sel.mf.clear(); this.sel.leads.clear(); this.hideBar(); },

  _rerender(seg){ if(seg==='eq') renderEqTable(); else if(seg==='mf') renderMfTable(); else renderLeadsTable(); },

  afterRender(seg){
    if(!this.isAdmin()) return;
    this.syncHeader(seg);
    this.updateBar(seg);
  },
  syncHeader(seg){
    const box=document.getElementById('bulk-all-'+seg); if(!box) return;
    const cbs=[...document.querySelectorAll(`#${seg}-table .bulk-cb`)];
    const checkedCount=cbs.filter(cb=>cb.checked).length;
    box.checked = cbs.length>0 && checkedCount===cbs.length;
    box.indeterminate = checkedCount>0 && checkedCount<cbs.length;
  },

  // ── RM option list ──
  rmList(seg){
    // Admin is included so clients can be mapped directly to Admin too
    const users=(DB.get('users')||DEFAULT_USERS).filter(u=>u.active!==false && u.role!=='mf_desk');
    let pool;
    if(seg==='eq') pool=users.filter(u=>u.role==='admin' || (u.segments||[]).includes('equity'));
    else if(seg==='mf') pool=users.filter(u=>u.role==='admin' || (u.segments||[]).includes('mf'));
    else pool=users; // leads: any RM
    return [...new Set(pool.map(u=>u.name))].sort((a,b)=>a.localeCompare(b));
  },

  // ── floating action bar ──
  ensureBar(){
    let bar=document.getElementById('bulkBar');
    if(bar) return bar;
    bar=document.createElement('div');
    bar.id='bulkBar'; bar.className='bulk-bar'; bar.style.display='none';
    document.body.appendChild(bar);
    return bar;
  },
  hideBar(){ const b=document.getElementById('bulkBar'); if(b) b.style.display='none'; },
  updateBar(seg){
    if(!this.isAdmin()) return;
    const bar=this.ensureBar();
    const n=this.sel[seg].size;
    if(n===0){ bar.style.display='none'; return; }
    bar.dataset.seg=seg;
    const totalFiltered=this.filtered(seg).length;
    const rmOpts=this.rmList(seg).map(r=>`<option value="${this._esc(r)}">${this._esc(r)}</option>`).join('');
    let shift='';
    if(seg==='eq') shift=`<button class="bb-btn bb-shift" onclick="BULK.shift('eq')">🏦 Shift→MF</button>`;
    else if(seg==='mf') shift=`<button class="bb-btn bb-shift" onclick="BULK.shift('mf')">📈 Shift→Equity</button>`;
    else shift=`<button class="bb-btn bb-shift" onclick="BULK.shift('leads','equity')">📈 →Equity</button>`
              +`<button class="bb-btn bb-shift" onclick="BULK.shift('leads','mf')">🏦 →MF</button>`;
    const selAll = n<totalFiltered
      ? `<a class="bb-link" onclick="BULK.selectAllFiltered('${seg}')">Select all ${totalFiltered}</a>` : '';
    bar.innerHTML=`
      <div class="bb-left">
        <span class="bb-count">${n} selected</span>${selAll}
      </div>
      <div class="bb-right">
        <select id="bb-rm" class="bb-sel"><option value="">— RM —</option>${rmOpts}</select>
        <button class="bb-btn bb-set" onclick="BULK.setRm()">✔ Set RM</button>
        <button class="bb-btn bb-unmap" onclick="BULK.unmap()">⊘ Unmap</button>
        ${shift}
        <button class="bb-btn bb-del" onclick="BULK.del()">🗑️ Delete</button>
        <button class="bb-btn bb-close" onclick="BULK.clear('${seg}')">✕</button>
      </div>`;
    bar.style.display='flex';
  },

  _selRecords(seg){
    const ids=this.sel[seg]; const key=this.KEY[seg];
    return (DB.get(key)||[]).filter(c=>ids.has(c.id));
  },

  // ── confirmation modal ──
  confirm(title, bodyHtml, okLabel, okClass, onYes){
    let m=document.getElementById('bulkConfirmModal');
    if(!m){
      m=document.createElement('div');
      m.id='bulkConfirmModal'; m.className='modal-overlay';
      m.setAttribute('onclick',"if(event.target===this)closeModal('bulkConfirmModal')");
      m.innerHTML=`<div class="modal" style="width:460px">
        <div class="modal-hdr"><h3 id="bulkConfirmTitle"></h3>
          <button class="modal-close" onclick="closeModal('bulkConfirmModal')">×</button></div>
        <div class="modal-body" id="bulkConfirmBody" style="font-size:.9rem;line-height:1.55"></div>
        <div class="modal-footer">
          <button class="btn btn-outline" onclick="closeModal('bulkConfirmModal')">Cancel</button>
          <button id="bulkConfirmOk" class="btn"></button>
        </div></div>`;
      document.body.appendChild(m);
    }
    document.getElementById('bulkConfirmTitle').textContent=title;
    document.getElementById('bulkConfirmBody').innerHTML=bodyHtml;
    const ok=document.getElementById('bulkConfirmOk');
    ok.textContent=okLabel; ok.className='btn '+(okClass||'btn-primary');
    ok.onclick=async()=>{ closeModal('bulkConfirmModal'); await onYes(); };
    m.classList.add('open');
  },

  // ── 1) SET RM (map / change) ──
  setRm(){
    const seg=this.ensureBar().dataset.seg;
    const newRm=normRm((document.getElementById('bb-rm')||{value:''}).value.trim());
    if(!newRm){ toast('Choose an RM first','error'); return; }
    const recs=this._selRecords(seg);
    const change=recs.filter(c=>(c.rm||'').trim().toLowerCase()!==newRm.toLowerCase());
    if(!change.length){ toast('In sabhi ka RM already '+newRm+' hai','error'); return; }
    this.confirm('Bulk RM Change',
      `<b>${change.length}</b> ${this.LBL[seg]} client(s) will get RM <b>${this._esc(newRm)}</b>.`
      + (recs.length-change.length?`<br><span style="color:var(--gray)">${recs.length-change.length} already ${this._esc(newRm)} — skip.</span>`:''),
      '✔ Confirm','', async()=>{
        const key=this.KEY[seg], logs=[];
        change.forEach(c=>{ const old=c.rm||'—'; c.rm=newRm; c.updated=today();
          logs.push({id:uid(),type:'bulk_rm_update',seg,client_id:c.id,client_name:c.name,rm:newRm,by:CU.name,date:new Date().toISOString(),changes:[{field:'rm',old,new:newRm}]}); });
        await DB.setClientsBulk(key,change);
        if(logs.length) await DB.addActivityLog(logs);
        this._done(seg,`✅ ${change.length} client(s) — RM set to ${newRm}`);
      });
  },

  // ── 2) UNMAP (clear RM) ──
  unmap(){
    const seg=this.ensureBar().dataset.seg;
    const recs=this._selRecords(seg).filter(c=>(c.rm||'').trim()!=='');
    if(!recs.length){ toast('None of these have an RM mapped','error'); return; }
    this.confirm('Bulk Unmap RM',
      `<b>${recs.length}</b> ${this.LBL[seg]} client(s) will have their RM removed (unassigned).`,
      '⊘ Unmap','btn-danger', async()=>{
        const key=this.KEY[seg], logs=[];
        recs.forEach(c=>{ const old=c.rm||'—'; c.rm=''; c.updated=today();
          logs.push({id:uid(),type:'bulk_rm_update',seg,client_id:c.id,client_name:c.name,rm:'',by:CU.name,date:new Date().toISOString(),changes:[{field:'rm',old,new:'(unmapped)'}]}); });
        await DB.setClientsBulk(key,recs);
        if(logs.length) await DB.addActivityLog(logs);
        this._done(seg,`✅ ${recs.length} client(s) unmapped`);
      });
  },

  // ── 3) SHIFT segment ──
  shift(seg,target){
    const recs=this._selRecords(seg);
    if(!recs.length) return;
    const cleanMob=v=>String(v||'').replace(/\D/g,'').slice(-10);
    const validPan=p=>/^[A-Z]{5}[0-9]{4}[A-Z]$/.test(String(p||'').trim().toUpperCase());

    if(seg==='eq'){ // Equity → MF (copy, keep equity). PAN required.
      const mf=DB.get('mf_clients')||[];
      const panSet=new Set(mf.filter(c=>c.pan).map(c=>c.pan.trim().toUpperCase()));
      const mobSet=new Set(mf.map(c=>cleanMob(c.mobile)).filter(Boolean));
      const nameSet=new Set(mf.map(c=>(c.name||'').trim().toUpperCase()));
      let create=[], noPan=0, dup=0;
      recs.forEach(c=>{
        const pan=(c.pan||'').trim().toUpperCase();
        if(!validPan(pan)){ noPan++; return; }
        if(panSet.has(pan) || mobSet.has(cleanMob(c.mobile)) || nameSet.has((c.name||'').trim().toUpperCase())){ dup++; return; }
        panSet.add(pan); create.push(c);
      });
      if(!create.length){ toast(`Nothing shifted — ${noPan} without PAN, ${dup} already in MF`,'error'); return; }
      this.confirm('Shift Equity → MF',
        `<b>${create.length}</b> Equity client(s) will be added to MF <i>(Equity record remains — copy)</i>.`
        + (noPan?`<br><span style="color:var(--orange)">⚠️ ${noPan} without a valid PAN — skipped (MF cannot be created without PAN).</span>`:'')
        + (dup?`<br><span style="color:var(--gray)">${dup} already in MF — skipped.</span>`:''),
        '🏦 Shift','', async()=>{
          const recsNew=[], logs=[];
          create.forEach(c=>{ let id=uid(); while(mf.some(x=>x.id===id)||recsNew.some(x=>x.id===id)) id=uid();
            recsNew.push({id,name:c.name,mobile:c.mobile||'',pan:(c.pan||'').trim().toUpperCase(),email:c.email||'',rm:c.rm||'',status:'Prospect',aum:null,sip_amount:null,sip_count:null,last_invest_date:'',last_call_date:c.last_call_date||'',next_call:'',followup_status:'',remarks:`Shifted from Equity${c.code?' (Code: '+c.code+')':''}`,created:today(),updated:today()});
            logs.push({id:uid(),type:'add',seg:'mf',client_id:id,client_name:c.name,rm:c.rm||'',by:CU.name,date:new Date().toISOString(),changes:[]}); });
          await DB.setClientsBulk('mf_clients',recsNew);
          if(logs.length) await DB.addActivityLog(logs);
          this._done('eq',`✅ ${recsNew.length} client(s) added to MF (Equity intact)`); renderMfTable();
        });

    } else if(seg==='mf'){ // MF → Equity (copy, keep MF). Code blank allowed.
      const eq=DB.get('eq_clients')||[];
      const mobSet=new Set(eq.map(c=>cleanMob(c.mobile)).filter(Boolean));
      const nameSet=new Set(eq.map(c=>(c.name||'').trim().toUpperCase()));
      let create=[], dup=0;
      recs.forEach(c=>{
        if(mobSet.has(cleanMob(c.mobile)) || nameSet.has((c.name||'').trim().toUpperCase())){ dup++; return; }
        nameSet.add((c.name||'').trim().toUpperCase()); create.push(c);
      });
      if(!create.length){ toast(`Nothing shifted — ${dup} already in Equity`,'error'); return; }
      // Client Code is mandatory for Equity — ask per record, skip any left blank.
      const codeMap={}; let noCode=0;
      for(const c of create){
        let cc=null;
        while(true){
          cc=prompt(`Enter Client Code / UCC (Demat Account) for "${c.name}":\n(Required — numbers only, no letters. Cancel to skip this client.)`,'');
          if(cc===null) break; // skip this client
          cc=cc.trim();
          if(!cc) continue;
          if(!/^[0-9]+$/.test(cc)){ alert('Client Code must be numbers only (no letters)'); cc=null; continue; }
          break;
        }
        if(cc){ codeMap[c.id]=cc; } else { noCode++; }
      }
      create=create.filter(c=>codeMap[c.id]);
      if(!create.length){ toast('Nothing shifted — no Client Code entered for any client','error'); return; }
      this.confirm('Shift MF → Equity',
        `<b>${create.length}</b> MF client(s) will be added to Equity <i>(MF record remains — copy)</i>.`
        + (dup?`<br><span style="color:var(--gray)">${dup} already in Equity — skipped.</span>`:'')
        + (noCode?`<br><span style="color:var(--orange)">⚠️ ${noCode} skipped — no Client Code entered.</span>`:''),
        '📈 Shift','', async()=>{
          const recsNew=[], logs=[];
          create.forEach(c=>{ let id=uid(); while(eq.some(x=>x.id===id)||recsNew.some(x=>x.id===id)) id=uid();
            recsNew.push({id,code:codeMap[c.id],name:c.name,mobile:c.mobile||'',email:c.email||'',rm:c.rm||'',status:'Active',asset_value:null,revenue:null,last_trade_date:'',last_trade_month:'',last_call_date:c.last_call_date||'',next_call:'',followup_status:'',remarks:`Shifted from MF${c.pan?' (PAN: '+c.pan+')':''}`,created:today(),updated:today()});
            logs.push({id:uid(),type:'add',seg:'equity',client_id:id,client_name:c.name,rm:c.rm||'',by:CU.name,date:new Date().toISOString(),changes:[]}); });
          await DB.setClientsBulk('eq_clients',recsNew);
          if(logs.length) await DB.addActivityLog(logs);
          this._done('mf',`✅ ${recsNew.length} client(s) added to Equity (MF intact)`); renderEqTable();
        });

    } else { // Leads → Equity / MF (MOVE — lead delete after convert, like single convertLead)
      const toMf = target==='mf';
      const key = toMf?'mf_clients':'eq_clients';
      const existing = DB.get(key)||[];
      const mobSet=new Set(existing.map(c=>cleanMob(c.mobile)).filter(Boolean));
      const nameSet=new Set(existing.map(c=>(c.name||'').trim().toUpperCase()));
      const panSet=new Set(existing.filter(c=>c.pan).map(c=>c.pan.trim().toUpperCase()));
      let create=[], minorCount=0, dup=0;
      recs.forEach(c=>{
        if(toMf){ const pan=(c.pan||'').trim().toUpperCase();
          if(validPan(pan)){
            if(panSet.has(pan)){ dup++; return; } panSet.add(pan);
          } else { minorCount++; } // no valid PAN → will be added as Minor (guardian-operated)
        }
        if(mobSet.has(cleanMob(c.mobile)) || nameSet.has((c.name||'').trim().toUpperCase())){ dup++; return; }
        nameSet.add((c.name||'').trim().toUpperCase()); create.push(c);
      });
      if(!create.length){ toast(`Nothing converted — ${dup} duplicate`,'error'); return; }
      // Client Code is mandatory for Equity — ask per lead, skip any left blank (stays in Leads).
      const codeMap={}; let noCode=0;
      if(!toMf){
        for(const c of create){
          let cc=null;
          while(true){
            cc=prompt(`Enter Client Code / UCC (Demat Account) for "${c.name}":\n(Required — numbers only, no letters. Cancel to skip this lead.)`,'');
            if(cc===null) break; // skip this lead — stays in Leads
            cc=cc.trim();
            if(!cc) continue;
            if(!/^[0-9]+$/.test(cc)){ alert('Client Code must be numbers only (no letters)'); cc=null; continue; }
            break;
          }
          if(cc){ codeMap[c.id]=cc; } else { noCode++; }
        }
        create=create.filter(c=>codeMap[c.id]);
        if(!create.length){ toast('Nothing converted — no Client Code entered for any lead','error'); return; }
      }
      this.confirm(`Convert Lead → ${toMf?'MF':'Equity'}`,
        `<b>${create.length}</b> lead(s) will become ${toMf?'MF Investor':'Equity Client'}s <i>(removed from the lead list — move)</i>.`
        + (toMf&&minorCount?`<br><span style="color:var(--teal)">👶 ${minorCount} lead(s) without PAN — added as Minor.</span>`:'')
        + (dup?`<br><span style="color:var(--gray)">${dup} duplicate — skipped.</span>`:'')
        + (!toMf&&noCode?`<br><span style="color:var(--orange)">⚠️ ${noCode} skipped — no Client Code entered.</span>`:''),
        toMf?'🏦 Convert':'📈 Convert','', async()=>{
          const recsNew=[], logs=[], movedIds=[];
          create.forEach(c=>{ let id=uid(); while(existing.some(x=>x.id===id)||recsNew.some(x=>x.id===id)) id=uid();
            let rec = toMf
              ? {id,name:c.name,mobile:c.mobile||'',pan:(c.pan||'').trim().toUpperCase(),email:'',rm:c.rm||'',status:'Prospect',is_minor:!validPan((c.pan||'').trim().toUpperCase()),aum:null,sip_amount:null,sip_count:null,last_invest_date:'',last_call_date:'',next_call:c.next_call||'',followup_status:c.followup_status||'',remarks:c.remarks||'Converted from Lead',created:today(),updated:today()}
              : {id,code:codeMap[c.id],name:c.name,mobile:c.mobile||'',email:'',rm:c.rm||'',status:'Active',asset_value:null,revenue:null,last_trade_date:'',last_trade_month:'',last_call_date:'',next_call:c.next_call||'',followup_status:c.followup_status||'',remarks:c.remarks||'Converted from Lead',created:today(),updated:today()};
            recsNew.push(rec); movedIds.push(c.id);
            logs.push({id:uid(),type:'add',seg:toMf?'mf':'equity',client_id:id,client_name:c.name,rm:c.rm||'',by:CU.name,date:new Date().toISOString(),changes:[]}); });
          await DB.setClientsBulk(key,recsNew);
          await this._bulkDelete('leads',movedIds);
          if(logs.length) await DB.addActivityLog(logs);
          this._done('leads',`✅ ${recsNew.length} lead(s) converted to ${toMf?'MF':'Equity'}`);
          if(toMf) renderMfTable(); else renderEqTable();
        });
    }
  },

  // ── 4) DELETE ──
  del(){
    const seg=this.ensureBar().dataset.seg;
    const recs=this._selRecords(seg);
    if(!recs.length) return;
    this.confirm('Bulk Delete',
      `<b style="color:var(--red)">${recs.length}</b> ${this.LBL[seg]} record(s) will be <b>permanently deleted</b>. This cannot be undone.`,
      '🗑️ Delete','btn-danger', async()=>{
        const ids=recs.map(c=>c.id), logs=[];
        recs.forEach(c=>logs.push({id:uid(),type:'delete',seg:seg==='eq'?'equity':seg,client_id:c.id,client_name:c.name,rm:c.rm||'',by:CU.name,date:new Date().toISOString(),changes:[]}));
        await this._bulkDelete(this.KEY[seg],ids);
        if(logs.length) await DB.addActivityLog(logs);
        this._done(seg,`✅ ${ids.length} record(s) deleted`);
      });
  },

  // ── shared: bulk delete via transaction (no clobber) ──
  async _bulkDelete(key,ids){
    // Delegated to the DB layer so sharded keys (eq_clients) are handled
    // correctly — deleting only from the shards that actually hold the ids.
    await DB.deleteClientsBulk(key, ids);
  },

  _done(seg,msg){
    this.sel[seg].clear();
    this._rerender(seg);
    this.hideBar();
    refreshDash(); updateBadges();
    toast(msg,'success');
  }
};

/* ══════════════════════════════════════════════════════════════════════════
   MF TRANSACTIONS — BULK OPERATIONS  (Admin only)
   Select transactions via checkbox → Approve · Decline · Mark Pending ·
   Re-attribute RM · Delete. Works on the mf_business 'entries' store; all ops
   show a confirmation summary (reuses BULK.confirm) and write in one pass via
   setMfBizEntries (same merge-on-write helper the single-row actions use).
   ══════════════════════════════════════════════════════════════════════════ */
const MFTBULK = {
  sel: new Set(),
  isAdmin(){ return CU && CU.role==='admin'; },
  _esc(s){ return String(s==null?'':s).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); },
  filtered(){ try{ return getFilteredMfTxns(); }catch(e){ return []; } },

  th(){
    if(!this.isAdmin()) return '';
    return `<th style="width:36px;text-align:center" onclick="event.stopPropagation()">`
      + `<input type="checkbox" id="bulk-all-mftxn" title="Select all on this page" `
      + `onclick="event.stopPropagation()" onchange="MFTBULK.togglePage(this.checked)"></th>`;
  },
  td(id){
    if(!this.isAdmin()) return '';
    const ck=this.sel.has(id)?'checked':'';
    return `<td style="text-align:center"><input type="checkbox" class="mftxn-bulk-cb" data-id="${id}" ${ck} `
      + `onchange="MFTBULK.toggle('${id}',this.checked)"></td>`;
  },

  toggle(id,checked){ if(checked) this.sel.add(id); else this.sel.delete(id); this.syncHeader(); this.updateBar(); },
  togglePage(checked){
    document.querySelectorAll('#mftxn-table .mftxn-bulk-cb').forEach(cb=>{
      cb.checked=checked; const id=cb.getAttribute('data-id');
      if(checked) this.sel.add(id); else this.sel.delete(id);
    });
    this.updateBar();
  },
  selectAllFiltered(){ this.filtered().forEach(e=>this.sel.add(e.id)); renderMfTxnTable(); this.updateBar(); },
  clear(){ this.sel.clear(); renderMfTxnTable(); this.hideBar(); },
  clearSel(){ this.sel.clear(); this.hideBar(); },

  afterRender(){ if(!this.isAdmin()) return; this.syncHeader(); this.updateBar(); },
  syncHeader(){
    const box=document.getElementById('bulk-all-mftxn'); if(!box) return;
    const cbs=[...document.querySelectorAll('#mftxn-table .mftxn-bulk-cb')];
    const c=cbs.filter(cb=>cb.checked).length;
    box.checked = cbs.length>0 && c===cbs.length;
    box.indeterminate = c>0 && c<cbs.length;
  },
  rmList(){ try{ return getSegRMs('mf'); }catch(e){ return []; } },

  ensureBar(){
    let bar=document.getElementById('mftBulkBar');
    if(bar) return bar;
    bar=document.createElement('div'); bar.id='mftBulkBar'; bar.className='bulk-bar'; bar.style.display='none';
    document.body.appendChild(bar); return bar;
  },
  hideBar(){ const b=document.getElementById('mftBulkBar'); if(b) b.style.display='none'; },
  updateBar(){
    if(!this.isAdmin()) return;
    const bar=this.ensureBar();
    const n=this.sel.size;
    if(n===0){ bar.style.display='none'; return; }
    const totalFiltered=this.filtered().length;
    const rmOpts=this.rmList().map(r=>`<option value="${this._esc(r)}">${this._esc(r)}</option>`).join('');
    const selAll = n<totalFiltered
      ? `<a class="bb-link" onclick="MFTBULK.selectAllFiltered()">Select all ${totalFiltered}</a>` : '';
    bar.innerHTML=`
      <div class="bb-left"><span class="bb-count">${n} selected</span>${selAll}</div>
      <div class="bb-right">
        <button class="bb-btn bb-set" onclick="MFTBULK.approve()">✅ Approve</button>
        <button class="bb-btn bb-del" onclick="MFTBULK.decline()">❌ Decline</button>
        <button class="bb-btn bb-unmap" onclick="MFTBULK.pending()">↩️ Pending</button>
        <select id="mft-bb-rm" class="bb-sel"><option value="">— RM —</option>${rmOpts}</select>
        <button class="bb-btn bb-shift" onclick="MFTBULK.setRm()">⇅ Set RM</button>
        <button class="bb-btn bb-del" onclick="MFTBULK.del()">🗑️ Delete</button>
        <button class="bb-btn bb-close" onclick="MFTBULK.clear()">✕</button>
      </div>`;
    bar.style.display='flex';
  },

  _selEntries(){ return getMfBizEntries().filter(e=>this.sel.has(e.id)); },

  // Mutate every selected entry in one pass, write once, then refresh.
  _apply(mutate, doneMsg){
    const entries=getMfBizEntries();
    let count=0;
    entries.forEach(e=>{ if(this.sel.has(e.id)){ if(mutate(e)!==false) count++; } });
    setMfBizEntries(entries);
    if(document.getElementById('reportModal')?.classList.contains('open')) newBusinessMonthlyReport();
    this._done(doneMsg.replace('{n}', count));
  },

  approve(){
    const recs=this._selEntries(); if(!recs.length) return;
    const todo=recs.filter(e=>(e.status||'Pending')!=='Approved');
    if(!todo.length){ toast('All of these are already Approved','error'); return; }
    BULK.confirm('Bulk Approve',
      `<b>${todo.length}</b> transaction(s) will be Approved.`
      + (recs.length-todo.length?`<br><span style="color:var(--gray)">${recs.length-todo.length} already Approved — skip.</span>`:''),
      '✅ Approve','', async()=>{ this._apply(e=>{ if((e.status||'Pending')==='Approved') return false; e.status='Approved'; e.decline_reason=''; }, '✅ {n} transaction(s) approved'); });
  },
  decline(){
    const recs=this._selEntries(); if(!recs.length) return;
    const todo=recs.filter(e=>(e.status||'Pending')!=='Declined');
    if(!todo.length){ toast('All of these are already Declined','error'); return; }
    const reason=(prompt('Decline reason (same for all — optional):','')||'').trim();
    BULK.confirm('Bulk Decline',
      `<b>${todo.length}</b> transaction(s) will be Declined.`
      + (reason?`<br>Reason: <i>${this._esc(reason)}</i>`:'')
      + (recs.length-todo.length?`<br><span style="color:var(--gray)">${recs.length-todo.length} already Declined — skip.</span>`:''),
      '❌ Decline','btn-danger', async()=>{ this._apply(e=>{ if((e.status||'Pending')==='Declined') return false; e.status='Declined'; e.decline_reason=reason; }, '✅ {n} transaction(s) declined'); });
  },
  pending(){
    const recs=this._selEntries(); if(!recs.length) return;
    const todo=recs.filter(e=>(e.status||'Pending')!=='Pending');
    if(!todo.length){ toast('All of these are already Pending','error'); return; }
    BULK.confirm('Bulk Mark Pending',
      `<b>${todo.length}</b> transaction(s) will be moved back to Pending.`,
      '↩️ Pending','', async()=>{ this._apply(e=>{ if((e.status||'Pending')==='Pending') return false; e.status='Pending'; e.decline_reason=''; }, '✅ {n} transaction(s) set to Pending'); });
  },
  setRm(){
    const recs=this._selEntries(); if(!recs.length) return;
    const newRm=normRm((document.getElementById('mft-bb-rm')||{value:''}).value.trim());
    if(!newRm){ toast('Choose an RM first','error'); return; }
    const change=recs.filter(e=>(e.rm||'').trim().toLowerCase()!==newRm.toLowerCase());
    if(!change.length){ toast('In sabka RM already '+newRm+' hai','error'); return; }
    BULK.confirm('Bulk RM Re-attribute',
      `<b>${change.length}</b> transaction(s) will get RM <b>${this._esc(newRm)}</b>.`
      + `<br><span style="color:var(--gray)">Only the attribution of these transactions will change — the client&#39;s actual RM stays the same.</span>`
      + (recs.length-change.length?`<br><span style="color:var(--gray)">${recs.length-change.length} already ${this._esc(newRm)} — skip.</span>`:''),
      '⇅ Set RM','', async()=>{ this._apply(e=>{ if((e.rm||'').trim().toLowerCase()===newRm.toLowerCase()) return false; e.rm=newRm; }, '✅ {n} transaction(s) — RM set to '+newRm); });
  },
  del(){
    const recs=this._selEntries(); if(!recs.length) return;
    BULK.confirm('Bulk Delete',
      `<b style="color:var(--red)">${recs.length}</b> transaction(s) will be <b>permanently deleted</b>. This cannot be undone.`,
      '🗑️ Delete','btn-danger', async()=>{
        const idSet=new Set([...this.sel]);
        const entries=getMfBizEntries().filter(e=>!idSet.has(e.id));
        setMfBizEntries(entries);
        if(document.getElementById('reportModal')?.classList.contains('open')) newBusinessMonthlyReport();
        this._done('✅ '+idSet.size+' transaction(s) deleted');
      });
  },

  _done(msg){
    this.sel.clear();
    renderMfTxnTable();
    this.hideBar();
    if(typeof refreshDash==='function') refreshDash();
    if(typeof updateBadges==='function') updateBadges();
    toast(msg,'success');
  }
};

/* bulk bar + checkbox styling */
(function(){
  const css=`
  .bulk-bar{position:fixed;left:50%;transform:translateX(-50%);bottom:18px;z-index:480;
    display:flex;align-items:center;gap:14px;flex-wrap:wrap;
    background:var(--navy,#0f2c5c);color:#fff;padding:10px 16px;border-radius:14px;
    box-shadow:0 10px 30px rgba(0,0,0,.28);max-width:96vw}
  .bulk-bar .bb-left{display:flex;align-items:center;gap:10px}
  .bulk-bar .bb-right{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
  .bulk-bar .bb-count{font-weight:700;white-space:nowrap}
  .bulk-bar .bb-link{color:#7fd4ff;cursor:pointer;font-size:.82rem;text-decoration:underline;white-space:nowrap}
  .bulk-bar .bb-sel{padding:6px 8px;border-radius:8px;border:none;font-size:.85rem;max-width:130px}
  .bulk-bar .bb-btn{border:none;border-radius:9px;padding:7px 11px;font-size:.82rem;font-weight:600;
    cursor:pointer;background:#fff;color:var(--navy,#0f2c5c);white-space:nowrap}
  .bulk-bar .bb-btn:hover{opacity:.9}
  .bulk-bar .bb-set{background:#16a34a;color:#fff}
  .bulk-bar .bb-unmap{background:#f59e0b;color:#fff}
  .bulk-bar .bb-shift{background:#0ea5e9;color:#fff}
  .bulk-bar .bb-del{background:#dc2626;color:#fff}
  .bulk-bar .bb-close{background:transparent;color:#fff;font-size:1rem;padding:6px 9px}
  .bulk-cb,#bulk-all-eq,#bulk-all-mf,#bulk-all-leads,.mftxn-bulk-cb,#bulk-all-mftxn{width:16px;height:16px;cursor:pointer;accent-color:var(--navy,#0f2c5c)}
  @media(max-width:640px){.bulk-bar{bottom:8px;gap:8px;padding:9px 12px;font-size:.85rem}.bulk-bar .bb-btn{padding:6px 9px;font-size:.78rem}}
  `;
  const st=document.createElement('style'); st.textContent=css;
  (document.head||document.documentElement).appendChild(st);
})();

/* ──────────────────────────────────────────────────────────────
   APP ZOOM CONTROL
   Floating −/+ control (bottom-right) to shrink/enlarge the whole
   app so all columns fit in one window. Level is saved in
   localStorage so it persists across logins/reloads.
   Click the % label to reset to 100%.
   ────────────────────────────────────────────────────────────── */
(function(){
  const MIN=0.5, MAX=1.5, STEP=0.1, KEY='dnAppZoom';
  function clamp(z){ return Math.min(MAX, Math.max(MIN, Math.round(z*100)/100)); }
  function apply(z){
    document.body.style.zoom = z;            // Chrome/Edge: true zoom, layout reflows so columns fit
    const lbl=document.getElementById('dnZoomVal');
    if(lbl) lbl.textContent = Math.round(z*100)+'%';
  }
  window.dnAppZoom=function(delta, reset){
    let z=parseFloat(localStorage.getItem(KEY)||'1');
    z = reset ? 1 : clamp(z+delta);
    localStorage.setItem(KEY, z);
    apply(z);
  };
  function build(){
    if(document.getElementById('dnZoomBar')) return;
    const bar=document.createElement('div');
    bar.id='dnZoomBar';
    bar.innerHTML =
      '<button id="dnZoomOut" title="Zoom out (Ctrl -)">−</button>'+
      '<span id="dnZoomVal" title="Reset 100%">100%</span>'+
      '<button id="dnZoomIn" title="Zoom in (Ctrl +)">+</button>';
    const s=document.createElement('style');
    s.textContent=
      '#dnZoomBar{display:inline-flex;align-items:center;gap:3px;background:#1e3a6b;'+
      'padding:3px 6px;border-radius:30px;margin-right:6px;font-family:inherit;'+
      'box-shadow:0 1px 4px rgba(0,0,0,.25);flex-shrink:0;}'+
      '#dnZoomBar button{width:24px;height:24px;border:none;border-radius:50%;'+
      'background:#2d508f;color:#fff;font-size:16px;line-height:1;cursor:pointer;'+
      'display:flex;align-items:center;justify-content:center;transition:background .15s;}'+
      '#dnZoomBar button:hover{background:#3d63ad;}'+
      '#dnZoomBar #dnZoomVal{color:#fff;font-size:11px;font-weight:700;min-width:38px;'+
      'text-align:center;cursor:pointer;user-select:none;}'+
      '@media print{#dnZoomBar{display:none!important;}}'+
      '@keyframes dnSqBlink{0%,100%{opacity:1;box-shadow:0 0 0 0 rgba(220,38,38,.55)}50%{opacity:.72;box-shadow:0 0 0 5px rgba(220,38,38,0)}}'+
      '#dnSqBell{animation:dnSqBlink 1.5s ease-in-out infinite;}'+
      '#dnSqBell:hover{animation-play-state:paused;}';
    document.head.appendChild(s);
    var host=document.querySelector('.topbar-right');
    var anchor=document.getElementById('segBadge');
    if(host){ host.insertBefore(bar, anchor||host.firstChild); }
    else { document.body.appendChild(bar); }
    document.getElementById('dnZoomOut').onclick=()=>window.dnAppZoom(-STEP);
    document.getElementById('dnZoomIn').onclick =()=>window.dnAppZoom(STEP);
    document.getElementById('dnZoomVal').onclick=()=>window.dnAppZoom(0,true);
    apply(clamp(parseFloat(localStorage.getItem(KEY)||'1')));
    buildSqBell(host, bar);
  }
  function buildSqBell(host, bar){
    if(document.getElementById('dnSqBell')) return;
    const bell=document.createElement('button');
    bell.id='dnSqBell';
    bell.title='Square-off (T+5) alerts';
    bell.style.cssText='display:inline-flex;align-items:center;gap:5px;background:#dc2626;color:#fff;border:none;'+
      'border-radius:20px;padding:5px 12px;margin-right:6px;font-size:.75rem;font-weight:700;cursor:pointer;'+
      'flex-shrink:0;white-space:nowrap';
    bell.innerHTML='🔻 <span class="sq-bell-label">Square-off (T+5)</span> <span id="dnSqBellCount" style="background:rgba(255,255,255,.28);'+
      'border-radius:10px;padding:1px 7px;margin-left:2px">0</span>';
    bell.onclick=()=>{ if(typeof showPage==='function') showPage('eq-squareoff'); };
    // Insert to the LEFT of the zoom bar (i.e. before it in DOM order)
    if(host && bar){ host.insertBefore(bell, bar); }
    else if(host){ host.insertBefore(bell, host.firstChild); }
    else { document.body.appendChild(bell); }
    if(typeof sqUpdateBell==='function') sqUpdateBell();
  }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',build);
  else build();
  // Keyboard shortcuts: Ctrl/Cmd + / - / 0 (in-app, doesn't touch browser zoom)
  document.addEventListener('keydown',e=>{
    if(!(e.ctrlKey||e.metaKey)) return;
    if(e.key==='='||e.key==='+'){e.preventDefault();window.dnAppZoom(STEP);}
    else if(e.key==='-'||e.key==='_'){e.preventDefault();window.dnAppZoom(-STEP);}
    else if(e.key==='0'){e.preventDefault();window.dnAppZoom(0,true);}
  });
})();

// ══════════════════════════════════════════════════════════════
// OTHER PRODUCTS MODULE
// Segments: health | life | fd | lamf | las
// Firestore key: other_products  (array in crm_data)
// ══════════════════════════════════════════════════════════════

const OP_TYPES = {
  health: { label:'Health Insurance', icon:'🏥', amtLabel:'Annual Premium (₹)', provLabel:'Insurance Company' },
  life:   { label:'Life Insurance',   icon:'🛡️', amtLabel:'Annual Premium (₹)', provLabel:'Insurance Company' },
  gi_motor:  { label:'Motor Insurance',      icon:'🚗', amtLabel:'Annual Premium (₹)', provLabel:'Insurance Company', group:'General Insurance' },
  gi_home:   { label:'Home Insurance',       icon:'🏠', amtLabel:'Annual Premium (₹)', provLabel:'Insurance Company', group:'General Insurance' },
  gi_travel: { label:'Travel Insurance',     icon:'✈️', amtLabel:'Premium (₹)',         provLabel:'Insurance Company', group:'General Insurance' },
  gi_fire:   { label:'Fire Insurance',       icon:'🔥', amtLabel:'Annual Premium (₹)', provLabel:'Insurance Company', group:'General Insurance' },
  gi_marine: { label:'Marine Insurance',     icon:'🚢', amtLabel:'Annual Premium (₹)', provLabel:'Insurance Company', group:'General Insurance' },
  gi_cyber:  { label:'Cyber Insurance',      icon:'🔒', amtLabel:'Annual Premium (₹)', provLabel:'Insurance Company', group:'General Insurance' },
  gi_other:  { label:'Other Gen. Insurance', icon:'📋', amtLabel:'Annual Premium (₹)', provLabel:'Insurance Company', group:'General Insurance' },
  fd:     { label:'Corporate FD',     icon:'🏦', amtLabel:'FD Amount (₹)',       provLabel:'Company / Bank' },
  lamf:   { label:'LAMF',             icon:'📋', amtLabel:'Loan Amount (₹)',     provLabel:'Lender' },
  las:    { label:'LAS',              icon:'📄', amtLabel:'Loan Amount (₹)',     provLabel:'Lender' },
};

let _opActiveTab = 'all';

// ── DB helpers ──
function getOpEntries(){ return DB.get('other_products') || []; }

// RM-wise filtered entries (same logic as getMyEqClients)
function getMyOpEntries(){
  const all = getOpEntries();
  if(!CU || CU.role==='admin') return all;
  const dealers = [...new Set([
    ...(CU.eq_dealers||[CU.name]),
    ...(CU.mf_dealers||[CU.name])
  ])].map(d=>d.trim().toUpperCase());
  return all.filter(e=>dealers.includes((e.rm||'').trim().toUpperCase()));
}
function setOpEntries(arr){
  DB.set('other_products', arr);
}
async function saveOpToFirestore(entry){
  try{
    const arr = getOpEntries();
    const idx = arr.findIndex(x=>x.id===entry.id);
    if(idx>=0) arr[idx]=entry; else arr.push(entry);
    setOpEntries(arr);
    if(window.fdb){
      await window.fdb.collection('crm_data').doc('other_products').set({data: arr});
    }
  }catch(e){ console.error('OP save error',e); }
}
async function deleteOpFromFirestore(id){
  try{
    const arr = getOpEntries().filter(x=>x.id!==id);
    setOpEntries(arr);
    if(window.fdb){
      await window.fdb.collection('crm_data').doc('other_products').set({data: arr});
    }
  }catch(e){ console.error('OP delete error',e); }
}
function loadOpFromFirestore(){
  if(!window.fdb) return;
  window.fdb.collection('crm_data').doc('other_products').get().then(doc=>{
    if(doc.exists){
      const data = doc.data().data || [];
      setOpEntries(data);
    }
  }).catch(()=>{});
}

// ── Tab switch ──
function opSwitchTab(tab){
  _opActiveTab = tab;
  document.querySelectorAll('#op-tab-bar .op-tab-btn').forEach(el=>{
    el.classList.toggle('active', el.id==='op-tab-'+tab);
  });
  // Update active banner
  const cfg = OP_TYPES[tab]||{};
  const colors = {
    all:   {bg:'#f1f5f9',color:'#334155',border:'#94a3b8'},
    health:{bg:'#dcfce7',color:'#15803d',border:'#86efac'},
    life:  {bg:'#ede9fe',color:'#6d28d9',border:'#c4b5fd'},
    fd:    {bg:'#fef3c7',color:'#92400e',border:'#fcd34d'},
    lamf:  {bg:'#dbeafe',color:'#1e40af',border:'#93c5fd'},
    las:      {bg:'#fce7f3',color:'#9d174d',border:'#f9a8d4'},
    gi_motor: {bg:'#fff7ed',color:'#c2410c',border:'#fdba74'},
    gi_home:  {bg:'#f0fdf4',color:'#166534',border:'#86efac'},
    gi_travel:{bg:'#eff6ff',color:'#1d4ed8',border:'#93c5fd'},
    gi_fire:  {bg:'#fef2f2',color:'#b91c1c',border:'#fca5a5'},
    gi_marine:{bg:'#f0f9ff',color:'#0369a1',border:'#7dd3fc'},
    gi_cyber: {bg:'#faf5ff',color:'#7e22ce',border:'#d8b4fe'},
    gi_other: {bg:'#f8fafc',color:'#475569',border:'#94a3b8'},
  };
  const c = colors[tab]||{bg:'#f0f0f0',color:'#333',border:'#ddd'};
  const banner = document.getElementById('op-active-banner');
  if(banner){
    banner.style.background = c.bg;
    banner.style.color = c.color;
    banner.style.border = '2px solid '+c.border;
    banner.textContent = tab==='all' ? '📋 All Products' : (cfg.icon+' '+cfg.label);
  }
  renderOpTable();
}

// ── Populate RM filter ──
function populateOpRmFilter(){
  const sel = document.getElementById('op-rm-filter');
  if(!sel) return;
  // Show RM filter only for Admin
  if(!CU || CU.role!=='admin'){
    sel.style.display='none';
    return;
  }
  sel.style.display='';
  const rms = [...new Set(getMyOpEntries().map(e=>e.rm||'').filter(Boolean))].sort();
  const cur = sel.value;
  sel.innerHTML = '<option value="">All RMs</option>' + rms.map(r=>`<option${r===cur?' selected':''}>${r}</option>`).join('');
}

// ── Main page render ──
function renderOpPage(){
  loadOpFromFirestore();
  populateOpRmFilter();
  populateOpModalRm();
  opSwitchTab(_opActiveTab||'health');
  updateOpBadge();
}

function renderOpTable(){
  const wrap = document.getElementById('op-table');
  if(!wrap) return;

  let data = _opActiveTab==='all' ? getMyOpEntries() : getMyOpEntries().filter(e=>e.product_type===_opActiveTab);

  const q = (document.getElementById('op-search')||{value:''}).value.toLowerCase();
  const rmF = (document.getElementById('op-rm-filter')||{value:''}).value;
  const stF = (document.getElementById('op-status-filter')||{value:''}).value;
  const fuF = (document.getElementById('op-fu-filter')||{value:''}).value;

  if(q) data = data.filter(e=>
    (e.client_name||'').toLowerCase().includes(q) ||
    (e.mobile||'').includes(q) ||
    (e.rm||'').toLowerCase().includes(q) ||
    (e.provider||'').toLowerCase().includes(q)
  );
  if(rmF) data = data.filter(e=>e.rm===rmF);
  if(stF) data = data.filter(e=>e.status===stF);
  if(fuF==='today') data = data.filter(e=>e.next_call===today());
  else if(fuF==='overdue') data = data.filter(e=>e.next_call&&e.next_call<today());
  else if(fuF==='pending') data = data.filter(e=>e.next_call||e.followup_status);

  const cnt = document.getElementById('op-count');
  if(cnt) cnt.textContent = data.length + ' entries';

  const cfg = OP_TYPES[_opActiveTab];

  if(!data.length){
    const lbl = _opActiveTab==='all' ? 'product' : cfg.label;
    wrap.innerHTML = `<div style="padding:40px;text-align:center;color:var(--gray)">
      No ${lbl} entries yet. <a href="#" onclick="openAddOpModal()" style="color:var(--teal)">Add one?</a>
    </div>`;
    return;
  }

  // Sort: overdue first, then by next_call, then by name
  data.sort((a,b)=>{
    const ao=a.next_call&&a.next_call<today(), bo=b.next_call&&b.next_call<today();
    if(ao&&!bo) return -1; if(!ao&&bo) return 1;
    if(a.next_call&&b.next_call) return a.next_call.localeCompare(b.next_call);
    return (a.client_name||'').localeCompare(b.client_name||'');
  });

  const isAll = _opActiveTab==='all';
  let h = `<table><thead><tr>
    <th>Client</th><th>Mobile</th><th>RM</th>
    ${isAll ? '<th>Product</th>' : ''}
    <th>${isAll ? 'Amount' : cfg.amtLabel.replace(' (₹)','')}</th>
    <th>${isAll ? 'Provider' : cfg.provLabel}</th>
    <th>Date</th><th>Next Call</th><th>Follow-up</th><th>Status</th><th>Remarks</th>
    <th>Actions</th>
  </tr></thead><tbody>`;

  const tabColors = {
    health:{bg:'#f0fdf4',border:'#bbf7d0'},life:{bg:'#f5f3ff',border:'#ddd6fe'},
    fd:{bg:'#fffbeb',border:'#fde68a'},lamf:{bg:'#eff6ff',border:'#bfdbfe'},las:{bg:'#fdf2f8',border:'#fbcfe8'},
    gi_motor:{bg:'#fff7ed',border:'#fed7aa'},gi_home:{bg:'#f0fdf4',border:'#bbf7d0'},
    gi_travel:{bg:'#eff6ff',border:'#bfdbfe'},gi_fire:{bg:'#fef2f2',border:'#fecaca'},
    gi_marine:{bg:'#f0f9ff',border:'#bae6fd'},gi_cyber:{bg:'#faf5ff',border:'#e9d5ff'},
    gi_other:{bg:'#f8fafc',border:'#e2e8f0'}
  };
  const tc = tabColors[_opActiveTab]||{};
  data.forEach(e=>{
    const overdue = e.next_call && e.next_call<today();
    const isToday = e.next_call===today();
    const fuBadge = overdue?'b-pending':isToday?'b-active':'b-na';
    const srcBadge = e.source_seg==='equity'
      ? '<span style="font-size:.68rem;background:var(--gold3,#fef3c7);color:var(--orange,#d97706);border-radius:4px;padding:1px 5px">EQ</span>'
      : e.source_seg==='mf'
      ? '<span style="font-size:.68rem;background:var(--teal2,#ccfbf1);color:var(--teal,#0d9488);border-radius:4px;padding:1px 5px">MF</span>'
      : '';
    const ptCfg = OP_TYPES[e.product_type]||{};
    const ptColors = {health:'#dcfce7',life:'#ede9fe',fd:'#fef3c7',lamf:'#dbeafe',las:'#fce7f3',gi_motor:'#fff7ed',gi_home:'#dcfce7',gi_travel:'#dbeafe',gi_fire:'#fef2f2',gi_marine:'#e0f2fe',gi_cyber:'#f3e8ff',gi_other:'#f1f5f9'};
    const ptText   = {health:'#15803d',life:'#6d28d9',fd:'#92400e',lamf:'#1e40af',las:'#9d174d',gi_motor:'#c2410c',gi_home:'#166534',gi_travel:'#1d4ed8',gi_fire:'#b91c1c',gi_marine:'#0369a1',gi_cyber:'#7e22ce',gi_other:'#475569'};
    const rowBg = isAll ? (ptColors[e.product_type]||'#f9f9f9') : tc.bg;
    h += `<tr class="${overdue?'row-alert':''}" style="${overdue?'':'background:'+rowBg}">
      <td><b>${e.client_name||'—'}</b> ${srcBadge}</td>
      <td><a href="tel:${e.mobile}" style="color:var(--navy);text-decoration:none">${e.mobile||'—'}</a></td>
      <td>${e.rm||'—'}</td>
      ${isAll ? `<td><span style="font-size:.75rem;font-weight:700;background:${ptColors[e.product_type]||'#eee'};color:${ptText[e.product_type]||'#333'};border-radius:4px;padding:2px 7px">${ptCfg.icon||''} ${ptCfg.label||e.product_type}</span></td>` : ''}
      <td>${e.amount?'₹'+fmtNum(e.amount):'—'}</td>
      <td style="font-size:.82rem">${e.provider||'—'}</td>
      <td>${fmtDate(e.date)||'—'}</td>
      <td>${fmtDate(e.next_call)||'—'}</td>
      <td><span class="badge ${fuBadge}">${e.followup_status||'—'}</span></td>
      <td><span class="badge ${e.status==='Active'?'b-active':e.status==='Closed'?'b-closed':'b-na'}">${e.status||'—'}</span></td>
      <td style="max-width:100px;overflow:hidden;text-overflow:ellipsis;font-size:.8rem" title="${e.remarks||''}">${e.remarks||'—'}</td>
      <td style="white-space:nowrap">
        <button class="btn-icon" onclick="editOpEntry('${e.id}')" title="Edit">✏️</button>
        <button class="btn-icon" onclick="viewOpEntry('${e.id}')" title="View">👁</button>
        ${e.mobile?`<a href="https://wa.me/91${e.mobile}" target="_blank" class="btn-icon" title="WhatsApp">💬</a>`:''}
        <button class="btn-icon" onclick="deleteOpEntry('${e.id}')" title="Delete" style="color:var(--red)">🗑️</button>
      </td>
    </tr>`;
  });
  h += '</tbody></table>';
  wrap.innerHTML = h;
}

// ── Badge ──
function updateOpBadge(){
  const entries = getMyOpEntries();
  const overdue = entries.filter(e=>e.next_call&&e.next_call<today()).length;
  const badge = document.getElementById('badge-op');
  if(badge){ badge.textContent=overdue>0?overdue:''; badge.style.display=overdue>0?'':'none'; }
}

// ── Modal open/close ──
function openAddOpModal(prefillType){
  document.getElementById('op-edit-id').value = '';
  document.getElementById('op-modal-title').textContent = 'Add Other Product Entry';
  document.getElementById('op-type').value = prefillType || _opActiveTab;
  document.getElementById('op-client-name').value = '';
  document.getElementById('op-client-id').value = '';
  document.getElementById('op-client-seg').value = '';
  document.getElementById('op-mobile').value = '';
  const _md=document.getElementById('op-mobile-display'); if(_md) _md.textContent='—';
  const _sd=document.getElementById('op-seg-display'); if(_sd) _sd.innerHTML='';
  document.getElementById('op-amount').value = '';
  document.getElementById('op-provider').value = '';
  document.getElementById('op-date').value = today();
  document.getElementById('op-next-call').value = '';
  document.getElementById('op-followup-status').value = '';
  document.getElementById('op-status').value = 'Active';
  document.getElementById('op-remarks').value = '';
  populateOpModalRm();
  opTypeChanged();
  document.getElementById('op-modal').classList.add('open');
}

function closeOpModal(){
  document.getElementById('op-modal').classList.remove('open');
}

function opTypeChanged(){
  const t = document.getElementById('op-type').value;
  const cfg = OP_TYPES[t]||OP_TYPES.health;
  document.getElementById('op-field-amount-label').textContent = cfg.amtLabel;
  document.getElementById('op-field-provider-label').textContent = cfg.provLabel;
  // Update modal header color
  const hdrColors = {
    health:{bg:'#15803d',sub:'Health Insurance'},
    life:  {bg:'#6d28d9',sub:'Life Insurance'},
    fd:    {bg:'#92400e',sub:'Corporate FD'},
    lamf:  {bg:'#1e40af',sub:'LAMF — Loan Against MF'},
    las:      {bg:'#9d174d',sub:'LAS — Loan Against Securities'},
    gi_motor: {bg:'#c2410c',sub:'Motor Insurance'},
    gi_home:  {bg:'#166534',sub:'Home Insurance'},
    gi_travel:{bg:'#1d4ed8',sub:'Travel Insurance'},
    gi_fire:  {bg:'#b91c1c',sub:'Fire Insurance'},
    gi_marine:{bg:'#0369a1',sub:'Marine Insurance'},
    gi_cyber: {bg:'#7e22ce',sub:'Cyber Insurance'},
    gi_other: {bg:'#475569',sub:'General Insurance — Other'},
  };
  const hc = hdrColors[t]||{bg:'var(--navy)',sub:''};
  const hdr = document.getElementById('op-modal-hdr');
  if(hdr) hdr.style.background = hc.bg;
  const icon = document.getElementById('op-modal-icon');
  if(icon) icon.textContent = cfg.icon;
  const sub = document.getElementById('op-modal-subtitle');
  if(sub) sub.textContent = hc.sub;
}

function populateOpModalRm(){
  const sel = document.getElementById('op-rm');
  if(!sel) return;
  const users = DB.get('users')||[];
  const cur = sel.value;
  // Admin sees all RMs; RM sees only themselves
  if(CU && CU.role==='admin'){
    const rms = users.filter(u=>u.role==='rm'||u.role==='admin').map(u=>u.name).sort();
    sel.innerHTML = '<option value="">— Select RM —</option>' + rms.map(r=>`<option${r===cur?' selected':''}>${r}</option>`).join('');
    sel.disabled = false;
  } else {
    sel.innerHTML = `<option value="${CU.name}" selected>${CU.name}</option>`;
    sel.disabled = true;
  }
  if(!cur && CU && (CU.role==='rm'||CU.role==='mfdesk')) sel.value = CU.name;
}

// ── Save ──
async function saveOpEntry(){
  const id = document.getElementById('op-edit-id').value;
  const clientName = document.getElementById('op-client-name').value.trim();
  const rm = document.getElementById('op-rm').value;
  const type = document.getElementById('op-type').value;
  if(!clientName){ toast('Client name required','error'); return; }
  if(!rm){ toast('RM required','error'); return; }

  // ── Duplicate check: same client_id OR same PAN + same product_type (skip on edit) ──
  if(!id){
    const clientId = document.getElementById('op-client-id').value;
    const clientSeg = document.getElementById('op-client-seg').value;
    // Get PAN of selected client
    let clientPan = '';
    if(clientId && clientSeg){
      const srcList = DB.get(clientSeg==='equity'?'eq_clients':'mf_clients')||[];
      const srcClient = srcList.find(x=>x.id===clientId);
      if(srcClient) clientPan = (srcClient.pan||'').toUpperCase().trim();
    }
    const existing = getOpEntries().find(e=>{
      if(e.product_type!==type) return false;
      if(clientId && e.client_id===clientId) return true;
      if(clientPan && (e.pan||'').toUpperCase().trim()===clientPan) return true;
      if(!clientId && (e.client_name||'').trim().toUpperCase()===(clientName||'').trim().toUpperCase()) return true;
      return false;
    });
    if(existing){
      const cfg2 = OP_TYPES[type]||{};
      toast(`⚠️ "${existing.client_name}" already has a ${cfg2.label} entry (RM: ${existing.rm||'—'})! Edit existing instead.`,'error');
      return;
    }
  }

  // Get PAN for cross-RM duplicate detection
  const _cid2 = document.getElementById('op-client-id').value;
  const _cseg2 = document.getElementById('op-client-seg').value;
  let _pan2 = '';
  if(_cid2 && _cseg2){
    const _sl2 = DB.get(_cseg2==='equity'?'eq_clients':'mf_clients')||[];
    const _sc2 = _sl2.find(x=>x.id===_cid2);
    if(_sc2) _pan2 = (_sc2.pan||'').toUpperCase().trim();
  }

  const entry = {
    id: id || uid(),
    product_type: type,
    client_name: clientName,
    client_id: _cid2,
    source_seg: _cseg2,
    pan: _pan2,
    mobile: document.getElementById('op-mobile').value,
    rm,
    amount: parseFloat(document.getElementById('op-amount').value)||0,
    provider: document.getElementById('op-provider').value.trim(),
    date: document.getElementById('op-date').value,
    next_call: document.getElementById('op-next-call').value,
    followup_status: document.getElementById('op-followup-status').value,
    status: document.getElementById('op-status').value,
    remarks: document.getElementById('op-remarks').value.trim(),
    updated: today(),
    created: id ? (getOpEntries().find(x=>x.id===id)||{}).created||today() : today(),
  };
  // Closed → RM list se hata do
  if(entry.status==='Closed') entry.rm='';

  await saveOpToFirestore(entry);
  closeOpModal();
  toast(`✅ ${OP_TYPES[type].label} entry ${id?'updated':'saved'}!`, 'success');
  populateOpRmFilter();
  renderOpTable();
  updateOpBadge();
}

// ── Edit ──
function editOpEntry(id){
  const e = getOpEntries().find(x=>x.id===id);
  if(!e){ toast('Entry not found','error'); return; }
  document.getElementById('op-edit-id').value = e.id;
  document.getElementById('op-modal-title').textContent = 'Edit Entry';
  document.getElementById('op-type').value = e.product_type;
  document.getElementById('op-client-name').value = e.client_name||'';
  document.getElementById('op-client-id').value = e.client_id||'';
  document.getElementById('op-client-seg').value = e.source_seg||'';
  document.getElementById('op-mobile').value = e.mobile||'';
  const _emd=document.getElementById('op-mobile-display'); if(_emd) _emd.textContent=e.mobile||'—';
  const _esd=document.getElementById('op-seg-display');
  if(_esd && e.source_seg){
    _esd.innerHTML = e.source_seg==='equity'
      ? '<span style="background:#fef3c7;color:#92400e;border-radius:4px;padding:1px 6px;font-weight:700">EQ</span>'
      : '<span style="background:#ccfbf1;color:#0d9488;border-radius:4px;padding:1px 6px;font-weight:700">MF</span>';
  }
  document.getElementById('op-amount').value = e.amount||'';
  document.getElementById('op-provider').value = e.provider||'';
  document.getElementById('op-date').value = e.date||'';
  document.getElementById('op-next-call').value = e.next_call||'';
  document.getElementById('op-followup-status').value = e.followup_status||'';
  document.getElementById('op-status').value = e.status||'Active';
  document.getElementById('op-remarks').value = e.remarks||'';
  populateOpModalRm();
  document.getElementById('op-rm').value = e.rm||'';
  opTypeChanged();
  document.getElementById('op-modal').classList.add('open');
}

// ── View ──
function viewOpEntry(id){
  const e = getOpEntries().find(x=>x.id===id);
  if(!e) return;
  const cfg = OP_TYPES[e.product_type]||OP_TYPES.health;
  alert(`${cfg.icon} ${cfg.label}\n\nClient: ${e.client_name}\nRM: ${e.rm}\nMobile: ${e.mobile||'—'}\nAmount: ${e.amount?'₹'+fmtNum(e.amount):'—'}\nProvider: ${e.provider||'—'}\nDate: ${fmtDate(e.date)||'—'}\nNext Call: ${fmtDate(e.next_call)||'—'}\nFollow-up: ${e.followup_status||'—'}\nStatus: ${e.status||'—'}\nRemarks: ${e.remarks||'—'}`);
}

// ── Delete ──
async function deleteOpEntry(id){
  if(!confirm('Delete this entry?')) return;
  await deleteOpFromFirestore(id);
  toast('Deleted','success');
  renderOpTable();
  updateOpBadge();
}

// ── Quick add from client row (🎯 button) ──
function quickAddOpFromClient(clientId, seg){
  // Get client data
  const clients = seg==='equity' ? (DB.get('eq_clients')||[]) : (DB.get('mf_clients')||[]);
  const client = clients.find(c=>c.id===clientId);
  if(!client){ toast('Client not found','error'); return; }

  openAddOpModal();

  // Prefill client fields
  document.getElementById('op-client-name').value = client.name||'';
  document.getElementById('op-client-id').value = client.id;
  document.getElementById('op-client-seg').value = seg;
  document.getElementById('op-mobile').value = client.mobile||'';
  document.getElementById('op-rm').value = client.rm||'';
  const mobDisp2 = document.getElementById('op-mobile-display');
  if(mobDisp2) mobDisp2.textContent = client.mobile||'—';
  const segDisp2 = document.getElementById('op-seg-display');
  if(segDisp2){
    segDisp2.innerHTML = seg==='equity'
      ? '<span style="background:#fef3c7;color:#92400e;border-radius:4px;padding:1px 6px;font-weight:700">EQ</span>'
      : '<span style="background:#ccfbf1;color:#0d9488;border-radius:4px;padding:1px 6px;font-weight:700">MF</span>';
  }

  // Navigate to other-products tab context
  _opActiveTab = _opActiveTab || 'health';
  document.getElementById('op-type').value = _opActiveTab;
  opTypeChanged();
}

// ── Client Picker ──
function openOpClientPicker(){
  document.getElementById('op-picker-search').value = '';
  document.getElementById('op-picker-seg').value = '';
  renderOpPicker();
  document.getElementById('op-picker-modal').classList.add('open');
}
function closeOpPicker(){
  document.getElementById('op-picker-modal').classList.remove('open');
}
function renderOpPicker(){
  const q = (document.getElementById('op-picker-search')||{value:''}).value.toLowerCase();
  const seg = (document.getElementById('op-picker-seg')||{value:''}).value;
  const list = document.getElementById('op-picker-list');
  if(!list) return;

  let eqClients = seg==='' || seg==='equity' ? getMyEqClients().map(c=>({...c,_seg:'equity'})) : [];
  let mfClients = seg==='' || seg==='mf' ? getMyMfClients().map(c=>({...c,_seg:'mf'})) : [];
  let all = [...eqClients, ...mfClients];

  if(q) all = all.filter(c=>
    (c.name||'').toLowerCase().includes(q)||
    (c.mobile||'').includes(q)||
    (c.pan||'').toLowerCase().includes(q)
  );
  all = all.slice(0,80);

  if(!all.length){ list.innerHTML='<div style="padding:20px;text-align:center;color:var(--gray)">No clients found</div>'; return; }

  // Build set of already-added PANs + client_ids across ALL RMs (any product type)
  const opEntries = getOpEntries();
  const currentProductType = document.getElementById('op-type') ? document.getElementById('op-type').value : '';
  const currentEditId = document.getElementById('op-edit-id') ? document.getElementById('op-edit-id').value : '';
  // PAN-based: collect all PANs already in other_products for this product type
  const addedPans = new Set();
  const addedIds  = new Set();
  opEntries.forEach(e=>{
    if(e.id === currentEditId) return; // skip current entry being edited
    if(currentProductType && e.product_type !== currentProductType) return; // only same product type
    if(e.pan)  addedPans.add((e.pan||'').toUpperCase().trim());
    if(e.client_id) addedIds.add(e.client_id);
  });

  // Also get PANs from eq/mf client records for cross-reference
  const allEqClients = DB.get('eq_clients')||[];
  const allMfClients = DB.get('mf_clients')||[];

  list.innerHTML = all.map(c=>{
    const badge = c._seg==='equity'
      ? '<span style="font-size:.68rem;background:var(--gold3,#fef3c7);color:var(--orange,#d97706);border-radius:4px;padding:1px 6px;margin-left:4px">EQ</span>'
      : '<span style="font-size:.68rem;background:var(--teal2,#ccfbf1);color:var(--teal,#0d9488);border-radius:4px;padding:1px 6px;margin-left:4px">MF</span>';

    // Check if already added: by client_id OR by PAN match across all clients
    const cPan = (c.pan||'').toUpperCase().trim();
    const alreadyById  = addedIds.has(c.id);
    const alreadyByPan = cPan && addedPans.has(cPan);
    const isAlready = alreadyById || alreadyByPan;

    // Find which RM added this client
    let addedByRm = '';
    if(isAlready){
      const matchEntry = opEntries.find(e=>{
        if(currentProductType && e.product_type !== currentProductType) return false;
        if(e.client_id===c.id) return true;
        if(cPan && (e.pan||'').toUpperCase().trim()===cPan) return true;
        return false;
      });
      if(matchEntry) addedByRm = matchEntry.rm || '';
    }

    if(isAlready){
      return `<div style="padding:10px 12px;border-bottom:1px solid #f0f0f0;display:flex;justify-content:space-between;align-items:center;opacity:.55;cursor:not-allowed;background:#f8f9fa">
        <div>
          <b>${c.name}</b>${badge}
          <div style="font-size:.78rem;color:var(--gray)">${c.mobile||'—'} &nbsp;•&nbsp; RM: ${c.rm||'—'}</div>
        </div>
        <span style="font-size:.7rem;background:#e2e8f0;color:#64748b;border-radius:6px;padding:2px 8px;white-space:nowrap">✅ Already added${addedByRm?' by '+addedByRm:''}</span>
      </div>`;
    }

    return `<div onclick="selectOpClient('${c.id}','${c._seg}')"
      style="padding:10px 12px;cursor:pointer;border-bottom:1px solid #f0f0f0;display:flex;justify-content:space-between;align-items:center"
      onmouseover="this.style.background='var(--bg)'" onmouseout="this.style.background=''">
      <div>
        <b>${c.name}</b>${badge}
        <div style="font-size:.78rem;color:var(--gray)">${c.mobile||'—'} &nbsp;•&nbsp; RM: ${c.rm||'—'}</div>
      </div>
    </div>`;
  }).join('');
}
function selectOpClient(clientId, seg){
  const clients = seg==='equity' ? (DB.get('eq_clients')||[]) : (DB.get('mf_clients')||[]);
  const c = clients.find(x=>x.id===clientId);
  if(!c) return;
  applyCallLimitsTo('op-next-call','nc');
  document.getElementById('op-client-name').value = c.name||'';
  document.getElementById('op-client-id').value = c.id;
  document.getElementById('op-client-seg').value = seg;
  document.getElementById('op-mobile').value = c.mobile||'';
  // Update display fields
  const mobDisp = document.getElementById('op-mobile-display');
  if(mobDisp) mobDisp.textContent = c.mobile||'—';
  const segDisp = document.getElementById('op-seg-display');
  if(segDisp){
    segDisp.innerHTML = seg==='equity'
      ? '<span style="background:#fef3c7;color:#92400e;border-radius:4px;padding:1px 6px;font-weight:700">EQ</span>'
      : '<span style="background:#ccfbf1;color:#0d9488;border-radius:4px;padding:1px 6px;font-weight:700">MF</span>';
  }
  if(!document.getElementById('op-rm').value) document.getElementById('op-rm').value = c.rm||'';
  closeOpPicker();
}

// Load on app start
window.addEventListener('load', ()=>{ setTimeout(()=>{ loadOpFromFirestore(); updateOpBadge(); }, 3000); });


// ══════════════════════════════════════════
// BROKERAGE CALCULATOR (Tools > Brokerage Calculator)
// Segment brokerage "shape": futures/commodity_futures=%, options/commodity_options=flat ₹/lot,
// equity_delivery/equity_intraday=% with a minimum ₹/share floor (whichever is higher).
// Statutory rates (STT/CTT, Stamp, Txn Charges, SEBI, GST) are fixed per segment and read-only —
// only the Brokerage Rate is editable by the RM.
// ══════════════════════════════════════════
var BC_SEG_BROK_TYPE = { futures:'percent', equity_delivery:'pct_min', equity_intraday:'pct_min', options:'flat',
                          commodity_futures:'percent', commodity_options:'flat',
                          currency_futures:'percent', currency_options:'flat' };

// Only F&O and Commodity segments trade in lots — Equity Delivery/Intraday use a plain quantity.
var BC_USES_LOTS = { futures:true, options:true, commodity_futures:true, commodity_options:true, currency_futures:true, currency_options:true };

var BC_SEGMENT_DEFAULTS = {
  futures:          { stt:0.05,  txn:0.0019, stamp:0.002, sebi:0.0001, sttSide:'sell', stampSide:'buy', brokPct:0.03 },
  options:          { stt:0.15,  txn:0.0355, stamp:0.003, sebi:0.0001, sttSide:'sell', stampSide:'buy', brokFlat:30 },
  equity_delivery:  { stt:0.10,  txn:0.00297,stamp:0.015, sebi:0.0001, sttSide:'both', stampSide:'buy', brokPct:0.3,  brokMin:0.03 },
  equity_intraday:  { stt:0.025, txn:0.00297,stamp:0.003, sebi:0.0001, sttSide:'sell', stampSide:'buy', brokPct:0.03, brokMin:0.03 },
  commodity_futures:{ stt:0.05,  txn:0.0021, stamp:0.002, sebi:0.0001, sttSide:'sell', stampSide:'buy', brokPct:0.03 },
  commodity_options:{ stt:0.05,  txn:0.00418,stamp:0.003, sebi:0.0001, sttSide:'sell', stampSide:'buy', brokFlat:30 },
  currency_futures: { stt:0,     txn:0.00005,stamp:0.0001,sebi:0.0001, sttSide:'sell', stampSide:'buy', brokPct:0.03 },   // STT doesn't apply to currency derivatives
  currency_options: { stt:0,     txn:0.002,  stamp:0.0001,sebi:0.0001, sttSide:'sell', stampSide:'buy', brokFlat:30 }     // STT doesn't apply to currency derivatives
};
// True original defaults (the "3" series) — never mutated. "Apply To All Segments" changes BC_SEGMENT_DEFAULTS
// for the session, but "Reset" always comes back to this untouched baseline.
var BC_ORIGINAL_DEFAULTS = JSON.parse(JSON.stringify(BC_SEGMENT_DEFAULTS));

var bcCurSeg = 'futures';
var bcLastCalc = {};
var bcAllSegmentsApplied = false;   // true right after "Apply To All Segments" — next "Save Rate" saves every segment

function bcUpdateBrokFields(){
  var shape = BC_SEG_BROK_TYPE[bcCurSeg];
  document.getElementById('bcBrokPctWrap').style.display = (shape==='percent'||shape==='pct_min') ? '' : 'none';
  document.getElementById('bcBrokMinWrap').style.display = (shape==='pct_min') ? '' : 'none';
  document.getElementById('bcBrokFlatWrap').style.display = (shape==='flat') ? '' : 'none';
  document.getElementById('bcBrokPctLabel').textContent = (shape==='pct_min') ? 'Brokerage Rate (%)' : 'Brokerage Rate (%) — Editable';
  document.getElementById('bcBrokHint').textContent = (shape==='pct_min')
    ? 'Whichever amount is higher applies — the percentage rate or the minimum per-share charge — calculated separately for each leg.' : '';
}

function bcUpdateQtyFields(){
  var usesLots = !!BC_USES_LOTS[bcCurSeg];
  document.getElementById('bcLotSizeWrap').style.display = usesLots ? '' : 'none';
  document.getElementById('bcLotsWrap').style.display = usesLots ? '' : 'none';
  document.getElementById('bcQtyPlainWrap').style.display = usesLots ? 'none' : '';
  document.getElementById('bcTotalQtyWrap').style.display = usesLots ? '' : 'none';
}

// Statutory rates are read-only for RM/staff; Admin can override them (e.g. after an exchange rate revision).
function bcApplyRoleAccess(){
  var isAdmin = (typeof CU!=='undefined' && CU && CU.role==='admin');
  ['bcSttRate','bcTxnRate','bcStampRate','bcSebiRate','bcGstRate'].forEach(function(id){
    var el = document.getElementById(id); if(!el) return;
    if(isAdmin) el.removeAttribute('readonly'); else el.setAttribute('readonly','readonly');
  });
  var lbl = document.getElementById('bcAdvToggleLbl');
  if(lbl) lbl.textContent = isAdmin ? '⚙️ View / Edit Statutory Rates (Admin)' : '⚙️ View Statutory Rates';
  var footer = document.getElementById('bcFooterNote');
  if(footer) footer.textContent = isAdmin
    ? 'Statutory rates (STT/CTT, Stamp Duty, Transaction Charges, SEBI Fees, GST) can be edited by Admin only — for example, after an exchange rate revision. RMs see these as fixed, auto-applied values.'
    : 'Statutory rates (STT/CTT, Stamp Duty, Transaction Charges, SEBI Fees, GST) are fixed by exchange and government rules, and are applied automatically for each segment. Only the Brokerage Rate needs to be entered.';
}

// "Locked prefix" rate control — RM can only change the decimal digits after "0." (e.g. 03, 3, or 35),
// so the leading "0." can't be accidentally deleted or fat-fingered into a whole-number rate.
function bcAutoLockChars(value){
  var str = String(value);
  if(str.indexOf('0.0')===0) return 3;   // e.g. 0.03, 0.035 → lock "0.0"
  if(str.indexOf('0.')===0) return 2;    // e.g. 0.3, 0.1  → lock "0."
  return 0;
}
function bcSetLockedRate(hiddenId, prefixId, digitId, value, lockChars){
  lockChars = lockChars || bcAutoLockChars(value);
  var str = String(value);
  var prefix, digits;
  if(str.indexOf('0.')===0 && str.length>=lockChars){ prefix=str.slice(0,lockChars); digits=str.slice(lockChars); }
  else if(str.indexOf('0.')===0){ prefix=str; digits=''; }
  else { prefix=''; digits=str; }   // fallback for a rate ≥ 1 (not expected for brokerage %, but stays safe)
  if(lockChars===2 && digits.length===1) digits += '0';   // e.g. 0.3 → shows as "0." + "30" = 0.30, same value, clearer display
  document.getElementById(prefixId).textContent = prefix;
  document.getElementById(digitId).value = digits;
  document.getElementById(hiddenId).value = value;
  // Range-warning bounds ALWAYS reference the true original ("3" series) default for this segment/field —
  // never the current session default (which "Apply To All Segments" may have changed) or a client's saved rate.
  var digitEl = document.getElementById(digitId);
  var origVal = (hiddenId==='bcBrokMinShare') ? BC_ORIGINAL_DEFAULTS[bcCurSeg].brokMin : BC_ORIGINAL_DEFAULTS[bcCurSeg].brokPct;
  var origPrefix = String(origVal).slice(0, bcAutoLockChars(origVal));
  digitEl.dataset.defaultVal = origVal;
  digitEl.dataset.floorVal = parseFloat(origPrefix + '1') || 0;
  bcSetWarnBox(digitId.replace(/Digit$/,'') + 'Warn', '');
}

// Solid red square alert that sits right beside the brokerage box — shows/hides based on whether msg is non-empty.
function bcSetWarnBox(warnId, msg){
  var el = document.getElementById(warnId);
  if(!el) return;
  el.textContent = msg;
  el.classList.toggle('show', !!msg);
}

function bcOnDigitChange(hiddenId, prefixId, digitId){
  bcAllSegmentsApplied = false;
  var digitEl = document.getElementById(digitId);
  var d = (digitEl.value||'').replace(/[^0-9]/g,'').slice(0,3);
  digitEl.value = d;
  var prefix = document.getElementById(prefixId).textContent;
  var combined = parseFloat(prefix + (d||'0'));
  document.getElementById(hiddenId).value = isNaN(combined) ? 0 : combined;

  var warnId = digitId.replace(/Digit$/,'') + 'Warn';
  var ceil = parseFloat(digitEl.dataset.defaultVal);
  var floor = parseFloat(digitEl.dataset.floorVal);
  if(!isNaN(ceil) && combined > ceil) bcSetWarnBox(warnId, '⚠️ Exceeds default ('+ceil+'%)');
  else if(!isNaN(floor) && combined < floor) bcSetWarnBox(warnId, '⚠️ Too low — min '+floor+'%');
  else bcSetWarnBox(warnId, '');
  bcCalc();
}

// Flat Brokerage (₹/Lot) — same soft min-1/max-default range check, for Options & Commodity Options.
function bcOnFlatChange(){
  bcAllSegmentsApplied = false;
  var el = document.getElementById('bcBrokFlat');
  var val = parseFloat(el.value);
  var ceil = parseFloat(el.dataset.defaultVal);
  if(!isNaN(val) && !isNaN(ceil) && val > ceil) bcSetWarnBox('bcBrokFlatWarn', '⚠️ Exceeds default (₹'+ceil+')');
  else if(!isNaN(val) && val < 1) bcSetWarnBox('bcBrokFlatWarn', '⚠️ Too low — min ₹1');
  else bcSetWarnBox('bcBrokFlatWarn', '');
  bcCalc();
}

function bcSetFlatBrokerage(value){
  var el = document.getElementById('bcBrokFlat');
  el.value = value;
  el.dataset.defaultVal = BC_ORIGINAL_DEFAULTS[bcCurSeg].brokFlat;
  bcSetWarnBox('bcBrokFlatWarn', '');
}

function bcApplyDefaultBrokerage(seg){
  var d = BC_SEGMENT_DEFAULTS[seg];
  var shape = BC_SEG_BROK_TYPE[seg];
  if(shape==='flat') bcSetFlatBrokerage(d.brokFlat);
  else if(shape==='pct_min'){
    bcSetLockedRate('bcBrokRate','bcBrokRatePrefix','bcBrokRateDigit', d.brokPct);
    bcSetLockedRate('bcBrokMinShare','bcBrokMinSharePrefix','bcBrokMinShareDigit', d.brokMin);
  }
  else bcSetLockedRate('bcBrokRate','bcBrokRatePrefix','bcBrokRateDigit', d.brokPct);
}

function bcSetSeg(seg){
  bcCurSeg = seg;
  document.querySelectorAll('#page-brokerage-calc .bc-seg-tab').forEach(function(t){ t.classList.toggle('active', t.dataset.seg===seg); });
  var d = BC_SEGMENT_DEFAULTS[seg];
  document.getElementById('bcSttRate').value = d.stt;
  document.getElementById('bcTxnRate').value = d.txn;
  document.getElementById('bcStampRate').value = d.stamp;
  document.getElementById('bcSebiRate').value = d.sebi;
  bcApplyRoleAccess();
  bcUpdateBrokFields();
  bcUpdateQtyFields();
  bcApplyDefaultBrokerage(seg);
  bcApplyClientOverride();
  bcCalc();
}

// ══════════════════════════════════════════
// CLIENT BROKERAGE PROFILES — shared across all RMs via Firestore.
// Saving stores the CURRENTLY-DISPLAYED segment's rate under the client name;
// switching segments for a saved client loads that segment's saved rate if present, else the default.
// ══════════════════════════════════════════
function BCCLIENTDOC(){ return window.fdb.collection('crm_data').doc('brokerage_clients'); }
var bcClients = {};
var bcClientsUnsub = null;
var bcCurrentClientKey = '';

function bcSubscribeClients(){
  try{
    if(bcClientsUnsub) bcClientsUnsub();
    bcClientsUnsub = BCCLIENTDOC().onSnapshot(function(doc){
      bcClients = (doc.exists && doc.data() && doc.data().clients) ? doc.data().clients : {};
      bcRenderClientDropdown();
      bcApplyClientOverride();
      if(bcSummaryOpen) bcRenderSummaryTable();
      if(bcAllSegOpen) bcRenderAllSegTable();
    });
  }catch(e){}
}

// Full lookup (all accessible clients) so exact-match selection always works, even beyond the visible suggestion cap.
var bcClientLookup = {};   // "Name (CODE)" -> code
var bcAllMyClients = [];
function bcPopulateClientDatalist(search){
  var dl = document.getElementById('bcClientDatalist');
  if(!dl) return;
  bcAllMyClients = (typeof getMyEqClients==='function') ? getMyEqClients() : [];
  bcClientLookup = {};
  bcAllMyClients.forEach(function(c){
    if(!c.code) return;
    var saved = bcClients[c.code] ? ' ✓ saved' : '';
    bcClientLookup[c.name+' ('+c.code+')'+saved] = c.code;
  });

  var list = bcAllMyClients.slice().sort(function(a,b){ return (a.name||'').localeCompare(b.name||''); });
  if(search){
    var s = search.toLowerCase();
    list = list.filter(function(c){ return (c.name||'').toLowerCase().indexOf(s)!==-1 || (c.code||'').toLowerCase().indexOf(s)!==-1; });
  }
  list = list.slice(0, 60);   // cap the visible suggestion list for performance; full list is still searchable by typing more
  var html = '<option value="➕ New Client (type any name — for a walk-in/prospective client)">';
  list.forEach(function(c){
    if(!c.code) return;
    var saved = bcClients[c.code] ? ' ✓ saved' : '';
    html += '<option value="'+(c.name+' ('+c.code+')'+saved).replace(/"/g,'&quot;')+'">';
  });
  dl.innerHTML = html;
}

function bcRenderClientDropdown(){
  bcPopulateClientDatalist();
}

// Fires on every keystroke — re-filters the suggestion list ourselves, and loads the client as soon as the typed
// text exactly matches one (either picked from suggestions or typed in full). Clearing the box goes back to defaults.
function bcOnClientSearchInput(){
  var val = (document.getElementById('bcClientSearch').value||'').trim();
  if(val.indexOf('➕ New Client')===0){
    document.getElementById('bcClientSearch').value = 'New Client';
    val = 'New Client';
  }
  bcPopulateClientDatalist(val);
  if(!val){
    bcCurrentClientKey = '';
    bcApplyDefaultBrokerage(bcCurSeg);
    document.getElementById('bcClientHint').textContent = 'Type any new client\'s name to calculate/save a rate for them, even if they\'re not in the client list yet.';
    bcCalc();
    return;
  }
  var code = bcClientLookup[val];
  if(code){
    bcCurrentClientKey = code;
    bcApplyClientOverride();
    bcLoadMultiTradeForClient();
    if(bcAllSegOpen) bcRenderAllSegTable();
  } else {
    bcCurrentClientKey = '';
    document.getElementById('bcClientHint').textContent = '';
  }
}

// If a client is selected and has a saved rate for the CURRENT segment, load it into the fields.
function bcApplyClientOverride(){
  var hintEl = document.getElementById('bcClientHint');
  if(!bcCurrentClientKey || !bcClients[bcCurrentClientKey]){ if(hintEl) hintEl.textContent=''; return; }
  var c = bcClients[bcCurrentClientKey];
  var shape = BC_SEG_BROK_TYPE[bcCurSeg];
  var saved = c[bcCurSeg];
  if(saved==null){ if(hintEl) hintEl.textContent = c.label+' — no saved rate for this segment yet (showing default).'; return; }
  if(shape==='flat') bcSetFlatBrokerage(saved);
  else if(shape==='pct_min'){
    bcSetLockedRate('bcBrokRate','bcBrokRatePrefix','bcBrokRateDigit', saved.pct);
    bcSetLockedRate('bcBrokMinShare','bcBrokMinSharePrefix','bcBrokMinShareDigit', saved.min);
  } else bcSetLockedRate('bcBrokRate','bcBrokRatePrefix','bcBrokRateDigit', saved);
  if(hintEl) hintEl.textContent = 'Showing saved rate for '+c.label+' (this segment).';
  bcCalc();
}

async function bcSaveClient(){
  var typedName = (document.getElementById('bcClientSearch').value||'').trim().replace(/\s*\(.*?\)\s*(✓ saved)?\s*$/,'');
  var myClients = (typeof getMyEqClients==='function') ? getMyEqClients() : [];
  var rec = bcCurrentClientKey ? myClients.find(function(c){ return c.code===bcCurrentClientKey; }) : null;
  var key, name;
  if(rec){ key = rec.code; name = rec.name; }
  else if(typedName){
    // Not one of your CRM clients (e.g. a new/prospective client) — save under a walk-in profile by name instead.
    key = 'custom_' + typedName.toLowerCase().replace(/[^a-z0-9]+/g,'_').replace(/^_+|_+$/g,'');
    name = typedName;
    bcCurrentClientKey = key;
  } else { alert('Type or select a client name first.'); return; }

  // Sync whatever is currently typed/shown for the active segment into BC_SEGMENT_DEFAULTS,
  // so it's included below along with every other segment's current default.
  var curShape = BC_SEG_BROK_TYPE[bcCurSeg];
  if(curShape==='flat') BC_SEGMENT_DEFAULTS[bcCurSeg].brokFlat = parseFloat(document.getElementById('bcBrokFlat').value)||0;
  else if(curShape==='pct_min'){
    BC_SEGMENT_DEFAULTS[bcCurSeg].brokPct = parseFloat(document.getElementById('bcBrokRate').value)||0;
    BC_SEGMENT_DEFAULTS[bcCurSeg].brokMin = parseFloat(document.getElementById('bcBrokMinShare').value)||0;
  } else BC_SEGMENT_DEFAULTS[bcCurSeg].brokPct = parseFloat(document.getElementById('bcBrokRate').value)||0;

  // Always save all 6 segments — whatever's currently set as each one's rate (default or edited).
  var segsToSave = {};
  Object.keys(BC_SEGMENT_DEFAULTS).forEach(function(seg){
    var def = BC_SEGMENT_DEFAULTS[seg];
    var shape = BC_SEG_BROK_TYPE[seg];
    if(shape==='flat') segsToSave[seg] = def.brokFlat;
    else if(shape==='pct_min') segsToSave[seg] = { pct: def.brokPct, min: def.brokMin };
    else segsToSave[seg] = def.brokPct;
  });

  try{
    var updatePayload = {};
    updatePayload['clients.'+key+'.label'] = name;
    Object.keys(segsToSave).forEach(function(seg){ updatePayload['clients.'+key+'.'+seg] = segsToSave[seg]; });
    await BCCLIENTDOC().set({}, {merge:true});   // ensure the doc exists before a dotted-path update
    await BCCLIENTDOC().update(updatePayload);
  }catch(e){
    // Fallback: full-document rewrite if update() fails (e.g. doc didn't exist yet)
    var current = JSON.parse(JSON.stringify(bcClients||{}));
    if(!current[key]) current[key] = { label:name };
    current[key].label = name;
    Object.keys(segsToSave).forEach(function(seg){ current[key][seg] = segsToSave[seg]; });
    try{ await BCCLIENTDOC().set({ clients: current }, {merge:true}); }
    catch(e2){ alert('Could not save: '+(e2&&e2.message?e2.message:e2)); return; }
  }
  document.getElementById('bcClientHint').textContent = 'Saved '+name+'\'s rate across all 6 segments'+(rec?'':' (walk-in/prospective client)')+'.';
  document.getElementById('bcClientSearch').value = name+' ('+key+') ✓ saved';
}

// Applies the digit the RM just typed into EVERY segment's DEFAULT rate (this session only — no client, no Firestore).
// e.g. typing "2" makes Futures/Commodity Futures default to 0.02%, Equity Delivery 0.2%, Equity Intraday 0.02%,
// and Options/Commodity Options ₹20/lot (flat segments scale ×10, so a "3" stays ₹30/lot, "2" → ₹20/lot, "1" → ₹10/lot).
function bcApplyDigitToAllSegments(){
  var shape = BC_SEG_BROK_TYPE[bcCurSeg];
  // A single digit drives everything. If starting FROM a flat segment (Options/Commodity Options), the typed
  // ₹ value is used as-is for both flat segments (no rescaling), and the equivalent "digit" for percent-based
  // segments is derived by dividing by 10 (₹20 → digit "2" → 0.02%). If starting from a percent-based segment,
  // the typed digit applies directly there, and flat segments scale up ×10 (digit "2" → ₹20/lot).
  var typedFlatValue = null, digit;
  if(shape==='flat'){
    typedFlatValue = parseFloat(document.getElementById('bcBrokFlat').value)||0;
    digit = String(Math.round(typedFlatValue/10));
  } else {
    digit = (document.getElementById('bcBrokRateDigit').value||'').replace(/[^0-9]/g,'') || '0';
  }
  var rateDigit = digit, minDigit = digit;

  ['futures','commodity_futures','currency_futures'].forEach(function(seg){
    var orig = BC_ORIGINAL_DEFAULTS[seg];
    var prefix = String(orig.brokPct).slice(0, bcAutoLockChars(orig.brokPct));
    BC_SEGMENT_DEFAULTS[seg].brokPct = parseFloat(prefix + rateDigit) || 0;
  });
  ['equity_delivery','equity_intraday'].forEach(function(seg){
    var orig = BC_ORIGINAL_DEFAULTS[seg];
    var pctPrefix = String(orig.brokPct).slice(0, bcAutoLockChars(orig.brokPct));
    var minPrefix = String(orig.brokMin).slice(0, bcAutoLockChars(orig.brokMin));
    BC_SEGMENT_DEFAULTS[seg].brokPct = parseFloat(pctPrefix + rateDigit) || 0;
    BC_SEGMENT_DEFAULTS[seg].brokMin = parseFloat(minPrefix + minDigit) || 0;
  });
  ['options','commodity_options','currency_options'].forEach(function(seg){
    BC_SEGMENT_DEFAULTS[seg].brokFlat = (typedFlatValue!=null) ? typedFlatValue : (parseFloat(rateDigit)||0) * 10;
  });

  bcAllSegmentsApplied = true;
  bcApplyDefaultBrokerage(bcCurSeg);   // refresh the currently-shown segment's fields — without re-loading any client override
  bcRefreshAllSegBrokerageInputs(false);
  bcCalc();
  document.getElementById('bcClientHint').textContent = 'Applied digit "'+rateDigit+'" as the new default across all 6 segments for this session.';
}

async function bcDeleteClient(){
  if(!bcCurrentClientKey || !bcClients[bcCurrentClientKey]){ alert('Select a client with a saved rate first.'); return; }
  var label = bcClients[bcCurrentClientKey].label;
  if(!confirm('Clear all saved brokerage rates for "'+label+'"? This only removes their saved rates here — their CRM client record is not affected.')) return;
  try{
    var del = {}; del['clients.'+bcCurrentClientKey] = firebase.firestore.FieldValue.delete();
    await BCCLIENTDOC().update(del);
  }catch(e){
    var current = JSON.parse(JSON.stringify(bcClients||{}));
    delete current[bcCurrentClientKey];
    try{ await BCCLIENTDOC().set({ clients: current }); }catch(e2){ alert('Could not delete: '+(e2&&e2.message?e2.message:e2)); return; }
  }
  bcCurrentClientKey = '';
  document.getElementById('bcClientSearch').value = '';
  document.getElementById('bcClientHint').textContent = '';
}

// Reload the default Brokerage Rate for the currently-selected segment — no full page refresh needed.
// Blanks Buy Price, Sell Price, and Lot Size/No. Of Lots/Quantity (whichever applies to the current segment).
function bcClearTradeFields(){
  document.getElementById('bcBuyPrice').value = '';
  document.getElementById('bcSellPrice').value = '';
  document.getElementById('bcLotSize').value = '';
  document.getElementById('bcLots').value = '';
  document.getElementById('bcQtyPlain').value = '';
}

function bcResetBrokerage(){
  // Undo any "Apply To All Segments" mutation — every segment's default goes back to the true original.
  Object.keys(BC_SEGMENT_DEFAULTS).forEach(function(seg){
    var orig = BC_ORIGINAL_DEFAULTS[seg];
    BC_SEGMENT_DEFAULTS[seg].brokPct = orig.brokPct;
    BC_SEGMENT_DEFAULTS[seg].brokMin = orig.brokMin;
    BC_SEGMENT_DEFAULTS[seg].brokFlat = orig.brokFlat;
  });
  bcApplyDefaultBrokerage(bcCurSeg);   // refresh the currently-shown segment's fields
  bcAllSegmentsApplied = false;
  bcClearTradeFields();
  bcRefreshAllSegBrokerageInputs(true);
  bcCalc();
}

// Blank out the Brokerage Rate for EVERY segment (session-wide) — not just the one currently shown.
function bcClearBrokerage(){
  bcAllSegmentsApplied = false;
  Object.keys(BC_SEGMENT_DEFAULTS).forEach(function(seg){
    BC_SEGMENT_DEFAULTS[seg].brokPct = 0;
    BC_SEGMENT_DEFAULTS[seg].brokMin = 0;
    BC_SEGMENT_DEFAULTS[seg].brokFlat = 0;
  });
  ['bcBrokRateWarn','bcBrokMinShareWarn','bcBrokFlatWarn'].forEach(function(id){ bcSetWarnBox(id, ''); });
  document.getElementById('bcBrokFlat').value = '';
  document.getElementById('bcBrokRateDigit').value = '';
  document.getElementById('bcBrokRate').value = 0;
  document.getElementById('bcBrokMinShareDigit').value = '';
  document.getElementById('bcBrokMinShare').value = 0;
  bcClearTradeFields();
  bcRefreshAllSegBrokerageInputs(true);
  bcCalc();
}

function bcToggleAdv(){
  document.getElementById('bcAdvRates').classList.toggle('open');
}

function bcFmt(n){
  var neg = n<0; n=Math.abs(n);
  var s = n.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2});
  return (neg?'-':'') + '₹' + s;
}

function bcCalc(){
  var buy = parseFloat(document.getElementById('bcBuyPrice').value)||0;
  var sell = parseFloat(document.getElementById('bcSellPrice').value)||0;
  var usesLots = !!BC_USES_LOTS[bcCurSeg];
  var qty, lots;
  if(usesLots){
    var lotSize = parseFloat(document.getElementById('bcLotSize').value)||0;
    lots = parseFloat(document.getElementById('bcLots').value)||0;
    qty = lotSize*lots;
    document.getElementById('bcTotalQtyShow').textContent = qty.toLocaleString('en-IN');
  } else {
    qty = parseFloat(document.getElementById('bcQtyPlain').value)||0;
    lots = 0;
  }
  var brokPct = parseFloat(document.getElementById('bcBrokRate').value)||0;
  var brokFlat = parseFloat(document.getElementById('bcBrokFlat').value)||0;
  var brokMin = parseFloat(document.getElementById('bcBrokMinShare').value)||0;
  var sttPct = parseFloat(document.getElementById('bcSttRate').value)||0;
  var txnPct = parseFloat(document.getElementById('bcTxnRate').value)||0;
  var stampPct = parseFloat(document.getElementById('bcStampRate').value)||0;
  var sebiPct = parseFloat(document.getElementById('bcSebiRate').value)||0;
  var gstPct = parseFloat(document.getElementById('bcGstRate').value)||0;
  var d = BC_SEGMENT_DEFAULTS[bcCurSeg];
  var brokShape = BC_SEG_BROK_TYPE[bcCurSeg];

  var buyTurnover = buy*qty, sellTurnover = sell*qty;
  var totalTurnover = buyTurnover+sellTurnover;

  var brokerage, brokLbl;
  if(brokShape==='flat'){
    brokerage = brokFlat * lots * 2;   // charged on both the buy order and the sell order
    brokLbl = 'Brokerage (₹'+brokFlat+' × '+lots+' lot(s) × 2 legs)';
  } else if(brokShape==='pct_min'){
    var buyBrok = (buy>0) ? Math.max(buyTurnover*brokPct/100, qty*brokMin) : 0;
    var sellBrok = (sell>0) ? Math.max(sellTurnover*brokPct/100, qty*brokMin) : 0;
    brokerage = buyBrok + sellBrok;
    brokLbl = 'Brokerage ('+brokPct+'% or ₹'+brokMin+'/share, whichever higher — both legs)';
  } else {
    brokerage = totalTurnover * brokPct/100;
    brokLbl = 'Brokerage ('+brokPct+'% of turnover)';
  }
  var txnCharges = totalTurnover * txnPct/100;
  var sebiFees = totalTurnover * sebiPct/100;
  var stamp = buyTurnover * stampPct/100;   // buy-side only
  var stt;
  if(d.sttSide==='both') stt = totalTurnover * sttPct/100;
  else stt = sellTurnover * sttPct/100;      // sell-side only (F&O convention)

  var gstBase = brokerage + txnCharges + sebiFees;
  var gst = gstBase * gstPct/100;

  var totalCharges = brokerage + stt + txnCharges + stamp + sebiFees + gst;
  var grossPL = sellTurnover - buyTurnover;
  var netPL = grossPL - totalCharges;

  var turnoverHtml =
    '<div class="bc-result-row"><span class="bc-lbl">Buy Turnover ('+qty.toLocaleString('en-IN')+' Qty × ₹'+buy+')</span><span class="bc-val">'+bcFmt(buyTurnover)+'</span></div>'
    +'<div class="bc-result-row"><span class="bc-lbl">Sell Turnover ('+qty.toLocaleString('en-IN')+' Qty × ₹'+sell+')</span><span class="bc-val">'+bcFmt(sellTurnover)+'</span></div>'
    +'<div class="bc-result-row bc-total" style="border-top:2px solid var(--gold);margin-top:2px;padding-top:10px"><span class="bc-lbl">Total Turnover</span><span class="bc-val">'+bcFmt(totalTurnover)+'</span></div>';
  document.getElementById('bcTurnoverBox').innerHTML = turnoverHtml;

  var rows = [
    [brokLbl, brokerage],
    ['STT / CTT'+(d.sttSide==='both'?' (buy + sell)':' (sell side)'), stt],
    ['Exchange Transaction Charges', txnCharges],
    ['Stamp Duty (buy side)', stamp],
    ['SEBI Turnover Fees', sebiFees],
    ['GST (' + gstPct + '% on Brokerage+Txn+SEBI)', gst]
  ];
  var html = rows.map(function(r){
    return '<div class="bc-result-row"><span class="bc-lbl">'+r[0]+'</span><span class="bc-val">'+bcFmt(r[1])+'</span></div>';
  }).join('');
  html += '<div class="bc-result-row bc-total"><span class="bc-lbl">Total Charges</span><span class="bc-val">'+bcFmt(totalCharges)+'</span></div>';
  document.getElementById('bcBreakdown').innerHTML = html;

  var box = document.getElementById('bcNetBox');
  var isProfit = netPL >= 0;
  box.className = 'bc-net-box ' + (isProfit ? 'bc-profit' : 'bc-loss');
  document.getElementById('bcNetAmt').textContent = bcFmt(netPL);
  var clientTag = (bcCurrentClientKey && bcClients[bcCurrentClientKey]) ? (bcClients[bcCurrentClientKey].label + ' — ') : '';
  document.getElementById('bcNetLbl').textContent = clientTag + 'Gross ' + (grossPL>=0?'Profit':'Loss') + ' ' + bcFmt(Math.abs(grossPL))
    + ' − Charges ' + bcFmt(totalCharges) + ' = Net ' + (isProfit?'Profit':'Loss');

  bcLastCalc = {
    clientLabel: (bcCurrentClientKey && bcClients[bcCurrentClientKey]) ? bcClients[bcCurrentClientKey].label : '',
    segLabel: BC_MULTI_SEG_LABELS[bcCurSeg] || bcCurSeg,
    buy:buy, sell:sell, qty:qty,
    buyTurnover:buyTurnover, sellTurnover:sellTurnover, totalTurnover:totalTurnover,
    rows:rows, totalCharges:totalCharges,
    grossPL:grossPL, netPL:netPL, isProfit:isProfit
  };

  document.getElementById('bcRatesNote').innerHTML =
    '<b>Note:</b> Only the Brokerage Rate is editable — please enter the client\'s actual rate. Selecting a segment automatically displays the appropriate field type (Percentage / Minimum / Flat). '
    +'Statutory charges — STT/CTT, Stamp Duty, Transaction Charges, SEBI Fees, and GST — are fixed government and exchange rates applied uniformly to every client: '
    +'Futures: STT 0.05% (sell side), Stamp Duty 0.002% (buy side), Transaction Charges ≈0.0019%, SEBI Fees 0.0001%. '
    +'Options: STT 0.15% (sell side, on premium), Transaction Charges 0.0355%, Stamp Duty 0.003% (buy side). '
    +'Equity Delivery and Intraday follow standard NSE/SEBI slabs. Commodity Futures: CTT 0.05% (sell side, on par with equity futures), MCX Transaction Charges 0.0021%, Stamp Duty 0.002%. '
    +'Commodity Options: CTT 0.05% (sell side, on premium), MCX Transaction Charges 0.00418%, Stamp Duty 0.003%. '
    +'Currency Futures/Options: STT does not apply (currency derivatives are exempt) — Transaction Charges 0.00005% (Futures) / 0.002% (Options, on premium), Stamp Duty 0.0001%. '
    +'GST is charged at 18% on the sum of Brokerage, Transaction Charges, and SEBI Fees. Rates reflect the April 2026 Budget revision — please verify against the latest exchange circular periodically.';
}

// ══════════════════════════════════════════
// MULTIPLE TRADES — COMBINED BILL
// Lets an RM split one client's position across segments (e.g. 100 Intraday + 100 Delivery)
// and get a single combined bill. Uses the selected client's saved rate per segment when available,
// else that segment's default rate.
// ══════════════════════════════════════════
var bcMultiRowCount = 0;
var BC_MULTI_SEG_LABELS = { equity_intraday:'Equity Intraday', equity_delivery:'Equity Delivery', futures:'Futures (F&O)',
                            options:'Options', commodity_futures:'Commodity Futures', commodity_options:'Commodity Options',
                            currency_futures:'Currency Futures', currency_options:'Currency Options' };

function bcAddMultiRow(){
  bcMultiRowCount++;
  var id = bcMultiRowCount;
  var opts = Object.keys(BC_MULTI_SEG_LABELS).map(function(k){ return '<option value="'+k+'">'+BC_MULTI_SEG_LABELS[k]+'</option>'; }).join('');
  var row = document.createElement('div');
  row.className = 'bc-multi-row';
  row.id = 'bcMultiRow'+id;
  row.innerHTML =
    '<div><label>Segment</label><select id="bcMultiSeg'+id+'" onchange="bcUpdateMultiRowFields('+id+')">'+opts+'</select></div>'
    +'<div><label>Buy Price (₹)</label><input type="number" id="bcMultiBuy'+id+'" placeholder="Buy Price"></div>'
    +'<div><label>Sell Price (₹)</label><input type="number" id="bcMultiSell'+id+'" placeholder="Sell Price"></div>'
    +'<div><label>Quantity (Total Shares)</label><input type="number" id="bcMultiQty'+id+'" placeholder="Quantity" min="1"></div>'
    +'<div id="bcMultiLotWrap'+id+'" style="display:none"><label>Lot Size</label><input type="number" id="bcMultiLot'+id+'" placeholder="Lot Size" min="1"></div>'
    +'<button type="button" class="bc-multi-remove" onclick="bcRemoveMultiRow('+id+')" title="Remove this line">✕</button>';
  document.getElementById('bcMultiRows').appendChild(row);
}

// Options/Commodity Options are flat-₹-per-lot — show a Lot Size field for that row so turnover
// (Quantity × Price) and brokerage (lots × flat rate) both use real, consistent numbers.
function bcUpdateMultiRowFields(id){
  var seg = document.getElementById('bcMultiSeg'+id).value;
  var isFlat = BC_SEG_BROK_TYPE[seg]==='flat';
  document.getElementById('bcMultiLotWrap'+id).style.display = isFlat ? '' : 'none';
}

function bcRemoveMultiRow(id){
  var row = document.getElementById('bcMultiRow'+id);
  if(row) row.remove();
}

// Removes every trade line and starts fresh with two blank rows, like the page's initial state.
function bcClearMultiTrade(){
  document.getElementById('bcMultiRows').innerHTML = '';
  document.getElementById('bcMultiResults').innerHTML = '';
  bcAddMultiRow();
  bcAddMultiRow();
}

function bcCalcMultiTrade(){
  var rows = document.querySelectorAll('#bcMultiRows .bc-multi-row');
  var resultsEl = document.getElementById('bcMultiResults');
  if(!rows.length){ resultsEl.innerHTML = '<div style="color:#dc2626;font-weight:700;font-size:.8rem">Add at least one trade line first.</div>'; return; }

  var grand = {brokerage:0, charges:0, total:0, grossPL:0};
  var rowsHtml = '';
  var anyApproxOptions = false;

  rows.forEach(function(rowEl){
    var id = rowEl.id.replace('bcMultiRow','');
    var seg = document.getElementById('bcMultiSeg'+id).value;
    var buy = parseFloat(document.getElementById('bcMultiBuy'+id).value)||0;
    var sell = parseFloat(document.getElementById('bcMultiSell'+id).value)||0;
    var qty = parseFloat(document.getElementById('bcMultiQty'+id).value)||0;
    if(!qty) return;

    var d = BC_SEGMENT_DEFAULTS[seg];
    var shape = BC_SEG_BROK_TYPE[seg];
    var clientRate = (bcCurrentClientKey && bcClients[bcCurrentClientKey]) ? bcClients[bcCurrentClientKey][seg] : null;

    var buyTurnover = buy*qty, sellTurnover = sell*qty, totalTurnover = buyTurnover+sellTurnover;
    var brokerage;
    if(shape==='flat'){
      var flatRate = (clientRate!=null) ? clientRate : d.brokFlat;
      var lotSizeEl = document.getElementById('bcMultiLot'+id);
      var lotSize = lotSizeEl ? (parseFloat(lotSizeEl.value)||0) : 0;
      var lots = lotSize>0 ? (qty/lotSize) : qty;   // if Lot Size wasn't entered, fall back to treating Quantity itself as lots
      brokerage = flatRate * lots * 2;
      if(!(lotSize>0)) anyApproxOptions = true;
    } else if(shape==='pct_min'){
      var pct = (clientRate!=null) ? clientRate.pct : d.brokPct;
      var min = (clientRate!=null) ? clientRate.min : d.brokMin;
      var buyBrok2 = (buy>0) ? Math.max(buyTurnover*pct/100, qty*min) : 0;
      var sellBrok2 = (sell>0) ? Math.max(sellTurnover*pct/100, qty*min) : 0;
      brokerage = buyBrok2 + sellBrok2;
    } else {
      var pctOnly = (clientRate!=null) ? clientRate : d.brokPct;
      brokerage = totalTurnover * pctOnly/100;
    }

    var txn = totalTurnover * d.txn/100;
    var sebi = totalTurnover * d.sebi/100;
    var stamp = buyTurnover * d.stamp/100;
    var stt = (d.sttSide==='both') ? totalTurnover*d.stt/100 : sellTurnover*d.stt/100;
    var gst = (brokerage+txn+sebi) * 0.18;
    var otherCharges = txn+sebi+stamp+stt+gst;
    var total = brokerage + otherCharges;

    grand.brokerage += brokerage; grand.charges += otherCharges; grand.total += total;
    grand.grossPL += (sellTurnover - buyTurnover);
    rowsHtml += '<tr><td>'+BC_MULTI_SEG_LABELS[seg]+' ('+qty+' qty)</td><td>'+bcFmt(totalTurnover)+'</td><td>'+bcFmt(brokerage)+'</td><td>'+bcFmt(otherCharges)+'</td><td>'+bcFmt(total)+'</td></tr>';
  });

  rowsHtml += '<tr class="bc-multi-total"><td>Combined Total</td><td></td><td>'+bcFmt(grand.brokerage)+'</td><td>'+bcFmt(grand.charges)+'</td><td>'+bcFmt(grand.total)+'</td></tr>';

  var netPL = grand.grossPL - grand.total;
  var isProfit = netPL >= 0;
  var multiClientTag = (bcCurrentClientKey && bcClients[bcCurrentClientKey]) ? (bcClients[bcCurrentClientKey].label + ' — ') : '';
  var netBoxHtml = '<div class="bc-multi-net-box '+(isProfit?'bc-profit':'bc-loss')+'">'
    +'<div class="bc-amt">'+bcFmt(netPL)+'</div>'
    +'<div class="bc-lbl2">'+multiClientTag+'Gross '+(grand.grossPL>=0?'Profit':'Loss')+' '+bcFmt(Math.abs(grand.grossPL))+' − Total Bill '+bcFmt(grand.total)+' = Net '+(isProfit?'Profit':'Loss')+'</div>'
    +'</div>';

  resultsEl.innerHTML =
    '<table class="bc-multi-table"><thead><tr><th>Segment</th><th>Turnover</th><th>Brokerage</th><th>Other Charges</th><th>Total Bill</th></tr></thead>'
    +'<tbody>'+rowsHtml+'</tbody></table>'
    +netBoxHtml
    +(bcCurrentClientKey && bcClients[bcCurrentClientKey] ? '<div class="bc-bulk-note" style="font-size:.68rem;color:var(--gray);margin-top:6px">Using '+bcClients[bcCurrentClientKey].label+'\'s saved rate where available, segment default otherwise.</div>' : '')
    +(anyApproxOptions ? '<div class="bc-bulk-note" style="font-size:.68rem;color:var(--gray);margin-top:2px">⚠️ Lot Size wasn\'t entered for one or more Options/Commodity Options rows — Quantity was treated as the lot count directly. Fill in Lot Size for an accurate turnover + brokerage split.</div>' : '');
}

// Saves the current Multiple-Trades row layout (segment/buy/sell/qty for each line) under the selected client,
// so it can be reloaded next time that client is picked.
async function bcSaveMultiTrade(){
  if(!bcCurrentClientKey || !bcAllMyClients){ alert('Please select a client above first.'); return; }
  var rec = bcAllMyClients.find(function(c){ return c.code===bcCurrentClientKey; });
  if(!rec){ alert('This client is not in your accessible client list.'); return; }
  var rows = document.querySelectorAll('#bcMultiRows .bc-multi-row');
  var lines = [];
  rows.forEach(function(rowEl){
    var id = rowEl.id.replace('bcMultiRow','');
    var lotEl = document.getElementById('bcMultiLot'+id);
    lines.push({
      seg: document.getElementById('bcMultiSeg'+id).value,
      buy: parseFloat(document.getElementById('bcMultiBuy'+id).value)||0,
      sell: parseFloat(document.getElementById('bcMultiSell'+id).value)||0,
      qty: parseFloat(document.getElementById('bcMultiQty'+id).value)||0,
      lotSize: lotEl ? (parseFloat(lotEl.value)||0) : 0
    });
  });
  if(!lines.length){ alert('Add at least one trade line first.'); return; }

  try{
    var updatePayload = {};
    updatePayload['clients.'+bcCurrentClientKey+'.label'] = rec.name;
    updatePayload['clients.'+bcCurrentClientKey+'.multiTrade'] = lines;
    await BCCLIENTDOC().set({}, {merge:true});
    await BCCLIENTDOC().update(updatePayload);
  }catch(e){
    var current = JSON.parse(JSON.stringify(bcClients||{}));
    if(!current[bcCurrentClientKey]) current[bcCurrentClientKey] = { label:rec.name };
    current[bcCurrentClientKey].multiTrade = lines;
    try{ await BCCLIENTDOC().set({ clients: current }, {merge:true}); }
    catch(e2){ alert('Could not save: '+(e2&&e2.message?e2.message:e2)); return; }
  }
  document.getElementById('bcClientHint').textContent = 'Saved this trade split for '+rec.name+'.';
}

// If the selected client has a saved trade split, load it into the Multiple-Trades rows.
function bcLoadMultiTradeForClient(){
  if(!bcCurrentClientKey || !bcClients[bcCurrentClientKey] || !bcClients[bcCurrentClientKey].multiTrade) return;
  var lines = bcClients[bcCurrentClientKey].multiTrade;
  document.getElementById('bcMultiRows').innerHTML = '';
  lines.forEach(function(line){
    bcAddMultiRow();
    var id = bcMultiRowCount;
    document.getElementById('bcMultiSeg'+id).value = line.seg;
    document.getElementById('bcMultiBuy'+id).value = line.buy||'';
    document.getElementById('bcMultiSell'+id).value = line.sell||'';
    document.getElementById('bcMultiQty'+id).value = line.qty||'';
    bcUpdateMultiRowFields(id);
    if(line.lotSize) document.getElementById('bcMultiLot'+id).value = line.lotSize;
  });
}

var bcSummaryOpen = false;
function bcToggleSummary(){
  bcSummaryOpen = !bcSummaryOpen;
  document.getElementById('bcSummaryWrap').style.display = bcSummaryOpen ? '' : 'none';
  if(bcSummaryOpen) bcRenderSummaryTable();
}

function bcFmtSegCell(seg, entry){
  if(!entry || entry[seg]==null) return '<td class="bc-summary-empty">—</td>';
  var v = entry[seg];
  var shape = BC_SEG_BROK_TYPE[seg];
  if(shape==='flat') return '<td>₹'+v+'/lot</td>';
  if(shape==='pct_min') return '<td>'+v.pct+'% / ₹'+v.min+'</td>';
  return '<td>'+v+'%</td>';
}

function bcRenderSummaryTable(){
  var wrap = document.getElementById('bcSummaryTable');
  if(!wrap) return;
  var accessibleCodes = {};
  (bcAllMyClients||[]).forEach(function(c){ if(c.code) accessibleCodes[c.code]=true; });
  var codes = Object.keys(bcClients||{}).filter(function(k){ return accessibleCodes[k] || k.indexOf('custom_')===0; })
    .sort(function(a,b){ return (bcClients[a].label||'').localeCompare(bcClients[b].label||''); });

  if(!codes.length){
    wrap.innerHTML = '<div style="font-size:.8rem;color:var(--gray)">No saved client rates yet — select a client above, set a rate, and click "Save Rate".</div>';
    return;
  }

  var segs = ['equity_intraday','equity_delivery','futures','options','commodity_futures','commodity_options','currency_futures','currency_options'];
  var shortLabels = { equity_intraday:'Eq Intraday', equity_delivery:'Eq Delivery', futures:'Futures',
                       options:'Options', commodity_futures:'Com Fut', commodity_options:'Com Opt',
                       currency_futures:'Cur Fut', currency_options:'Cur Opt' };
  var html = '<table class="bc-summary-table"><thead><tr><th>Client</th>'
    + segs.map(function(s){ return '<th>'+shortLabels[s]+'</th>'; }).join('')
    + '<th>Split</th><th>Link</th></tr></thead><tbody>';
  codes.forEach(function(code){
    var entry = bcClients[code];
    var safeCode = code.replace(/'/g,"\\'");
    html += '<tr><td style="cursor:pointer;text-decoration:underline" title="Click to load this client above" onclick="bcSelectClientFromSummary(\''+safeCode+'\')">'+entry.label+' ('+code+')</td>'
      + segs.map(function(s){ return bcFmtSegCell(s, entry); }).join('')
      + '<td>'+(entry.multiTrade && entry.multiTrade.length ? entry.multiTrade.length+' lines' : '<span class="bc-summary-empty">—</span>')+'</td>'
      + '<td><button type="button" class="bc-mini-btn" style="border-color:var(--navy);color:var(--navy);white-space:nowrap" onclick="bcShareClientLink(\''+safeCode+'\')">🔗 Share Link</button></td></tr>';
  });
  html += '</tbody></table>';
  wrap.innerHTML = html;
}

// Builds a locked client link (same encoding the standalone DN_Client_Calculator.html page reads) from this
// client's already-saved rates, and copies it to the clipboard.
// Clicking a client's name in the Summary table loads their saved rate into the calculator above,
// for whichever segment tab is currently active — so you can immediately enter a trade and check it.
function bcSelectClientFromSummary(code){
  bcCurrentClientKey = code;
  var entry = bcClients[code];
  if(!entry) return;
  var searchBox = document.getElementById('bcClientSearch');
  searchBox.value = (code.indexOf('custom_')===0) ? entry.label : (entry.label + ' (' + code + ')');
  bcApplyClientOverride();
  bcLoadMultiTradeForClient();
  if(bcAllSegOpen) bcRenderAllSegTable();
  window.scrollTo({top:0, behavior:'smooth'});
}

// A general-purpose link — not tied to any client, Brokerage Rate stays editable for whoever opens it.
function bcGenerateOpenLink(){
  var name = (document.getElementById('bcClientSearch').value||'').trim();
  if(name==='New Client') name = '';
  var url = window.location.origin + '/DN_Client_Calculator.html?mode=open' + (name ? '&n=' + encodeURIComponent(name) : '');
  var copied = false;
  try{
    var ta = document.createElement('textarea');
    ta.value = url; ta.style.position='fixed'; ta.style.opacity='0';
    document.body.appendChild(ta); ta.select(); ta.setSelectionRange(0,99999);
    copied = document.execCommand('copy');
    document.body.removeChild(ta);
  }catch(e){}
  prompt(copied ? 'Link copied! Share this open link (rate stays editable):' : 'Copy this open link (rate stays editable):', url);
}

function bcShareClientLink(code){
  var entry = bcClients[code];
  if(!entry){ alert('No saved rates found for this client.'); return; }
  var rates = {};
  Object.keys(BC_SEGMENT_DEFAULTS).forEach(function(seg){
    if(entry[seg]!=null) rates[seg] = entry[seg];
  });
  if(!Object.keys(rates).length){ alert('This client has no saved segment rates yet.'); return; }
  var encoded = btoa(unescape(encodeURIComponent(JSON.stringify(rates)))).replace(/\+/g,'-').replace(/\//g,'_');
  var url = window.location.origin + '/DN_Client_Calculator.html?r=' + encoded + '&n=' + encodeURIComponent(entry.label);
  var copied = false;
  try{
    var ta = document.createElement('textarea');
    ta.value = url; ta.style.position='fixed'; ta.style.opacity='0';
    document.body.appendChild(ta); ta.select(); ta.setSelectionRange(0,99999);
    copied = document.execCommand('copy');
    document.body.removeChild(ta);
  }catch(e){}
  prompt(copied ? 'Link copied! Share this with '+entry.label+':' : 'Copy this link and share with '+entry.label+':', url);
}

// Builds a clean, standalone summary (segment, rate, trade, turnover, charges, net P&L) for Print / Save PDF —
// everything else on the page is hidden during print via the @media print rules.
function bcPrintSummary(){
  var c = bcLastCalc;
  if(!c || c.qty==null){ bcCalc(); c = bcLastCalc; }
  var today = new Date().toLocaleDateString('en-IN', {day:'2-digit', month:'short', year:'numeric'});

  var allSegs = ['equity_intraday','equity_delivery','futures','options','commodity_futures','commodity_options','currency_futures','currency_options'];
  var clientEntry = (bcCurrentClientKey && bcClients[bcCurrentClientKey]) ? bcClients[bcCurrentClientKey] : null;
  var allRatesHtml = allSegs.map(function(seg){
    var shape = BC_SEG_BROK_TYPE[seg];
    var v = clientEntry && clientEntry[seg]!=null ? clientEntry[seg] : (
      shape==='flat' ? BC_SEGMENT_DEFAULTS[seg].brokFlat :
      shape==='pct_min' ? {pct:BC_SEGMENT_DEFAULTS[seg].brokPct, min:BC_SEGMENT_DEFAULTS[seg].brokMin} :
      BC_SEGMENT_DEFAULTS[seg].brokPct );
    var display = (shape==='flat') ? ('₹'+v+'/lot') : (shape==='pct_min') ? (v.pct+'% / ₹'+v.min+' min') : (v+'%');
    var isCurrent = (seg===bcCurSeg);
    return '<div class="pv-row"'+(isCurrent?' style="background:'+"var(--gold3)"+'"':'')+'><span class="pv-lbl">'+BC_MULTI_SEG_LABELS[seg]+(isCurrent?' (this trade)':'')+'</span><span class="pv-val">'+display+'</span></div>';
  }).join('');

  var rowsHtml = c.rows.map(function(r){
    return '<div class="pv-row"><span class="pv-lbl">'+r[0]+'</span><span class="pv-val">'+bcFmt(r[1])+'</span></div>';
  }).join('');

  var html =
    '<div class="pv-hdr"><h1>D N <span class="pv-gold">INVESTMENT</span></h1><div class="pv-sub">Brokerage &amp; Charges Calculation</div></div>'
    +'<div class="pv-meta"><span>'+(c.clientLabel ? 'Client: '+c.clientLabel : 'Client: —')+'</span><span>'+today+'</span></div>'
    +'<div class="pv-section"><h2>Brokerage Rate — All Segments</h2>'
      +allRatesHtml
    +'</div>'
    +'<div class="pv-section"><h2>Segment: '+c.segLabel+'</h2>'
      +'<div class="pv-row"><span class="pv-lbl">Buy Price</span><span class="pv-val">₹'+c.buy+'</span></div>'
      +'<div class="pv-row"><span class="pv-lbl">Sell Price</span><span class="pv-val">₹'+c.sell+'</span></div>'
      +'<div class="pv-row"><span class="pv-lbl">Quantity</span><span class="pv-val">'+c.qty.toLocaleString('en-IN')+'</span></div>'
    +'</div>'
    +'<div class="pv-section"><h2>Turnover</h2>'
      +'<div class="pv-row"><span class="pv-lbl">Buy Turnover</span><span class="pv-val">'+bcFmt(c.buyTurnover)+'</span></div>'
      +'<div class="pv-row"><span class="pv-lbl">Sell Turnover</span><span class="pv-val">'+bcFmt(c.sellTurnover)+'</span></div>'
      +'<div class="pv-row pv-total"><span class="pv-lbl">Total Turnover</span><span class="pv-val">'+bcFmt(c.totalTurnover)+'</span></div>'
    +'</div>'
    +'<div class="pv-section"><h2>Charges Breakdown</h2>'
      +rowsHtml
      +'<div class="pv-row pv-total"><span class="pv-lbl">Total Charges</span><span class="pv-val">'+bcFmt(c.totalCharges)+'</span></div>'
    +'</div>'
    +'<div class="pv-net '+(c.isProfit?'pv-profit':'pv-loss')+'">'
      +'<div class="pv-amt">'+bcFmt(c.netPL)+'</div>'
      +'<div class="pv-lbl2">Gross '+(c.grossPL>=0?'Profit':'Loss')+' '+bcFmt(Math.abs(c.grossPL))+' − Charges '+bcFmt(c.totalCharges)+' = Net '+(c.isProfit?'Profit':'Loss')+'</div>'
    +'</div>'
    +'<div class="pv-footer">Statutory charges (STT/CTT, Stamp Duty, Transaction Charges, SEBI Fees, GST) are fixed government/exchange rates. Please verify against your latest contract note.</div>';

  document.getElementById('bcPrintView').innerHTML = html;
  window.print();
}

// ══════════════════════════════════════════
// ALL SEGMENTS — ONE SCREEN
// Every segment gets its own row: its own Brokerage Rate on the left, its own Trade Qty & Price on the right —
// no tab-switching needed. Uses the selected client's saved rate per segment where available, else the default.
// ══════════════════════════════════════════
var bcAllSegOpen = true;
var BC_ALL_SEGS = ['equity_intraday','equity_delivery','futures','options','commodity_futures','commodity_options','currency_futures','currency_options'];

function bcToggleAllSegView(){
  bcAllSegOpen = !bcAllSegOpen;
  document.getElementById('bcAllSegWrap').style.display = bcAllSegOpen ? '' : 'none';
  if(bcAllSegOpen) bcRenderAllSegTable();
}

function bcRenderAllSegTable(){
  var wrap = document.getElementById('bcAllSegTable');
  if(!wrap) return;
  var clientEntry = (bcCurrentClientKey && bcClients[bcCurrentClientKey]) ? bcClients[bcCurrentClientKey] : null;

  var html = '<div class="bc-allseg-row bc-allseg-head"><div>Segment</div><div>Brokerage Rate</div><div>Buy Price</div><div>Sell Price</div><div>Quantity</div><div>Total Charges</div><div>Net P/L</div></div>';

  BC_ALL_SEGS.forEach(function(seg){
    var shape = BC_SEG_BROK_TYPE[seg];
    var d = BC_SEGMENT_DEFAULTS[seg];
    var saved = clientEntry ? clientEntry[seg] : null;
    var brokCell;
    if(shape==='flat'){
      var flatVal = (saved!=null) ? saved : d.brokFlat;
      brokCell = '<input type="number" class="bc-allseg-brok-flat" id="bcAllBrokFlat_'+seg+'" value="'+flatVal+'" title="₹ per lot" oninput="bcOnAllFlatChange(\''+seg+'\')">'
               + '<div class="bc-rate-warn-box no-upper" id="bcAllFlatWarn_'+seg+'"></div>';
    } else if(shape==='pct_min'){
      var pctVal = (saved!=null) ? saved.pct : d.brokPct;
      var minVal = (saved!=null) ? saved.min : d.brokMin;
      brokCell = '<div class="bc-locked-rate"><span class="bc-rate-prefix" id="bcAllRatePfx_'+seg+'">0.0</span>'
                 +'<input type="text" class="bc-rate-digit" id="bcAllRateDigit_'+seg+'" maxlength="3" inputmode="numeric" onfocus="this.select()" oninput="bcOnAllDigitChange(\''+seg+'\',false)"></div>'
               + '<input type="hidden" id="bcAllRate_'+seg+'" value="'+pctVal+'">'
               + '<div class="bc-rate-warn-box no-upper" id="bcAllRateWarn_'+seg+'"></div>'
               + '<div class="bc-locked-rate"><span class="bc-rate-prefix" id="bcAllMinPfx_'+seg+'">0.0</span>'
                 +'<input type="text" class="bc-rate-digit" id="bcAllMinDigit_'+seg+'" maxlength="2" inputmode="numeric" onfocus="this.select()" oninput="bcOnAllDigitChange(\''+seg+'\',true)"></div>'
               + '<input type="hidden" id="bcAllMin_'+seg+'" value="'+minVal+'">'
               + '<div class="bc-rate-warn-box no-upper" id="bcAllMinWarn_'+seg+'"></div>';
    } else {
      var pctOnly = (saved!=null) ? saved : d.brokPct;
      brokCell = '<div class="bc-locked-rate"><span class="bc-rate-prefix" id="bcAllRatePfx_'+seg+'">0.0</span>'
                 +'<input type="text" class="bc-rate-digit" id="bcAllRateDigit_'+seg+'" maxlength="3" inputmode="numeric" onfocus="this.select()" oninput="bcOnAllDigitChange(\''+seg+'\',false)"></div>'
               + '<input type="hidden" id="bcAllRate_'+seg+'" value="'+pctOnly+'">'
               + '<div class="bc-rate-warn-box no-upper" id="bcAllRateWarn_'+seg+'"></div>';
    }
    html += '<div class="bc-allseg-row" id="bcAllRow_'+seg+'">'
      + '<div class="bc-allseg-seg">'+BC_MULTI_SEG_LABELS[seg]+'</div>'
      + '<div class="bc-allseg-brok">'+brokCell+'</div>'
      + '<div><input type="number" class="bc-allseg-trade" id="bcAllBuy_'+seg+'" placeholder="Buy" oninput="bcCalcAllSegRow(\''+seg+'\')"></div>'
      + '<div><input type="number" class="bc-allseg-trade" id="bcAllSell_'+seg+'" placeholder="Sell" oninput="bcCalcAllSegRow(\''+seg+'\')"></div>'
      + '<div><input type="number" class="bc-allseg-trade" id="bcAllQty_'+seg+'" placeholder="Qty" min="1" oninput="bcCalcAllSegRow(\''+seg+'\')"></div>'
      + '<div class="bc-allseg-charges" id="bcAllCharges_'+seg+'">₹0.00</div>'
      + '<div class="bc-allseg-net bc-profit" id="bcAllNet_'+seg+'">₹0.00</div>'
      + '</div>';
  });
  wrap.innerHTML = '<div class="bc-allseg-table">'+html+'</div>';

  // Now that the locked-rate elements exist, format each one's prefix/digit and set its range-warning bounds.
  BC_ALL_SEGS.forEach(function(seg){
    var shape = BC_SEG_BROK_TYPE[seg];
    if(shape==='flat'){
      bcSetAllFlatBounds(seg);
    } else if(shape==='pct_min'){
      bcSetLockedRateForSeg('bcAllRate_'+seg,'bcAllRatePfx_'+seg,'bcAllRateDigit_'+seg, parseFloat(document.getElementById('bcAllRate_'+seg).value)||0, seg, false);
      bcSetLockedRateForSeg('bcAllMin_'+seg,'bcAllMinPfx_'+seg,'bcAllMinDigit_'+seg, parseFloat(document.getElementById('bcAllMin_'+seg).value)||0, seg, true);
    } else {
      bcSetLockedRateForSeg('bcAllRate_'+seg,'bcAllRatePfx_'+seg,'bcAllRateDigit_'+seg, parseFloat(document.getElementById('bcAllRate_'+seg).value)||0, seg, false);
    }
  });
}

// Same "locked prefix + editable digit" control as the old single-segment view, but scoped to one row's segment
// (rather than the globally-active bcCurSeg) — so ceiling/floor warnings always reference THAT segment's own
// original ("3" series) default, regardless of which row you're editing.
function bcSetLockedRateForSeg(hiddenId, prefixId, digitId, value, seg, isMin){
  var lockChars = bcAutoLockChars(value);
  var str = String(value);
  var prefix, digits;
  if(str.indexOf('0.')===0 && str.length>=lockChars){ prefix=str.slice(0,lockChars); digits=str.slice(lockChars); }
  else if(str.indexOf('0.')===0){ prefix=str; digits=''; }
  else { prefix=''; digits=str; }
  if(lockChars===2 && digits.length===1) digits += '0';
  document.getElementById(prefixId).textContent = prefix;
  document.getElementById(digitId).value = digits;
  document.getElementById(hiddenId).value = value;

  var origVal = isMin ? BC_ORIGINAL_DEFAULTS[seg].brokMin : BC_ORIGINAL_DEFAULTS[seg].brokPct;
  var origPrefix = String(origVal).slice(0, bcAutoLockChars(origVal));
  var digitEl = document.getElementById(digitId);
  digitEl.dataset.defaultVal = origVal;
  digitEl.dataset.floorVal = parseFloat(origPrefix + '1') || 0;
  bcSetWarnBox((isMin ? 'bcAllMinWarn_' : 'bcAllRateWarn_') + seg, '');
}

function bcSetAllFlatBounds(seg){
  var el = document.getElementById('bcAllBrokFlat_'+seg);
  el.dataset.defaultVal = BC_ORIGINAL_DEFAULTS[seg].brokFlat;
  bcSetWarnBox('bcAllFlatWarn_'+seg, '');
}

function bcOnAllDigitChange(seg, isMin){
  var digitId = (isMin ? 'bcAllMinDigit_' : 'bcAllRateDigit_') + seg;
  var hiddenId = (isMin ? 'bcAllMin_' : 'bcAllRate_') + seg;
  var prefixId = (isMin ? 'bcAllMinPfx_' : 'bcAllRatePfx_') + seg;
  var warnId = (isMin ? 'bcAllMinWarn_' : 'bcAllRateWarn_') + seg;
  var digitEl = document.getElementById(digitId);
  var d = (digitEl.value||'').replace(/[^0-9]/g,'').slice(0,3);
  digitEl.value = d;
  var prefix = document.getElementById(prefixId).textContent;
  var combined = parseFloat(prefix + (d||'0'));
  document.getElementById(hiddenId).value = isNaN(combined) ? 0 : combined;

  var ceil = parseFloat(digitEl.dataset.defaultVal);
  var floor = parseFloat(digitEl.dataset.floorVal);
  if(!isNaN(ceil) && combined > ceil) bcSetWarnBox(warnId, '⚠️ Exceeds default ('+ceil+'%)');
  else if(!isNaN(floor) && combined < floor) bcSetWarnBox(warnId, '⚠️ Too low — min '+floor+'%');
  else bcSetWarnBox(warnId, '');
  bcCalcAllSegRow(seg);
}

function bcOnAllFlatChange(seg){
  var el = document.getElementById('bcAllBrokFlat_'+seg);
  var val = parseFloat(el.value);
  var ceil = parseFloat(el.dataset.defaultVal);
  var warnId = 'bcAllFlatWarn_'+seg;
  if(!isNaN(val) && !isNaN(ceil) && val > ceil) bcSetWarnBox(warnId, '⚠️ Exceeds default (₹'+ceil+')');
  else if(!isNaN(val) && val < 1) bcSetWarnBox(warnId, '⚠️ Too low — min ₹1');
  else bcSetWarnBox(warnId, '');
  bcCalcAllSegRow(seg);
}

// Refreshes every All-Segments row's Brokerage input(s) from BC_SEGMENT_DEFAULTS (used after Apply-To-All / Reset /
// Clear), without touching Buy/Sell/Qty unless clearTrade is true — then recomputes each row's result.
function bcRefreshAllSegBrokerageInputs(clearTrade){
  BC_ALL_SEGS.forEach(function(seg){
    var shape = BC_SEG_BROK_TYPE[seg];
    var d = BC_SEGMENT_DEFAULTS[seg];
    if(shape==='flat'){
      var elFlat = document.getElementById('bcAllBrokFlat_'+seg); if(elFlat){ elFlat.value = d.brokFlat; bcSetAllFlatBounds(seg); }
    } else if(shape==='pct_min'){
      if(document.getElementById('bcAllRate_'+seg)) bcSetLockedRateForSeg('bcAllRate_'+seg,'bcAllRatePfx_'+seg,'bcAllRateDigit_'+seg, d.brokPct, seg, false);
      if(document.getElementById('bcAllMin_'+seg)) bcSetLockedRateForSeg('bcAllMin_'+seg,'bcAllMinPfx_'+seg,'bcAllMinDigit_'+seg, d.brokMin, seg, true);
    } else {
      if(document.getElementById('bcAllRate_'+seg)) bcSetLockedRateForSeg('bcAllRate_'+seg,'bcAllRatePfx_'+seg,'bcAllRateDigit_'+seg, d.brokPct, seg, false);
    }
    if(clearTrade){
      ['bcAllBuy_','bcAllSell_','bcAllQty_'].forEach(function(pfx){
        var el = document.getElementById(pfx+seg); if(el) el.value = '';
      });
    }
    if(document.getElementById('bcAllBuy_'+seg)) bcCalcAllSegRow(seg);
  });
}

function bcCalcAllSegRow(seg){
  var buy = parseFloat(document.getElementById('bcAllBuy_'+seg).value)||0;
  var sell = parseFloat(document.getElementById('bcAllSell_'+seg).value)||0;
  var qty = parseFloat(document.getElementById('bcAllQty_'+seg).value)||0;
  var d = BC_SEGMENT_DEFAULTS[seg];
  var shape = BC_SEG_BROK_TYPE[seg];

  var buyTurnover = buy*qty, sellTurnover = sell*qty, totalTurnover = buyTurnover+sellTurnover;
  var brokerage;
  if(shape==='flat'){
    var flatRate = parseFloat(document.getElementById('bcAllBrokFlat_'+seg).value)||0;
    brokerage = flatRate * qty * 2;   // Quantity treated as lot count for flat-brokerage segments
  } else if(shape==='pct_min'){
    var pct = parseFloat(document.getElementById('bcAllRate_'+seg).value)||0;
    var min = parseFloat(document.getElementById('bcAllMin_'+seg).value)||0;
    var buyBrok = (buy>0) ? Math.max(buyTurnover*pct/100, qty*min) : 0;
    var sellBrok = (sell>0) ? Math.max(sellTurnover*pct/100, qty*min) : 0;
    brokerage = buyBrok + sellBrok;
  } else {
    var pctOnly = parseFloat(document.getElementById('bcAllRate_'+seg).value)||0;
    brokerage = totalTurnover * pctOnly/100;
  }

  var txn = totalTurnover * d.txn/100;
  var sebi = totalTurnover * d.sebi/100;
  var stamp = buyTurnover * d.stamp/100;
  var stt = (d.sttSide==='both') ? totalTurnover*d.stt/100 : sellTurnover*d.stt/100;
  var gst = (brokerage+txn+sebi) * 0.18;
  var totalCharges = brokerage+stt+txn+stamp+sebi+gst;
  var netPL = (sellTurnover-buyTurnover) - totalCharges;

  document.getElementById('bcAllCharges_'+seg).textContent = bcFmt(totalCharges);
  var netEl = document.getElementById('bcAllNet_'+seg);
  netEl.textContent = bcFmt(netPL);
  netEl.className = 'bc-allseg-net ' + (netPL>=0 ? 'bc-profit' : 'bc-loss');

  // Mirror this row into the (hidden) single-segment fields so the detailed Turnover/Charges/Net cards,
  // Save Rate, Print Summary, and Apply-To-All all continue to work against whichever row was just edited.
  bcCurSeg = seg;
  document.getElementById('bcDetailSegLabel').textContent = BC_MULTI_SEG_LABELS[seg];
  document.getElementById('bcSttRate').value = d.stt;
  document.getElementById('bcTxnRate').value = d.txn;
  document.getElementById('bcStampRate').value = d.stamp;
  document.getElementById('bcSebiRate').value = d.sebi;
  document.getElementById('bcBuyPrice').value = buy;
  document.getElementById('bcSellPrice').value = sell;
  document.getElementById('bcQtyPlain').value = qty;
  document.getElementById('bcLotSize').value = qty;
  document.getElementById('bcLots').value = 1;
  if(shape==='flat'){
    document.getElementById('bcBrokFlat').value = document.getElementById('bcAllBrokFlat_'+seg).value;
  } else if(shape==='pct_min'){
    document.getElementById('bcBrokRate').value = document.getElementById('bcAllRate_'+seg).value;
    document.getElementById('bcBrokMinShare').value = document.getElementById('bcAllMin_'+seg).value;
  } else {
    document.getElementById('bcBrokRate').value = document.getElementById('bcAllRate_'+seg).value;
  }
  bcCalc();
}

// Initialize on script load — all fields exist in the DOM even while the page is hidden
bcSetSeg('equity_intraday');
bcSubscribeClients();
bcAddMultiRow();
bcAddMultiRow();
bcRenderAllSegTable();
