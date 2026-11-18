/* ═══════════════════════════════════════════════════════════════════════
   BACK OFFICE WORK TRACKER  (17-Nov-2026)
   ARN Transfer, Bank Mandate/Account update, Nominee, KYC, Mobile/Email,
   Address change, Transmission, DDPI/POA, Demat closure ... har request ek
   ticket ki tarah: Docs Pending → Submitted → In Process → Completed/Rejected.

   Firestore:
     crm_data/bo_requests  → { data:[request,...] }
     crm_data/bo_types     → { data:["ARN Transfer", ...] }   (admin editable)

   Saare writes TRANSACTION-SAFE hain (latest Firestore copy padh ke sirf
   apna record badalte hain) — blind full-array overwrite nahi, taaki do log
   ek saath kaam karein to kisi ka data na ude (mf_business wipe jaisa).

   Access (Users & Settings → user edit → "Back Office Work" checkboxes):
     Admin → sab kuch (Delete sirf Admin).
     bo_eq_access → Equity ki sabhi requests dekh/add/edit/status.
     bo_mf_access → MF ki sabhi requests dekh/add/edit/status.
     Baaki RM → sirf apne clients ki requests DEKH sakta hai (read-only).
   Depends on app-1.js globals: DB, CU, fdb, uid, today, fmtDate, toast,
   closeModal, escapeHtml, getMyEqClients, getMyMfClients, getSegRMs.
   ═══════════════════════════════════════════════════════════════════════ */
window.BO = (function(){
  const KEY='bo_requests', TKEY='bo_types';
  // Segment-wise lists (17-Nov-2026) — entry ke time sirf client ke segment ke options dikhte hain.
  // Admin "⚙️ Lists" se badal sakta hai. Firestore crm_data/bo_types = {mf_types,eq_types,mf_modes,eq_modes}
  const MF_TYPES=['ARN Transfer','Address Change','Bank Account Change','Bank Mandate (OTM) Registration',
    'Folio Consolidation','KYC Modification','Minor to Major','Mobile / Email Update','Nominee Update','Transmission','Other'];
  const MF_MODES=['BSE StAR MF','CAMS','Client Login','Courier Dispatch','KFintech','Mail','MFCentral','NSE MF','Physical'];
  const DEFAULTS={
    mf_types:MF_TYPES,
    // Equity = poori list (MF ke saare options bhi) — user request 17-Nov-2026
    eq_types:[...new Set([...MF_TYPES,'DDPI / POA','Demat Closure','Demat Transfer / Broker Change','Segment Activation'])],
    mf_modes:MF_MODES,
    eq_modes:[...new Set([...MF_MODES,'CIS','Web Admin'])]
  };
  const ALWAYS_MODES=['Courier Dispatch'];
  // Alphabetical (case-insensitive), "Other" hamesha last
  function sortList(list){
    return [...new Set(list)].sort((a,b)=>{
      const ao=/^other$/i.test(a), bo=/^other$/i.test(b);
      if(ao!==bo) return ao?1:-1;
      return a.localeCompare(b,'en',{sensitivity:'base'});
    });
  }
  // purane spelling ko sahi naam pe map karo
  const MODE_FIX={'MF Central':'MFCentral','BSE StAR':'BSE StAR MF','BSE Star':'BSE StAR MF','Kfintech':'KFintech','Courier':'Courier Dispatch','Courior Dispatch':'Courier Dispatch'};
  const normMode = m => MODE_FIX[m] || m || '';
  const STATUSES=['Docs Pending','Submitted','Mailed','In Process','Completed','Rejected'];
  const ST_COLOR={
    'Docs Pending':{bg:'#fef3c7',fg:'#92400e'},
    'Submitted':{bg:'#dbeafe',fg:'#1e40af'},
    'Mailed':{bg:'#ccfbf1',fg:'#0f766e'},
    'In Process':{bg:'#ede9fe',fg:'#6d28d9'},
    'Completed':{bg:'#d1fae5',fg:'#065f46'},
    'Rejected':{bg:'#fee2e2',fg:'#991b1b'}
  };
  const OVERDUE_DAYS=7;
  const PAGE=50;
  let page=1, editingId=null, picked=null, _statusDefaulted=false;

  const esc = s => (typeof escapeHtml==='function') ? escapeHtml(String(s==null?'':s)) : String(s==null?'':s);
  const $ = id => document.getElementById(id);
  const isOpen = r => r.status!=='Completed' && r.status!=='Rejected';

  // ── access ──
  // Permission user record se (fresh DB copy — CU purana ho sakta hai)
  function me(){ if(!CU) return null; return (DB.get('users')||[]).find(u=>u.id===CU.id) || CU; }
  function isAdmin(){ return !!(CU && CU.role==='admin'); }
  // seg: 'equity' | 'mf' — is segment ki entry/edit permission hai?
  function canSeg(seg){
    if(isAdmin()) return true;
    const u=me(); if(!u) return false;
    return seg==='equity' ? u.bo_eq_access===true : u.bo_mf_access===true;
  }
  function canAddAny(){ return canSeg('equity') || canSeg('mf'); }
  function canManage(){ return canAddAny(); }
  function canDelete(){ return !!(CU && CU.role==='admin'); }
  function myDealers(){
    if(!CU) return [];
    return [...new Set([...(CU.eq_dealers||[CU.name]),...(CU.mf_dealers||[CU.name]),CU.name])]
      .map(d=>String(d||'').trim().toUpperCase()).filter(Boolean);
  }

  // ── data ──
  function all(){ const d=DB.get(KEY); return Array.isArray(d)?d:[]; }
  function cfg(){
    const t=DB.get(TKEY), out={};
    Object.keys(DEFAULTS).forEach(k=>{
      let v = (t && !Array.isArray(t) && Array.isArray(t[k]) && t[k].length) ? t[k] : null;
      if(!v && Array.isArray(t) && t.length && /_types$/.test(k)) v=t;   // purana flat format
      let list=(v||DEFAULTS[k]).map(x=>/_modes$/.test(k)?normMode(x):x);
      // Ye options hamesha rahenge (Admin ki saved list me na ho tab bhi) — 17-Nov-2026
      if(/_modes$/.test(k)) list=list.concat(ALWAYS_MODES);
      out[k]=sortList(list);
    });
    return out;
  }
  const segKey = seg => seg==='equity' ? 'eq' : 'mf';
  function typesFor(seg){ return cfg()[segKey(seg)+'_types']; }
  function modesFor(seg){ return cfg()[segKey(seg)+'_modes']; }
  function types(){ const c=cfg(); return sortList([...c.mf_types,...c.eq_types]); }
  function optList(list, cur, placeholder){
    const l=sortList([...list, ...((cur && !list.includes(cur))?[cur]:[])]);
    return `<option value="">${placeholder}</option>`+l.map(x=>`<option ${x===cur?'selected':''}>${esc(x)}</option>`).join('');
  }
  // entry ke waqt default segment: edit me record ka, nayi entry me agar sirf ek segment ki permission ho to wahi
  function defaultSeg(r){
    if(r) return r.seg;
    if(picked) return picked.seg;
    if(canSeg('mf') && !canSeg('equity')) return 'mf';
    if(canSeg('equity') && !canSeg('mf')) return 'equity';
    return null;
  }
  function fillSegOptions(seg, curType, curMode){
    const t=$('bo_type'), m=$('bo_mode'); if(!t||!m) return;
    if(!seg){
      t.innerHTML='<option value="">— Select a client first —</option>'; t.disabled=true;
      m.innerHTML='<option value="">— Select a client first —</option>'; m.disabled=true;
      return;
    }
    t.disabled=false; m.disabled=false;
    t.innerHTML=optList(typesFor(seg), curType||'', '— Select '+(seg==='equity'?'Equity':'MF')+' Request —');
    m.innerHTML=optList(modesFor(seg), curMode||'', '— Select —');
    const lb=$('bo_seg_label'); if(lb) lb.textContent = seg==='equity' ? '📈 Equity' : '🏦 Mutual Fund';
  }
  function visible(){
    const list=all();
    if(isAdmin()) return list;
    const ds=myDealers();
    // permission wala segment poora + apne clients (RM) ki requests
    return list.filter(r=>canSeg(r.seg) || ds.includes(String(clientRm(r)).trim().toUpperCase()) || ds.includes(String(r.rm||'').trim().toUpperCase()) || r.created_by===CU.name);
  }
  function clientRm(r){
    const list=DB.get(r.seg==='equity'?'eq_clients':'mf_clients')||[];
    const c=list.find(x=>x.id===r.client_id);
    return (c && c.rm) || r.rm || '';
  }
  function ageDays(r){
    const from = r.date || (r.created_at||'').slice(0,10);
    const to = isOpen(r) ? today() : (r.completed_date || today());
    if(!from) return 0;
    return Math.max(0, Math.round((new Date(to)-new Date(from))/86400000));
  }

  // Transaction-safe mutate of the whole bo_requests array.
  // mutate(arr) mutates in place; return false to abort.
  // 17-Nov-2026 fix: pehle "optimistic local" save hota tha — server write fail
  // hone par bhi entry us user ki screen par dikhti rehti thi (jaise saved ho),
  // par doosron (Admin) tak kabhi nahi pahunchti thi. Ab local copy SIRF server
  // confirm hone ke baad badalti hai; fail par clear error, kuch fake nahi dikhta.
  // Deleted ids ka "tombstone" bhi doc me rakha jata hai taaki recovery kabhi
  // deleted request wapas na laaye.
  async function mutate(fn){
    if(typeof fdb==='undefined'){ await window.waitForFdb(8000); }
    if(typeof fdb==='undefined') return {ok:false, error:'Offline — could not connect to the server'};
    try{
      const ref=fdb.collection('crm_data').doc(KEY);
      let finalArr=null, aborted=false;
      await fdb.runTransaction(async tx=>{
        const doc=await tx.get(ref);
        const dd=(doc.exists && doc.data()) || {};
        const latest=Array.isArray(dd.data) ? JSON.parse(JSON.stringify(dd.data)) : [];
        const deleted=Array.isArray(dd.deleted) ? dd.deleted.slice() : [];
        const before=latest.length;
        const beforeIds=new Set(latest.map(x=>x.id));
        if(fn(latest)===false){ aborted=true; finalArr=null; return; }
        aborted=false;
        // collapse guard — kabhi bhi healthy list ek jhatke me khaali na ho
        if(before>=5 && latest.length===0) throw new Error('Blocked: this would have wiped all back office requests');
        const afterIds=new Set(latest.map(x=>x.id));
        beforeIds.forEach(id=>{ if(!afterIds.has(id) && !deleted.includes(id)) deleted.push(id); });
        finalArr=latest;
        tx.set(ref,{data:DB._clean(latest), deleted:deleted.slice(-2000), updated:new Date().toISOString()});
      });
      if(finalArr){ DB.setLocal(KEY, finalArr); _serverSeen=true; }
      return {ok:true, aborted};
    }catch(e){
      console.log('bo_requests write error:',e);
      return {ok:false, error:e.message};
    }
  }

  // ── Live sync (realtime listener) ──
  // Har open tab ko server ka latest data turant milta hai — kisi bhi user ki
  // nayi entry/status change sab screens par bina refresh ke dikhegi.
  let _unsub=null, _serverSeen=false, _rescueDone=false;
  // Is browser me pehle se padi local copy (app-1 ka sync isse overwrite kare,
  // usse pehle hi yaad rakh lo) — recovery ke liye.
  let _bootLocal=[];
  try{ _bootLocal=JSON.parse(localStorage.getItem('dninvest_'+KEY)||'[]')||[]; }catch(_){ _bootLocal=[]; }
  function startLive(){
    if(_unsub || typeof fdb==='undefined' || !CU) return;
    _unsub='starting';   // re-entry guard (snapshot callback khud updateBadge → startLive bulata hai)
    try{
      const u=fdb.collection('crm_data').doc(KEY).onSnapshot(doc=>{
        const dd=(doc.exists && doc.data()) || {};
        const arr=Array.isArray(dd.data)?dd.data:[];
        DB.setLocal(KEY, arr); _serverSeen=true;
        rescueUnsaved(arr, Array.isArray(dd.deleted)?dd.deleted:[]);
        if(isPageActive()){ populateFilters(); ensureSpecialStatusOptions(); renderStats(); renderTable(); }
        updateBadge(); renderDashCard();
        setSync(true);
      }, err=>{ console.log('bo live sync error',err); setSync(false, err.message); _unsub=null; });
      _unsub=u;
    }catch(e){ console.log('bo live start err',e); _unsub=null; }
  }
  // Recovery: is user ki jo requests pehle sirf is browser me save hui thi
  // (server tak nahi pahunchi), unhe ek baar server par bhej do.
  async function rescueUnsaved(serverArr, deleted){
    if(_rescueDone || !CU) return; _rescueDone=true;
    const have=new Set(serverArr.map(x=>x.id)), del=new Set(deleted||[]);
    const missing=(_bootLocal||[]).filter(r=>r && r.id && r.created_by===CU.name && !have.has(r.id) && !del.has(r.id));
    if(!missing.length) return;
    const res=await mutate(arr=>{
      const ids=new Set(arr.map(x=>x.id)); let added=0;
      missing.forEach(r=>{ if(!ids.has(r.id)){ arr.push(r); added++; } });
      if(!added) return false;
    });
    if(res.ok && !res.aborted) toast(missing.length+' request(s) that had not reached the server are now saved ✓','success');
    else if(!res.ok) toast('⚠️ '+missing.length+' request(s) could not be saved to the server — '+(res.error||'please retry'),'error');
  }
  function setSync(ok, msg){
    const el=$('bo-sync'); if(!el) return;
    el.innerHTML = ok ? '<span style="color:#059669">● Live</span>' : `<span style="color:#dc2626" title="${esc(msg||'')}">● Offline — click Refresh</span>`;
  }

  async function saveTypes(list){
    DB.setLocal(TKEY, list);
    if(typeof fdb==='undefined') return {ok:false,error:'Offline'};
    try{ await fdb.collection('crm_data').doc(TKEY).set({data:DB._clean(list),updated:new Date().toISOString()}); return {ok:true}; }
    catch(e){ return {ok:false,error:e.message}; }
  }
  // Page khulte hi Firestore se fresh copy (doosre user ke changes turant dikhein)
  async function refreshFromServer(){
    if(typeof fdb==='undefined') return;
    startLive();
    try{
      const [a,b]=await Promise.all([fdb.collection('crm_data').doc(KEY).get(), fdb.collection('crm_data').doc(TKEY).get()]);
      if(a.exists && a.data() && Array.isArray(a.data().data)) DB.setLocal(KEY, a.data().data);
      if(b.exists && b.data() && b.data().data) DB.setLocal(TKEY, b.data().data);
      setSync(true);
    }catch(e){ console.log('bo refresh err',e); setSync(false, e.message); }
  }

  // ── client picker source ──
  function pickerClients(){
    // sirf permission wale segment ke (sabhi) clients
    const eq = canSeg('equity') ? (DB.get('eq_clients')||[]) : [];
    const mf = canSeg('mf') ? (DB.get('mf_clients')||[]) : [];
    const out=[];
    eq.forEach(c=>out.push({seg:'equity',id:c.id,name:c.name||'',code:c.code||'',pan:c.pan||'',mobile:c.mobile||'',rm:c.rm||''}));
    mf.forEach(c=>out.push({seg:'mf',id:c.id,name:c.name||'',code:c.client_id||'',pan:c.pan||'',mobile:c.mobile||'',rm:c.rm||''}));
    return out;
  }

  // ── UI helpers ──
  function stBadge(s){
    const c=ST_COLOR[s]||{bg:'#f1f5f9',fg:'#475569'};
    return `<span class="badge" style="background:${c.bg};color:${c.fg}">${esc(s||'—')}</span>`;
  }
  function fillSelect(id, values, first){
    const el=$(id); if(!el) return;
    const cur=el.value;
    el.innerHTML=(first?`<option value="">${first}</option>`:'')+values.map(v=>`<option value="${esc(v)}">${esc(v)}</option>`).join('');
    if([...el.options].some(o=>o.value===cur)) el.value=cur;
  }
  function populateFilters(){
    fillSelect('bo-type-filter', types(), 'All Types');
    // Status list dobara banane se "All Open"/"Overdue" (special options) mit jaate the aur
    // chuna hua filter "All Status" ho jata tha (live update ke baad) — 18-Nov-2026 fix
    { const sf=$('bo-status-filter'); const keep=sf?sf.value:'';
      fillSelect('bo-status-filter', STATUSES, 'All Status');
      ensureSpecialStatusOptions();
      if(sf && [...sf.options].some(o=>o.value===keep)) sf.value=keep; }
    const rms=[...new Set(visible().map(r=>clientRm(r)).filter(Boolean))].sort();
    fillSelect('bo-rm-filter', rms, 'All RMs');
    const rmF=$('bo-rm-filter'); if(rmF) rmF.style.display=canAddAny()?'':'none';
    const months=[...new Set(visible().map(r=>(r.date||'').slice(0,7)).filter(Boolean))].sort().reverse();
    const mEl=$('bo-month-filter');
    if(mEl){
      const cur=mEl.value;
      mEl.innerHTML='<option value="">All Months</option>'+months.map(m=>{
        const d=new Date(m+'-01'); return `<option value="${m}">${d.toLocaleDateString('en-IN',{month:'short',year:'numeric'})}</option>`;
      }).join('');
      if([...mEl.options].some(o=>o.value===cur)) mEl.value=cur;
    }
    const addBtn=$('bo-add-btn'); if(addBtn) addBtn.style.display=canAddAny()?'':'none';
    const typesBtn=$('bo-types-btn'); if(typesBtn) typesBtn.style.display=(CU&&CU.role==='admin')?'inline-flex':'none';
  }

  function filtered(){
    const q=($('bo-search')?.value||'').trim().toLowerCase();
    const st=$('bo-status-filter')?.value||'';
    const ty=$('bo-type-filter')?.value||'';
    const rm=$('bo-rm-filter')?.value||'';
    const sg=$('bo-seg-filter')?.value||'';
    const mo=$('bo-month-filter')?.value||'';
    return visible().filter(r=>{
      if(st==='__OPEN__'){ if(!isOpen(r)) return false; }
      else if(st==='__OVERDUE__'){ if(!(isOpen(r) && ageDays(r)>OVERDUE_DAYS)) return false; }
      else if(st && r.status!==st) return false;
      if(ty && r.type!==ty) return false;
      if(rm && clientRm(r)!==rm) return false;
      if(sg && r.seg!==sg) return false;
      if(mo && !(r.date||'').startsWith(mo)) return false;
      if(q){
        const hay=[r.client_name,r.client_code,r.pan,r.folio,r.mode,r.ref_no,r.rta,r.remarks,r.type,r.rm].join(' ').toLowerCase();
        if(!hay.includes(q)) return false;
      }
      return true;
    }).sort((a,b)=>{
      // open pehle (sabse purani pehle), phir closed (latest pehle)
      const ao=isOpen(a), bo=isOpen(b);
      if(ao!==bo) return ao?-1:1;
      if(ao) return String(a.date||'').localeCompare(String(b.date||''));
      return String(b.completed_date||b.date||'').localeCompare(String(a.completed_date||a.date||''));
    });
  }

  function renderStats(){
    const v=visible(), open=v.filter(isOpen);
    const overdue=open.filter(r=>ageDays(r)>OVERDUE_DAYS).length;
    const docs=open.filter(r=>r.status==='Docs Pending').length;
    const mo=today().slice(0,7);
    const doneMonth=v.filter(r=>r.status==='Completed' && (r.completed_date||'').startsWith(mo)).length;
    const chip=(n,l,c,f)=>`<div class="sq-stat-chip" style="--chip-accent:${c};cursor:pointer" onclick="BO.quickFilter('${f}')"><span class="n">${n}</span><span class="l">${l}</span></div>`;
    const el=$('bo-stats'); if(!el) return;
    el.innerHTML=chip(open.length,'Open Requests','#1e40af','__OPEN__')
      +chip(overdue,'Overdue (>'+OVERDUE_DAYS+' days)','#dc2626','__OVERDUE__')
      +chip(docs,'Docs Pending','#d97706','Docs Pending')
      +chip(doneMonth,'Completed this month','#059669','Completed');
  }

  function renderTable(){
    const rows=filtered();
    const pages=Math.max(1,Math.ceil(rows.length/PAGE));
    if(page>pages) page=pages;
    const slice=rows.slice((page-1)*PAGE, page*PAGE);
    const cnt=$('bo-count'); if(cnt) cnt.textContent=rows.length+' request'+(rows.length===1?'':'s');
    let h=`<table><thead><tr>
      <th>Date</th><th>Client</th><th>Seg</th><th>Code / Folio</th><th>Request Type</th><th>RTA / AMC</th>
      <th>Client RM</th><th>Entry By</th><th>Status</th><th>Submitted Via</th><th>Submitted</th><th>Completed</th><th>Days</th><th>Remarks</th><th>Actions</th>
    </tr></thead><tbody>`;
    if(!slice.length) h+=`<tr><td colspan="15" style="text-align:center;color:var(--gray);padding:18px">No back office requests found</td></tr>`;
    slice.forEach(r=>{
      const age=ageDays(r), od=isOpen(r)&&age>OVERDUE_DAYS, mg=canSeg(r.seg);
      h+=`<tr${od?' style="background:#fff1f2"':''}>
        <td>${fmtDate(r.date)}</td>
        <td title="${esc(r.client_name)}"><b>${esc(r.client_name)}</b>${r.pan?`<br><small style="color:var(--gray)">${esc(r.pan)}</small>`:''}</td>
        <td><span class="badge ${r.seg==='equity'?'b-eq':'b-mf'}">${r.seg==='equity'?'EQ':'MF'}</span></td>
        <td>${esc(r.client_code||'')}${r.folio?`<br><small style="color:var(--gray)">F: ${esc(r.folio)}</small>`:''}</td>
        <td><b>${esc(r.type)}</b></td>
        <td>${esc(r.rta||'—')}</td>
        <td><b style="color:var(--navy)">${esc(clientRm(r)||'—')}</b></td>
        <td title="${esc(r.created_at?('Entry: '+r.created_at.slice(0,16).replace('T',' ')):'')}">${esc(r.created_by||'—')}${r.updated_by&&r.updated_by!==r.created_by?`<br><small style="color:var(--gray)">Upd: ${esc(r.updated_by)}</small>`:''}</td>
        <td>${mg?`<select onchange="BO.quickStatus('${r.id}',this.value)" style="padding:2px 4px;font-size:.66rem;border-radius:6px;font-weight:700;background:${(ST_COLOR[r.status]||{}).bg||'#fff'};color:${(ST_COLOR[r.status]||{}).fg||'#333'}">${STATUSES.map(s=>`<option ${s===r.status?'selected':''}>${s}</option>`).join('')}</select>`:stBadge(r.status)}</td>
        <td>${r.mode?`<span class="badge" style="background:#e0f2fe;color:#075985">${esc(normMode(r.mode))}</span>`:esc(r.ref_no||'—')}</td>
        <td>${fmtDate(r.submitted_date)||'—'}</td>
        <td>${fmtDate(r.completed_date)||'—'}</td>
        <td style="font-weight:700;color:${od?'#dc2626':isOpen(r)?'var(--navy)':'var(--gray)'}">${age}${od?' ⚠️':''}</td>
        <td title="${esc(r.remarks)}" style="max-width:160px">${esc(r.remarks||'')}</td>
        <td style="white-space:nowrap">
          <button class="btn-icon" onclick="BO.openView('${r.id}')" title="View / History">👁️</button>
          ${mg?`<button class="btn-icon" onclick="BO.openEdit('${r.id}')" title="Edit">✏️</button>`:''}
          ${canDelete()?`<button class="btn-icon" onclick="BO.del('${r.id}')" title="Delete" style="color:var(--red)">🗑️</button>`:''}
        </td></tr>`;
    });
    h+='</tbody></table>';
    const t=$('bo-table'); if(t) t.innerHTML=h;
    restick();
    // pagination
    const pg=$('bo-pg');
    if(pg){
      if(pages<=1){ pg.innerHTML=''; }
      else{
        let p=`<button class="pg-btn" ${page===1?'disabled':''} onclick="BO.go(${page-1})">‹</button>`;
        for(let i=1;i<=pages;i++){ if(i===1||i===pages||Math.abs(i-page)<=2) p+=`<button class="pg-btn ${i===page?'active':''}" onclick="BO.go(${i})">${i}</button>`; else if(!p.endsWith('…</span>')) p+='<span class="pg-info">…</span>'; }
        p+=`<button class="pg-btn" ${page===pages?'disabled':''} onclick="BO.go(${page+1})">›</button><span class="pg-info">${page}/${pages} (${rows.length})</span>`;
        pg.innerHTML=p;
      }
    }
  }

  // Left-right scroll par Date → Code/Folio tak column fix
  function restick(){ try{ if(typeof stickyLeftCols==='function') stickyLeftCols(document.querySelector('#bo-table table'), /^CODE\s*\/\s*FOLIO/i); }catch(_){} }
  function render(){
    populateFilters();
    ensureSpecialStatusOptions();
    // Page pehli baar khule to default "All Open" (user ke chune filter ko baad me nahi chhedte)
    if(!_statusDefaulted){ _statusDefaulted=true; const sf=$('bo-status-filter'); if(sf) sf.value='__OPEN__'; page=1; }
    renderStats(); renderTable();
    // background me server se fresh copy, phir dobara render
    refreshFromServer().then(()=>{ if(isPageActive()){ populateFilters(); ensureSpecialStatusOptions(); renderStats(); renderTable(); } updateBadge(); });
  }
  function ensureSpecialStatusOptions(){
    const s=$('bo-status-filter'); if(!s) return;
    if(![...s.options].some(o=>o.value==='__OPEN__')){
      const cur=s.value;
      s.insertAdjacentHTML('beforeend','<option value="__OPEN__">🔵 All Open</option><option value="__OVERDUE__">🔴 Overdue (&gt;'+OVERDUE_DAYS+' days)</option>');
      s.value=cur;
    }
  }
  function isPageActive(){ return !!document.getElementById('page-bo-work')?.classList.contains('active'); }
  function filter(){ page=1; renderTable(); }
  function go(p){ page=p; renderTable(); }
  function quickFilter(v){ const s=$('bo-status-filter'); if(s){ ensureSpecialStatusOptions(); s.value=v; } filter(); }
  function resetFilters(){ ['bo-search','bo-status-filter','bo-type-filter','bo-rm-filter','bo-seg-filter','bo-month-filter'].forEach(id=>{ const e=$(id); if(e) e.value=''; }); ensureSpecialStatusOptions(); const sf=$('bo-status-filter'); if(sf) sf.value='__OPEN__'; filter(); }

  // ── Add / Edit modal ──
  function modalHtml(r){
    const mg = r ? canSeg(r.seg) : canAddAny();
    const sOpts=STATUSES.map(s=>`<option ${(r?r.status:'Docs Pending')===s?'selected':''}>${s}</option>`).join('');
    return `
      <div class="form-row" ${r?'style="display:none"':''}>
        <div class="form-field" style="position:relative">
          <label>Client * (Equity / MF)</label>
          <input type="text" id="bo_client_search" placeholder="🔍 Name / Code / PAN / Mobile..." autocomplete="off" oninput="BO.searchClient()" onfocus="BO.searchClient()">
          <div id="bo_client_results" style="display:none;position:absolute;top:100%;left:0;right:0;background:#fff;border:1px solid var(--border);border-radius:8px;box-shadow:var(--shadow2);max-height:260px;overflow-y:auto;z-index:60;margin-top:4px"></div>
        </div>
        <div class="form-field"><label>Selected Client</label><input type="text" id="bo_client_sel" readonly style="background:#f7f7f7" placeholder="No client selected"></div>
      </div>
      ${r?`<div style="background:var(--bg);border-radius:8px;padding:8px 12px;margin-bottom:8px;font-size:.82rem"><b>${esc(r.client_name)}</b> · ${r.seg==='equity'?'Equity':'MF'} · ${esc(r.client_code||'')} · Client RM: ${esc(clientRm(r)||'—')} · Entry By: ${esc(r.created_by||'—')}</div>`:''}
      <div class="form-row three">
        <div class="form-field"><label>Request Type * <span id="bo_seg_label" style="color:var(--teal)"></span></label><select id="bo_type"></select></div>
        <div class="form-field"><label>Request Date *</label><input type="date" id="bo_date" value="${esc(r?r.date:today())}"></div>
        <div class="form-field"><label>Status</label><select id="bo_status" ${mg?'':'disabled'}>${sOpts}</select></div>
      </div>
      <div class="form-row three">
        <div class="form-field"><label>RTA / AMC / Broker</label><input type="text" id="bo_rta" list="bo_rta_list" value="${esc(r?r.rta:'')}" placeholder="CAMS / KFintech / AMC name">
          <datalist id="bo_rta_list"><option>Broker (Equity)</option><option>CAMS</option><option>CDSL</option><option>KFintech</option><option>MFCentral</option><option>NSDL</option></datalist></div>
        <div class="form-field"><label>Folio No. (if any)</label><input type="text" id="bo_folio" value="${esc(r?r.folio:'')}" placeholder="Folio / multiple folios"></div>
        <div class="form-field"><label>Submitted Via</label><select id="bo_mode"></select></div>
      </div>
      <div class="form-row three" ${mg?'':'style="display:none"'}>
        <div class="form-field"><label>Submitted Date</label><input type="date" id="bo_sub" value="${esc(r?r.submitted_date:'')}"></div>
        <div class="form-field"><label>Completed Date</label><input type="date" id="bo_done" value="${esc(r?r.completed_date:'')}"></div>
        <div class="form-field"></div>
      </div>
      <div class="form-row full"><div class="form-field"><label>Remarks</label><textarea id="bo_remarks" placeholder="Documents required / pending items...">${esc(r?r.remarks:'')}</textarea></div></div>
      ${(!r && !(canSeg('equity')&&canSeg('mf')))?`<p style="font-size:.74rem;color:var(--gray)">ℹ️ You have permission for ${canSeg('equity')?'Equity':'Mutual Fund'} requests only — the search will show only those clients.</p>`:''}`;
  }
  function openAdd(){
    if(!canAddAny()){ toast('You do not have Back Office entry permission. Please contact Admin.','error'); return; }
    editingId=null; picked=null;
    $('boModalTitle').textContent='➕ New Back Office Request';
    $('boModalBody').innerHTML=modalHtml(null);
    fillSegOptions(defaultSeg(null));
    $('boModal').classList.add('open');
  }
  function openEdit(id){
    const r=all().find(x=>x.id===id); if(!r) return;
    if(!canSeg(r.seg)){ toast('You do not have edit permission for this segment.','error'); return; }
    editingId=id; picked=null;
    $('boModalTitle').textContent='✏️ Edit Request — '+r.type;
    $('boModalBody').innerHTML=modalHtml(r);
    fillSegOptions(r.seg, r.type, normMode(r.mode));
    $('boModal').classList.add('open');
  }
  function searchClient(){
    const q=($('bo_client_search')?.value||'').trim().toLowerCase();
    const out=$('bo_client_results'); if(!out) return;
    if(q.length<2){ out.style.display='none'; out.innerHTML=''; return; }
    const m=pickerClients().filter(c=>(c.name+' '+c.code+' '+c.pan+' '+c.mobile).toLowerCase().includes(q)).slice(0,30);
    out.innerHTML = m.length ? m.map(c=>`<div style="padding:8px 12px;cursor:pointer;border-bottom:1px solid #f0f0f0" onmouseover="this.style.background='#f7f7f7'" onmouseout="this.style.background='#fff'" onclick="BO.pick('${c.seg}','${c.id}')">
        <div style="font-weight:600;font-size:.85rem"><span class="badge ${c.seg==='equity'?'b-eq':'b-mf'}">${c.seg==='equity'?'EQ':'MF'}</span> ${esc(c.name)}</div>
        <div style="font-size:.74rem;color:var(--gray)">${esc(c.code||'—')} · ${esc(c.pan||'—')} · ${esc(c.mobile||'—')} · RM: ${esc(c.rm||'—')}</div></div>`).join('')
      : '<div style="padding:10px;color:var(--gray);font-size:.85rem">No client found</div>';
    out.style.display='block';
  }
  function pick(seg,id){
    const c=pickerClients().find(x=>x.seg===seg && x.id===id); if(!c) return;
    const prevSeg=picked?picked.seg:defaultSeg(null);
    picked=c;
    // segment badla to us segment ki list; same segment to chuna hua option bana rahe
    if(prevSeg!==seg) fillSegOptions(seg);
    else fillSegOptions(seg, $('bo_type')?.value, $('bo_mode')?.value);
    $('bo_client_sel').value=c.name+' ('+(seg==='equity'?'EQ':'MF')+(c.code?' · '+c.code:'')+')';
    $('bo_client_search').value='';
    const out=$('bo_client_results'); out.style.display='none'; out.innerHTML='';
  }
  let _saving=false;
  async function save(){
    if(_saving) return;
    const editRec = editingId ? all().find(x=>x.id===editingId) : null;
    const segOf = editRec ? editRec.seg : (picked ? picked.seg : null);
    if(segOf && !canSeg(segOf)){ toast('You do not have entry permission for this segment.','error'); return; }
    const mg=true;
    const type=$('bo_type').value, date=$('bo_date').value;
    if(!editingId && !picked){ toast('Please select a client','error'); return; }
    if(!type){ toast('Please select a request type','error'); return; }
    if(!date){ toast('Please enter the request date','error'); return; }
    let status = mg ? $('bo_status').value : 'Docs Pending';
    let sub = mg ? $('bo_sub').value : '';
    let done = mg ? $('bo_done').value : '';
    if(status==='Submitted' && !sub) sub=today();
    if((status==='Completed'||status==='Rejected') && !done) done=today();
    if(done && status!=='Completed' && status!=='Rejected') done='';
    const fields={
      type, date, status,
      rta:$('bo_rta').value.trim(), folio:$('bo_folio').value.trim(),
      mode: mg ? $('bo_mode').value : '',
      submitted_date:sub, completed_date:done,
      remarks:$('bo_remarks').value.trim()
    };
    const now=new Date().toISOString();
    let res;
    _saving=true; { const sb=$('boSaveBtn'); if(sb){ sb.disabled=true; sb.textContent='⏳ Saving...'; } }
    if(editingId){
      const id=editingId;
      res=await mutate(arr=>{
        const r=arr.find(x=>x.id===id); if(!r) return false;
        const prevStatus=r.status;
        Object.assign(r, fields, {updated_by:CU.name, updated_at:now});
        r.history=r.history||[];
        if(prevStatus!==fields.status) r.history.push({at:now,by:CU.name,status:fields.status,note:'Status changed: '+prevStatus+' → '+fields.status});
        else r.history.push({at:now,by:CU.name,status:fields.status,note:'Details edited'});
      });
    } else {
      const rec={id:uid(), seg:picked.seg, client_id:picked.id, client_name:picked.name, client_code:picked.code,
        pan:picked.pan, rm:picked.rm, ...fields, created_by:CU.name, created_at:now,
        history:[{at:now,by:CU.name,status:fields.status,note:'Request created'}]};
      res=await mutate(arr=>{ if(arr.some(x=>x.id===rec.id)) return false; arr.push(rec); });
    }
    _saving=false; const sbtn=$('boSaveBtn'); if(sbtn){ sbtn.disabled=false; sbtn.textContent='💾 Save Request'; }
    if(!res.ok){ toast('⚠️ NOT SAVED — '+(res.error||'server error')+'. Please check your internet and click Save again.','error'); return; }
    closeModal('boModal');
    toast(editingId?'Request updated successfully ✓':'Request added successfully ✓','success');
    editingId=null; picked=null;
    afterChange();
  }
  async function quickStatus(id,status){
    const rr=all().find(x=>x.id===id); if(!rr || !canSeg(rr.seg)) return;
    const now=new Date().toISOString();
    const res=await mutate(arr=>{
      const r=arr.find(x=>x.id===id); if(!r || r.status===status) return false;
      const prev=r.status; r.status=status;
      if(status==='Submitted' && !r.submitted_date) r.submitted_date=today();
      if((status==='Completed'||status==='Rejected') && !r.completed_date) r.completed_date=today();
      if(status!=='Completed' && status!=='Rejected') r.completed_date='';
      r.updated_by=CU.name; r.updated_at=now;
      (r.history=r.history||[]).push({at:now,by:CU.name,status,note:'Status changed: '+prev+' → '+status});
    });
    if(!res.ok && !res.aborted) toast('⚠️ Update failed — '+(res.error||''),'error');
    else if(res.ok) toast('Status updated to '+status,'success');
    afterChange();
  }
  async function del(id){
    if(!canDelete()) return;
    const r=all().find(x=>x.id===id); if(!r) return;
    if(!confirm('Are you sure you want to delete this request?\n\n'+r.client_name+' — '+r.type)) return;
    const res=await mutate(arr=>{ const i=arr.findIndex(x=>x.id===id); if(i<0) return false; arr.splice(i,1); });
    if(!res.ok && !res.aborted){ toast('⚠️ Delete failed — '+(res.error||''),'error'); return; }
    toast('Request deleted','success'); afterChange();
  }
  function afterChange(){ if(isPageActive()){ populateFilters(); ensureSpecialStatusOptions(); renderStats(); renderTable(); } updateBadge(); renderDashCard(); }

  // ── view / history ──
  function openView(id){
    const r=all().find(x=>x.id===id); if(!r) return;
    const hist=(r.history||[]).slice().reverse();
    $('boModalTitle').textContent='📄 '+r.type+' — '+r.client_name;
    $('boModalBody').innerHTML=`
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:12px">
        ${di2('Client',r.client_name)}${di2('Segment',r.seg==='equity'?'Equity':'Mutual Fund')}${di2('Code',r.client_code)}
        ${di2('PAN',r.pan)}${di2('Client RM',clientRm(r))}${di2('Status',stBadge(r.status),true)}
        ${di2('Request Date',fmtDate(r.date))}${di2('Submitted',fmtDate(r.submitted_date))}${di2('Completed',fmtDate(r.completed_date))}
        ${di2('RTA / AMC',r.rta)}${di2('Folio',r.folio)}${di2('Submitted Via',normMode(r.mode))}${r.ref_no?di2('Ref / Ack No. (old)',r.ref_no):''}
        ${di2('Days',ageDays(r)+(isOpen(r)?' (Open)':''))}${di2('Entry By',r.created_by)}${di2('Last Updated',r.updated_by?(r.updated_by+' · '+fmtDate((r.updated_at||'').slice(0,10))):'')}
      </div>
      ${r.remarks?`<div class="form-field"><label>Remarks</label><div style="font-size:.85rem;white-space:pre-wrap">${esc(r.remarks)}</div></div>`:''}
      <div class="form-section">🕐 History (${hist.length})</div>
      ${hist.map(h=>`<div class="call-log-item"><div class="call-date">${fmtDate((h.at||'').slice(0,10))} ${(h.at||'').slice(11,16)} — ${esc(h.by||'')}</div><div class="call-note">${esc(h.note||'')}</div></div>`).join('')||'<p style="color:var(--gray);font-size:.82rem">No history</p>'}
      ${canSeg(r.seg)?`<div style="text-align:right;margin-top:10px"><button class="btn btn-primary" onclick="BO.openEdit('${r.id}')">✏️ Edit</button></div>`:''}`;
    $('boModal').classList.add('open');
    const sb=$('boSaveBtn'); if(sb) sb.style.display='none';
  }
  function di2(l,v,raw){ return `<div class="form-field"><label>${l}</label><div style="font-size:.85rem;font-weight:600;color:var(--navy);padding:3px 0">${v?(raw?v:esc(v)):'<span style="color:#ccc">—</span>'}</div></div>`; }

  // Client View popup me history section
  function clientHistoryHtml(clientId, seg){
    const list=all().filter(r=>r.client_id===clientId && r.seg===(seg==='equity'?'equity':'mf'))
      .sort((a,b)=>String(b.date||'').localeCompare(String(a.date||'')));
    let h=`<div class="form-section" style="margin-top:14px">🗂️ Back Office Requests (${list.length})</div>`;
    if(!list.length) return h+'<p style="color:var(--gray);font-size:.82rem">No back office requests yet.</p>';
    return h+list.map(r=>`<div class="call-log-item" style="cursor:pointer;border-left-color:${(ST_COLOR[r.status]||{}).fg||'#999'}" onclick="closeModal('viewModal');BO.openView('${r.id}')">
      <div class="call-date">${fmtDate(r.date)} — ${esc(r.type)}${r.rta?' · '+esc(r.rta):''}</div>
      <div class="call-status">${stBadge(r.status)} · By: ${esc(r.created_by||'—')} ${r.mode?'· Via: '+esc(normMode(r.mode)):''} ${isOpen(r)?'· '+ageDays(r)+' days':''}</div>
      ${r.remarks?`<div class="call-note">${esc(r.remarks)}</div>`:''}</div>`).join('');
  }

  // ── Request types (admin) ──
  const LIST_LABELS={mf_types:'🏦 Mutual Fund — Request Types',mf_modes:'🏦 Mutual Fund — Submitted Via',eq_types:'📈 Equity — Request Types',eq_modes:'📈 Equity — Submitted Via'};
  function openTypes(){
    if(!(CU&&CU.role==='admin')) return;
    const c=cfg();
    $('boModalTitle').textContent='⚙️ Request Types / Submitted Via';
    $('boModalBody').innerHTML=`<p style="font-size:.8rem;color:var(--gray);margin-bottom:8px">One option per line. During entry, only the list for the client's segment is shown. Lists are sorted alphabetically ("Other" is always last). Existing requests are not affected.</p>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
      ${Object.keys(LIST_LABELS).map(k=>`<div class="form-field"><label>${LIST_LABELS[k]}</label>
        <textarea id="bo_list_${k}" style="width:100%;min-height:${/_types$/.test(k)?220:150}px;padding:8px;border:1.5px solid var(--border);border-radius:8px;font-family:inherit;font-size:.82rem">${esc(c[k].join('\n'))}</textarea></div>`).join('')}
      </div>
      <div style="display:flex;justify-content:space-between;margin-top:10px">
        <button class="btn btn-outline" onclick="BO.defaultTypes()">↺ Reset to Default Lists</button>
        <button class="btn btn-primary" onclick="BO.saveTypesFromModal()">💾 Save</button></div>`;
    const sb=$('boSaveBtn'); if(sb) sb.style.display='none';
    $('boModal').classList.add('open');
  }
  async function saveTypesFromModal(){
    const obj={};
    for(const k of Object.keys(LIST_LABELS)){
      const list=[...new Set(($('bo_list_'+k).value||'').split('\n').map(x=>x.trim()).filter(Boolean))];
      if(!list.length){ toast(LIST_LABELS[k]+' — at least one option is required','error'); return; }
      obj[k]=list;
    }
    const r=await saveTypes(obj);
    if(!r.ok){ toast('⚠️ Save failed — '+(r.error||''),'error'); return; }
    closeModal('boModal'); toast('Lists saved successfully ✓','success'); if(isPageActive()) populateFilters();
  }

  // ── Excel export ──
  function exportXlsx(){
    const rows=filtered();
    const cols=[{header:'Date',width:12},{header:'Client',width:28},{header:'Segment',width:9},{header:'Code',width:12},{header:'PAN',width:12},
      {header:'Request Type',width:24},{header:'RTA / AMC',width:14},{header:'Folio',width:14},{header:'Client RM',width:12},
      {header:'Status',width:13,color:v=>dnStatusColor(v)},{header:'Submitted Via',width:14},{header:'Submitted',width:12},{header:'Completed',width:12},
      {header:'Days',width:7,num:true},{header:'Remarks',width:40},{header:'Entry By',width:12}];
    const data=rows.map(r=>[fmtDate(r.date),r.client_name,r.seg==='equity'?'Equity':'MF',r.client_code,r.pan,r.type,r.rta,r.folio,clientRm(r),
      r.status,normMode(r.mode)||r.ref_no||'',fmtDate(r.submitted_date),fmtDate(r.completed_date),ageDays(r),r.remarks,r.created_by]);
    dnXlsx('Back_Office_Requests_'+today()+'.xlsx','D N Investment — Back Office Requests ('+today()+')',cols,data);
  }

  // ── Sidebar badge + dashboard card ──
  function updateBadge(){
    try{ startLive(); }catch(_){}
    const nb=$('nb-bo'); if(!nb || !CU) return;
    const open=visible().filter(isOpen);
    const od=open.filter(r=>ageDays(r)>OVERDUE_DAYS).length;
    nb.textContent=open.length;
    nb.style.display=open.length?'':'none';
    nb.classList.toggle('red', od>0);
    nb.title=open.length+' open'+(od?', '+od+' overdue':'');
  }
  function renderDashCard(){
    const card=$('boDashCard'); if(!card || !CU) return;
    const open=visible().filter(isOpen);
    if(!open.length){ card.style.display='none'; return; }
    card.style.display='';
    const od=open.filter(r=>ageDays(r)>OVERDUE_DAYS);
    const byType={}; open.forEach(r=>{ byType[r.type]=(byType[r.type]||0)+1; });
    const top=open.slice().sort((a,b)=>ageDays(b)-ageDays(a)).slice(0,5);
    card.innerHTML=`<h3 onclick="showPage('bo-work')" style="cursor:pointer">🗂️ Pending Back Office Work
        <span style="background:var(--navy);color:#fff;border-radius:10px;padding:1px 8px;font-size:.65rem;margin-left:auto">${open.length} open</span>
        ${od.length?`<span style="background:var(--red);color:#fff;border-radius:10px;padding:1px 8px;font-size:.65rem">${od.length} overdue</span>`:''}</h3>
      <div style="display:flex;flex-wrap:wrap;gap:5px;margin-bottom:6px">${Object.entries(byType).sort((a,b)=>b[1]-a[1]).map(([t,n])=>`<span class="badge" style="background:var(--blue2);color:var(--navy)">${esc(t)}: ${n}</span>`).join('')}</div>
      ${top.map(r=>{ const a=ageDays(r), o=a>OVERDUE_DAYS; return `<div class="bar-row" style="cursor:pointer" onclick="BO.openView('${r.id}')">
        <span class="bar-name" style="max-width:none;flex:1">${esc(r.client_name)} <small style="color:var(--gray)">— ${esc(r.type)} · RM: ${esc(clientRm(r)||'—')} · By: ${esc(r.created_by||'—')}</small></span>
        ${stBadge(r.status)}<span style="font-size:.72rem;font-weight:700;margin-left:8px;color:${o?'var(--red)':'var(--gray)'}">${a}d</span></div>`; }).join('')}`;
  }

  // dropdown band karo bahar click par
  document.addEventListener('click', e=>{
    const inp=$('bo_client_search'), out=$('bo_client_results');
    if(inp && out && !inp.contains(e.target) && !out.contains(e.target)) out.style.display='none';
  });
  // modal khulte waqt Save button wapas dikhao (view/types me chhupta hai)
  function resetSaveBtn(){ const sb=$('boSaveBtn'); if(sb) sb.style.display=''; }
  const _openAdd=openAdd, _openEdit=openEdit;

  return {
    render, filter, go, quickFilter, resetFilters,
    openAdd:()=>{ resetSaveBtn(); _openAdd(); },
    openEdit:id=>{ resetSaveBtn(); _openEdit(id); },
    openView, searchClient, pick, save, quickStatus, del,
    openTypes, saveTypesFromModal, exportXlsx,
    defaultTypes:()=>{ Object.keys(DEFAULTS).forEach(k=>{ const t=document.getElementById('bo_list_'+k); if(t) t.value=DEFAULTS[k].join('\n'); }); },
    updateBadge, renderDashCard, clientHistoryHtml, restick,
    STATUSES
  };
})();
// app-1 ka refreshDash/updateBadges is file ke load hone se pehle chal chuka ho sakta hai
try{ if(typeof CU!=='undefined' && CU){ BO.updateBadge(); BO.renderDashCard(); } }catch(_){}
