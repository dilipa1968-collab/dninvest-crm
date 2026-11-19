/* ═══════════════════════════════════════════════════════════════════════
   CLIENT PROFILE FORM ↔ CRM  (19-Nov-2026)
   - MF Investors me client ke naam par RIGHT-CLICK → "📋 Send Profile Form"
     → WhatsApp par client ko personal link jaata hai (naam/mobile pehle se bhare).
   - Bhejte waqt client ka us din ka SIP / MF value (AUM) snapshot
     crm_data/profile_sends me save hota hai (transaction-safe).
   - Client form bharta hai → Firestore "clientProfiles" (dninvest-profile.html).
   - MF Investors → "📋 Client Profiles" → saare profiles + client ka CURRENT
     SIP aur MF value (aur bhejte waqt ka bhi) — Excel export ke saath.
   Equity clients ko bhi bhej sakte hain (Equity Clients → naam par right-click).
   Client Equity + MF dono me ho (PAN se match) to form me dono ki value dikhti hai.
   Access: Admin sab; RM sirf apne clients ke profiles.
   Depends on app-1.js: DB, CU, fdb, uid, today, fmtDate, fmtNum, toast,
   escapeHtml, closeModal, getMyMfClients.
   ═══════════════════════════════════════════════════════════════════════ */
window.PROF = (function(){
  const SEND_KEY='profile_sends', COL='clientProfiles';
  const esc = s => (typeof escapeHtml==='function') ? escapeHtml(String(s==null?'':s)) : String(s==null?'':s);
  const $ = id => document.getElementById(id);
  const money = v => (v==null||v===''||isNaN(+v)) ? '—' : '₹'+(typeof fmtNum==='function'?fmtNum(+v):Math.round(+v).toLocaleString('en-IN'));
  let _profiles=[], _sends=[], _loaded=false;

  // Fast lookup maps — DB list badle (naya array) tabhi dobara bante hain
  let _ix={eqA:null, mfA:null};
  function ix(){
    const eqA=DB.get('eq_clients')||[], mfA=DB.get('mf_clients')||[];
    if(_ix.eqA===eqA && _ix.mfA===mfA) return _ix;
    const byId=(a)=>{ const m=new Map(); a.forEach(c=>c&&m.set(c.id,c)); return m; };
    const group=(a,key)=>{ const m=new Map(); a.forEach(c=>{ if(!c) return; const k=key(c); if(!k) return; (m.get(k)||m.set(k,[]).get(k)).push(c); }); return m; };
    const panK=c=>{ const p=String(c.pan||'').trim().toUpperCase(); return (!c.is_minor && /^[A-Z]{5}[0-9]{4}[A-Z]$/.test(p)) ? p : ''; };
    const mobK=c=>{ const d=String(c.mobile||'').replace(/\D/g,''); const m=d.length>10?d.slice(-10):d; return m.length===10?m:''; };
    _ix={eqA, mfA, eqId:byId(eqA), mfId:byId(mfA), eqPan:group(eqA,panK), mfPan:group(mfA,panK), eqMob:group(eqA,mobK), mfMob:group(mfA,mobK)};
    return _ix;
  }
  function mfClient(id){ return ix().mfId.get(id); }
  function eqClient(id){ return ix().eqId.get(id); }
  const PAN = c => String((c&&c.pan)||'').trim().toUpperCase();
  const okPan = p => /^[A-Z]{5}[0-9]{4}[A-Z]$/.test(p);
  // Equity ↔ MF same person: PAN se (minor ko chhod ke — wo guardian ka PAN carry karta hai);
  // PAN na ho to mobile se, sirf jab ek hi match ho
  function twinOf(c, seg){
    if(!c) return null;
    const I=ix(); const panMap = seg==='equity' ? I.mfPan : I.eqPan, mobMap = seg==='equity' ? I.mfMob : I.eqMob;
    const p=PAN(c);
    if(okPan(p) && !c.is_minor){
      const hits=panMap.get(p)||[];
      if(hits.length===1) return hits[0];
      if(hits.length>1) return null;
    }
    const m=cleanMobile(c.mobile);
    if(m && m.length===10){ const hits=mobMap.get(m)||[]; if(hits.length===1) return hits[0]; }
    return null;
  }
  // {eq, mf} pair for a client of either segment
  function pairOf(c, seg){ const t=twinOf(c,seg); return seg==='equity' ? {eq:c, mf:t} : {eq:t, mf:c}; }
  const eqVal = e => e ? (+e.asset_value||0) : 0;
  // Invested amount (principal) — AUM import ke aum_detail.inv se
  function investedOf(c){ const v=c && c.aum_detail && +c.aum_detail.inv; return v>0 ? v : 0; }
  function cleanMobile(m){ const d=String(m||'').replace(/\D/g,''); return d.length>10 ? d.slice(-10) : d; }
  function canSeeClient(c, seg){
    if(!CU || !c) return false;
    if(CU.role==='admin') return true;
    try{
      const mine = seg==='equity' ? getMyEqClients() : getMyMfClients();
      return mine.some(x=>x.id===c.id);
    }catch(_){ return (c.rm||'').toLowerCase()===(CU.name||'').toLowerCase(); }
  }
  function canSeePair(pr){ return canSeeClient(pr.eq,'equity') || canSeeClient(pr.mf,'mf'); }
  function linkFor(c, seg, sendId){
    const base=location.origin+location.pathname.replace(/[^/]*$/,'')+'dninvest-profile.html';
    const pr=pairOf(c, seg);
    const p=new URLSearchParams({cid:c.id, seg:seg==='equity'?'eq':'mf', rm:c.rm||CU.name||'', n:c.name||'', m:cleanMobile(c.mobile)});
    // Client ki maujooda investment — form me "Your current investments" box me dikhti hai
    // (Equity + MF dono, agar dono segment me client hai)
    const mf=pr.mf;
    if(mf){
      const inv=investedOf(mf);
      if(inv) p.set('inv', Math.round(inv));
      if(+mf.aum) p.set('val', Math.round(+mf.aum));
      if(+mf.sip_amount) p.set('sip', Math.round(+mf.sip_amount));
      if(+mf.sip_count) p.set('sc', +mf.sip_count);
    }
    if(eqVal(pr.eq)) p.set('eq', Math.round(eqVal(pr.eq)));
    if(sendId) p.set('s',sendId);
    return base+'?'+p.toString();
  }

  // Transaction-safe append to crm_data/profile_sends
  async function recordSend(rec){
    if(typeof fdb==='undefined') return {ok:false,error:'Offline'};
    try{
      const ref=fdb.collection('crm_data').doc(SEND_KEY);
      await fdb.runTransaction(async tx=>{
        const d=await tx.get(ref);
        const arr=(d.exists && Array.isArray(d.data().data)) ? d.data().data.slice() : [];
        arr.push(rec);
        tx.set(ref,{data:DB._clean(arr.slice(-5000)), updated:new Date().toISOString()});
      });
      return {ok:true};
    }catch(e){ console.log('profile send log error',e); return {ok:false,error:e.message}; }
  }

  async function send(cid, mode, seg){
    seg = seg==='equity' ? 'equity' : 'mf';
    const c = seg==='equity' ? eqClient(cid) : mfClient(cid);
    if(!c){ toast('Client not found','error'); return; }
    if(!canSeeClient(c, seg)){ toast('This client is mapped to '+(c.rm||'another RM')+' — you can send only to your own clients','error'); return; }
    const mob=cleanMobile(c.mobile);
    const sendId=uid();
    const link=linkFor(c, seg, sendId);
    const pr=pairOf(c, seg), mf=pr.mf;
    // Bhejte waqt ka SIP / MF value / Equity value snapshot
    const rec={ id:sendId, cid:c.id, seg, eq_id:pr.eq?pr.eq.id:'', mf_id:mf?mf.id:'', name:c.name||'', mobile:mob, rm:c.rm||'', by:CU.name, at:new Date().toISOString(),
      sip:mf?(+mf.sip_amount||0):0, sip_count:mf?(+mf.sip_count||0):0, aum:mf?(+mf.aum||0):0, inv:mf?investedOf(mf):0, eq:eqVal(pr.eq), via:mode==='copy'?'copy':'whatsapp' };
    if(mode==='copy'){
      try{ await navigator.clipboard.writeText(link); toast('Profile form link copied ✓','success'); }
      catch(_){ prompt('Copy this link:', link); }
    } else {
      if(!/^[6-9]\d{9}$/.test(mob)){ toast('Mobile number missing / wrong for '+c.name+' — use "Copy Link" instead','error'); return; }
      const first=(c.name||'').split(' ')[0];
      const msg=`Dear ${first.charAt(0)+first.slice(1).toLowerCase()},\n\nGreetings from D N Investment!\n\nTo help us plan your investments better, please fill this short profile form (takes about 2 minutes):\n${link}\n\nThank you,\n${CU.name||''}\nD N Investment`;
      window.open('https://wa.me/91'+mob+'?text='+encodeURIComponent(msg),'_blank');
    }
    const r=await recordSend(rec);
    if(!r.ok) console.log('profile send not logged:', r.error);
    _loaded=false;
  }

  // ── Speed (19-Nov-2026): pehle har baar 2000 profiles server se aati thi.
  // Ab profiles is browser me cache hoti hain (CRM ka baaki client data bhi
  // aise hi cache hota hai) → popup TURANT khulta hai; peeche se sirf NAYI
  // profiles (last wale ke baad ki) server se aati hain.
  const CACHE='dninvest_profiles_cache';
  const tsMs = v => v ? (v.toMillis ? v.toMillis() : (v.toDate ? v.toDate().getTime() : (typeof v==='number'?v:(Date.parse(v)||0)))) : 0;
  function norm(d){ const o={...d}; o._ms=tsMs(d.submittedAt); delete o.submittedAt; return o; }
  function readCache(){ try{ const c=JSON.parse(localStorage.getItem(CACHE)||'null'); return (c && Array.isArray(c.p)) ? c : null; }catch(_){ return null; } }
  function writeCache(){ try{ localStorage.setItem(CACHE, JSON.stringify({p:_profiles, s:_sends, at:Date.now()})); }catch(_){ /* quota — memory me to hai */ } }
  async function load(full){
    if(typeof fdb==='undefined'){ await window.waitForFdb?.(8000); }
    const newest = (!full && _profiles.length) ? Math.max(..._profiles.map(p=>p._ms||0)) : 0;
    // Har server request par 15 sec ki seema — warna "Loading…" hamesha atka reh sakta tha
    const T=(pr,what)=>Promise.race([pr, new Promise((_,rej)=>setTimeout(()=>rej(new Error(what+' — no reply from server in 15 sec (internet slow / blocked)')),15000))]);
    let q=fdb.collection(COL).orderBy('submittedAt','desc');
    if(newest && firebase?.firestore?.Timestamp?.fromMillis) q=q.where('submittedAt','>',firebase.firestore.Timestamp.fromMillis(newest));
    q = q.limit(newest ? 500 : 1000);
    // "Sent" log alag se — wo fail ho to bhi profiles dikhein
    const sendsP = T(fdb.collection('crm_data').doc(SEND_KEY).get(),'Sent-log').catch(e=>{ console.warn('profile_sends load failed',e); return null; });
    let ps;
    try{ ps = await T(q.get(),'Profiles'); }
    catch(e){
      if(/no reply from server/.test(e.message||'')) throw e;   // server hi nahi mil raha — dobara 15 sec mat ruko
      console.warn('profiles ordered query failed, trying plain read',e);
      // Fallback: bina orderBy (index/field ki dikkat ho to bhi chale) — sort browser me
      ps = await T(fdb.collection(COL).limit(1000).get(),'Profiles');
    }
    const sd = await sendsP;
    const fresh=ps.docs.map(d=>norm({_id:d.id, ...d.data()}));
    let added=fresh.length;
    if(newest){
      const ids=new Set(_profiles.map(p=>p._id));
      const add=fresh.filter(p=>!ids.has(p._id)); added=add.length;
      _profiles = add.concat(_profiles);
    } else _profiles = fresh;
    _profiles.sort((a,b)=>(b._ms||0)-(a._ms||0));
    if(sd) _sends=(sd.exists && Array.isArray(sd.data().data)) ? sd.data().data : [];
    _loaded=true;
    writeCache();
    return newest ? added : 0;
  }
  const subDate = p => p._ms ? new Date(p._ms) : (p.submittedAt?.toDate ? p.submittedAt.toDate() : null);
  // Profile → CRM client(s): link ka cid+seg, warna mobile se; phir doosre segment ka twin
  function pairForProfile(p){
    const seg = p.seg==='eq' ? 'equity' : 'mf';
    let c = p.cid ? (seg==='equity'?eqClient(p.cid):mfClient(p.cid)) : null;
    let s2 = seg;
    if(!c){
      const m=cleanMobile(p.mobile);
      if(m){
        const mfh=ix().mfMob.get(m)||[];
        const eqh=ix().eqMob.get(m)||[];
        if(mfh.length===1){ c=mfh[0]; s2='mf'; } else if(eqh.length===1){ c=eqh[0]; s2='equity'; }
      }
    }
    return c ? pairOf(c, s2) : {eq:null, mf:null};
  }
  function sendOf(p, pr){
    if(p.sendId){ const s=_sends.find(x=>x.id===p.sendId); if(s) return s; }
    const ids=[p.cid, pr.eq&&pr.eq.id, pr.mf&&pr.mf.id].filter(Boolean); if(!ids.length) return null;
    return _sends.filter(x=>ids.includes(x.cid)).sort((a,b)=>String(b.at).localeCompare(String(a.at)))[0]||null;
  }
  function rows(){
    const q=($('prof-search')?.value||'').trim().toLowerCase();
    const rf=$('prof-risk')?.value||'';
    return _profiles.map(p=>{ const pr=pairForProfile(p); return {p, c:pr.mf, e:pr.eq, s:sendOf(p,pr)}; })
      .filter(r=>{
        if(CU.role!=='admin'){ if(!canSeePair({eq:r.e, mf:r.c})) return false; }
        if(rf && r.p.riskProfile!==rf) return false;
        if(q && !([r.p.name,r.p.mobile,r.c&&r.c.rm,r.e&&r.e.rm,r.p.rm].join(' ').toLowerCase().includes(q))) return false;
        return true;
      });
  }
  function render(){
    const list=rows();
    const pend=(CU.role==='admin' ? _sends : _sends.filter(s=>{ const sg=s.seg==='equity'?'equity':'mf'; const c=sg==='equity'?eqClient(s.cid):mfClient(s.cid); return c && canSeeClient(c, sg); }))
      .filter(s=>!_profiles.some(p=>p.sendId===s.id || (p.cid && p.cid===s.cid && subDate(p) && subDate(p).toISOString()>=s.at)));
    const pc=$('prof-pending'); if(pc) pc.textContent = pend.length ? `⏳ ${pend.length} sent, not filled yet` : '';
    $('prof-count').textContent=list.length+' profile'+(list.length===1?'':'s');
    const riskC={Conservative:'#0369a1',Moderate:'#b45309',Aggressive:'#b91c1c'};
    let h=`<table><thead><tr><th>Date</th><th>Client</th><th>Mobile</th><th>RM</th><th>Risk Profile</th><th>Monthly Savings</th><th>Mode</th><th>Planned SIP</th>
      <th title="Client's SIP in CRM right now">Current SIP</th><th title="MF amount invested (principal) — from CRM right now">MF Invested</th><th title="Client's MF value (AUM) in CRM right now">MF Value</th><th title="Equity holding value in CRM right now">Equity Value</th><th title="When the form was sent">At Send (SIP / MF / EQ)</th><th></th></tr></thead><tbody>`;
    if(!list.length) h+=`<tr><td colspan="14" style="text-align:center;color:var(--gray);padding:16px">No profiles yet — right-click a client name in MF Investors → 📋 Send Profile Form</td></tr>`;
    list.forEach((r,i)=>{
      const d=subDate(r.p);
      h+=`<tr style="cursor:pointer" onclick="PROF.detail(${i})">
        <td>${d?d.toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'}):'—'}</td>
        <td><b>${esc(r.p.name)}</b> ${r.e?'<span class="badge b-eq" style="font-size:.55rem">EQ</span>':''}${r.c?'<span class="badge b-mf" style="font-size:.55rem">MF</span>':''}${(r.c||r.e)?'':' <small style="color:#b45309" title="Not matched to any client in CRM">⚠️ not in CRM</small>'}</td>
        <td>${esc(r.p.mobile)}</td><td>${esc((r.c&&r.c.rm)||(r.e&&r.e.rm)||r.p.rm||'—')}</td>
        <td><b style="color:${riskC[r.p.riskProfile]||'var(--navy)'}">${esc(r.p.riskProfile||'—')}</b> <small>(${esc(r.p.riskScore??'')}/15)</small></td>
        <td>${esc(r.p.monthlySavings||'—')}</td><td>${esc(r.p.mode||'—')}</td><td>${esc(r.p.sipAmount||'—')}</td>
        <td><b>${r.c?money(r.c.sip_amount):'—'}</b>${r.c&&r.c.sip_count?` <small>(${r.c.sip_count})</small>`:''}</td>
        <td><b>${r.c&&investedOf(r.c)?money(investedOf(r.c)):'—'}</b></td>
        <td><b>${r.c?money(r.c.aum):'—'}</b></td>
        <td><b>${r.e&&eqVal(r.e)?money(eqVal(r.e)):'—'}</b></td>
        <td>${r.s?`${money(r.s.sip)} / ${money(r.s.aum)} / ${r.s.eq?money(r.s.eq):'—'}<br><small style="color:var(--gray)">${fmtDate(String(r.s.at).slice(0,10))} by ${esc(r.s.by)}</small>`:'—'}</td>
        <td><button class="btn-icon" title="View full profile">👁️</button></td></tr>`;
    });
    $('prof-table').innerHTML=h+'</tbody></table>';
    window._profRows=list;
  }
  function setStatus(t){ const el=$('prof-sync'); if(el) el.textContent=t||''; }
  async function open(full){
    $('profModal').classList.add('open');
    if(!_profiles.length){ const c=readCache(); if(c){ _profiles=c.p; _sends=c.s||[]; } }
    if(_profiles.length){ render(); setStatus('🔄 Checking for new profiles…'); }
    else $('prof-table').innerHTML='<div style="padding:20px;color:var(--gray)">Loading profiles…</div>';
    let n=0;
    try{ n=await load(!!full); }
    catch(e){
      console.error('Client Profiles load error:', e);
      if(_profiles.length){ setStatus('⚠️ Could not reach server — showing saved list ('+(e.message||e)+')'); }
      else $('prof-table').innerHTML=`<div style="padding:20px;color:var(--red)">⚠️ Could not load profiles — ${esc(e.message||e)}<br><small style="color:var(--gray)">Check internet and click 🔄 Refresh. If it keeps happening, send a screenshot of this message.</small></div>`;
      return;
    }
    try{
      render();
      setStatus(n ? `✓ ${n} new` : '✓ Up to date');
      setTimeout(()=>setStatus(''),4000);
    }catch(e){
      console.error('Client Profiles render error:', e);
      $('prof-table').innerHTML=`<div style="padding:20px;color:var(--red)">⚠️ ${_profiles.length} profile(s) loaded but the list could not be shown — ${esc(e.message||e)}<br><small style="color:var(--gray)">Please send a screenshot of this message.</small></div>`;
    }
  }
  const LABELS=[['incomeSource','Income Source'],['monthlyIncome','Monthly Income'],['monthlySavings','Monthly Savings'],['loan','Loan'],['loanAmount','Loan Outstanding'],
    ['emiAmount','Monthly EMI'],['emergencyFund','Emergency Fund'],['healthCover','Health Cover'],['lifeCover','Life Cover'],['goal','Goals'],['mode','Investment Mode'],
    ['sipAmount','Planned SIP'],['period','Investment Period'],['r1','Income Regularity'],['r2','If Market Falls 20%'],['r3','Equity / MF Experience'],['r4','Expectation'],['r5','Age']];
  function detail(i){
    const r=(window._profRows||[])[i]; if(!r) return;
    const p=r.p, d=subDate(p);
    const v=k=>Array.isArray(p[k])?p[k].join(', '):(p[k]||'');
    $('profDetailTitle').textContent='📋 '+(p.name||'Client Profile');
    $('profDetailBody').innerHTML=`
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px">
        ${box('Risk Profile',(p.riskProfile||'—')+' ('+(p.riskScore??'')+'/15)')}
        ${box('Current SIP',r.c?money(r.c.sip_amount)+(r.c.sip_count?' ('+r.c.sip_count+' SIPs)':''):'—')}
        ${box('Invested',r.c&&investedOf(r.c)?money(investedOf(r.c)):'—')}
        ${box('MF Value',r.c?money(r.c.aum):'—')}
        ${box('Equity Value',r.e&&eqVal(r.e)?money(eqVal(r.e)):'—')}
        ${box('At Send (SIP/MF/EQ)',r.s?money(r.s.sip)+' / '+money(r.s.aum)+' / '+(r.s.eq?money(r.s.eq):'—'):'—')}
      </div>
      <div style="font-size:.78rem;color:var(--gray);margin-bottom:8px">Mobile: <b>${esc(p.mobile)}</b> · RM: <b>${esc([r.e&&('EQ: '+(r.e.rm||'—')), r.c&&('MF: '+(r.c.rm||'—'))].filter(Boolean).join(' · ')||p.rm||'—')}</b> · Submitted: <b>${d?d.toLocaleString('en-IN'):'—'}</b></div>
      <table><tbody>${LABELS.filter(([k])=>v(k)).map(([k,l])=>`<tr><td style="color:var(--gray);width:42%">${l}</td><td style="font-weight:600">${esc(v(k))}</td></tr>`).join('')}</tbody></table>`;
    $('profDetailModal').classList.add('open');
  }
  function box(l,v){ return `<div style="background:var(--bg);border-radius:10px;padding:8px 10px"><div style="font-size:.62rem;color:var(--gray);font-weight:700;text-transform:uppercase">${l}</div><div style="font-weight:800;color:var(--navy);font-size:.9rem">${esc(v)}</div></div>`; }
  function exportXlsx(){
    const list=rows();
    const cols=[{header:'Submitted',width:18},{header:'Client',width:26},{header:'Mobile',width:12},{header:'RM',width:12},
      {header:'Current SIP (₹)',width:14,money:true},{header:'SIP Count',width:9,num:true},{header:'MF Invested (₹)',width:14,money:true},{header:'MF Value (₹)',width:14,money:true},{header:'Equity Value (₹)',width:14,money:true},
      {header:'SIP at Send (₹)',width:14,money:true},{header:'MF Value at Send (₹)',width:16,money:true},{header:'Sent On',width:12},
      {header:'Risk Score',width:9,num:true},{header:'Risk Profile',width:13}, ...LABELS.map(([,l])=>({header:l,width:18}))];
    const data=list.map(r=>{ const p=r.p, d=subDate(p);
      return [d?d.toLocaleString('en-IN'):'', p.name, p.mobile, (r.c&&r.c.rm)||(r.e&&r.e.rm)||p.rm||'',
        r.c?(+r.c.sip_amount||0):'', r.c?(+r.c.sip_count||0):'', r.c?investedOf(r.c):'', r.c?(+r.c.aum||0):'', r.e?eqVal(r.e):'',
        r.s?r.s.sip:'', r.s?r.s.aum:'', r.s?fmtDate(String(r.s.at).slice(0,10)):'',
        p.riskScore??'', p.riskProfile||'', ...LABELS.map(([k])=>Array.isArray(p[k])?p[k].join(', '):(p[k]||''))]; });
    dnXlsx('Client_Profiles_'+today()+'.xlsx','D N Investment — Client Profiles ('+today()+')',cols,data);
  }
  // Right-click menu ke upar ka hissa (MF investor ke liye)
  function menuHtml(cid, seg){
    seg = seg==='equity' ? 'equity' : 'mf';
    const it=(icon,label,js,title)=>`<div onclick="${js};closeClientSeminarMenu()" title="${title||''}" style="padding:8px 10px;border-radius:7px;cursor:pointer;font-weight:600" onmouseover="this.style.background='#F0F5FB'" onmouseout="this.style.background='transparent'">${icon} ${label}</div>`;
    return `<div style="padding:7px 10px;font-weight:700;color:var(--navy);border-bottom:1px solid var(--border,#eee);margin-bottom:2px">📋 Client Profile Form</div>`
      + it('💬','Send Profile Form (WhatsApp)',`PROF.send('${cid}','wa','${seg}')`,'Opens WhatsApp with a personal form link for this client')
      + it('🔗','Copy Profile Form Link',`PROF.send('${cid}','copy','${seg}')`)
      + it('👁️','View Submitted Profiles',`PROF.openFor('${cid}','${seg}')`)
      + `<div style="height:6px;border-bottom:1px solid var(--border,#eee);margin-bottom:4px"></div>`;
  }
  async function openFor(cid, seg){
    const c = seg==='equity' ? eqClient(cid) : mfClient(cid);
    await open();
    const s=$('prof-search'); if(s && c){ s.value=cleanMobile(c.mobile)||c.name; render(); }
  }
  return { send, open, openFor, render, detail, exportXlsx, menuHtml };
})();
