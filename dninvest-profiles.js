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
   🆕 IPO section (ipo* fields) — detail, Excel, list me IPO badge.
   📈 Form me Equity / Trading section (eq* fields) — detail, Excel aur list me 📈 badge.
   ⚡ Profiles login ke baad peeche se pehle hi load (prefetch) — Clear Cache ke baad bhi popup jaldi.
   📊 RM-wise Sent / Filled / Pending (toolbar button + ⏳ par click), blank form bhi log hota hai.
   👤 RM ko wo profiles bhi dikhti hain jo form usne khud bheja (client CRM me na ho /
      MF hamare yahan na ho). Kisi aur RM ka client nikle to uski investment chhupi rehti hai.
   🔗 Mobile alag ho to detail popup → "Link to CRM Client" (20-Nov-2026).
   💡 Detail popup me "Suggested Plan" (dninvest-profile-plan.js) apne aap banta hai (20-Nov-2026).
   Depends on app-1.js: DB, CU, fdb, uid, today, fmtDate, fmtNum, toast,
   escapeHtml, closeModal, getMyMfClients.
   ═══════════════════════════════════════════════════════════════════════ */
window.PROF = (function(){
  const SEND_KEY='profile_sends', COL='clientProfiles';
  const esc = s => (typeof escapeHtml==='function') ? escapeHtml(String(s==null?'':s)) : String(s==null?'':s);
  const $ = id => document.getElementById(id);
  const money = v => (v==null||v===''||isNaN(+v)) ? '—' : '₹'+(typeof fmtNum==='function'?fmtNum(+v):Math.round(+v).toLocaleString('en-IN'));
  let _profiles=[], _sends=[], _loaded=false, _deleted=new Set();
  // Manual link (20-Nov-2026): profile ka mobile CRM se alag ho → RM haath se jodta hai.
  // crm_data/profile_links → {links:{<profile mobile>:{cid,seg,by,at}}}. Mobile par key
  // hai, isliye client form dobara bhare (record overwrite ho) tab bhi link bana rehta hai.
  let _links={};
  const LINK_KEY='profile_links';

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
  // Cache key "dninvest_" se shuru NAHI hota → CRM ka 🧹 Clear Cache ise nahi mitata, popup turant
  // khulta hai. Clear Cache ka pata "dninvest_prof_ok" marker se chalta hai (wo mit jaata hai) →
  // tab purani list dikhate hue peeche se POORI list dobara aati hai (fresh data bhi, wait bhi nahi).
  const CACHE='dnprof_cache_v2', OLD_CACHE='dninvest_profiles_cache', OK_MARK='dninvest_prof_ok';
  const tsMs = v => v ? (v.toMillis ? v.toMillis() : (v.toDate ? v.toDate().getTime() : (typeof v==='number'?v:(Date.parse(v)||0)))) : 0;
  function norm(d){ const o={...d}; o._ms=tsMs(d.submittedAt); delete o.submittedAt; return o; }
  function readCache(){ try{ const c=JSON.parse(localStorage.getItem(CACHE)||localStorage.getItem(OLD_CACHE)||'null'); return (c && Array.isArray(c.p)) ? c : null; }catch(_){ return null; } }
  function writeCache(){
    const v=JSON.stringify({p:_profiles, s:_sends, l:_links, at:Date.now()});
    try{ localStorage.setItem(CACHE, v); }
    catch(_){ try{ localStorage.setItem(CACHE, JSON.stringify({p:_profiles, s:[], l:_links, at:Date.now()})); }catch(__){ /* quota — memory me to hai */ } }
    try{ localStorage.removeItem(OLD_CACHE); }catch(_){}
  }
  // Ek hi waqt me ek hi load (prefetch chal raha ho to popup usi ka intezaar kare, dobara na mangaye)
  let _loadP=null, _loadFull=false;
  function load(full){
    if(_loadP && (_loadFull || !full)) return _loadP;
    _loadFull=!!full;
    _loadP=loadNow(full).finally(()=>{ _loadP=null; _loadFull=false; });
    return _loadP;
  }
  async function loadNow(full){
    if(typeof fdb==='undefined'){ await window.waitForFdb?.(8000); }
    const newest = (!full && _profiles.length) ? Math.max(..._profiles.map(p=>p._ms||0)) : 0;
    // Har server request par 15 sec ki seema — warna "Loading…" hamesha atka reh sakta tha
    const T=(pr,what)=>Promise.race([pr, new Promise((_,rej)=>setTimeout(()=>rej(new Error(what+' — no reply from server in 15 sec (internet slow / blocked)')),15000))]);
    let q=fdb.collection(COL).orderBy('submittedAt','desc');
    if(newest && firebase?.firestore?.Timestamp?.fromMillis) q=q.where('submittedAt','>',firebase.firestore.Timestamp.fromMillis(newest));
    q = q.limit(newest ? 500 : 1000);
    // Saari requests EK SAATH (pehle ek ke baad ek jaati thin → Clear Cache ke baad bahut der lagti thi)
    const sendsP = T(fdb.collection('crm_data').doc(SEND_KEY).get(),'Sent-log').catch(e=>{ console.warn('profile_sends load failed',e); return null; });
    const delP = T(fdb.collection('crm_data').doc('profile_deleted').get(),'Deleted-list').catch(()=>null);
    const lkP = T(fdb.collection('crm_data').doc(LINK_KEY).get(),'Links').catch(()=>null);
    let ps;
    try{ ps = await T(q.get(),'Profiles'); }
    catch(e){
      if(/no reply from server/.test(e.message||'')) throw e;   // server hi nahi mil raha — dobara 15 sec mat ruko
      console.warn('profiles ordered query failed, trying plain read',e);
      // Fallback: bina orderBy (index/field ki dikkat ho to bhi chale) — sort browser me
      ps = await T(fdb.collection(COL).limit(1000).get(),'Profiles');
    }
    const [sd, del, lk] = await Promise.all([sendsP, delP, lkP]);
    if(del && del.exists && Array.isArray(del.data().ids)) _deleted=new Set(del.data().ids);
    if(lk) _links = (lk.exists && lk.data().links) || {};
    const fresh=ps.docs.map(d=>norm({_id:d.id, ...d.data()}));
    let added=fresh.length;
    if(newest){
      // Same document dobara aaya (client ne form dobara bhara → overwrite) → purana hata ke naya
      const freshIds=new Set(fresh.map(p=>p._id));
      const old=new Set(_profiles.map(p=>p._id));
      added=fresh.filter(p=>!old.has(p._id)).length + fresh.filter(p=>old.has(p._id)).length;
      _profiles = fresh.concat(_profiles.filter(p=>!freshIds.has(p._id)));
    } else _profiles = fresh;
    // "Deleted" list sirf is browser ke PURANE cache par lagti hai. Jo record abhi server
    // se aaya hai wo Firebase me maujood hai — yani delete ke baad client ne dobara bhara
    // (form mobile-number wali same ID par save karta hai). Use kabhi mat chhupao. (19-Nov-2026 fix)
    const freshIds=new Set(fresh.map(p=>p._id));
    _profiles = _profiles.filter(p=>freshIds.has(p._id) || !_deleted.has(p._id));
    _profiles.sort((a,b)=>(b._ms||0)-(a._ms||0));
    if(sd) _sends=(sd.exists && Array.isArray(sd.data().data)) ? sd.data().data : [];
    _loaded=true;
    writeCache();
    try{ localStorage.setItem(OK_MARK,'1'); }catch(_){}
    return newest ? added : 0;
  }
  const subDate = p => p._ms ? new Date(p._ms) : (p.submittedAt?.toDate ? p.submittedAt.toDate() : null);
  // Profile → CRM client(s): link ka cid+seg, warna mobile se; phir doosre segment ka twin
  function pairForProfile(p){
    const seg = p.seg==='eq' ? 'equity' : 'mf';
    let c = p.cid ? (seg==='equity'?eqClient(p.cid):mfClient(p.cid)) : null;
    let s2 = seg;
    if(!c){
      const L=_links[cleanMobile(p.mobile)];
      if(L){ c = L.seg==='equity' ? eqClient(L.cid) : mfClient(L.cid); if(c) s2 = L.seg==='equity'?'equity':'mf'; }
    }
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
  // Form kisne bheja: CRM se bheja → send log ka "by"; blank form → link me RM ka naam
  function senderOf(r){ return (r.s && r.s.by) || (r.p && r.p.blankForm ? (r.p.rm||'') : ''); }
  // Profile ka RM: MF RM → (MF na ho to) form bhejne wala → Equity RM. (20-Nov-2026)
  // Wajah: bahut se client equity me ek RM ke paas, MF kisi ke paas map nahi — MF ka kaam
  // jis RM ne form bheja wahi karega.
  function rmOf(r){
    const mf=r.c&&r.c.rm, eq=r.e&&r.e.rm, by=senderOf(r);
    const main = mf || by || eq || r.p.rm || '';
    const sub=[];
    if(eq && eq.toLowerCase()!==main.toLowerCase()) sub.push('EQ: '+eq);
    if(by && by.toLowerCase()!==main.toLowerCase()) sub.push('📤 '+by);
    return {main, sub, mf, eq, by};
  }
  function rows(){
    const q=($('prof-search')?.value||'').trim().toLowerCase();
    const rf=$('prof-risk')?.value||'';
    // Ek client ne do baar form bhara ho → sirf sabse naya (mobile se pehchaan; _profiles pehle se naya→purana sorted)
    const seenMob=new Set();
    const latest=_profiles.filter(p=>{ const k=cleanMobile(p.mobile)||('id:'+p._id); if(seenMob.has(k)) return false; seenMob.add(k); return true; });
    const me=String(CU.name||'').trim().toLowerCase(), same=v=>String(v||'').trim().toLowerCase()===me;
    return latest.map(p=>{
        const pr=pairForProfile(p), s=sendOf(p,pr);
        if(CU.role==='admin') return {p, c:pr.mf, e:pr.eq, s};
        // RM ko dikhe: (1) client CRM me uska hai, YA (2) form usi ne bheja (send log me "by",
        // ya link / blank form me uska naam) — chahe client ka MF / equity hamare yahan ho ya na ho.
        // (2) me client CRM me kisi AUR RM ka nikla → profile dikhegi, par us client ki
        // investment (SIP / MF / Equity) nahi — sirf "🔒 mapped to <RM>". (20-Nov-2026)
        const own = canSeePair(pr);
        const sentByMe = (s && same(s.by)) || same(p.rm);
        if(own) return {p, c:pr.mf, e:pr.eq, s};
        if(!sentByMe) return null;
        const o=pr.mf||pr.eq;
        return {p, c:null, e:null, s:(s && same(s.by))?s:null, other: o ? (o.rm||'another RM') : ''};
      })
      .filter(r=>{
        if(!r) return false;
        if(rf && r.p.riskProfile!==rf) return false;
        if(q && !([r.p.name,r.p.mobile,r.c&&r.c.rm,r.e&&r.e.rm,r.p.rm,r.other,r.s&&r.s.by].join(' ').toLowerCase().includes(q))) return false;
        return true;
      });
  }
  function render(){
    const list=rows();
    const pend=(CU.role==='admin' ? _sends : _sends.filter(s=>{ if(String(s.by||'').trim().toLowerCase()===String(CU.name||'').trim().toLowerCase()) return true; const sg=s.seg==='equity'?'equity':'mf'; const c=sg==='equity'?eqClient(s.cid):mfClient(s.cid); return c && canSeeClient(c, sg); }))
      .filter(s=>!s.blank && !filledFor(s));
    const pc=$('prof-pending'); if(pc){ pc.textContent = pend.length ? `⏳ ${pend.length} sent, not filled yet` : ''; pc.style.cursor='pointer'; pc.title='Click — RM-wise sent / filled / pending'; pc.onclick=()=>PROF.summary(); }
    $('prof-count').textContent=list.length+' profile'+(list.length===1?'':'s');
    const riskC={Conservative:'#0369a1',Moderate:'#b45309',Aggressive:'#b91c1c'};
    let h=`<table><thead><tr><th>Date</th><th>Client</th><th>Mobile</th><th>RM</th><th>Risk Profile</th><th>Monthly Savings</th><th>Mode</th><th>Planned SIP</th>
      <th title="Client's SIP in CRM right now">Current SIP</th><th title="MF amount invested (principal) — from CRM right now">MF Invested</th><th title="Client's MF value (AUM) in CRM right now">MF Value</th><th title="Equity holding value in CRM right now">Equity Value</th><th title="When the form was sent">At Send (SIP / MF / EQ)</th><th title="Investments with other brokers / banks (client's own answer)">Elsewhere</th><th></th></tr></thead><tbody>`;
    if(!list.length) h+=`<tr><td colspan="15" style="text-align:center;color:var(--gray);padding:16px">No profiles yet — right-click a client name in MF Investors → 📋 Send Profile Form</td></tr>`;
    list.forEach((r,i)=>{
      const d=subDate(r.p);
      h+=`<tr style="cursor:pointer" onclick="PROF.detail(${i})">
        <td>${d?d.toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'}):'—'}</td>
        <td><b>${esc(r.p.name)}</b> ${r.p.ipoApply && r.p.ipoApply!=='No'?`<span title="IPO: ${esc(r.p.ipoApply)}${r.p.ipoAmount?' · '+esc(r.p.ipoAmount):''}${r.p.ipoAccounts?' · '+esc(r.p.ipoAccounts)+' accounts':''}" style="font-size:.55rem;font-weight:800;color:#7c3aed;background:#f3e8ff;border-radius:6px;padding:1px 5px">IPO</span> `:''}${r.p.eqInterest && r.p.eqInterest!=='No'?`<span title="Share market: ${esc(r.p.eqInterest)}${r.p.eqStyle&&r.p.eqStyle.length?' · '+esc([].concat(r.p.eqStyle).join(', ')):''}" style="font-size:.7rem">📈</span> `:''}${r.e?'<span class="badge b-eq" style="font-size:.55rem">EQ</span>':''}${r.c?'<span class="badge b-mf" style="font-size:.55rem">MF</span>':''}${r.other?` <small style="color:#6d28d9;font-weight:700" title="Client is mapped to ${esc(r.other)} in the CRM — investment details are visible to them and Admin">🔒 ${esc(r.other)}</small>`:(r.c||r.e)?'':(r.p.blankForm?' <small style="color:#0f766e;font-weight:700" title="Blank form — not matched to CRM. Existing client? Open 👁️ → 🔗 Link to CRM Client">🆕 NEW</small>':' <small style="color:#b45309" title="Not matched to any client in CRM — open 👁️ → 🔗 Link to CRM Client">⚠️ not in CRM</small>')}</td>
        <td>${esc(r.p.mobile)}</td><td>${(()=>{ const R=rmOf(r); return `<b>${esc(R.main||'—')}</b>${R.sub.length?`<br><small style="color:var(--gray);white-space:nowrap" title="EQ = equity RM · 📤 = form sent by">${esc(R.sub.join(' · '))}</small>`:''}`; })()}</td>
        <td><b style="color:${riskC[r.p.riskProfile]||'var(--navy)'}">${esc(r.p.riskProfile||'—')}</b> <small>(${esc(r.p.riskScore??'')}/15)</small></td>
        <td>${esc(r.p.monthlySavings||'—')}</td><td>${esc([].concat(r.p.mode||[]).join(', ')||'—')}</td><td>${esc(r.p.sipAmount||'—')}</td>
        <td><b>${r.c?money(r.c.sip_amount):'—'}</b>${r.c&&r.c.sip_count?` <small>(${r.c.sip_count})</small>`:''}</td>
        <td><b>${r.c&&investedOf(r.c)?money(investedOf(r.c)):'—'}</b></td>
        <td><b>${r.c?money(r.c.aum):'—'}</b></td>
        <td><b>${r.e&&eqVal(r.e)?money(eqVal(r.e)):'—'}</b></td>
        <td>${r.s?`${money(r.s.sip)} / ${money(r.s.aum)} / ${r.s.eq?money(r.s.eq):'—'}<br><small style="color:var(--gray)">${fmtDate(String(r.s.at).slice(0,10))} by ${esc(r.s.by)}</small>`:'—'}</td>
        <td title="${esc(elsewhereText(r.p))}" style="max-width:220px;font-size:.62rem">${esc(elsewhereText(r.p))||'—'}</td>
        <td style="white-space:nowrap"><button class="btn-icon" title="View full profile">👁️</button>${window.PLAN?`<button class="btn-icon" title="Suggested Plan" onclick="event.stopPropagation();PROF.plan(${i})">💡</button>`:''}${CU.role==='admin'?`<button class="btn-icon" title="Delete this profile" style="color:var(--red)" onclick="event.stopPropagation();PROF.del(${i})">🗑️</button>`:''}</td></tr>`;
    });
    $('prof-table').innerHTML=h+'</tbody></table>';
    window._profRows=list;
  }
  function setStatus(t){ const el=$('prof-sync'); if(el) el.textContent=t||''; }
  async function open(full){
    $('profModal').classList.add('open');
    if(!_profiles.length){ const c=readCache(); if(c){ _profiles=c.p; _sends=c.s||[]; _links=c.l||{}; } }
    let afterClear=false; try{ afterClear=!localStorage.getItem(OK_MARK); }catch(_){}
    if(afterClear) full=true;   // 🧹 Clear Cache ke baad: list turant dikhao, peeche se poori fresh list
    if(_profiles.length){ render(); setStatus(full ? '🔄 Refreshing full list…' : '🔄 Checking for new profiles…'); }
    else $('prof-table').innerHTML='<div style="padding:20px;color:var(--gray)">Loading profiles… <small>(first load after Clear Cache can take a few seconds)</small></div>';
    watchCrmData();
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
  // Clear Cache ke baad CRM ka client data (MF / Equity) der se aata hai → tab tak profiles
  // "not in CRM" dikhti hain. Data aate hi list dobara banao (popup khula ho tab tak, max 60 sec).
  let _watchT=null;
  function watchCrmData(){
    clearInterval(_watchT);
    const sig=()=>{ const a=DB.get('mf_clients')||[], b=DB.get('eq_clients')||[]; return a.length+'|'+b.length; };
    let last=sig(), n=0;
    _watchT=setInterval(()=>{
      n++; const open=$('profModal')&&$('profModal').classList.contains('open');
      if(!open || n>40){ clearInterval(_watchT); return; }
      const now=sig(); if(now!==last){ last=now; try{ render(); }catch(_){} }
    },1500);
  }
  // "Elsewhere" ka chhota summary — sirf jo None nahi hai
  function elsewhereText(p){
    const parts=[];
    const add=(k,l)=>{ const v=p[k]; if(v && v!=='None') parts.push(l+' '+v); };
    add('oEquity','EQ'); add('oMf','MF'); add('oSip','SIP'); add('oFd','FD');
    const o=(Array.isArray(p.oOther)?p.oOther:[]).filter(x=>x!=='None'); if(o.length) parts.push(o.join(', '));
    return parts.join(' · ');
  }
  const LABELS=[['incomeSource','Income Source'],['monthlyIncome','Monthly Income'],['monthlySavings','Monthly Savings'],['loan','Loan'],['loanAmount','Loan Outstanding'],
    ['emiAmount','Monthly EMI'],['emergencyFund','Emergency Fund'],['healthCover','Health Cover'],['lifeCover','Life Cover'],
    ['oEquity','Elsewhere — Shares / Equity'],['oMf','Elsewhere — MF Holding'],['oSip','Elsewhere — Monthly SIP'],['oFd','Elsewhere — FD / Bonds'],['oOther','Elsewhere — Other Investments'],['otherDetails','Elsewhere — Broker / Details'],
    ['goal','Goals'],['mode','Investment Mode'],
    ['sipAmount','Planned SIP'],['period','Investment Period'],
    ['eqInterest','Equity — Interested'],['eqStyle','Equity — Trading Type'],['eqExp','Equity — Experience'],['eqCapital','Equity — Capital'],
    ['eqMonthly','Equity — Monthly Amount'],['eqFreq','Equity — Trading Frequency'],['eqHolding','Equity — Holding Period'],['eqLoss','Equity — Max Loss Bearable'],['eqSupport','Equity — Help Wanted'],
    ['ipoApply','IPO — Applies'],['ipoAmount','IPO — Amount per IPO'],['ipoAccounts','IPO — Family Demat Accounts'],['ipoGoal','IPO — Purpose'],['ipoUpi','IPO — UPI Linked'],['ipoHelp','IPO — Help Wanted'],['r1','Income Regularity'],['r2','If Market Falls 20%'],['r3','Equity / MF Experience'],['r4','Expectation'],['r5','Age']];
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
      <div style="font-size:.78rem;color:var(--gray);margin-bottom:8px">Mobile: <b>${esc(p.mobile)}</b> · RM: <b>${esc((()=>{ const R=rmOf(r); return [R.mf&&('MF: '+R.mf), R.eq&&('EQ: '+R.eq)].filter(Boolean).join(' · ') || R.main || '—'; })())}</b>${senderOf(r)?` · Form sent by: <b>${esc(senderOf(r))}</b>`:''} · Submitted: <b>${d?d.toLocaleString('en-IN'):'—'}</b></div>
      ${linkBox(i, r)}
      ${window.PLAN ? `<div style="text-align:right;margin:-4px 0 8px"><button class="btn" style="background:var(--navy);color:#f5d98a;border:none" onclick="PROF.plan(${i})">💡 Open Suggested Plan</button></div>` : ''}
      <div style="font-size:.7rem;font-weight:800;color:var(--gray);text-transform:uppercase;margin:4px 0 4px">Client's Answers</div>
      <table><tbody>${LABELS.filter(([k])=>v(k)).map(([k,l])=>`<tr><td style="color:var(--gray);width:42%">${l}</td><td style="font-weight:600">${esc(v(k))}</td></tr>`).join('')}</tbody></table>
      ${CU.role==='admin'?`<div style="text-align:right;margin-top:12px"><button class="btn btn-danger" onclick="PROF.del(${i})">🗑️ Delete Profile</button></div>`:''}`;
    $('profDetailModal').classList.add('open');
  }
  // ── 🔗 Manual link: profile ↔ CRM client ──
  function linkBox(i, r){
    const mob=cleanMobile(r.p.mobile), L=_links[mob];
    if(r.other) return `<div style="background:#ede9fe;border-radius:8px;padding:7px 10px;margin-bottom:10px;font-size:.74rem;color:#4c1d95">🔒 This client is mapped to <b>${esc(r.other)}</b> in the CRM. You can see the profile and plan because you sent the form — the client's investments (SIP / MF / Equity) are visible only to ${esc(r.other)} and Admin.</div>`;
    if(r.c||r.e){
      if(!L) return '';
      const nm=(r.c||r.e).name||'';
      return `<div style="background:#eef3fc;border-radius:8px;padding:6px 10px;margin-bottom:10px;font-size:.74rem;display:flex;justify-content:space-between;align-items:center;gap:6px;flex-wrap:wrap">
        <span>🔗 Linked manually to <b>${esc(nm)}</b> <small style="color:var(--gray)">(by ${esc(L.by||'—')}, ${esc(String(L.at||'').slice(0,10))})</small></span>
        <button class="btn btn-outline" style="padding:3px 9px;font-size:.7rem" onclick="PROF.unlink(${i})">✖ Unlink</button></div>`;
    }
    return `<div style="background:#fff4e5;border:1px solid #f5c98a;border-radius:10px;padding:9px 10px;margin-bottom:10px">
      <div style="font-size:.76rem;font-weight:800;color:#b45309;margin-bottom:5px">⚠️ This profile is not linked to any CRM client (mobile ${esc(r.p.mobile)} not found). Existing client? Link it:</div>
      <input id="plink-q" type="text" placeholder="🔍 Search name / PAN / mobile (min 3 letters)" oninput="PROF.linkSearch(${i})" style="width:100%;padding:7px 9px;border:1px solid var(--border);border-radius:8px;font-size:.8rem">
      <div id="plink-res" style="max-height:220px;overflow:auto;margin-top:5px"></div></div>`;
  }
  function linkCandidates(){
    let mf=[], eq=[];
    if(CU && CU.role==='admin'){ mf=DB.get('mf_clients')||[]; eq=DB.get('eq_clients')||[]; }
    else { try{ mf=getMyMfClients()||[]; }catch(_){} try{ eq=getMyEqClients()||[]; }catch(_){} }
    return mf.map(c=>({c,seg:'mf'})).concat(eq.map(c=>({c,seg:'equity'})));
  }
  function linkSearch(i){
    const q=($('plink-q')?.value||'').trim().toLowerCase(), box=$('plink-res'); if(!box) return;
    if(q.length<3){ box.innerHTML=''; return; }
    const qd=q.replace(/\D/g,'');
    const hits=linkCandidates().filter(({c})=> c && (String(c.name||'').toLowerCase().includes(q) || String(c.pan||'').toLowerCase().includes(q) || (qd.length>=4 && cleanMobile(c.mobile).includes(qd)))).slice(0,30);
    if(!hits.length){ box.innerHTML='<div style="font-size:.74rem;color:var(--gray);padding:6px">No client found</div>'; return; }
    box.innerHTML=hits.map(({c,seg})=>{
      const val = seg==='mf' ? 'MF '+money(c.aum)+(+c.sip_amount?' · SIP '+money(c.sip_amount):'') : 'EQ '+money(eqVal(c));
      return `<div onclick="PROF.link(${i},'${seg}','${esc(c.id)}')" style="display:flex;justify-content:space-between;gap:8px;padding:6px 8px;border-bottom:1px solid var(--border);cursor:pointer;font-size:.76rem" onmouseover="this.style.background='#F0F5FB'" onmouseout="this.style.background='transparent'">
        <span><span class="badge ${seg==='mf'?'b-mf':'b-eq'}" style="font-size:.55rem">${seg==='mf'?'MF':'EQ'}</span> <b>${esc(c.name)}</b> <small style="color:var(--gray)">${esc(c.mobile||'—')} · ${esc(c.pan||'no PAN')} · RM ${esc(c.rm||'—')}</small></span>
        <span style="white-space:nowrap;font-weight:700;color:var(--navy)">${val}</span></div>`;
    }).join('');
  }
  function reopen(pid){
    render();
    const j=(window._profRows||[]).findIndex(x=>x.p._id===pid);
    if(j>=0) detail(j); else { try{ closeModal('profDetailModal'); }catch(_){} }
  }
  async function link(i, seg, cid){
    const r=(window._profRows||[])[i]; if(!r) return;
    const c = seg==='equity' ? eqClient(cid) : mfClient(cid);
    if(!c){ toast('Client not found','error'); return; }
    if(!canSeeClient(c, seg)){ toast('This client is mapped to '+(c.rm||'another RM'),'error'); return; }
    const mob=cleanMobile(r.p.mobile); if(!mob){ toast('Profile has no mobile','error'); return; }
    if(!confirm(`Link this profile (${r.p.name}, ${r.p.mobile}) to CRM client:\n\n${c.name} (${seg==='mf'?'MF':'Equity'}, ${c.mobile||'no mobile'})?`)) return;
    const rec={cid:c.id, seg, by:CU.name||'', at:new Date().toISOString()};
    try{ await fdb.collection('crm_data').doc(LINK_KEY).set({links:{[mob]:rec}},{merge:true}); }
    catch(e){ toast('⚠️ Not linked — '+(e.message||e),'error'); return; }
    _links[mob]=rec; writeCache();
    toast('Linked ✓ — investments are now shown','success');
    reopen(r.p._id);
  }
  async function unlink(i){
    const r=(window._profRows||[])[i]; if(!r) return;
    const mob=cleanMobile(r.p.mobile); if(!_links[mob]) return;
    if(!confirm('Remove the manual link for '+r.p.name+'?')) return;
    try{ await fdb.collection('crm_data').doc(LINK_KEY).update({['links.'+mob]:firebase.firestore.FieldValue.delete()}); }
    catch(e){ toast('⚠️ Not removed — '+(e.message||e),'error'); return; }
    delete _links[mob]; writeCache();
    toast('Link removed','success');
    reopen(r.p._id);
  }
  // ══════ 📊 RM-wise: kitne link bheje / bhare / baaki (20-Nov-2026) ══════
  function filledFor(s){ return _profiles.some(p=>p.sendId===s.id || (s.cid && p.cid && p.cid===s.cid && subDate(p) && subDate(p).toISOString()>=s.at)); }
  const nk = v => String(v||'').trim().toLowerCase();
  // Profile kisne bhejwayi: send log (sendId) → blank form ka RM → us client ko aakhri bhejne wala → link ka RM
  function senderOfProfile(p){
    if(p.sendId){ const s=_sends.find(x=>x.id===p.sendId); if(s) return s.by||p.rm||''; }
    if(p.blankForm) return p.rm||'';
    if(p.cid){ const s=_sends.filter(x=>x.cid===p.cid).sort((a,b)=>String(b.at).localeCompare(String(a.at)))[0]; if(s) return s.by||''; }
    return p.rm||'';
  }
  let _sumRange='all', _sumOpen='';
  function inRange(iso){
    if(_sumRange==='all') return true;
    const t=Date.parse(iso)||0, now=new Date();
    if(_sumRange==='7') return t>=Date.now()-7*864e5;
    if(_sumRange==='30') return t>=Date.now()-30*864e5;
    if(_sumRange==='month') return t>=new Date(now.getFullYear(),now.getMonth(),1).getTime();
    return true;
  }
  function sumData(){
    const isAdmin=CU.role==='admin', me=nk(CU.name);
    const R={}; const row=n=>{ const k=nk(n)||'(unknown)'; return R[k]||(R[k]={name:n||'(unknown)',sent:0,clients:new Set(),pend:new Map(),blank:0,blankFilled:0,filled:0,last:'',blankList:[],sentList:[]}); };
    // Bheje gaye links
    _sends.filter(s=>inRange(s.at)).forEach(s=>{
      const x=row(s.by); if(String(s.at)>x.last) x.last=String(s.at);
      if(s.blank){ x.blank++; return; }
      x.sent++; x.clients.add(s.cid);
    });
    // Pending: client ka aakhri send abhi tak nahi bhara
    const latest=new Map(); _sends.filter(s=>!s.blank && s.cid && inRange(s.at)).forEach(s=>{ const k=nk(s.by)+'|'+s.cid; const o=latest.get(k); if(!o || String(s.at)>String(o.at)) latest.set(k,{...s, times:(o?o.times:0)+1}); else o.times++; });
    latest.forEach(s=>{ if(!filledFor(s)) row(s.by).pend.set(s.cid, s); });
    // Bhare gaye profiles (ek mobile = ek, sabse naya)
    const seen=new Set();
    _profiles.forEach(p=>{ const m=cleanMobile(p.mobile)||p._id; if(seen.has(m)) return; seen.add(m);
      if(!inRange(p._ms?new Date(p._ms).toISOString():'')) return;
      const by=senderOfProfile(p); const x=row(by); x.filled++; if(p.blankForm){ x.blankFilled++; x.blankList.push(p); } else x.sentList.push(p); });
    let list=Object.values(R);
    if(!isAdmin) list=list.filter(x=>nk(x.name)===me);
    return list.sort((a,b)=>(b.sent+b.blank)-(a.sent+a.blank) || a.name.localeCompare(b.name));
  }
  async function summary(){
    $('profSumModal').classList.add('open');
    if(!_profiles.length){ const c=readCache(); if(c){ _profiles=c.p; _sends=c.s||[]; _links=c.l||{}; } }
    renderSummary();
    if(!_loaded){ try{ await load(false); renderSummary(); }catch(e){ console.warn('summary load',e); } }
  }
  // Summary se seedha profile / plan kholna (main list ka search / filter hata kar)
  function openById(pid, what){
    let list=window._profRows||[]; let j=list.findIndex(r=>r.p._id===pid);
    if(j<0){ const sr=$('prof-search'), rk=$('prof-risk'); if(sr) sr.value=''; if(rk) rk.value=''; render(); list=window._profRows||[]; j=list.findIndex(r=>r.p._id===pid); }
    if(j<0){ toast('Profile not found in the list — click 🔄 Refresh','error'); return; }
    try{ closeModal('profSumModal'); }catch(_){}
    if(what==='plan') plan(j); else detail(j);
  }
  function sumRange(v){ _sumRange=v; renderSummary(); }
  function sumToggle(n){ _sumOpen = _sumOpen===n ? '' : n; renderSummary(); }
  function renderSummary(){
    const body=$('profSumBody'); if(!body) return;
    const L=sumData();
    const T=L.reduce((t,x)=>{ t.cl+=x.clients.size; t.pend+=x.pend.size; t.blank+=x.blank; t.bf+=x.blankFilled; t.f+=x.filled; return t; },{cl:0,pend:0,blank:0,bf:0,f:0});
    const pctN=(a,b)=>b?Math.round(a/b*100):null;
    const pcol=n=>n==null?'#94a3b8':n>=60?'#0f7a45':n>=30?'#b45309':'#c0392b';
    const fillCell=n=>n==null?'<span style="color:#94a3b8">—</span>':`<div style="display:flex;align-items:center;gap:6px;justify-content:center"><div style="width:54px;height:7px;background:#e5e7eb;border-radius:4px;overflow:hidden"><div style="width:${n}%;height:100%;background:${pcol(n)}"></div></div><b style="color:${pcol(n)};min-width:34px;text-align:right">${n}%</b></div>`;
    const fdt=iso=>iso?new Date(iso).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'}):'—';
    const rng=[['all','All time'],['month','This month'],['30','Last 30 days'],['7','Last 7 days']].map(([v,l])=>`<option value="${v}"${v===_sumRange?' selected':''}>${l}</option>`).join('');
    const tFill=pctN(T.cl-T.pend,T.cl);
    const C='text-align:center'; const TH=`style="${C};white-space:nowrap"`;
    let h=`<style>
      #profSumBody .sumT{width:100%;border-collapse:separate;border-spacing:0;font-size:.8rem;table-layout:fixed}
      #profSumBody .sumT th{background:var(--navy);color:#fff;font-size:.66rem;letter-spacing:.4px;text-transform:uppercase;padding:9px 6px;line-height:1.25}
      #profSumBody .sumT th:first-child{border-radius:10px 0 0 0;text-align:left;padding-left:12px} #profSumBody .sumT th:last-child{border-radius:0 10px 0 0}
      #profSumBody .sumT td{padding:9px 6px;border-bottom:1px solid var(--border)}
      #profSumBody .sumT tr.rmrow:hover td{background:#F0F5FB}
      #profSumBody .sub{border:1px solid var(--border);border-radius:10px;overflow:hidden;margin:4px 0 10px;background:#fff}
      #profSumBody .sub h4{margin:0;padding:7px 10px;font-size:.72rem;text-transform:uppercase;letter-spacing:.4px}
      #profSumBody .sub table{width:100%;border-collapse:collapse;font-size:.76rem;table-layout:fixed}
      #profSumBody .sub th{background:#f4f6fa;color:var(--gray);font-size:.62rem;text-transform:uppercase;letter-spacing:.3px;padding:6px 8px;text-align:left}
      #profSumBody .sub td{padding:6px 8px;border-top:1px solid #eef1f6;vertical-align:middle;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    </style>
    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:12px">
      <select onchange="PROF.sumRange(this.value)" style="padding:7px 10px;border:1px solid var(--border);border-radius:8px;font-weight:600">${rng}</select>
      <span style="font-size:.76rem;color:var(--gray)">Click an RM row to see pending, filled and blank-form clients.</span></div>
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:8px;margin-bottom:14px">
      ${[['Clients Sent',T.cl,'#1a56db','Unique clients sent a form from the CRM'],['Filled',T.cl-T.pend,'#0f7a45','Clients who filled the form they were sent'],['Pending',T.pend,'#b45309','Sent but not filled yet'],['Fill Rate',tFill==null?'—':tFill+'%',pcol(tFill),'Filled ÷ Clients Sent'],['Blank Forms Sent',T.blank,'#0f766e','Blank form links shared'],['Total Profiles',T.f,'#0f1f3d','All profiles received in this period']]
        .map(([l,v,c,t])=>`<div title="${t}" style="background:#fff;border:1px solid var(--border);border-top:4px solid ${c};border-radius:10px;padding:9px 12px"><div style="font-size:.64rem;color:var(--gray);font-weight:800;text-transform:uppercase;letter-spacing:.4px">${l}</div><div style="font-weight:900;color:${c};font-size:1.4rem;line-height:1.3">${v}</div></div>`).join('')}
    </div>
    <div style="overflow-x:auto"><table class="sumT"><colgroup><col style="width:17%"><col><col><col><col><col style="width:14%"><col><col><col><col style="width:12%"></colgroup>
      <thead><tr><th>RM</th>
        <th ${TH} title="Unique clients sent a form from the CRM">Clients<br>Sent</th>
        <th ${TH} title="Total links sent, including resends">Total<br>Links</th>
        <th ${TH}>Filled</th><th ${TH}>Pending</th><th ${TH}>Fill Rate</th>
        <th ${TH} title="Blank form links shared">Blank<br>Sent</th><th ${TH} title="Profiles received through a blank form">Blank<br>Filled</th>
        <th ${TH} title="All profiles credited to this RM (sent + blank)">Total<br>Profiles</th><th ${TH}>Last<br>Sent</th></tr></thead><tbody>`;
    if(!L.length) h+=`<tr><td colspan="10" style="${C};color:var(--gray);padding:16px">No forms sent in this period</td></tr>`;
    L.forEach(x=>{
      const cl=x.clients.size, f=cl-x.pend.size, open=_sumOpen===x.name;
      const nm=esc(x.name).replace(/'/g,"\\'");
      const num=(v,c)=>`<td style="${C};font-weight:700;${v&&c?'color:'+c:v?'':'color:#94a3b8'}">${v}</td>`;
      h+=`<tr class="rmrow" style="cursor:pointer;${open?'background:#F0F5FB':''}" onclick="PROF.sumToggle('${nm}')">
        <td style="padding-left:12px"><span style="display:inline-block;width:14px;color:var(--gray)">${open?'▾':'▸'}</span><b>${esc(x.name)}</b></td>
        ${num(cl,'#1a56db')}${num(x.sent)}${num(f,'#0f7a45')}${num(x.pend.size,'#b45309')}
        <td>${fillCell(pctN(f,cl))}</td>
        ${num(x.blank,'#0f766e')}${num(x.blankFilled,'#0f766e')}${num(x.filled,'#0f1f3d')}
        <td style="${C};color:var(--gray);font-size:.74rem;white-space:nowrap">${x.last?fdt(x.last):'—'}</td></tr>`;
      if(!open) return;
      const P=[...x.pend.values()].sort((a,b)=>String(a.at).localeCompare(String(b.at)));
      const segB=sg=>`<span class="badge ${sg==='equity'?'b-eq':'b-mf'}" style="font-size:.55rem;margin-left:4px">${sg==='equity'?'EQ':'MF'}</span>`;
      let inner='';
      if(P.length) inner+=`<div class="sub"><h4 style="background:#fff4e5;color:#b45309">⏳ Pending — sent, not filled (${P.length}) · oldest first</h4>
        <table><colgroup><col style="width:34%"><col style="width:16%"><col style="width:16%"><col style="width:18%"><col style="width:16%"></colgroup>
        <thead><tr><th>Client</th><th>Mobile</th><th>Sent On</th><th>Waiting</th><th style="text-align:right">Action</th></tr></thead><tbody>
        ${P.map(s=>{ const days=Math.floor((Date.now()-Date.parse(s.at))/864e5);
          return `<tr><td title="${esc(s.name)}"><b>${esc(s.name)}</b>${segB(s.seg)}</td><td>${esc(s.mobile||'—')}</td><td>${fdt(s.at)}</td>
          <td><b style="color:${days>=7?'#c0392b':days>=3?'#b45309':'var(--gray)'}">${days===0?'Today':days+' day'+(days>1?'s':'')}</b>${s.times>1?` <small style="color:var(--gray)">· sent ${s.times}×</small>`:''}</td>
          <td style="text-align:right"><button class="btn btn-outline" style="padding:3px 10px;font-size:.7rem" onclick="event.stopPropagation();PROF.send('${esc(s.cid)}','wa','${s.seg==='equity'?'equity':'mf'}')">💬 Resend</button></td></tr>`; }).join('')}
        </tbody></table></div>`;
      else if(cl) inner+=`<div class="sub"><h4 style="background:#e3f7ee;color:#0f7a45">✅ All forms sent from the CRM have been filled</h4></div>`;
      const filledTbl=(list,title,bg,col)=> !list.length ? '' : `<div class="sub"><h4 style="background:${bg};color:${col}">${title} (${list.length}) · newest first</h4>
        <table><colgroup><col style="width:26%"><col style="width:14%"><col style="width:14%"><col style="width:13%"><col style="width:23%"><col style="width:10%"></colgroup>
        <thead><tr><th>Client</th><th>Mobile</th><th>Filled On</th><th>Risk</th><th>CRM Match</th><th style="text-align:right">Open</th></tr></thead><tbody>
        ${list.slice().sort((a,b)=>(b._ms||0)-(a._ms||0)).map(p=>{
          const pr=pairForProfile(p), c=pr.mf||pr.eq, d=subDate(p);
          const tag = c ? `<span style="color:#0f7a45;font-weight:700" title="${esc(c.name)} · ${pr.mf?'MF':'EQ'} RM ${esc(c.rm||'—')}">✓ ${pr.mf?'MF':'EQ'} · RM ${esc(c.rm||'—')}</span>`
                        : (p.blankForm?'<span style="color:#0f766e;font-weight:700">🆕 New — not in CRM</span>':'<span style="color:#b45309;font-weight:700">⚠️ Not in CRM</span>');
          const pid=esc(p._id).replace(/'/g,"\\'");
          return `<tr><td title="${esc(p.name)}"><b>${esc(p.name)}</b></td><td>${esc(p.mobile||'—')}</td><td>${d?d.toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'}):'—'}</td>
            <td><b style="color:${{Conservative:'#0369a1',Moderate:'#b45309',Aggressive:'#b91c1c'}[p.riskProfile]||'var(--navy)'}">${esc(p.riskProfile||'—')}</b></td><td>${tag}</td>
            <td style="text-align:right"><button class="btn-icon" title="View profile" onclick="event.stopPropagation();PROF.openById('${pid}','detail')">👁️</button>${window.PLAN?`<button class="btn-icon" title="Suggested plan" onclick="event.stopPropagation();PROF.openById('${pid}','plan')">💡</button>`:''}</td></tr>`; }).join('')}
        </tbody></table></div>`;
      inner += filledTbl(x.blankList,'🆕 Filled via blank form','#e0f7f4','#0f766e') + filledTbl(x.sentList,'✅ Filled via CRM link','#e3f7ee','#0f7a45');
      h+=`<tr><td colspan="10" style="background:#f8fafc;padding:8px 12px 4px">${inner||'<div style="font-size:.76rem;color:var(--gray);padding:6px">No activity in this period</div>'}</td></tr>`;
    });
    body.innerHTML=h+`</tbody></table></div><p style="font-size:.7rem;color:var(--gray);margin-top:10px">Note: blank-form links are counted from 20-Nov-2026 onwards. Earlier blank forms still appear under "Blank Filled" and "Total Profiles".</p>`;
  }
  // 💡 Suggested Plan — separate popup (not inside the profile detail) — 20-Nov-2026
  function plan(i){
    const r=(window._profRows||[])[i]; if(!r || !window.PLAN) return;
    try{ closeModal('profDetailModal'); }catch(_){}
    const p=r.p, R=rmOf(r);
    $('profPlanTitle').textContent='💡 Plan — '+(p.name||'Client');
    $('profPlanBody').innerHTML=`<div style="font-size:.76rem;color:var(--gray);margin-bottom:8px">Mobile: <b>${esc(p.mobile)}</b> · RM: <b>${esc(R.main||'—')}</b>${R.sub.length?' <small>('+esc(R.sub.join(' · '))+')</small>':''} · Risk: <b>${esc(p.riskProfile||'—')}</b>
      <button class="btn btn-outline" style="padding:3px 9px;font-size:.7rem;margin-left:6px" onclick="closeModal('profPlanModal');PROF.detail(${i})">👁️ View Profile</button></div>${PLAN.html(i)}`;
    $('profPlanModal').classList.add('open');
  }
  function box(l,v){ return `<div style="background:var(--bg);border-radius:10px;padding:8px 10px"><div style="font-size:.62rem;color:var(--gray);font-weight:700;text-transform:uppercase">${l}</div><div style="font-weight:800;color:var(--navy);font-size:.9rem">${esc(v)}</div></div>`; }
  function exportXlsx(){
    const list=rows();
    const cols=[{header:'Submitted',width:18},{header:'Client',width:26},{header:'Mobile',width:12},{header:'RM',width:12},{header:'EQ RM',width:12},{header:'Form Sent By',width:13},
      {header:'Current SIP (₹)',width:14,money:true},{header:'SIP Count',width:9,num:true},{header:'MF Invested (₹)',width:14,money:true},{header:'MF Value (₹)',width:14,money:true},{header:'Equity Value (₹)',width:14,money:true},
      {header:'SIP at Send (₹)',width:14,money:true},{header:'MF Value at Send (₹)',width:16,money:true},{header:'Sent On',width:12},
      {header:'Risk Score',width:9,num:true},{header:'Risk Profile',width:13}, ...LABELS.map(([,l])=>({header:l,width:18}))];
    const data=list.map(r=>{ const p=r.p, d=subDate(p);
      return [d?d.toLocaleString('en-IN'):'', p.name, p.mobile, rmOf(r).main, rmOf(r).eq||'', senderOf(r),
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
  // Admin: profile delete — us client (mobile) ke saare purane records bhi
  async function del(i){
    if(!CU || CU.role!=='admin') return;
    const r=(window._profRows||[])[i]; if(!r) return;
    const mob=cleanMobile(r.p.mobile);
    const docs=_profiles.filter(p=> p._id===r.p._id || (mob && cleanMobile(p.mobile)===mob));
    if(!confirm(`Delete the profile of ${r.p.name} (${r.p.mobile})?${docs.length>1?`\n\n${docs.length} records of this client will be deleted (including older submissions).`:''}\n\nThis cannot be undone.`)) return;
    const ids=docs.map(p=>p._id);
    try{
      await Promise.all(ids.map(id=>fdb.collection(COL).doc(id).delete()));
      // doosre browsers ke cache se bhi hatane ke liye "deleted" list
      try{ await fdb.collection('crm_data').doc('profile_deleted').set({ids:firebase.firestore.FieldValue.arrayUnion(...ids)},{merge:true}); }catch(_){}
    }catch(e){ toast('⚠️ Not deleted — '+(e.message||e),'error'); return; }
    ids.forEach(id=>_deleted.add(id));
    _profiles=_profiles.filter(p=>!ids.includes(p._id));
    writeCache();
    try{ closeModal('profDetailModal'); }catch(_){}
    render();
    toast('Profile deleted','success');
  }
  // Blank form — naye client ke liye (CRM me abhi nahi). RM ka naam link me jaata hai.
  function blankLink(sendId){
    const base=location.origin+location.pathname.replace(/[^/]*$/,'')+'dninvest-profile.html';
    const q={rm:CU&&CU.name||''}; if(sendId) q.s=sendId;
    return base+'?'+new URLSearchParams(q).toString();
  }
  async function blank(mode){
    // Blank form bhi "sent" log me (RM-wise ginti ke liye) — 20-Nov-2026
    const sendId=uid();
    const link=blankLink(sendId);
    recordSend({id:sendId, blank:true, cid:'', seg:'', name:'(Blank form)', mobile:'', rm:CU.name||'', by:CU.name||'', at:new Date().toISOString(), via:mode==='copy'?'copy':'whatsapp'})
      .then(r=>{ if(!r.ok) console.log('blank send not logged:', r.error); });
    if(mode==='copy'){
      try{ await navigator.clipboard.writeText(link); toast('Blank form link copied ✓','success'); }catch(_){ prompt('Copy this link:', link); }
      return;
    }
    const msg=`Greetings from D N Investment!\n\nPlease fill this short investment profile form (takes about 2 minutes) so we can suggest the right plan for you:\n${link}\n\nThank you,\n${CU&&CU.name||''}\nD N Investment`;
    window.open('https://wa.me/?text='+encodeURIComponent(msg),'_blank');   // WhatsApp me contact khud chunein
  }
  async function openFor(cid, seg){
    const c = seg==='equity' ? eqClient(cid) : mfClient(cid);
    await open();
    const s=$('prof-search'); if(s && c){ s.value=cleanMobile(c.mobile)||c.name; render(); }
  }
  // ⚡ Prefetch (20-Nov-2026): login ke baad CRM ka data aate hi profiles PEECHE se load ho jaati hain
  // → 🧹 Clear Cache / naye browser ke baad bhi "📋 Client Profiles" kholte hi list taiyaar milti hai.
  (function prefetch(){
    let tries=0;
    const tick=()=>{
      tries++;
      const ready = typeof CU!=='undefined' && CU && typeof fdb!=='undefined' && fdb && typeof DB!=='undefined' && (DB.get('mf_clients')||[]).length;
      if(!ready){ if(tries<60) setTimeout(tick,3000); return; }   // max ~3 min wait
      let need=!_profiles.length;
      try{ if(!localStorage.getItem(OK_MARK)) need=true; }catch(_){}
      if(!_profiles.length){ const c=readCache(); if(c){ _profiles=c.p; _sends=c.s||[]; _links=c.l||{}; } }
      const go=()=>load(need).then(()=>{ if($('profModal')?.classList.contains('open')) render(); }).catch(e=>console.warn('profile prefetch failed',e));
      (window.requestIdleCallback ? requestIdleCallback(go,{timeout:8000}) : setTimeout(go,2000));
    };
    setTimeout(tick,4000);
  })();
  return { send, open, openFor, render, detail, exportXlsx, menuHtml, del, blank, linkSearch, link, unlink, summary, sumRange, sumToggle, plan, openById };
})();
