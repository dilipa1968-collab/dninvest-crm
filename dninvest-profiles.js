/* ═══════════════════════════════════════════════════════════════════════
   CLIENT PROFILE FORM ↔ CRM  (19-Nov-2026)
   - MF Investors me client ke naam par RIGHT-CLICK → "📋 Send Profile Form"
     → WhatsApp par client ko personal link jaata hai (naam/mobile pehle se bhare).
   - Bhejte waqt client ka us din ka SIP / MF value (AUM) snapshot
     crm_data/profile_sends me save hota hai (transaction-safe).
   - Client form bharta hai → Firestore "clientProfiles" (dninvest-profile.html).
   - MF Investors → "📋 Client Profiles" → saare profiles + client ka CURRENT
     SIP aur MF value (aur bhejte waqt ka bhi) — Excel export ke saath.
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

  function mfClient(id){ return (DB.get('mf_clients')||[]).find(c=>c.id===id); }
  function cleanMobile(m){ const d=String(m||'').replace(/\D/g,''); return d.length>10 ? d.slice(-10) : d; }
  function canSeeClient(c){
    if(!CU) return false;
    if(CU.role==='admin') return true;
    try{ return getMyMfClients().some(x=>x.id===c.id); }catch(_){ return (c.rm||'').toLowerCase()===(CU.name||'').toLowerCase(); }
  }
  function linkFor(c, sendId){
    const base=location.origin+location.pathname.replace(/[^/]*$/,'')+'dninvest-profile.html';
    const p=new URLSearchParams({cid:c.id, rm:c.rm||CU.name||'', n:c.name||'', m:cleanMobile(c.mobile)});
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

  async function send(cid, mode){
    const c=mfClient(cid);
    if(!c){ toast('Client not found','error'); return; }
    if(!canSeeClient(c)){ toast('This investor is mapped to '+(c.rm||'another RM')+' — you can send only to your own investors','error'); return; }
    const mob=cleanMobile(c.mobile);
    const sendId=uid();
    const link=linkFor(c, sendId);
    // Bhejte waqt ka SIP / MF value snapshot
    const rec={ id:sendId, cid:c.id, name:c.name||'', mobile:mob, rm:c.rm||'', by:CU.name, at:new Date().toISOString(),
      sip:+c.sip_amount||0, sip_count:+c.sip_count||0, aum:+c.aum||0, via:mode==='copy'?'copy':'whatsapp' };
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

  async function load(){
    if(typeof fdb==='undefined'){ await window.waitForFdb?.(8000); }
    const [ps, sd]=await Promise.all([
      fdb.collection(COL).orderBy('submittedAt','desc').limit(2000).get(),
      fdb.collection('crm_data').doc(SEND_KEY).get()
    ]);
    _profiles=ps.docs.map(d=>({_id:d.id, ...d.data()}));
    _sends=(sd.exists && Array.isArray(sd.data().data)) ? sd.data().data : [];
    _loaded=true;
  }
  const subDate = p => p.submittedAt?.toDate ? p.submittedAt.toDate() : null;
  // Profile → CRM client: pehle link ka cid, warna mobile se
  function clientOf(p){
    const all=DB.get('mf_clients')||[];
    if(p.cid){ const c=all.find(x=>x.id===p.cid); if(c) return c; }
    const m=cleanMobile(p.mobile); if(!m) return null;
    const hits=all.filter(x=>cleanMobile(x.mobile)===m);
    return hits.length===1 ? hits[0] : null;
  }
  function sendOf(p, c){
    if(p.sendId){ const s=_sends.find(x=>x.id===p.sendId); if(s) return s; }
    const cidx=c?c.id:p.cid; if(!cidx) return null;
    return _sends.filter(x=>x.cid===cidx).sort((a,b)=>String(b.at).localeCompare(String(a.at)))[0]||null;
  }
  function rows(){
    const q=($('prof-search')?.value||'').trim().toLowerCase();
    const rf=$('prof-risk')?.value||'';
    return _profiles.map(p=>{ const c=clientOf(p); return {p, c, s:sendOf(p,c)}; })
      .filter(r=>{
        if(CU.role!=='admin'){ if(!r.c || !canSeeClient(r.c)) return false; }
        if(rf && r.p.riskProfile!==rf) return false;
        if(q && !([r.p.name,r.p.mobile,r.c&&r.c.rm,r.p.rm].join(' ').toLowerCase().includes(q))) return false;
        return true;
      });
  }
  function render(){
    const list=rows();
    const pend=(CU.role==='admin' ? _sends : _sends.filter(s=>{ const c=mfClient(s.cid); return c && canSeeClient(c); }))
      .filter(s=>!_profiles.some(p=>p.sendId===s.id || (p.cid && p.cid===s.cid && subDate(p) && subDate(p).toISOString()>=s.at)));
    const pc=$('prof-pending'); if(pc) pc.textContent = pend.length ? `⏳ ${pend.length} sent, not filled yet` : '';
    $('prof-count').textContent=list.length+' profile'+(list.length===1?'':'s');
    const riskC={Conservative:'#0369a1',Moderate:'#b45309',Aggressive:'#b91c1c'};
    let h=`<table><thead><tr><th>Date</th><th>Client</th><th>Mobile</th><th>RM</th><th>Risk Profile</th><th>Monthly Savings</th><th>Mode</th><th>Planned SIP</th>
      <th title="Client's SIP in CRM right now">Current SIP</th><th title="Client's MF value (AUM) in CRM right now">Current MF Value</th><th title="When the form was sent">SIP / MF Value at Send</th><th></th></tr></thead><tbody>`;
    if(!list.length) h+=`<tr><td colspan="12" style="text-align:center;color:var(--gray);padding:16px">No profiles yet — right-click a client name in MF Investors → 📋 Send Profile Form</td></tr>`;
    list.forEach((r,i)=>{
      const d=subDate(r.p);
      h+=`<tr style="cursor:pointer" onclick="PROF.detail(${i})">
        <td>${d?d.toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'}):'—'}</td>
        <td><b>${esc(r.p.name)}</b>${r.c?'':' <small style="color:#b45309" title="Not matched to an MF investor in CRM">⚠️ not in CRM</small>'}</td>
        <td>${esc(r.p.mobile)}</td><td>${esc((r.c&&r.c.rm)||r.p.rm||'—')}</td>
        <td><b style="color:${riskC[r.p.riskProfile]||'var(--navy)'}">${esc(r.p.riskProfile||'—')}</b> <small>(${esc(r.p.riskScore??'')}/15)</small></td>
        <td>${esc(r.p.monthlySavings||'—')}</td><td>${esc(r.p.mode||'—')}</td><td>${esc(r.p.sipAmount||'—')}</td>
        <td><b>${r.c?money(r.c.sip_amount):'—'}</b>${r.c&&r.c.sip_count?` <small>(${r.c.sip_count})</small>`:''}</td>
        <td><b>${r.c?money(r.c.aum):'—'}</b></td>
        <td>${r.s?`${money(r.s.sip)} / ${money(r.s.aum)}<br><small style="color:var(--gray)">${fmtDate(String(r.s.at).slice(0,10))} by ${esc(r.s.by)}</small>`:'—'}</td>
        <td><button class="btn-icon" title="View full profile">👁️</button></td></tr>`;
    });
    $('prof-table').innerHTML=h+'</tbody></table>';
    window._profRows=list;
  }
  async function open(){
    $('profModal').classList.add('open');
    $('prof-table').innerHTML='<div style="padding:20px;color:var(--gray)">Loading profiles…</div>';
    try{ await load(); render(); }
    catch(e){ $('prof-table').innerHTML=`<div style="padding:20px;color:var(--red)">⚠️ Could not load profiles — ${esc(e.message)}</div>`; }
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
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px">
        ${box('Risk Profile',(p.riskProfile||'—')+' ('+(p.riskScore??'')+'/15)')}
        ${box('Current SIP',r.c?money(r.c.sip_amount)+(r.c.sip_count?' ('+r.c.sip_count+' SIPs)':''):'—')}
        ${box('Current MF Value',r.c?money(r.c.aum):'—')}
        ${box('At Send',r.s?money(r.s.sip)+' / '+money(r.s.aum):'—')}
      </div>
      <div style="font-size:.78rem;color:var(--gray);margin-bottom:8px">Mobile: <b>${esc(p.mobile)}</b> · RM: <b>${esc((r.c&&r.c.rm)||p.rm||'—')}</b> · Submitted: <b>${d?d.toLocaleString('en-IN'):'—'}</b></div>
      <table><tbody>${LABELS.filter(([k])=>v(k)).map(([k,l])=>`<tr><td style="color:var(--gray);width:42%">${l}</td><td style="font-weight:600">${esc(v(k))}</td></tr>`).join('')}</tbody></table>`;
    $('profDetailModal').classList.add('open');
  }
  function box(l,v){ return `<div style="background:var(--bg);border-radius:10px;padding:8px 10px"><div style="font-size:.62rem;color:var(--gray);font-weight:700;text-transform:uppercase">${l}</div><div style="font-weight:800;color:var(--navy);font-size:.9rem">${esc(v)}</div></div>`; }
  function exportXlsx(){
    const list=rows();
    const cols=[{header:'Submitted',width:18},{header:'Client',width:26},{header:'Mobile',width:12},{header:'RM',width:12},
      {header:'Current SIP (₹)',width:14,money:true},{header:'SIP Count',width:9,num:true},{header:'Current MF Value (₹)',width:16,money:true},
      {header:'SIP at Send (₹)',width:14,money:true},{header:'MF Value at Send (₹)',width:16,money:true},{header:'Sent On',width:12},
      {header:'Risk Score',width:9,num:true},{header:'Risk Profile',width:13}, ...LABELS.map(([,l])=>({header:l,width:18}))];
    const data=list.map(r=>{ const p=r.p, d=subDate(p);
      return [d?d.toLocaleString('en-IN'):'', p.name, p.mobile, (r.c&&r.c.rm)||p.rm||'',
        r.c?(+r.c.sip_amount||0):'', r.c?(+r.c.sip_count||0):'', r.c?(+r.c.aum||0):'',
        r.s?r.s.sip:'', r.s?r.s.aum:'', r.s?fmtDate(String(r.s.at).slice(0,10)):'',
        p.riskScore??'', p.riskProfile||'', ...LABELS.map(([k])=>Array.isArray(p[k])?p[k].join(', '):(p[k]||''))]; });
    dnXlsx('Client_Profiles_'+today()+'.xlsx','D N Investment — Client Profiles ('+today()+')',cols,data);
  }
  // Right-click menu ke upar ka hissa (MF investor ke liye)
  function menuHtml(cid){
    const it=(icon,label,js,title)=>`<div onclick="${js};closeClientSeminarMenu()" title="${title||''}" style="padding:8px 10px;border-radius:7px;cursor:pointer;font-weight:600" onmouseover="this.style.background='#F0F5FB'" onmouseout="this.style.background='transparent'">${icon} ${label}</div>`;
    return `<div style="padding:7px 10px;font-weight:700;color:var(--navy);border-bottom:1px solid var(--border,#eee);margin-bottom:2px">📋 Client Profile Form</div>`
      + it('💬','Send Profile Form (WhatsApp)',`PROF.send('${cid}','wa')`,'Opens WhatsApp with a personal form link for this client')
      + it('🔗','Copy Profile Form Link',`PROF.send('${cid}','copy')`)
      + it('👁️','View Submitted Profiles',`PROF.openFor('${cid}')`)
      + `<div style="height:6px;border-bottom:1px solid var(--border,#eee);margin-bottom:4px"></div>`;
  }
  async function openFor(cid){
    const c=mfClient(cid);
    await open();
    const s=$('prof-search'); if(s && c){ s.value=cleanMobile(c.mobile)||c.name; render(); }
  }
  return { send, open, openFor, render, detail, exportXlsx, menuHtml };
})();
