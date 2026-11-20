/* ═══════════════════════════════════════════════════════════════════════
   💡 SUGGESTED PLAN — Client Profile se apne aap banne wala plan (20-Nov-2026)
   - Client Profiles → 👁️ detail popup me dikhta hai (koi button nahi dabana).
   - 5 Health Checks (🔴🟡🟢): Emergency Fund, Life Cover, Health Cover,
     EMI Load, Savings Rate.
   - Suggested monthly amount (savings ke hisab se) + Emergency Fund ka hissa
     + category-wise SIP split (risk profile, period, goals, direct shares).
   - RM ke liye "Talking Points" (sirf CRM me — client ko nahi jaate).
   - 📤 WhatsApp (Hindi + English) aur 🖨️ Print / PDF (DN Investment logo).
   - RM amount badal sakta hai → split dobara ban jaata hai.
   Sab fixed rules (if-else) hain — koi AI / internet nahi. Final faisla RM ka.
   Depends on: dninvest-profiles.js (window._profRows), escapeHtml, toast, CU.
   ═══════════════════════════════════════════════════════════════════════ */
window.PLAN = (function(){
  const esc = s => (typeof escapeHtml==='function') ? escapeHtml(String(s==null?'':s)) : String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const K = s => String(s||'').replace(/[^a-z0-9]/gi,'').toLowerCase();   // dash / space ka farak na pade
  const r500 = n => Math.max(0, Math.round(n/500)*500);
  const inr = n => '₹'+Math.round(n).toLocaleString('en-IN');
  const big = n => n>=1e7 ? '₹'+(+(n/1e7).toFixed(2))+' Cr' : n>=1e5 ? '₹'+(+(n/1e5).toFixed(1))+' L' : inr(n);
  const bigHi = n => n>=1e7 ? '₹'+(+(n/1e7).toFixed(2))+' करोड़' : n>=1e5 ? '₹'+(+(n/1e5).toFixed(1))+' लाख' : inr(n);

  // Form ke option → [low, high]  (Above wale me high andaze se)
  const B = (o)=>{ const m={}; Object.entries(o).forEach(([k,v])=>m[K(k)]=v); return m; };
  const BANDS = {
    monthlyIncome: B({"Below ₹25,000":[15000,25000],"₹25,000 – 50,000":[25000,50000],"₹50,000 – 1 Lakh":[50000,100000],"₹1 – 2 Lakh":[100000,200000],"Above ₹2 Lakh":[200000,300000]}),
    monthlySavings: B({"Below ₹5,000":[2000,5000],"₹5,000 – 10,000":[5000,10000],"₹10,000 – 25,000":[10000,25000],"₹25,000 – 50,000":[25000,50000],"Above ₹50,000":[50000,75000]}),
    emiAmount: B({"Below ₹5,000":[0,5000],"₹5,000 – 15,000":[5000,15000],"₹15,000 – 30,000":[15000,30000],"Above ₹30,000":[30000,45000]}),
    loanAmount: B({"Below ₹5 Lakh":[0,5e5],"₹5 – 20 Lakh":[5e5,20e5],"₹20 – 50 Lakh":[20e5,50e5],"Above ₹50 Lakh":[50e5,75e5]}),
    emergencyFund: B({"None":[0,0],"Below ₹1 Lakh":[0,1e5],"₹1 – 3 Lakh":[1e5,3e5],"₹3 – 5 Lakh":[3e5,5e5],"Above ₹5 Lakh":[5e5,8e5]}),
    lifeCover: B({"None":[0,0],"Below ₹50 Lakh":[0,50e5],"₹50 Lakh – 1 Crore":[50e5,1e7],"Above ₹1 Crore":[1e7,1.5e7]}),
    healthCover: B({"None":[0,0],"Below ₹5 Lakh":[0,5e5],"₹5 – 10 Lakh":[5e5,10e5],"Above ₹10 Lakh":[10e5,15e5]}),
    sipAmount: B({"₹1,000 – 5,000":[1000,5000],"₹5,000 – 10,000":[5000,10000],"₹10,000 – 25,000":[10000,25000],"Above ₹25,000":[25000,40000],"No SIP":[0,0]})
  };
  const band = (p,k) => BANDS[k] && BANDS[k][K(p[k])] || null;
  const mid = b => b ? (b[0]+b[1])/2 : 0;
  const has = (p,k,v) => (Array.isArray(p[k])?p[k]:[p[k]]).some(x=>K(x)===K(v));
  const BIG_EQ = [K('₹5 – 25 Lakh'), K('Above ₹25 Lakh')];

  const ST = { g:{c:'#0f7a45',bg:'#e3f7ee',e:'🟢'}, a:{c:'#b45309',bg:'#fff4e5',e:'🟡'}, r:{c:'#c0392b',bg:'#fdf0ee',e:'🔴'}, n:{c:'#5d6e8a',bg:'#f4f6fa',e:'⚪'} };

  const CAT = {
    flexi:{en:'Flexi Cap',hi:'फ्लेक्सी कैप',c:'#1a56db',why:'Core holding — fund manager har size me invest kar sakta hai',whyHi:'मुख्य निवेश — हर साइज़ की कंपनी में'},
    large:{en:'Large Cap / Nifty 50 Index',hi:'लार्ज कैप / इंडेक्स',c:'#0f1f3d',why:'Badi, stable companies',whyHi:'बड़ी, स्थिर कंपनियाँ'},
    lm:{en:'Large & Mid Cap',hi:'लार्ज एंड मिड कैप',c:'#0891b2',why:'Stability + growth',whyHi:'स्थिरता + बढ़त'},
    mid:{en:'Mid Cap',hi:'मिड कैप',c:'#6d28d9',why:'Lambe samay me zyada growth',whyHi:'लंबे समय में ज़्यादा बढ़त'},
    small:{en:'Small Cap',hi:'स्मॉल कैप',c:'#c2185b',why:'High growth, high risk — chhota hissa',whyHi:'ऊँची बढ़त, ऊँचा जोखिम — छोटा हिस्सा'},
    multi:{en:'Multi Asset Allocation',hi:'मल्टी एसेट',c:'#c9942a',why:'Equity + Debt + Gold — girawat me sahara',whyHi:'इक्विटी + डेट + सोना — गिरावट में सहारा'},
    bal:{en:'Balanced Advantage',hi:'बैलेंस्ड एडवांटेज',c:'#0f7a45',why:'Market ke hisab se equity kam-zyada',whyHi:'बाज़ार के हिसाब से इक्विटी कम-ज़्यादा'},
    debt:{en:'Short Duration / Corporate Bond',hi:'डेट फंड',c:'#64748b',why:'Kam utaar-chadhav, stable return',whyHi:'कम उतार-चढ़ाव'},
    elss:{en:'ELSS (Tax Saver)',hi:'ELSS (टैक्स सेवर)',c:'#b91c1c',why:'80C tax bachat, 3 saal lock-in',whyHi:'80C टैक्स बचत, 3 साल लॉक-इन'},
    liquid:{en:'Liquid / Arbitrage Fund (Emergency)',hi:'लिक्विड फंड (इमरजेंसी)',c:'#94a3b8',why:'Emergency fund banane ke liye — kabhi bhi nikaal sakte hain',whyHi:'इमरजेंसी फंड के लिए — कभी भी निकाल सकते हैं'}
  };
  // Order = priority (chhoti SIP me peeche wale pehle hat-te hain)
  const ALLOC = {
    0:[['bal',35],['large',25],['multi',20],['debt',20]],
    1:[['flexi',35],['lm',20],['bal',25],['large',20]],
    2:[['flexi',30],['lm',25],['mid',20],['multi',15],['small',10]],
    3:[['flexi',30],['mid',25],['lm',20],['small',15],['multi',10]]
  };
  const LVL = ['Conservative','Moderate','Aggressive','Aggressive'];
  const LVL_HI = {Conservative:'सुरक्षित (Conservative)',Moderate:'संतुलित (Moderate)',Aggressive:'आक्रामक (Aggressive)'};

  const _over = {};   // RM ne amount badla → profile id ke hisab se

  // ══════════════ RULES ENGINE ══════════════
  function analyse(r){
    const p=r.p||{};
    const inc=band(p,'monthlyIncome'), sav=band(p,'monthlySavings');
    const incBase = inc ? (inc[0]>=200000 ? 200000 : mid(inc)) : 0;
    const savMid = sav ? mid(sav) : 0;
    const irregular = K(p.r1)===K('Irregular') || has(p,'incomeSource','Business') || has(p,'incomeSource','Professional');
    const retired = has(p,'incomeSource','Retired / Pension');
    const old = K(p.r5)===K('Above 55');
    const loan = K(p.loan)==='yes';
    const checks=[], talk=[], notes=[];

    // 1. Emergency Fund
    const months = irregular ? 9 : 6;
    const expense = incBase ? Math.max(incBase - savMid, incBase*0.4) : 0;
    const efTarget = expense ? Math.max(1e5, Math.round(expense*months/1e5)*1e5) : 0;
    const ef = band(p,'emergencyFund'); const efMid = mid(ef);
    let efS='n';
    if(ef && efTarget){ const q=efMid/efTarget; efS = K(p.emergencyFund)==='none' ? 'r' : q>=1 ? 'g' : q>=0.5 ? 'a' : 'r'; }
    checks.push({key:'ef', en:'Emergency Fund', hi:'इमरजेंसी फंड', s:efS, have:p.emergencyFund||'—',
      adv: efS==='g' ? 'Sufficient ✓' : efTarget ? `Build ~${big(efTarget)} (${months} months' expenses)` : 'Check',
      advHi: efS==='g' ? 'पर्याप्त ✓' : efTarget ? `लगभग ${bigHi(efTarget)} रखें (${months} महीने का खर्च)` : 'जाँचें'});

    // 2. Life Cover
    const loanB = loan ? band(p,'loanAmount') : null;
    const annual = incBase*12;
    let need = retired ? 0 : annual*(old?5:10) + (loanB?mid(loanB):0);
    need = need>=1e7 ? Math.ceil(need/25e5)*25e5 : Math.ceil(need/5e5)*5e5;
    const lc = band(p,'lifeCover');
    let lcS='n';
    if(lc && need){ lcS = lc[0]>=need ? 'g' : lc[1]>=need ? 'a' : mid(lc)>=need*0.5 ? 'a' : 'r'; if(K(p.lifeCover)==='none') lcS='r'; }
    else if(retired) lcS='n';
    checks.push({key:'life', en:'Life / Term Cover', hi:'जीवन / टर्म बीमा', s:lcS, have:p.lifeCover||'—',
      adv: retired ? 'Not critical after retirement' : lcS==='g' ? 'Sufficient ✓' : need ? `Need ~${big(need)} term plan${loan?' (incl. loan)':''}` : 'Check',
      advHi: retired ? 'रिटायरमेंट के बाद ज़रूरी नहीं' : lcS==='g' ? 'पर्याप्त ✓' : need ? `लगभग ${bigHi(need)} का टर्म प्लान${loan?' (लोन सहित)':''}` : 'जाँचें'});

    // 3. Health Cover
    const hk=K(p.healthCover); let hS='n', hA='Check', hH='जाँचें';
    if(hk==='none'){ hS='r'; hA='Take family floater ₹10 L+'; hH='₹10 लाख+ फैमिली फ्लोटर लें'; }
    else if(hk===K('Below ₹5 Lakh')){ hS='r'; hA='Too low — increase to ₹10 L+'; hH='बहुत कम — ₹10 लाख+ करें'; }
    else if(hk===K('₹5 – 10 Lakh')){ hS='a'; hA='Add super top-up ₹15–20 L (low cost)'; hH='₹15–20 लाख का सुपर टॉप-अप जोड़ें (सस्ता)'; }
    else if(hk===K('Above ₹10 Lakh')){ hS='g'; hA='Good ✓'; hH='अच्छा ✓'; }
    checks.push({key:'health', en:'Health Cover', hi:'स्वास्थ्य बीमा', s:hS, have:p.healthCover||'—', adv:hA, advHi:hH});

    // 4. EMI Load
    if(!loan){ checks.push({key:'emi', en:'EMI Load', hi:'EMI का बोझ', s:'g', have:'No loan', adv:'No loan ✓', advHi:'कोई लोन नहीं ✓'}); }
    else {
      const emi=mid(band(p,'emiAmount')); const q=incBase?emi/incBase:0;
      const s = !incBase ? 'n' : q<0.25 ? 'g' : q<0.4 ? 'a' : 'r';
      checks.push({key:'emi', en:'EMI Load', hi:'EMI का बोझ', s, have:(p.emiAmount||'—')+(incBase?` (~${Math.round(q*100)}% of income)`:''),
        adv: s==='g'?'Manageable ✓':s==='a'?'On the higher side — avoid new loans':'Too high — prepay / restructure first',
        advHi: s==='g'?'संभालने लायक ✓':s==='a'?'थोड़ा ज़्यादा — नया लोन न लें':'बहुत ज़्यादा — पहले लोन घटाएँ'});
    }

    // 5. Savings Rate
    if(incBase && sav){
      const q=savMid/incBase, s = q>=0.2 ? 'g' : q>=0.1 ? 'a' : 'r';
      checks.push({key:'sav', en:'Savings Rate', hi:'बचत दर', s, have:`${p.monthlySavings} (~${Math.round(q*100)}%)`,
        adv: s==='g'?'Healthy ✓':'Low for this income — review monthly budget',
        advHi: s==='g'?'अच्छी ✓':'आय के हिसाब से कम — मासिक बजट देखें'});
    }

    // ── Risk level ──
    const li = LVL.indexOf(p.riskProfile);
    let lvl = li<0 ? 1 : li;   // purani profile me risk na ho to Moderate
    const period = K(p.period);
    if(period===K('3 – 5 years')){ if(lvl>0) notes.push('Period sirf 3–5 saal → allocation ek level safe kiya'); lvl=Math.max(0,lvl-1); }
    if(old && lvl>1){ lvl=1; notes.push('Age 55+ → Aggressive se Moderate kiya'); }
    if(lvl===2 && period===K('10+ years')) lvl=3;
    if(retired && has(p,'goal','Retirement') && lvl>1) lvl=1;

    // ── Amount ──
    const planned = band(p,'sipAmount');
    const noSip = K(p.sipAmount)===K('No SIP');
    let auto = sav ? Math.max(500, r500(Math.min(savMid*0.85, sav[1]))) : (planned?r500(mid(planned)):5000);
    const total = _over[r.p._id] || auto;
    let efPart = efS==='r' ? Math.max(500, r500(total*0.33)) : efS==='a' ? Math.max(500, r500(total*0.2)) : 0;
    if(efPart && total-efPart<500) efPart = Math.max(0,total-500);
    const sip = total-efPart;

    // ── Allocation ──
    let slots = ALLOC[lvl].map(([k,w])=>({k,w}));
    const bigEq = BIG_EQ.includes(K(p.oEquity)) || (r.e && +r.e.asset_value>=5e5);
    if(bigEq && slots.some(s=>s.k==='small')){
      const sm=slots.find(s=>s.k==='small'); slots.find(s=>s.k==='flexi').w+=sm.w; slots=slots.filter(s=>s.k!=='small');
      notes.push('Direct shares pehle se ₹5 L+ → Small Cap hataya (MF ka kaam diversification)');
    }
    const tax = has(p,'goal','Tax Saving');
    if(tax){ slots.forEach(s=>s.w*=0.8); slots.splice(1,0,{k:'elss',w:20}); }   // ELSS 20% — core fund ke baad
    // chhoti SIP me kam funds (priority order se)
    const maxN = sip<3000 ? 2 : sip<6000 ? 3 : sip<10000 ? 4 : 6;
    slots=slots.slice(0,maxN);
    const W=slots.reduce((a,s)=>a+s.w,0);
    slots.forEach(s=>{ s.amt=Math.max(500, r500(sip*s.w/W)); });
    const diff = sip - slots.reduce((a,s)=>a+s.amt,0); slots.reduce((a,s)=>s.amt>a.amt?s:a,slots[0]).amt += diff;
    slots = slots.filter(s=>s.amt>0);
    const alloc = slots.map(s=>({k:s.k, amt:s.amt, ...CAT[s.k]}));
    if(efPart) alloc.push({k:'liquid', amt:efPart, ...CAT.liquid});

    // ── RM Talking Points (internal) ──
    const curSip = r.c ? (+r.c.sip_amount||0) : 0;
    if(curSip) talk.push(`Client ki current SIP hamare saath ${inr(curSip)} hai — confirm karein ye savings me shamil hai ya nahi. Suggested ${inr(total)} ${total>curSip?`→ ${inr(total-curSip)} badhana`:'— current SIP kaafi hai'}.`);
    if(planned && !noSip && sip>planned[1]) talk.push(`Client ne ${p.sipAmount} socha hai, savings ke hisab se ${inr(sip)} ho sakta hai — dheere se upar le jaayein.`);
    if(noSip) talk.push(`Client SIP nahi chahta (${p.mode||'Lumpsum'}) — yahi split lumpsum par % me lagayein.`);
    if(tax) talk.push('Tax regime confirm karein — NEW regime me ELSS ka faayda nahi, wo hissa Flexi Cap me daalein. PPF/EPF se 80C pehle hi bhar sakta hai.');
    if(efS==='r' && (BIG_EQ.includes(K(p.oEquity)) || K(p.oFd)!=='none' && p.oFd)) talk.push('Emergency fund jaldi banane ke liye shares / FD se ek lumpsum Liquid Fund me rakhwayein.');
    if(efPart) talk.push(`Emergency target (${big(efTarget)}) pura hone par ${inr(efPart)} wala hissa equity SIP me shift karein.`);
    if(lcS==='r'||lcS==='a') talk.push('Term plan ki baat karein — LIC endowment / money-back se cover kam milta hai.');
    if(has(p,'oOther','LIC / Insurance Plans')) talk.push('LIC / insurance plans ka review — premium, maturity aur cover dekhein.');
    if(p.oMf && K(p.oMf)!=='none') talk.push(`Dusri jagah MF ${p.oMf} hai — consolidation / portfolio review offer karein.`);
    if(p.oSip && K(p.oSip)!=='none') talk.push(`Dusri jagah SIP ${p.oSip} chal rahi hai — total equity exposure dhyan me rakhein.`);
    if((has(p,'goal','Child Education')||has(p,'goal','House / Car')) && period===K('3 – 5 years')) talk.push('Short-term goal (padhai / ghar / gaadi) ke liye us hisse ko equity ki jagah hybrid / debt me rakhein.');
    if(K(p.r2)===K('Sell')) talk.push('Client 20% girawat par bechna chahta hai — volatility samjhayein, SIP band na kare.');
    const stepUp = !old && !retired && K(p.r1)!==K('Irregular');

    return {checks, total, sip, efPart, efTarget, alloc, lvlName:LVL[lvl], talk, notes, stepUp, noSip, tax, auto, curSip};
  }

  // ══════════════ CRM CARD ══════════════
  function html(i){
    const r=(window._profRows||[])[i]; if(!r||!r.p) return '';
    let A; try{ A=analyse(r); }catch(e){ console.error('PLAN error',e); return `<div style="color:var(--red);font-size:.75rem;margin:8px 0">⚠️ Suggested plan nahi ban paya — ${esc(e.message)}</div>`; }
    const red=A.checks.filter(c=>c.s==='r').length, amb=A.checks.filter(c=>c.s==='a').length;
    const chk = A.checks.map(c=>{ const s=ST[c.s]; return `<div style="background:${s.bg};border-left:4px solid ${s.c};border-radius:8px;padding:7px 9px">
        <div style="font-size:.66rem;font-weight:800;color:${s.c};text-transform:uppercase">${s.e} ${esc(c.en)}</div>
        <div style="font-size:.7rem;color:var(--gray)">${esc(c.have)}</div>
        <div style="font-size:.74rem;font-weight:700;color:var(--navy);margin-top:2px">${esc(c.adv)}</div></div>`; }).join('');
    const bar = A.alloc.map(a=>`<div title="${esc(a.en)} ${inr(a.amt)}" style="width:${a.amt/A.total*100}%;background:${a.c}"></div>`).join('');
    const rows = A.alloc.map(a=>`<tr><td style="width:14px"><span style="display:inline-block;width:11px;height:11px;border-radius:3px;background:${a.c}"></span></td>
        <td><b>${esc(a.en)}</b><br><small style="color:var(--gray)">${esc(a.why)}</small></td>
        <td style="text-align:right;font-weight:800;color:var(--navy);white-space:nowrap">${inr(a.amt)}</td>
        <td style="text-align:right;color:var(--gray);font-size:.7rem">${Math.round(a.amt/A.total*100)}%</td></tr>`).join('');
    return `<div id="planCard" style="border:2px solid var(--gold);border-radius:14px;padding:12px;margin:4px 0 14px;background:linear-gradient(180deg,#fdf6e3,#fff 120px)">
      <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px;margin-bottom:8px">
        <div style="font-weight:900;color:var(--navy);font-size:1rem">💡 Suggested Plan <small style="font-weight:600;color:var(--gray);font-size:.68rem">auto · ${esc(A.lvlName)}</small></div>
        <div style="font-size:.7rem;font-weight:700">${red?`<span style="color:#c0392b">🔴 ${red} gap${red>1?'s':''}</span> `:''}${amb?`<span style="color:#b45309">🟡 ${amb}</span> `:''}${!red&&!amb?'<span style="color:#0f7a45">🟢 All good</span>':''}</div>
      </div>
      <div style="font-size:.7rem;font-weight:800;color:var(--gray);text-transform:uppercase;margin-bottom:5px">Financial Health Check</div>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:6px;margin-bottom:12px">${chk}</div>
      <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:6px;margin-bottom:6px">
        <div style="font-size:.7rem;font-weight:800;color:var(--gray);text-transform:uppercase">Suggested Monthly ${A.noSip?'Investment':'SIP'}
          <span style="font-size:1.15rem;color:var(--navy);text-transform:none;margin-left:4px">${inr(A.total)}</span>
          ${A.efPart?`<small style="text-transform:none;font-weight:600">(${inr(A.sip)} equity + ${inr(A.efPart)} emergency)</small>`:''}</div>
        <div style="display:flex;gap:4px;align-items:center">
          <input id="plan-amt" type="number" step="500" min="500" value="${A.total}" style="width:95px;padding:5px 7px;border:1px solid var(--border);border-radius:7px;font-size:.78rem">
          <button class="btn btn-outline" style="padding:5px 9px;font-size:.72rem" onclick="PLAN.setAmt(${i})">↻ Recalc</button>
          ${_over[r.p._id]?`<button class="btn btn-outline" style="padding:5px 9px;font-size:.72rem" title="Auto amount (${inr(A.auto)}) wapas" onclick="PLAN.setAmt(${i},true)">Auto</button>`:''}
        </div>
      </div>
      <div style="display:flex;height:16px;border-radius:8px;overflow:hidden;margin-bottom:6px">${bar}</div>
      <table style="width:100%;font-size:.76rem;margin-bottom:6px"><tbody>${rows}</tbody></table>
      ${A.stepUp?'<div style="font-size:.72rem;color:#0f7a45;font-weight:700;margin-bottom:6px">📈 Har saal 10% step-up lagwayein</div>':''}
      ${A.notes.length?`<div style="font-size:.68rem;color:var(--gray);margin-bottom:6px">ℹ️ ${A.notes.map(esc).join(' · ')}</div>`:''}
      ${A.talk.length?`<div style="background:#eef3fc;border-radius:8px;padding:8px 10px;margin-bottom:8px">
        <div style="font-size:.68rem;font-weight:800;color:#1a56db;text-transform:uppercase;margin-bottom:3px">🗣️ RM Talking Points <small style="font-weight:600;text-transform:none;color:var(--gray)">(sirf aapke liye — client ko nahi jaata)</small></div>
        <ul style="margin:0;padding-left:16px;font-size:.73rem;line-height:1.45">${A.talk.map(t=>`<li>${esc(t)}</li>`).join('')}</ul></div>`:''}
      <div style="display:flex;gap:6px;flex-wrap:wrap">
        <button class="btn" style="background:#25D366;color:#fff;border:none" onclick="PLAN.wa(${i})">📤 Send Plan on WhatsApp</button>
        <button class="btn btn-outline" onclick="PLAN.copy(${i})">📋 Copy Text</button>
        <button class="btn btn-outline" onclick="PLAN.print(${i})">🖨️ Print / PDF</button>
      </div>
      <div style="font-size:.62rem;color:var(--gray);margin-top:6px">Suggestion fixed rules par hai — scheme aur final amount client se baat karke RM tay karein.</div>
    </div>`;
  }

  function setAmt(i, reset){
    const r=(window._profRows||[])[i]; if(!r) return;
    if(reset) delete _over[r.p._id];
    else { const v=r500(+(document.getElementById('plan-amt')||{}).value||0); if(v<500){ toast&&toast('Amount kam se kam ₹500','error'); return; } _over[r.p._id]=v; }
    const el=document.getElementById('planCard'); if(el) el.outerHTML=html(i);
  }

  // ══════════════ CLIENT MESSAGE (Hindi + English) ══════════════
  function text(i){
    const r=(window._profRows||[])[i]; if(!r) return '';
    const A=analyse(r), p=r.p;
    const first=String(p.name||'').trim().split(/\s+/)[0]||''; const nm=first.charAt(0).toUpperCase()+first.slice(1).toLowerCase();
    const L=[];
    L.push('*D N INVESTMENT — Your Investment Plan*','*आपका निवेश प्लान*','');
    L.push(`Dear ${nm},`,'Thank you for filling the profile form. Based on your answers, here is our suggested plan.','प्रोफ़ाइल फ़ॉर्म भरने के लिए धन्यवाद। आपके उत्तरों के आधार पर हमारा सुझाया गया प्लान:','');
    L.push(`*Risk Profile / जोखिम प्रोफ़ाइल:* ${A.lvlName} — ${LVL_HI[A.lvlName]||''}`,'');
    L.push('*Financial Health Check / वित्तीय स्वास्थ्य जाँच*');
    A.checks.forEach(c=>{ L.push(`${ST[c.s].e} ${c.en}: ${c.adv}`, `    ${c.hi}: ${c.advHi}`); });
    L.push('',`*Suggested Monthly ${A.noSip?'Investment':'SIP'} / सुझाया गया मासिक निवेश: ${inr(A.total)}*`);
    A.alloc.forEach(a=>L.push(`• ${a.en} / ${a.hi} — ${inr(a.amt)}`));
    if(A.efPart) L.push('',`💰 ${inr(A.efPart)}/month goes to a Liquid Fund until your emergency fund (~${big(A.efTarget)}) is built; then it moves to equity.`,`💰 ${inr(A.efPart)} हर महीने लिक्विड फंड में, जब तक इमरजेंसी फंड (~${bigHi(A.efTarget)}) न बन जाए; फिर ये इक्विटी में।`);
    if(A.tax) L.push('','🧾 ELSS gives tax benefit only under the Old Tax Regime.','🧾 ELSS का टैक्स लाभ सिर्फ़ पुरानी टैक्स व्यवस्था (Old Regime) में मिलता है।');
    if(A.stepUp) L.push('','📈 Increase your SIP by 10% every year.','📈 हर साल अपनी SIP 10% बढ़ाएँ।');
    L.push('','This is a suggestion based on your profile. Your Relationship Manager will discuss the final schemes with you.','यह आपकी प्रोफ़ाइल पर आधारित सुझाव है। आपके रिलेशनशिप मैनेजर अंतिम स्कीम पर आपसे बात करेंगे।');
    L.push('','_Mutual Fund investments are subject to market risks, read all scheme related documents carefully._','_म्यूचुअल फंड निवेश बाज़ार जोखिमों के अधीन हैं, योजना से जुड़े सभी दस्तावेज़ ध्यान से पढ़ें।_');
    L.push('',(typeof CU!=='undefined'&&CU&&CU.name)||'', 'D N Investment · ARN-67217');
    return L.join('\n').trim();
  }
  function wa(i){
    const r=(window._profRows||[])[i]; if(!r) return;
    const m=String(r.p.mobile||'').replace(/\D/g,'').slice(-10);
    const url=/^[6-9]\d{9}$/.test(m) ? 'https://wa.me/91'+m : 'https://wa.me/';
    window.open(url+'?text='+encodeURIComponent(text(i)),'_blank');
  }
  async function copy(i){
    const t=text(i);
    try{ await navigator.clipboard.writeText(t); toast&&toast('Plan text copied ✓','success'); }catch(_){ prompt('Copy:',t); }
  }

  // ══════════════ PRINT / PDF ══════════════
  function print(i){
    const r=(window._profRows||[])[i]; if(!r) return;
    const A=analyse(r), p=r.p;
    const base=location.origin+location.pathname.replace(/[^/]*$/,'');
    const d=new Date().toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'});
    const chk=A.checks.map(c=>{ const s=ST[c.s]; return `<div class="ck" style="background:${s.bg};border-left:5px solid ${s.c}">
      <div class="ckh" style="color:${s.c}">${s.e} ${esc(c.en)} <span class="hi">· ${esc(c.hi)}</span></div>
      <div class="ckv">${esc(c.have)}</div><div class="cka">${esc(c.adv)}</div><div class="hi">${esc(c.advHi)}</div></div>`; }).join('');
    const bar=A.alloc.map(a=>`<div style="width:${a.amt/A.total*100}%;background:${a.c}"></div>`).join('');
    const rows=A.alloc.map(a=>`<tr><td><span class="sw" style="background:${a.c}"></span></td><td><b>${esc(a.en)}</b> <span class="hi">· ${esc(a.hi)}</span><br><small>${esc(a.why)} · <span class="hi">${esc(a.whyHi)}</span></small></td><td class="amt">${inr(a.amt)}</td><td class="pc">${Math.round(a.amt/A.total*100)}%</td></tr>`).join('');
    const w=window.open('','_blank'); if(!w){ toast&&toast('Popup blocked — allow popups','error'); return; }
    w.document.write(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>Investment Plan – ${esc(p.name)}</title>
<style>
*{box-sizing:border-box;margin:0;padding:0} body{font-family:system-ui,"Segoe UI",Roboto,sans-serif;color:#0f1f3d;padding:22px;max-width:800px;margin:auto;-webkit-print-color-adjust:exact;print-color-adjust:exact}
.hi{font-family:"Noto Sans Devanagari","Nirmala UI","Mangal",sans-serif;color:#7a6a3a;font-size:.86em}
header{display:flex;align-items:center;gap:14px;border-bottom:3px solid #c9942a;padding-bottom:10px;margin-bottom:14px}
header img{height:58px} header h1{font-size:22px;letter-spacing:3px} header .t{font-size:11px;color:#c9942a;letter-spacing:2px;font-weight:700} header .s{font-size:10.5px;color:#5d6e8a}
.ttl{display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:12px} .ttl h2{font-size:18px} .ttl div{font-size:11.5px;color:#5d6e8a;text-align:right}
.pill{display:inline-block;background:#0f1f3d;color:#f5d98a;border-radius:20px;padding:4px 12px;font-size:12px;font-weight:700;margin-bottom:12px}
h3{font-size:12px;text-transform:uppercase;letter-spacing:1px;color:#5d6e8a;margin:14px 0 7px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:8px}
.ck{border-radius:8px;padding:8px 10px} .ckh{font-weight:800;font-size:12px;text-transform:uppercase} .ckv{font-size:11px;color:#5d6e8a} .cka{font-weight:700;font-size:13px;margin-top:2px}
.big{font-size:26px;font-weight:900} .bar{display:flex;height:20px;border-radius:10px;overflow:hidden;margin:8px 0}
table{width:100%;border-collapse:collapse;font-size:12.5px} td{padding:6px 4px;border-bottom:1px solid #e3e8f0;vertical-align:top} small{color:#5d6e8a}
.sw{display:inline-block;width:13px;height:13px;border-radius:3px;margin-top:2px} .amt{text-align:right;font-weight:800;white-space:nowrap} .pc{text-align:right;color:#5d6e8a}
.box{background:#fdf6e3;border:1px solid #f5d98a;border-radius:8px;padding:9px 11px;margin-top:10px;font-size:12px;line-height:1.5}
.disc{margin-top:16px;font-size:10.5px;color:#5d6e8a;border-top:1px solid #e3e8f0;padding-top:8px;line-height:1.5}
@media print{body{padding:10px} button{display:none}}
</style></head><body>
<header><img src="${base}dn-logo.png" onerror="if(!this.dataset.f){this.dataset.f=1;this.src='${base}logo_new.png'}else{this.style.display='none'}">
<div><h1>D N INVESTMENT</h1><div class="t">TRUSTED WEALTH PARTNER</div><div class="s">AMFI Registered Mutual Fund Distributor · ARN-67217 · Bistupur, Jamshedpur</div></div></header>
<div class="ttl"><h2>Suggested Investment Plan <span class="hi">· सुझाया गया निवेश प्लान</span></h2><div>${esc(p.name)}<br>${esc(p.mobile)} · ${d}</div></div>
<div class="pill">Risk Profile: ${esc(A.lvlName)} · <span style="font-family:'Nirmala UI','Mangal',sans-serif">${esc(LVL_HI[A.lvlName]||'')}</span></div>
<h3>Financial Health Check <span class="hi">· वित्तीय स्वास्थ्य जाँच</span></h3><div class="grid">${chk}</div>
<h3>Suggested Monthly ${A.noSip?'Investment':'SIP'} <span class="hi">· सुझाया गया मासिक निवेश</span></h3>
<div class="big">${inr(A.total)}${A.efPart?` <small style="font-size:12px;font-weight:600">(${inr(A.sip)} equity + ${inr(A.efPart)} emergency)</small>`:''}</div>
<div class="bar">${bar}</div><table><tbody>${rows}</tbody></table>
${A.efPart?`<div class="box">💰 ${inr(A.efPart)}/month goes to a Liquid Fund until your emergency fund (~${big(A.efTarget)}) is built; then it moves to equity.<br><span class="hi">${inr(A.efPart)} हर महीने लिक्विड फंड में, जब तक इमरजेंसी फंड (~${bigHi(A.efTarget)}) न बन जाए; फिर ये इक्विटी में।</span></div>`:''}
${A.tax?`<div class="box">🧾 ELSS gives tax benefit only under the Old Tax Regime.<br><span class="hi">ELSS का टैक्स लाभ सिर्फ़ पुरानी टैक्स व्यवस्था में मिलता है।</span></div>`:''}
${A.stepUp?`<div class="box">📈 Increase your SIP by 10% every year — small step-ups make a big difference over time.<br><span class="hi">हर साल SIP 10% बढ़ाएँ — छोटी बढ़त लंबे समय में बड़ा फ़र्क लाती है।</span></div>`:''}
<div class="disc">This is a suggestion based on the information you provided. Your Relationship Manager will discuss specific schemes and the final plan with you.<br>
<span class="hi">यह आपके द्वारा दी गई जानकारी पर आधारित सुझाव है। आपके रिलेशनशिप मैनेजर स्कीम और अंतिम प्लान पर आपसे बात करेंगे।</span><br><br>
<b>Mutual Fund investments are subject to market risks, read all scheme related documents carefully.</b><br><span class="hi">म्यूचुअल फंड निवेश बाज़ार जोखिमों के अधीन हैं, योजना से जुड़े सभी दस्तावेज़ ध्यान से पढ़ें।</span></div>
<div style="text-align:center;margin-top:14px"><button onclick="print()" style="padding:9px 22px;border:none;border-radius:8px;background:#c9942a;color:#fff;font-weight:700;cursor:pointer">🖨️ Print / Save as PDF</button></div>
</body></html>`);
    w.document.close();
  }

  return { analyse, html, setAmt, text, wa, copy, print };
})();
