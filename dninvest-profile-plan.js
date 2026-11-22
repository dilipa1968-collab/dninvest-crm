/* ═══════════════════════════════════════════════════════════════════════
   💡 SUGGESTED PLAN — Client Profile se apne aap banne wala plan (20-Nov-2026)
   - Client Profiles → 👁️ detail popup me dikhta hai (koi button nahi dabana).
   - 5 Health Checks (🔴🟡🟢): Emergency Fund, Life Cover, Health Cover,
     EMI Load, Savings Rate.
   - Suggested monthly amount (savings ke hisab se) + Emergency Fund ka hissa
     + category-wise SIP split (risk profile, period, goals, direct shares).
   - RM ke liye "Talking Points" (sirf CRM me — client ko nahi jaate).
   - 🖼️ Colorful image (PNG) WhatsApp par — mobile par share sheet, computer par
     copy + Ctrl+V. 💬 Text bhi, 🖨️ PDF bhi (Hindi + English, DN Investment logo).
   - 22-Nov-2026: SIRF 5 categories — Large & Mid, Mid, Multi Cap, Flexi, Small (Large Cap nahi).
     4 combos (Recommended / Simple / Mid & Small Focus / Multi Cap). Long-term → Mid + Small zaroor.
   - 26 categories (Equity / Hybrid / Debt / Others) + scheme naam suggestion
     (FUND_NAME_LIST + clients ki schemes, category ke hisab se).
   - ✏️ Edit Plan: RM health-check advice, funds, scheme naam, amount, note badal sakta
     hai (is browser me save). Image / text / PDF edited plan se hi bante hain.
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

  // g = dropdown group, kw = scheme naam pehchanne ka pattern (suggestion list ke liye), nk = jo nahi hona chahiye
  const CAT = {
    flexi:{g:'Equity',en:'Flexi Cap',hi:'फ्लेक्सी कैप',c:'#1a56db',why:'Core holding — invests across large, mid & small companies',whyHi:'मुख्य निवेश — हर साइज़ की कंपनी में',kw:/flexi ?cap/i},
    multicap:{g:'Equity',en:'Multi Cap',hi:'मल्टी कैप',c:'#2563eb',why:'Min. 25% each in large, mid & small caps',whyHi:'लार्ज, मिड, स्मॉल — हर एक में कम से कम 25%',kw:/multi ?cap/i},
    large:{g:'Equity',en:'Large Cap / Nifty 50 Index',hi:'लार्ज कैप / इंडेक्स',c:'#0f1f3d',why:'Large, stable companies',whyHi:'बड़ी, स्थिर कंपनियाँ',kw:/large ?cap|bluechip|blue chip|top 100|nifty 50|sensex|front ?line/i,nk:/mid|next 50/i},
    lm:{g:'Equity',en:'Large & Mid Cap',hi:'लार्ज एंड मिड कैप',c:'#0891b2',why:'Stability + growth',whyHi:'स्थिरता + बढ़त',kw:/large ?(&|and) ?mid|emerging equit|equity opportunit/i},
    mid:{g:'Equity',en:'Mid Cap',hi:'मिड कैप',c:'#6d28d9',why:'Higher growth over the long term',whyHi:'लंबे समय में ज़्यादा बढ़त',kw:/mid ?cap|midcap/i,nk:/large|small/i},
    small:{g:'Equity',en:'Small Cap',hi:'स्मॉल कैप',c:'#c2185b',why:'High growth, high risk — small portion',whyHi:'ऊँची बढ़त, ऊँचा जोखिम — छोटा हिस्सा',kw:/small ?cap/i},
    focused:{g:'Equity',en:'Focused Fund',hi:'फोकस्ड फंड',c:'#4338ca',why:'Concentrated portfolio of max. 30 stocks',whyHi:'अधिकतम 30 चुनिंदा शेयर',kw:/focus/i},
    value:{g:'Equity',en:'Value / Contra',hi:'वैल्यू / कॉन्ट्रा',c:'#7c3aed',why:'Buys good companies at lower valuations',whyHi:'अच्छी कंपनियाँ सस्ते भाव पर',kw:/value|contra/i},
    divy:{g:'Equity',en:'Dividend Yield',hi:'डिविडेंड यील्ड',c:'#9333ea',why:'High-dividend companies — steadier equity',whyHi:'ज़्यादा डिविडेंड देने वाली कंपनियाँ',kw:/dividend yield/i},
    index:{g:'Equity',en:'Index Fund / ETF (Next 50, Midcap 150 etc.)',hi:'इंडेक्स फंड / ETF',c:'#334155',why:'Low cost, follows the index',whyHi:'कम खर्च, इंडेक्स के साथ चलता है',kw:/index|etf|next 50|nifty|sensex/i,nk:/gold|silver/i},
    elss:{g:'Equity',en:'ELSS (Tax Saver)',hi:'ELSS (टैक्स सेवर)',c:'#b91c1c',why:'80C tax saving, 3-year lock-in',whyHi:'80C टैक्स बचत, 3 साल लॉक-इन',kw:/elss|tax ?saver|long term equity|tax ?plan|taxsaver|tax advantage/i},
    thematic:{g:'Equity',en:'Sectoral / Thematic',hi:'सेक्टोरल / थीमैटिक',c:'#ea580c',why:'One sector / theme — high risk, small portion only',whyHi:'एक सेक्टर / थीम — ज़्यादा जोखिम, छोटा हिस्सा',kw:/bank|financial serv|infra|pharma|health|technolog|digital|consum|manufactur|psu|business cycle|energy|mnc|thematic|innovation|defen|transport|auto|esg|quant|momentum|special opp/i,nk:/banking ?(&|and) ?psu|debt|bond/i},
    intl:{g:'Equity',en:'International / Global FoF',hi:'इंटरनेशनल फंड',c:'#0e7490',why:'Global diversification (US / world markets)',whyHi:'विदेशी बाज़ार में निवेश',kw:/international|global|nasdaq|us equity|overseas|world|s&p 500|china|japan/i},
    bal:{g:'Hybrid',en:'Balanced Advantage',hi:'बैलेंस्ड एडवांटेज',c:'#0f7a45',why:'Adjusts equity based on market levels',whyHi:'बाज़ार के हिसाब से इक्विटी कम-ज़्यादा',kw:/balanced advantage|dynamic asset|asset allocator/i},
    aggh:{g:'Hybrid',en:'Aggressive Hybrid (Equity & Debt)',hi:'एग्रेसिव हाइब्रिड',c:'#15803d',why:'65–80% equity + debt — smoother ride',whyHi:'65–80% इक्विटी + डेट',kw:/aggressive hybrid|equity hybrid|hybrid equity|equity ?& ?debt|equity and debt|balanced fund/i},
    multi:{g:'Hybrid',en:'Multi Asset Allocation',hi:'मल्टी एसेट',c:'#c9942a',why:'Equity + Debt + Gold — cushions market falls',whyHi:'इक्विटी + डेट + सोना — गिरावट में सहारा',kw:/multi ?asset/i},
    eqsav:{g:'Hybrid',en:'Equity Savings',hi:'इक्विटी सेविंग्स',c:'#65a30d',why:'Low equity + arbitrage + debt — low volatility',whyHi:'कम जोखिम वाला हाइब्रिड',kw:/equity savings/i},
    conh:{g:'Hybrid',en:'Conservative Hybrid',hi:'कंज़र्वेटिव हाइब्रिड',c:'#4d7c0f',why:'Mostly debt + small equity — regular income style',whyHi:'ज़्यादातर डेट + थोड़ी इक्विटी',kw:/conservative hybrid|regular savings|hybrid debt|monthly income/i},
    arb:{g:'Hybrid',en:'Arbitrage Fund',hi:'आर्बिट्राज फंड',c:'#64748b',why:'Very low risk, equity taxation — for 3+ months parking',whyHi:'बहुत कम जोखिम — 3+ महीने के लिए',kw:/arbitrage/i},
    liquid:{g:'Debt',en:'Liquid / Arbitrage Fund (Emergency)',hi:'लिक्विड फंड (इमरजेंसी)',c:'#94a3b8',why:'For building emergency fund — withdraw anytime',whyHi:'इमरजेंसी फंड के लिए — कभी भी निकाल सकते हैं',kw:/liquid|overnight|money market|ultra short|arbitrage/i},
    debt:{g:'Debt',en:'Short Duration / Corporate Bond',hi:'डेट फंड',c:'#475569',why:'Low volatility, steady returns',whyHi:'कम उतार-चढ़ाव',kw:/short (term|duration)|corporate bond|banking ?(&|and) ?psu|low duration|medium duration|credit risk|floater|floating/i,nk:/ultra short/i},
    gilt:{g:'Debt',en:'Dynamic Bond / Gilt',hi:'डायनामिक बॉन्ड / गिल्ट',c:'#1e293b',why:'Government bonds — gains when interest rates fall',whyHi:'सरकारी बॉन्ड — ब्याज दर घटने पर फ़ायदा',kw:/gilt|dynamic bond|constant maturity|government sec|g-sec|long duration/i},
    gold:{g:'Others',en:'Gold Fund / ETF',hi:'गोल्ड फंड',c:'#ca8a04',why:'Hedge against inflation & market falls',whyHi:'महँगाई और गिरावट से बचाव',kw:/gold/i},
    silver:{g:'Others',en:'Silver Fund / ETF',hi:'सिल्वर फंड',c:'#a1a1aa',why:'Precious metal diversification',whyHi:'चाँदी में निवेश',kw:/silver/i},
    child:{g:'Others',en:"Children's Fund (Solution)",hi:'चिल्ड्रन फंड',c:'#db2777',why:"For child's education — 5-year lock-in",whyHi:'बच्चों की पढ़ाई के लिए — 5 साल लॉक-इन',kw:/child/i},
    retire:{g:'Others',en:'Retirement Fund (Solution)',hi:'रिटायरमेंट फंड',c:'#9f1239',why:'For retirement — 5-year lock-in',whyHi:'रिटायरमेंट के लिए — 5 साल लॉक-इन',kw:/retire|pension/i}
  };
  const GROUPS=['Equity','Hybrid','Debt','Others'];

  // ── Scheme naam suggestions: CRM ki built-in fund list + clients ki asli schemes ──
  let _names=null;
  function allNames(){
    if(_names) return _names;
    let L=[];
    try{ if(typeof getCrmSchemeNames==='function') L=L.concat(getCrmSchemeNames()||[]); }catch(_){}
    try{ if(typeof FUND_NAME_LIST!=='undefined') L=L.concat(FUND_NAME_LIST); }catch(_){}
    const seen=new Set(); _names=[];
    L.forEach(n=>{ n=String(n||'').trim(); const k=n.toLowerCase(); if(n && !seen.has(k)){ seen.add(k); _names.push(n); } });
    return _names;
  }
  function namesFor(k){
    const C=CAT[k]; if(!C||!C.kw) return [];
    return allNames().filter(n=>C.kw.test(n) && !(C.nk&&C.nk.test(n)) && !/idcw|dividend payout|dividend reinvest/i.test(n) || (k==='divy' && C.kw.test(n)))
      .sort((a,b)=>(/growth/i.test(b)-/growth/i.test(a)) || a.localeCompare(b)).slice(0,300);
  }
  const dlOpts = k => namesFor(k).map(n=>`<option value="${esc(n)}"></option>`).join('');

  // ══ FUND COMBINATIONS (22-Nov-2026 v3) ══
  // SIRF 5 equity categories (Dilip ji ka rule): Large & Mid, Mid, Multi Cap, Flexi, Small.
  // Large Cap / Index / Value / BAF / Multi Asset / Gold / Debt auto-plan me NAHI (RM edit me chahe to daal sakta hai).
  // Long-term (5–10 / 10+ saal) client → Mid Cap + Small Cap ZAROOR.
  // TPL[combo][risk 0-2][horizon 0-2] — order = priority (chhoti SIP me peeche wale hat-te hain)
  const TPL = {
    rec: [
      [ [['lm',55],['flexi',45]], [['flexi',40],['lm',35],['mid',15],['small',10]], [['flexi',35],['lm',30],['mid',20],['small',15]] ],
      [ [['flexi',45],['lm',35],['mid',20]], [['flexi',30],['lm',25],['mid',25],['small',20]], [['flexi',25],['mid',30],['small',25],['lm',20]] ],
      [ [['flexi',40],['lm',30],['mid',30]], [['flexi',25],['mid',30],['small',25],['lm',20]], [['mid',30],['small',30],['flexi',25],['lm',15]] ]
    ],
    simple: [
      [ [['lm',60],['flexi',40]], [['flexi',50],['mid',30],['small',20]], [['flexi',45],['mid',30],['small',25]] ],
      [ [['flexi',60],['lm',40]], [['flexi',45],['mid',30],['small',25]], [['flexi',40],['mid',30],['small',30]] ],
      [ [['flexi',50],['mid',50]], [['flexi',40],['mid',30],['small',30]], [['mid',40],['small',35],['flexi',25]] ]
    ],
    growth: [
      [ [['lm',50],['mid',50]], [['lm',35],['mid',40],['small',25]], [['mid',40],['lm',30],['small',30]] ],
      [ [['lm',45],['mid',55]], [['mid',40],['small',35],['lm',25]], [['mid',40],['small',40],['lm',20]] ],
      [ [['mid',50],['lm',30],['small',20]], [['mid',40],['small',40],['flexi',20]], [['small',45],['mid',40],['flexi',15]] ]
    ],
    multicap: [
      [ [['multicap',50],['lm',50]], [['multicap',45],['lm',25],['mid',15],['small',15]], [['multicap',40],['lm',20],['mid',20],['small',20]] ],
      [ [['multicap',55],['lm',45]], [['multicap',40],['mid',25],['small',20],['lm',15]], [['multicap',35],['mid',30],['small',25],['lm',10]] ],
      [ [['multicap',50],['mid',50]], [['multicap',35],['mid',30],['small',35]], [['multicap',30],['mid',35],['small',35]] ]
    ]
  };
  const COMBO = {
    rec:{en:'Recommended — Balanced Growth',hi:'सुझाया गया — संतुलित ग्रोथ',short:'⭐ Recommended',
      why:'Flexi Cap and Large & Mid Cap as the core, with Mid and Small Cap for long-term growth.',
      whyHi:'फ्लेक्सी कैप और लार्ज एंड मिड कैप मुख्य फंड, लंबी अवधि की बढ़त के लिए मिड और स्मॉल कैप।'},
    simple:{en:'Simple — 2 to 3 Funds',hi:'सरल — 2 से 3 फंड',short:'🎯 Simple',
      why:'Just 2–3 funds — a Flexi Cap core with Mid and Small Cap for growth. Easy to track.',
      whyHi:'सिर्फ़ 2–3 फंड — फ्लेक्सी कैप मुख्य, बढ़त के लिए मिड और स्मॉल कैप। आसान निगरानी।'},
    growth:{en:'High Growth — Mid & Small Focus',hi:'ऊँची बढ़त — मिड और स्मॉल कैप',short:'🚀 Mid & Small Focus',
      why:'Higher share in Mid and Small Cap for maximum long-term growth, balanced with Large & Mid Cap.',
      whyHi:'लंबी अवधि में ज़्यादा बढ़त के लिए मिड और स्मॉल कैप में बड़ा हिस्सा, साथ में लार्ज एंड मिड कैप।'},
    multicap:{en:'Multi Cap Based',hi:'मल्टी कैप आधारित',short:'🧩 Multi Cap',
      why:'Multi Cap as the core (min. 25% each in large, mid & small), with Mid and Small Cap added for long-term growth.',
      whyHi:'मल्टी कैप मुख्य फंड (लार्ज, मिड, स्मॉल में कम से कम 25%), लंबी अवधि के लिए मिड और स्मॉल कैप।'}
  };
  const COMBOS=['rec','simple','growth','multicap'];
  const LVL = ['Conservative','Moderate','Aggressive'];
  const LVL_HI = {Conservative:'सुरक्षित (Conservative)',Moderate:'संतुलित (Moderate)',Aggressive:'आक्रामक (Aggressive)'};

  const _over = {};   // RM ne amount badla → profile id ke hisab se

  // ══════════════ RULES ENGINE ══════════════
  function analyse(r, combo){
    const p=r.p||{};
    const inc=band(p,'monthlyIncome'), sav=band(p,'monthlySavings');
    const incBase = inc ? (inc[0]>=200000 ? 200000 : mid(inc)) : 0;
    const savMid = sav ? mid(sav) : 0;
    const irregular = K(p.r1)===K('Irregular') || has(p,'incomeSource','Business') || has(p,'incomeSource','Professional');
    const retired = has(p,'incomeSource','Retired / Pension');
    const old = K(p.r5)===K('Above 55');
    const loan = K(p.loan)==='yes';
    const modeTxt = [].concat(p.mode||[]).join(', ');
    const wantsEq = has(p,'mode','Equity (Direct Shares)'), wantsIpo = has(p,'mode','IPO');
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

    // ── Risk level + horizon ──
    const li = LVL.indexOf(p.riskProfile);
    let lvl = li<0 ? 1 : Math.min(2,li);   // purani profile me risk na ho to Moderate
    const period = K(p.period);
    const H = period===K('3 – 5 years') ? 0 : period===K('10+ years') ? 2 : 1;
    if(old && lvl>1){ lvl=1; notes.push('Age 55+ → capped at Moderate'); }
    if(K(p.r2)===K('Sell') && lvl>1){ lvl=1; notes.push('Would sell on a 20% fall → capped at Moderate'); }
    if(retired && has(p,'goal','Retirement') && lvl>0){ lvl=Math.min(lvl,1); }
    const shortGoal = (has(p,'goal','House / Car')||has(p,'goal','Child Education')) && H===0;
    if(shortGoal && lvl>0){ lvl--; notes.push('House / car / education goal within 3–5 years → one level safer'); }

    // ── Amount ──
    const planned = band(p,'sipAmount');
    const noSip = K(p.sipAmount)===K('No SIP');
    let auto = sav ? Math.max(500, r500(Math.min(savMid*0.85, sav[1]))) : (planned?r500(mid(planned)):5000);
    const total = _over[r.p._id] || auto;
    let efPart = efS==='r' ? Math.max(500, r500(total*0.33)) : efS==='a' ? Math.max(500, r500(total*0.2)) : 0;
    if(efPart && total-efPart<1000) efPart = Math.max(0,total-1000);
    const sip = total-efPart;

    // ── Allocation (chuna hua combination) ──
    combo = COMBO[combo] ? combo : 'rec';
    let slots = TPL[combo][lvl][H].map(([k,w])=>({k,w}));
    const LONG = H>=1;   // 5–10 / 10+ saal
    // Tax Saving: ELSS core fund ki JAGAH (ELSS bhi flexi-cap jaisa hi hai → dono rakhne se overlap)
    const tax = has(p,'goal','Tax Saving');
    let elssCapNote='';
    if(tax){
      const core = slots.find(s=>s.k==='flexi' || (s.k==='multicap' && combo!=='multicap'));   // Multi Cap combo me Multi Cap rehta hai
      if(core){ core.k='elss'; }
      else { slots.forEach(s=>s.w*=0.8); slots.splice(1,0,{k:'elss',w:20}); }
    }
    // Zaroori funds: long-term → Mid + Small; tax → ELSS
    const must = [...(LONG?['mid','small']:[]), ...(tax?['elss']:[])];
    must.forEach(k=>{ if(!slots.some(s=>s.k===k)) slots.push({k,w:15}); });
    // Fund count — SIP ke size se (bahut funds = overlap + tracking mushkil)
    let maxN = sip<2000 ? 1 : sip<5000 ? 2 : sip<10000 ? 3 : sip<20000 ? 4 : 5;
    if(LONG && !tax && sip<3000) maxN=1;          // chhoti long-term SIP → ek Multi Cap (mid + small dono andar)
    if(LONG && sip>=3000) maxN=Math.max(maxN,3);   // core + Mid + Small
    if(maxN===1){
      // Ek hi fund: long-term → Multi Cap (usme mid + small dono 25%+), tax → ELSS
      slots=[{k: tax?'elss' : LONG?'multicap' : slots[0].k, w:1}];
      if(LONG && !tax) notes.push('Small SIP — one Multi Cap fund gives Mid + Small exposure (min. 25% each)');
    } else {
      maxN = Math.max(maxN, Math.min(must.length, Math.floor(sip/1000)));
      let keep=slots.slice(0,maxN);
      must.forEach(k=>{ if(keep.some(s=>s.k===k)) return;
        let j=-1; for(let x=keep.length-1;x>=0;x--){ if(!must.includes(keep[x].k)){ j=x; break; } }
        const add=slots.find(s=>s.k===k); if(j>=0) keep[j]=add; else if(keep.length<maxN) keep.push(add); });
      slots=keep;
    }
    if(LONG && slots.some(s=>s.k==='small') && (BIG_EQ.includes(K(p.oEquity)) || (r.e && +r.e.asset_value>=5e5)))
      notes.push('Client already holds ₹5 L+ in direct shares — check overlap with Mid / Small Cap stocks');
    const W=slots.reduce((a,s)=>a+s.w,0);
    slots.forEach(s=>{ s.amt=r500(sip*s.w/W); });
    // ₹1,000 se kam wala fund hata kar sabse bade me jodo
    let tiny; while(slots.length>1 && (tiny=slots.find(s=>s.amt<1000 && !must.includes(s.k)) || (slots.every(s=>s.amt<1000||must.includes(s.k)) && slots.find(s=>s.amt<500)))){ slots=slots.filter(s=>s!==tiny); slots.reduce((a,s)=>s.amt>a.amt?s:a,slots[0]).amt+=tiny.amt; }
    const diff = sip - slots.reduce((a,s)=>a+s.amt,0); slots.reduce((a,s)=>s.amt>a.amt?s:a,slots[0]).amt += diff;
    // ELSS: 80C limit ₹1.5 L/saal = ₹12,500/month — upar ka flexi me
    const el=slots.find(s=>s.k==='elss');
    if(el && el.amt>12500){ const extra=el.amt-12500; el.amt=12500; slots.push({k:'flexi',amt:extra}); elssCapNote='ELSS capped at ₹12,500/month (80C limit); the rest goes to Flexi Cap'; notes.push(elssCapNote); }
    slots = slots.filter(s=>s.amt>0);
    const alloc = slots.map(s=>({k:s.k, amt:s.amt, ...CAT[s.k]}));
    if(efPart) alloc.push({k:'liquid', amt:efPart, ...CAT.liquid});

    // ── RM Talking Points (internal) ──
    const curSip = r.c ? (+r.c.sip_amount||0) : 0;
    if(curSip) talk.push(`Current SIP with us is ${inr(curSip)} — confirm whether it is already counted in the savings figure. Suggested ${inr(total)}${total>curSip?` → increase by ${inr(total-curSip)}`:' — current SIP is sufficient'}.`);
    if(planned && !noSip && sip>planned[1]) talk.push(`Client planned ${p.sipAmount}; savings allow about ${inr(sip)} — increase gradually.`);
    if(noSip) talk.push(`Client does not want a SIP (${modeTxt||'Lumpsum'}) — apply the same split as % of the lumpsum.`);
    if(tax) talk.push('Confirm the tax regime — ELSS gives no benefit under the NEW regime (move that part to Flexi Cap). PPF / EPF may already fill the 80C limit.');
    if(efS==='r' && (BIG_EQ.includes(K(p.oEquity)) || K(p.oFd)!=='none' && p.oFd)) talk.push('To build the emergency fund faster, suggest moving a lumpsum from shares / FD into a Liquid Fund.');
    if(efPart) talk.push(`Once the emergency target (${big(efTarget)}) is reached, shift the ${inr(efPart)} Liquid SIP to equity.`);
    if(lcS==='r'||lcS==='a') talk.push('Discuss a term plan — LIC endowment / money-back policies give low cover.');
    if(has(p,'oOther','LIC / Insurance Plans')) talk.push('Review existing LIC / insurance plans — premium, maturity value and cover.');
    if(p.oMf && K(p.oMf)!=='none') talk.push(`Holds MF ${p.oMf} elsewhere — offer consolidation / portfolio review.`);
    if(p.oSip && K(p.oSip)!=='none') talk.push(`Running SIP ${p.oSip} elsewhere — keep total equity exposure in mind.`);
    if((has(p,'goal','Child Education')||has(p,'goal','House / Car')) && period===K('3 – 5 years')) talk.push('For the short-term goal (education / house / car), keep that portion in hybrid / debt instead of equity.');
    if(K(p.r2)===K('Sell')) talk.push('Client would sell on a 20% fall — explain volatility and advise not to stop SIPs.');
    const stepUp = !old && !retired && K(p.r1)!==K('Irregular');
    if(irregular && !noSip) talk.push('Irregular / business income — keep a smaller base SIP and top up in good months.');
    if(wantsEq||wantsIpo) talk.push(`Client is interested in ${[wantsEq&&'direct equity (shares)',wantsIpo&&'IPOs'].filter(Boolean).join(' and ')} — ${r.e?`existing equity client (RM ${r.e.rm||'—'}); coordinate with them`:'check demat / trading account with us; open one if needed'}${wantsIpo?'. Share upcoming IPOs and help apply via UPI / ASBA':''}.`);
    if(/lumpsum/i.test(modeTxt)) talk.push('Do not put a lumpsum into equity at once — park it in Liquid / Arbitrage and STP over 6–12 months into this split.');
    if(old) talk.push('Age 55+ — discuss an SWP plan for post-retirement income.');
    if(p.oMf && BIG_EQ.includes(K(p.oMf))) talk.push('Large MF holding elsewhere — get the statement and check overlap to avoid duplicate fund types.');

    return {checks, total, sip, efPart, efTarget, alloc, lvlName:LVL[lvl], talk, notes, stepUp, noSip, tax, auto, curSip, combo, comboEn:COMBO[combo].en, comboHi:COMBO[combo].hi, comboWhy:COMBO[combo].why, comboWhyHi:COMBO[combo].whyHi};
  }

  // ══════════════ RM EDITS ══════════════
  // _edit[profileId] = {checks:{key:{s,adv,advHi}}, alloc:[{k,scheme,amt}], note, by, at}
  // Is browser me save (localStorage) — RM baad me khole to edit wahi milta hai.
  // Keys "dninvest_" se shuru NAHI — CRM ka 🧹 Clear Cache RM ke edits / combo nahi mitayega
  const EKEY='dnplan_edits', CKEY='dnplan_combo';
  const loadKey=(k,old)=>{ try{ const v=localStorage.getItem(k)||localStorage.getItem(old); if(v && !localStorage.getItem(k)){ localStorage.setItem(k,v); localStorage.removeItem(old); } return JSON.parse(v||'{}')||{}; }catch(_){ return {}; } };
  let _edit = loadKey(EKEY,'dninvest_plan_edits');
  const saveEdits = ()=>{ try{ localStorage.setItem(EKEY, JSON.stringify(_edit)); }catch(_){} };
  let _combo = loadKey(CKEY,'dninvest_plan_combo');
  const _editing = {};   // pid → true jab editor khula ho
  const _draft = {};     // editor ka beech ka data

  // Auto plan + RM edits = final plan (card, WhatsApp, image, print — sab yahi use karte hain)
  function final(r){
    const A=analyse(r, _combo[r.p._id]||'rec'), E=_edit[r.p._id];
    const F={...A, note:'', edited:false, checks:A.checks.map(c=>({...c})), alloc:A.alloc.map(a=>({...a, scheme:''}))};
    if(!E) return F;
    F.edited=true; F.by=E.by; F.at=E.at; F.note=E.note||'';
    F.checks.forEach(c=>{ const x=E.checks&&E.checks[c.key]; if(x){ c.s=x.s||c.s; c.adv=x.adv!=null?x.adv:c.adv; c.advHi=x.advHi!=null?x.advHi:c.advHi; } });
    if(Array.isArray(E.alloc) && E.alloc.length){
      F.alloc=E.alloc.filter(a=>CAT[a.k] && +a.amt>0).map(a=>({k:a.k, amt:+a.amt, scheme:a.scheme||'', ...CAT[a.k]}));
      F.total=F.alloc.reduce((s,a)=>s+a.amt,0);
      F.efPart=F.alloc.filter(a=>a.k==='liquid').reduce((s,a)=>s+a.amt,0);
      F.sip=F.total-F.efPart;
      F.tax=F.alloc.some(a=>a.k==='elss');
    }
    return F;
  }

  // ══════════════ CRM CARD ══════════════
  const btnS='padding:5px 10px;font-size:.72rem';
  function html(i){
    const r=(window._profRows||[])[i]; if(!r||!r.p) return '';
    let F; try{ F=final(r); }catch(e){ console.error('PLAN error',e); return `<div style="color:var(--red);font-size:.75rem;margin:8px 0">⚠️ Could not build the suggested plan — ${esc(e.message)}</div>`; }
    if(_editing[r.p._id]) return editorHtml(i, r, F);
    const red=F.checks.filter(c=>c.s==='r').length, amb=F.checks.filter(c=>c.s==='a').length;
    const chk = F.checks.map(c=>{ const s=ST[c.s]||ST.n; return `<div style="background:${s.bg};border-left:4px solid ${s.c};border-radius:8px;padding:7px 9px">
        <div style="font-size:.66rem;font-weight:800;color:${s.c};text-transform:uppercase">${s.e} ${esc(c.en)}</div>
        <div style="font-size:.7rem;color:var(--gray)">${esc(c.have)}</div>
        <div style="font-size:.74rem;font-weight:700;color:var(--navy);margin-top:2px">${esc(c.adv)}</div></div>`; }).join('');
    const tot=F.total||1;
    const bar = F.alloc.map(a=>`<div title="${esc(a.en)} ${inr(a.amt)}" style="width:${a.amt/tot*100}%;background:${a.c}"></div>`).join('');
    const rows = F.alloc.map(a=>`<tr><td style="width:14px"><span style="display:inline-block;width:11px;height:11px;border-radius:3px;background:${a.c}"></span></td>
        <td><b>${esc(a.en)}</b>${a.scheme?`<br><span style="color:#1a56db;font-weight:700">${esc(a.scheme)}</span>`:''}<br><small style="color:var(--gray)">${esc(a.why)}</small></td>
        <td style="text-align:right;font-weight:800;color:var(--navy);white-space:nowrap">${inr(a.amt)}</td>
        <td style="text-align:right;color:var(--gray);font-size:.7rem">${Math.round(a.amt/tot*100)}%</td></tr>`).join('');
    return `<div id="planCard" style="border:2px solid var(--gold);border-radius:14px;padding:12px;margin:4px 0 14px;background:linear-gradient(180deg,#fdf6e3,#fff 120px)">
      <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px;margin-bottom:8px">
        <div style="font-weight:900;color:var(--navy);font-size:1rem">💡 Suggested Plan <small style="font-weight:600;color:var(--gray);font-size:.68rem">${F.edited?`✏️ edited by ${esc(F.by||'RM')}`:'auto'} · ${esc(F.lvlName)}</small></div>
        <div style="display:flex;gap:6px;align-items:center;font-size:.7rem;font-weight:700">${red?`<span style="color:#c0392b">🔴 ${red}</span>`:''}${amb?`<span style="color:#b45309">🟡 ${amb}</span>`:''}${!red&&!amb?'<span style="color:#0f7a45">🟢 All good</span>':''}
          <button class="btn btn-outline" style="${btnS}" onclick="PLAN.edit(${i})">✏️ Edit Plan</button></div>
      </div>
      <div style="font-size:.7rem;font-weight:800;color:var(--gray);text-transform:uppercase;margin-bottom:5px">Financial Health Check</div>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:6px;margin-bottom:12px">${chk}</div>
      ${F.edited?'':`<div style="font-size:.7rem;font-weight:800;color:var(--gray);text-transform:uppercase;margin-bottom:5px">Fund Combination <small style="text-transform:none;font-weight:600">— choose what suits the client</small></div>
      <div style="display:flex;gap:5px;flex-wrap:wrap;margin-bottom:5px">${COMBOS.map(k=>`<button class="btn ${k===F.combo?'':'btn-outline'}" style="${btnS};${k===F.combo?'background:var(--navy);color:#f5d98a;border:1px solid var(--navy)':''}" onclick="PLAN.setCombo(${i},'${k}')">${COMBO[k].short}</button>`).join('')}</div>
      <div style="font-size:.7rem;color:var(--gray);margin-bottom:10px">${esc(F.comboWhy)}</div>`}
      <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:6px;margin-bottom:6px">
        <div style="font-size:.7rem;font-weight:800;color:var(--gray);text-transform:uppercase">Suggested Monthly ${F.noSip?'Investment':'SIP'}
          <span style="font-size:1.15rem;color:var(--navy);text-transform:none;margin-left:4px">${inr(F.total)}</span>
          ${F.efPart?`<small style="text-transform:none;font-weight:600">(${inr(F.sip)} equity + ${inr(F.efPart)} emergency)</small>`:''}</div>
        ${F.edited?'':`<div style="display:flex;gap:4px;align-items:center">
          <input id="plan-amt" type="number" step="500" min="500" value="${F.total}" style="width:95px;padding:5px 7px;border:1px solid var(--border);border-radius:7px;font-size:.78rem">
          <button class="btn btn-outline" style="${btnS}" onclick="PLAN.setAmt(${i})">↻ Recalc</button>
          ${_over[r.p._id]?`<button class="btn btn-outline" style="${btnS}" title="Back to auto amount (${inr(F.auto)})" onclick="PLAN.setAmt(${i},true)">Auto</button>`:''}</div>`}
      </div>
      <div style="display:flex;height:16px;border-radius:8px;overflow:hidden;margin-bottom:6px">${bar}</div>
      <table style="width:100%;font-size:.76rem;margin-bottom:6px"><tbody>${rows}</tbody></table>
      ${F.stepUp?'<div style="font-size:.72rem;color:#0f7a45;font-weight:700;margin-bottom:6px">📈 Set up a 10% annual step-up</div>':''}
      ${F.note?`<div style="background:#fdf6e3;border:1px solid #f5d98a;border-radius:8px;padding:6px 9px;font-size:.74rem;margin-bottom:6px;white-space:pre-wrap"><b>📝 Note for client:</b> ${esc(F.note)}</div>`:''}
      ${F.notes.length&&!F.edited?`<div style="font-size:.68rem;color:var(--gray);margin-bottom:6px">ℹ️ ${F.notes.map(esc).join(' · ')}</div>`:''}
      ${F.talk.length?`<div style="background:#eef3fc;border-radius:8px;padding:8px 10px;margin-bottom:8px">
        <div style="font-size:.68rem;font-weight:800;color:#1a56db;text-transform:uppercase;margin-bottom:3px">🗣️ RM Talking Points <small style="font-weight:600;text-transform:none;color:var(--gray)">(internal — not sent to the client)</small></div>
        <ul style="margin:0;padding-left:16px;font-size:.73rem;line-height:1.45">${F.talk.map(t=>`<li>${esc(t)}</li>`).join('')}</ul></div>`:''}
      <div style="display:flex;gap:6px;flex-wrap:wrap">
        <button class="btn" style="background:#25D366;color:#fff;border:none" onclick="PLAN.sendImage(${i})" title="Creates a colourful image and opens WhatsApp">🖼️ Send Colorful Plan (WhatsApp)</button>
        <button class="btn btn-outline" onclick="PLAN.wa(${i})" title="Plain text message only">💬 Send as Text</button>
        <button class="btn btn-outline" onclick="PLAN.downloadImage(${i})">⬇️ Image</button>
        <button class="btn btn-outline" onclick="PLAN.print(${i})">🖨️ PDF</button>
      </div>
      <div style="font-size:.62rem;color:var(--gray);margin-top:6px">Rule-based suggestion — the RM finalises schemes and amounts after discussing with the client.</div>
    </div>`;
  }

  function setAmt(i, reset){
    const r=(window._profRows||[])[i]; if(!r) return;
    if(reset) delete _over[r.p._id];
    else { const v=r500(+(document.getElementById('plan-amt')||{}).value||0); if(v<500){ toast&&toast('Minimum amount is ₹500','error'); return; } _over[r.p._id]=v; }
    redraw(i);
  }
  function setCombo(i,k){
    const r=(window._profRows||[])[i]; if(!r||!COMBO[k]) return;
    if(k==='rec') delete _combo[r.p._id]; else _combo[r.p._id]=k;
    try{ localStorage.setItem(CKEY, JSON.stringify(_combo)); }catch(_){}
    redraw(i);
  }
  function redraw(i){ const el=document.getElementById('planCard'); if(el) el.outerHTML=html(i); }

  // ══════════════ EDITOR ══════════════
  const inS='padding:5px 7px;border:1px solid var(--border);border-radius:7px;font-size:.76rem';
  function editorHtml(i, r, F){
    const pid=r.p._id;
    const D=_draft[pid] || (_draft[pid]={
      checks:F.checks.map(c=>({key:c.key,en:c.en,s:c.s,adv:c.adv,advHi:c.advHi})),
      alloc:F.alloc.map(a=>({k:a.k,scheme:a.scheme||'',amt:a.amt})), note:F.note||''});
    const catOpts=k=>GROUPS.map(g=>`<optgroup label="${g}">`+Object.entries(CAT).filter(([,c])=>c.g===g).map(([kk,c])=>`<option value="${kk}"${kk===k?' selected':''}>${esc(c.en)}</option>`).join('')+'</optgroup>').join('');
    const stOpts=s=>[['g','🟢 Good'],['a','🟡 Attention'],['r','🔴 Gap'],['n','⚪ N/A']].map(([v,l])=>`<option value="${v}"${v===s?' selected':''}>${l}</option>`).join('');
    const ck=D.checks.map((c,n)=>`<div style="display:grid;grid-template-columns:120px 105px 1fr;gap:5px;align-items:center;margin-bottom:5px">
        <b style="font-size:.72rem">${esc(c.en)}</b>
        <select id="pe-cs-${n}" style="${inS}">${stOpts(c.s)}</select>
        <div style="display:grid;gap:3px"><input id="pe-ca-${n}" value="${esc(c.adv)}" placeholder="Advice (English)" style="${inS}">
        <input id="pe-ch-${n}" value="${esc(c.advHi)}" placeholder="Advice in Hindi (optional)" style="${inS}"></div></div>`).join('');
    const al=D.alloc.map((a,n)=>`<div style="display:grid;grid-template-columns:1.1fr 1.3fr 90px 28px;gap:5px;align-items:center;margin-bottom:5px">
        <select id="pe-k-${n}" style="${inS}" onchange="PLAN.edCat(${n})">${catOpts(a.k)}</select>
        <div><input id="pe-sc-${n}" list="pe-dl-${n}" value="${esc(a.scheme)}" placeholder="Scheme name — type / pick (optional)" autocomplete="off" style="${inS};width:100%">
        <datalist id="pe-dl-${n}">${dlOpts(a.k)}</datalist></div>
        <input id="pe-a-${n}" type="number" step="500" min="0" value="${a.amt}" oninput="PLAN.edSum(${i})" style="${inS}">
        <button class="btn-icon" title="Remove" style="color:var(--red)" onclick="PLAN.edDel(${i},${n})">✖</button></div>`).join('');
    const sum=D.alloc.reduce((s,a)=>s+(+a.amt||0),0);
    return `<div id="planCard" style="border:2px dashed var(--blue);border-radius:14px;padding:12px;margin:4px 0 14px;background:#f8fbff">
      <div style="font-weight:900;color:var(--navy);margin-bottom:8px">✏️ Edit Plan <small style="font-weight:600;color:var(--gray);font-size:.68rem">— changes here are what the client receives</small></div>
      <div style="font-size:.7rem;font-weight:800;color:var(--gray);text-transform:uppercase;margin-bottom:5px">Health Check</div>${ck}
      <div style="font-size:.7rem;font-weight:800;color:var(--gray);text-transform:uppercase;margin:10px 0 5px">Funds / SIP Split</div>${al}
      <div style="display:flex;justify-content:space-between;align-items:center;margin:4px 0 10px">
        <button class="btn btn-outline" style="${btnS}" onclick="PLAN.edAdd(${i})">➕ Add Fund</button>
        <b style="font-size:.85rem;color:var(--navy)">Total: <span id="pe-sum">${inr(sum)}</span></b></div>
      <div style="font-size:.7rem;font-weight:800;color:var(--gray);text-transform:uppercase;margin-bottom:4px">📝 Note for client <small style="text-transform:none;font-weight:600">(optional — Hindi or English; included in the image and message)</small></div>
      <textarea id="pe-note" rows="2" style="${inS};width:100%;resize:vertical">${esc(D.note)}</textarea>
      <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:10px">
        <button class="btn btn-primary" onclick="PLAN.edSave(${i})">✅ Save Plan</button>
        <button class="btn btn-outline" onclick="PLAN.edCancel(${i})">Cancel</button>
        ${_edit[pid]?`<button class="btn btn-outline" style="color:var(--red)" onclick="PLAN.edReset(${i})">↺ Reset to Auto Plan</button>`:''}
      </div></div>`;
  }
  const rowOf = i => (window._profRows||[])[i];
  function edRead(i){   // DOM → draft
    const r=rowOf(i); if(!r) return null; const D=_draft[r.p._id]; if(!D) return null;
    const v=id=>{ const el=document.getElementById(id); return el?el.value:null; };
    D.checks.forEach((c,n)=>{ if(v('pe-cs-'+n)!=null){ c.s=v('pe-cs-'+n); c.adv=v('pe-ca-'+n); c.advHi=v('pe-ch-'+n); } });
    D.alloc.forEach((a,n)=>{ if(v('pe-k-'+n)!=null){ a.k=v('pe-k-'+n); a.scheme=v('pe-sc-'+n).trim(); a.amt=Math.max(0,Math.round(+v('pe-a-'+n)||0)); } });
    if(v('pe-note')!=null) D.note=v('pe-note');
    return D;
  }
  function edCat(n){   // category badli → us category ke scheme suggestions
    const k=(document.getElementById('pe-k-'+n)||{}).value, dl=document.getElementById('pe-dl-'+n);
    if(dl) dl.innerHTML=dlOpts(k);
    const sc=document.getElementById('pe-sc-'+n); if(sc && sc.value && CAT[k] && !CAT[k].kw.test(sc.value)) sc.value='';
  }
  function edit(i){ const r=rowOf(i); if(!r) return; _editing[r.p._id]=true; delete _draft[r.p._id]; redraw(i); }
  function edSum(i){ const D=edRead(i); const el=document.getElementById('pe-sum'); if(D&&el) el.textContent=inr(D.alloc.reduce((s,a)=>s+(+a.amt||0),0)); }
  function edAdd(i){ const D=edRead(i); if(!D) return; D.alloc.push({k:'flexi',scheme:'',amt:1000}); redraw(i); }
  function edDel(i,n){ const D=edRead(i); if(!D) return; D.alloc.splice(n,1); redraw(i); }
  function edCancel(i){ const r=rowOf(i); if(!r) return; delete _editing[r.p._id]; delete _draft[r.p._id]; redraw(i); }
  function edSave(i){
    const r=rowOf(i); const D=edRead(i); if(!r||!D) return;
    const alloc=D.alloc.filter(a=>+a.amt>0);
    if(!alloc.length){ toast&&toast('Enter an amount for at least one fund','error'); return; }
    const checks={}; D.checks.forEach(c=>checks[c.key]={s:c.s,adv:c.adv,advHi:c.advHi});
    _edit[r.p._id]={checks, alloc, note:(D.note||'').trim(), by:(typeof CU!=='undefined'&&CU&&CU.name)||'', at:new Date().toISOString()};
    saveEdits();
    delete _editing[r.p._id]; delete _draft[r.p._id];
    redraw(i); toast&&toast('Plan saved ✓','success');
  }
  function edReset(i){
    const r=rowOf(i); if(!r) return;
    if(!confirm('Discard your edits and go back to the auto plan?')) return;
    delete _edit[r.p._id]; saveEdits(); delete _editing[r.p._id]; delete _draft[r.p._id]; redraw(i);
  }

  // ══════════════ CLIENT MESSAGE (Hindi + English) ══════════════
  const firstName = p => { const f=String(p.name||'').trim().split(/\s+/)[0]||''; return f.charAt(0).toUpperCase()+f.slice(1).toLowerCase(); };
  const rmName = () => (typeof CU!=='undefined'&&CU&&CU.name)||'';
  function text(i){
    const r=rowOf(i); if(!r) return '';
    const F=final(r), p=r.p, L=[];
    L.push('*D N INVESTMENT — Your Investment Plan*','*आपका निवेश प्लान*','');
    L.push(`Dear ${firstName(p)},`,'Thank you for filling the profile form. Based on your answers, here is our suggested plan.','प्रोफ़ाइल फ़ॉर्म भरने के लिए धन्यवाद। आपके उत्तरों के आधार पर हमारा सुझाया गया प्लान:','');
    L.push(`*Risk Profile / जोखिम प्रोफ़ाइल:* ${F.lvlName} — ${LVL_HI[F.lvlName]||''}`,'');
    L.push('*Financial Health Check / वित्तीय स्वास्थ्य जाँच*');
    F.checks.forEach(c=>{ L.push(`${(ST[c.s]||ST.n).e} ${c.en}: ${c.adv}`); if(c.advHi) L.push(`    ${c.hi}: ${c.advHi}`); });
    L.push('',`*Suggested Monthly ${F.noSip?'Investment':'SIP'} / सुझाया गया मासिक निवेश: ${inr(F.total)}*`);
    if(!F.edited) L.push(`_${F.comboEn} — ${F.comboWhy}_`,`_${F.comboHi} — ${F.comboWhyHi}_`);
    F.alloc.forEach(a=>L.push(`• ${a.en} / ${a.hi} — ${inr(a.amt)}${a.scheme?`\n    _${a.scheme}_`:''}`));
    if(F.efPart) L.push('',`💰 ${inr(F.efPart)}/month goes to a Liquid Fund until your emergency fund (~${big(F.efTarget)}) is built; then it moves to equity.`,`💰 ${inr(F.efPart)} हर महीने लिक्विड फंड में, जब तक इमरजेंसी फंड (~${bigHi(F.efTarget)}) न बन जाए; फिर ये इक्विटी में।`);
    if(F.tax) L.push('','🧾 ELSS gives tax benefit only under the Old Tax Regime.','🧾 ELSS का टैक्स लाभ सिर्फ़ पुरानी टैक्स व्यवस्था (Old Regime) में मिलता है।');
    if(F.stepUp) L.push('','📈 Increase your SIP by 10% every year.','📈 हर साल अपनी SIP 10% बढ़ाएँ।');
    if(F.note) L.push('','📝 '+F.note);
    L.push('','This is a suggestion based on your profile. Your Relationship Manager will discuss the final schemes with you.','यह आपकी प्रोफ़ाइल पर आधारित सुझाव है। आपके रिलेशनशिप मैनेजर अंतिम स्कीम पर आपसे बात करेंगे।');
    L.push('','_Mutual Fund investments are subject to market risks, read all scheme related documents carefully._','_म्यूचुअल फंड निवेश बाज़ार जोखिमों के अधीन हैं, योजना से जुड़े सभी दस्तावेज़ ध्यान से पढ़ें।_');
    L.push('',rmName(), 'D N Investment · ARN-67217');
    return L.join('\n').trim();
  }
  function caption(i){
    const p=rowOf(i).p;
    return [`Dear ${firstName(p)},`,'Your investment plan from D N Investment is attached. Please have a look — we will call you to discuss.',
      'आपका निवेश प्लान संलग्न है। कृपया देखें — हम आपसे बात करने के लिए कॉल करेंगे।','',
      '_Mutual Fund investments are subject to market risks, read all scheme related documents carefully._','',rmName(),'D N Investment'].join('\n');
  }
  const waUrl = (p,t) => { const m=String(p.mobile||'').replace(/\D/g,'').slice(-10); return (/^[6-9]\d{9}$/.test(m)?'https://wa.me/91'+m:'https://wa.me/')+'?text='+encodeURIComponent(t); };
  function wa(i){ const r=rowOf(i); if(!r) return; window.open(waUrl(r.p, text(i)),'_blank'); }
  async function copy(i){ const t=text(i); try{ await navigator.clipboard.writeText(t); toast&&toast('Plan text copied ✓','success'); }catch(_){ prompt('Copy:',t); } }

  // ══════════════ COLORFUL IMAGE (canvas → PNG) ══════════════
  const FONT = '"Segoe UI","Nirmala UI","Noto Sans Devanagari","Mangal",Roboto,system-ui,sans-serif';
  const fnt = (sz,w) => `${w||400} ${sz}px ${FONT}`;
  let _logo=null;
  function logo(){
    if(_logo) return _logo;
    const base=location.origin+location.pathname.replace(/[^/]*$/,'');
    _logo=new Promise(res=>{ const tryLoad=(srcs)=>{ if(!srcs.length) return res(null); const im=new Image(); im.onload=()=>res(im); im.onerror=()=>tryLoad(srcs.slice(1)); im.src=srcs[0]; }; tryLoad([base+'dn-logo.png', base+'logo_new.png']); setTimeout(()=>res(null),4000); });
    return _logo;
  }
  function rr(ctx,x,y,w,h,r){ ctx.beginPath(); ctx.moveTo(x+r,y); ctx.arcTo(x+w,y,x+w,y+h,r); ctx.arcTo(x+w,y+h,x,y+h,r); ctx.arcTo(x,y+h,x,y,r); ctx.arcTo(x,y,x+w,y,r); ctx.closePath(); }
  function lines(ctx, t, font, maxW){
    ctx.font=font; const out=[];
    String(t||'').split('\n').forEach(par=>{ let cur=''; par.split(/\s+/).forEach(w=>{ const nx=cur?cur+' '+w:w; if(ctx.measureText(nx).width>maxW && cur){ out.push(cur); cur=w; } else cur=nx; }); out.push(cur); });
    return out;
  }
  function txt(ctx, t, x, y, maxW, font, color, lh){ const ls=lines(ctx,t,font,maxW); ctx.fillStyle=color; ctx.textBaseline='top'; ls.forEach((l,n)=>ctx.fillText(l,x,y+n*lh)); return y+ls.length*lh; }

  async function makeCanvas(i){
    const r=rowOf(i); const F=final(r), p=r.p;
    const W=1080, P=48, IW=W-2*P;
    const cv=document.createElement('canvas'); cv.width=W; cv.height=4200;
    const c=cv.getContext('2d');
    c.fillStyle='#ffffff'; c.fillRect(0,0,W,cv.height);
    // Header
    const g=c.createLinearGradient(0,0,W,0); g.addColorStop(0,'#0f1f3d'); g.addColorStop(1,'#1e3a6e');
    c.fillStyle=g; c.fillRect(0,0,W,180);
    c.fillStyle='#c9942a'; c.fillRect(0,180,W,6);
    const im=await logo(); let hx=P;
    if(im){ const h=120, w=im.width*h/im.height; c.fillStyle='#fff'; rr(c,P-8,22,w+16,h+16,14); c.fill(); c.drawImage(im,P,30,w,h); hx=P+w+30; }
    txt(c,'D N INVESTMENT',hx,40,W-hx-P,fnt(50,900),'#f5d98a',58);
    txt(c,'TRUSTED WEALTH PARTNER',hx,100,W-hx-P,fnt(20,700),'#c9942a',26);
    txt(c,'Mutual Fund Distributor · ARN-67217 · Bistupur, Jamshedpur',hx,130,W-hx-P,fnt(19),'#c8d3e6',24);
    let y=216;
    // Title
    y=txt(c,'Your Investment Plan',P,y,IW,fnt(44,900),'#0f1f3d',52);
    y=txt(c,'आपका निवेश प्लान',P,y,IW,fnt(30,700),'#9a7a2a',40);
    const d=new Date().toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'});
    y=txt(c,`Prepared for ${p.name||''} · ${d}`,P,y+6,IW,fnt(24,600),'#5d6e8a',32)+14;
    // Risk pill
    const pill=`Risk Profile: ${F.lvlName}  ·  ${LVL_HI[F.lvlName]||''}`; c.font=fnt(24,800); const pw=c.measureText(pill).width+44;
    c.fillStyle='#0f1f3d'; rr(c,P,y,pw,50,25); c.fill(); txt(c,pill,P+22,y+11,pw,fnt(24,800),'#f5d98a',30); y+=74;
    // Health check
    const sec=(en,hi)=>{ c.fillStyle='#c9942a'; c.fillRect(P,y+6,8,30); txt(c,en,P+20,y,IW,fnt(26,900),'#0f1f3d',34); c.font=fnt(26,900); const w=c.measureText(en).width; txt(c,'· '+hi,P+30+w,y+3,IW,fnt(22,600),'#9a7a2a',30); y+=50; };
    sec('FINANCIAL HEALTH CHECK','वित्तीय स्वास्थ्य जाँच');
    const cw=(IW-20)/2, ci=cw-46;
    const cardH=ch=>{ let h=18; h+=lines(c,ch.en,fnt(23,800),ci).length*30; h+=lines(c,ch.have,fnt(19),ci).length*25; h+=4+lines(c,ch.adv,fnt(23,700),ci).length*30; if(ch.advHi) h+=lines(c,ch.advHi,fnt(21),ci).length*29; return h+18; };
    for(let n=0;n<F.checks.length;n+=2){
      const pair=F.checks.slice(n,n+2), h=Math.max(...pair.map(cardH));
      pair.forEach((ch,k)=>{ const s=ST[ch.s]||ST.n, x=P+k*(cw+20);
        c.fillStyle=s.bg; rr(c,x,y,cw,h,16); c.fill(); c.fillStyle=s.c; rr(c,x,y,10,h,5); c.fill();
        c.beginPath(); c.arc(x+36,y+33,11,0,Math.PI*2); c.fill();
        let yy=txt(c,ch.en.toUpperCase(),x+56,y+18,ci-30,fnt(23,800),s.c,30);
        yy=txt(c,ch.hi+' · '+ch.have,x+26,yy+2,ci,fnt(19),'#5d6e8a',25);
        yy=txt(c,ch.adv,x+26,yy+4,ci,fnt(23,700),'#0f1f3d',30);
        if(ch.advHi) txt(c,ch.advHi,x+26,yy,ci,fnt(21),'#7a6a3a',29);
      });
      y+=h+16;
    }
    y+=14;
    // SIP
    sec(`SUGGESTED MONTHLY ${F.noSip?'INVESTMENT':'SIP'}`,'सुझाया गया मासिक निवेश');
    const bg=c.createLinearGradient(P,0,W-P,0); bg.addColorStop(0,'#fdf6e3'); bg.addColorStop(1,'#fff');
    c.fillStyle=bg; rr(c,P,y,IW,110,18); c.fill(); c.strokeStyle='#f5d98a'; c.lineWidth=2; c.stroke();
    txt(c,inr(F.total),P+28,y+18,IW,fnt(64,900),'#0f1f3d',72);
    txt(c,'per month · प्रति माह',P+30+ (c.font=fnt(64,900), c.measureText(inr(F.total)).width)+16,y+30,IW,fnt(22,700),'#5d6e8a',28);
    if(F.efPart) txt(c,`${inr(F.sip)} equity + ${inr(F.efPart)} emergency fund`,P+30+(c.font=fnt(64,900),c.measureText(inr(F.total)).width)+16,y+62,IW,fnt(20,600),'#9a7a2a',26);
    y+=130;
    if(!F.edited){
      c.fillStyle='#0f1f3d'; c.font=fnt(22,800); const cl=F.comboEn; const cw2=c.measureText(cl).width+36;
      rr(c,P,y,cw2,40,20); c.fill(); txt(c,cl,P+18,y+8,cw2,fnt(22,800),'#f5d98a',26);
      txt(c,F.comboHi,P+cw2+14,y+10,IW-cw2-14,fnt(21,700),'#9a7a2a',26); y+=50;
      y=txt(c,F.comboWhy,P,y,IW,fnt(20),'#5d6e8a',26); y=txt(c,F.comboWhyHi,P,y,IW,fnt(20),'#7a6a3a',27)+12;
    }
    // bar
    const tot=F.total||1; let bx=P; c.save(); rr(c,P,y,IW,36,18); c.clip();
    F.alloc.forEach(a=>{ const w=IW*a.amt/tot; c.fillStyle=a.c; c.fillRect(bx,y,w+1,36); bx+=w; }); c.restore(); y+=56;
    // rows
    F.alloc.forEach(a=>{
      const tx=P+50, tw=IW-50-230; let yy=y+10;
      c.fillStyle=a.c; rr(c,P,y+12,30,30,7); c.fill();
      yy=txt(c,a.en,tx,yy,tw,fnt(26,800),'#0f1f3d',33);
      yy=txt(c,a.hi,tx,yy,tw,fnt(22,600),'#9a7a2a',29);
      if(a.scheme) yy=txt(c,a.scheme,tx,yy+2,tw,fnt(23,700),'#1a56db',30);
      yy=txt(c,a.why,tx,yy+2,tw,fnt(19),'#5d6e8a',25);
      c.textAlign='right'; txt(c,inr(a.amt),W-P,y+12,230,fnt(32,900),'#0f1f3d',38); txt(c,Math.round(a.amt/tot*100)+'%',W-P,y+54,230,fnt(21,700),a.c,26); c.textAlign='left';
      y=Math.max(yy,y+90)+12; c.fillStyle='#e3e8f0'; c.fillRect(P,y,IW,2); y+=10;
    });
    y+=12;
    // boxes
    const box=(icon,en,hi,col,bgc)=>{ const w=IW-80; const h=20+lines(c,en,fnt(22,700),w).length*30+(hi?lines(c,hi,fnt(21),w).length*29:0)+18;
      c.fillStyle=bgc; rr(c,P,y,IW,h,14); c.fill(); c.fillStyle=col; rr(c,P,y,8,h,4); c.fill();
      txt(c,icon,P+22,y+18,40,fnt(28),col,32); let yy=txt(c,en,P+66,y+20,w,fnt(22,700),'#0f1f3d',30); if(hi) txt(c,hi,P+66,yy,w,fnt(21),'#7a6a3a',29); y+=h+12; };
    if(F.efPart) box('💰',`${inr(F.efPart)}/month goes to a Liquid Fund until your emergency fund (~${big(F.efTarget)}) is built; then it moves to equity.`,`${inr(F.efPart)} हर महीने लिक्विड फंड में, जब तक इमरजेंसी फंड (~${bigHi(F.efTarget)}) न बन जाए; फिर ये इक्विटी में।`,'#64748b','#f1f5f9');
    if(F.tax) box('🧾','ELSS gives tax benefit only under the Old Tax Regime.','ELSS का टैक्स लाभ सिर्फ़ पुरानी टैक्स व्यवस्था में मिलता है।','#b91c1c','#fdf0ee');
    if(F.stepUp) box('📈','Increase your SIP by 10% every year — small step-ups make a big difference over time.','हर साल SIP 10% बढ़ाएँ — छोटी बढ़त लंबे समय में बड़ा फ़र्क लाती है।','#0f7a45','#e3f7ee');
    if(F.note) box('📝','Note from your RM: '+F.note,'','#c9942a','#fdf6e3');
    y+=8;
    y=txt(c,'This is a suggestion based on the information you provided. Your Relationship Manager will discuss specific schemes and the final plan with you.',P,y,IW,fnt(19),'#5d6e8a',25);
    y=txt(c,'यह आपके द्वारा दी गई जानकारी पर आधारित सुझाव है। आपके रिलेशनशिप मैनेजर स्कीम और अंतिम प्लान पर आपसे बात करेंगे।',P,y+2,IW,fnt(19),'#7a6a3a',26);
    y=txt(c,'Mutual Fund investments are subject to market risks, read all scheme related documents carefully.',P,y+10,IW,fnt(19,700),'#0f1f3d',25);
    y=txt(c,'म्यूचुअल फंड निवेश बाज़ार जोखिमों के अधीन हैं, योजना से जुड़े सभी दस्तावेज़ ध्यान से पढ़ें।',P,y+2,IW,fnt(19,600),'#7a6a3a',26)+22;
    // footer
    c.fillStyle=g; c.fillRect(0,y,W,96); c.fillStyle='#c9942a'; c.fillRect(0,y,W,5);
    txt(c,rmName()?`Your RM: ${rmName()}`:'D N Investment',P,y+22,IW/2,fnt(26,800),'#f5d98a',32);
    txt(c,'D N Investment · Bistupur, Jamshedpur',P,y+56,IW,fnt(19),'#c8d3e6',24);
    y+=96;
    const out=document.createElement('canvas'); out.width=W; out.height=Math.min(y,cv.height);
    out.getContext('2d').drawImage(cv,0,0);
    return out;
  }
  const fileName = p => 'Investment_Plan_'+String(p.name||'Client').trim().replace(/[^a-z0-9]+/gi,'_')+'.png';
  const toBlob = cv => new Promise(res=>cv.toBlob(res,'image/png'));
  function dl(blob,name){ const a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download=name; document.body.appendChild(a); a.click(); setTimeout(()=>{ URL.revokeObjectURL(a.href); a.remove(); },1500); }

  async function downloadImage(i){
    const r=rowOf(i); if(!r) return;
    try{ const b=await toBlob(await makeCanvas(i)); dl(b,fileName(r.p)); toast&&toast('Image downloaded ✓','success'); }
    catch(e){ console.error(e); toast&&toast('⚠️ Could not create image — '+(e.message||e),'error'); }
  }
  async function sendImage(i){
    const r=rowOf(i); if(!r) return;
    let blob; try{ blob=await toBlob(await makeCanvas(i)); }catch(e){ console.error(e); toast&&toast('⚠️ Could not create image — '+(e.message||e),'error'); return; }
    const name=fileName(r.p), cap=caption(i);
    // 1) Mobile (Android / iPhone): seedha share sheet → WhatsApp → contact chunein
    try{
      const file=new File([blob],name,{type:'image/png'});
      if(navigator.canShare && navigator.canShare({files:[file]}) && /Android|iPhone|iPad/i.test(navigator.userAgent)){
        await navigator.share({files:[file], text:cap}); return;
      }
    }catch(e){ if(e && e.name==='AbortError') return; console.warn('share failed',e); }
    // 2) Computer: image clipboard me copy + download, aur client ka WhatsApp chat khule → Ctrl+V
    let copied=false;
    try{ if(window.ClipboardItem && navigator.clipboard && navigator.clipboard.write){ await navigator.clipboard.write([new ClipboardItem({'image/png':blob})]); copied=true; } }catch(e){ console.warn('clipboard image failed',e); }
    dl(blob,name);
    window.open(waUrl(r.p, cap),'_blank');
    toast&&toast(copied ? '🖼️ Image copied — press Ctrl+V in the WhatsApp chat, then send' : '🖼️ Image downloaded — attach it in WhatsApp with 📎', 'success');
  }

  // ══════════════ PRINT / PDF ══════════════
  async function print(i){
    const r=rowOf(i); if(!r) return;
    const w=window.open('','_blank'); if(!w){ toast&&toast('Popup blocked — allow popups','error'); return; }
    w.document.write('<p style="font-family:sans-serif;padding:20px">Preparing plan…</p>');
    let url; try{ url=(await makeCanvas(i)).toDataURL('image/png'); }catch(e){ w.document.body.innerHTML='⚠️ '+esc(e.message||e); return; }
    w.document.open();
    w.document.write(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>Investment Plan – ${esc(r.p.name)}</title>
<style>body{margin:0;background:#eee;text-align:center} img{width:100%;max-width:800px;display:block;margin:0 auto;background:#fff} button{margin:14px;padding:10px 22px;border:none;border-radius:8px;background:#c9942a;color:#fff;font-weight:700;cursor:pointer}
@page{size:A4;margin:8mm} @media print{body{background:#fff} button{display:none} img{max-width:100%}}</style></head>
<body><button onclick="print()">🖨️ Print / Save as PDF</button><img src="${url}"></body></html>`);
    w.document.close();
  }

  return { analyse, final, html, setAmt, setCombo, text, wa, copy, print, sendImage, downloadImage, edit, edCat, edSum, edAdd, edDel, edCancel, edSave, edReset };
})();
