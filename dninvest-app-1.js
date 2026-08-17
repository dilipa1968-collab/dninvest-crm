
// ══════════════════════════════════════════
// GLOBAL AUTO-UPPERCASE
// Whatever anyone types into any text field or textarea across the whole CRM
// is automatically converted to CAPITAL LETTERS as they type — so it's saved
// in uppercase everywhere too, since the underlying value itself is uppercase.
// Excluded: password/email/url/number/date/time/color/file fields (uppercasing
// those would break login, email validity, numeric parsing, etc.), and any
// field explicitly marked data-no-uppercase="1" for a deliberate exception.
// ══════════════════════════════════════════
(function(){
  var EXCLUDED_TYPES = ['password','email','url','number','date','datetime-local','month','week','time','color','file','range','hidden','checkbox','radio'];
  document.addEventListener('input', function(e){
    var el = e.target;
    if(!el || (el.tagName!=='INPUT' && el.tagName!=='TEXTAREA')) return;
    if(el.tagName==='INPUT' && EXCLUDED_TYPES.indexOf((el.type||'text').toLowerCase())!==-1) return;
    if(el.dataset && el.dataset.noUppercase==='1') return;
    var start = el.selectionStart, end = el.selectionEnd;
    var upper = el.value.toUpperCase();
    if(upper === el.value) return;   // nothing to change — avoid disturbing cursor unnecessarily
    el.value = upper;
    try{ el.setSelectionRange(start, end); }catch(err){}
  }, true);
})();

// ══════════════════════════════════════════
// DATA STORE (localStorage + memory)
// ══════════════════════════════════════════
// Previously we used per-document Firestore collections (eq_clients_v2,
// mf_clients_v2) for eq/mf clients to avoid huge array rewrites. However,
// these new collections may not be covered by existing Firestore security
// rules, causing silent write/read failures (records "disappear" after
// refresh). We've reverted to the proven-working single-array-document
// storage in 'crm_data', but use a merge-on-write strategy: before writing
// a single record's change, re-fetch the latest array from Firestore and
// merge the change in, minimizing data loss from concurrent edits.
const PER_DOC_COLLECTIONS = {}; // no longer used, kept for compatibility

// ══════════════════════════════════════════
// SHARDING (Firestore 1 MiB per-document limit)
// ══════════════════════════════════════════
// A single Firestore document can never exceed 1,048,576 bytes. 'eq_clients'
// crossed that limit (~3400 clients in one array), so ALL writes started
// failing with "exceeds the maximum allowed size".
//
// Fix: split the array across N sibling documents in the SAME 'crm_data'
// collection (so existing security rules keep working):
//     crm_data/eq_clients__s0 ... crm_data/eq_clients__s7
//
// Each record is assigned to a shard by a stable hash of its `id`, so a
// record ALWAYS lives in the same shard. Benefits over index-slicing:
//   • editing one client rewrites only ~1/8th of the data (faster + cheaper)
//   • no overflow risk as the client count grows
//   • no re-shuffling when records are added/removed
//
// Everything below is contained inside the DB layer — the rest of the app
// keeps calling DB.get()/DB.setClient()/DB.setClientsBulk() unchanged.
//
// To shard another key later (e.g. mf_clients when it grows), just add it
// here. NOTE: changing a shard COUNT requires a full re-migration — bump
// the number and delete the old crm_data/<key>__sN docs first.
const SHARD_CFG = {
  eq_clients: 8,   // ~3400 clients → ~425/shard → ~130 KB/shard (lots of headroom)
};

const DB = {
  // ── shard helpers ──────────────────────────────────────────
  _shardCache: {},   // { key: [shard0Array, shard1Array, ...] }
  _shardSeen: {},    // { key: Set(shardIndex) } — realtime readiness guard
  _isSharded(key){ return !!SHARD_CFG[key]; },
  _shardName(key,i){ return key+'__s'+i; },
  _shardRef(key,i){ return fdb.collection('crm_data').doc(this._shardName(key,i)); },
  // Stable string hash → shard index. Same id always maps to the same shard.
  _shardOf(key,id){
    const n = SHARD_CFG[key];
    const s = String(id==null?'':id);
    let h = 0;
    for(let i=0;i<s.length;i++){ h = (Math.imul(h,31) + s.charCodeAt(i)) | 0; }
    return Math.abs(h) % n;
  },
  // Split a full array into per-shard arrays
  _splitShards(key,arr){
    const n = SHARD_CFG[key];
    const parts = Array.from({length:n},()=>[]);
    (arr||[]).forEach(r=>{ if(r) parts[this._shardOf(key,r.id)].push(r); });
    return parts;
  },
  // Flatten the in-memory shard cache back into one array
  _mergeShards(key){
    const parts = this._shardCache[key]||[];
    return [].concat(...parts.map(p=>p||[]));
  },
  // Read every shard doc in parallel. Returns array of arrays, or null per
  // shard where the doc doesn't exist yet.
  async _readShards(key){
    const n = SHARD_CFG[key];
    const snaps = await Promise.all(
      Array.from({length:n},(_,i)=>this._shardRef(key,i).get())
    );
    return snaps.map(s=>(s.exists && s.data() && Array.isArray(s.data().data)) ? s.data().data : null);
  },
  // Overwrite ALL shards from a full array (atomic batch — all or nothing)
  async _writeAllShards(key,arr){
    const parts = this._splitShards(key,arr);
    const batch = fdb.batch();
    parts.forEach((p,i)=>{
      batch.set(this._shardRef(key,i), {data:p, updated:new Date().toISOString(), shard:i, count:p.length});
    });
    await batch.commit();
    this._shardCache[key] = parts;
    return parts;
  },
  // One-time migration: if no shard doc exists yet but the legacy oversized
  // document does, copy it into shards. Deterministic + idempotent, so it's
  // safe even if two browsers do it at the same instant. The legacy doc is
  // intentionally LEFT IN PLACE as a backup (it is simply never read again).
  async _ensureMigrated(key){
    let parts = await this._readShards(key);
    if(parts.some(p=>p!==null)) return parts.map(p=>p||[]);   // already sharded
    let legacy = [];
    try{
      const d = await fdb.collection('crm_data').doc(key).get();
      if(d.exists && d.data() && Array.isArray(d.data().data)) legacy = d.data().data;
    }catch(e){ console.log('legacy read failed for',key,e); }
    if(!legacy.length) return parts.map(()=>[]);
    parts = await this._writeAllShards(key, legacy);
    console.log('✅ MIGRATED',key,'→',legacy.length,'records across',SHARD_CFG[key],'shards',
                parts.map(p=>p.length));
    try{ toast('Storage upgraded: '+legacy.length+' records split into '+SHARD_CFG[key]+' shards','success'); }catch(e){}
    return parts;
  },
  get(key){
    // In-memory cache — avoid JSON.parse on every call for large datasets.
    // A write (set/setClient/setClientsBulk) must clear the cache for that key.
    if(this._mem && this._mem[key] !== undefined) return this._mem[key];
    try{
      const v=localStorage.getItem('dninvest_'+key);
      const parsed = v ? JSON.parse(v) : null;
      if(!this._mem) this._mem = {};
      this._mem[key] = parsed;
      return parsed;
    }catch(e){return null;}
  },
  // Generic set for small/array data (users, call_logs, mf_business)
  set(key,val){
    if(this._mem) this._mem[key] = val;   // update cache immediately with new value
    try{
      localStorage.setItem('dninvest_'+key,JSON.stringify(val));
    }catch(e){}
    // Sharded keys: a whole-array replace must rewrite every shard
    if(this._isSharded(key) && typeof fdb!=='undefined'){
      this._writing[key] = (this._writing[key]||0) + 1;
      return this._writeAllShards(key, val||[])
        .then(()=>console.log('Firebase synced (sharded):',key,(val||[]).length))
        .catch(e=>{ console.log('Firebase error:',e); toast('Sync error: '+e.message,'error'); })
        .finally(()=>{ this._writing[key] = Math.max(0,(this._writing[key]||1)-1); });
    }
    try{
      if(typeof fdb!=='undefined'){
        fdb.collection('crm_data').doc(key).set({data:val,updated:new Date().toISOString()})
          .then(()=>console.log('Firebase synced:',key))
          .catch(e=>{ console.log('Firebase error:',e); toast('Sync error: '+e.message,'error'); });
      }
    }catch(e){}
  },
  // Set just the local copy without writing to Firebase
  setLocal(key,val){
    if(this._mem) this._mem[key] = val;   // keep cache in sync
    try{ localStorage.setItem('dninvest_'+key,JSON.stringify(val)); }catch(e){}
  },
  // ── mf_business: transactional append (avoids the classic race where two
  // RMs save around the same moment and the second write's blind full-array
  // overwrite silently wipes the first RM's brand-new entry — same class of
  // bug as addActivityLog below, applied to MF Transactions/Demat entries) ──
  _mfbWriting:0,
  async appendMfBizEntry(arrayKey, entry){   // arrayKey: 'entries' (MF txns) or 'eq_entries' (Demat opens)
    let biz = this.get('mf_business');
    let curEntries = Array.isArray(biz) ? biz.slice() : (biz?.entries||[]).slice();
    let curEq = Array.isArray(biz) ? [] : (biz?.eq_entries||[]).slice();
    if(arrayKey==='entries') curEntries.push(entry); else curEq.push(entry);
    this.setLocal('mf_business', {entries:curEntries, eq_entries:curEq});
    if(typeof fdb==='undefined') return {entries:curEntries, eq_entries:curEq};
    this._mfbWriting++;
    try{
      const docRef = fdb.collection('crm_data').doc('mf_business');
      let finalData=null;
      await fdb.runTransaction(async (tx)=>{
        const doc = await tx.get(docRef);
        const latest = (doc.exists && doc.data()) ? doc.data().data : null;
        let lEntries = Array.isArray(latest) ? latest.slice() : (latest?.entries||[]).slice();
        let lEq = Array.isArray(latest) ? [] : (latest?.eq_entries||[]).slice();
        if(arrayKey==='entries'){ if(!lEntries.some(e=>e&&e.id===entry.id)) lEntries.push(entry); }
        else { if(!lEq.some(e=>e&&e.id===entry.id)) lEq.push(entry); }
        finalData = {entries:lEntries, eq_entries:lEq};
        tx.set(docRef, {data:finalData, updated:new Date().toISOString()});
      });
      if(finalData){ this.setLocal('mf_business', finalData); }
      return finalData;
    }catch(e){
      console.log('mf_business append error:',e);
      try{ toast('Sync error: '+e.message,'error'); }catch(e2){}
      return null;
    }finally{
      this._mfbWriting=Math.max(0,this._mfbWriting-1);
    }
  },
  // Transaction-safe update of ONE existing mf_business entry by id — same
  // idea as mutateSeminar/mutateUsers above. `mutate(freshEntry)` receives
  // the LATEST version of this entry straight from Firestore and mutates it
  // in place; return `false` to abort. Editing a business entry used to read
  // the whole entries array locally, edit it, and write the WHOLE array back
  // with a blind DB.set — exactly the same "someone else's concurrent
  // change gets clobbered" risk fixed elsewhere for seminars/users.
  async updateMfBizEntry(arrayKey, id, mutate){
    let biz = this.get('mf_business');
    let curEntries = Array.isArray(biz) ? biz.slice() : (biz?.entries||[]).slice();
    let curEq = Array.isArray(biz) ? [] : (biz?.eq_entries||[]).slice();
    const localArr = arrayKey==='entries' ? curEntries : curEq;
    const lIdx = localArr.findIndex(e=>e&&e.id===id);
    if(lIdx>=0){
      const clone = JSON.parse(JSON.stringify(localArr[lIdx]));
      if(mutate(clone)!==false){ localArr[lIdx]=clone; this.setLocal('mf_business', {entries:curEntries, eq_entries:curEq}); }
    }
    if(typeof fdb==='undefined') return {ok:false, error:'Offline — could not connect to Firebase'};
    this._mfbWriting++;
    let finalData=null, aborted=false;
    try{
      const docRef = fdb.collection('crm_data').doc('mf_business');
      await fdb.runTransaction(async (tx)=>{
        const doc = await tx.get(docRef);
        const latest = (doc.exists && doc.data()) ? doc.data().data : null;
        let lEntries = Array.isArray(latest) ? latest.slice() : (latest?.entries||[]).slice();
        let lEq = Array.isArray(latest) ? [] : (latest?.eq_entries||[]).slice();
        const arr = arrayKey==='entries' ? lEntries : lEq;
        const idx = arr.findIndex(e=>e&&e.id===id);
        if(idx<0){ aborted=true; return; }
        const fresh = {...arr[idx]};
        const res = mutate(fresh);
        if(res===false){ aborted=true; return; }
        arr[idx]=fresh;
        finalData = {entries:lEntries, eq_entries:lEq};
        tx.set(docRef, {data:finalData, updated:new Date().toISOString()});
      });
      if(finalData) this.setLocal('mf_business', finalData);
      return {ok:true, aborted};
    }catch(e){
      console.log('mf_business update error:',e);
      try{ toast('Sync error: '+e.message,'error'); }catch(e2){}
      return {ok:false, error:e.message};
    }finally{
      this._mfbWriting=Math.max(0,this._mfbWriting-1);
    }
  },
  // Append one or more entries to the shared append-only activity_logs array
  // WITHOUT clobbering other RMs' concurrent entries. Transaction merge-by-id,
  // keeps the newest 2000 by date.
  _alWriting:0,
  async addActivityLog(entryOrArray){
    const entries = Array.isArray(entryOrArray) ? entryOrArray : [entryOrArray];
    if(!entries.length) return;
    const capSort = arr => arr.slice().sort((a,b)=>String(b.date||'').localeCompare(String(a.date||''))).slice(0,2000);
    // 1) optimistic local
    let local=[];
    try{ local=JSON.parse(localStorage.getItem('dninvest_activity_logs')||'[]'); }catch(e){ local=[]; }
    const lById={}; local.forEach(x=>{ if(x&&x.id) lById[x.id]=x; });
    entries.forEach(e=>{ if(e&&e.id) lById[e.id]=e; });
    try{ localStorage.setItem('dninvest_activity_logs', JSON.stringify(capSort(Object.values(lById)))); }catch(e){}
    // 2) transactional merge-write
    if(typeof fdb==='undefined') return;
    this._alWriting++;
    try{
      const docRef = fdb.collection('crm_data').doc('activity_logs');
      let finalData = null;
      await fdb.runTransaction(async (tx)=>{
        const doc = await tx.get(docRef);
        let latest = (doc.exists && doc.data() && Array.isArray(doc.data().data)) ? doc.data().data : [];
        const byId={};
        latest.forEach(x=>{ if(x&&x.id) byId[x.id]=x; });
        entries.forEach(e=>{ if(e&&e.id) byId[e.id]=e; });
        finalData = capSort(Object.values(byId));
        tx.set(docRef, {data:finalData, updated:new Date().toISOString()});
      });
      if(finalData){ try{ localStorage.setItem('dninvest_activity_logs',JSON.stringify(finalData)); }catch(e){} }
    }catch(e){
      console.log('Activity log sync error:',e);
    }finally{
      this._alWriting=Math.max(0,this._alWriting-1);
    }
  },
  // ── Equity Active/Inactive daily snapshot (for the dashboard trend card) ──
  // Har din pehli baar dashboard khulne par aaj ki Active/Inactive count save
  // hoti hai. Merge-by-(date+scope) — same din dobara refresh hone par sirf
  // aaj ki entry update hoti hai, purani dates chhedi nahi jati (isliye kal
  // vs aaj ka diff sahi milta hai). scope = 'ALL' (admin, poora base) ya RM
  // ka apna id (RM apna hi trend dekhta hai) — dono independent rehte hain
  // taaki alag-alag users ek dusre ka data overwrite na karein.
  _easWriting:0,
  async addEqActivitySnapshot(entry){   // entry = {date, scope, active, inactive, total}
    const rowId = entry.date+'__'+entry.scope;
    let local=[];
    try{ local=JSON.parse(localStorage.getItem('dninvest_eq_activity_snapshots')||'[]'); }catch(e){ local=[]; }
    const byId={}; local.forEach(x=>{ if(x&&x.date&&x.scope) byId[x.date+'__'+x.scope]=x; });
    byId[rowId]=entry;
    const capSort = arr => arr.slice().sort((a,b)=>String(b.date||'').localeCompare(String(a.date||''))).slice(0,400);
    const merged = capSort(Object.values(byId));
    try{ localStorage.setItem('dninvest_eq_activity_snapshots', JSON.stringify(merged)); }catch(e){}
    if(typeof fdb==='undefined') return merged;
    this._easWriting++;
    try{
      const docRef = fdb.collection('crm_data').doc('eq_activity_snapshots');
      let finalData=null;
      await fdb.runTransaction(async (tx)=>{
        const doc = await tx.get(docRef);
        let latest = (doc.exists && doc.data() && Array.isArray(doc.data().data)) ? doc.data().data : [];
        const byId2={}; latest.forEach(x=>{ if(x&&x.date&&x.scope) byId2[x.date+'__'+x.scope]=x; });
        byId2[rowId]=entry;
        finalData = capSort(Object.values(byId2));
        tx.set(docRef, {data:finalData, updated:new Date().toISOString()});
      });
      if(finalData){ try{ localStorage.setItem('dninvest_eq_activity_snapshots',JSON.stringify(finalData)); }catch(e){} }
      return finalData;
    }catch(e){
      console.log('Eq activity snapshot sync error:',e);
      return merged;
    }finally{
      this._easWriting=Math.max(0,this._easWriting-1);
    }
  },
  // localStorage-only history (addEqActivitySnapshot) sirf UNHI snapshots se
  // bharta hai jo isi browser/device se save hue hon — agar RM kal kisi doosre
  // device se login kiya tha ya cache clear ho gaya, toh yahan "kal" ka data
  // dikhega hi nahi chahe Firestore me maujood ho. Ye ek-baar-per-session
  // Firestore se poora shared doc fetchch kar local cache ko top-up karta hai
  // taaki "vs yesterday" kisi bhi device pe sahi dikhe.
  async fetchEqActivitySnapshots(){
    if(typeof fdb==='undefined') return null;
    try{
      const doc = await fdb.collection('crm_data').doc('eq_activity_snapshots').get();
      if(!doc.exists || !doc.data() || !Array.isArray(doc.data().data)) return null;
      const remote = doc.data().data;
      let local=[];
      try{ local=JSON.parse(localStorage.getItem('dninvest_eq_activity_snapshots')||'[]'); }catch(e){ local=[]; }
      const byId={}; local.forEach(x=>{ if(x&&x.date&&x.scope) byId[x.date+'__'+x.scope]=x; });
      remote.forEach(x=>{ if(x&&x.date&&x.scope) byId[x.date+'__'+x.scope]=x; });
      const merged = Object.values(byId).sort((a,b)=>String(b.date||'').localeCompare(String(a.date||''))).slice(0,400);
      try{ localStorage.setItem('dninvest_eq_activity_snapshots', JSON.stringify(merged)); }catch(e){}
      return merged;
    }catch(e){ console.log('Eq activity snapshot fetch error:',e); return null; }
  },
  // ── MF Invested Amount daily snapshot (for the dashboard trend card + date-wise history) ──
  // Same pattern as addEqActivitySnapshot — merge-by-(date+scope), never touches past dates.
  _masWriting:0,
  async addMfAumSnapshot(entry){   // entry = {date, scope, additions, additionsAmt, redemptions, redemptionsAmt, totalInvested}
    const rowId = entry.date+'__'+entry.scope;
    let local=[];
    try{ local=JSON.parse(localStorage.getItem('dninvest_mf_aum_snapshots')||'[]'); }catch(e){ local=[]; }
    const byId={}; local.forEach(x=>{ if(x&&x.date&&x.scope) byId[x.date+'__'+x.scope]=x; });
    byId[rowId]=entry;
    const capSort = arr => arr.slice().sort((a,b)=>String(b.date||'').localeCompare(String(a.date||''))).slice(0,400);
    const merged = capSort(Object.values(byId));
    try{ localStorage.setItem('dninvest_mf_aum_snapshots', JSON.stringify(merged)); }catch(e){}
    if(typeof fdb==='undefined') return merged;
    this._masWriting++;
    try{
      const docRef = fdb.collection('crm_data').doc('mf_aum_snapshots');
      let finalData=null;
      await fdb.runTransaction(async (tx)=>{
        const doc = await tx.get(docRef);
        let latest = (doc.exists && doc.data() && Array.isArray(doc.data().data)) ? doc.data().data : [];
        const byId2={}; latest.forEach(x=>{ if(x&&x.date&&x.scope) byId2[x.date+'__'+x.scope]=x; });
        byId2[rowId]=entry;
        finalData = capSort(Object.values(byId2));
        tx.set(docRef, {data:finalData, updated:new Date().toISOString()});
      });
      if(finalData){ try{ localStorage.setItem('dninvest_mf_aum_snapshots',JSON.stringify(finalData)); }catch(e){} }
      return finalData;
    }catch(e){
      console.log('MF AUM snapshot sync error:',e);
      return merged;
    }finally{
      this._masWriting=Math.max(0,this._masWriting-1);
    }
  },
  async fetchMfAumSnapshots(){
    if(typeof fdb==='undefined') return null;
    try{
      const doc = await fdb.collection('crm_data').doc('mf_aum_snapshots').get();
      if(!doc.exists || !doc.data() || !Array.isArray(doc.data().data)) return null;
      const remote = doc.data().data;
      let local=[];
      try{ local=JSON.parse(localStorage.getItem('dninvest_mf_aum_snapshots')||'[]'); }catch(e){ local=[]; }
      const byId={}; local.forEach(x=>{ if(x&&x.date&&x.scope) byId[x.date+'__'+x.scope]=x; });
      remote.forEach(x=>{ if(x&&x.date&&x.scope) byId[x.date+'__'+x.scope]=x; });
      const merged = Object.values(byId).sort((a,b)=>String(b.date||'').localeCompare(String(a.date||''))).slice(0,400);
      try{ localStorage.setItem('dninvest_mf_aum_snapshots', JSON.stringify(merged)); }catch(e){}
      return merged;
    }catch(e){ console.log('MF AUM snapshot fetch error:',e); return null; }
  },
  // ── MF Invested Amount change log — append-only, one entry per client per
  // date whose Invested Amount changed during an AUM By Client import. Unlike
  // the live invested_change_amt field on the client (which a later import can
  // overwrite), this log keeps every past day's changes so "date-wise history"
  // can show the exact named client list for any past date — no guessing
  // needed (id = clientId+date, so re-importing the same day just updates it).
  _mclWriting:0,
  async addMfChangeLog(entry){   // entry = {id, date, clientId, name, rm, prevInvested, newInvested, delta}
    return this.addMfChangeLogBatch([entry]);
  },
  // Same as addMfChangeLog but for many entries in ONE Firestore transaction —
  // used by the AUM import, which can touch hundreds of clients' Invested
  // Amount in a single upload. Calling addMfChangeLog once per client (fire-
  // and-forget, unawaited) meant dozens/hundreds of transactions racing to
  // read-modify-write the SAME 'mf_change_log' document at once; under that
  // much contention some would exhaust their retries and fail silently (only
  // a console.log, no visible error) — additions/redemptions for some clients
  // would just never make it into the log, exactly like nothing happened.
  async addMfChangeLogBatch(entries){
    entries = (entries||[]).filter(Boolean);
    if(!entries.length) return;
    let local=[];
    try{ local=JSON.parse(localStorage.getItem('dninvest_mf_change_log')||'[]'); }catch(e){ local=[]; }
    const byId={}; local.forEach(x=>{ if(x&&x.id) byId[x.id]=x; });
    entries.forEach(entry=>{ byId[entry.id]=entry; });
    try{ localStorage.setItem('dninvest_mf_change_log', JSON.stringify(Object.values(byId))); }catch(e){}
    if(typeof fdb==='undefined') return;
    this._mclWriting++;
    try{
      const docRef = fdb.collection('crm_data').doc('mf_change_log');
      let finalData=null;
      await fdb.runTransaction(async (tx)=>{
        const doc = await tx.get(docRef);
        let latest = (doc.exists && doc.data() && Array.isArray(doc.data().data)) ? doc.data().data : [];
        const byId2={}; latest.forEach(x=>{ if(x&&x.id) byId2[x.id]=x; });
        entries.forEach(entry=>{ byId2[entry.id]=entry; });
        finalData = DB._pruneCallLogs(Object.values(byId2), 850000); // same date-priority pruning as call_logs
        tx.set(docRef, {data:finalData, updated:new Date().toISOString()});
      });
      if(finalData){ try{ localStorage.setItem('dninvest_mf_change_log',JSON.stringify(finalData)); }catch(e){} }
    }catch(e){
      console.log('MF change log batch sync error:',e);
      try{ toast('⚠️ Some Invested Amount changes may not have saved — please re-import if the Additions/Redemptions list looks incomplete','error'); }catch(e2){}
    }finally{
      this._mclWriting=Math.max(0,this._mclWriting-1);
    }
  },
  async fetchMfChangeLog(){
    if(typeof fdb==='undefined') return null;
    try{
      const doc = await fdb.collection('crm_data').doc('mf_change_log').get();
      if(!doc.exists || !doc.data() || !Array.isArray(doc.data().data)) return null;
      const remote = doc.data().data;
      let local=[];
      try{ local=JSON.parse(localStorage.getItem('dninvest_mf_change_log')||'[]'); }catch(e){ local=[]; }
      const byId={}; local.forEach(x=>{ if(x&&x.id) byId[x.id]=x; });
      remote.forEach(x=>{ if(x&&x.id) byId[x.id]=x; });
      const merged = Object.values(byId);
      try{ localStorage.setItem('dninvest_mf_change_log', JSON.stringify(merged)); }catch(e){}
      return merged;
    }catch(e){ console.log('MF change log fetch error:',e); return null; }
  },
  // Remove specific entries from the change log by id — used for a precise,
  // targeted cleanup (e.g. removing only entries flagged `estimated:true`),
  // unlike editing live client fields which can't distinguish good from bad.
  async removeMfChangeLogEntries(ids){
    const idSet = new Set(ids);
    let local=[];
    try{ local=JSON.parse(localStorage.getItem('dninvest_mf_change_log')||'[]'); }catch(e){ local=[]; }
    local = local.filter(x=>!(x&&idSet.has(x.id)));
    try{ localStorage.setItem('dninvest_mf_change_log', JSON.stringify(local)); }catch(e){}
    if(typeof fdb==='undefined') return local;
    try{
      const docRef = fdb.collection('crm_data').doc('mf_change_log');
      let finalData=null;
      await fdb.runTransaction(async (tx)=>{
        const doc = await tx.get(docRef);
        let latest = (doc.exists && doc.data() && Array.isArray(doc.data().data)) ? doc.data().data : [];
        finalData = latest.filter(x=>!(x&&idSet.has(x.id)));
        tx.set(docRef, {data:finalData, updated:new Date().toISOString()});
      });
      if(finalData){ try{ localStorage.setItem('dninvest_mf_change_log',JSON.stringify(finalData)); }catch(e){} }
      return finalData;
    }catch(e){
      console.log('MF change log remove error:',e);
      return local;
    }
  },
  // Keep the shared call_logs document safely under Firestore's 1 MiB limit.
  // Newest entries are kept; oldest are dropped once the JSON size crosses the cap.
  _pruneCallLogs(arr, maxBytes){
    const sorted = (arr||[]).slice().sort((a,b)=>
      String((b&&(b.ts||b.date))||'').localeCompare(String((a&&(a.ts||a.date))||'')));
    const kept=[]; let size=2;                       // "[]" baseline
    for(const x of sorted){
      const s=JSON.stringify(x).length+1;            // +1 for comma
      if(size+s>maxBytes) break;
      kept.push(x); size+=s;
    }
    return kept;
  },
  async addCallLog(entry){
    // 1) optimistic local append
    let local=[];
    try{ local=JSON.parse(localStorage.getItem('dninvest_call_logs')||'[]'); }catch(e){ local=[]; }
    if(!local.some(x=>x.id===entry.id)) local.push(entry);
    try{ localStorage.setItem('dninvest_call_logs',JSON.stringify(local)); }catch(e){}
    // 2) transactional merge-write to Firestore (no clobber, retries on conflict)
    if(typeof fdb==='undefined') return;
    this._clWriting++;
    try{
      const docRef = fdb.collection('crm_data').doc('call_logs');
      let finalData = null;
      await fdb.runTransaction(async (tx)=>{
        const doc = await tx.get(docRef);
        let latest = (doc.exists && doc.data() && Array.isArray(doc.data().data)) ? doc.data().data : [];
        const byId={};
        latest.forEach(x=>{ if(x&&x.id) byId[x.id]=x; });
        if(entry&&entry.id) byId[entry.id]=entry;   // add/replace this entry
        finalData = Object.values(byId);
        // Trim oldest logs so the document stays under Firestore's 1 MiB cap
        finalData = DB._pruneCallLogs(finalData, 850000);
        tx.set(docRef, {data:finalData, updated:new Date().toISOString()});
      });
      if(finalData){
        try{ localStorage.setItem('dninvest_call_logs',JSON.stringify(finalData)); }catch(e){}
      }
      console.log('Call log transaction-synced');
    }catch(e){
      console.log('Call log sync error:',e);
      toast('Call sync error: '+(e&&e.message?e.message:e),'error');
    }finally{
      this._clWriting=Math.max(0,this._clWriting-1);
    }
  },
  // Save/update a single client record (eq_clients or mf_clients).
  // Merge-on-write: re-fetch latest array from Firestore, merge this one
  // record in, then write the merged array back. This avoids clobbering
  // other users' concurrent changes while keeping the proven array storage.
  // Tracks keys with an in-flight Firestore write, so the real-time listener
  // can avoid overwriting localStorage with a stale snapshot mid-write.
  _writing: {},
  // Sharded variant: touches ONLY the one shard this record hashes to.
  async _setClientSharded(key, rec){
    const si  = this._shardOf(key, rec.id);
    const ref = this._shardRef(key, si);
    this._writing[key] = (this._writing[key]||0) + 1;
    try{
      await fdb.runTransaction(async (tx)=>{
        const doc = await tx.get(ref);
        let latest = (doc.exists && doc.data() && Array.isArray(doc.data().data))
                   ? doc.data().data
                   : ((this._shardCache[key]||[])[si] || []);
        const lidx = latest.findIndex(c=>c.id===rec.id);
        if(lidx>=0) latest[lidx]=rec; else latest.push(rec);
        tx.set(ref, {data:latest, updated:new Date().toISOString(), shard:si, count:latest.length});
        if(this._shardCache[key]) this._shardCache[key][si] = latest;
      });
      console.log('Firebase shard-synced:',key,'s'+si,rec.id);
    }catch(e){ console.log('setClient(shard) error:',e); toast('Sync error: '+e.message,'error'); }
    finally{ this._writing[key] = Math.max(0,(this._writing[key]||1)-1); }
  },
  async setClient(key, rec){
    // update local copy immediately for responsiveness
    let arr = this.get(key)||[];
    let idx = arr.findIndex(c=>c.id===rec.id);
    if(idx>=0) arr[idx]=rec; else arr.push(rec);
    this.setLocal(key, arr);

    if(this._isSharded(key) && typeof fdb!=='undefined'){
      return this._setClientSharded(key, rec);
    }

    if(typeof fdb==='undefined'){
      // Firebase SDK never connected this session — the record only exists in
      // THIS browser's local cache. Must be flagged, else the caller shows a
      // false "saved" message and the record silently vanishes on next sync.
      return {ok:false, error:'Offline — could not connect to Firebase'};
    }

    this._writing[key] = (this._writing[key]||0) + 1;
    try{
      const docRef = fdb.collection('crm_data').doc(key);
      let finalData = null;
      await fdb.runTransaction(async (tx)=>{
        const doc = await tx.get(docRef);
        let latest = (doc.exists && doc.data() && doc.data().data) ? doc.data().data : arr;
        const lidx = latest.findIndex(c=>c.id===rec.id);
        if(lidx>=0) latest[lidx]=rec; else latest.push(rec);
        tx.set(docRef, {data:latest, updated:new Date().toISOString()});
        finalData = latest;
      });
      // adopt the merged result locally too, in case other records changed
      if(finalData) this.setLocal(key, finalData);
      console.log('Firebase transaction-synced:',key,rec.id);
      return {ok:true};
    }catch(e){
      console.log('setClient error:',e);
      toast('Sync error: '+e.message,'error');
      return {ok:false, error:e.message};
    }
    finally{ this._writing[key] = Math.max(0,(this._writing[key]||1)-1); }
  },
  // Transaction-safe seminar mutation, used for every attendee add/edit/
  // remove and seminar-metadata edit. `mutate(freshSeminar)` receives the
  // LATEST seminar record straight from Firestore at write time — not the
  // caller's local copy, which may be stale by the time the write lands —
  // and mutates it in place. This is what fixes attendees silently
  // "disappearing": two RMs adding different attendees to the same seminar
  // around the same time used to each blindly overwrite the WHOLE seminar
  // record with their own stale local snapshot, so whichever save landed
  // second wiped out the other RM's new attendee. Now every save is applied
  // on top of whatever is actually on the server at that moment, so
  // concurrent adds/edits from different RMs never clobber each other.
  // Return `false` from mutate() to abort the write (e.g. "already added").
  async mutateSeminar(seminarId, mutate){
    // Keep the local UI responsive with an optimistic local-only update too.
    let localArr = this.get('seminars')||[];
    const lIdx = localArr.findIndex(x=>x.id===seminarId);
    if(lIdx>=0){
      const clone = JSON.parse(JSON.stringify(localArr[lIdx]));
      if(mutate(clone)!==false){ localArr=localArr.slice(); localArr[lIdx]=clone; this.setLocal('seminars', localArr); }
    }

    if(typeof fdb==='undefined') return {ok:false, error:'Offline — could not connect to Firebase'};
    const docRef = fdb.collection('crm_data').doc('seminars');
    let finalData=null, aborted=false;
    this._writing['seminars'] = (this._writing['seminars']||0) + 1;
    try{
      await fdb.runTransaction(async (tx)=>{
        const doc = await tx.get(docRef);
        let latest = (doc.exists && doc.data() && doc.data().data) ? doc.data().data : (this.get('seminars')||[]);
        const idx = latest.findIndex(x=>x.id===seminarId);
        if(idx<0){ aborted=true; return; }
        const sem = {...latest[idx], attendees:(latest[idx].attendees||[]).map(a=>({...a}))};
        const res = mutate(sem);
        if(res===false){ aborted=true; return; }
        latest = latest.slice();
        latest[idx] = sem;
        tx.set(docRef, {data:latest, updated:new Date().toISOString()});
        finalData = latest;
      });
      if(finalData) this.setLocal('seminars', finalData);
      return {ok:true, aborted};
    }catch(e){
      console.log('mutateSeminar error:', e);
      toast('Sync error: '+e.message,'error');
      return {ok:false, error:e.message};
    } finally{ this._writing['seminars'] = Math.max(0,(this._writing['seminars']||1)-1); }
  },
  // Transaction-safe bulk mutation of the whole `users` array (same idea as
  // mutateSeminar above). `mutate(freshUsers)` receives the LATEST users
  // array straight from Firestore and mutates it in place; return `false`
  // to skip the write entirely (e.g. nothing actually changed).
  // This exists because granting/expiring Temporary Access used to blindly
  // overwrite the whole `users` doc with a possibly-stale local snapshot —
  // e.g. cleanExpiredTempAccess() ran on page load using whatever was in
  // this browser's local cache at that moment. If admin had *just* granted
  // someone temp access a moment earlier and that grant hadn't finished
  // syncing to this particular tab yet, the cleanup would write back the
  // OLDER copy (without the new grant) and silently erase it — exactly the
  // "refresh karte hi temp access hat jaata hai" bug.
  async mutateUsers(mutate){
    let localArr = this.get('users')||[];
    const clone = JSON.parse(JSON.stringify(localArr));
    if(mutate(clone)!==false) this.setLocal('users', clone);

    if(typeof fdb==='undefined') return {ok:false, error:'Offline — could not connect to Firebase'};
    const docRef = fdb.collection('crm_data').doc('users');
    let finalData=null, aborted=false;
    this._writing['users'] = (this._writing['users']||0) + 1;
    try{
      await fdb.runTransaction(async (tx)=>{
        const doc = await tx.get(docRef);
        let latest = (doc.exists && doc.data() && doc.data().data) ? doc.data().data : (this.get('users')||[]);
        latest = JSON.parse(JSON.stringify(latest));
        const res = mutate(latest);
        if(res===false){ aborted=true; return; }
        tx.set(docRef, {data:latest, updated:new Date().toISOString()});
        finalData = latest;
      });
      if(finalData) this.setLocal('users', finalData);
      return {ok:true, aborted};
    }catch(e){
      console.log('mutateUsers error:', e);
      toast('Sync error: '+e.message,'error');
      return {ok:false, error:e.message};
    } finally{ this._writing['users'] = Math.max(0,(this._writing['users']||1)-1); }
  },
  // Delete a single client record (merge-on-write delete)
  async deleteClient(key, id){
    let arr = (this.get(key)||[]).filter(c=>c.id!==id);
    this.setLocal(key, arr);

    if(this._isSharded(key) && typeof fdb!=='undefined'){
      const si  = this._shardOf(key, id);
      const ref = this._shardRef(key, si);
      this._writing[key] = (this._writing[key]||0) + 1;
      try{
        await fdb.runTransaction(async (tx)=>{
          const doc = await tx.get(ref);
          let latest = (doc.exists && doc.data() && Array.isArray(doc.data().data))
                     ? doc.data().data
                     : ((this._shardCache[key]||[])[si] || []);
          latest = latest.filter(c=>c.id!==id);
          tx.set(ref, {data:latest, updated:new Date().toISOString(), shard:si, count:latest.length});
          if(this._shardCache[key]) this._shardCache[key][si] = latest;
        });
      }catch(e){ console.log('deleteClient(shard) error:',e); toast('Sync error: '+e.message,'error'); }
      finally{ this._writing[key] = Math.max(0,(this._writing[key]||1)-1); }
      return;
    }

    this._writing[key] = (this._writing[key]||0) + 1;
    try{
      if(typeof fdb!=='undefined'){
        const docRef = fdb.collection('crm_data').doc(key);
        let finalData = null;
        await fdb.runTransaction(async (tx)=>{
          const doc = await tx.get(docRef);
          let latest = (doc.exists && doc.data() && doc.data().data) ? doc.data().data : arr;
          latest = latest.filter(c=>c.id!==id);
          tx.set(docRef, {data:latest, updated:new Date().toISOString()});
          finalData = latest;
        });
        if(finalData) this.setLocal(key, finalData);
      }
    }catch(e){ console.log('deleteClient error:',e); toast('Sync error: '+e.message,'error'); }
    finally{ this._writing[key] = Math.max(0,(this._writing[key]||1)-1); }
  },
  // Sharded bulk merge: groups records by shard, reads only the affected
  // shards, then writes them back in ONE transaction (all reads before all
  // writes, as Firestore requires).
  async _setBulkSharded(key, records){
    const groups = {};
    records.forEach(r=>{ const i=this._shardOf(key,r.id); (groups[i]=groups[i]||[]).push(r); });
    const idxs = Object.keys(groups).map(Number);
    this._writing[key] = (this._writing[key]||0) + 1;
    try{
      await fdb.runTransaction(async (tx)=>{
        const refs = idxs.map(i=>this._shardRef(key,i));
        const docs = [];
        for(const ref of refs) docs.push(await tx.get(ref));   // ALL reads first
        idxs.forEach((i,k)=>{
          const d = docs[k];
          let latest = (d.exists && d.data() && Array.isArray(d.data().data)) ? d.data().data : [];
          const m = {}; latest.forEach(c=>{ if(c) m[c.id]=c; });
          groups[i].forEach(r=>m[r.id]=r);
          const out = Object.values(m);
          tx.set(refs[k], {data:out, updated:new Date().toISOString(), shard:i, count:out.length});
          if(this._shardCache[key]) this._shardCache[key][i] = out;
        });
      });
      console.log('Bulk shard-synced:',key,records.length,'records across shards',idxs);
    }catch(e){ console.log('Bulk sync(shard) error:',e); toast('Sync error: '+(e.message||e),'error'); }
    finally{ this._writing[key] = Math.max(0,(this._writing[key]||1)-1); }
  },
  // Bulk delete by ids — shard-aware, mirrors _setBulkSharded
  async deleteClientsBulk(key, ids){
    const idSet = new Set(ids);
    const arr = (this.get(key)||[]).filter(c=>!idSet.has(c.id));
    this.setLocal(key, arr);
    if(typeof fdb==='undefined') return;

    if(this._isSharded(key)){
      const groups = {};
      ids.forEach(id=>{ const i=this._shardOf(key,id); (groups[i]=groups[i]||new Set()).add(id); });
      const idxs = Object.keys(groups).map(Number);
      this._writing[key] = (this._writing[key]||0) + 1;
      try{
        await fdb.runTransaction(async (tx)=>{
          const refs = idxs.map(i=>this._shardRef(key,i));
          const docs = [];
          for(const ref of refs) docs.push(await tx.get(ref));   // ALL reads first
          idxs.forEach((i,k)=>{
            const d = docs[k];
            let latest = (d.exists && d.data() && Array.isArray(d.data().data)) ? d.data().data : [];
            const out = latest.filter(c=>!groups[i].has(c.id));
            tx.set(refs[k], {data:out, updated:new Date().toISOString(), shard:i, count:out.length});
            if(this._shardCache[key]) this._shardCache[key][i] = out;
          });
        });
      }catch(e){ toast('Delete sync error: '+(e.message||e),'error'); }
      finally{ this._writing[key] = Math.max(0,(this._writing[key]||1)-1); }
      return;
    }

    this._writing[key] = (this._writing[key]||0) + 1;
    try{
      const ref = fdb.collection('crm_data').doc(key);
      await fdb.runTransaction(async tx=>{
        const doc = await tx.get(ref);
        let latest = (doc.exists && doc.data() && doc.data().data) ? doc.data().data : arr;
        latest = latest.filter(c=>!idSet.has(c.id));
        tx.set(ref, {data:latest, updated:new Date().toISOString()});
        this.setLocal(key, latest);
      });
    }catch(e){ toast('Delete sync error: '+(e.message||e),'error'); }
    finally{ this._writing[key] = Math.max(0,(this._writing[key]||1)-1); }
  },
  // Bulk save (import) - merge multiple records into the latest array
  async setClientsBulk(key, records){
    let arr = this.get(key)||[];
    let map = {}; arr.forEach(c=>map[c.id]=c);
    records.forEach(r=>map[r.id]=r);
    let merged = Object.values(map);
    this.setLocal(key, merged);

    if(this._isSharded(key) && typeof fdb!=='undefined'){
      return this._setBulkSharded(key, records);
    }

    this._writing[key] = (this._writing[key]||0) + 1;
    try{
      if(typeof fdb!=='undefined'){
        const docRef = fdb.collection('crm_data').doc(key);
        let finalData = null;
        await fdb.runTransaction(async (tx)=>{
          const doc = await tx.get(docRef);
          let latest = (doc.exists && doc.data() && doc.data().data) ? doc.data().data : merged;
          let lmap = {}; latest.forEach(c=>lmap[c.id]=c);
          records.forEach(r=>lmap[r.id]=r);
          latest = Object.values(lmap);
          tx.set(docRef, {data:latest, updated:new Date().toISOString()});
          finalData = latest;
        });
        if(finalData) this.setLocal(key, finalData);
        console.log('Bulk transaction-synced:',key,records.length);
      }
    }catch(e){ console.log('Bulk sync error:',e); toast('Sync error: '+e.message,'error'); }
    finally{ this._writing[key] = Math.max(0,(this._writing[key]||1)-1); }
  },
  load(){
    if(!this.get('users')) this.set('users', DEFAULT_USERS);
    if(!this.get('call_logs')) this.set('call_logs', []);
    if(!this.get('mf_business')) this.set('mf_business', []);
  },
  async syncFromFirebase(){
    if(typeof fdb==='undefined'){ await window.waitForFdb(8000); }
    if(typeof fdb==='undefined') return;
    // All 17 collections fetched IN PARALLEL (not one-by-one) — this runs on every
    // login, page refresh, and the 30-min auto-reload, so a sequential for-loop here
    // directly adds up to slow login/page-open times (each collection = one network
    // round-trip; sequential = sum of all of them, parallel = the slowest single one).
    await Promise.all(['eq_clients','mf_clients','leads','seminars','users','call_logs','mf_business','announcement','activity_logs','rm_messages','meeting_agenda','meeting_agenda_archive','learned_fund_names','incentive_config','rm_sales_summary','comm_history','eq_risk'].map(async (key)=>{
      try{
        // ── sharded keys: read every shard, auto-migrate on first run ──
        if(this._isSharded(key)){
          const parts = await this._ensureMigrated(key);
          this._shardCache[key] = parts.map(p=>p||[]);
          this._shardSeen[key]  = new Set(parts.map((_,i)=>i));   // all shards known
          const merged = this._mergeShards(key);
          if(merged.length){
            localStorage.setItem('dninvest_'+key, JSON.stringify(merged));
            console.log('Loaded from Firebase (sharded):',key, merged.length,'records',
                        this._shardCache[key].map(p=>p.length));
          }
          return;
        }
        const doc=await fdb.collection('crm_data').doc(key).get();
        if(doc.exists && doc.data() && Object.prototype.hasOwnProperty.call(doc.data(),'data')){
          const d = doc.data().data;
          if(key==='eq_risk'){
            // eq_risk Firestore me COMPACT form me rehta hai: {codeJson:string, updated, count}.
            // Yahan wapas normal shape {code:{...}, updated, count} me rehydrate karte hain
            // taaki getEqRisk()/eqRiskFor() bina kisi change ke chalte rahein.
            // Backward-compatible: agar purana {code:{...}} format mila to seedha use.
            let codeObj={};
            if(d && typeof d.codeJson==='string'){ try{ codeObj=JSON.parse(d.codeJson)||{}; }catch(_){ codeObj={}; } }
            else if(d && d.code && typeof d.code==='object'){ codeObj=d.code; }
            const norm={ code:codeObj, updated:(d&&d.updated)||'', count:(d&&d.count)||Object.keys(codeObj).length };
            localStorage.setItem('dninvest_eq_risk', JSON.stringify(norm));
            console.log('Loaded from Firebase: eq_risk (compact)', norm.count);
            return;
          }
          if(Array.isArray(d)){
            if(key==='call_logs'){
              // append-only & shared: merge server + any local-only entries by id
              let existing=[];
              try{ existing=JSON.parse(localStorage.getItem('dninvest_call_logs')||'[]'); }catch(e){ existing=[]; }
              const byId={};
              existing.forEach(x=>{ if(x&&x.id) byId[x.id]=x; });
              d.forEach(x=>{ if(x&&x.id) byId[x.id]=x; });
              localStorage.setItem('dninvest_call_logs', JSON.stringify(Object.values(byId)));
              console.log('Loaded+merged from Firebase: call_logs', Object.values(byId).length);
            } else if(key==='activity_logs'){
              let existing=[];
              try{ existing=JSON.parse(localStorage.getItem('dninvest_activity_logs')||'[]'); }catch(e){ existing=[]; }
              const byId={};
              existing.forEach(x=>{ if(x&&x.id) byId[x.id]=x; });
              d.forEach(x=>{ if(x&&x.id) byId[x.id]=x; });
              const merged=Object.values(byId).sort((a,b)=>String(b.date||'').localeCompare(String(a.date||''))).slice(0,2000);
              localStorage.setItem('dninvest_activity_logs', JSON.stringify(merged));
              console.log('Loaded+merged from Firebase: activity_logs', merged.length);
            } else if(d.length>0){
              localStorage.setItem('dninvest_'+key,JSON.stringify(d));
              console.log('Loaded from Firebase:',key, d.length,'records');
            }
          } else {
            localStorage.setItem('dninvest_'+key,JSON.stringify(d));
            console.log('Loaded from Firebase:',key, 'object');
          }
        }
      }catch(e){ console.log('Firebase sync error for',key,':',e); }
    }));
    try{ if(typeof clearEqRiskCache==='function') clearEqRiskCache(); }catch(e){}
  }
};

// ══════════════════════════════════════════
// DEFAULT USERS
// ══════════════════════════════════════════
const DEFAULT_USERS = [
  {id:'u1',username:'admin',password:'Admin@123',name:'Admin',role:'admin',segments:['equity','mf'],eq_dealers:[],mf_dealers:[],active:true},
  {id:'u2',username:'dilip',password:'Dilip@123',name:'Dilip Sir',role:'admin',segments:['equity','mf'],eq_dealers:[],mf_dealers:[],active:true},
  {id:'u3',username:'puja',password:'Puja@123',name:'Puja',role:'staff',segments:['equity','mf'],eq_dealers:['PUJA','PUJA '],mf_dealers:['PUJA MADAM','PUJA ','PUJA','PUJA/MEGHA'],active:true},
  {id:'u4',username:'rohit',password:'Rohit@123',name:'Rohit',role:'staff',segments:['equity','mf'],eq_dealers:['ROHIT','ROHIT '],mf_dealers:['ROHIT BHAIYA','ROHIT ','ROHIT'],active:true},
  {id:'u5',username:'raju',password:'Raju@123',name:'Raju',role:'staff',segments:['equity','mf'],eq_dealers:['RAJU','RAJU '],mf_dealers:['RAJU SIR','RAJU'],active:true},
  {id:'u6',username:'komal',password:'Komal@123',name:'Komal',role:'staff',segments:['equity','mf'],eq_dealers:['KOMAL','KOMAL '],mf_dealers:['KOMAL','KOMAL (G)'],active:true},
  {id:'u7',username:'riya',password:'Riya@123',name:'Riya',role:'staff',segments:['equity','mf'],eq_dealers:['RIYA','RIYA '],mf_dealers:['RIYA','RIYA '],active:true},
  {id:'u8',username:'bharat',password:'Bharat@123',name:'Bharat',role:'staff',segments:['equity','mf'],eq_dealers:['BHARAT','BHARAT '],mf_dealers:['BHARAT BHAIYA','BHARAT'],active:true},
  {id:'u9',username:'khokhan',password:'Khokhan@123',name:'Khokhan',role:'staff',segments:['equity','mf'],eq_dealers:['KHOKHAN','KHOKHAN '],mf_dealers:['KHOKHAN SIR','KHOKHAN'],active:true},
  {id:'u10',username:'megha',password:'Megha@123',name:'Megha',role:'staff',segments:['mf'],eq_dealers:[],mf_dealers:['MEGHA','MEGHA '],active:true},
  {id:'u11',username:'anjali',password:'Anjali@123',name:'Anjali',role:'staff',segments:['mf'],eq_dealers:[],mf_dealers:['ANJALI','ANJALI '],active:true},
];

// Normalizes an RM name string to its canonical casing as registered in the users list
// (e.g. "riya", "RIYA", "  riya  " -> "Riya"), so the same RM is never split into
// multiple buckets on dashboards/reports due to inconsistent casing. If the name
// doesn't match any registered user, falls back to simple Title Case so at least
// "riya kumari" and "RIYA KUMARI" still merge together.
function normRm(rmStr){
  const raw = (rmStr||'').trim();
  if(!raw) return raw;
  const users = DB.get('users') || DEFAULT_USERS;
  const match = users.find(u=>(u.name||'').trim().toLowerCase()===raw.toLowerCase());
  if(match) return match.name.trim();
  return raw.toLowerCase().replace(/\b\w/g, ch=>ch.toUpperCase());
}

// ══════════════════════════════════════════
// STATE
// ══════════════════════════════════════════
let CU = null; // current user
// Back Office = office-wide upload role (Brokerage + Square-off), scoped like
// admin ONLY for data visibility in those two features — never for edit/delete
// power or any other page (that stays gated strictly to role==='admin').
function isBackOfficeOrAdmin(){ return !!(CU && (CU.role==='admin' || CU.role==='backoffice' || CU.backoffice_access===true)); }
// The single "Allow Broker RMS Risk file upload" checkbox grants BOTH the daily
// Risk file AND Square-off (T+5) upload/view — they're both office-wide broker
// files uploaded together, so one permission covers both.
function canUploadSquareoff(){ return !!(CU && (CU.role==='admin' || CU.role==='backoffice' || CU.risk_upload===true || CU.backoffice_access===true)); }
let PG_SIZE = 50;
let eqPage=1, mfPage=1, leadsPage=1;
let eqSortField=null, eqSortDir=1, mfSortField=null, mfSortDir=1, leadsSortField=null, leadsSortDir=1;
// Restore column-sort state after page reload (app auto-reloads every 30 min),
// so the user's chosen sort (e.g. Name) is not lost on the auto-refresh.
try{
  const _ss=JSON.parse(localStorage.getItem('dninvest_sort_state')||'{}');
  if(_ss.eqField!==undefined && _ss.eqField!==null){ eqSortField=_ss.eqField; eqSortDir=_ss.eqDir||1; }
  if(_ss.mfField!==undefined && _ss.mfField!==null){ mfSortField=_ss.mfField; mfSortDir=_ss.mfDir||1; }
  if(_ss.leadsField!==undefined && _ss.leadsField!==null){ leadsSortField=_ss.leadsField; leadsSortDir=_ss.leadsDir||1; }
}catch(e){}
function _saveSortState(){
  try{ localStorage.setItem('dninvest_sort_state', JSON.stringify({eqField:eqSortField,eqDir:eqSortDir,mfField:mfSortField,mfDir:mfSortDir,leadsField:leadsSortField,leadsDir:leadsSortDir})); }catch(e){}
}
let eqFiltered=[], mfFiltered=[], leadsFiltered=[];
let currentCallTarget = null;
let currentEditId = null;
let currentEditLeadId = null;
let currentReportData = [];
let activeNtTab='30', activeEqfTab='today', activeMffTab='today', activeRptTab='equity', activeEqncTab='60', activeMfncTab='60';

const MN = ['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

function today(){
  // Use device local date directly — avoids double IST offset bug
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth()+1).padStart(2,'0');
  const d = String(now.getDate()).padStart(2,'0');
  return `${y}-${m}-${d}`;
}
function upc(v){ return v==null?'':String(v).toUpperCase(); }
function fmtDate(d){ if(!d) return ''; try{ if(/^\d{4}-\d{2}-\d{2}/.test(d)){ const p=d.split('T')[0].split('-'); return +p[2]+'-'+MN[+p[1]]+'-'+p[0]; } const dt=new Date(d); if(isNaN(dt)) return d; return dt.getDate()+'-'+MN[dt.getMonth()+1]+'-'+dt.getFullYear();}catch(e){return d||'';} }
// Time from a call log's ISO timestamp (e.g. "3:45 PM"). Purane logs me ts na ho to khali.
function fmtTime(ts){ if(!ts) return ''; try{const dt=new Date(ts); if(isNaN(dt)) return ''; return dt.toLocaleTimeString('en-IN',{hour:'numeric',minute:'2-digit',hour12:true});}catch(e){return '';} }
function daysDiff(d){ if(!d) return null; let dt; if(/^\d{4}-\d{2}-\d{2}/.test(d)){ const p=d.split('T')[0].split('-'); dt=new Date(+p[0],+p[1]-1,+p[2]); } else { dt=new Date(d); } if(isNaN(dt)) return null; const now=new Date(); const today=new Date(now.getFullYear(),now.getMonth(),now.getDate()); return Math.floor((today-dt)/(1000*60*60*24)); }
function daysBetween(d1,d2){
  const parse=d=>{ if(!d) return null; let dt; if(/^\d{4}-\d{2}-\d{2}/.test(d)){ const p=d.split('T')[0].split('-'); dt=new Date(+p[0],+p[1]-1,+p[2]); } else { dt=new Date(d); } return isNaN(dt)?null:dt; };
  const a=parse(d1), b=parse(d2);
  if(!a||!b) return null;
  return Math.floor((b-a)/(1000*60*60*24));
}
function addDays(d,n){ const dt=new Date(d); dt.setDate(dt.getDate()+n); return dt.toISOString().split('T')[0]; }
let _uidCounter=0;
function uid(){ _uidCounter=(_uidCounter+1)%1000; return Date.now().toString(36)+'_'+_uidCounter.toString(36)+Math.random().toString(36).substr(2,6); }
function v(x){ return (x!=null&&x!=='')?x:'—'; }

// ══════════════════════════════════════════
// AUTH
// ══════════════════════════════════════════
document.getElementById('lpwd').addEventListener('keydown',e=>{ if(e.key==='Enter') doLogin(); });
document.getElementById('lusr').addEventListener('keydown',e=>{ if(e.key==='Enter') document.getElementById('lpwd').focus(); });

// Detects if the current device is a mobile/handheld device using a
// combination of User-Agent sniffing AND screen width. Both signals are
// checked together so that e.g. a desktop browser window resized small
// is not wrongly blocked, and a mobile browser spoofing UA is still
// caught by the screen-size check (and vice versa).
function isMobileDevice(){
  const uaMobile = /Android|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini|Mobile/i.test(navigator.userAgent);
  const smallScreen = Math.min(window.innerWidth, window.innerHeight) < 768;
  return uaMobile && smallScreen;
}

async function doLogin(){
  const u = document.getElementById('lusr').value.trim().toLowerCase();
  const p = document.getElementById('lpwd').value;
  if(typeof fdb!=='undefined'){
    try{ await DB.syncFromFirebase(); }catch(e){}
  }
  // The active/inactive flag is flipped by runAutoSchedule(), which until now
  // only ran inside an already logged-in session. If nobody was logged in when
  // the window opened (8:00 AM Mon-Fri), every RM's flag stayed at last night's
  // `false` and login was wrongly blocked. Run the schedule here so the flag is
  // always current before the office-hours check below.
  if(typeof fdb!=='undefined'){
    try{ await refreshHolidaySet(); runAutoSchedule(); }catch(e){}
  }
  const users = DB.get('users') || DEFAULT_USERS;
  const td = today();
  // RM can login with PIN (4-digit) OR password; Admin uses password only.
  // Special case: if the account was auto-deactivated by the 10 AM late-login
  // check (lateAbsentMarked === today), login is still allowed — this is what
  // converts their attendance from "Absent" to "Late" and reactivates them.
  // Any other active===false (manual Admin deactivation, or outside the
  // scheduled working window) still blocks login as before.
  // Check credentials first (ignore active flag)
  const userExists = users.find(x=>{
    if(x.username!==u) return false;
    if(x.password===p) return true;
    if(x.role==='rm' && x.pin && x.pin===p) return true;
    return false;
  });
  // Departed employees (Mark as Left) can never log in, regardless of office
  // hours or manualOverride — this is a hard lock, separate from the daily
  // active/inactive auto-schedule.
  if(userExists && userExists.left_company){
    document.getElementById('lerr').textContent = '🚶 This account is marked as left the company. Please contact Admin.';
    document.getElementById('lerr').style.display='block';
    return;
  }
  // If user found but inactive → block ONLY if the clock really is outside
  // today's working window. If we're inside the window, the stored flag is
  // simply stale (auto-schedule not yet run on this device) → repair it and
  // let them in. Manual Admin deactivation still blocks (manualOverride).
  const insideWindow = isWithinActiveWindowNow();
  if(userExists && userExists.active===false && userExists.lateAbsentMarked!==td){
    if(insideWindow && !userExists.manualOverride){
      DB.mutateUsers(fixList=>{
        const fi = fixList.findIndex(x=>x.id===userExists.id);
        if(fi<0) return false;
        fixList[fi].active = true;
      });
      userExists.active = true;
    } else {
      document.getElementById('lerr').textContent = userExists.manualOverride
        ? '🔒 Your access has been disabled by Admin. Please contact Admin.'
        : '🕐 Login not allowed outside office hours. Please try again during working hours.';
      document.getElementById('lerr').style.display='block';
      return;
    }
  }
  const user = users.find(x=>{
    if(x.username!==u) return false;
    if(x.left_company) return false;
    if(x.active===false && x.lateAbsentMarked!==td && !(insideWindow && !x.manualOverride)) return false;
    if(x.password===p) return true;
    if(x.role==='rm' && x.pin && x.pin===p) return true;
    return false;
  });
  if(user){
    // Admin always allowed. RM is blocked if Admin has disabled CRM access
    // via the master switch on the HR Portal (shared Firestore document).
    if(user.role!=='admin'){
      const blocked = await isCrmBlockedForRm();
      if(blocked){
        document.getElementById('lerr').textContent='🔒 Admin has temporarily closed the CRM. Please try again later.';
        document.getElementById('lerr').style.display='block';
        return;
      }
      // RM can only login from desktop — mobile devices are blocked.
      if(isMobileDevice()){
        document.getElementById('lerr').textContent='💻 CRM login is only allowed from Desktop/Laptop. Mobile login is not permitted.';
        document.getElementById('lerr').style.display='block';
        return;
      }
    }
    CU = user;
    CU._loginAt = Date.now();
    localStorage.setItem('dninvest_session', JSON.stringify({username:u, password:p, at:Date.now()}));
    document.getElementById('lerr').style.display='none';
    document.getElementById('loginScreen').style.display='none';
    document.getElementById('app').style.display='block';
    initApp();
    // Silently record HR attendance in the background (same as HR Portal login),
    // so RMs don't need to open the HR Portal separately just to be marked present.
    if(user.role!=='admin'){ recordHrAttendanceOnCrmLogin(user); startCrmHeartbeat(user); }
  } else {
    document.getElementById('lerr').style.display='block';
  }
}

// Checks the shared access-control document (managed from the HR Portal's
// Employees page) to see if Admin has disabled CRM access for RMs.
async function isCrmBlockedForRm(){
  try{
    if(typeof fdb==='undefined') return false;
    const doc = await fdb.collection('shared_control').doc('access').get();
    if(doc.exists && doc.data() && doc.data().data){
      return doc.data().data.crmEnabled===false;
    }
  }catch(e){ console.log('Access control check failed:',e); }
  return false;
}

// ─── CRM HEARTBEAT → HR PORTAL OUT-TIME ─────────────────────────────────────
// RMs spend the whole day in the CRM (not the HR Portal), so the CRM must also
// send heartbeat pings to hr_data/heartbeats every 5 minutes. The HR Portal's
// 6:15 PM auto-out-time job uses the LAST heartbeat as the out time — without
// CRM pings, an RM's out time gets stuck at whenever their HR tab last pinged.
// Same doc structure as HR Portal: {data:{Name:{date,time}}, updated}.
// Shared, cached (fetched once per page-load) live HR employee-name list —
// base 9 + anyone added later via HR Portal. Both the attendance-recorder and
// the heartbeat name-resolver use this SAME function now; previously each had
// its own hardcoded copy of just the base 9, so a new hire (e.g. "Shyam")
// could match correctly in one place but not the other — his attendance got
// recorded under the right name, but his live heartbeat/out-time updates
// still resolved a name via the OTHER (stale) hardcoded list, so his "Out"
// time looked frozen while everyone else's kept advancing.
let _hrNamesCache = null;
async function getLiveHrNames(){
  if(_hrNamesCache) return _hrNamesCache;
  const base = ['Puja','Rohit','Raju','Komal','Riya','Bharat','Khokhan','Megha','Anjali'];
  try{
    if(typeof fdb!=='undefined'){
      const ecSnap = await fdb.collection('hr_data').doc('employee_changes').get();
      const ecData = (ecSnap.exists && ecSnap.data() && ecSnap.data().data) ? ecSnap.data().data : {};
      const added = Array.isArray(ecData.added) ? ecData.added : [];
      added.forEach(e=>{ if(e && e.name && !base.some(n=>n.toLowerCase()===String(e.name).trim().toLowerCase())) base.push(String(e.name).trim()); });
    }
  }catch(e){ console.log('[ATT] employee_changes fetch failed, using base list only:', e.message); }
  _hrNamesCache = base;
  return base;
}
function matchHrName(user, hrNames){
  const rawName = String(user.name || user.username || '').trim();
  const match = hrNames.find(n =>
    n.toLowerCase() === rawName.toLowerCase() ||
    n.toLowerCase() === rawName.split(' ')[0].toLowerCase() ||
    n.toLowerCase() === String(user.username||'').toLowerCase()
  );
  return match || rawName;
}
async function hrNameForCrmUser(user){
  return matchHrName(user, await getLiveHrNames());
}
let _crmHbTimer=null, _crmHbName=null, _crmHbBlocked=false;
function _crmHbCutoffH(){
  const dow=new Date().getDay(); // 0=Sun,6=Sat (device time = IST)
  if(dow>=1 && dow<=5) return 18.25; // Mon–Fri 6:15 PM
  if(dow===6) return 14.25;          // Saturday 2:15 PM
  return null;                        // Sunday — no cutoff
}
async function sendCrmHeartbeat(){
  try{
    if(typeof fdb==='undefined' || !_crmHbName) return;
    // Session jo cutoff ke baad start hua — heartbeat mat bhejo, warna
    // late-night fresh login ka time out-time ban jata hai (HR portal jaisa hi fix)
    if(_crmHbBlocked) return;
    const n=new Date();
    const hhmm=String(n.getHours()).padStart(2,'0')+':'+String(n.getMinutes()).padStart(2,'0');
    const docRef=fdb.collection('hr_data').doc('heartbeats');
    await fdb.runTransaction(async (tx)=>{
      const doc=await tx.get(docRef);
      let latest=(doc.exists && doc.data() && doc.data().data)?doc.data().data:{};
      latest[_crmHbName]={date:today(), time:hhmm};
      tx.set(docRef, {data:latest, updated:new Date().toISOString()});
    });
    // LIVE OUT-TIME: RMs poora din CRM me rehte hain, isliye out-time yahin se live
    // save karo — HR portal ya kisi device ke 6:15 PM par khule hone par depend na kare.
    // Sirf 'out' field update hoti hai; half-day ka faisla HR ka autoOutTimeCheck cutoff
    // par karta hai. Out tabhi badalti hai jab naya time purane se aage ho (max 1 write/min).
    await crmLiveOutTime(hhmm);
  }catch(e){ console.log('[HB] ping failed:', e); }
}
// Aaj ke attendance record ki out-time live update — CRM se seedhe HR ki attendance
// doc me (hr_data/attendance). Sirf out field; status ko haath nahi.
async function crmLiveOutTime(hhmm){
  try{
    if(typeof fdb==='undefined' || !_crmHbName) return;
    const td=today();
    const docRef=fdb.collection('hr_data').doc('attendance');
    await fdb.runTransaction(async (tx)=>{
      const doc=await tx.get(docRef);
      let latest=(doc.exists && doc.data() && doc.data().data)?doc.data().data:{};
      const arr=latest[_crmHbName]; if(!arr) return;
      const idx=arr.findIndex(r=>r.date===td); if(idx<0) return;
      const rec=arr[idx];
      if(!(rec.status==='Present'||rec.status==='Late'||rec.status==='Half day')) return;
      if(rec.out && !(hhmm>rec.out)) return; // same/earlier — no write needed
      arr[idx]={...rec, out:hhmm};
      latest[_crmHbName]=arr;
      tx.set(docRef, {data:latest, updated:new Date().toISOString()});
    });
  }catch(e){ console.log('[HB] out-time update failed:', e); }
}
async function startCrmHeartbeat(user){
  if(_crmHbTimer) return; // already running
  _crmHbName=await hrNameForCrmUser(user);
  const n=new Date(), cutoff=_crmHbCutoffH();
  _crmHbBlocked = (cutoff!==null && (n.getHours()+n.getMinutes()/60)>=cutoff);
  sendCrmHeartbeat(); // immediate first ping (skips itself automatically in a blocked session)
  _crmHbTimer=setInterval(sendCrmHeartbeat, 5*60000); // every 5 min
  bindCrmHbEdgePings();
}
// Tab hide/close/blur hone par extra ping — background me browser 5-min timer ko
// throttle kar deta hai, isliye last-active moment tighter capture karne ke liye.
let _crmHbEdgeBound=false;
function bindCrmHbEdgePings(){
  if(_crmHbEdgeBound) return; _crmHbEdgeBound=true;
  document.addEventListener('visibilitychange', ()=>{ if(document.visibilityState==='hidden') sendCrmHeartbeat(); });
  window.addEventListener('pagehide', ()=>{ sendCrmHeartbeat(); });
  window.addEventListener('blur', ()=>{ sendCrmHeartbeat(); });
}

// Auto-marks HR attendance the moment an RM logs into the CRM — silent,
// background, no UI shown. In-time = login time minus 5 minutes (same rule
// as the HR Portal's own login). Writes to the SAME Firestore collection
// ('hr_data', doc 'attendance') that the HR Portal reads, so both stay in sync.
// On Sunday or a Stock Exchange holiday (HR Portal's holiday calendar), the
// status is recorded as "Holiday" instead of "Present".
async function recordHrAttendanceOnCrmLogin(user){
  try{
    // If Firebase isn't ready yet (slow mobile load), wait for it rather than
    // silently skipping — otherwise a first login on a bad connection records
    // no attendance at all for the day.
    if(typeof fdb==='undefined'){ await window.waitForFdb(8000); }
    if(typeof fdb==='undefined'){ console.log('[ATT] fdb still undefined after wait, skip'); return; }
    // Canonical HR employee name — MUST match dninvest-hr.html's EMPLOYEES list
    // exactly, so attendance is saved under the same key the HR Portal reads.
    // getLiveHrNames() covers the stable base 9 PLUS anyone added later via HR
    // Portal's "+ Add employee" (fetched live from hr_data/employee_changes) —
    // shared with hrNameForCrmUser() so both paths always agree on the same name.
    const name = matchHrName(user, await getLiveHrNames());
    const rawName = String(user.name || user.username || '').trim();
    const td = today();
    console.log('[ATT] user='+rawName+' matched='+name+' date='+td);

    // Read current doc first
    const docRef = fdb.collection('hr_data').doc('attendance');
    const snap = await docRef.get();
    const existing = (snap.exists && snap.data() && snap.data().data) ? snap.data().data : {};
    const todayRec = existing[name] && existing[name].find(r=>r.date===td);
    console.log('[ATT] todayRec='+JSON.stringify(todayRec||null));

    // If today was auto-marked "Absent" by the 10 AM late-login check, this is
    // a delayed login — flip it to "Late" and reactivate the user immediately,
    // instead of leaving them deactivated/absent for the rest of the day.
    if(todayRec && todayRec.status==='Absent'){
      const inDate = new Date();
      const inTime = String(inDate.getHours()).padStart(2,'0')+':'+String(inDate.getMinutes()).padStart(2,'0');
      const record = {date:td, in:inTime, out:'', status:'Late'};
      // Retry with exponential backoff (3 attempts)
      for(let attempt=1; attempt<=3; attempt++){
        try{
          await fdb.runTransaction(async (tx)=>{
            const doc = await tx.get(docRef);
            let latest = (doc.exists && doc.data() && doc.data().data) ? doc.data().data : {};
            if(!latest[name]) latest[name]=[];
            const idx = latest[name].findIndex(r=>r.date===td);
            if(idx>=0) latest[name][idx]=record; else latest[name].push(record);
            tx.set(docRef, {data:latest, updated:new Date().toISOString()});
          });
          console.log('[ATT] ✅ Late flip saved (attempt '+attempt+')');
          break;
        }catch(e){
          console.log('[ATT] Late flip attempt '+attempt+' failed:', e.message);
          if(attempt<3) await new Promise(r=>setTimeout(r, attempt*3000));
        }
      }
      // Reactivate the user account that the 10 AM check had deactivated
      await DB.mutateUsers(usersList=>{
        const uidx = usersList.findIndex(x=>x.id===user.id);
        if(uidx<0) return false;
        usersList[uidx].active = true;
        usersList[uidx].manualOverride = false;
        usersList[uidx].lateAbsentMarked = '';
      });
      CU.active = true; // keep current session's in-memory user object in sync
      return;
    }

    if(todayRec) {
      // Already marked today with a normal status — but if it was wrongly
      // flipped to "Half day" too early (out-time before 1:30 PM cutoff has
      // even arrived, e.g. from the retroactive-migration bug, or simply
      // stale), and this RM is actively logging back in well before the real
      // cutoff, treat it the same as a fresh check-in: heal it back to
      // Present/Late. Mirrors dninvest-hr.html's healTodayAttendanceIfNeeded —
      // without this, a manual admin fix (e.g. editing out-time) followed by
      // the RM just reopening/re-logging into CRM had no way to actually
      // recompute status, since this function used to just skip silently here.
      if(todayRec.status==='Half day' && todayRec.out){
        const dayOfWeek2 = getISTDayOfWeek();
        let cut2=null; if(dayOfWeek2>=1&&dayOfWeek2<=5) cut2=18.25; else if(dayOfWeek2===6) cut2=14.25;
        const nowD=new Date(); const nowIst2=new Date(nowD.getTime()+nowD.getTimezoneOffset()*60000+5.5*3600000);
        const nowH2=nowIst2.getHours()+nowIst2.getMinutes()/60;
        if(cut2!==null && nowH2<cut2){
          const m2=/^(\d{1,2}):(\d{2})$/.exec(todayRec.in||'');
          const lateCut2=(dayOfWeek2===6)?10.75:10;
          const st2=(m2 && (Number(m2[1])+Number(m2[2])/60)>=lateCut2)?'Late':'Present';
          try{
            await fdb.runTransaction(async (tx)=>{
              const doc = await tx.get(docRef);
              let latest = (doc.exists && doc.data() && doc.data().data) ? doc.data().data : {};
              const idx = (latest[name]||[]).findIndex(r=>r.date===td);
              if(idx>=0) latest[name][idx]={...latest[name][idx], out:'', status:st2};
              tx.set(docRef, {data:latest, updated:new Date().toISOString()});
            });
            console.log('[ATT] ✅ healed premature Half day → '+st2);
          }catch(e){ console.log('[ATT] heal attempt failed:', e.message); }
        }
      }
      console.log('[ATT] already marked, skip');
      return;
    }

    // Determine in-time (login time - 2 min)
    const inDate = new Date(Date.now() - 2*60*1000);
    const inTime = String(inDate.getHours()).padStart(2,'0')+':'+String(inDate.getMinutes()).padStart(2,'0');

    // Check holiday calendar (Sunday or Stock Exchange holiday set from HR Portal)
    let status = 'Present';
    const dayOfWeek = getISTDayOfWeek(); // IST-aware — avoids UTC midnight bug
    if(dayOfWeek===0){
      status = 'Holiday'; // Sunday only
    } else {
      try{
        const holDoc = await fdb.collection('hr_data').doc('holidays').get();
        const holidays = (holDoc.exists && holDoc.data() && holDoc.data().data) ? holDoc.data().data : [];
        if(holidays.some(h=>h.date===td)) status='Holiday';
      }catch(e){}
    }

    console.log('[ATT] saving record: name='+name+' in='+inTime+' status='+status);
    const record = {date:td, in:inTime, out:'', status};

    // Retry with exponential backoff (3 attempts) — handles network blips
    const MAX_ATTEMPTS = 3;
    let saved = false;
    for(let attempt=1; attempt<=MAX_ATTEMPTS; attempt++){
      try{
        await fdb.runTransaction(async (tx)=>{
          const doc = await tx.get(docRef);
          let latest = (doc.exists && doc.data() && doc.data().data) ? doc.data().data : {};
          if(!latest[name]) latest[name]=[];
          const idx = latest[name].findIndex(r=>r.date===td);
          if(idx>=0) latest[name][idx]=record; else latest[name].push(record);
          tx.set(docRef, {data:latest, updated:new Date().toISOString()});
        });
        console.log('[ATT] ✅ saved successfully for '+name+' (attempt '+attempt+')');
        saved = true;
        break;
      }catch(e){
        console.log('[ATT] attempt '+attempt+' failed:', e.message);
        if(attempt < MAX_ATTEMPTS){
          const delay = attempt * 3000; // 3s, 6s
          console.log('[ATT] retrying in '+delay+'ms...');
          await new Promise(r=>setTimeout(r, delay));
        }
      }
    }
    if(!saved) console.log('[ATT] ❌ all '+MAX_ATTEMPTS+' attempts failed for '+name);
  }catch(e){
    console.log('[ATT] ❌ CRM auto-attendance error:', e);
  }
}

// Opens the HR Portal in a new tab, pre-filling the username so the person
// only needs to type their password/PIN once more (browser security means
// we can't auto-login across tabs, but this saves the username step).
function openHrPortal(){
  if(!CU || !CU.username){ alert('Please login first'); return; }
  // Generate a one-time token valid for 30 seconds — HR reads it and auto-logs-in
  // without asking for a password again (since the user already authenticated in CRM).
  const token = Math.random().toString(36).slice(2) + Date.now().toString(36);
  const hrName = (()=>{
    const HR_NAMES = ['Puja','Rohit','Raju','Komal','Riya','Bharat','Khokhan','Megha','Anjali'];
    const raw = String(CU.name||CU.username||'').trim();
    return HR_NAMES.find(n=>
      n.toLowerCase()===raw.toLowerCase() ||
      n.toLowerCase()===raw.split(' ')[0].toLowerCase() ||
      n.toLowerCase()===String(CU.username||'').toLowerCase()
    ) || raw;
  })();
  try{
    localStorage.setItem('dnihr_autologin_token', JSON.stringify({
      token, name:hrName, isAdmin:(CU.role==='admin'), at:Date.now()
    }));
  }catch(e){}
  window.open('dninvest-hr.html?autologin='+token+'&u='+encodeURIComponent(CU.username), '_blank');
}

function doLogout(){
  CU=null;
  localStorage.removeItem('dninvest_session');
  sessionStorage.removeItem('dninvest_session'); // clean up old-style sessions too
  document.getElementById('loginScreen').style.display='flex';
  document.getElementById('app').style.display='none';
  document.getElementById('lusr').value='';
  document.getElementById('lpwd').value='';
}

// ══════════════════════════════════════════
// FORGOT PASSWORD
// ══════════════════════════════════════════
const MASTER_RECOVERY_CODE = 'DNINVEST2026';

function openForgotPassword(){
  document.querySelector('#loginScreen .login-box').style.display='none';
  document.getElementById('forgotBox').style.display='block';
  document.getElementById('fp_username').value='';
  document.getElementById('fp_code').value='';
  document.getElementById('fp_newpass').value='';
  document.getElementById('fp_err').style.display='none';
  document.getElementById('fp_ok').style.display='none';
}

function closeForgotPassword(){
  document.getElementById('forgotBox').style.display='none';
  document.querySelector('#loginScreen .login-box').style.display='block';
}

async function doResetPassword(){
  const username=document.getElementById('fp_username').value.trim().toLowerCase();
  const code=document.getElementById('fp_code').value.trim();
  const newpass=document.getElementById('fp_newpass').value;
  const errEl=document.getElementById('fp_err');
  const okEl=document.getElementById('fp_ok');
  errEl.style.display='none';
  okEl.style.display='none';

  if(!username || !code || !newpass){
    errEl.textContent='Please fill all fields';
    errEl.style.display='block';
    return;
  }
  if(newpass.length<4){
    errEl.textContent='Password must be at least 4 characters';
    errEl.style.display='block';
    return;
  }
  if(code!==MASTER_RECOVERY_CODE){
    errEl.textContent='Invalid recovery code';
    errEl.style.display='block';
    return;
  }

  // Make sure we have the real, latest users list from Firebase before
  // writing back - otherwise (e.g. on a fresh browser that hasn't synced
  // yet) we'd overwrite everyone's passwords with DEFAULT_USERS.
  if(typeof fdb!=='undefined'){
    try{ await DB.syncFromFirebase(); }catch(e){}
  }
  const r = await DB.mutateUsers(users=>{
    const idx = users.findIndex(x=>x.username===username);
    if(idx<0) return false;
    users[idx].password = newpass;
  });
  if(r.aborted){
    errEl.textContent='Username not found';
    errEl.style.display='block';
    return;
  }

  okEl.style.display='block';
  document.getElementById('fp_username').value='';
  document.getElementById('fp_code').value='';
  document.getElementById('fp_newpass').value='';
}

// Auto-login if session exists (persistent — survives browser/app close).
// Session is cleared only by Logout, wrong/changed credentials, or RM-on-mobile.
async function tryAutoLogin(){
  // Client "player mode" (?play in URL): skip CRM login entirely — the quiz
  // player overlay handles everything. Guard so RM/admin auto-login never runs.
  if(typeof QUIZ!=='undefined' && ((QUIZ.isPlayMode && QUIZ.isPlayMode()) || (QUIZ.isScreenMode && QUIZ.isScreenMode()))) return;
  // Wait for Firebase to be ready before doing anything — on a slow mobile
  // connection the SDK scripts may not have loaded yet when this fires. Without
  // this, auto-login used to run with fdb undefined → no client sync + skipped
  // attendance (an RM would see an empty client list and no "Present" mark).
  await window.waitForFdb(8000);
  // Migrate any old sessionStorage session to localStorage (one-time)
  let saved = localStorage.getItem('dninvest_session');
  if(!saved){
    const oldSess = sessionStorage.getItem('dninvest_session');
    if(oldSess){ localStorage.setItem('dninvest_session', oldSess); sessionStorage.removeItem('dninvest_session'); saved = oldSess; }
  }
  if(saved){
    try{
      const {username, password, at} = JSON.parse(saved);
      const users = DB.get('users') || DEFAULT_USERS;
      const td = today();
      // Same credential + active rules as doLogin():
      // - RM can match via password OR 4-digit PIN (session stores whatever was typed)
      // - active===false allowed only if lateAbsentMarked===today (late-login reactivation)
      // Without this, PIN-login RMs were force-logged-out on every 30-min auto reload.
      const user = users.find(x=>{
        if(x.username!==username) return false;
        const credOk = (x.password===password) || (x.role==='rm' && x.pin && x.pin===password);
        if(!credOk) return false;
        if(x.active===false && x.lateAbsentMarked!==td) return false;
        return true;
      });
      if(user){
        // Don't auto-login RMs on mobile — force them back to the login
        // screen so the desktop-only restriction in doLogin() applies.
        if(user.role!=='admin' && isMobileDevice()){
          localStorage.removeItem('dninvest_session');
          return;
        }
        // RM: respect the CRM master switch (Admin can close CRM from HR Portal).
        // Session is kept (not cleared) so auto-login resumes once reopened.
        if(user.role!=='admin'){
          try{ if(await isCrmBlockedForRm()) return; }catch(e){}
        }
        // Daily fresh-login for RMs on shared computers: the FIRST auto-login
        // after 8 AM each day must instead ask for a password. If the saved
        // session was established before today's 8 AM boundary, clear it and
        // stay on the login screen. (Admin is exempt — stays auto-logged in.)
        if(user.role!=='admin'){
          const now8 = new Date(); now8.setHours(8,0,0,0);
          const nowMs = Date.now();
          const loginAt = Number(at)||0;
          if(nowMs >= now8.getTime() && loginAt < now8.getTime()){
            localStorage.removeItem('dninvest_session');
            return; // require manual password login
          }
        }
        CU = user;
        CU._loginAt = Number(at)||Date.now();
        document.getElementById('loginScreen').style.display='none';
        document.getElementById('app').style.display='block';
        initApp();
        // Restart heartbeat pings after page refresh (session restore skips doLogin)
        // AND mark today's HR attendance — with persistent auto-login an RM may
        // never hit doLogin() on a new day. The function is idempotent: it skips
        // if today is already marked, and flips a 10 AM "Absent" to "Late".
        if(user.role!=='admin'){ recordHrAttendanceOnCrmLogin(user); startCrmHeartbeat(user); }
        return;
      }
    }catch(e){}
    localStorage.removeItem('dninvest_session');
  }
}
window.addEventListener('DOMContentLoaded', tryAutoLogin);

// ══════════════════════════════════════════
// INIT
// ══════════════════════════════════════════
// ── Admin-configurable Call Date Limits (locks RM date entry) ─────────────
// Admin sets earliest/latest allowed dates for Last Call & Next Call. RMs are
// restricted (input min/max + save validation). Admin itself is unrestricted.
// mode:'rolling' → offsets are DAYS from today (recomputed on every read, so the
// window auto-rolls daily and never needs re-clicking). mode:'fixed' → frozen dates.
let CALL_LIMITS = { configured:false, rLcMin:'', rLcMax:'', rNcMin:'', rNcMax:'' };
const CL_DEFAULT_ROLL = { rLcMin:'', rLcMax:0, rNcMin:0, rNcMax:90 };
// Local-date arithmetic (no UTC/ISO — avoids IST midnight date shift)
function _dayOffset(n){
  const d=new Date(); d.setHours(12,0,0,0); d.setDate(d.getDate()+Number(n));
  const y=d.getFullYear(), m=String(d.getMonth()+1).padStart(2,'0'), dd=String(d.getDate()).padStart(2,'0');
  return `${y}-${m}-${dd}`;
}
function _off(v){ return (v===''||v===null||v===undefined||isNaN(v)) ? '' : _dayOffset(v); }
function _plus3m(){ return _dayOffset(90); }
async function loadCallLimits(){
  try{
    if(typeof fdb==='undefined') return;
    const doc = await fdb.collection('shared_control').doc('call_limits').get();
    if(doc.exists && doc.data() && doc.data().data){
      const d=doc.data().data;
      const num=v=>(v===''||v===null||v===undefined)?'':Number(v);
      // Only rolling limits are supported now — any old fixed-mode dates
      // in the doc (from before Fixed Dates was removed) are ignored.
      CALL_LIMITS = { configured:true,
        rLcMin:num(d.rLcMin), rLcMax:num(d.rLcMax), rNcMin:num(d.rNcMin), rNcMax:num(d.rNcMax) };
    }
  }catch(e){ console.log('call limits load failed', e); }
  try{ if(getCurrentPageId()==='admin') populateCallLimitInputs(); }catch(e){}
}
// Effective min/max for the CURRENT user. Admin = no limits.
function effectiveCallLimits(){
  if(CU && CU.role==='admin') return {lcMin:'',lcMax:'',ncMin:'',ncMax:''};
  const r = CALL_LIMITS.configured ? CALL_LIMITS : CL_DEFAULT_ROLL;
  return {lcMin:_off(r.rLcMin), lcMax:_off(r.rLcMax), ncMin:_off(r.rNcMin), ncMax:_off(r.rNcMax)};
}
function renderCallLimitPreview(){
  const el=document.getElementById('cl-preview'); if(!el) return;
  const gn=id=>{const v=(document.getElementById(id)||{value:''}).value; return v===''?'':Number(v);};
  const fd=s=>{ if(!s) return '<i>no limit</i>'; const p=s.split('-'); return p[2]+'-'+p[1]+'-'+p[0]; };
  el.innerHTML = '<b>Effect for today (' + fd(today()) + '):</b><br>'
    + '📞 Last Call: ' + fd(_off(gn('cl-r-lc-min'))) + ' &nbsp;se&nbsp; ' + fd(_off(gn('cl-r-lc-max'))) + '<br>'
    + '🔜 Next Call: ' + fd(_off(gn('cl-r-nc-min'))) + ' &nbsp;se&nbsp; ' + fd(_off(gn('cl-r-nc-max')))
    + '<br><span style="color:var(--gray)">Tomorrow this window will automatically shift forward by 1 day.</span>';
}
function clearCallLimits(){
  ['cl-r-lc-min','cl-r-lc-max','cl-r-nc-min','cl-r-nc-max']
    .forEach(id=>{const el=document.getElementById(id); if(el) el.value='';});
  renderCallLimitPreview();
}
// Keep a prefilled default inside the allowed window, else the form opens
// with a value the RM can't actually save.
// Static (non-template) date inputs get their limits applied imperatively.
function applyCallLimitsTo(id, kind){
  const el=document.getElementById(id); if(!el) return;
  const c=effectiveCallLimits(); const mn=kind==='lc'?c.lcMin:c.ncMin, mx=kind==='lc'?c.lcMax:c.ncMax;
  if(mn) el.min=mn; else el.removeAttribute('min');
  if(mx) el.max=mx; else el.removeAttribute('max');
}
function _clamp(d, min, max){ if(!d) return d; if(min && d<min) return min; if(max && d>max) return max; return d; }
function _clampLC(d){ const c=effectiveCallLimits(); return _clamp(d, c.lcMin, c.lcMax); }
function _clampNC(d){ const c=effectiveCallLimits(); return _clamp(d, c.ncMin, c.ncMax); }
function _lcAttr(){ const c=effectiveCallLimits(); return (c.lcMin?` min="${c.lcMin}"`:'')+(c.lcMax?` max="${c.lcMax}"`:''); }
function _ncAttr(){ const c=effectiveCallLimits(); return (c.ncMin?` min="${c.ncMin}"`:'')+(c.ncMax?` max="${c.ncMax}"`:''); }

// ---- Next Calling Date: Sunday / holiday guard -------------------------
// A next-call date must never land on a Sunday or a Stock Exchange holiday
// (holiday calendar comes from the HR Portal via _holidaySet).
function _isOffDay(d){
  if(!d) return false;
  const dt = new Date(d+'T00:00:00');
  if(isNaN(dt)) return false;
  if(dt.getDay()===0) return true;                    // Sunday
  try{ if(_holidaySet && _holidaySet.has(d)) return true; }catch(e){}
  return false;
}
function _offDayLabel(d){
  const dt = new Date(d+'T00:00:00');
  return dt.getDay()===0 ? 'Sunday' : 'Holiday';
}
// Roll forward to the next working day; if that crosses the allowed max,
// roll backward instead so the date stays inside the call-limit window.
function _nextWorkingDay(d){
  if(!d) return d;
  const c = effectiveCallLimits();
  let x = d;
  for(let i=0; i<21 && _isOffDay(x); i++) x = addDays(x,1);
  if(c.ncMax && x > c.ncMax){
    x = d;
    for(let i=0; i<21 && _isOffDay(x); i++) x = addDays(x,-1);
    if(c.ncMin && x < c.ncMin) return d; // no working day in window — leave as is
  }
  return _isOffDay(x) ? d : x;
}
function _clampNCWork(d){ return _nextWorkingDay(_clampNC(d)); }

// Delegated guard — works for inputs rendered inside modals after load.
document.addEventListener('change', function(e){
  const el = e.target;
  if(!el || el.type!=='date') return;
  if(!['f_next_call','l_next_call','cl_next'].includes(el.id)) return;
  const v = el.value;
  if(!v || !_isOffDay(v)) return;
  const fixed = _nextWorkingDay(v);
  if(fixed && fixed!==v){
    const was = _offDayLabel(v);
    el.value = fixed;
    try{ toast('Call date cannot fall on '+was+' — changed to '+fmtDate(fixed),'warn'); }
    catch(err){ alert('Call date cannot fall on '+was+'. Set to '+fixed+' instead.'); }
  } else {
    try{ toast('No working day found in this window — please check the dates','warn'); }catch(err){}
  }
});
// -----------------------------------------------------------------------
function populateCallLimitInputs(){
  const set=(id,v)=>{const el=document.getElementById(id); if(el) el.value=(v===''||v===null||v===undefined)?'':v;};
  const r = CALL_LIMITS.configured ? CALL_LIMITS : CL_DEFAULT_ROLL;
  set('cl-r-lc-min',r.rLcMin); set('cl-r-lc-max',r.rLcMax); set('cl-r-nc-min',r.rNcMin); set('cl-r-nc-max',r.rNcMax);
  ['cl-r-lc-min','cl-r-lc-max','cl-r-nc-min','cl-r-nc-max'].forEach(id=>{
    const el=document.getElementById(id); if(el && !el._clBound){ el._clBound=1; el.addEventListener('input',renderCallLimitPreview); }
  });
  renderCallLimitPreview();
}
// Reset to the built-in DEFAULT (rolling): Last Call up to today; Next Call
// today .. +3 months. Removes any admin-set fixed limits.
async function resetCallLimits(){
  if(!CU || CU.role!=='admin'){ toast('Only Admin can change this','error'); return; }
  CALL_LIMITS = { configured:false, ...CL_DEFAULT_ROLL };
  try{
    if(typeof fdb!=='undefined') await fdb.collection('shared_control').doc('call_limits').delete();
    populateCallLimitInputs();
    toast('Default rolling limits restored ✓ (Last Call: up to today · Next Call: today +90 days) — no need to click daily','success');
  }catch(e){ toast('Reset failed: '+(e&&e.message||e),'error'); }
}
async function saveCallLimits(){
  if(!CU || CU.role!=='admin'){ toast('Only Admin can change this','error'); return; }
  const gv=id=>(document.getElementById(id)||{value:''}).value||'';
  const gn=id=>{const v=gv(id); return v===''?'':Number(v);};
  const d={ rLcMin:gn('cl-r-lc-min'), rLcMax:gn('cl-r-lc-max'), rNcMin:gn('cl-r-nc-min'), rNcMax:gn('cl-r-nc-max') };
  if(d.rLcMin!=='' && d.rLcMax!=='' && d.rLcMin>d.rLcMax){ toast('Last Call: "minimum" days is greater than "maximum" days','error'); return; }
  if(d.rNcMin!=='' && d.rNcMax!=='' && d.rNcMin>d.rNcMax){ toast('Next Call: "minimum" days is greater than "maximum" days','error'); return; }
  CALL_LIMITS = { configured:true, ...d };
  const allBlank = (d.rLcMin===''&&d.rLcMax===''&&d.rNcMin===''&&d.rNcMax==='');
  if(allBlank && !confirm('All fields are blank — this means NO LIMIT will apply to RMs.\n\nAre you sure you want to save?')) return;
  try{
    if(typeof fdb!=='undefined') await fdb.collection('shared_control').doc('call_limits').set({data:d, updated:new Date().toISOString()});
    toast('Rolling limits saved ✓ — will shift forward automatically every day','success');
    renderCallLimitPreview();
  }catch(e){ toast('Save failed: '+(e&&e.message||e),'error'); }
}

function initApp(){
  // Defensive: force-clear all search boxes on every fresh load, regardless
  // of WHY they might have stray text (Chrome autofill, a 3rd-party typing
  // tool auto-inserting saved text, stale DOM, etc). A leftover value here
  // silently filters an entire table to "0 results" and looks like a data
  // bug when it's really just an un-cleared search box.
  ['leads-search','eq-search','mf-search','eqnc-search','mfnc-search'].forEach(id=>{
    const el=document.getElementById(id); if(el) el.value='';
  });
  // Sync from Firebase first, then setup real-time listeners.
  // If fdb isn't ready yet (slow load), wait for it so we don't skip the
  // whole sync and leave the RM staring at an empty client list.
  if(typeof fdb==='undefined'){
    window.waitForFdb(8000).then(()=>{ if(typeof fdb!=='undefined') initApp(); });
    return;
  }
  if(typeof fdb!=='undefined'){
    DB.syncFromFirebase().then(()=>{
      DB.load(); // only seed defaults AFTER real Firestore data has been loaded
      refreshDash(); updateBadges(); populateRmDropdowns();
      if(getCurrentPageId()==='eq-clients') renderEqTable();
      if(getCurrentPageId()==='mf-clients') renderMfTable();
      if(getCurrentPageId()==='leads') renderLeadsTable();
      checkAnnouncement();
      checkFollowupAlert();
      updateMsgBadge();
      checkRmReply();
      refreshHolidaySet().then(runAutoSchedule); // Load holidays, then run auto-schedule on load
      cleanExpiredTempAccess(); // Clean expired temp access
      loadCallLimits(); // admin-configured call date locks

      // Real-time: call date limits (RM gets Admin's change without reload)
      fdb.collection('shared_control').doc('call_limits').onSnapshot(doc=>{
        if(doc.exists && doc.data() && doc.data().data){
          const d=doc.data().data;
          const num=v=>(v===''||v===null||v===undefined)?'':Number(v);
          CALL_LIMITS = { configured:true, rLcMin:num(d.rLcMin), rLcMax:num(d.rLcMax), rNcMin:num(d.rNcMin), rNcMax:num(d.rNcMax) };
          try{ if(getCurrentPageId()==='admin') populateCallLimitInputs(); }catch(e){}
        }
      });

      // Real-time: force-logout — only needed for non-admin users.
      if(CU && CU.role !== 'admin'){
        fdb.collection('crm_data').doc('force_logout').onSnapshot(doc=>{
          if(!doc.exists || !doc.data() || !doc.data().data || !CU) return;
          const flags = doc.data().data;
          const myFlagAt = flags[CU.username];
          if(myFlagAt && myFlagAt > (CU._loginAt||0)){
            alert('⚠️ Admin has logged you out of the CRM.');
            doLogout();
          }
        });
      }

      // Real-time listener for announcements (single object, not array).
      // Any onSnapshot listener that has NO error callback silently dies forever
      // on the first permission/network hiccup — RM then only gets the update on
      // next full page reload, not live. attachAnnouncementListener() below wires
      // an error callback that auto-reconnects (with backoff) so it self-heals.
      let _annRetryDelay = 2000;
      function attachAnnouncementListener(){
        fdb.collection('crm_data').doc('announcement').onSnapshot(doc=>{
          _annRetryDelay = 2000; // reset backoff on any successful event
          if(doc.exists && doc.data() && Object.prototype.hasOwnProperty.call(doc.data(),'data')){
            const newData = doc.data().data;
            const existing = DB.get('announcement');
            if(JSON.stringify(newData) !== JSON.stringify(existing)){
              DB.setLocal('announcement', newData); // updates localStorage AND in-memory cache
              console.log('Real-time update: announcement');
              if(getCurrentPageId()==='announcements'){ renderAnnouncementAdmin(); renderInbox(); }
              else if(getCurrentPageId()==='admin') renderAnnouncementAdmin();
              checkAnnouncement();
            }
          }
        }, err=>{
          console.log('Announcement listener error, reconnecting in', _annRetryDelay, 'ms:', err);
          setTimeout(attachAnnouncementListener, _annRetryDelay);
          _annRetryDelay = Math.min(_annRetryDelay * 2, 60000); // exponential backoff, capped 60s
        });
      }
      attachAnnouncementListener();

      // comm_history — instant onSnapshot (Admin sends announcement → RM sees it immediately)
      fdb.collection('crm_data').doc('comm_history').onSnapshot(doc=>{
        if(doc.exists && doc.data() && Object.prototype.hasOwnProperty.call(doc.data(),'data')){
          const newData = doc.data().data;
          const existing = DB.get('comm_history');
          if(JSON.stringify(newData) !== JSON.stringify(existing)){
            DB.setLocal('comm_history', newData);
            if(CU && CU.role==='admin' && getCurrentPageId()==='announcements'){ try{ renderCommHistory(); }catch(e){} }
          }
        }
      });

      // special_offers — fetch on login + every 3 hours (not realtime, saves billing)
      async function fetchSpecialOffers(){
        try{
          const snap = await fdb.collection('crm_data').doc('special_offers').get();
          if(snap.exists && snap.data() && Object.prototype.hasOwnProperty.call(snap.data(),'data')){
            const newData = snap.data().data;
            const existing = DB.get('special_offers');
            if(JSON.stringify(newData) !== JSON.stringify(existing)){
              DB.setLocal('special_offers', newData);
              if(CU && CU.role==='admin'){ if(getCurrentPageId()==='announcements') renderOffersAdmin(); }
              else { try{ showOfferPopupIfAny(true); }catch(e){} }
            }
          }
        }catch(e){}
      }
      fetchSpecialOffers();
      setInterval(fetchSpecialOffers, 3 * 60 * 60 * 1000);   // every 3 hours

      // Real-time listener for rm_messages (two-way messaging)
      fdb.collection('crm_data').doc('rm_messages').onSnapshot(doc=>{
        if(doc.exists && doc.data() && doc.data().data){
          const newData = doc.data().data;
          const existing = JSON.parse(localStorage.getItem('dninvest_rm_messages')||'[]');
          if(JSON.stringify(newData) !== JSON.stringify(existing)){
            localStorage.setItem('dninvest_rm_messages', JSON.stringify(newData));
            console.log('Real-time update: rm_messages');
            updateMsgBadge();
            if(getCurrentPageId()==='admin'||getCurrentPageId()==='announcements') renderInbox();
            checkRmReply();
          }
        }
      });

      // Real-time listeners for small array collections (users, call_logs, mf_business)
      ['call_logs','users','mf_business','meeting_agenda','meeting_agenda_archive','learned_fund_names','incentive_config','rm_sales_summary','squareoff'].forEach(key=>{
        fdb.collection('crm_data').doc(key).onSnapshot(doc=>{
          if(doc.exists && doc.data() && doc.data().data){
            const newData = doc.data().data;
            const existing = JSON.parse(localStorage.getItem('dninvest_'+key)||'[]');
            // call_logs & activity_logs are append-only & shared by all RMs:
            // MERGE by id so neither a just-added local entry nor another RM's
            // entry is lost to a stale snapshot.
            if(key==='call_logs' || key==='activity_logs'){
              const byId={};
              existing.forEach(x=>{ if(x&&x.id) byId[x.id]=x; });
              newData.forEach(x=>{ if(x&&x.id) byId[x.id]=x; });
              let merged=Object.values(byId);
              if(key==='activity_logs') merged=merged.sort((a,b)=>String(b.date||'').localeCompare(String(a.date||''))).slice(0,2000);
              if(JSON.stringify(merged)!==JSON.stringify(existing)){
                localStorage.setItem('dninvest_'+key, JSON.stringify(merged));
                console.log('Real-time merge:', key);
                if(key==='call_logs' && getCurrentPageId()==='leads') renderLeadsTable();
                if(getCurrentPageId()==='activity-log') renderActivityLog();
                refreshDash(); updateBadges();
              }
              return;
            }
            if(JSON.stringify(newData) !== JSON.stringify(existing)){
              localStorage.setItem('dninvest_'+key, JSON.stringify(newData));
              console.log('Real-time update:', key);
              if(key==='users'){
                populateRmDropdowns();
                if(getCurrentPageId()==='admin') renderAdmin();
                // Refresh current page if RM got temp access granted
                else if(CU.role!=='admin'){
                  const pg = getCurrentPageId();
                  if(pg==='eq-clients') renderEqTable();
                  else if(pg==='mf-clients') renderMfTable();
                  else if(pg==='eq-followup') renderFollowup('eqf');
                  else if(pg==='mf-followup') renderFollowup('mff');
                  else if(pg==='eq-nocall') renderNoCall('equity');
                  else if(pg==='mf-nocall') renderNoCall('mf');
                  else if(pg==='dashboard') refreshDash();
                }
              }
              if((key==='meeting_agenda'||key==='meeting_agenda_archive') && getCurrentPageId()==='meeting-agenda') renderMeetingAgenda();
              if(key==='incentive_config' || key==='mf_business'){
                const pg=getCurrentPageId();
                if(pg==='mf-txns' && typeof renderMfTxnTable==='function') renderMfTxnTable();
                if(pg==='eq-demat' && typeof renderEqDematTable==='function') renderEqDematTable();
              }
              if(key==='squareoff' && getCurrentPageId()==='eq-squareoff' && typeof renderSquareoff==='function') renderSquareoff();
              refreshDash(); updateBadges();
            }
          }
        });
      });

      // Real-time listener for eq_risk (Broker RMS Risk Val / Ac Bal).
      // Stored in Firestore as a COMPACT payload {codeJson:string, updated, count}
      // to dodge the 40K-index-entries-per-doc limit — rehydrate back to the
      // normal {code:{...}, updated, count} shape before caching, same as the
      // initial load does, so getEqRisk()/eqRiskFor() need no changes.
      fdb.collection('crm_data').doc('eq_risk').onSnapshot(doc=>{
        if(!(doc.exists && doc.data() && doc.data().data)) return;
        const d = doc.data().data;
        let codeObj={};
        if(d && typeof d.codeJson==='string'){ try{ codeObj=JSON.parse(d.codeJson)||{}; }catch(_){ codeObj={}; } }
        else if(d && d.code && typeof d.code==='object'){ codeObj=d.code; }
        const norm={ code:codeObj, updated:(d&&d.updated)||'', count:(d&&d.count)||Object.keys(codeObj).length };
        const existingRaw = localStorage.getItem('dninvest_eq_risk');
        if(existingRaw===JSON.stringify(norm)) return; // no real change
        localStorage.setItem('dninvest_eq_risk', JSON.stringify(norm));
        console.log('Real-time update: eq_risk (compact,', norm.count, 'clients)');
        if(typeof clearEqRiskCache==='function') clearEqRiskCache();
        if(getCurrentPageId()==='eq-clients' && typeof renderEqTable==='function') renderEqTable();
      });

      // Real-time listeners for eq_clients/mf_clients arrays.
      // Skip updates that originate from this client's own pending (unconfirmed)
      // write, AND skip while a setClient/deleteClient/setClientsBulk write is
      // in flight (DB._writing[key] > 0) to avoid a stale Firestore snapshot
      // overwriting localStorage between our read and our write.
      // Re-render whatever page is open after a real-time change lands.
      const _afterRealtime = (key)=>{
        console.log('Real-time update:', key);
        if(key==='eq_clients' && getCurrentPageId()==='eq-clients') renderEqTable();
        if(key==='eq_clients' && getCurrentPageId()==='eq-followup') renderFollowup('eqf');
        if(key==='eq_clients' && getCurrentPageId()==='eq-notrade') renderNoTrade();
        if(key==='eq_clients' && getCurrentPageId()==='eq-nocall') renderNoCall('equity');
        if(key==='mf_clients' && getCurrentPageId()==='mf-clients') renderMfTable();
        if(key==='mf_clients' && getCurrentPageId()==='mf-followup') renderFollowup('mff');
        if(key==='mf_clients' && getCurrentPageId()==='mf-nocall') renderNoCall('mf');
        if(key==='mf_clients' && getCurrentPageId()==='mf-sip') renderSip();
        if(key==='leads' && getCurrentPageId()==='leads') renderLeadsTable();
        if(key==='seminars' && getCurrentPageId()==='seminars') renderSeminarsTable();
        if(getCurrentPageId()==='dashboard') refreshDash();
        updateBadges();
      };

      ['eq_clients','mf_clients','leads','seminars'].forEach(key=>{
        // ── sharded key: one listener per shard, merged back together ──
        if(DB._isSharded(key)){
          const n = SHARD_CFG[key];
          if(!DB._shardCache[key]) DB._shardCache[key] = Array.from({length:n},()=>[]);
          if(!DB._shardSeen[key])  DB._shardSeen[key]  = new Set();
          for(let i=0;i<n;i++){
            DB._shardRef(key,i).onSnapshot(doc=>{
              if(doc.metadata.hasPendingWrites) return;
              if(DB._writing[key]>0) return;
              if(!(doc.exists && doc.data() && Array.isArray(doc.data().data))) return;
              DB._shardCache[key][i] = doc.data().data;
              DB._shardSeen[key].add(i);
              // Don't publish a half-loaded picture: wait until every shard
              // has reported in at least once.
              if(DB._shardSeen[key].size < n) return;
              const merged   = DB._mergeShards(key);
              // Fast change check: compare count first, then spot-check a few items.
              // Full JSON.stringify of 3000 clients is expensive — avoid it.
              const existingRaw = localStorage.getItem('dninvest_'+key)||'[]';
              const existingLen = (existingRaw.match(/\{/g)||[]).length;
              if(existingLen === merged.length){
                // Same count — spot-check first and last item ids
                try{
                  const ex = JSON.parse(existingRaw);
                  if(ex[0]&&merged[0]&&ex[0].id===merged[0].id&&ex[ex.length-1]&&merged[merged.length-1]&&ex[ex.length-1].id===merged[merged.length-1].id) return;
                }catch(e){}
              }
              localStorage.setItem('dninvest_'+key, JSON.stringify(merged));
              if(DB._mem) DB._mem[key] = merged;
              _afterRealtime(key);
            });
          }
          return;
        }
        fdb.collection('crm_data').doc(key).onSnapshot(doc=>{
          if(doc.metadata.hasPendingWrites) return;
          if(DB._writing[key]>0) return;
          if(doc.exists && doc.data() && doc.data().data){
            const newData = doc.data().data;
            const existing = JSON.parse(localStorage.getItem('dninvest_'+key)||'[]');
            if(JSON.stringify(newData) !== JSON.stringify(existing)){
              localStorage.setItem('dninvest_'+key, JSON.stringify(newData));
              _afterRealtime(key);
            }
          }
        });
      });
    }).catch(()=>{ DB.load(); refreshDash(); updateBadges(); populateRmDropdowns(); checkAnnouncement(); checkFollowupAlert(); updateMsgBadge(); checkRmReply(); });
  } else {
    DB.load(); refreshDash(); updateBadges(); populateRmDropdowns(); checkAnnouncement(); checkFollowupAlert();
  }

  const segs = CU.segments||[];
  const hasEq = segs.includes('equity');
  const hasMf = segs.includes('mf');

  document.getElementById('uLabel').textContent = CU.name;
  const si = document.getElementById('segBadge');
  if(hasEq&&hasMf){si.textContent='Equity + MF';si.className='seg-indicator seg-both';}
  else if(hasEq){si.textContent='Equity';si.className='seg-indicator seg-eq';}
  else {si.textContent='MF';si.className='seg-indicator seg-mf';}

  // Show/hide nav sections
  const eqSec = document.getElementById('eq-nav-section');
  const mfSec = document.getElementById('mf-nav-section');
  const adminSec = document.getElementById('admin-nav-section');
  ['eq-clients','eq-followup','eq-notrade','eq-nocall'].forEach(id=>{
    const el=document.getElementById('nav-'+id);
    if(el) el.style.display=hasEq?'flex':'none';
  });
  if(eqSec) eqSec.style.display=hasEq?'':'none';
  ['mf-clients','mf-followup','mf-sip','mf-nocall','mf-txns'].forEach(id=>{
    const el=document.getElementById('nav-'+id);
    if(el) el.style.display=hasMf?'flex':'none';
  });
  if(mfSec) mfSec.style.display=hasMf?'':'none';
  if(adminSec) adminSec.style.display=CU.role==='admin'?'':'none';
  const adminNav=document.getElementById('nav-admin');
  if(adminNav) adminNav.style.display=CU.role==='admin'?'flex':'none';
  const annNav=document.getElementById('nav-announcements');
  if(annNav) annNav.style.display=CU.role==='admin'?'flex':'none';
  const quizNav=document.getElementById('nav-seminar-quiz');
  if(quizNav) quizNav.style.display=CU.role==='admin'?'flex':'none';
  const dupNav=document.getElementById('nav-duplicates');
  if(dupNav) dupNav.style.display=CU.role==='admin'?'flex':'none';

  // MF Desk — a scoped "mini admin" role for MF back-office staff. They can see
  // and enter MF Transactions for ANY RM (to backfill ones RMs forget to log),
  // but get no other access — everything else in the sidebar stays hidden.
  if(CU.role==='mf_desk'){
    ['nav-dashboard','nav-leads','nav-seminars',
     'nav-mf-clients','nav-mf-followup','nav-mf-sip','nav-mf-nocall',
     'nav-reports','nav-activity-log'].forEach(id=>{
      const el=document.getElementById(id);
      if(el) el.style.display='none';
    });
    const mfTxnNav=document.getElementById('nav-mf-txns');
    if(mfTxnNav) mfTxnNav.style.display='flex';
  }

  // Back Office — an office-wide upload/import role. Can bulk-import Equity
  // clients and MF investors (AUM/SIP/contact Excel uploads), and view Square-off
  // (T+5) via the header/mobile shortcut. No Demat Opening, MF Transactions,
  // Leads, Brokerage, or any other page, and no add/edit/delete power anywhere
  // (those buttons are separately hidden for this role below).
  if(CU.role==='backoffice'){
    ['nav-dashboard','nav-leads','nav-seminars',
     'nav-eq-followup','nav-eq-notrade','nav-eq-nocall','nav-eq-demat',
     'nav-mf-followup','nav-mf-sip','nav-mf-nocall','nav-mf-txns',
     'nav-other-products','nav-reports','nav-activity-log',
     'nav-announcements','nav-admin','nav-duplicates'].forEach(id=>{
      const el=document.getElementById(id);
      if(el) el.style.display='none';
    });
    ['main-nav-section','op-nav-section','reports-nav-section'].forEach(id=>{
      const el=document.getElementById(id);
      if(el) el.style.display='none';
    });
    // nav-eq-clients and nav-mf-clients stay visible — that's where the
    // Import Excel (Equity) and AUM/SIP/Contact upload (MF) buttons live.
    const eqClientsNav=document.getElementById('nav-eq-clients');
    if(eqClientsNav) eqClientsNav.style.display='flex';
    const mfClientsNav=document.getElementById('nav-mf-clients');
    if(mfClientsNav) mfClientsNav.style.display='flex';
  }

  // Messages nav — only for RMs (not admin, not Back Office — not a page they use)
  const msgNav = document.getElementById('nav-msg-admin');
  const msgSec = document.getElementById('msg-admin-nav-section');
  const showMsgNav = CU.role!=='admin' && CU.role!=='backoffice';
  if(msgNav) msgNav.style.display = showMsgNav ? 'flex' : 'none';
  if(msgSec) msgSec.style.display = showMsgNav ? '' : 'none';
  updateMsgBadge();
  // Show Change PIN/Password button for all users
  const credBtn = document.getElementById('changeCredBtn');
  if(credBtn) credBtn.style.display = '';

  // HR Portal shortcut button — visible only to admin (top-right corner)
  const hrBtn = document.getElementById('hrPortalTopBtn');
  if(hrBtn) hrBtn.style.display = '';

  // Demat A/c Opening nav item — visible when user has Equity access
  const dematNav = document.getElementById('nav-eq-demat');
  if(dematNav) dematNav.style.display = hasEq ? 'flex' : 'none';

  // Bulk Excel import is admin-only; RMs with risk_upload or backoffice_access
  // permission also get it. Back Office (role or additive access) gets Equity
  // Import Excel + MF Import (AUM/SIP/Contact) mapped to ALL clients.
  const eqImportBtn=document.getElementById('eqImportBtn');
  const canImport = CU.role==='admin' || CU.risk_upload===true || CU.role==='backoffice' || CU.backoffice_access===true;
  if(eqImportBtn){
    eqImportBtn.style.display = canImport ? 'inline-flex' : 'none';
    // Relabel for risk-only RMs (no broader Back Office access) so it's clear what they can upload
    if(CU.role!=='admin' && CU.role!=='backoffice' && !CU.backoffice_access && CU.risk_upload===true){ eqImportBtn.innerHTML='📥 Upload Risk File'; }
  }
  const eqBulkBtn=document.getElementById('eqBulkBtn');
  if(eqBulkBtn) eqBulkBtn.style.display=CU.role==='admin'?'inline-flex':'none';
  const mfBulkDobBtn=document.getElementById('mfBulkDobBtn');
  if(mfBulkDobBtn) mfBulkDobBtn.style.display=CU.role==='admin'?'inline-flex':'none';
  const eqFixMobileBtn=document.getElementById('eqFixMobileBtn');
  if(eqFixMobileBtn) eqFixMobileBtn.style.display=CU.role==='admin'?'inline-flex':'none';
  const eqFixStatusBtn=document.getElementById('eqFixStatusBtn');
  if(eqFixStatusBtn) eqFixStatusBtn.style.display=CU.role==='admin'?'inline-flex':'none';
  const mfImportBtn=document.getElementById('mfImportBtn');
  if(mfImportBtn) mfImportBtn.style.display=isBackOfficeOrAdmin()?'inline-flex':'none';
  const leadsImportBtn=document.getElementById('leadsImportBtn');
  if(leadsImportBtn) leadsImportBtn.style.display=CU.role==='admin'?'inline-flex':'none';
  document.querySelectorAll('.admin-only-inc').forEach(el=>el.style.display=CU.role==='admin'?'inline-flex':'none');
  // Back Office: upload + view only — no manual Add Client/Investor
  const eqAddBtn=document.getElementById('eqAddClientBtn');
  if(eqAddBtn) eqAddBtn.style.display=CU.role==='backoffice'?'none':'';
  const mfAddBtn=document.getElementById('mfAddClientBtn');
  if(mfAddBtn) mfAddBtn.style.display=CU.role==='backoffice'?'none':'';
  document.querySelectorAll('.admin-only-sq').forEach(el=>el.style.display=canUploadSquareoff()?'inline-flex':'none');

  document.getElementById('dash-date').textContent = new Date().toLocaleDateString('en-IN',{weekday:'long',day:'numeric',month:'long',year:'numeric'});

  populateRmDropdowns();
  refreshDash();
  updateBadges();

  // MF Desk lands directly on MF Transactions — Dashboard isn't relevant to
  // this scoped role and its nav item is hidden, so don't leave them on it.
  if(CU.role==='mf_desk') showPage('mf-txns');
  if(CU.role==='backoffice'){
    showPage('eq-clients');
    ['mnav-dashboard','mnav-fu','mnav-ann'].forEach(id=>{
      const el=document.getElementById(id);
      if(el) el.style.display='none';
    });
  }
}

function getTempAccessDealers(seg){
  // Always fetch fresh from DB (not CU, which may be stale)
  const users = DB.get('users')||[];
  const me = users.find(u=>u.id===CU.id);
  if(!me || !me.tempAccess || !me.tempAccess.length) return [];
  const today = new Date().toISOString().split('T')[0];
  const valid = me.tempAccess.filter(t=>t.expiry>=today);
  if(!valid.length) return [];
  const extraDealers = [];
  const seen = new Set();
  valid.forEach(t=>{
    const absentUser = users.find(u=>u.id===t.absentUserId);
    if(!absentUser) return;
    const dealers = seg==='eq' ? (absentUser.eq_dealers||[absentUser.name]) : (absentUser.mf_dealers||[absentUser.name]);
    dealers.forEach(d=>{
      const key=String(d||'').trim().toUpperCase();
      // Do absent RM ka same dealer ho to ek hi baar (pehle duplicate aata tha)
      if(!key || seen.has(key)) return;
      seen.add(key); extraDealers.push(key);
    });
  });
  return extraDealers;
}

function getMyEqClients(){
  const all = DB.get('eq_clients')||[];
  if(CU.role==='admin') return all;
  const dealers=(CU.eq_dealers||[CU.name]).map(d=>d.trim().toUpperCase());
  const tempDealers = getTempAccessDealers('eq');
  const allDealers = [...new Set([...dealers,...tempDealers])];
  return all.filter(c=>allDealers.includes((c.rm||'').trim().toUpperCase()));
}
// Helper - active eq clients (Closed exclude)
function getActiveEqClients(){
  return getMyEqClients().filter(c=>c.status!=='Closed');
}
function getMyMfClients(){
  const all = DB.get('mf_clients')||[];
  if(CU.role==='admin') return all;
  const dealers=(CU.mf_dealers||[CU.name]).map(d=>d.trim().toUpperCase());
  const tempDealers = getTempAccessDealers('mf');
  const allDealers = [...new Set([...dealers,...tempDealers])];
  return all.filter(c=>allDealers.includes((c.rm||'').trim().toUpperCase()));
}
function getMyLeads(){
  const all = DB.get('leads')||[];
  if(CU.role==='admin') return all;
  const dealers=[...new Set([...(CU.eq_dealers||[CU.name]),...(CU.mf_dealers||[CU.name])])].map(d=>d.trim().toUpperCase());
  return all.filter(c=>dealers.includes((c.rm||'').trim().toUpperCase()));
}
function getAllRMs(){
  return (DB.get('users')||[]).filter(u=>u.active!==false).map(u=>u.name);
}
function getSegRMs(seg){
  // Excludes 'mf_desk' and 'backoffice' — both are back-office roles, not actual
  // RMs clients get assigned to, so they must never show up in RM-assignment
  // dropdowns (Lead RM, Attendee RM, client RM, etc.) anywhere in the app.
  return (DB.get('users')||[]).filter(u=>u.active!==false && u.role!=='mf_desk' && u.role!=='backoffice' && (u.segments||[]).includes(seg)).map(u=>u.name);
}

// RM ke apne dealer names (ek RM ke ek se zyada dealer codes ho sakte hain)
// Temp access ko RM-wise group karta hai: har absent RM ka EK option, jo uske
// saare dealer names (PUJA / PUJA MADAM / PUJA/MEGHA ...) ko cover karta hai.
function getTempAccessGroups(seg){
  const users = DB.get('users')||[];
  const me = users.find(u=>u.id===CU.id);
  if(!me || !me.tempAccess || !me.tempAccess.length) return [];
  const today = new Date().toISOString().split('T')[0];
  const groups=[];
  me.tempAccess.filter(t=>t.expiry>=today).forEach(t=>{
    const au = users.find(u=>u.id===t.absentUserId);
    if(!au) return;
    const raw = seg==='eq' ? au.eq_dealers : au.mf_dealers;
    const list = (Array.isArray(raw) && raw.length) ? raw : [au.name];
    const dealers=[...new Set(list.map(d=>String(d||'').trim().toUpperCase()).filter(Boolean))];
    if(dealers.length) groups.push({name:au.name||dealers[0], dealers});
  });
  return groups;
}

// Client ka RM selected filter se match karta hai? __G__: wale option ek se
// zyada dealer names ko cover karte hain.
function rmMatches(cRm, sel){
  const v=String(cRm||'').trim().toUpperCase();
  if(String(sel).startsWith('__G__:')) return String(sel).slice(6).split('|').includes(v);
  return v===String(sel).trim().toUpperCase();
}

function getMyDealerNames(seg){
  const raw = seg==='eq' ? CU.eq_dealers : CU.mf_dealers;
  // Khaali array bhi truthy hota hai — isliye length check zaroori, warna
  // apna naam list se gayab ho jata tha.
  const list = (Array.isArray(raw) && raw.length) ? raw : [CU.name];
  const out=[], seen=new Set();
  list.forEach(d=>{ const v=String(d||'').trim(); const k=v.toUpperCase();
    if(v && !seen.has(k)){ seen.add(k); out.push(v); } });
  return out;
}

function populateRmDropdowns(){
  const isAdmin = CU.role==='admin';
  // Admin gets a "No RM" filter to find unassigned clients and shift them
  const noRmOpt = isAdmin ? '<option value="__NONE__">— No RM (Unassigned) —</option>' : '';
  const esc = s => String(s).replace(/"/g,'&quot;');

  // Admin ke liye purana behaviour: har RM ka naam.
  // RM/desk ke liye: apna EK option + har temp RM ka EK option (uske saare
  // dealer names andar group ho jate hain, isliye PUJA MADAM / PUJA/MEGHA
  // jaisi variants alag se list me nahi aati).
  function build(seg, elId){
    const el=document.getElementById(elId);
    if(!el) return;
    let html='<option value="">All RMs</option>'+noRmOpt;
    if(isAdmin){
      const seen=new Set();
      getSegRMs(seg==='eq'?'equity':'mf').forEach(r=>{
        const k=String(r||'').trim().toUpperCase();
        if(!k||seen.has(k)) return; seen.add(k);
        html+=`<option value="${esc(r)}">${escapeHtml(String(r))}</option>`;
      });
      el.innerHTML=html; el.disabled=false;
      return;
    }
    const mine=getMyDealerNames(seg);
    const myVal='__G__:'+mine.map(d=>d.toUpperCase()).join('|');
    html+=`<option value="${esc(myVal)}">${escapeHtml(CU.name||mine[0]||'Me')}</option>`;
    const groups=getTempAccessGroups(seg);
    groups.forEach(g=>{
      html+=`<option value="${esc('__G__:'+g.dealers.join('|'))}">${escapeHtml(g.name)} 🔄 (temp)</option>`;
    });
    el.innerHTML=html;
    // Temp access ho to dropdown khula (default All = apna + temp), warna lock.
    if(groups.length){ el.disabled=false; el.value=''; }
    else { el.value=myVal; el.disabled=true; }
  }
  build('eq','eq-rm');
  build('mf','mf-rm');

  const le=document.getElementById('leads-rm');
  if(le){
    let html='<option value="">All RMs</option>'+noRmOpt;
    if(isAdmin){
      const seen=new Set();
      [...getSegRMs('equity'),...getSegRMs('mf')].forEach(r=>{
        const k=String(r||'').trim().toUpperCase();
        if(!k||seen.has(k)) return; seen.add(k);
        html+=`<option value="${esc(r)}">${escapeHtml(String(r))}</option>`;
      });
      le.innerHTML=html; le.disabled=false;
    } else {
      const mine=[...new Set([...getMyDealerNames('eq'),...getMyDealerNames('mf')].map(d=>d.toUpperCase()))];
      const myVal='__G__:'+mine.join('|');
      html+=`<option value="${esc(myVal)}">${escapeHtml(CU.name||mine[0]||'Me')}</option>`;
      const seen=new Set();
      [...getTempAccessGroups('eq'),...getTempAccessGroups('mf')].forEach(g=>{
        const k=String(g.name).toUpperCase();
        if(seen.has(k)) return; seen.add(k);
        html+=`<option value="${esc('__G__:'+g.dealers.join('|'))}">${escapeHtml(g.name)} 🔄 (temp)</option>`;
      });
      le.innerHTML=html;
      if(seen.size){ le.disabled=false; le.value=''; }
      else { le.value=myVal; le.disabled=true; }
    }
  }
}

// ══════════════════════════════════════════
// NAVIGATION
// ══════════════════════════════════════════
function getCurrentPageId(){
  const el = document.querySelector('.page.active');
  return el ? el.id.replace('page-','') : '';
}
function showPage(id){
  if(typeof BULK!=='undefined' && BULK.clearAll) BULK.clearAll();
  if(typeof MFTBULK!=='undefined' && MFTBULK.clearSel) MFTBULK.clearSel();
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n=>n.classList.remove('active'));
  document.getElementById('page-'+id).classList.add('active');
  const ni=document.getElementById('nav-'+id);
  if(ni) ni.classList.add('active');
  updateMobileNav(id);

  if(id==='eq-clients') renderEqTable();
  else if(id==='mf-clients') renderMfTable();
  else if(id==='leads') renderLeadsTable();
  else if(id==='seminars'){ renderSeminarsTable(); if(typeof QUIZ!=='undefined') QUIZ.initLinkControl(); }
  else if(id==='eq-followup'){ renderFollowup('eqf'); }
  else if(id==='mf-followup'){ renderFollowup('mff'); }
  else if(id==='eq-notrade') renderNoTrade();
  else if(id==='eq-squareoff') renderSquareoff();
  else if(id==='eq-nocall') renderNoCall('equity');
  else if(id==='mf-nocall') renderNoCall('mf');
  else if(id==='mf-sip') renderSip();
  else if(id==='mf-txns'){ renderMfTxnPage(); }
  else if(id==='eq-demat'){ renderEqDematPage(); }
  else if(id==='reports') renderReports();
  else if(id==='activity-log') renderActivityLog();
  else if(id==='duplicates') DUP.scan();
  else if(id==='admin'){ renderAdmin(); populateCallLimitInputs(); }
  else if(id==='announcements'){ renderAnnouncementAdmin(); renderInbox(); populateOfferTarget(); renderOffersAdmin(); renderCommHistory(); const of=document.getElementById('offer-from'); if(of && !of.value) of.value=today(); }
  else if(id==='rm-messages') renderRmMessages();
  else if(id==='dashboard') refreshDash();
  else if(id==='other-products') renderOpPage();

  closeSidebar();
}

function toggleSidebar(){
  document.getElementById('sidebar').classList.toggle('open');
  document.getElementById('sidebarOverlay').classList.toggle('open');
}

// Mobile bottom-nav active state — maps current page to the right tab
function updateMobileNav(id){
  const map={
    'dashboard':'mnav-dashboard',
    'eq-clients':'mnav-eq','eq-notrade':'mnav-eq','eq-nocall':'mnav-eq','eq-demat':'mnav-eq',
    'eq-squareoff':'mnav-sq',
    'eq-followup':'mnav-fu','mf-followup':'mnav-fu',
    'mf-clients':'mnav-mf','mf-sip':'mnav-mf','mf-nocall':'mnav-mf','mf-txns':'mnav-mf',
    'announcements':'mnav-ann'
  };
  document.querySelectorAll('.mnav-item').forEach(b=>b.classList.remove('active'));
  const el=document.getElementById(map[id]||'');
  if(el) el.classList.add('active');
}
function toggleDashCard(id){
  const el=document.getElementById(id);
  if(el) el.classList.toggle('open');
}
function closeSidebar(){
  document.getElementById('sidebar').classList.remove('open');
  document.getElementById('sidebarOverlay').classList.remove('open');
}

function switchTab(group, tab){
  const tabBar = document.querySelector(`[onclick="showPage('${
    group==='eqf'?'eq-followup':group==='mff'?'mf-followup':
    group==='nt'?'eq-notrade':group==='rpt'?'reports':''
  }')"]`)?.closest('.page')?.querySelector('.tab-bar');

  // Generic tab switch
  const container = document.getElementById(group+'-content') || document.getElementById('rpt-'+tab);
  if(group==='eqf'){ activeEqfTab=tab; renderFollowup('eqf'); }
  else if(group==='mff'){ activeMffTab=tab; renderFollowup('mff'); }
  else if(group==='nt'){ activeNtTab=tab; renderNoTrade(); }
  else if(group==='eqnc'){ activeEqncTab=tab; renderNoCall('equity'); }
  else if(group==='mfnc'){ activeMfncTab=tab; renderNoCall('mf'); }
  else if(group==='dup'){
    document.querySelectorAll('#page-duplicates .tab-item').forEach((el,i)=>{
      el.classList.toggle('active',['eq','mf'][i]===tab);
    });
    document.querySelectorAll('#page-duplicates .tab-panel').forEach(p=>p.classList.remove('active'));
    const tp=document.getElementById('dup-'+tab);
    if(tp) tp.classList.add('active');
    DUP.activeTab=tab; DUP.renderBar();
    return;
  }
  else if(group==='rpt'){
    activeRptTab=tab;
    document.querySelectorAll('#page-reports .tab-item').forEach((el,i)=>{
      el.classList.toggle('active',['equity','mf','combined'][i]===tab);
    });
    document.querySelectorAll('#page-reports .tab-panel').forEach(p=>p.classList.remove('active'));
    const tp=document.getElementById('rpt-'+tab);
    if(tp) tp.classList.add('active');
    return;
  }
  // Update tab highlight
  const allTabs = document.querySelectorAll(`[onclick^="switchTab('${group}"]`);
  allTabs.forEach(el=>el.classList.toggle('active', el.getAttribute('onclick').includes("'"+tab+"'")));
}

// ══════════════════════════════════════════
// DASHBOARD
// ══════════════════════════════════════════
function hardRefresh(){
  if('caches' in window){
    caches.keys().then(function(names){
      names.forEach(function(name){ caches.delete(name); });
    }).then(function(){
      window.location.reload(true);
    });
  } else {
    window.location.reload(true);
  }
}

// ── Birthday "Wish All" stepper: opens each client's WhatsApp chat one-by-one ──
let _bdayQueue=[], _bdayStep=0, _bdayCards=[], _bdayMeta=[];

// ── 🎂 Birthday wish SENT stamp ──
// Jab RM Wish/Card bhejta hai, client record pe {y, by, at, via} stamp lag jaata
// hai. Ye Firestore me sync hota hai → Admin ke dashboard pe turant "✅ Sent by
// RIYA" dikh jaata hai. Har saal ka alag (y = year), to agle saal reset.
async function markBdayWished(idx, via){
  const m = _bdayMeta[idx];
  if(!m || !m.id) return;
  const list = DB.get(m.key)||[];
  const ex = list.find(c=>c.id===m.id);
  if(!ex) return;
  const yr = parseInt(today().slice(0,4),10);
  const already = ex.bday_wish && ex.bday_wish.y===yr;
  const stamp = {y:yr, by:CU.name, at:new Date().toISOString(), via:via||'wish'};
  ex.bday_wish = stamp;
  ex.updated = today();
  try{ await DB.setClient(m.key, ex); }catch(e){ console.log('bday stamp sync error',e); }
  if(!already){
    try{
      await DB.addActivityLog([{
        id: uid(), type:'birthday_wish', seg: m.key==='eq_clients'?'equity':'mf',
        client_id: ex.id, client_name: ex.name, rm: ex.rm||'',
        by: CU.name, date: stamp.at,
        changes: [{field:'Birthday Wish', old:'—', new:(via==='card'?'Card bheja':'WhatsApp wish bheja')}]
      }]);
    }catch(e){}
  }
  refreshDash();
}

function wishAllBirthdays(){
  const btn=document.getElementById('wishAllBtn');
  if(!_bdayQueue.length) return;
  if(_bdayStep>=_bdayQueue.length) _bdayStep=0;   // allow restart
  const c=_bdayQueue[_bdayStep];
  window.open(c.url,'_blank','noopener');
  if(c.idx!==undefined) markBdayWished(c.idx,'wish');
  _bdayStep++;
  if(btn){
    if(_bdayStep>=_bdayQueue.length){
      btn.innerHTML='✅ All opened — click to restart';
    } else {
      const nx=_bdayQueue[_bdayStep];
      btn.innerHTML=`Next ▶ ${escapeHtml((nx.name||'').split(' ')[0]||nx.name)} (${_bdayStep+1}/${_bdayQueue.length})`;
    }
  }
}

// ── 🎨 Birthday CARD IMAGE for the CLIENT (send on WhatsApp) ──
// Ivory + brand-teal design, no gold. Vector balloons, ribbon confetti.
function birthdayCardImage(idx){
  const rec=_bdayCards[idx]||{};
  const name=(rec.name||'Friend').trim();
  const num=rec.num||'';
  const first=name.split(' ')[0]||name;
  const W=1080,H=1080;
  const cv=document.createElement('canvas'); cv.width=W; cv.height=H; const g=cv.getContext('2d');
  const TEAL='#0d9488', TEAL_D='#0a5f58', CORAL='#F0714E', NAVY='#12203B', INK='#5A6273';
  function rr(x,y,w,h,r){g.beginPath();g.moveTo(x+r,y);g.arcTo(x+w,y,x+w,y+h,r);g.arcTo(x+w,y+h,x,y+h,r);g.arcTo(x,y+h,x,y,r);g.arcTo(x,y,x+w,y,r);g.closePath();}
  // warm ivory background
  const bg=g.createLinearGradient(0,0,W*0.5,H);
  bg.addColorStop(0,'#FFFDF7'); bg.addColorStop(1,'#F3EFE4');
  g.fillStyle=bg; g.fillRect(0,0,W,H);
  const glow=g.createRadialGradient(W/2,H*0.44,60,W/2,H*0.44,H*0.7);
  glow.addColorStop(0,'rgba(13,148,136,0.07)'); glow.addColorStop(1,'rgba(13,148,136,0)');
  g.fillStyle=glow; g.fillRect(0,0,W,H);
  // faint diagonal texture, washed back
  g.strokeStyle='rgba(13,148,136,.10)'; g.lineWidth=1;
  for(let d=-H; d<W; d+=26){ g.beginPath(); g.moveTo(d,0); g.lineTo(d+H,H); g.stroke(); }
  g.fillStyle='rgba(255,253,247,.82)'; g.fillRect(0,0,W,H);
  // vector balloons
  function balloon(x,y,rw,rh,c1,c2,tilt){
    g.save(); g.translate(x,y); g.rotate(tilt);
    const q=g.createLinearGradient(-rw,-rh,rw,rh); q.addColorStop(0,c1); q.addColorStop(1,c2);
    g.fillStyle=q; g.beginPath(); g.ellipse(0,0,rw,rh,0,0,7); g.fill();
    g.fillStyle='rgba(255,255,255,.35)'; g.beginPath(); g.ellipse(-rw*.32,-rh*.36,rw*.20,rh*.28,-0.5,0,7); g.fill();
    g.fillStyle=c2; g.beginPath(); g.moveTo(-9,rh); g.lineTo(9,rh); g.lineTo(0,rh+16); g.closePath(); g.fill();
    g.strokeStyle='rgba(18,32,59,.30)'; g.lineWidth=2; g.beginPath(); g.moveTo(0,rh+16);
    g.bezierCurveTo(26,rh+90,-26,rh+150,6,rh+215); g.stroke();
    g.restore();
  }
  balloon(166,238,52,64,'#F58A6A',CORAL,-0.16);
  balloon(272,184,44,55,'#3FBFB0',TEAL,0.12);
  balloon(914,238,52,64,'#3FBFB0',TEAL,0.16);
  balloon(808,184,44,55,'#F58A6A',CORAL,-0.12);
  // ribbon confetti (kept clear of the centre panel)
  function conf(x,y,w,h,c,rot){ g.save(); g.translate(x,y); g.rotate(rot); g.fillStyle=c; g.fillRect(-w/2,-h/2,w,h); g.restore(); }
  const cc=[TEAL,CORAL,'#F4C542','#7C6BE0','#3FBFB0'];
  let _seed=11; const rnd=()=>{ _seed=(_seed*16807)%2147483647; return _seed/2147483647; };
  for(let i=0;i<44;i++){ const x=rnd()*W, y=rnd()*H;
    if(x>130&&x<950&&y>296&&y<836) continue;
    g.globalAlpha=.55+rnd()*.35; conf(x,y,7+rnd()*7,3+rnd()*4,cc[i%5],rnd()*3.14); }
  g.globalAlpha=1;
  // teal frame + coral inner line + corner dots
  g.strokeStyle=TEAL; g.lineWidth=8; rr(40,40,W-80,H-80,44); g.stroke();
  g.strokeStyle=CORAL; g.lineWidth=2; rr(58,58,W-116,H-116,34); g.stroke();
  g.fillStyle=TEAL;
  [[58,58,1,1],[W-58,58,-1,1],[58,H-58,1,-1],[W-58,H-58,-1,-1]].forEach(c=>{
    g.save(); g.translate(c[0],c[1]); g.scale(c[2],c[3]);
    g.beginPath(); g.arc(30,30,5,0,7); g.fill(); g.restore(); });
  // white panel
  g.save(); g.shadowColor='rgba(18,32,59,.16)'; g.shadowBlur=44; g.shadowOffsetY=14;
  g.fillStyle='#FFFFFF'; rr(104,300,W-208,540,32); g.fill(); g.restore();
  g.strokeStyle='rgba(13,148,136,.28)'; g.lineWidth=1.5; rr(104,300,W-208,540,32); g.stroke();
  g.fillStyle=TEAL; rr(104,300,W-208,10,5); g.fill();
  // HAPPY BIRTHDAY, letter-spaced
  g.textAlign='center';
  g.fillStyle=TEAL_D; g.font='500 27px Georgia, serif';
  (function(t,y){ const sp=8; let tot=0,i;
    for(i=0;i<t.length;i++) tot+=g.measureText(t[i]).width+sp; tot-=sp;
    let x=W/2-tot/2; g.textAlign='left';
    for(i=0;i<t.length;i++){ g.fillText(t[i],x,y); x+=g.measureText(t[i]).width+sp; }
    g.textAlign='center'; })('HAPPY BIRTHDAY',396);
  g.strokeStyle='rgba(240,113,78,.75)'; g.lineWidth=2;
  g.beginPath(); g.moveTo(W/2-272,386); g.lineTo(W/2-212,386); g.moveTo(W/2+212,386); g.lineTo(W/2+272,386); g.stroke();
  g.fillStyle=CORAL; [W/2-200,W/2+200].forEach(x=>{ g.beginPath(); g.arc(x,386,4,0,7); g.fill(); });
  // NAME — navy, auto-fit (shrink, then wrap to 2 lines)
  g.fillStyle=NAVY;
  const nm=name.toUpperCase();
  const maxNameW=W-300;
  let fs=76; g.font='500 '+fs+'px Georgia, serif';
  if(g.measureText(nm).width<=maxNameW){
    g.fillText(nm, W/2, 500);
  } else {
    while(g.measureText(nm).width>maxNameW && fs>52){ fs-=2; g.font='500 '+fs+'px Georgia, serif'; }
    if(g.measureText(nm).width<=maxNameW){
      g.fillText(nm, W/2, 500);
    } else {
      const words=nm.split(' '); let l1='',l2='';
      for(const w of words){ const t=(l1?l1+' ':'')+w;
        if(g.measureText(t).width<=maxNameW && !l2) l1=t; else l2=(l2?l2+' ':'')+w; }
      if(!l2){ l2=l1; l1=''; }
      fs=56; g.font='500 '+fs+'px Georgia, serif';
      while((g.measureText(l1).width>maxNameW||g.measureText(l2).width>maxNameW)&&fs>38){ fs-=2; g.font='500 '+fs+'px Georgia, serif'; }
      if(l1){ g.fillText(l1, W/2, 474); g.fillText(l2, W/2, 474+fs+6); }
      else  { g.fillText(l2, W/2, 500); }
    }
  }
  // divider with coral diamond
  g.strokeStyle=TEAL; g.lineWidth=2.5;
  g.beginPath(); g.moveTo(W/2-110,550); g.lineTo(W/2-24,550); g.moveTo(W/2+24,550); g.lineTo(W/2+110,550); g.stroke();
  g.save(); g.translate(W/2,550); g.rotate(Math.PI/4); g.fillStyle=CORAL; g.fillRect(-7,-7,14,14); g.restore();
  // message
  g.fillStyle=INK; g.font='32px Georgia, serif';
  g.fillText('Dear '+first+', wishing you a day filled with joy', W/2, 614);
  g.fillText('and a year of health, happiness', W/2, 658);
  g.fillText('and prosperity.', W/2, 702);
  // brand
  g.strokeStyle='rgba(18,32,59,.12)'; g.lineWidth=1;
  g.beginPath(); g.moveTo(W/2-150,740); g.lineTo(W/2+150,740); g.stroke();
  g.fillStyle=TEAL_D; g.font='500 48px Georgia, serif'; g.fillText('D N INVESTMENT', W/2, 794);
  g.fillStyle='#9AA1AF'; g.font='23px Georgia, serif'; g.fillText('Jamshedpur  ·  Your Trusted Financial Partner', W/2, 826);
  // bottom teal ribbon
  g.fillStyle=TEAL; rr(W/2-320,880,640,58,29); g.fill();
  g.fillStyle='#FFFFFF'; g.font='500 26px Georgia, serif';
  g.fillText('Many Many Happy Returns of the Day', W/2, 918);

  cv.toBlob(function(blob){
    const file=new File([blob], 'Birthday_'+first+'.png', {type:'image/png'});
    const url=URL.createObjectURL(blob);
    const canShare=(navigator.canShare && (function(){ try{ return navigator.canShare({files:[file]}); }catch(e){ return false; } })());
    const ov=document.createElement('div');
    ov.style.cssText='position:fixed;inset:0;z-index:100000;background:rgba(0,0,0,.8);display:flex;flex-direction:column;align-items:center;justify-content:center;gap:14px;padding:16px;overflow:auto';
    ov.innerHTML=
      '<img src="'+url+'" style="max-width:min(90vw,420px);max-height:64vh;border-radius:16px;box-shadow:0 12px 40px rgba(0,0,0,.5)">'+
      '<div style="display:flex;gap:10px;flex-wrap:wrap;justify-content:center">'+
        (num?'<button id="_bcSend" style="padding:12px 20px;border:0;border-radius:10px;background:#25D366;color:#fff;font-weight:800;font-size:15px;cursor:pointer">📲 Send to '+first+'</button>':'')+
        (canShare?'<button id="_bcShare" style="padding:12px 20px;border:0;border-radius:10px;background:#128C7E;color:#fff;font-weight:800;font-size:15px;cursor:pointer">📤 Share</button>':'')+
        '<button id="_bcDl" style="padding:12px 20px;border:0;border-radius:10px;background:#7c3aed;color:#fff;font-weight:800;font-size:15px;cursor:pointer">⬇️ Download</button>'+
        '<button id="_bcCl" style="padding:12px 20px;border:0;border-radius:10px;background:#374151;color:#fff;font-weight:800;font-size:15px;cursor:pointer">✖ Close</button>'+
      '</div>'+
      '<div id="_bcHint" style="color:#e5e7eb;font-size:12.5px;text-align:center;max-width:440px;line-height:1.5">'+
        (num?(isMobileDevice()&&canShare
              ?('Tap “Send to '+first+'” → the share sheet will open, pick <b>WhatsApp</b> and send it to '+first+'. 🎉')
              :('Tap “Send to '+first+'” → the image will be copied and '+first+"'s WhatsApp chat will open. Paste it there with <b>Ctrl+V</b> and send. 🎉"))
            :'This client has no number on file — download the image and send it on WhatsApp instead.')+
      '</div>';
    document.body.appendChild(ov);
    const hint=ov.querySelector('#_bcHint');
    function close(){ URL.revokeObjectURL(url); ov.remove(); }
    ov.querySelector('#_bcCl').onclick=close;
    ov.querySelector('#_bcDl').onclick=function(){ const a=document.createElement('a'); a.href=url; a.download='Birthday_'+first+'.png'; a.click(); };
    const shr=ov.querySelector('#_bcShare');
    if(shr) shr.onclick=async function(){ try{ await navigator.share({files:[file], title:'Happy Birthday', text:'Happy Birthday '+first+'! 🎉🎂 — D N Investment'}); markBdayWished(idx,'card'); }catch(e){} };
    const send=ov.querySelector('#_bcSend');
    if(send) send.onclick=function(){
      // ── MOBILE: clipboard-paste WhatsApp par kaam nahi karta. Native share
      // sheet se image + text seedha client ko chala jata hai — best route.
      if(isMobileDevice() && canShare){
        navigator.share({files:[file], title:'Happy Birthday',
          text:'Happy Birthday '+first+'! 🎉🎂 — D N Investment'})
          .then(function(){ markBdayWished(idx,'card');
            hint.innerHTML='✅ Share sheet me WhatsApp chunkar '+first+' ko bhej do. 🎉'; })
          .catch(function(){});
        return;
      }
      // ── DESKTOP: clipboard.write ko gesture ke andar hi START karo (await
      // nahi) — warna gesture khatam ho jata hai aur wa.me popup block ho
      // jati hai. Isliye copy chalu karke turant window.open, phir hint update.
      let writeP=null;
      try{ writeP=navigator.clipboard.write([new ClipboardItem({'image/png':blob})]); }catch(e){ writeP=Promise.reject(e); }
      const win=window.open('https://wa.me/'+num,'_blank','noopener');
      markBdayWished(idx,'card');
      hint.innerHTML='⏳ Copying image...';
      writeP.then(function(){
        hint.innerHTML = win
          ? ('✅ Image copied and '+first+"'s chat has opened — press <b>Ctrl+V</b> in WhatsApp and send. 🎉")
          : ('✅ Image copied. The popup was blocked — <a href="https://wa.me/'+num+'" target="_blank" style="color:#25D366;font-weight:800">click here</a> to open the chat, then press <b>Ctrl+V</b>. 🎉');
      }).catch(function(){
        try{ const a=document.createElement('a'); a.href=url; a.download='Birthday_'+first+'.png'; a.click(); }catch(e){}
        hint.innerHTML='📥 Copy failed — the image has been downloaded instead. <b>Attach</b> it in '+first+"'s chat and send.";
      });
    };
    ov.addEventListener('click', function(ev){ if(ev.target===ov) close(); });
  }, 'image/png');
}

// MF Invested Amount — Increased vs Decreased dashboard card. Uses the prev_invested/invested_change_amt
// fields stamped whenever an AUM By Client import changes a client's AUM value
// (see the mf import merge logic). Simpler than the Equity Trade Activity card —
// no day-over-day snapshot history, just live counts + amounts from the last import.
function renderMfAumTrend(){
  const el = document.getElementById('mfAumTrend');
  if(!el) return;
  const mf = getMyMfClients();
  window.__mfAumRows = mf;
  const totalInvested = mf.reduce((s,c)=>s+(parseFloat(c.aum_detail && c.aum_detail.inv)||0),0);
  const td = today();
  const changed = mfChangeLogRows(td);
  const increased = changed.filter(r=>r.delta>0);
  const decreased = changed.filter(r=>r.delta<0);
  const sumInc = increased.reduce((s,r)=>s+r.delta,0);
  const sumDec = decreased.reduce((s,r)=>s+Math.abs(r.delta),0);
  const isAdmin = CU.role==='admin';
  const cardClick = isAdmin ? 'showMfAumRmSplit()' : 'showMfAumList()';
  const cardTitle = isAdmin ? 'Click for RM-wise breakdown' : 'Click for full list';
  let estimatedLeftoverCount = 0;
  if(isAdmin){
    try{ estimatedLeftoverCount = (JSON.parse(localStorage.getItem('dninvest_mf_change_log')||'[]')||[]).filter(x=>x&&x.estimated===true).length; }catch(e){}
  }
  const footerNote = isAdmin
    ? `📅 ${fmtDate(today())} · today's changes from the AUM By Client import · click card for RM-wise split · <span style="text-decoration:underline;cursor:pointer" onclick="event.stopPropagation();showMfAumList()">full list</span> · <span style="text-decoration:underline;cursor:pointer" onclick="event.stopPropagation();showMfAumHistory()">date-wise history</span>`
      + (estimatedLeftoverCount ? `<br><span style="text-decoration:underline;color:#92400e;cursor:pointer" onclick="event.stopPropagation();removeEstimatedMfChangeEntries()" title="Remove old approximate (guess-based) entries">🧹 ${estimatedLeftoverCount} approximate entries — click to remove</span>` : '')
    : `📅 ${fmtDate(today())} · today's changes from the AUM By Client import · click card for full list · <span style="text-decoration:underline;cursor:pointer" onclick="event.stopPropagation();showMfAumHistory()">date-wise history</span>`;
  const zeroBalance = mf.filter(c=>!(parseFloat(c.aum)||0));
  el.innerHTML = `
    <div class="dash-stat-grid dash-stat-grid-4" onclick="${cardClick}" title="${cardClick}">
      <div class="dash-stat-box" style="background:#eef3fc;border:1.5px solid #bfdbfe">
        <div class="dash-stat-lbl" style="font-size:.58rem;color:var(--gray)">🔵 TOTAL INVESTED</div>
        <div style="font-size:1.3rem;font-weight:900;color:var(--blue);line-height:1.2">₹${fmtNum(totalInvested)}</div>
        <span style="font-size:.62rem;font-weight:700;color:var(--gray)">${mf.length} investors</span>
      </div>
      <div class="dash-stat-box" style="background:#f0fdf4;border:1.5px solid #bbf7d0">
        <div class="dash-stat-lbl" style="font-size:.58rem;color:var(--gray)">🟢 ADDITIONS</div>
        <div style="font-size:1.3rem;font-weight:900;color:var(--green);line-height:1.2">${increased.length}</div>
        <span style="font-size:.62rem;font-weight:700;color:var(--green)">+₹${fmtNum(sumInc)}</span>
      </div>
      <div class="dash-stat-box" style="background:#fef2f2;border:1.5px solid #fecaca">
        <div class="dash-stat-lbl" style="font-size:.58rem;color:var(--gray)">🔴 REDEMPTIONS</div>
        <div style="font-size:1.3rem;font-weight:900;color:var(--red);line-height:1.2">${decreased.length}</div>
        <span style="font-size:.62rem;font-weight:700;color:var(--red)">-₹${fmtNum(sumDec)}</span>
      </div>
      <div class="dash-stat-box" style="background:#f5f3ff;border:1.5px solid #ddd6fe;cursor:pointer" onclick="event.stopPropagation();showMfZeroBalance()" title="Click to see full list">
        <div class="dash-stat-lbl" style="font-size:.58rem;color:#6d28d9">⚪ ZERO BALANCE</div>
        <div style="font-size:1.3rem;font-weight:900;color:#6d28d9;line-height:1.2">${zeroBalance.length}</div>
        <span style="font-size:.62rem;font-weight:700;color:#6d28d9">AUM = 0 / blank</span>
      </div>
    </div>
    <p style="color:var(--gray);font-size:.62rem;margin-top:5px;text-align:center">${footerNote}</p>`;

  // Save today's TRUE (unfiltered — every RM sees only their own scope anyway
  // via getMyMfClients) snapshot for the date-wise history table.
  const scope = isAdmin ? 'ALL' : (CU.id || CU.name);
  DB.addMfAumSnapshot({date:td, scope, additions:increased.length, additionsAmt:sumInc, redemptions:decreased.length, redemptionsAmt:sumDec, totalInvested})
    .catch(e=>console.log('MF AUM snapshot save failed',e));

  // One-time-per-session top-up from Firestore, same reasoning as the Eq card —
  // a device that's never saved a snapshot locally still needs "vs yesterday".
  if(!window.__masSynced){
    window.__masSynced = true;
    DB.fetchMfAumSnapshots().then(()=>{});
    DB.fetchMfChangeLog().then(()=>{});
  }
}

// Zero balance MF investors — AUM = 0 or blank (never invested / fully redeemed)
function showMfZeroBalance(){
  const rows = window.__mfAumRows || [];
  const zero = rows.filter(c=>!(parseFloat(c.aum)||0))
    .sort((a,b)=>(a.name||'').localeCompare(b.name||''));
  if(!zero.length){ toast('No zero-balance investors found','info'); return; }
  showReport(`⚪ Zero Balance MF Investors (${zero.length})`,
    ['Name','RM','Mobile','PAN','Status'],
    zero.map(c=>[c.name||'—', c.rm||'—', c.mobile||'—', c.pan||'—', c.status||'—']));
}

// Reads MF invested-amount changes for a given date from the permanent,
// append-only change log (dninvest_mf_change_log) — NOT from the live
// invested_change_amt/prev_invested fields on mf_clients, which any cleanup
// or re-import can legitimately clear/overwrite. The log is the durable
// source of truth: every real change is written here once and never
// touched again, so dashboard totals stay correct even after a client's
// live fields get reset. Pass rmName to scope to one RM (admin drilldown);
// otherwise scoped to the logged-in user's own dealer names (+ temp access),
// same rule as getMyMfClients — admin sees everything.
function mfChangeLogRows(dateStr, rmName){
  let log=[];
  try{ log = JSON.parse(localStorage.getItem('dninvest_mf_change_log')||'[]'); }catch(e){ log=[]; }
  let rows = log.filter(x=>x && x.date===dateStr);
  if(rmName){
    rows = rows.filter(x=>(x.rm||'').trim().toUpperCase()===String(rmName).trim().toUpperCase());
  } else if(CU.role!=='admin'){
    const dealers=(CU.mf_dealers||[CU.name]).map(d=>d.trim().toUpperCase());
    const tempDealers = getTempAccessDealers('mf');
    const allDealers=[...new Set([...dealers,...tempDealers])];
    rows = rows.filter(x=>allDealers.includes((x.rm||'').trim().toUpperCase()));
  }
  return rows;
}

// One-time cleanup: removes only the approximated/guessed change-log entries
// (flagged estimated:true) that were created while the now-reverted estimate
// logic was briefly live — these were never real purchase/redemption
// amounts, just a rough guess. Precise by design (unlike the earlier live-
// field cleanup attempts): only touches entries explicitly flagged as
// estimated, so genuine exact changes are never at risk. Safe to run once;
// no-ops harmlessly if there's nothing to clean.
async function removeEstimatedMfChangeEntries(){
  if(CU.role!=='admin'){ toast('Admin only','error'); return; }
  let log=[];
  try{ log = JSON.parse(localStorage.getItem('dninvest_mf_change_log')||'[]'); }catch(e){ log=[]; }
  const bad = log.filter(x=>x && x.estimated===true);
  if(!bad.length){ toast('No approximate (estimated) entries found','info'); return; }
  if(!confirm(`Remove ${bad.length} approximate entries? These were guess-based figures, not the real purchase/redemption amount. The next AUM import will produce correct exact figures.`)) return;
  await DB.removeMfChangeLogEntries(bad.map(x=>x.id));
  const badKey = new Set(bad.map(x=>x.clientId+'__'+x.date));
  const mf = DB.get('mf_clients')||[];
  // Only touch the specific records flagged as estimated, and write them via
  // setClientsBulk (transaction merge) instead of a blind full-array DB.set —
  // a blind overwrite here could silently wipe another RM's concurrent edit
  // to a *different* client made while this admin cleanup was running.
  const changed = [];
  mf.forEach(c=>{
    if(c.invested_change_date && badKey.has(c.id+'__'+c.invested_change_date)){
      const clean={...c};
      delete clean.invested_change_amt; delete clean.invested_change_date; delete clean.prev_invested;
      changed.push(clean);
    }
  });
  if(changed.length) await DB.setClientsBulk('mf_clients', changed);
  toast(`✅ ${bad.length} approximate entries removed — refreshing dashboard`,'success');
  setTimeout(()=>location.reload(), 1200);
}

// Same scoping as mfChangeLogRows (own dealers unless admin / rmName given),
// but over an inclusive [fromDate,toDate] range instead of a single day —
// backs the date-range picker in showMfAumList.
function mfChangeLogRange(fromDate, toDate, rmName){
  let log=[];
  try{ log = JSON.parse(localStorage.getItem('dninvest_mf_change_log')||'[]'); }catch(e){ log=[]; }
  let rows = log.filter(x=>x && x.date && x.date>=fromDate && x.date<=toDate);
  if(rmName){
    rows = rows.filter(x=>(x.rm||'').trim().toUpperCase()===String(rmName).trim().toUpperCase());
  } else if(CU.role!=='admin'){
    const dealers=(CU.mf_dealers||[CU.name]).map(d=>d.trim().toUpperCase());
    const tempDealers = getTempAccessDealers('mf');
    const allDealers=[...new Set([...dealers,...tempDealers])];
    rows = rows.filter(x=>allDealers.includes((x.rm||'').trim().toUpperCase()));
  }
  return rows;
}

// Full list of every MF investor whose Invested Amount changed, split into
// two sections: Additions (invested amount increased) and Redemptions
// (invested amount decreased) — never mixed into one table. Defaults to
// Today; user can widen the range with the date pickers and hit Apply.
function showMfAumList(fromDate, toDate){
  const td = today();
  fromDate = fromDate || td;
  toDate = toDate || td;
  const changed = mfChangeLogRange(fromDate, toDate).sort((a,b)=>Math.abs(b.delta)-Math.abs(a.delta));
  if(!changed.length){ toast('No Invested Amount change record for this range — this will appear here after an AUM By Client import','info'); return; }
  const additions = changed.filter(r=>r.delta>0);
  const redemptions = changed.filter(r=>r.delta<0);
  const sumInc = additions.reduce((s,r)=>s+r.delta,0);
  const sumDec = redemptions.reduce((s,r)=>s+Math.abs(r.delta),0);
  const showDateCol = fromDate!==toDate;

  const rowHtml = r => `<tr><td>${escapeHtml(r.name||'—')}</td><td>${escapeHtml(r.rm||'—')}</td><td>₹${fmtNum(r.prevInvested||0)}</td><td>₹${fmtNum(r.newInvested||0)}</td><td style="font-weight:700;color:${r.delta>0?'var(--green)':'var(--red)'}">${r.delta>0?'▲ +':'▼ -'}₹${fmtNum(Math.abs(r.delta))}</td>${showDateCol?`<td>${fmtDate(r.date)}</td>`:''}</tr>`;

  const rangeBar = `
    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:12px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:8px 10px">
      <span style="font-size:.72rem;font-weight:700;color:var(--gray)">📅 RANGE:</span>
      <input type="date" id="mfAumFromDate" value="${fromDate}" style="padding:5px;font-size:.75rem">
      <span style="font-size:.72rem;color:var(--gray)">to</span>
      <input type="date" id="mfAumToDate" value="${toDate}" style="padding:5px;font-size:.75rem">
      <button class="btn btn-sm btn-outline" onclick="showMfAumList(document.getElementById('mfAumFromDate').value, document.getElementById('mfAumToDate').value)">Apply</button>
      <button class="btn btn-sm btn-outline" onclick="showMfAumList()">Today</button>
    </div>`;

  const sectionHtml = (label, color, bg, rows, sum) => `
    <div style="margin-bottom:6px;padding:7px 10px;background:${bg};border-radius:6px;font-weight:800;font-size:.8rem;color:${color}">
      ${label} — ${rows.length} client${rows.length===1?'':'s'} · ₹${fmtNum(sum)}
    </div>
    <div class="tbl-wrap"><div class="tbl-scroll"><table><thead><tr><th>Name</th><th>RM</th><th>Previous Invested</th><th>Current Invested</th><th>Change</th>${showDateCol?'<th>Date</th>':''}</tr></thead>
    <tbody>${rows.length ? rows.map(rowHtml).join('') : `<tr><td colspan="${showDateCol?6:5}" style="text-align:center;color:var(--gray)">None</td></tr>`}</tbody></table></div></div>`;

  const body = rangeBar
    + sectionHtml('🟢 ADDITIONS', 'var(--green)', '#f0fdf4', additions, sumInc)
    + `<div style="height:14px"></div>`
    + sectionHtml('🔴 REDEMPTIONS', 'var(--red)', '#fef2f2', redemptions, sumDec);

  const rangeLabel = fromDate===toDate ? fmtDate(fromDate) : `${fmtDate(fromDate)} to ${fmtDate(toDate)}`;
  document.getElementById('reportModalTitle').textContent = `📈 MF Invested Amount — Changes (${rangeLabel})`;
  document.getElementById('reportModalBody').innerHTML = body;
  // Keep Export CSV working on this modal: combined table with an explicit Type column.
  currentReportData = {
    title: 'MF Invested Amount Changes '+rangeLabel,
    headers: ['Type','Name','RM','Previous Invested','Current Invested','Change','Date'],
    rows: changed.map(r=>[r.delta>0?'Addition':'Redemption', r.name, r.rm||'—', r.prevInvested||0, r.newInvested||0, r.delta, r.date])
  };
  document.getElementById('reportModal').classList.add('open');
}

// Admin-only: RM-wise split of AUM increases/decreases.
function showMfAumRmSplit(){
  const td = today();
  const changed = mfChangeLogRows(td);
  const rmMap = {};
  changed.forEach(r=>{
    const rm = r.rm || '— (no RM)';
    if(!rmMap[rm]) rmMap[rm] = {inc:0, dec:0, incAmt:0, decAmt:0};
    if(r.delta>0){ rmMap[rm].inc++; rmMap[rm].incAmt+=r.delta; }
    else { rmMap[rm].dec++; rmMap[rm].decAmt+=Math.abs(r.delta); }
  });
  const table = Object.entries(rmMap).map(([rm,v])=>{
    const rmCell = rm==='— (no RM)' ? rm : `<span style="text-decoration:underline;cursor:pointer;color:var(--blue)" onclick="closeModal('reportModal');showMfAumList()">${escapeHtml(rm)}</span>`;
    return [rmCell, v.inc, '₹'+fmtNum(v.incAmt), v.dec, '₹'+fmtNum(v.decAmt)];
  }).sort((a,b)=>(b[1]+b[3])-(a[1]+a[3]));
  if(!table.length){ toast('No Invested Amount change record for today','info'); return; }
  showReport('📈 MF Invested Amount Changes (Today) — RM-wise', ['RM','Additions','Addition Amt','Redemptions','Redemption Amt'], table);
}

// Date-wise history of MF Invested Amount changes — mirrors the Equity
// Active/Inactive date-wise history card. Unlike Equity (which has to
// guess likely clients for undated deltas), every MF change is logged
// exactly (see DB.addMfChangeLog in the AUM import merge), so "Changed
// Clients" always shows the real named list — no guessing needed.
function showMfAumHistory(rmName){
  let scope;
  if(rmName){
    scope = rmScopeIdByName(rmName);
    if(!scope){ toast(`No history record found for ${rmName}`,'info'); return; }
  } else {
    scope = CU.role==='admin' ? 'ALL' : (CU.id || CU.name);
  }
  let hist=[];
  try{ hist = JSON.parse(localStorage.getItem('dninvest_mf_aum_snapshots')||'[]'); }catch(e){ hist=[]; }
  const rows = hist.filter(x=>x&&x.scope===scope).sort((a,b)=>String(b.date).localeCompare(String(a.date)));
  if(!rows.length){ toast(rmName?`No history built yet for ${rmName} — it will be created once their dashboard is opened tomorrow`:'No history built yet — please check again after opening the dashboard tomorrow','info'); return; }
  const table = rows.map(r=>{
    const changed = mfChangesByDate(r.date, rmName);
    const rmArg = rmName ? `'${escapeHtml(rmName).replace(/'/g,"\\'")}'` : '';
    const changedCell = changed.length
      ? `<span style="text-decoration:underline;cursor:pointer;color:var(--blue)" onclick="showMfChangeDrilldown('${r.date}',${rmArg})" title="Click to see client names">${changed.length} client${changed.length>1?'s':''} ▶</span>`
      : '<span style="color:var(--gray);font-size:.78rem">—</span>';
    const addCell = r.additions ? `<span style="color:var(--green);font-weight:700">${r.additions} (+₹${fmtNum(r.additionsAmt)})</span>` : '0';
    const redCell = r.redemptions ? `<span style="color:var(--red);font-weight:700">${r.redemptions} (-₹${fmtNum(r.redemptionsAmt)})</span>` : '0';
    return [fmtDate(r.date), addCell, redCell, '₹'+fmtNum(r.totalInvested||0), changedCell];
  });
  const label = rmName ? ` (${rmName})` : (scope==='ALL'?' (All)':'');
  showReport('📈 MF Invested Amount — Date-wise History'+label,
    ['Date','Additions','Redemptions','Total Invested','Changed Clients'], table);
}

// Exact named list of MF clients whose Invested Amount changed on a given
// date, from the persistent change log (not the live client field, which a
// later import can overwrite) — optionally filtered to one RM.
function mfChangesByDate(dateStr, rmName){
  let log=[];
  try{ log = JSON.parse(localStorage.getItem('dninvest_mf_change_log')||'[]'); }catch(e){ log=[]; }
  let rows = log.filter(x=>x&&x.date===dateStr);
  if(rmName) rows = rows.filter(x=>(x.rm||'').trim().toUpperCase()===rmName.trim().toUpperCase());
  return rows;
}

function showMfChangeDrilldown(dateStr, rmName){
  const rows = mfChangesByDate(dateStr, rmName).sort((a,b)=>Math.abs(b.delta)-Math.abs(a.delta));
  if(!rows.length){ toast('No named record found for this date','info'); return; }
  const table = rows.map(c=>{
    const up = c.delta>0;
    return [escapeHtml(c.name||'—'), c.rm||'—', '₹'+fmtNum(c.prevInvested||0), '₹'+fmtNum(c.newInvested||0),
      `<span style="color:${up?'var(--green)':'var(--red)'};font-weight:700">${up?'▲ +':'▼ -'}₹${fmtNum(Math.abs(c.delta))}</span>`];
  });
  const label = rmName ? ` — ${rmName}` : '';
  showReport(`📋 MF Changes on ${fmtDate(dateStr)}${label}`, ['Name','RM','Previous Invested','Current Invested','Change'], table);
}

// Generic Show More / Show Less toggle — reusable by any dashboard list card.
// extraId = the hidden container holding the extra rows, btnWrapId = the wrapper holding the toggle button.
function dashShowMoreToggle(extraId, btnWrapId, extraCount){
  const extra = document.getElementById(extraId);
  const wrap = document.getElementById(btnWrapId);
  if(!extra || !wrap) return;
  const btn = wrap.querySelector('button');
  const isHidden = extra.style.display==='none';
  extra.style.display = isHidden ? '' : 'none';
  if(btn) btn.textContent = isHidden ? '▲ Show Less' : `▼ Show ${extraCount} More`;
}

function refreshDash(){
  const eq = getMyEqClients();
  const activeEq = getActiveEqClients();
  const mf = getMyMfClients();
  const segs=CU.segments||[];
  const hasEq=segs.includes('equity'), hasMf=segs.includes('mf');

  const eqActive=activeEq.filter(c=>c.status==='Active').length;
  const mfInv=mf.filter(c=>c.status==='Investor').length;
  // Dashboard ke KPI card ("Total AUM"/"Monthly SIP") sirf RM ke apne clients
  // ka figure dikhaye — temp-access (absent colleague cover) wale clients
  // in totals me na judein. Baaki jagah (lists, follow-ups) temp access
  // waisa hi kaam karta rahega.
  const ownMfDealers=(CU.mf_dealers||[CU.name]).map(d=>d.trim().toUpperCase());
  const ownMf = CU.role==='admin' ? mf : mf.filter(c=>ownMfDealers.includes((c.rm||'').trim().toUpperCase()));
  const totalAUM=ownMf.reduce((s,c)=>s+(parseFloat(c.aum)||0),0);
  const totalSIP=ownMf.reduce((s,c)=>s+(parseFloat(c.sip_amount)||0),0);
  const pendEq=activeEq.filter(c=>c.next_call&&c.next_call<=today()).length;
  const pendMf=mf.filter(c=>c.next_call&&c.next_call<=today()).length;

  let statsHtml='';
  if(hasEq){
    // "Asset" card bhi sirf RM ke apne dealer-name wale clients ka risk_val
    // jode — temp-access (absent colleague cover) clients ka asset ab isme
    // nahi judega. Pehle ye poori company ka total dikhata tha (bug).
    const ownEqDealers=(CU.eq_dealers||[CU.name]).map(d=>d.trim().toUpperCase());
    const ownEq = CU.role==='admin' ? eq : eq.filter(c=>ownEqDealers.includes((c.rm||'').trim().toUpperCase()));
    const totalRisk=ownEq.reduce((s,c)=>{const rk=eqRiskFor(c.code); return s+(rk?(rk.risk_val||0):0);},0);
    statsHtml+=sc(eq.length,'Total EQ Clients','','equity');
    statsHtml+=sc('₹'+fmtNum(totalRisk),'Asset','gold','equity');
  }
  if(hasEq||hasMf){
    statsHtml+=sc(pendEq+pendMf,'Follow-ups Due','red','');
  }
  if(hasMf){
    statsHtml+=sc(mf.length,'MF Investors','teal','mf');
    statsHtml+=sc('₹'+fmtNum(totalAUM),'Total AUM','gold','mf');
    statsHtml+=sc('₹'+fmtNum(totalSIP),'Monthly SIP','purple','mf');
  }
  document.getElementById('dashStats').innerHTML=statsHtml;

  // Equity RM chart
  if(hasEq){
    const rmC={};
    eq.forEach(c=>{ rmC[c.rm]=(rmC[c.rm]||0)+1; });
    const rs=Object.entries(rmC).sort((a,b)=>b[1]-a[1]);
    const mx=rs[0]?.[1]||1;
    document.getElementById('rmEqChart').innerHTML = rs.length
      ? rs.map(([n,v])=>`<div class="bar-row"><span class="bar-name">${n}</span><div class="bar-wrap"><div class="bar-fill" style="width:${(v/mx*100).toFixed(0)}%"></div></div><span class="bar-num">${v}</span></div>`).join('')
      : '<p style="color:var(--gray);font-size:.82rem;padding:8px 0">No equity clients yet</p>';
    document.getElementById('rmEqCard').style.display=hasEq?'':'none';
    const rmEqCountEl=document.getElementById('rmEqCount'); if(rmEqCountEl) rmEqCountEl.textContent=rs.length+' RMs';
  }

  // MF RM AUM chart
  if(hasMf){
    const rmA={};
    mf.forEach(c=>{ rmA[c.rm]=(rmA[c.rm]||0)+(parseFloat(c.aum)||0); });
    const rs2=Object.entries(rmA).sort((a,b)=>b[1]-a[1]);
    const mx2=rs2[0]?.[1]||1;
    const totMfAum=rs2.reduce((s,[,v])=>s+v,0);
    document.getElementById('rmMfChart').innerHTML = rs2.length
      ? rs2.map(([n,v])=>`<div class="bar-row"><span class="bar-name">${n}</span><div class="bar-wrap"><div class="bar-fill" style="width:${(v/mx2*100).toFixed(0)}%;background:var(--teal)"></div></div><span class="bar-num">₹${fmtNum(v)}</span></div>`).join('')
        + `<div class="bar-row" style="font-weight:700;border-top:2px solid var(--border,#ddd);margin-top:4px;padding-top:6px"><span class="bar-name">TOTAL</span><div class="bar-wrap"></div><span class="bar-num">₹${fmtNum(totMfAum)}</span></div>`
      : '<p style="color:var(--gray);font-size:.82rem;padding:8px 0">No MF clients yet</p>';
    document.getElementById('rmMfCard').style.display=hasMf?'':'none';
    const rmMfCountEl=document.getElementById('rmMfCount'); if(rmMfCountEl) rmMfCountEl.textContent=rs2.length+' RMs';
  }

  // No-trade alerts
  const alerts = activeEq.map(c=>({...c,days:daysDiff(c.last_trade_date)}))
    .filter(c=>c.days!==null&&c.days>30)
    .sort((a,b)=>b.days-a.days).slice(0,8);
  document.getElementById('noTradeAlert').innerHTML = alerts.length
    ? alerts.map(c=>{
        const cls=c.days>=180?'r180':c.days>=90?'r90':c.days>=60?'r60':'r30';
        return `<div class="alert-row ${cls}"><span>${c.name}</span><span>${c.days} days</span></div>`;
      }).join('')
    : '<p style="color:var(--green);font-size:.82rem;padding:8px 0">✅ No alerts</p>';

  // Active/Inactive daily trend (1-saal se trade nahi kiya = Inactive)
  if(hasEq) renderEqActivityTrend(activeEq);
  if(hasMf) renderMfAumTrend();

  // No-call alerts (Equity)
  const noCallEq = activeEq.map(c=>({...c,days:daysDiff(c.last_call_date)}))
    .filter(c=>c.days===null||c.days>=60)
    .sort((a,b)=>{const av=a.days===null?Infinity:a.days, bv=b.days===null?Infinity:b.days; return bv-av;})
    .slice(0,8);
  document.getElementById('noCallEqAlert').innerHTML = noCallEq.length
    ? noCallEq.map(c=>{
        const cls=c.days===null||c.days>=180?'r180':c.days>=90?'r90':'r60';
        const label=c.days===null?'Never':c.days+' days';
        return `<div class="alert-row ${cls}"><span>${c.name}</span><span>${label}</span></div>`;
      }).join('')
    : '<p style="color:var(--green);font-size:.82rem;padding:8px 0">✅ No alerts</p>';

  // No-call alerts (MF)
  const noCallMf = mf.map(c=>({...c,days:daysDiff(c.last_call_date)}))
    .filter(c=>c.days===null||c.days>=60)
    .sort((a,b)=>{const av=a.days===null?Infinity:a.days, bv=b.days===null?Infinity:b.days; return bv-av;})
    .slice(0,8);
  document.getElementById('noCallMfAlert').innerHTML = noCallMf.length
    ? noCallMf.map(c=>{
        const cls=c.days===null||c.days>=180?'r180':c.days>=90?'r90':'r60';
        const label=c.days===null?'Never':c.days+' days';
        return `<div class="alert-row ${cls}"><span>${c.name}</span><span>${label}</span></div>`;
      }).join('')
    : '<p style="color:var(--green);font-size:.82rem;padding:8px 0">✅ No alerts</p>';

  // Follow-ups — Today + Overdue (Equity + MF + Leads + Other Products)
  const leads = getMyLeads();
  const op    = getMyOpEntries();
  const tdStr = today();
  const fuAll=[
    ...eq.filter(c=>c.next_call&&c.next_call<=tdStr).map(c=>({name:c.name,rm:c.rm,next_call:c.next_call,seg:'EQ'})),
    ...mf.filter(c=>c.next_call&&c.next_call<=tdStr).map(c=>({name:c.name,rm:c.rm,next_call:c.next_call,seg:'MF'})),
    ...leads.filter(c=>c.next_call&&c.next_call<=tdStr).map(c=>({name:c.name,rm:c.rm,next_call:c.next_call,seg:'Lead'})),
    ...op.filter(c=>c.next_call&&c.next_call<=tdStr).map(c=>({name:c.client_name||c.name,rm:c.rm,next_call:c.next_call,seg:c.product_type||'Other'}))
  ].sort((a,b)=>a.next_call.localeCompare(b.next_call));
  const segCls = s=>s==='EQ'?'b-eq':s==='MF'?'b-mf':s==='Lead'?'b-lead':'b-op';
  const fuHeadEl=document.getElementById('fuHeadCount');
  if(fuHeadEl){fuHeadEl.textContent=fuAll.length>0?fuAll.length:'';fuHeadEl.style.display=fuAll.length>0?'':'none';}
  const FU_LIMIT = 5;
  const fuRowsHtml = fuAll.map(c=>{
    const isOverdue = c.next_call < tdStr;
    return `<div class="bar-row" style="border-left:3px solid ${isOverdue?'var(--red)':'transparent'};padding-left:9px;">
      <span class="bar-name">${escapeHtml(c.name||'—')}</span>
      <span class="badge ${segCls(c.seg)}" style="margin:0 6px;flex-shrink:0">${c.seg}</span>
      <span style="font-size:.72rem;color:${isOverdue?'var(--red)':'var(--gray)'};margin-right:4px;font-weight:${isOverdue?'700':'400'}">${isOverdue?'⚠️ '+fmtDate(c.next_call):'Today'}</span>
      <span style="font-size:.72rem;color:var(--gray);margin-left:auto">${escapeHtml(c.rm||'')}</span>
    </div>`;
  });
  document.getElementById('todayFollowup').innerHTML = fuAll.length
    ? fuRowsHtml.slice(0,FU_LIMIT).join('')
      + (fuAll.length>FU_LIMIT ? `<div id="fuExtra" style="display:none">${fuRowsHtml.slice(FU_LIMIT).join('')}</div>
         <div id="fuMoreWrap" style="text-align:center;margin-top:6px">
           <button onclick="dashShowMoreToggle('fuExtra','fuMoreWrap',${fuAll.length-FU_LIMIT})" style="background:none;border:1.5px solid var(--border);border-radius:20px;padding:4px 16px;font-size:.72rem;font-weight:700;color:var(--navy);cursor:pointer">▼ Show ${fuAll.length-FU_LIMIT} More</button>
         </div>` : '')
    : '<p style="color:var(--green);font-size:.82rem;padding:8px 0">✅ No pending follow-ups</p>';

  // ── 🎂 Today's Birthdays (match dob month+day with today) ──
  const bdayEl = document.getElementById('todayBirthday');
  if(bdayEl){
    const todayMD = tdStr.slice(5,10);            // mm-dd
    const curYear = parseInt(tdStr.slice(0,4),10);
    const bdays = [
      ...eq.filter(c=>c.dob && c.dob.slice(5,10)===todayMD).map(c=>({id:c.id,key:'eq_clients',name:c.name,rm:c.rm,mobile:c.mobile,dob:c.dob,bw:c.bday_wish,seg:'EQ'})),
      ...mf.filter(c=>c.dob && c.dob.slice(5,10)===todayMD).map(c=>({id:c.id,key:'mf_clients',name:c.name,rm:c.rm,mobile:c.mobile,dob:c.dob,bw:c.bday_wish,seg:'MF'}))
    ];
    const bHead=document.getElementById('bdayHeadCount');
    const _nSent=bdays.filter(c=>c.bw && c.bw.y===curYear).length;
    if(bHead){
      bHead.innerHTML=bdays.length>0?`${_nSent}/${bdays.length} sent`:'';
      bHead.style.display=bdays.length>0?'':'none';
      bHead.style.background=(bdays.length && _nSent===bdays.length)?'#16a34a':'#e11d8f';
    }
    _bdayQueue=[]; _bdayStep=0; _bdayCards=[]; _bdayMeta=[];   // reset stepper on each dashboard refresh
    bdayEl.innerHTML = bdays.length
      ? (()=>{
        const rows = bdays.map((c,idx)=>{
          const nm=c.name||'—';
          let d=String(c.mobile||'').replace(/\D/g,'');
          if(d.length===10) d='91'+d;                       // add country code for wa.me
          const yr=parseInt(String(c.dob).slice(0,4),10);
          const turns=(yr&&curYear>yr)?` <small style="color:var(--gray)">(turns ${curYear-yr})</small>`:'';
          const msg=`Dear ${nm.split(' ')[0]||nm}, Wishing you a very Happy Birthday! 🎉🎂 May the year ahead bring you great health, happiness and prosperity. Warm regards, D N Investment.`;
          const url=`https://wa.me/${d}?text=${encodeURIComponent(msg)}`;
          const sent = c.bw && c.bw.y===curYear ? c.bw : null;
          if(d.length>=12 && !sent) _bdayQueue.push({name:nm, url, idx});   // Wish-All: only the pending ones
          _bdayCards[idx]={name:nm, num:(d.length>=12?d:'')};   // name + wa number for card
          _bdayMeta[idx]={id:c.id, key:c.key, name:nm};         // for the sent-stamp
          // ✅ Sent stamp — Admin ko dikhta hai ki kisne aur kab bheja
          let sentTag='';
          if(sent){
            let tm='';
            try{ tm=new Date(sent.at).toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit',hour12:true,timeZone:'Asia/Kolkata'}); }catch(e){}
            sentTag=`<span class="bday-sent" title="${escapeHtml(sent.by||'')} ne ${escapeHtml(sent.via==='card'?'card':'wish')} bheja${tm?' — '+tm:''}"
              style="background:#dcfce7;color:#166534;font-size:.66rem;font-weight:800;padding:2px 7px;border-radius:7px;white-space:normal;line-height:1.3">
              ✅ Sent${sent.by?' · '+escapeHtml(sent.by):''}${tm?' · '+tm:''}</span>`;
          }
          const waBtn = d.length>=12
            ? `<a href="${url}" target="_blank" rel="noopener" onclick="markBdayWished(${idx},'wish')" style="margin-left:auto;background:${sent?'#9ca3af':'#25D366'};color:#fff;font-size:.7rem;font-weight:700;padding:3px 10px;border-radius:8px;text-decoration:none;flex-shrink:0">💬 ${sent?'Re-send':'Wish'}</a>`
            : `<span style="margin-left:auto;font-size:.68rem;color:var(--gray);flex-shrink:0">No number</span>`;
          const cardBtn=`<button onclick="birthdayCardImage(${idx})" title="Make colourful card image for client" style="margin-left:6px;background:${sent?'#9ca3af':'#e11d8f'};color:#fff;border:none;font-size:.7rem;font-weight:700;padding:3px 9px;border-radius:8px;cursor:pointer;flex-shrink:0">🎨 Card</button>`;
          return `<div class="bar-row bday-row" style="border-left:3px solid ${sent?'#16a34a':'#e11d8f'};padding-left:9px;${sent?'background:#f6fef9;':''}">
            <div class="bday-main">
              <span class="bar-name">${escapeHtml(nm)}${turns}</span>
              <span class="badge ${c.seg==='EQ'?'b-eq':'b-mf'}" style="flex-shrink:0">${c.seg}</span>
              <span style="font-size:.72rem;color:var(--gray)">${escapeHtml(c.rm||'')}</span>
              ${sentTag}
            </div>
            <div class="bday-acts">${waBtn}${cardBtn}</div>
          </div>`;
        }); // rows stays an array so we can slice for Show More
        const BD_LIMIT = 5;
        const rowsHtml = rows.slice(0,BD_LIMIT).join('')
          + (rows.length>BD_LIMIT ? `<div id="bdExtra" style="display:none">${rows.slice(BD_LIMIT).join('')}</div>
             <div id="bdMoreWrap" style="text-align:center;margin-top:6px">
               <button onclick="dashShowMoreToggle('bdExtra','bdMoreWrap',${rows.length-BD_LIMIT})" style="background:none;border:1.5px solid var(--border);border-radius:20px;padding:4px 16px;font-size:.72rem;font-weight:700;color:var(--navy);cursor:pointer">▼ Show ${rows.length-BD_LIMIT} More</button>
             </div>` : '');
        const wishAll = _bdayQueue.length>=2
          ? `<button id="wishAllBtn" onclick="wishAllBirthdays()" style="width:100%;margin-bottom:10px;background:#25D366;color:#fff;border:none;font-size:.8rem;font-weight:800;padding:8px;border-radius:9px;cursor:pointer">💬 Wish All Pending (${_bdayQueue.length}) — opens one by one</button>`
          : '';
        const doneBar = (bdays.length && _nSent===bdays.length)
          ? `<div style="background:#dcfce7;color:#166534;font-size:.78rem;font-weight:800;padding:7px;border-radius:9px;margin-bottom:10px;text-align:center">✅ All ${bdays.length} birthday wishes for today have been sent</div>`
          : '';
        return doneBar + wishAll + rowsHtml;
      })()
      : '<p style="color:var(--gray);font-size:.82rem;padding:8px 0">🎂 No birthdays today</p>';
  }

  // Redemption / SIP Stop audit (Admin side) — last entry date per MF RM,
  // sorted with the most overdue (or never-logged) RM at the top, so Admin
  // can see at a glance who needs a follow-up nudge.
  const auditCard = document.getElementById('mfAuditCard');
  if(auditCard && CU.role==='admin'){
    auditCard.style.display='';
    const entries = getMfBizEntries();
    const mfRMs = getSegRMs('mf');
    const rows = mfRMs.map(rm=>{
      const mine = entries.filter(e=>(e.rm||'').trim().toLowerCase()===rm.trim().toLowerCase());
      const lastOf = type=>{
        const list = mine.filter(e=>e.type===type).sort((a,b)=>(b.date||'').localeCompare(a.date||''));
        return list[0]?.date || null;
      };
      const lastRed = lastOf('Redemption'), lastStop = lastOf('SIP Stop');
      const dRed = daysDiff(lastRed), dStop = daysDiff(lastStop);
      const worst = Math.max(dRed===null?9999:dRed, dStop===null?9999:dStop);
      return {rm, lastRed, lastStop, dRed, dStop, worst};
    }).sort((a,b)=>b.worst-a.worst);

    const colorOf = d => d===null||d>=60 ? '#f3e8ff' : d>=30 ? '#fef3c7' : d>=15 ? '#fee2e2' : '#dcfce7';
    const textColorOf = d => d===null||d>=60 ? '#6d28d9' : d>=30 ? '#92400e' : d>=15 ? '#991b1b' : '#166534';
    const label = d => d===null ? 'Never' : d+'d ago';
    const badge = d => `<span style="display:inline-block;min-width:72px;text-align:center;padding:3px 8px;border-radius:8px;font-size:.74rem;font-weight:700;background:${colorOf(d)};color:${textColorOf(d)}">${label(d)}</span>`;

    document.getElementById('mfAuditTable').innerHTML = rows.length
      ? `<div class="bar-row" style="font-size:.72rem;color:var(--gray);font-weight:700;padding-bottom:6px">
           <span class="bar-name">RM</span><span style="width:90px;text-align:center">Redemption</span><span style="width:90px;text-align:center">SIP Stop</span>
         </div>` +
        rows.map(r=>`
          <div class="bar-row">
            <span class="bar-name">${r.rm}</span>
            <span style="width:90px;display:flex;justify-content:center">${badge(r.dRed)}</span>
            <span style="width:90px;display:flex;justify-content:center">${badge(r.dStop)}</span>
          </div>`).join('')
      : '<p style="color:var(--gray);font-size:.82rem;padding:8px 0">No MF RMs found</p>';
  } else if(auditCard){
    auditCard.style.display='none';
  }

  updateBadges();
}

// ══════════════════════════════════════════
// EQUITY ACTIVE/INACTIVE TREND (dashboard card)
// ══════════════════════════════════════════
// "Inactive" yahan wahi matlab rakhta hai jo poore app me hai (EQ_INACTIVE_DAYS
// = 365 din, dekho fixStatusByLastTrade / deriveEqStatus): jis client ne 1 saal
// (ya usse zyada) se trade nahi kiya — ya kabhi trade hi nahi kiya (last_trade_date
// blank) — wo is count me Inactive maana jata hai. Closed clients already
// getActiveEqClients() se bahar reh jate hain.
//
// Har roz dashboard khulne par aaj ki date ke saath Active/Inactive count ek
// chhoti si history me save hoti hai (DB.addEqActivitySnapshot — merge-by-date,
// purani dates chhedi nahi jati). Isse:
//   • Aaj ka number turant dikhta hai
//   • Kal se aaj ka diff (▲/▼) turant dikhta hai
//   • Card par click karke pura date-wise history table khul jata hai
//
// scope: Admin poore base ka trend dekhta hai ('ALL'), har RM apna hi trend
// (apne CU.id se) — dono ek dusre ka data overwrite nahi karte.
function renderEqActivityTrend(activeEq){
  const el = document.getElementById('eqActivityTrend');
  if(!el) return;

  const isAdmin = CU.role==='admin';

  // Cache the full (unfiltered) raw client rows so a later click (admin
  // RM-wise breakdown) or an RM-filter change can reuse them without
  // recomputing/refetching.
  window.__eqActivityAllRows = activeEq;
  window.__eqActivityRows = activeEq; // kept for backward-compat with showEqActivityRmSplit()

  // Admin-only RM filter dropdown — populate once, keep selection sticky.
  const rmSel = document.getElementById('eqActivityRmFilter');
  if(rmSel){
    if(isAdmin){
      if(!rmSel.dataset.filled){
        const rms=[...new Set(getSegRMs('equity'))].sort((a,b)=>a.localeCompare(b));
        rmSel.innerHTML = '<option value="">👥 All RMs</option>' + rms.map(r=>`<option value="${escapeHtml(r)}">${escapeHtml(r)}</option>`).join('');
        rmSel.dataset.filled = '1';
      }
      rmSel.style.display = '';
    } else {
      rmSel.style.display = 'none';
      rmSel.value = '';
    }
  }
  const rmFilter = (isAdmin && rmSel) ? (rmSel.value||'').trim() : '';

  // Rows this render actually shows (all, or one RM if admin has filtered).
  const rows = rmFilter
    ? activeEq.filter(c=>(c.rm||'').trim().toUpperCase()===rmFilter.toUpperCase())
    : activeEq;

  // "Kabhi trade nahi kiya" clients ka last_trade_date blank hota hai — status
  // field unhe already chhoo nahi paata (deriveEqStatus '' return karta hai jab
  // date pata nahi), isliye unhe yahan explicitly Inactive gin lete hain taaki
  // "kabhi trade nahi kiya" wale bhi is count me shamil rahein.
  const nActive   = rows.filter(c=>c.status==='Active').length;
  const nInactive = rows.filter(c=>c.status==='Inactive' || (!c.status && !c.last_trade_date) || (c.status!=='Active' && !c.last_trade_date)).length;
  const total     = rows.length;

  // The scope that gets *saved* to the shared snapshot history is always the
  // true full-base ('ALL') or the RM's own login scope — never the admin's
  // filter selection, so filtering never corrupts the real daily history.
  const scope = isAdmin ? 'ALL' : (CU.id || CU.name);
  const td = today();
  const entry = {date:td, scope, active:activeEq.filter(c=>c.status==='Active').length,
    inactive:activeEq.filter(c=>c.status==='Inactive' || (!c.status && !c.last_trade_date) || (c.status!=='Active' && !c.last_trade_date)).length,
    total:activeEq.length};

  // Local history (already-known snapshots) — for yesterday's diff, without
  // waiting on the async Firestore round-trip. When admin has an RM filtered,
  // look up that RM's own scope (id) so the diff reflects that RM, not 'ALL'.
  let hist=[];
  try{ hist = JSON.parse(localStorage.getItem('dninvest_eq_activity_snapshots')||'[]'); }catch(e){ hist=[]; }
  const diffScope = rmFilter ? rmScopeIdByName(rmFilter) : scope;
  const mine = diffScope ? hist.filter(x=>x&&x.scope===diffScope).sort((a,b)=>String(a.date).localeCompare(String(b.date))) : [];
  const yesterdayEntry = mine.filter(x=>x.date<td && x.total>0).slice(-1)[0] || null;

  const diffLabel=(cur,prev)=>{
    if(prev==null) return '<span style="font-size:.62rem;color:var(--gray)">no data yesterday</span>';
    const d=cur-prev;
    if(d===0) return '<span style="font-size:.64rem;color:var(--gray)">— no change vs yesterday</span>';
    const up=d>0;
    return `<span style="font-size:.64rem;font-weight:700;color:${up?'var(--red)':'var(--green)'}">${up?'▲':'▼'} ${Math.abs(d)} vs yesterday</span>`;
  };

  const cardClick = isAdmin ? (rmFilter ? `showEqActivityHistory('${escapeHtml(rmFilter)}')` : 'showEqActivityRmSplit()') : 'showEqActivityHistory()';
  const cardTitle = isAdmin ? (rmFilter ? `Click for ${rmFilter}'s date-wise history` : 'Click for RM-wise breakdown') : 'Click for date-wise history';
  const badImportCount = isAdmin ? findEqBadImportCandidates().length : 0;
  const footerNote = isAdmin
    ? (rmFilter
        ? `📅 ${fmtDate(td)} · showing <b>${escapeHtml(rmFilter)}</b> only · click card for day-by-day history · <span style="text-decoration:underline;cursor:pointer" onclick="event.stopPropagation();showEqActivityRmSplit()">all-RM split</span>`
        : `📅 ${fmtDate(td)} · click card for RM-wise split · <span style="text-decoration:underline;cursor:pointer" onclick="event.stopPropagation();showEqActivityHistory()">date-wise history</span>`)
      + (badImportCount ? ` · <span style="text-decoration:underline;cursor:pointer;color:#dc2626;font-weight:700" onclick="event.stopPropagation();openEqBadImportReview()">🧹 ${badImportCount} suspicious today — review</span>` : '')
    : `📅 ${fmtDate(td)} · click card for full date-wise history`;

  // "Today's Wins/Loss" — always visible, shows both gains and losses
  const eqWins = yesterdayEntry ? Math.max(0, nActive - yesterdayEntry.active) : 0;
  const eqLoss = yesterdayEntry ? Math.max(0, yesterdayEntry.active - nActive) : 0;
  const mfWins = mfChangeLogRows(td).filter(r => r.delta > 0 && !(parseFloat(r.prevInvested)||0)).length;
  const totalWins = eqWins + mfWins;
  const totalLoss = eqLoss;

  // Build win/loss lines
  const winLines = [
    eqWins>0 ? `<span style="color:#15803d">+${eqWins} EQ Active</span>` : '',
    mfWins>0 ? `<span style="color:#15803d">+${mfWins} MF New</span>` : '',
    eqLoss>0 ? `<span style="color:#dc2626">-${eqLoss} EQ Inactive</span>` : '',
  ].filter(Boolean).join(' · ');

  const noData = !yesterdayEntry;
  const boxBg = totalWins>0 && totalLoss===0
    ? 'linear-gradient(135deg,#fef9c3,#fef3c7)'
    : totalLoss>0 && totalWins===0
      ? 'linear-gradient(135deg,#fef2f2,#fee2e2)'
      : totalWins>0 && totalLoss>0
        ? 'linear-gradient(135deg,#fef9c3,#fef2f2)'
        : '#f8fafc';
  const boxBorder = totalWins>0 && totalLoss===0 ? '#f59e0b'
    : totalLoss>0 && totalWins===0 ? '#f87171'
    : totalWins>0 && totalLoss>0 ? '#f59e0b'
    : 'var(--border)';
  const icon = totalWins>0 && totalLoss===0 ? '🏆'
    : totalLoss>0 && totalWins===0 ? '📉'
    : totalWins>0 && totalLoss>0 ? '⚡'
    : '📊';
  const mainNum = totalWins>0 ? `<span style="color:#b45309">+${totalWins}</span>`
    : totalLoss>0 ? `<span style="color:#dc2626">-${totalLoss}</span>`
    : `<span style="color:var(--gray)">—</span>`;

  const winsBox = `
      <div class="dash-stat-box" style="background:${boxBg};border:1.5px solid ${boxBorder};cursor:pointer" onclick="event.stopPropagation();showTodaysWinLossList()" title="Click to see which clients changed today">
        <div class="dash-stat-lbl" style="font-size:.58rem;color:#92400e">${icon} TODAY'S WIN/LOSS</div>
        <div style="font-size:1.3rem;font-weight:900;line-height:1.2">${mainNum}</div>
        <span style="font-size:.6rem;font-weight:700">${noData ? '<span style="color:var(--gray)">no data yet</span>' : (winLines || '<span style="color:var(--gray)">no change</span>')}</span>
      </div>`;

  el.innerHTML = `
    <div class="dash-stat-grid dash-stat-grid-3" onclick="${cardClick}" title="${cardTitle}">
      <div class="dash-stat-box" style="background:#f0fdf4;border:1.5px solid #bbf7d0">
        <div class="dash-stat-lbl" style="font-size:.58rem;color:var(--gray)">🟢 ACTIVE (within 1yr)</div>
        <div style="font-size:1.3rem;font-weight:900;color:var(--green);line-height:1.2">${nActive}</div>
        ${diffLabel(nActive, yesterdayEntry?yesterdayEntry.active:null)}
      </div>
      <div class="dash-stat-box" style="background:#fef2f2;border:1.5px solid #fecaca">
        <div class="dash-stat-lbl" style="font-size:.58rem;color:var(--gray)" title="1yr+ no trade / never traded">🔴 INACTIVE (1yr+)</div>
        <div style="font-size:1.3rem;font-weight:900;color:var(--red);line-height:1.2">${nInactive}</div>
        ${diffLabel(nInactive, yesterdayEntry?yesterdayEntry.inactive:null)}
      </div>
      ${winsBox}
    </div>
    <p style="color:var(--gray);font-size:.62rem;margin-top:5px;text-align:center">${footerNote}</p>`;

  // Save today's TRUE (unfiltered) snapshot (fire-and-forget — don't block
  // the dashboard render, and don't let an admin's RM filter overwrite it).
  DB.addEqActivitySnapshot(entry).catch(e=>console.log('activity snapshot save failed',e));
  // Sync heights of both activity cards after render so they align perfectly
  requestAnimationFrame(()=>{
    const eq = document.getElementById('eqActivityCard');
    const mf = document.getElementById('mfAumCard');
    if(eq && mf){
      eq.style.minHeight=''; mf.style.minHeight='';
      const h = Math.max(eq.offsetHeight, mf.offsetHeight);
      eq.style.minHeight = h+'px'; mf.style.minHeight = h+'px';
    }
  });

  // One-time-per-session top-up from Firestore: local cache only has
  // snapshots this exact browser has ever written, so a device Puja hasn't
  // used before (or a cleared cache) shows "no data yesterday" even though
  // her scope's data exists in the shared doc. Pull it once and re-render.
  if(!window.__easSynced){
    window.__easSynced = true;
    DB.fetchEqActivitySnapshots().then(merged=>{
      if(merged && window.__eqActivityAllRows) renderEqActivityTrend(window.__eqActivityAllRows);
    });
  }
}

// Re-renders the Trade Activity card using already-loaded client rows when
// admin changes the RM filter dropdown — no refetch needed.
function onEqActivityRmFilterChange(){
  const rows = window.__eqActivityAllRows || [];
  renderEqActivityTrend(rows);
}

// Maps an RM's display name (as used on client records / the filter
// dropdown) to their user id (the scope key used when that RM's own
// dashboard saves its daily snapshot). Returns '' if not found.
function rmScopeIdByName(name){
  if(!name) return '';
  const u = (DB.get('users')||[]).find(u=>u.name && u.name.trim().toUpperCase()===name.trim().toUpperCase());
  return u ? (u.id || u.name) : '';
}

// Admin-only: RM-wise split of Active vs Inactive equity clients (opens in the
// shared report modal — sortable, exportable). Uses the cached rows from the
// last renderEqActivityTrend() call so no extra data fetch is needed.
function showEqActivityRmSplit(){
  const rows = window.__eqActivityRows || [];
  const rmMap = {};
  rows.forEach(c=>{
    const rm = c.rm || '— (no RM)';
    if(!rmMap[rm]) rmMap[rm] = {active:0, inactive:0};
    const isInactive = c.status==='Inactive' || (c.status!=='Active' && !c.last_trade_date);
    if(c.status==='Active') rmMap[rm].active++;
    else if(isInactive) rmMap[rm].inactive++;
  });
  const table = Object.entries(rmMap).map(([rm,v])=>{
    const total = v.active+v.inactive;
    const pct = total ? Math.round(v.active/total*100)+'%' : '—';
    // RM name is clickable (where it's a real RM, not the "no RM" bucket) —
    // jumps back to the dashboard card filtered to just that RM.
    const rmCell = rm==='— (no RM)' ? rm
      : `<span style="text-decoration:underline;cursor:pointer;color:var(--blue)" onclick="showEqActivityRmClients('${escapeHtml(rm).replace(/'/g,"\\'")}')" title="See ${escapeHtml(rm)}'s clients">${escapeHtml(rm)}</span>`;
    return [rmCell, v.active, v.inactive, total, pct];
  }).sort((a,b)=>b[3]-a[3]);
  if(!table.length){ toast('Data has not loaded yet — refresh the dashboard and try again','info'); return; }
  showReport('📊 Trade Activity — RM-wise (Active vs Inactive)', ['RM','Active','Inactive','Total','% Active'], table);
}

// Called from the RM-wise split table — sets the dashboard card's RM filter
// to the clicked RM and scrolls back to it.

// Shows which clients changed Active/Inactive status today — called from Win/Loss box click
function showTodaysWinLossList(){
  const td = today();
  const logs = DB.get('activity_logs')||[];
  // Find status changes logged today
  const todayChanges = logs.filter(l=>
    l.seg==='equity' && l.type==='edit' &&
    String(l.date||'').slice(0,10)===td &&
    Array.isArray(l.changes) && l.changes.some(ch=>ch.field==='status')
  );

  // Also check clients whose last_trade_date = today (became active by trading, not by edit)
  const allEq = window.__eqActivityAllRows || [];
  const tradedToday = allEq.filter(c=>c.status==='Active' && c.last_trade_date===td);

  const editRows = todayChanges.map(l=>{
    const ch = l.changes.find(c=>c.field==='status');
    const isWin = ch.new==='Active';
    return [
      escapeHtml(l.client_name||'—'),
      l.rm||'—',
      `<span style="color:${isWin?'var(--green)':'var(--red)'};font-weight:700">${isWin?'▲ → Active':'▼ → Inactive'}</span>`,
      'Status edit'
    ];
  });

  const tradeRows = tradedToday
    .filter(c=>!todayChanges.find(l=>l.client_name===c.name)) // avoid duplicate
    .map(c=>[
      escapeHtml(c.name||'—'),
      c.rm||'—',
      `<span style="color:var(--green);font-weight:700">▲ Traded today</span>`,
      'Trade'
    ]);

  // MF new investors added today (mirrors the mfWins count shown on the card)
  const mfNewRows = mfChangeLogRows(td)
    .filter(r => r.delta > 0 && !(parseFloat(r.prevInvested)||0))
    .map(r=>[
      escapeHtml(r.name||'—'),
      r.rm||'—',
      `<span style="color:var(--green);font-weight:700">▲ MF New (₹${fmtNum(r.delta)})</span>`,
      'New investor'
    ]);

  const rows = [...editRows, ...tradeRows, ...mfNewRows];
  if(!rows.length){
    toast('No status changes found today — check Activity Log for details','info');
    return;
  }
  showReport(`⚡ Today's Win/Loss — ${fmtDate(td)}`, ['Client','RM','Change','Reason'], rows);
}

// Show all Active + Inactive clients for a specific RM when clicked in the RM-wise split table
function showEqActivityRmClients(rmName){
  const allClients = window.__eqActivityAllRows || [];
  const filtered = allClients.filter(c=>(c.rm||'').trim().toUpperCase()===rmName.trim().toUpperCase());
  if(!filtered.length){ toast('No clients found for '+rmName,'info'); return; }
  const active   = filtered.filter(c=>c.status==='Active');
  const inactive = filtered.filter(c=>c.status==='Inactive'||(!c.last_trade_date&&c.status!=='Active'));
  const rows = [
    ...active.sort((a,b)=>(a.name||'').localeCompare(b.name||'')).map(c=>[
      escapeHtml(c.name||'—'),
      c.code||'—',
      c.mobile||'—',
      `<span style="color:var(--green);font-weight:700">Active</span>`,
      c.last_trade_date ? fmtDate(c.last_trade_date) : '—'
    ]),
    ...inactive.sort((a,b)=>(a.name||'').localeCompare(b.name||'')).map(c=>[
      escapeHtml(c.name||'—'),
      c.code||'—',
      c.mobile||'—',
      `<span style="color:var(--red);font-weight:700">Inactive</span>`,
      c.last_trade_date ? fmtDate(c.last_trade_date) : 'Never'
    ])
  ];
  showReport(
    `📋 ${rmName} — Clients (${active.length} Active, ${inactive.length} Inactive)`,
    ['Name','Code','Mobile','Status','Last Trade'],
    rows
  );
}

function filterEqActivityByRm(rmName){
  const sel = document.getElementById('eqActivityRmFilter');
  if(sel){ sel.value = rmName; onEqActivityRmFilterChange(); }
  const card = document.getElementById('eqActivityCard');
  if(card) card.scrollIntoView({behavior:'smooth', block:'center'});
}

// rmName (optional, admin-only): when passed, shows that specific RM's own
// day-by-day history (their own login scope) instead of the admin's 'ALL'
// base-wide history. Relies on that RM having opened their dashboard on a
// given day (which is when their own snapshot gets saved) — if an RM hasn't
// logged in on a particular date, that date simply won't have an entry for them.
// Returns the individual clients whose equity status changed on a given
// calendar date (YYYY-MM-DD), read from the activity_logs audit trail.
// rmName (optional): restrict to that RM's clients only — used when viewing
// a single RM's own history rather than the admin's whole-base 'ALL' view.
function statusChangesByDate(dateStr, rmName){
  // Non-admin viewing their OWN dashboard calls this without an explicit
  // rmName — must still scope to just their own clients, never the whole
  // base. Only admin (or an explicit rmName drill-in) sees everyone/anyone.
  const effectiveRm = rmName || (CU && CU.role!=='admin' ? CU.name : null);
  const logs = DB.get('activity_logs')||[];
  return logs.filter(l=>{
      if(l.seg!=='equity' || l.type!=='edit' || !Array.isArray(l.changes)) return false;
      if(String(l.date||'').slice(0,10)!==dateStr) return false;
      if(effectiveRm && (l.rm||'').trim().toUpperCase()!==effectiveRm.trim().toUpperCase()) return false;
      return l.changes.some(ch=>ch.field==='status');
    })
    .map(l=>{
      const ch = l.changes.find(ch=>ch.field==='status');
      return {name:l.client_name||'—', rm:l.rm||'—', old:ch.old, new:ch.new};
    });
}

// Drilldown from the date-wise history table: lists which named clients
// flipped Active/Inactive on that specific date (with a Back link to return
// to the date-wise table).
function showStatusChangeDrilldown(dateStr, rmName){
  const changes = statusChangesByDate(dateStr, rmName);
  const rows = changes.map(c=>[escapeHtml(c.name), escapeHtml(c.rm), c.old, c.new]);
  showReport(`📋 Status Changes — ${fmtDate(dateStr)}${rmName?' ('+rmName+')':''}`,
    ['Client','RM','Old Status','New Status'], rows);
  // Add a Back link above the table so admins can return to the date-wise view.
  const body = document.getElementById('reportModalBody');
  if(body){
    const back = document.createElement('div');
    back.style.cssText='margin-bottom:10px';
    back.innerHTML = `<span style="text-decoration:underline;cursor:pointer;color:var(--blue);font-size:.8rem" onclick="showEqActivityHistory(${rmName?`'${escapeHtml(rmName).replace(/'/g,"\\'")}'`:''})">← Back to date-wise history</span>`;
    body.prepend(back);
  }
}

// Fallback for dates where the ▲/▼ count moved but no activity_log entry
// exists (change happened before audit logging was switched on, or through
// an untracked path). Shortlists Inactive clients whose last trade date is
// right around the 365-day cutoff — the client that silently crossed over
// is almost always in this narrow list, so admin/RM can eyeball and confirm.
function showLikelyStatusFlipCandidates(dateStr, dActive, rmName){
  const effectiveRm = rmName || (CU && CU.role!=='admin' ? CU.name : null);
  const allEq = DB.get('eq_clients')||[];
  const scopeFilter = c => !effectiveRm || (c.rm||'').trim().toUpperCase()===effectiveRm.trim().toUpperCase();

  // Active count went UP that day — the only way a client flips Inactive→Active on a specific
  // date is by actually trading that day, so look for an exact last_trade_date match instead of
  // the "near 365-day threshold" guess (which is for the opposite, became-inactive, direction).
  if(dActive>0 && dateStr){
    const rows = allEq.filter(c=>scopeFilter(c) && String(c.last_trade_date||'').slice(0,10)===dateStr)
      .map(c=>[c.rm||'—', c.code||'', c.name, fmtDate(c.last_trade_date)||'—', c.status||'—']);
    if(!rows.length){
      toast('No client record found trading on this date — the status must have changed for another reason','info');
      return;
    }
    showReport(`🔍 Likely Clients — traded on ${fmtDate(dateStr)} (best guess)`, ['RM','Code','Name','Trade Date','Status'], rows);
    return;
  }

  // Active count went DOWN (or unknown direction) — shortlist Inactive clients whose last trade
  // date is right around the 365-day cutoff, since that's almost always the client who quietly
  // crossed over.
  const eq = allEq.filter(c=>c.status==='Inactive');
  const scoped = eq.filter(scopeFilter);
  const rows = scoped.map(c=>({...c, days:daysDiff(c.last_trade_date)}))
    .filter(c=>c.days!=null && c.days>=EQ_INACTIVE_DAYS+1 && c.days<=EQ_INACTIVE_DAYS+10)
    .sort((a,b)=>a.days-b.days)
    .map(c=>[c.rm||'—', c.code||'', c.name, fmtDate(c.last_trade_date)||'—', c.days+'d ago']);
  if(!rows.length){
    toast("No exact match found around this threshold — this client's status must have changed for another reason",'info');
    return;
  }
  showReport('🔍 Likely Client — crossed 1-year no-trade mark recently (best guess)', ['RM','Code','Name','Last Trade','Days Ago'], rows);
}

function showEqActivityHistory(rmName){
  let scope;
  if(rmName){
    scope = rmScopeIdByName(rmName);
    if(!scope){ toast(`No history record found for ${rmName}`,'info'); return; }
  } else {
    scope = CU.role==='admin' ? 'ALL' : (CU.id || CU.name);
  }
  let hist=[];
  try{ hist = JSON.parse(localStorage.getItem('dninvest_eq_activity_snapshots')||'[]'); }catch(e){ hist=[]; }
  const rows = hist.filter(x=>x&&x.scope===scope).sort((a,b)=>String(b.date).localeCompare(String(a.date)));
  if(!rows.length){ toast(rmName?`No history built yet for ${rmName} — it will be created once their dashboard is opened tomorrow`:'No history built yet — please check again after opening the dashboard tomorrow','info'); return; }
  const table = rows.map((r,i)=>{
    const prev = rows[i+1]; // next row is the earlier date (desc-sorted)
    // A snapshot with total===0 is a placeholder/incomplete day (e.g. this
    // RM's scope had no reliable data yet) — treat it like "no data", not a
    // real baseline, so it doesn't produce a fake ▲198-style delta.
    const prevValid = (prev && prev.total>0) ? prev : null;
    const dA = prevValid ? r.active-prevValid.active : null;
    const dI = prevValid ? r.inactive-prevValid.inactive : null;
    const fmtDiff = d => d==null ? '—' : d===0 ? '0' : (d>0?'▲'+d:'▼'+Math.abs(d));
    const changed = statusChangesByDate(r.date, rmName);
    const rmArg = rmName ? `'${escapeHtml(rmName).replace(/'/g,"\\'")}'` : '';
    // Offer the best-guess finder whenever we don't have a logged, named list
    // — this covers both "Δ moved but no log entry" AND "no valid baseline
    // yet to even compute a Δ" (e.g. an RM's very first tracked day), so RM
    // and Admin both get a way to find the client, not just a dead-end "—".
    let changedCell;
    if(changed.length){
      changedCell = `<span style="text-decoration:underline;cursor:pointer;color:var(--blue)" onclick="showStatusChangeDrilldown('${r.date}',${rmArg})" title="Click to see client names">${changed.length} client${changed.length>1?'s':''} ▶</span>`;
    } else {
      const dActiveArg = dA==null ? 0 : dA;
      changedCell = `<span style="text-decoration:underline;cursor:pointer;color:var(--orange,#D97706)" onclick="showLikelyStatusFlipCandidates('${r.date}',${dActiveArg},${rmArg})" title="No log entry for this date — best-guess shortlist of clients">🔍 find</span>`;
    }
    return [fmtDate(r.date), r.active, fmtDiff(dA), r.inactive, fmtDiff(dI), r.total, changedCell];
  });
  const label = rmName ? ` (${rmName})` : (scope==='ALL'?' (All)':'');
  showReport('📊 Active/Inactive — Date-wise History'+label,
    ['Date','Active','Δ Active','Inactive','Δ Inactive','Total','Changed Clients'], table);
}

function sc(n,l,cls,seg){
  const segBadge=seg?`<span class="badge ${seg==='equity'?'b-eq':'b-mf'}" style="float:right;margin-top:2px">${seg==='equity'?'EQ':'MF'}</span>`:'';
  const len=String(n).length;
  const sizeCls = len>9?'stat-n-sm':len>7?'stat-n-md':'';
  return `<div class="stat-card ${cls}" onclick="showPage('${seg==='equity'?'eq-clients':seg==='mf'?'mf-clients':'eq-clients'}')">
    ${segBadge}<div class="stat-n ${sizeCls}">${n}</div><div class="stat-l">${l}</div></div>`;
}
function fmtNum(n){ if(n>=10000000) return (n/10000000).toFixed(2)+'Cr'; if(n>=100000) return (n/100000).toFixed(2)+'L'; if(n>=1000) return (n/1000).toFixed(1)+'K'; return Math.round(n); }

function updateSeminarBlink(){
  const _semNav=document.getElementById('nav-seminars');
  if(!_semNav) return;
  const _t=today();
  const _hasUpcoming=(DB.get('seminars')||[]).some(s=>s.date && s.date>=_t);
  _semNav.classList.toggle('blink-seminar', _hasUpcoming);
}
setInterval(updateSeminarBlink, 5000);

// Auto-refresh the whole CRM every 30 minutes so data always stays up to date.
// Guarded: if the user is mid-way through filling a form (a modal is open, or
// they're actively typing in a text field) the reload is postponed instead of
// firing blindly — otherwise an RM's unsaved entry (new client, lead, call
// note, etc.) would silently vanish the moment the 30-min timer landed
// mid-edit, which looked to the RM like their update "got auto-deleted".
// It keeps re-checking every minute until the coast is clear, then reloads.
function _safeAutoReload(){
  const modalOpen = document.querySelector('.modal-overlay.open, .modal.open');
  const ae = document.activeElement;
  const typing = ae && (ae.tagName==='INPUT' || ae.tagName==='TEXTAREA' || ae.isContentEditable) && ae.value;
  if(modalOpen || typing){
    setTimeout(_safeAutoReload, 60*1000); // recheck in 1 min
    return;
  }
  location.reload();
}
setInterval(_safeAutoReload, 30*60*1000);

function updateBadges(){
  const eq=getMyEqClients(), mf=getMyMfClients();
  const activeEq=getActiveEqClients();
  document.getElementById('nb-leads').textContent=getMyLeads().length;
  document.getElementById('nb-seminars').textContent=getMySeminars().length;
  updateSeminarBlink();
  document.getElementById('nb-eq').textContent=eq.length;
  document.getElementById('nb-mf').textContent=mf.length;
  document.getElementById('nb-sip').textContent=mf.filter(c=>c.sip_amount>0).length;
  const eqf=activeEq.filter(c=>c.next_call&&c.next_call<=today()).length;
  const mff=mf.filter(c=>c.next_call&&c.next_call<=today()).length;
  document.getElementById('nb-eqf').textContent=eqf;
  document.getElementById('nb-mff').textContent=mff;
  // Mobile bottom-nav follow-up badge (Equity + MF due today combined)
  const mnb=document.getElementById('mnav-fu-badge');
  if(mnb){ const t=eqf+mff; mnb.textContent=t>99?'99+':t; mnb.style.display=t>0?'':'none'; }
  document.getElementById('nb-eqnt').textContent=activeEq.filter(c=>daysDiff(c.last_trade_date)>=30).length;
  document.getElementById('nb-eqnc').textContent=activeEq.filter(c=>{const d=daysDiff(c.last_call_date); return d===null||d>=60;}).length;
  document.getElementById('nb-mfnc').textContent=mf.filter(c=>{const d=daysDiff(c.last_call_date); return d===null||d>=60;}).length;
  try{ const sqb=document.getElementById('nb-eqsq'); if(sqb){ const n=sqMyRows().length; sqb.textContent=n; sqb.style.display=n>0?'':'none'; } if(typeof sqUpdateBell==='function') sqUpdateBell(); }catch(e){}

  // Admin-only: Pending counts for MF Transactions and Demat (nav badge + dashboard card)
  if(CU && CU.role==='admin'){
    const mfPending = getMfBizEntries().filter(e=>(e.status||'Pending')==='Pending').length;
    const dmPending = getEqDematEntries().filter(e=>(e.status||'Pending')==='Pending').length;

    // MF Transactions nav badge
    const nbMf = document.getElementById('nb-mftxn-pending');
    if(nbMf){ nbMf.textContent=mfPending; nbMf.style.display=mfPending>0?'':'none'; }

    // Dashboard card pending pills
    const dmfCard = document.getElementById('dash-mftxn-pending');
    const dmfCnt  = document.getElementById('dash-mftxn-pending-count');
    if(dmfCard && dmfCnt){ dmfCnt.textContent=mfPending; dmfCard.style.display=mfPending>0?'':'none'; }

    const ddCard = document.getElementById('dash-demat-pending');
    const ddCnt  = document.getElementById('dash-demat-pending-count');
    if(ddCard && ddCnt){ ddCnt.textContent=dmPending; ddCard.style.display=dmPending>0?'':'none'; }
  }
}

// ══════════════════════════════════════════
// EQUITY TABLE
// ══════════════════════════════════════════
function filterEq(){ eqPage=1; renderEqTable(); }
function toggleFuFilterMenu(seg){
  const menu = document.getElementById(seg+'-fu-menu');
  if(!menu) return;
  const isOpen = menu.style.display==='block';
  document.querySelectorAll('.fu-filter-menu').forEach(m=>m.style.display='none');
  if(!isOpen){
    const anchor = menu.parentElement;
    const r = anchor.getBoundingClientRect();
    menu.style.position='fixed';
    menu.style.top=(r.bottom)+'px';
    menu.style.left='auto';
    menu.style.right=(window.innerWidth-r.right)+'px';
    menu.style.display='block';
    setTimeout(()=>{
      document.addEventListener('click', function closeMenu(){
        menu.style.display='none';
        document.removeEventListener('click', closeMenu);
      }, {once:true});
    },0);
  } else {
    menu.style.display='none';
  }
}
function setFuFilter(seg,val){
  const sel = document.getElementById(seg+'-followup-filter');
  if(sel){ sel.value = val; }
  const menu = document.getElementById(seg+'-fu-menu');
  if(menu) menu.style.display='none';
  if(seg==='eq') filterEq(); else filterMf();
}
function sortEqTable(colIndex){
  if(eqSortField===colIndex) eqSortDir = -eqSortDir;
  else { eqSortField=colIndex; eqSortDir=1; }
  eqPage=1;
  _saveSortState();
  renderEqTable();
}

// ══════════════════════════════════════════
// GENERIC SORTABLE TABLE HELPER (for follow-up,
// no-trade/no-call alerts, SIP tracker, seminars,
// reports etc.)
// ══════════════════════════════════════════
let _sortState = {};
function clickSort(tableKey, field, type, renderFn){
  const cur = _sortState[tableKey] || {field:null, dir:1};
  if(cur.field===field){
    cur.dir = -cur.dir;
  } else {
    cur.field=field;
    // Date columns: pehli click descending (latest first)
    cur.dir = (type==='date') ? -1 : 1;
  }
  _sortState[tableKey]=cur;
  _sortState[tableKey].type=type;
  renderFn();
}
function applySort(data, tableKey, getters){
  const st=_sortState[tableKey];
  if(!st || !st.field || !getters[st.field]) return data;
  const cfg = getters[st.field];
  const get = cfg.get, type = cfg.type;
  return data.slice().sort((a,b)=>{
    let va=get(a), vb=get(b);
    if(type==='num'){ va=parseFloat(va)||0; vb=parseFloat(vb)||0; return st.dir*(va-vb); }
    if(type==='date'){
      va=va||''; vb=vb||'';
      if(!va && !vb) return 0;
      if(!va) return 1; if(!vb) return -1;
      // Robust date parse — YYYY-MM-DD preferred, also handles DD-MMM-YY/YYYY
      function parseD(s){
        if(!s) return 0;
        // YYYY-MM-DD (standard storage format)
        const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
        if(iso) return new Date(+iso[1], +iso[2]-1, +iso[3]).getTime();
        // DD-MMM-YY or DD-MMM-YYYY (e.g. 3-Jun-26 or 3-Jun-2026)
        const mn2={'jan':0,'feb':1,'mar':2,'apr':3,'may':4,'jun':5,'jul':6,'aug':7,'sep':8,'oct':9,'nov':10,'dec':11};
        const dmy = s.match(/^(\d{1,2})[-\/]([a-zA-Z]{3})[-\/](\d{2,4})$/);
        if(dmy){
          let yr = +dmy[3];
          if(yr < 100) yr += yr < 50 ? 2000 : 1900;
          return new Date(yr, mn2[dmy[2].toLowerCase()]||0, +dmy[1]).getTime();
        }
        return new Date(s).getTime()||0;
      }
      return st.dir*(parseD(va)-parseD(vb));
    }
    va=(va==null?'':va).toString().toLowerCase(); vb=(vb==null?'':vb).toString().toLowerCase();
    return st.dir*va.localeCompare(vb, undefined, {numeric:true});
  });
}
function sortArrow(tableKey, field){
  const st=_sortState[tableKey];
  return (st && st.field===field) ? (st.dir===1?' ▲':' ▼') : '';
}
function sortTh(label, tableKey, field, type, renderFnName){
  return `<th onclick="clickSort('${tableKey}','${field}','${type}',${renderFnName})" style="cursor:pointer" title="Click to sort">${label}${sortArrow(tableKey,field)}</th>`;
}

function resetFilters(t){
  ['search','status','rm','comeback','followup-filter','last-call-from','last-call-to','next-call-from','next-call-to','last-trade-from','last-trade-to','last-biz-from','last-biz-to'].forEach(f=>{
    const el=document.getElementById(t+'-'+f); if(el) el.value='';
  });
  if(t==='eq') filterEq();
  else if(t==='mf') filterMf();
  else if(t==='leads') filterLeads();
}

// ══════════════════════════════════════════
// RESIZABLE TABLE COLUMNS — generic system (Equity, MF, Leads)
// Drag a header's right edge to resize; widths persist per table.
// ══════════════════════════════════════════
const COL_CFG = {
  eq: {
    cont:'eq-table',
    keys:['code','name','mobile','rm','risk','acbal','lasttrade','lastcall','nextcall','followup','remarks','actions','status','call'],
    def:{code:64,name:150,mobile:92,rm:64,risk:78,acbal:72,lasttrade:88,lastcall:76,nextcall:76,followup:92,remarks:110,actions:88,status:60,call:42}
  },
  mf: {
    cont:'mf-table',
    keys:['name','mobile','pan','rm','aum','sipamt','sipcnt','lastcall','nextcall','followup','remarks','actions','status','call'],
    def:{name:150,mobile:92,pan:88,rm:64,aum:82,sipamt:78,sipcnt:60,lastcall:76,nextcall:76,followup:92,remarks:110,actions:88,status:60,call:42}
  },
  leads: {
    cont:'leads-table',
    keys:['name','mobile','rm','source','calls','lastcall','nextcall','followup','remarks','actions'],
    def:{name:150,mobile:92,rm:64,source:170,calls:52,lastcall:76,nextcall:76,followup:92,remarks:120,actions:88}
  }
};
function getColW(tid){
  const cfg=COL_CFG[tid]; if(!cfg) return {};
  let saved={};
  try{ saved=JSON.parse(localStorage.getItem('dninvest_colw_'+tid)||'{}'); }catch(e){}
  const w={};
  cfg.keys.forEach(k=>{ w[k]=(saved[k]&&saved[k]>=24)?saved[k]:cfg.def[k]; });
  return w;
}
function saveColW(tid,w){ try{ localStorage.setItem('dninvest_colw_'+tid, JSON.stringify(w)); }catch(e){} }
// Returns "<colgroup>...</colgroup>" plus the total table width, for a table.
function colGroup(tid, hasCheckbox){
  const cfg=COL_CFG[tid]; const W=getColW(tid);
  const cg=(hasCheckbox?'<col style="width:30px">':'')+cfg.keys.map(k=>`<col style="width:${W[k]}px">`).join('');
  const total=(hasCheckbox?30:0)+cfg.keys.reduce((s,k)=>s+W[k],0);
  return {cg:`<colgroup>${cg}</colgroup>`, total};
}
// Attach drag handles to a rendered table's headers.
function enableColResize(tid){
  const cfg=COL_CFG[tid]; if(!cfg) return;
  const tbl=document.querySelector('#'+cfg.cont+' table'); if(!tbl) return;
  const cols=tbl.querySelectorAll('colgroup col');
  const ths=tbl.querySelectorAll('thead th');
  if(!cols.length||!ths.length) return;
  const hasCb=(CU&&CU.role==='admin');
  const off=hasCb?1:0;
  const W=getColW(tid);
  cfg.keys.forEach((key,i)=>{
    const th=ths[i+off], col=cols[i+off];
    if(!th||!col) return;
    const handle=document.createElement('span');
    handle.className='eqrz';
    handle.addEventListener('mousedown', e=>_rzStart(e,tid,col,key,W));
    handle.addEventListener('touchstart', e=>_rzStart(e,tid,col,key,W), {passive:false});
    th.appendChild(handle);
  });
}
let _rz=null;
function _rzStart(e,tid,col,key,W){
  e.preventDefault(); e.stopPropagation();
  const pt=e.touches?e.touches[0]:e;
  _rz={tid,col,key,W,startX:pt.clientX,startW:parseInt(col.style.width)||COL_CFG[tid].def[key]};
  document.body.classList.add('eq-resizing');
  e.target.classList.add('active');
  document.addEventListener('mousemove',_rzMove);
  document.addEventListener('mouseup',_rzEnd);
  document.addEventListener('touchmove',_rzMove,{passive:false});
  document.addEventListener('touchend',_rzEnd);
}
function _rzMove(e){
  if(!_rz) return;
  if(e.cancelable) e.preventDefault();
  const pt=e.touches?e.touches[0]:e;
  let nw=_rz.startW+(pt.clientX-_rz.startX);
  if(nw<30) nw=30;
  const old=parseInt(_rz.col.style.width)||_rz.startW;
  _rz.col.style.width=nw+'px';
  const tbl=document.querySelector('#'+COL_CFG[_rz.tid].cont+' table');
  if(tbl){ const tw=(parseInt(tbl.style.width)||tbl.offsetWidth)+(nw-old); tbl.style.width=tw+'px'; }
}
function _rzEnd(){
  if(_rz){ _rz.W[_rz.key]=parseInt(_rz.col.style.width)||COL_CFG[_rz.tid].def[_rz.key]; saveColW(_rz.tid,_rz.W); }
  _rz=null;
  document.body.classList.remove('eq-resizing');
  document.querySelectorAll('.eqrz.active').forEach(el=>el.classList.remove('active'));
  document.removeEventListener('mousemove',_rzMove);
  document.removeEventListener('mouseup',_rzEnd);
  document.removeEventListener('touchmove',_rzMove);
  document.removeEventListener('touchend',_rzEnd);
}

function renderEqTable(){
  // Self-heal: if any path (client edit, call update, background sync) cleared the
  // chosen sort, restore the user's last sort from localStorage so the table never
  // silently reverts to default order.
  if(eqSortField===null){
    try{ const _ss=JSON.parse(localStorage.getItem('dninvest_sort_state')||'{}');
      if(_ss.eqField!==undefined && _ss.eqField!==null){ eqSortField=_ss.eqField; eqSortDir=_ss.eqDir||1; } }catch(e){}
  }
  let data = getMyEqClients();
  // Parse the RMS risk map ONCE per render (was being re-parsed per comparison
  // during sort → tens of thousands of JSON.parse of a 2700-entry object → hang).
  const _riskMap = (getEqRisk().code) || {};
  const _riskOf = code => (code ? _riskMap[String(code).trim()] : null) || null;
  const q=(document.getElementById('eq-search')||{value:''}).value.toLowerCase();
  const st=(document.getElementById('eq-status')||{value:''}).value;
  const rm=(document.getElementById('eq-rm')||{value:''}).value;
  const comebackFilter=(document.getElementById('eq-comeback')||{value:''}).value;
  const fu=(document.getElementById('eq-followup-filter')||{value:''}).value;
  const lcFrom=(document.getElementById('eq-last-call-from')||{value:''}).value;
  const lcTo=(document.getElementById('eq-last-call-to')||{value:''}).value;
  const ncFrom=(document.getElementById('eq-next-call-from')||{value:''}).value;
  const ncTo=(document.getElementById('eq-next-call-to')||{value:''}).value;
  const ltFrom=(document.getElementById('eq-last-trade-from')||{value:''}).value;
  const ltTo=(document.getElementById('eq-last-trade-to')||{value:''}).value;

  if(q){ const qt=q.trim(); data=data.filter(c=>(c.name||'').toLowerCase().includes(qt)||(c.mobile||'').includes(qt)||(c.code||'').toLowerCase().includes(qt)||(c.rm||'').toLowerCase().includes(qt)); }
  if(st==='DNC') data=data.filter(c=>c.do_not_call===true);
  else if(st) data=data.filter(c=>c.status===st && !c.do_not_call);
  else data=data.filter(c=>!c.do_not_call); // default: DNC hide
  if(rm==='__NONE__') data=data.filter(c=>!(c.rm||'').trim());
  else if(rm) data=data.filter(c=>rmMatches(c.rm, rm));
  if(comebackFilter) data=data.filter(c=>c.comeback_tag===comebackFilter && c.comeback_date===c.last_trade_date);
  if(fu==='pending') data=data.filter(c=>(c.followup_status||'').trim().toUpperCase()==='PENDING'||c.next_call);
  if(fu==='today') data=data.filter(c=>c.next_call===today());
  if(fu==='overdue') data=data.filter(c=>c.next_call&&c.next_call<today());
  if(fu==='__BLANK__') data=data.filter(c=>!(c.followup_status||'').trim());
  if(fu&&!['pending','today','overdue','__BLANK__'].includes(fu)) data=data.filter(c=>(c.followup_status||'').trim().toUpperCase()===fu.trim().toUpperCase());
  if(lcFrom||lcTo) data=data.filter(c=>{
    const d=c.last_call_date; if(!d) return false;
    if(lcFrom && d<lcFrom) return false;
    if(lcTo && d>lcTo) return false;
    return true;
  });
  if(ncFrom||ncTo) data=data.filter(c=>{
    const d=c.next_call; if(!d) return false;
    if(ncFrom && d<ncFrom) return false;
    if(ncTo && d>ncTo) return false;
    return true;
  });
  if(ltFrom||ltTo) data=data.filter(c=>{
    const d=c.last_trade_date; if(!d) return false;
    if(ltFrom && d<ltFrom) return false;
    if(ltTo && d>ltTo) return false;
    return true;
  });

  // Column AutoFilter
  data = CF.applyEq(data);

  eqFiltered=data;
  const lbl=document.getElementById('eq-total-label');
  if(lbl) lbl.textContent=`(${data.length})`;
  const rc=document.getElementById('eq-count');
  if(rc) rc.textContent=data.length+' clients';

  // Sort full filtered dataset (not just current page) by selected field
  const eqFieldMap = {0:'code',1:'name',2:'mobile',3:'rm',4:'asset_value',5:'last_trade_date',6:'last_call_date',7:'next_call',8:'followup_status',9:'remarks'};
  if(eqSortField!==null){
    const field = eqFieldMap[eqSortField];
    data = data.slice().sort((a,b)=>{
      // Risk Val (col 10) — one click: biggest + on top → down to biggest −.
      // Second click: biggest − (debit) on top → down to biggest +.
      // Clients WITHOUT risk data always sink to the very bottom, either way.
      if(eqSortField===10 || eqSortField===11){
        const ra=_riskOf(a.code), rb=_riskOf(b.code);
        const ha=ra!=null, hb=rb!=null;
        if(!ha && !hb) return 0;
        if(!ha) return 1;   // a has no data → push to bottom
        if(!hb) return -1;  // b has no data → push to bottom
        const fld = eqSortField===11 ? 'ac_bal' : 'risk_val';
        const va=ra[fld]||0, vb=rb[fld]||0;
        if(va===vb) return 0;
        return -eqSortDir*(va-vb); // dir=1 → + on top; dir=-1 → − on top
      }
      let va=a[field], vb=b[field];
      // numeric fields
      if(['asset_value'].includes(field)){
        va = parseFloat(va)||0; vb = parseFloat(vb)||0;
        return eqSortDir*(va-vb);
      }
      // date fields (ISO strings sort correctly as strings, but blanks should go last)
      if(['last_call_date','last_trade_date','next_call'].includes(field)){
        va = va||''; vb = vb||'';
        if(!va && !vb) return 0;
        if(!va) return 1; if(!vb) return -1;
        return eqSortDir*(va<vb?-1:va>vb?1:0);
      }
      va = (va||'').toString().toLowerCase(); vb=(vb||'').toString().toLowerCase();
      return eqSortDir*va.localeCompare(vb, undefined, {numeric:true});
    });
    eqFiltered = data;
  }

  const rows=data.slice((eqPage-1)*PG_SIZE, eqPage*PG_SIZE);
  const isAdmin = CU.role==='admin';
  const _eqArrow=i=>eqSortField===i?(eqSortDir===1?' ▲':' ▼'):'';
  const _eqSortTh=(i,label,cfCol)=>CF.th('eq',cfCol,`<span onclick="sortEqTable(${i})" style="cursor:pointer">${label}${_eqArrow(i)}</span>`);
  // Column widths (%) in display order. Checkbox col only exists for admin.
  // Order: Code Name Mobile RM Risk AcBal LastTrade LastCall NextCall Followup Remarks Actions Status Call
  const _eqCG = colGroup('eq', isAdmin);
  let h=`<table style="width:${_eqCG.total}px">${_eqCG.cg}<thead><tr>${BULK.th('eq')}
    <th style="cursor:pointer" onclick="sortEqTable(0)">Code${_eqArrow(0)}</th>
    ${_eqSortTh(1,'Name','name')}
    ${_eqSortTh(2,'Mobile','mobile')}
    ${_eqSortTh(3,'RM','rm')}
    <th onclick="sortEqTable(10)" style="cursor:pointer;white-space:nowrap">Risk Val${_eqArrow(10)}</th>
    <th onclick="sortEqTable(11)" style="cursor:pointer;white-space:nowrap">Ac Bal${_eqArrow(11)}</th>
    <th onclick="sortEqTable(5)" style="cursor:pointer;white-space:nowrap">Last Trade${_eqArrow(5)}</th>
    <th onclick="sortEqTable(6)" style="cursor:pointer;white-space:nowrap">Last Call${_eqArrow(6)}</th>
    <th onclick="sortEqTable(7)" style="cursor:pointer;white-space:nowrap">Next Call${_eqArrow(7)}</th>
    ${CF.th('eq','followup_status',`<span onclick="sortEqTable(8)" style="cursor:pointer">Follow-up${_eqArrow(8)}</span>`)}
    <th onclick="sortEqTable(9)" style="cursor:pointer">Remarks${_eqArrow(9)}</th>
    <th>Actions</th>${CF.th('eq','status','Status')}<th>Call</th>
  </tr></thead><tbody>`;

  if(!rows.length) h+=`<tr><td colspan="15" style="text-align:center;padding:36px;color:#bbb">No equity clients. <a href="#" onclick="openAddClient('equity')" style="color:var(--navy)">Add one?</a></td></tr>`;

  rows.forEach(c=>{
    const days=daysDiff(c.last_trade_date);
    const rowCls=days>=90?'row-alert':c.status==='Inactive'?'row-inactive':'';
    const comebackTag = (c.comeback_tag && c.comeback_date===c.last_trade_date) ? c.comeback_tag : '';
    const comebackBg = comebackTag==='yellow'?'#fef9e7':comebackTag==='green'?'#eafaf0':comebackTag==='blue'?'#eaf2ff':'';
    const rowStyle = comebackBg ? ` style="background:${comebackBg}"` : '';
    const fuBadge=c.next_call?(c.next_call<today()?'b-pending':c.next_call===today()?'b-active':'b-na'):'b-na';
    h+=`<tr class="${rowCls}"${rowStyle}${comebackTag?` title="Comeback trade — ${comebackTag==='blue'?'~1':comebackTag==='green'?'~3':'~6'}+ month gap"`:''}>${BULK.td('eq',c.id)}
      <td>${c.code||'—'}</td>
      <td style="font-weight:600;cursor:context-menu" oncontextmenu="showClientSeminarMenu(event,'${c.id}','equity')" title="Right-click → Add to Seminar">${c.name}${(c.asset_value>=500000)?'<span title="HNI — Asset Value ≥ ₹5L" style="margin-left:4px;font-size:.65rem;background:#7c3aed;color:#fff;border-radius:4px;padding:0 4px;font-weight:700;vertical-align:middle">H</span>':''}</td>
      <td><a href="tel:${c.mobile}" style="color:var(--navy);text-decoration:none">${c.mobile||'—'}</a></td>
      <td>${c.rm||'—'}</td>
      <td style="white-space:nowrap">${(()=>{const rk=_riskOf(c.code);return rk?`<a href="#" onclick="openEqRisk('${(c.code||'').replace(/'/g,"\\'")}','${(c.name||'').replace(/'/g,"\\'")}');return false" style="text-decoration:none">${fmtRiskMoney(rk.risk_val)}</a>`:'<span style="color:#ccc">—</span>';})()}</td>
      <td style="white-space:nowrap">${(()=>{const rk=_riskOf(c.code);return rk?`<a href="#" onclick="openEqRisk('${(c.code||'').replace(/'/g,"\\'")}','${(c.name||'').replace(/'/g,"\\'")}');return false" style="text-decoration:none">${fmtRiskMoney(rk.ac_bal)}</a>`:'<span style="color:#ccc">—</span>';})()}</td>
      <td>${fmtDate(c.last_trade_date)||'—'}${days!==null?`<br><small style="color:${days>=90?'var(--red)':days>=30?'var(--orange)':'var(--green)'}">${days}d ago</small>`:''}</td>
      <td>${fmtDate(c.last_call_date)||'—'}</td>
      <td>${fmtDate(c.next_call)||'—'}</td>
      <td class="eqc-wrap"><span class="badge ${fuBadge}">${c.followup_status||'—'}</span>${c.do_not_call?'<br><span style="color:var(--red);font-weight:700;font-size:.72rem">🚫 DNC</span>':''}</td>
      <td class="eqc-rmk" title="${c.remarks||''}">${c.remarks||'—'}</td>
      <td>
        ${CU.role!=='backoffice'?`<button class="btn-icon" onclick="editClient('${c.id}','equity')" title="Edit">✏️</button>`:''}
        <button class="btn-icon" onclick="viewClient('${c.id}','equity')" title="View">👁</button>
        ${CU.role!=='backoffice'?`<button class="btn-icon" onclick="addEquityToMf('${c.id}')" title="Add to Mutual Fund" style="color:var(--teal)">🏦</button>`:''}
        ${CU.role!=='backoffice'?`<button class="btn-icon" onclick="quickAddOpFromClient('${c.id}','equity')" title="Map to Other Product" style="color:var(--purple,#7c3aed)">🎯</button>`:''}
        ${CU.role==='admin'?`<button class="btn-icon" onclick="confirmDeleteClient('${c.id}','equity','${(c.name||'').replace(/'/g,"\\'")}')" title="Delete" style="color:var(--red)">🗑️</button>`:''}
      </td>
      <td><span class="badge ${c.status==='Active'?'b-active':c.status==='Closed'?'b-closed':'b-inactive'}">${c.status||'—'}</span></td>
      <td>
        ${c.mobile?`<a href="https://wa.me/91${c.mobile}" target="_blank" class="btn-icon" title="WhatsApp">💬</a>`:''}
      </td>
      </tr>`;
  });
  h+='</tbody></table>';
  document.getElementById('eq-table').innerHTML=h;
  enableColResize('eq');
  BULK.afterRender('eq');
  renderPg('eq',data.length,eqPage);
}

// ══════════════════════════════════════════
// MF TABLE
// ══════════════════════════════════════════
function filterMf(){ mfPage=1; renderMfTable(); }
function sortMfTable(colIndex){
  if(mfSortField===colIndex) mfSortDir = -mfSortDir;
  else { mfSortField=colIndex; mfSortDir=1; }
  mfPage=1;
  _saveSortState();
  renderMfTable();
}

function changePageSize(tab, val){
  PG_SIZE = parseInt(val)||50;
  const eqSel=document.getElementById('eq-pagesize'), mfSel=document.getElementById('mf-pagesize');
  if(eqSel) eqSel.value=val;
  if(mfSel) mfSel.value=val;
  if(tab==='eq'){ eqPage=1; renderEqTable(); }
  else { mfPage=1; renderMfTable(); }
}

function renderMfTable(){
  if(mfSortField===null){
    try{ const _ss=JSON.parse(localStorage.getItem('dninvest_sort_state')||'{}');
      if(_ss.mfField!==undefined && _ss.mfField!==null){ mfSortField=_ss.mfField; mfSortDir=_ss.mfDir||1; } }catch(e){}
  }
  let data=getMyMfClients();
  const q=(document.getElementById('mf-search')||{value:''}).value.toLowerCase();
  const st=(document.getElementById('mf-status')||{value:''}).value;
  const rm=(document.getElementById('mf-rm')||{value:''}).value;
  const fu=(document.getElementById('mf-followup-filter')||{value:''}).value;
  const ncFrom=(document.getElementById('mf-next-call-from')||{value:''}).value;
  const ncTo=(document.getElementById('mf-next-call-to')||{value:''}).value;
  const lcFrom=(document.getElementById('mf-last-call-from')||{value:''}).value;
  const lcTo=(document.getElementById('mf-last-call-to')||{value:''}).value;
  const lbFrom=(document.getElementById('mf-last-biz-from')||{value:''}).value;
  const lbTo=(document.getElementById('mf-last-biz-to')||{value:''}).value;

  if(q){ const qt=q.trim(); data=data.filter(c=>(c.name||'').toLowerCase().includes(qt)||(c.mobile||'').includes(qt)||(c.pan||'').toLowerCase().includes(qt)||(c.rm||'').toLowerCase().includes(qt)); }
  if(st==='DNC') data=data.filter(c=>c.do_not_call===true);
  else if(st) data=data.filter(c=>c.status===st && !c.do_not_call);
  else data=data.filter(c=>!c.do_not_call); // default: DNC hide
  if(rm==='__NONE__') data=data.filter(c=>!(c.rm||'').trim());
  else if(rm) data=data.filter(c=>rmMatches(c.rm, rm));
  if(fu==='pending') data=data.filter(c=>(c.followup_status||'').trim().toUpperCase()==='PENDING'||c.next_call);
  if(fu==='today') data=data.filter(c=>c.next_call===today());
  if(fu==='overdue') data=data.filter(c=>c.next_call&&c.next_call<today());
  if(fu==='__BLANK__') data=data.filter(c=>!(c.followup_status||'').trim());
  if(fu&&!['pending','today','overdue','__BLANK__'].includes(fu)) data=data.filter(c=>(c.followup_status||'').trim().toUpperCase()===fu.trim().toUpperCase());
  if(ncFrom||ncTo) data=data.filter(c=>{
    const d=c.next_call; if(!d) return false;
    if(ncFrom && d<ncFrom) return false;
    if(ncTo && d>ncTo) return false;
    return true;
  });
  if(lcFrom||lcTo) data=data.filter(c=>{
    const d=c.last_call_date; if(!d) return false;
    if(lcFrom && d<lcFrom) return false;
    if(lcTo && d>lcTo) return false;
    return true;
  });
  if(lbFrom||lbTo) data=data.filter(c=>{
    const d=c.last_invest_date; if(!d) return false;
    if(lbFrom && d<lbFrom) return false;
    if(lbTo && d>lbTo) return false;
    return true;
  });

  // Column AutoFilter
  data = CF.applyMf(data);

  mfFiltered=data;
  const lbl=document.getElementById('mf-total-label');
  if(lbl) lbl.textContent=`(${data.length})`;
  const rc=document.getElementById('mf-count');
  if(rc) rc.textContent=data.length+' investors';

  const mfFieldMap = {0:'name',1:'mobile',2:'pan',3:'rm',4:'aum',5:'sip_amount',6:'sip_count',7:'last_call_date',8:'next_call',9:'followup_status',10:'remarks'};
  if(mfSortField!==null){
    const field = mfFieldMap[mfSortField];
    data = data.slice().sort((a,b)=>{
      let va=a[field], vb=b[field];
      if(['aum','sip_amount','sip_count'].includes(field)){
        va = parseFloat(va)||0; vb = parseFloat(vb)||0;
        return mfSortDir*(va-vb);
      }
      if(['next_call','last_call_date','last_invest_date'].includes(field)){
        va = va||''; vb = vb||'';
        if(!va && !vb) return 0;
        if(!va) return 1; if(!vb) return -1;
        return mfSortDir*(va<vb?-1:va>vb?1:0);
      }
      va = (va||'').toString().toLowerCase(); vb=(vb||'').toString().toLowerCase();
      return mfSortDir*va.localeCompare(vb, undefined, {numeric:true});
    });
    mfFiltered = data;
  }

  const rows=data.slice((mfPage-1)*PG_SIZE,mfPage*PG_SIZE);
  const isAdmin = CU.role==='admin';
  const _mfArrow=i=>mfSortField===i?(mfSortDir===1?' ▲':' ▼'):'';
  const _mfSortTh=(i,label,cfCol)=>CF.th('mf',cfCol,`<span onclick="sortMfTable(${i})" style="cursor:pointer">${label}${_mfArrow(i)}</span>`);
  const _mfCG = colGroup('mf', isAdmin);
  let h=`<table style="width:${_mfCG.total}px">${_mfCG.cg}<thead><tr>${BULK.th('mf')}
    ${_mfSortTh(0,'Name','name')}
    ${_mfSortTh(1,'Mobile','mobile')}
    ${_mfSortTh(2,'PAN','pan')}
    ${_mfSortTh(3,'MF RM','rm')}
    <th onclick="sortMfTable(4)" style="cursor:pointer;white-space:nowrap">AUM${_mfArrow(4)}</th>
    <th onclick="sortMfTable(5)" style="cursor:pointer;white-space:nowrap">SIP Amt${_mfArrow(5)}</th>
    <th onclick="sortMfTable(6)" style="cursor:pointer;white-space:nowrap">SIP Cnt${_mfArrow(6)}</th>
    <th onclick="sortMfTable(7)" style="cursor:pointer;white-space:nowrap">Last Call${_mfArrow(7)}</th>
    <th onclick="sortMfTable(8)" style="cursor:pointer;white-space:nowrap">Next Call${_mfArrow(8)}</th>
    ${CF.th('mf','followup_status',`<span onclick="sortMfTable(9)" style="cursor:pointer">Follow-up${_mfArrow(9)}</span>`)}
    <th onclick="sortMfTable(10)" style="cursor:pointer">Remarks${_mfArrow(10)}</th>
    <th>Actions</th>${CF.th('mf','status','Status')}<th>Call</th>
  </tr></thead><tbody>`;

  if(!rows.length) h+=`<tr><td colspan="15" style="text-align:center;padding:36px;color:#bbb">No MF investors. <a href="#" onclick="openAddClient('mf')" style="color:var(--teal)">Add one?</a></td></tr>`;

  rows.forEach(c=>{
    const fuBadge=c.next_call?(c.next_call<today()?'b-pending':c.next_call===today()?'b-active':'b-na'):'b-na';
    h+=`<tr class="row-mf">${BULK.td('mf',c.id)}
      <td style="font-weight:600;cursor:context-menu" oncontextmenu="showClientSeminarMenu(event,'${c.id}','mf')" title="Right-click → Add to Seminar">${c.name}${(c.aum>=300000)?'<span title="HNI — AUM ≥ ₹3L" style="margin-left:4px;font-size:.65rem;background:#0d9488;color:#fff;border-radius:4px;padding:0 4px;font-weight:700;vertical-align:middle">H</span>':''}</td>
      <td><a href="tel:${c.mobile}" style="color:var(--navy);text-decoration:none">${c.mobile||'—'}</a></td>
      <td>${c.pan||'—'}</td>
      <td>${c.rm||'—'}</td>
      <td>${c.aum?`<a href="#" onclick="openMfAum('${c.id}');return false" style="text-decoration:none;color:var(--navy)" title="Click for Invested / Gain-Loss / XIRR">₹${fmtNum(c.aum)}</a>`:'—'}</td>
      <td>${c.sip_amount?'₹'+fmtNum(c.sip_amount):'—'}</td>
      <td>${sipCntCell(c)}</td>
      <td>${fmtDate(c.last_call_date)||'—'}</td>
      <td>${fmtDate(c.next_call)||'—'}</td>
      <td><span class="badge ${fuBadge}">${c.followup_status||'—'}</span>${c.do_not_call?'<br><span style="color:var(--red);font-weight:700;font-size:.72rem">🚫 DNC</span>':''}</td>
      <td style="max-width:90px;overflow:hidden;text-overflow:ellipsis" title="${c.remarks||''}">${c.remarks||'—'}</td>
      <td>
        ${CU.role!=='backoffice'?`<button class="btn-icon" onclick="editClient('${c.id}','mf')" title="Edit">✏️</button>`:''}
        <button class="btn-icon" onclick="viewClient('${c.id}','mf')" title="View">👁</button>
        ${CU.role!=='backoffice'?`<button class="btn-icon" onclick="quickAddOpFromClient('${c.id}','mf')" title="Map to Other Product" style="color:#7c3aed">🎯</button>`:''}
        ${CU.role==='admin'?`<button class="btn-icon" onclick="confirmDeleteClient('${c.id}','mf','${(c.name||'').replace(/'/g,"\\'")}')" title="Delete" style="color:var(--red)">🗑️</button>`:''}
      </td>
      <td><span class="badge ${c.status==='Investor'?'b-investor':'b-prospect'}">${c.status||'—'}</span></td>
      <td>
        ${c.mobile?`<a href="https://wa.me/91${c.mobile}" target="_blank" class="btn-icon" title="WhatsApp">💬</a>`:''}
      </td>
      </tr>`;
  });
  h+='</tbody></table>';
  document.getElementById('mf-table').innerHTML=h;
  enableColResize('mf');
  BULK.afterRender('mf');
  renderPg('mf',data.length,mfPage);
}

// ══════════════════════════════════════════
// LEADS
// ══════════════════════════════════════════
function filterLeads(){ leadsPage=1; renderLeadsTable(); }

function sortLeadsTable(colIndex){
  if(leadsSortField===colIndex) leadsSortDir=-leadsSortDir;
  else { leadsSortField=colIndex; leadsSortDir=1; }
  _saveSortState();
  renderLeadsTable();
}

function renderLeadsTable(){
  if(leadsSortField===null){
    try{ const _ss=JSON.parse(localStorage.getItem('dninvest_sort_state')||'{}');
      if(_ss.leadsField!==undefined && _ss.leadsField!==null){ leadsSortField=_ss.leadsField; leadsSortDir=_ss.leadsDir||1; } }catch(e){}
  }
  let data=getMyLeads();
  // Count calls logged per lead (from shared call_logs, seg='lead')
  const _leadCallCounts={};
  (DB.get('call_logs')||[]).forEach(l=>{ if(l.seg==='lead') _leadCallCounts[l.client_id]=(_leadCallCounts[l.client_id]||0)+1; });
  window._leadCallCounts=_leadCallCounts;
  const q=(document.getElementById('leads-search')||{value:''}).value.toLowerCase();
  const rm=(document.getElementById('leads-rm')||{value:''}).value;
  const fu=(document.getElementById('leads-followup-filter')||{value:''}).value;
  const nextCallDate=(document.getElementById('leads-next-call-date')||{value:''}).value;

  if(q) data=data.filter(c=>(c.name||'').toLowerCase().includes(q)||(c.mobile||'').includes(q));
  if(rm==='__NONE__') data=data.filter(c=>!(c.rm||'').trim());
  else if(rm) data=data.filter(c=>rmMatches(c.rm, rm));
  if(fu==='today') data=data.filter(c=>c.next_call===today());
  if(fu==='overdue') data=data.filter(c=>c.next_call&&c.next_call<today());
  if(fu&&!['today','overdue'].includes(fu)) data=data.filter(c=>(c.followup_status||'').trim().toUpperCase()===fu.trim().toUpperCase());
  if(nextCallDate) data=data.filter(c=>c.next_call===nextCallDate);

  leadsFiltered=data;
  const lbl=document.getElementById('leads-total-label');
  if(lbl) lbl.textContent=`(${data.length})`;
  const rc=document.getElementById('leads-count');
  if(rc) rc.textContent=data.length+' leads';

  const leadsFieldMap={0:'name',1:'mobile',2:'rm',3:'__calls',4:'last_call',5:'next_call',6:'followup_status',7:'remarks'};
  if(leadsSortField!==null){
    const field=leadsFieldMap[leadsSortField];
    data=data.slice().sort((a,b)=>{
      if(field==='__calls'){
        const ca=window._leadCallCounts[a.id]||0, cb=window._leadCallCounts[b.id]||0;
        return leadsSortDir*(ca-cb);
      }
      let va=a[field], vb=b[field];
      if(field==='next_call'){
        va=va||''; vb=vb||'';
        if(!va && !vb) return 0;
        if(!va) return 1; if(!vb) return -1;
        return leadsSortDir*(va<vb?-1:va>vb?1:0);
      }
      va=(va||'').toString().toLowerCase(); vb=(vb||'').toString().toLowerCase();
      return leadsSortDir*va.localeCompare(vb,undefined,{numeric:true});
    });
    leadsFiltered=data;
  }

  // Column AutoFilter
  data = CF.applyLeads(data);
  leadsFiltered=data;

  const rows=data.slice((leadsPage-1)*PG_SIZE,leadsPage*PG_SIZE);
  const isAdmin = CU.role==='admin';
  const _lArrow=i=>leadsSortField===i?(leadsSortDir===1?' ▲':' ▼'):'';
  const _lSortTh=(i,label,cfCol)=>CF.th('leads',cfCol,`<span onclick="sortLeadsTable(${i})" style="cursor:pointer">${label}${_lArrow(i)}</span>`);
  const _lIsAdmin = (CU && CU.role==='admin');
  const _lCG = colGroup('leads', _lIsAdmin);
  let h=`<table style="width:${_lCG.total}px">${_lCG.cg}<thead><tr>${BULK.th('leads')}
    ${_lSortTh(0,'Name','name')}
    ${_lSortTh(1,'Mobile','mobile')}
    ${_lSortTh(2,'RM','rm')}
    ${CF.th('leads','source','Source')}
    <th onclick="sortLeadsTable(3)" style="cursor:pointer;white-space:nowrap">Calls${_lArrow(3)}</th>
    <th onclick="sortLeadsTable(4)" style="cursor:pointer;white-space:nowrap">Last Call${_lArrow(4)}</th>
    <th onclick="sortLeadsTable(5)" style="cursor:pointer;white-space:nowrap">Next Call${_lArrow(5)}</th>
    ${CF.th('leads','followup_status',`<span onclick="sortLeadsTable(6)" style="cursor:pointer">Follow-up${_lArrow(6)}</span>`)}
    <th onclick="sortLeadsTable(7)" style="cursor:pointer">Remarks${_lArrow(7)}</th>
    <th>Actions</th>
  </tr></thead><tbody>`;

  if(!rows.length) h+=`<tr><td colspan="11" style="text-align:center;padding:36px;color:#bbb">No leads. <a href="#" onclick="openAddLead()" style="color:var(--teal)">Add one?</a></td></tr>`;

  rows.forEach(c=>{
    const fuBadge=c.next_call?(c.next_call<today()?'b-pending':c.next_call===today()?'b-active':'b-na'):'b-na';
    h+=`<tr class="row-leads">${BULK.td('leads',c.id)}
      <td style="font-weight:600">${c.name}</td>
      <td><a href="tel:${c.mobile}" style="color:var(--navy);text-decoration:none">${c.mobile||'—'}</a></td>
      <td>${c.rm||'—'}</td>
      <td class="lead-source-cell" data-lid="${c.id}">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:6px">
          <span class="ls-view" onclick="editLeadSourceInline('${c.id}')" style="cursor:pointer;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${(c.source||'').replace(/"/g,'&quot;')}">${c.source||'—'}</span>
          <button class="btn-icon" style="padding:0 2px;font-size:.7rem;flex-shrink:0" title="Edit Source" onclick="editLeadSourceInline('${c.id}')">✏️</button>
        </div>
      </td>
      <td style="text-align:center">${(window._leadCallCounts[c.id]||0)>0
          ? `<span onclick="event.stopPropagation();viewLeadCalls('${c.id}')" title="View call history" style="cursor:pointer;background:var(--teal,#0d9488);color:#fff;border-radius:10px;padding:1px 8px;font-size:.72rem;font-weight:700">📞 ${window._leadCallCounts[c.id]}</span>`
          : '<span style="color:#bbb">—</span>'}</td>
      <td>${fmtDate(c.last_call)||'—'}</td>
      <td>${fmtDate(c.next_call)||'—'}</td>
      <td><span class="badge ${fuBadge}">${c.followup_status||'—'}</span></td>
      <td style="max-width:160px;overflow:hidden;text-overflow:ellipsis" title="${c.remarks||''}">${c.remarks||'—'}</td>
      <td>
        <button class="btn-icon" onclick="editLead('${c.id}')" title="Edit">✏️</button>
        ${c.mobile?`<a href="https://wa.me/91${c.mobile}" target="_blank" class="btn-icon" title="WhatsApp">💬</a>`:''}
        <button class="btn-icon" onclick="convertLead('${c.id}','equity')" title="Convert to Equity Client" style="color:var(--orange)">📈</button>
        <button class="btn-icon" onclick="convertLead('${c.id}','mf')" title="Convert to MF Investor" style="color:var(--teal)">🏦</button>
        ${(CU.role==='admin'||CU.role==='rm')?`<button class="btn-icon" onclick="confirmDeleteLead('${c.id}','${(c.name||'').replace(/'/g,"\\'")}')" title="Delete" style="color:var(--red)">🗑️</button>`:''}
      </td></tr>`;
  });
  h+='</tbody></table>';
  document.getElementById('leads-table').innerHTML=h;
  enableColResize('leads');
  BULK.afterRender('leads');
  renderPg('leads',data.length,leadsPage);
}

// Inline edit — Source cell in Leads table (click text or ✏️, Enter/blur saves, Esc cancels)
function editLeadSourceInline(id){
  const td=document.querySelector(`td.lead-source-cell[data-lid="${id}"]`);
  if(!td || td.querySelector('input')) return;
  const leads=DB.get('leads')||[];
  const lead=leads.find(x=>x.id===id);
  if(!lead) return;
  const cur=lead.source||'';
  td.innerHTML=`<input type="text" value="${cur.replace(/"/g,'&quot;')}" placeholder="e.g. DSP Seminar, Reference: Name" style="width:100%;min-width:110px;font-size:.8rem;padding:3px 6px;border:1px solid var(--teal);border-radius:5px;outline:none">`;
  const inp=td.querySelector('input');
  inp.focus(); inp.select();
  inp.addEventListener('keydown',e=>{
    if(e.key==='Enter'){ e.preventDefault(); inp.blur(); }
    else if(e.key==='Escape'){ e.preventDefault(); renderLeadsTable(); }
  });
  inp.addEventListener('blur',()=>saveLeadSourceInline(id, inp.value));
}

async function saveLeadSourceInline(id, val){
  const leads=DB.get('leads')||[];
  const lead=leads.find(x=>x.id===id);
  if(!lead) return;
  const newVal=(val||'').trim();
  if(newVal===(lead.source||'')){ renderLeadsTable(); return; }
  lead.source=newVal; lead.updated=today();
  await DB.setClient('leads', lead);
  toast('Source updated','success');
  renderLeadsTable();
}

function leadForm(c){
  return `
  <div class="form-section">Lead Information</div>
  <div class="form-row three">
    <div class="form-field"><label>Name *</label><input id="l_name" value="${c?.name||''}" placeholder="Full name"></div>
    <div class="form-field"><label>Mobile Number</label><input id="l_mobile" value="${c?.mobile||''}" placeholder="10 digit mobile"></div>
    ${rmFieldHtmlLead(c)}
  </div>
  <div class="form-section">Follow-up</div>
  <div class="form-row three">
    <div class="form-field"><label>Last Calling Date</label><input id="l_last_call" type="date" value="${c?.last_call||''}"${_lcAttr()}></div>
    <div class="form-field"><label>Next Calling Date</label><input id="l_next_call" type="date" value="${c?.next_call||''}"${_ncAttr()}></div>
    <div class="form-field"><label>Follow-up Status</label><select id="l_followup">
      <option value="">—</option>
      <option ${c?.followup_status==='Pending'?'selected':''}>Pending</option>
      <option ${c?.followup_status==='Done'?'selected':''}>Done</option>
      <option ${c?.followup_status==='Not Required'?'selected':''}>Not Required</option>
      <option ${c?.followup_status==='Not Interested'?'selected':''}>Not Interested</option>
      <option ${c?.followup_status==='Closed'?'selected':''}>Closed</option>
      <option ${c?.followup_status==='Transfer Request'?'selected':''}>Transfer Request</option>
      <option ${c?.followup_status==='Call Not Received'?'selected':''}>Call Not Received</option>
      <option ${c?.followup_status==='Call Not Connected'?'selected':''}>Call Not Connected</option>
      <option ${c?.followup_status==='Call After Some Time'?'selected':''}>Call After Some Time</option>
      <option ${c?.followup_status==='Interested In SIP'?'selected':''}>Interested In SIP</option>
      <option ${c?.followup_status==='Interested In Lumpsum'?'selected':''}>Interested In Lumpsum</option>
      <option ${c?.followup_status==='Interested In Other Product'?'selected':''}>Interested In Other Product</option>
      <option ${c?.followup_status==='Open Demat Account'?'selected':''}>Open Demat Account</option>
      <option ${c?.followup_status==='Come To Office'?'selected':''}>Come To Office</option>
    </select></div>
  </div>
  <div class="form-row single">
    <div class="form-field"><label>Remarks</label><input id="l_remarks" value="${c?.remarks||''}" placeholder="Any remarks"></div>
  </div>
  ${callMergeSection()}`;
}

function rmFieldHtmlLead(c){
  const rms=[...new Set([...getSegRMs('equity'),...getSegRMs('mf')])];
  if(CU.role!=='admin'){
    const myName=CU.name;
    return `<div class="form-field"><label>RM</label>
      <input type="text" value="${myName}" disabled style="background:var(--bg);color:var(--gray)">
      <input type="hidden" id="l_rm" value="${myName}"></div>`;
  }
  const opts=rms.map(r=>`<option ${c&&c.rm===r?'selected':''}>${r}</option>`).join('');
  return `<div class="form-field"><label>RM</label><select id="l_rm"><option value="">Select RM</option>${opts}</select></div>`;
}

function openAddLead(){
  currentEditLeadId=null;
  document.getElementById('leadModalTitle').textContent='Add Lead';
  document.getElementById('leadSaveBtn').textContent='Save Lead';
  document.getElementById('leadModalBody').innerHTML=leadForm(null);
  document.getElementById('leadModal').classList.add('open');
}

function editLead(id){
  currentEditLeadId=id;
  const leads=DB.get('leads')||[];
  const c=leads.find(x=>x.id===id);
  if(!c) return;
  document.getElementById('leadModalTitle').textContent='Edit Lead';
  document.getElementById('leadSaveBtn').textContent='Update';
  document.getElementById('leadModalBody').innerHTML=leadForm(c);
  document.getElementById('leadModal').classList.add('open');
}

async function saveLead(){
  const name=(document.getElementById('l_name')||{value:''}).value.trim();
  const rm=normRm((document.getElementById('l_rm')||{value:''}).value.trim());
  if(!name){ toast('Lead name is required','error'); return; }

  // RM call-date lock (Admin-configured). Admin is unrestricted.
  if(CU && CU.role!=='admin'){
    const _cl=effectiveCallLimits();
    const _lc=(document.getElementById('l_last_call')||{value:''}).value;
    const _nc=(document.getElementById('l_next_call')||{value:''}).value;
    const _fd=s=>{ const p=String(s).split('-'); return p.length===3? p[2]+'-'+p[1]+'-'+p[0] : s; };
    if(_lc){ if(_cl.lcMin && _lc<_cl.lcMin){ toast('Last Call date cannot be before '+_fd(_cl.lcMin),'error'); return; }
             if(_cl.lcMax && _lc>_cl.lcMax){ toast('Last Call date cannot be after '+_fd(_cl.lcMax),'error'); return; } }
    if(_nc){ if(_cl.ncMin && _nc<_cl.ncMin){ toast('Next Call date cannot be before '+_fd(_cl.ncMin),'error'); return; }
             if(_cl.ncMax && _nc>_cl.ncMax){ toast('Next Call date cannot be after '+_fd(_cl.ncMax),'error'); return; } }
  }

  const gv2=id=>{ const el=document.getElementById(id); return el?el.value.trim():''; };
  const leads=DB.get('leads')||[];

  // Mobile (digits only, last 10) for duplicate checks
  const cleanMob=v=>String(v||'').replace(/\D/g,'').slice(-10);
  const mobileRaw=gv2('l_mobile');
  const mob=cleanMob(mobileRaw);

  // Duplicate checks only when adding a NEW lead (skip while editing existing)
  if(!currentEditLeadId && mob){
    // 1) Same mobile already present in Leads
    const dupLead=leads.find(l=>cleanMob(l.mobile)===mob);
    if(dupLead){ toast(`⚠️ This mobile already exists in Leads: "${dupLead.name}"`,'error'); return; }
    // 2) Already an Equity client with a Client Code
    const eqDup=(DB.get('eq_clients')||[]).find(c=>c.code && String(c.code).trim() && cleanMob(c.mobile)===mob);
    if(eqDup){ toast(`⚠️ Already an Equity Client (Code: ${eqDup.code}): "${eqDup.name}"`,'error'); return; }
    // 3) Already an MF investor with a PAN
    const mfDup=(DB.get('mf_clients')||[]).find(c=>c.pan && String(c.pan).trim() && cleanMob(c.mobile)===mob);
    if(mfDup){ toast(`⚠️ Already an MF Investor (PAN: ${mfDup.pan}): "${mfDup.name}"`,'error'); return; }
  }

  let newId=currentEditLeadId;
  if(!newId){
    newId=uid();
    while(leads.some(x=>x.id===newId)) newId=uid();
  }
  const rec={
    id:newId, name, mobile:mobileRaw, rm,
    source:(currentEditLeadId?leads.find(x=>x.id===currentEditLeadId)?.source:'')||'',
    last_call:gv2('l_last_call'), next_call:gv2('l_next_call'), followup_status:gv2('l_followup'), remarks:gv2('l_remarks'),
    created:currentEditLeadId?undefined:today(), updated:today()
  };
  // Closed → RM list se hata do
  if(rec.followup_status==='Closed') rec.rm='';
  if(currentEditLeadId){
    const idx=leads.findIndex(x=>x.id===currentEditLeadId);
    if(idx>=0){ const old=leads[idx]; rec.created=old.created||today(); }
  } else {
    rec.created=today();
  }
  const _saveRes = await DB.setClient('leads',rec);
  if(_saveRes && _saveRes.ok===false){
    toast('⚠️ The lead was NOT saved to the server ('+(_saveRes.error||'connection issue')+'). Check your internet and press "Save" again — the form has been kept open.','error');
    return;   // modal stays open, no data is lost
  }
  maybeLogMergedCall('lead', newId, name, rm);
  closeModal('leadModal');
  toast(currentEditLeadId?'Lead updated!':'Lead added!','success');
  renderLeadsTable();
  updateBadges();
}

async function confirmDeleteLead(id,name){
  const leads = DB.get('leads')||[];
  const lead = leads.find(x=>x.id===id);
  // Admin can delete any lead. RM can delete ONLY their own (lead.rm === apna naam).
  const isOwnLead = lead && CU.role==='rm' && lead.rm===CU.name;
  if(CU.role!=='admin' && !isOwnLead){ toast('You can only delete your own leads','error'); return; }
  if(!confirm(`Delete lead "${name}"? This cannot be undone.`)) return;
  await DB.deleteClient('leads',id);
  toast('Lead deleted','success');
  renderLeadsTable();
  updateBadges();
}

// Adds an existing Equity client into Mutual Fund as an MF Investor.
// The Equity record stays untouched — the client will exist in BOTH segments.
let _atmEqId=null;
function addEquityToMf(eqId){
  const eqClients=DB.get('eq_clients')||[];
  const eq=eqClients.find(x=>x.id===eqId);
  if(!eq){ toast('Equity client not found','error'); return; }

  _atmEqId=eqId;
  document.getElementById('atm-client-name').textContent=eq.name;
  document.getElementById('atm-pan').value=(eq.pan||'').toUpperCase();
  const _atmMinor=document.getElementById('atm-minor'); if(_atmMinor){ _atmMinor.checked=false; }
  const _atmReq=document.getElementById('atm-pan-req'); if(_atmReq){ _atmReq.style.display=''; }

  // RM dropdown: MF RM list, defaulting to the equity RM if they also do MF
  // Only Admin can change the MF RM — RMs can only add to their own name.
  const mfRMs=getSegRMs('mf');
  const rmSel=document.getElementById('atm-rm');
  if(CU.role==='admin'){
    rmSel.innerHTML=mfRMs.map(r=>`<option ${r===eq.rm?'selected':''}>${escapeHtml(r)}</option>`).join('');
    if(!mfRMs.includes(eq.rm) && mfRMs.length){ rmSel.value=mfRMs[0]; }
    rmSel.disabled=false;
  } else {
    rmSel.innerHTML=`<option selected>${escapeHtml(CU.name)}</option>`;
    rmSel.disabled=true;
  }

  document.getElementById('addToMfModal').classList.add('open');
  setTimeout(()=>document.getElementById('atm-pan').focus(),100);
}

async function confirmAddEquityToMf(){
  const eqId=_atmEqId;
  const eqClients=DB.get('eq_clients')||[];
  const eq=eqClients.find(x=>x.id===eqId);
  if(!eq){ toast('Equity client not found','error'); closeModal('addToMfModal'); return; }

  const isMinor = document.getElementById('atm-minor')?.checked || false;
  let pan=(document.getElementById('atm-pan').value||'').trim().toUpperCase();
  if(!pan && !isMinor){ toast('PAN is required to add as MF Investor','error'); return; }
  if(pan && !/^[A-Z]{5}[0-9]{4}[A-Z]$/.test(pan)){ toast('Invalid PAN format (e.g. ABCDE1234F)','error'); return; }

  const mfRm=document.getElementById('atm-rm').value;
  if(!mfRm){ toast('Please select an MF RM','error'); return; }

  const mfClients=DB.get('mf_clients')||[];
  const cleanMob=v=>String(v||'').replace(/\D/g,'').slice(-10);
  // Duplicate guards: same PAN, or same mobile/name already in MF.
  // For a minor the PAN may be the guardian's (already used) — skip the PAN block.
  if(pan && !isMinor){
    const dupPan=mfClients.find(c=>c.pan && c.pan.trim().toUpperCase()===pan);
    if(dupPan){ toast(`⚠️ Already an MF Investor: "${dupPan.name}" (PAN: ${dupPan.pan})`,'error'); return; }
  }
  const mob=cleanMob(eq.mobile);
  const dupOther=mfClients.find(c=>(mob && cleanMob(c.mobile)===mob) || (c.name||'').trim().toUpperCase()===(eq.name||'').trim().toUpperCase());
  if(dupOther){ toast(`⚠️ "${dupOther.name}" already exists in Mutual Fund`,'error'); return; }

  let newId=uid();
  while(mfClients.some(x=>x.id===newId)) newId=uid();
  const rec={
    id:newId, name:eq.name, mobile:eq.mobile||'', pan, email:eq.email||'',
    rm:mfRm, status:'Prospect', is_minor:isMinor,
    aum:null, sip_amount:null, sip_count:null,
    last_invest_date:'', last_call_date:eq.last_call_date||'',
    next_call:'', followup_status:'', remarks:`Added from Equity${eq.code?' (Code: '+eq.code+')':''}`,
    created:today(), updated:today()
  };
  await DB.setClient('mf_clients', rec);
  DB.addActivityLog({
    id:uid(), type:'add', seg:'mf', client_id:newId,
    client_name:eq.name, rm:mfRm, by:CU.name,
    date:new Date().toISOString(), changes:[]
  });
  closeModal('addToMfModal');
  toast(`✅ "${eq.name}" added to Mutual Fund (RM: ${mfRm})! Equity record kept.`,'success');
  renderMfTable(); refreshDash(); updateBadges();
}

// Lead → MF conversion via modal (PAN + Minor checkbox, like Add-to-MF).
let _cmfLeadId=null;
function openConvertMf(id){
  const leads=DB.get('leads')||[];
  const lead=leads.find(x=>x.id===id);
  if(!lead) return;
  _cmfLeadId=id;
  document.getElementById('cmf-name').textContent=lead.name;
  document.getElementById('cmf-pan').value='';
  const mn=document.getElementById('cmf-minor'); if(mn) mn.checked=false;
  const rq=document.getElementById('cmf-pan-req'); if(rq) rq.style.display='';
  document.getElementById('convertMfModal').classList.add('open');
  setTimeout(()=>document.getElementById('cmf-pan').focus(),100);
}

async function confirmConvertLeadMf(){
  const id=_cmfLeadId; if(!id) return;
  const leads=DB.get('leads')||[];
  const lead=leads.find(x=>x.id===id);
  if(!lead){ closeModal('convertMfModal'); return; }

  const isMinor0 = document.getElementById('cmf-minor')?.checked || false;
  let pan=(document.getElementById('cmf-pan').value||'').trim().toUpperCase();
  if(!pan && !isMinor0){ toast('PAN is required — or tick 👶 Minor','error'); return; }
  if(pan && !/^[A-Z]{5}[0-9]{4}[A-Z]$/.test(pan)){ toast('Invalid PAN format (e.g. ABCDE1234F)','error'); return; }

  let isMinor=isMinor0;
  const mf=DB.get('mf_clients')||[];
  // Guardian's PAN may already exist for a minor — allow after confirm.
  if(pan && !isMinor){
    const dupPan=mf.find(c=>c.pan && c.pan.trim().toUpperCase()===pan);
    if(dupPan){
      const asMinor = confirm(`⚠️ This PAN already belongs to "${dupPan.name}".\n\nIf "${lead.name}" is a MINOR investing under this person (guardian), you can still add them.\n\nOK = add as Minor (guardian's PAN)\nCancel = stop`);
      if(!asMinor) return;
      isMinor=true;
    }
  }

  let newId=uid();
  while(mf.some(x=>x.id===newId)) newId=uid();
  const rec={
    id:newId, name:lead.name, mobile:lead.mobile, pan, email:'',
    rm:lead.rm, status:'Prospect', is_minor:isMinor,
    aum:null, sip_amount:null, sip_count:null,
    last_invest_date:'', last_call_date:'',
    next_call:lead.next_call, followup_status:lead.followup_status, remarks:lead.remarks,
    created:today(), updated:today()
  };
  await DB.setClient('mf_clients',rec);
  await DB.deleteClient('leads',id);
  closeModal('convertMfModal');
  toast(`✅ "${lead.name}" converted to MF Investor${isMinor?' (Minor)':''}!`,'success');
  renderLeadsTable(); renderMfTable(); refreshDash(); updateBadges();
}

async function convertLead(id,seg){
  if(seg==='mf'){ openConvertMf(id); return; }
  if(seg==='equity'){ openConvertEq(id); return; }
}

// Lead → Equity conversion via colorful modal (Client Code / UCC, numbers only, RM shown clearly)
let _ceqLeadId=null;
function openConvertEq(id){
  const leads=DB.get('leads')||[];
  const lead=leads.find(x=>x.id===id);
  if(!lead) return;
  _ceqLeadId=id;
  document.getElementById('ceq-name').textContent=lead.name;
  document.getElementById('ceq-rm').textContent=lead.rm||'— (no RM assigned)';
  const inp=document.getElementById('ceq-code');
  inp.value='';
  inp.style.borderColor='';
  const warn=document.getElementById('ceq-code-warn'); if(warn) warn.style.display='none';
  document.getElementById('convertEqModal').classList.add('open');
  setTimeout(()=>inp.focus(),100);
}

function ceqCodeInput(el){
  const cleaned=el.value.replace(/[^0-9]/g,'');
  const warn=document.getElementById('ceq-code-warn');
  if(el.value!==cleaned && warn){ warn.style.display=''; } else if(warn){ warn.style.display='none'; }
  el.value=cleaned;
  el.style.borderColor = cleaned ? 'var(--teal)' : '';
}

async function confirmConvertLeadEq(){
  const id=_ceqLeadId; if(!id) return;
  const leads=DB.get('leads')||[];
  const lead=leads.find(x=>x.id===id);
  if(!lead){ closeModal('convertEqModal'); return; }

  const inp=document.getElementById('ceq-code');
  let clientCode=(inp.value||'').trim();
  if(!clientCode){ inp.style.borderColor='var(--red)'; toast('Client Code is required to convert to Equity Client','error'); return; }
  if(!/^[0-9]+$/.test(clientCode)){ inp.style.borderColor='var(--red)'; toast('Client Code must be numbers only (no letters)','error'); return; }

  const clients=DB.get('eq_clients')||[];
  const dupCode=clients.find(c=>c.code && c.code.trim()===clientCode);
  if(dupCode){ toast(`⚠️ Already exists as Equity Client: "${dupCode.name}" (Code: ${dupCode.code})`,'error'); return; }

  let newId=uid();
  while(clients.some(x=>x.id===newId)) newId=uid();
  const rec={
    id:newId, code:clientCode, name:lead.name, mobile:lead.mobile, email:'',
    rm:lead.rm, status:'Active',
    asset_value:null, revenue:null,
    last_trade_date:'', last_trade_month:'',
    last_call_date:'', next_call:lead.next_call,
    followup_status:lead.followup_status, remarks:lead.remarks,
    created:today(), updated:today()
  };
  await DB.setClient('eq_clients',rec);

  // Equity: Add "Open Demat Account" entry in New Business
  try{
    const newEqEntry = {
      id: uid(),
      client_id: newId,
      client_name: lead.name,
      client_code: clientCode,
      rm: lead.rm,
      type: 'Open Demat Account',
      date: today(),
      mobile: lead.mobile||'',
      remarks: 'Converted from Lead',
      created_by: CU.name,
      created: today(),
      status: CU.role==='admin' ? 'Approved' : 'Pending',
      decline_reason: ''
    };
    await DB.appendMfBizEntry('eq_entries', newEqEntry);
  }catch(e){ console.error('eq new business entry error',e); }

  await DB.deleteClient('leads',id);
  closeModal('convertEqModal');
  toast(`✅ "${lead.name}" converted to Equity Client — Demat entry added in New Business!`,'success');
  renderLeadsTable();
  renderEqTable();
  refreshDash();
  updateBadges();
}

// ══════════════════════════════════════════
// LEADS BULK IMPORT
// ══════════════════════════════════════════
let leadsImportData = null;

function openLeadsImportModal(){
  leadsImportData = null;
  document.getElementById('leads-import-preview').innerHTML='';
  document.getElementById('leads-import-file').value='';
  document.getElementById('leadsImportBtn2').disabled=true;
  document.getElementById('leadsImportModal').classList.add('open');
}

function parseLeadsExcel(rows){
  let hdrIdx=-1, colMap={};
  const wanted = {
    name: ['name','leadname','clientname','fullname'],
    mobile: ['mobile','mobileno','phone','contactno','mobilenumber','phonenumber'],
    rm: ['rm','assignedto','dealer','dealername','employeename','relationshipmanager'],
    next_call: ['nextcall','nextcalldate','nextcallingdate','followupdate'],
    followup_status: ['followupstatus','followup','status'],
    remarks: ['remarks','remark','notes','comment','comments']
  };
  for(let i=0;i<Math.min(rows.length,10);i++){
    const row=rows[i];
    const map={};
    row.forEach((cell,ci)=>{
      const h=normHdr(cell);
      for(const [field,variants] of Object.entries(wanted)){
        if(variants.includes(h) && map[field]===undefined) map[field]=ci;
      }
    });
    if(map.name!==undefined){ hdrIdx=i; colMap=map; break; }
  }
  if(hdrIdx===-1) return null;
  return rows.slice(hdrIdx+1)
    .filter(r=>r.some(c=>c!==''))
    .map(r=>({
      name: colMap.name!==undefined ? String(r[colMap.name]||'').trim() : '',
      mobile: colMap.mobile!==undefined ? String(r[colMap.mobile]||'').replace(/\D/g,'').slice(-10) : '',
      rm: colMap.rm!==undefined ? String(r[colMap.rm]||'').trim() : '',
      next_call: colMap.next_call!==undefined ? parseExcelDate(r[colMap.next_call]) : '',
      followup_status: colMap.followup_status!==undefined ? String(r[colMap.followup_status]||'').trim() : '',
      remarks: colMap.remarks!==undefined ? String(r[colMap.remarks]||'').trim() : ''
    }))
    .filter(r=>r.name);
}

function handleLeadsImportFile(input){
  const file=input.files[0];
  if(!file) return;
  readExcel(file, function(err, rows){
    if(err){ toast('File read error: '+err.message,'error'); return; }
    const data=parseLeadsExcel(rows);
    if(!data || !data.length){
      toast('Header row not found. Excel must have at least a "Name" column.','error');
      return;
    }
    leadsImportData=data;
    document.getElementById('leads-import-preview').innerHTML=
      `<div style="background:var(--green2);color:var(--green);padding:10px;border-radius:8px;font-size:.85rem;font-weight:600">
       ✅ ${data.length} leads found</div>`;
    document.getElementById('leadsImportBtn2').disabled=false;
  });
}

async function doLeadsImport(){
  if(!leadsImportData || !leadsImportData.length) return;
  const existing=DB.get('leads')||[];
  const existingMobiles=new Set(existing.filter(c=>c.mobile).map(c=>c.mobile.trim()));

  let added=0, skipped=0;
  const newRecords=[];
  leadsImportData.forEach(row=>{
    if(row.mobile && existingMobiles.has(row.mobile)){ skipped++; return; }
    let newId=uid();
    while(existing.some(x=>x.id===newId) || newRecords.some(x=>x.id===newId)) newId=uid();
    newRecords.push({
      id:newId, name:row.name, mobile:row.mobile, rm:row.rm||(CU.role!=='admin'?CU.name:''),
      next_call:row.next_call||'', followup_status:row.followup_status||'', remarks:row.remarks||'',
      created:today(), updated:today()
    });
    if(row.mobile) existingMobiles.add(row.mobile);
    added++;
  });

  if(newRecords.length) await DB.setClientsBulk('leads', newRecords);
  closeModal('leadsImportModal');
  toast(`${added} leads imported${skipped?`, ${skipped} skipped (duplicate mobile)`:''}`,'success');
  renderLeadsTable();
  updateBadges();
}

// ══════════════════════════════════════════
// SEMINARS
// ══════════════════════════════════════════
let currentEditSeminarId = null;
let currentSeminarId = null;

function getMySeminars(){
  // All users (Admin & RMs) see all seminars - seminars are company-wide events
  return DB.get('seminars')||[];
}

function renderSeminarsTable(){
  try{
  let seminars = getMySeminars().slice().map(s=>{
    const attendees = s.attendees||[];
    return {
      ...s,
      _total: attendees.length,
      _eq: attendees.filter(a=>a.type==='equity').length,
      _mf: attendees.filter(a=>a.type==='mf').length,
      _guest: attendees.filter(a=>a.type==='guest').length,
      _attended: attendees.filter(a=>a.status==='Attended').length,
      _willcome: attendees.filter(a=>a.rsvp==='Yes').length,
      _wait: attendees.filter(a=>a.rsvp==='Wait').length,
      _expected: attendees.reduce((sum,a)=>sum+(a.expected_count||0),0),
    };
  });
  if(!_sortState.seminars || !_sortState.seminars.field){
    seminars.sort((a,b)=>(b.date||'').localeCompare(a.date||''));
  } else {
    seminars = applySort(seminars, 'seminars', {
      date:{get:s=>s.date, type:'date'},
      name:{get:s=>s.name, type:'str'},
      notes:{get:s=>s.notes, type:'str'},
      total:{get:s=>s._total, type:'num'},
      eq:{get:s=>s._eq, type:'num'},
      mf:{get:s=>s._mf, type:'num'},
      guest:{get:s=>s._guest, type:'num'},
      willcome:{get:s=>s._willcome, type:'num'},
      wait:{get:s=>s._wait, type:'num'},
      expected:{get:s=>s._expected, type:'num'},
      attended:{get:s=>s._attended, type:'num'},
    });
  }
  document.getElementById('nb-seminars').textContent = seminars.length;

  const cont = document.getElementById('seminars-table');
  if(!seminars.length){
    cont.innerHTML = `<div style="text-align:center;padding:48px;color:#bbb">No seminars yet. <a href="#" onclick="openAddSeminar()" style="color:var(--teal)">Add one?</a></div>`;
    return;
  }
  let h=`<table><thead><tr>
    ${sortTh('Date','seminars','date','date','renderSeminarsTable')}
    ${sortTh('Seminar Name','seminars','name','str','renderSeminarsTable')}
    ${sortTh('Venue / Notes','seminars','notes','str','renderSeminarsTable')}
    ${sortTh('Total','seminars','total','num','renderSeminarsTable')}
    ${sortTh('Equity','seminars','eq','num','renderSeminarsTable')}
    ${sortTh('MF','seminars','mf','num','renderSeminarsTable')}
    ${sortTh('Guests','seminars','guest','num','renderSeminarsTable')}
    ${sortTh('Will Come','seminars','willcome','num','renderSeminarsTable')}
    ${sortTh('Wait','seminars','wait','num','renderSeminarsTable')}
    ${sortTh('Expected People','seminars','expected','num','renderSeminarsTable')}
    ${sortTh('Attended','seminars','attended','num','renderSeminarsTable')}
    <th>Actions</th></tr></thead><tbody>`;
  seminars.forEach(s=>{
    const eqCount=s._eq, mfCount=s._mf, guestCount=s._guest, attendedCount=s._attended, willComeCount=s._willcome, waitCount=s._wait, expectedTotal=s._expected;
    const attendees = s.attendees||[];
    h+=`<tr>
      <td>${fmtDate(s.date)||'—'}</td>
      <td style="font-weight:600;cursor:pointer;color:var(--navy)" onclick="openSeminarDetail('${s.id}')">${s.name}</td>
      <td>${s.notes||'—'}</td>
      <td>${attendees.length}</td>
      <td>${eqCount}</td>
      <td>${mfCount}</td>
      <td>${guestCount}</td>
      <td>${willComeCount}</td>
      <td>${waitCount}</td>
      <td>${expectedTotal||'—'}</td>
      <td>${attendedCount}</td>
      <td>
        <button class="btn-icon" onclick="openSeminarDetail('${s.id}')" title="View / Manage Attendees">👁️</button>
        ${(CU.role==='admin'||s.created_by===CU.name)?`<button class="btn-icon" onclick="editSeminar('${s.id}')" title="Edit">✏️</button>`:''}
        ${CU.role==='admin'?`<button class="btn-icon" onclick="confirmDeleteSeminar('${s.id}','${(s.name||'').replace(/'/g,"\\'")}')" title="Delete" style="color:var(--red)">🗑️</button>`:''}
      </td></tr>`;
  });
  h+='</tbody></table>';
  cont.innerHTML=h;
  }catch(e){
    console.error('renderSeminarsTable error:', e);
    const _errCont = document.getElementById('eqnc-content')||document.getElementById('mfnc-content')||document.getElementById('nt-content')||document.getElementById('sip-table')||document.getElementById('seminars-table')||document.getElementById((arguments[0]==='eqf'?'eqf':'mff')+'-content');
    if(_errCont) _errCont.innerHTML = '<div style="text-align:center;padding:36px;color:var(--red)">⚠️ Error loading data: '+(e.message||e)+'</div>';
  }
}

function openAddSeminar(){
  currentEditSeminarId=null;
  document.getElementById('seminarModalTitle').textContent='Add Seminar';
  document.getElementById('seminarSaveBtn').textContent='Save Seminar';
  document.getElementById('sem_name').value='';
  document.getElementById('sem_date').value=today();
  document.getElementById('sem_notes').value='';
  document.getElementById('seminarModal').classList.add('open');
}

function editSeminar(id){
  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===id);
  if(!s) return;
  currentEditSeminarId=id;
  document.getElementById('seminarModalTitle').textContent='Edit Seminar';
  document.getElementById('seminarSaveBtn').textContent='Update';
  document.getElementById('sem_name').value=s.name||'';
  document.getElementById('sem_date').value=s.date||'';
  document.getElementById('sem_notes').value=s.notes||'';
  document.getElementById('seminarModal').classList.add('open');
}

async function saveSeminar(){
  const name=document.getElementById('sem_name').value.trim();
  const date=document.getElementById('sem_date').value;
  const notes=document.getElementById('sem_notes').value.trim();
  if(!name||!date){ toast('Seminar name and date are required','error'); return; }

  if(currentEditSeminarId){
    // Editing name/date/notes of an existing seminar — go through mutateSeminar
    // (not a blind setClient) so this can never race with someone concurrently
    // adding/editing an attendee on the same seminar and wipe their change.
    const r = await DB.mutateSeminar(currentEditSeminarId, sem=>{
      sem.name=name; sem.date=date; sem.notes=notes; sem.updated=today();
    });
    if(!r.ok || r.aborted) return;
  } else {
    // Brand-new seminar (fresh id) — no concurrent-edit risk, plain setClient is fine.
    const seminars=DB.get('seminars')||[];
    let newId=uid();
    while(seminars.some(x=>x.id===newId)) newId=uid();
    const rec={id:newId, name, date, notes, attendees:[], created_by:CU.name, created:today(), updated:today()};
    await DB.setClient('seminars',rec);
  }
  closeModal('seminarModal');
  toast(currentEditSeminarId?'Seminar updated!':'Seminar added!','success');
  renderSeminarsTable();
}

async function confirmDeleteSeminar(id,name){
  if(CU.role!=='admin') return;
  if(!confirm(`Delete seminar "${name}"? This cannot be undone.`)) return;
  await DB.deleteClient('seminars',id);
  toast('Seminar deleted','success');
  renderSeminarsTable();
}

let semAttFilter = {q:'', rm:'', type:'', rsvp:'', status:''};
function onSemFilterChange(){
  semAttFilter.q      = (document.getElementById('semF-q').value||'').trim().toLowerCase();
  semAttFilter.rm     = document.getElementById('semF-rm').value;
  semAttFilter.type   = document.getElementById('semF-type').value;
  semAttFilter.rsvp   = document.getElementById('semF-rsvp').value;
  semAttFilter.status = document.getElementById('semF-status').value;
  renderSeminarAttendees();
}
function clearSemFilters(){
  semAttFilter = {q:'', rm:'', type:'', rsvp:'', status:''};
  ['semF-q','semF-rm','semF-type','semF-rsvp','semF-status'].forEach(id=>{ const el=document.getElementById(id); if(el) el.value=''; });
  renderSeminarAttendees();
}
function rebuildSemRmOptions(attendees){
  const sel=document.getElementById('semF-rm');
  if(!sel) return;
  const cur=sel.value;
  const rms=[...new Set(attendees.map(a=>(a.rm||'').trim()).filter(Boolean))].sort((a,b)=>a.localeCompare(b));
  sel.innerHTML='<option value="">All RMs</option>'+rms.map(r=>`<option value="${r.replace(/"/g,'&quot;')}">${r}</option>`).join('');
  if(rms.some(r=>r===cur)) sel.value=cur;
}
function openSeminarDetail(id){
  currentSeminarId=id;
  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===id);
  if(!s) return;
  document.getElementById('seminarDetailTitle').textContent=s.name;
  document.getElementById('seminarDetailSub').textContent=`Date: ${fmtDate(s.date)}${s.notes?' · '+s.notes:''}`;
  const _lqBtn=document.getElementById('seminarLiveQuizBtn');
  if(_lqBtn) _lqBtn.style.display = (typeof CU!=='undefined' && CU && CU.role==='admin') ? '' : 'none';
  clearSemFilters();
  document.getElementById('seminarDetailModal').classList.add('open');
}

let seminarAttSort = {col:'name', dir:1};

function sortSeminarAttendees(col){
  if(seminarAttSort.col===col) seminarAttSort.dir = -seminarAttSort.dir;
  else { seminarAttSort.col=col; seminarAttSort.dir=1; }
  renderSeminarAttendees();
}

function renderSeminarAttendees(){
  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===currentSeminarId);
  const cont=document.getElementById('seminar-attendees-table');
  if(!s){ cont.innerHTML=''; return; }
  const {col,dir}=seminarAttSort;
  const allAtt=(s.attendees||[]);
  rebuildSemRmOptions(allAtt);
  const f=semAttFilter;
  const attendees=allAtt.filter(a=>{
    if(f.q && !((a.name||'').toLowerCase().includes(f.q) || (a.mobile||'').includes(f.q))) return false;
    if(f.rm && (a.rm||'').trim()!==f.rm) return false;
    if(f.type && (a.type||'guest')!==f.type) return false;
    if(f.rsvp){ if(f.rsvp==='__none'){ if(a.rsvp) return false; } else if(a.rsvp!==f.rsvp) return false; }
    if(f.status && (a.status||'Pending')!==f.status) return false;
    return true;
  }).sort((a,b)=>{
    let av,bv;
    if(col==='rm'){ av=(a.rm||'').toLowerCase(); bv=(b.rm||'').toLowerCase(); }
    else { av=(a.name||'').toLowerCase(); bv=(b.name||'').toLowerCase(); }
    return av.localeCompare(bv)*dir;
  });
  const cntEl=document.getElementById('semF-count');
  if(cntEl) cntEl.textContent = attendees.length===allAtt.length ? allAtt.length+' attendees' : attendees.length+' of '+allAtt.length+' shown';
  if(!allAtt.length){
    cont.innerHTML=`<div style="text-align:center;padding:36px;color:#bbb">No attendees added yet. Click "+ Add Attendee" to add from Equity Clients / MF Investors.</div>`;
    return;
  }
  if(!attendees.length){
    cont.innerHTML=`<div style="text-align:center;padding:36px;color:#bbb">No attendees match the filters — try clearing the filters.</div>`;
    return;
  }
  const arrow=(c)=> col===c ? (dir===1?' ▲':' ▼') : '';
  // hide/show "Add Selected to Leads" button
  const semAddBtn = document.getElementById('semAddLeadBtn');
  if(semAddBtn) semAddBtn.style.display='none';
  let h=`<table><thead><tr>
    <th style="width:32px"><input type="checkbox" id="semChkAll" title="Select All" onchange="toggleAllSeminarChk(this)"></th>
    <th onclick="sortSeminarAttendees('name')" style="cursor:pointer" title="Click to sort">Name${arrow('name')}</th>
    <th>Mobile</th>
    <th onclick="sortSeminarAttendees('rm')" style="cursor:pointer" title="Click to sort">RM${arrow('rm')}</th>
    <th>Type</th><th>Response</th><th>Headcount</th><th>Status</th><th>Remarks</th><th>Actions</th></tr></thead><tbody>`;
  const _allRMs=[...new Set([...getSegRMs('equity'),...getSegRMs('mf')])].filter(Boolean).sort((x,y)=>x.localeCompare(y));
  attendees.forEach(a=>{
    const isMyAttendee = CU.role==='admin' || (a.rm||'').trim().toLowerCase()===(CU.name||'').trim().toLowerCase();
    const dis = isMyAttendee ? '' : 'disabled style="opacity:0.45;pointer-events:none"';
    h+=`<tr data-id="${a.id}">
      <td><input type="checkbox" class="sem-chk" data-id="${a.id}" onchange="onSemChkChange()"></td>
      <td style="font-weight:600">${a.name}</td>
      <td>${a.mobile||'—'}</td>
      <td>${CU.role==='admin'
        ? `<select class="att-rm" data-id="${a.id}" data-prev="${(a.rm||'').replace(/"/g,'&quot;')}" onchange="onAttRmChange(this)" style="padding:4px 6px;font-size:.72rem;border-radius:6px;border:1px solid var(--border);max-width:110px">
          <option value="" ${!a.rm?'selected':''}>— None —</option>
          ${_allRMs.map(r=>`<option ${r.trim().toLowerCase()===(a.rm||'').trim().toLowerCase()?'selected':''}>${r}</option>`).join('')}
        </select>`
        : (a.rm||'—')}</td>
      <td><select class="att-type" data-id="${a.id}" data-prev="${a.type||'guest'}" ${dis} onchange="onAttTypeChange(this)" style="padding:4px 6px;font-size:.72rem;border-radius:6px;border:1px solid var(--border)">
        <option value="equity" ${a.type==='equity'?'selected':''}>Equity</option>
        <option value="mf" ${a.type==='mf'?'selected':''}>MF</option>
        <option value="guest" ${(!a.type||a.type==='guest')?'selected':''}>Guest</option>
      </select></td>
      <td><select class="att-rsvp" ${dis} style="padding:4px 6px;font-size:.72rem;border-radius:6px;border:1px solid var(--border)">
        <option value="" ${!a.rsvp?'selected':''}>—</option>
        <option value="Yes" ${a.rsvp==='Yes'?'selected':''}>Yes</option>
        <option value="No" ${a.rsvp==='No'?'selected':''}>No</option>
        <option value="Wait" ${a.rsvp==='Wait'?'selected':''}>Wait</option>
      </select></td>
      <td><input class="att-count" type="number" min="0" value="${a.expected_count!=null?a.expected_count:''}" placeholder="—" ${dis} style="width:60px;padding:4px 6px;font-size:.72rem;border-radius:6px;border:1px solid var(--border)"></td>
      <td><select class="att-status" ${dis} style="padding:4px 6px;font-size:.72rem;border-radius:6px;border:1px solid var(--border)">
        <option value="Pending" ${a.status==='Pending'||!a.status?'selected':''}>Pending</option>
        <option value="Attended" ${a.status==='Attended'?'selected':''}>Attended</option>
        <option value="Not Attended" ${a.status==='Not Attended'?'selected':''}>Not Attended</option>
      </select></td>
      <td><input class="att-remarks" type="text" value="${(a.remarks||'').replace(/"/g,'&quot;')}" placeholder="Remarks" ${dis} style="width:170px;padding:4px 6px;font-size:.72rem;border-radius:6px;border:1px solid var(--border)"></td>
      <td style="white-space:nowrap">${a.mobile?`<button class="btn-icon" onclick="sendFeedbackWA('${a.id}')" title="Send feedback link on WhatsApp" style="color:#25D366">💬</button>`:''}${isMyAttendee?`<button class="btn-icon" onclick="openEditAttendee('${a.id}')" title="Edit name / mobile / RM">✏️</button><button class="btn-icon" onclick="removeAttendee('${a.id}')" title="Remove" style="color:var(--red)">🗑️</button>`:'<span style="color:#ccc;font-size:.75rem">—</span>'}</td>
      </tr>`;
  });
  h+='</tbody></table>';
  cont.innerHTML=h;
}

// Attendee "Type" dropdown — Equity/MF selection only allowed if a client
// record exists in that segment with the SAME mobile AND RM as this
// attendee. Otherwise revert and tell the user why (avoids mis-tagging a
// guest as a real client just by picking from the dropdown).
function onAttTypeChange(sel){
  const newType=sel.value;
  const prev=sel.dataset.prev||'guest';
  if(newType==='guest'){ sel.dataset.prev='guest'; return; }

  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===currentSeminarId);
  const a=(s?.attendees||[]).find(x=>x.id===sel.dataset.id);
  if(!a){ sel.value=prev; return; }

  const normMob=(v)=>(v||'').replace(/\D/g,'');
  const normRmV=(v)=>(v||'').trim().toLowerCase();

  const list = newType==='equity' ? (DB.get('eq_clients')||[]) : (DB.get('mf_clients')||[]);
  const match = list.some(c=>
    normMob(c.mobile)===normMob(a.mobile) && normMob(a.mobile)!=='' &&
    normRmV(c.rm)===normRmV(a.rm)
  );

  if(!match){
    toast(`Cannot change type — no ${newType==='equity'?'Equity':'MF'} client record found with the same mobile and RM`,'error');
    sel.value=prev;
    return;
  }
  sel.dataset.prev=newType;
}


// ── Seminar → Lead helpers ──────────────────────────────────────────────────

function toggleAllSeminarChk(masterChk){
  document.querySelectorAll('.sem-chk').forEach(c=>{ c.checked=masterChk.checked; });
  onSemChkChange();
}

function onSemChkChange(){
  const anyChecked = !!document.querySelector('.sem-chk:checked');
  const btn=document.getElementById('semAddLeadBtn');
  if(btn) btn.style.display = anyChecked ? '' : 'none';
  // update select-all state
  const all=document.querySelectorAll('.sem-chk');
  const checked=document.querySelectorAll('.sem-chk:checked');
  const masterChk=document.getElementById('semChkAll');
  if(masterChk){
    masterChk.indeterminate = checked.length>0 && checked.length<all.length;
    masterChk.checked = all.length>0 && checked.length===all.length;
  }
}

async function addSeminarAttendeesToLeads(){
  const checked=[...document.querySelectorAll('.sem-chk:checked')].map(c=>c.dataset.id);
  if(!checked.length){ toast('No attendee selected','error'); return; }

  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===currentSeminarId);
  if(!s) return;

  // Only admin or own-RM attendees selectable — but we still double-check
  const isAllowed = att => CU.role==='admin' || (att.rm||'').trim().toLowerCase()===(CU.name||'').trim().toLowerCase();

  const cleanMob=v=>String(v||'').replace(/\D/g,'').slice(-10);
  const leads=DB.get('leads')||[];

  let added=0, skippedDup=[], skippedGuest=[];

  for(const aid of checked){
    const att=(s.attendees||[]).find(x=>x.id===aid);
    if(!att || !isAllowed(att)) continue;

    // Guests (no mobile/name link) — skip with note
    if(att.type==='guest'){
      skippedGuest.push(att.name||'Guest');
      continue;
    }

    const mob=cleanMob(att.mobile||'');

    // Duplicate checks — mobile match anywhere (leads, eq, mf)
    if(mob){
      if(leads.find(l=>cleanMob(l.mobile)===mob)){ skippedDup.push(att.name+' (already in Leads)'); continue; }
      if((DB.get('eq_clients')||[]).find(c=>cleanMob(c.mobile)===mob)){ skippedDup.push(att.name+' (Equity Client)'); continue; }
      if((DB.get('mf_clients')||[]).find(c=>cleanMob(c.mobile)===mob)){ skippedDup.push(att.name+' (MF Investor)'); continue; }
    }

    // Build lead record
    const newId=uid();
    const rm = normRm(att.rm||CU.name||'');
    const rec={
      id:newId,
      name:att.name||'',
      mobile:att.mobile||'',
      rm,
      last_call:'',
      next_call:'',
      followup_status:'Pending',
      remarks:`Seminar: ${s.name}${s.date?' ('+fmtDate(s.date)+')':''}`,
      created:today(),
      updated:today()
    };
    await DB.setClient('leads', rec);
    added++;
  }

  // Uncheck all
  document.querySelectorAll('.sem-chk').forEach(c=>c.checked=false);
  const masterChk=document.getElementById('semChkAll');
  if(masterChk){ masterChk.checked=false; masterChk.indeterminate=false; }
  document.getElementById('semAddLeadBtn').style.display='none';

  // Summary toast
  let msg=`${added} lead${added!==1?'s':''} added!`;
  if(skippedDup.length) msg+=` | ${skippedDup.length} already exist (skip).`;
  if(skippedGuest.length) msg+=` | ${skippedGuest.length} guest skip.`;
  toast(msg, added>0?'success':'error');

  renderLeadsTable();
  updateBadges();
}

// ── End Seminar → Lead helpers ───────────────────────────────────────────────

async function setAttendeeStatus(attendeeId,status){
  await DB.mutateSeminar(currentSeminarId, sem=>{
    const a=(sem.attendees||[]).find(x=>x.id===attendeeId);
    if(!a) return false;
    a.status=status;
    sem.updated=today();
  });
  renderSeminarsTable();
}

async function setAttendeeRsvp(attendeeId,rsvp){
  await DB.mutateSeminar(currentSeminarId, sem=>{
    const a=(sem.attendees||[]).find(x=>x.id===attendeeId);
    if(!a) return false;
    a.rsvp=rsvp;
    sem.updated=today();
  });
}

async function setAttendeeCount(attendeeId,val){
  await DB.mutateSeminar(currentSeminarId, sem=>{
    const a=(sem.attendees||[]).find(x=>x.id===attendeeId);
    if(!a) return false;
    a.expected_count = val===''?null:parseInt(val);
    sem.updated=today();
  });
}

async function setAttendeeRemarks(attendeeId,val){
  await DB.mutateSeminar(currentSeminarId, sem=>{
    const a=(sem.attendees||[]).find(x=>x.id===attendeeId);
    if(!a) return false;
    a.remarks = val;
    sem.updated=today();
  });
}

// Admin-only: RM ko table se hi change karo (equity + mf dono RMs). Client-linked
// attendee ho to master record ka RM bhi sync ho jata hai.
async function onAttRmChange(sel){
  if(CU.role!=='admin'){ sel.value=sel.dataset.prev||''; toast('Only Admin can change RM','error'); return; }
  const newRm=normRm(sel.value);
  let a=null, srcId=null, srcType=null;
  const r = await DB.mutateSeminar(currentSeminarId, sem=>{
    a=(sem.attendees||[]).find(x=>x.id===sel.dataset.id);
    if(!a) return false;
    a.rm=newRm;
    sem.updated=today();
    srcId=a.source_id; srcType=a.type;
  });
  if(!r.ok || r.aborted) return;
  // Client master RM sync (equity/mf linked attendee)
  if(srcId && (srcType==='equity'||srcType==='mf')){
    const coll = srcType==='equity' ? 'eq_clients' : 'mf_clients';
    const list=DB.get(coll)||[];
    const c=list.find(x=>x.id===srcId);
    if(c && (c.rm||'')!==newRm){ c.rm=newRm; await DB.setClient(coll,c); }
  }
  sel.dataset.prev=newRm;
  toast('RM updated'+(newRm?' → '+newRm:''),'success');
  try{ rebuildSemRmOptions((DB.get('seminars')||[]).find(x=>x.id===currentSeminarId)?.attendees||[]); }catch(e){}
}

async function saveSeminarAttendeeChanges(){
  // Read the DOM edits first (independent of any stale local `s` snapshot),
  // then apply them inside the transaction onto whatever is actually on the
  // server at write time — same fix as every other attendee mutation here.
  const edits=[];
  document.querySelectorAll('#seminar-attendees-table tbody tr').forEach(row=>{
    const id=row.dataset.id;
    if(!id) return;
    const rsvpEl=row.querySelector('.att-rsvp');
    const countEl=row.querySelector('.att-count');
    const statusEl=row.querySelector('.att-status');
    const remarksEl=row.querySelector('.att-remarks');
    const typeEl=row.querySelector('.att-type');
    edits.push({
      id,
      rsvp: rsvpEl?rsvpEl.value:undefined,
      count: countEl?(countEl.value===''?null:parseInt(countEl.value)):undefined,
      status: statusEl?statusEl.value:undefined,
      remarks: remarksEl?remarksEl.value:undefined,
      type: typeEl?typeEl.value:undefined
    });
  });
  await DB.mutateSeminar(currentSeminarId, sem=>{
    edits.forEach(e=>{
      const a=(sem.attendees||[]).find(x=>x.id===e.id);
      if(!a) return;
      // Only save changes for attendees this user is actually allowed to edit.
      // Other RMs' rows are rendered disabled (read-only) in this table, but
      // their <select>/<input> elements still exist in the DOM — reading and
      // re-saving their values here was silently overwriting other RMs'
      // attendance status (e.g. resetting "Attended" back to "Pending").
      const isMyAttendee = CU.role==='admin' || (a.rm||'').trim().toLowerCase()===(CU.name||'').trim().toLowerCase();
      if(!isMyAttendee) return;
      if(e.rsvp!==undefined) a.rsvp=e.rsvp;
      if(e.count!==undefined) a.expected_count=e.count;
      if(e.status!==undefined) a.status=e.status;
      if(e.remarks!==undefined) a.remarks=e.remarks;
      if(e.type!==undefined) a.type=e.type;
    });
    sem.updated=today();
  });
  renderSeminarsTable();
  toast('Changes saved!','success');
}

function exportSeminarAttendees(){
  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===currentSeminarId);
  if(!s) return;
  const attendees=(s.attendees||[]).slice().sort((a,b)=>(a.name||'').localeCompare(b.name||''));
  if(!attendees.length){ toast('No attendees to export','error'); return; }

  const headers=['#','Name','Mobile','RM','Type','Response','Headcount','Status','Remarks'];
  const rows=attendees.map((a,i)=>[
    i+1, upc(a.name), upc(a.mobile), upc(a.rm), upc(a.type==='equity'?'Equity':a.type==='mf'?'MF':'Guest'),
    upc(a.rsvp||'—'), a.expected_count!=null?a.expected_count:'', upc(a.status||'Pending'), upc(a.remarks)
  ]);
  const willCome = attendees.filter(x=>x.rsvp==='Yes').length;
  const waitCount = attendees.filter(x=>x.rsvp==='Wait').length;
  const expectedTotal = attendees.reduce((sum,x)=>sum+(x.expected_count||0),0);
  const attended = attendees.filter(x=>x.status==='Attended').length;
  const colCount = headers.length;

  const esc = v => String(v==null?'':v).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

  let html = `<table border="1" cellspacing="0" cellpadding="6" style="border-collapse:collapse;font-family:Calibri,Arial,sans-serif;font-size:11pt;border-color:#808a99">`;
  // Title row
  html += `<tr><td colspan="${colCount}" style="background:#00B0F0;color:#FFFFFF;font-weight:bold;font-size:14pt;text-align:center;padding:10px">${esc(s.name)}</td></tr>`;
  // Subtitle row
  html += `<tr><td colspan="${colCount}" style="background:#00B0F0;color:#FFFFFF;font-size:11pt;text-align:center;padding:8px">Date: ${esc(fmtDate(s.date)||'—')}${s.notes?'   |   Venue: '+esc(s.notes):''}</td></tr>`;
  // Summary row
  html += `<tr><td colspan="${colCount}" style="background:#00B050;color:#FFFFFF;font-weight:bold;font-size:11pt;text-align:center;padding:8px">Total Attendees: ${attendees.length}   |   Will Attend: ${willCome}   |   Wait: ${waitCount}   |   Expected Headcount: ${expectedTotal}   |   Attended: ${attended}</td></tr>`;
  // Blank spacer row
  html += `<tr>${headers.map(()=>`<td style="border:none"></td>`).join('')}</tr>`;
  // Header row
  html += `<tr>${headers.map(h=>`<td style="border:1px solid #808a99;background:#FFFF00;color:#FF0000;font-weight:bold;font-size:11pt;text-align:center;padding:8px">${esc(h)}</td>`).join('')}</tr>`;
  // Data rows
  rows.forEach((row,ri)=>{
    const bg = ri%2===1 ? '#F2F6FB' : '#FFFFFF';
    html += '<tr>' + row.map((val,ci)=>{
      const align = ci===1 ? 'left' : 'center';
      return `<td style="border:1px solid #808a99;background:${bg};font-size:11pt;text-align:${align};padding:7px">${esc(val)}</td>`;
    }).join('') + '</tr>';
  });
  html += '</table>';

  const fullHtml = `<html xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:x="urn:schemas-microsoft-com:office:excel" xmlns="http://www.w3.org/TR/REC-html40">
  <head><meta charset="UTF-8"><!--[if gte mso 9]><xml><x:ExcelWorkbook><x:ExcelWorksheets><x:ExcelWorksheet><x:Name>Attendees</x:Name><x:WorksheetOptions><x:DisplayGridlines/></x:WorksheetOptions></x:ExcelWorksheet></x:ExcelWorksheets></x:ExcelWorkbook></xml><![endif]--></head>
  <body>${html}</body></html>`;

  const blob = new Blob(['\ufeff'+fullHtml], {type:'application/vnd.ms-excel'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = (s.name||'seminar').replace(/[^a-z0-9]+/gi,'_')+'_attendees_'+today()+'.xls';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
  toast('Export done!','success');
}

// ── Seminar Feedback responses (inside CRM) ───────────────────
function fbIntLabel(v){ return (v==='Haan'||v==='Yes')?'Yes':((v==='Shayad'||v==='Maybe')?'Maybe':((v==='Nahi'||v==='No')?'No':(v||''))); }
async function openSeminarFeedback(){
  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===currentSeminarId);
  if(!s) return;

  let ov=document.getElementById('sem-fb-overlay');
  if(!ov){
    ov=document.createElement('div');
    ov.id='sem-fb-overlay';
    ov.style.cssText='position:fixed;inset:0;background:rgba(15,23,42,.6);z-index:10000;display:flex;align-items:center;justify-content:center;padding:16px';
    document.body.appendChild(ov);
  }
  ov.onclick=e=>{ if(e.target===ov) ov.remove(); };
  ov.innerHTML='<div style="background:#fff;border-radius:14px;width:min(900px,96vw);max-height:88vh;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 20px 50px rgba(0,0,0,.3)" onclick="event.stopPropagation()"><div style="padding:40px;text-align:center;color:#888">Loading feedback...</div></div>';

  let all=[];
  try{
    const snap=await fdb.collection('seminar_feedback').get();
    all=snap.docs.map(d=>({...d.data(), _docId:d.id}));
  }catch(e){
    ov.innerHTML='<div style="background:#fff;border-radius:14px;padding:30px" onclick="event.stopPropagation()">Could not load feedback — please check your internet.<br><br><button class="btn btn-outline" onclick="document.getElementById(\'sem-fb-overlay\').remove()">Close</button></div>';
    return;
  }

  // Is seminar ke responses (id se, purane responses name-match fallback)
  const resp=all.filter(r=>String(r.seminarId||'')===String(s.id) || (r.seminarName||'').startsWith(s.name||'~~'))
                .sort((a,b)=>(b.ts||'').localeCompare(a.ts||''));
  window._semFbList=resp;
  window._semFbSeminar={name:s.name||'', date:s.date||'', location:s.location||''};

  const avg=k=>resp.length?(resp.reduce((t,r)=>t+Number(r[k]||0),0)/resp.length).toFixed(1):'—';
  const haan=resp.filter(r=>fbIntLabel(r.interested)==='Yes');
  const stars=n=>'<span style="color:#f59e0b">'+('★'.repeat(n||0))+('☆'.repeat(5-(n||0)))+'</span>';
  const chip=iv=>{const v=fbIntLabel(iv);return v==='Yes'?'<span style="background:#e6f4ea;color:#137333;padding:2px 10px;border-radius:12px;font-size:11px;font-weight:600">Yes ✓</span>'
    :(v==='Maybe'?'<span style="background:#fef7e0;color:#b06000;padding:2px 10px;border-radius:12px;font-size:11px">Maybe</span>'
    :'<span style="background:#fce8e6;color:#c5221f;padding:2px 10px;border-radius:12px;font-size:11px">No</span>');};
  const prodChips=r=>(r.products||[]).map(p=>'<span style="display:inline-block;background:#f0ebf8;color:#673ab7;padding:1px 8px;border-radius:10px;margin:1px;font-size:10px">'+esc(p)+'</span>').join('');
  const topicChips=r=>(r.topics||[]).map(p=>'<span style="display:inline-block;background:#e8f0fe;color:#1967d2;padding:1px 8px;border-radius:10px;margin:1px;font-size:10px">'+esc(p)+'</span>').join('');
  const refChips=(r,i)=>(r.references||[]).map((rf,j)=>'<span style="display:inline-block;background:#fdf2f8;color:#be185d;border:1px solid #fbcfe8;padding:1px 6px;border-radius:10px;margin:1px;font-size:10px">'+esc(rf.name)+(rf.mobile?' \u00b7 '+esc(rf.mobile):'')+' <a href="javascript:void(0)" onclick="fbRefToLead('+i+','+j+')" title="Add this reference as Lead" style="text-decoration:none;font-weight:700;color:#be185d">\u2795</a></span>').join('')||'<span style="color:#ccc;font-size:.72rem">\u2014</span>';
  const esc=t=>String(t||'').replace(/</g,'&lt;');

  const cards='<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:10px;margin-bottom:12px">'
    +'<div style="background:#f0ebf8;border-radius:8px;padding:10px 12px"><div style="font-size:10.5px;color:#673ab7;text-transform:uppercase">Responses</div><div style="font-size:20px;font-weight:700;color:#673ab7">'+resp.length+'</div></div>'
    +'<div style="background:#fff7ed;border-radius:8px;padding:10px 12px"><div style="font-size:10.5px;color:#b45309;text-transform:uppercase">Avg Experience</div><div style="font-size:20px;font-weight:700;color:#b45309">'+avg('rating')+' / 5</div></div>'
    +'<div style="background:#fff7ed;border-radius:8px;padding:10px 12px"><div style="font-size:10.5px;color:#b45309;text-transform:uppercase">Avg Content</div><div style="font-size:20px;font-weight:700;color:#b45309">'+avg('contentRating')+' / 5</div></div>'
    +'<div style="background:#e6f4ea;border-radius:8px;padding:10px 12px"><div style="font-size:10.5px;color:#137333;text-transform:uppercase">Interested (Yes)</div><div style="font-size:20px;font-weight:700;color:#137333">'+haan.length+'</div></div>'
    +'<div style="background:#fdf2f8;border-radius:8px;padding:10px 12px"><div style="font-size:10.5px;color:#be185d;text-transform:uppercase">References</div><div style="font-size:20px;font-weight:700;color:#be185d">'+resp.reduce((t,r)=>t+((r.references||[]).length),0)+'</div></div>'
    +'</div>';

  const rows=resp.map((r,i)=>'<tr style="border-bottom:1px solid #eee'+(fbIntLabel(r.interested)==='Yes'?';background:#f4fbf6':'')+'">'
    +'<td style="padding:7px 8px;white-space:nowrap;color:#888">'+(r.ts?new Date(r.ts).toLocaleDateString('en-IN',{day:'2-digit',month:'short'}):'')+'</td>'
    +'<td style="padding:7px 8px;font-weight:600">'+esc(r.name)+'</td>'
    +'<td style="padding:7px 8px">'+esc(r.mobile)+'</td>'
    +'<td style="padding:7px 8px">'+(r.rm?'<span style="background:#e0f7fc;color:#0891b2;padding:2px 10px;border-radius:12px;font-size:11px;font-weight:600">'+esc(r.rm)+'</span>':'<span style="color:#ccc;font-size:.72rem">—</span>')+'</td>'
    +'<td style="padding:7px 8px;white-space:nowrap">'+stars(r.rating)+'</td>'
    +'<td style="padding:7px 8px;white-space:nowrap">'+stars(r.contentRating)+'</td>'
    +'<td style="padding:7px 8px">'+chip(r.interested)+'</td>'
    +'<td style="padding:7px 8px"><div style="max-width:150px;white-space:normal;line-height:1.9">'+prodChips(r)+'</div></td>'
    +'<td style="padding:7px 8px"><div style="max-width:220px;white-space:normal;word-break:break-word;overflow-wrap:anywhere;line-height:1.9">'+refChips(r,i)+'</div></td>'
    +'<td style="padding:7px 8px;white-space:nowrap;position:sticky;right:0;background:'+(fbIntLabel(r.interested)==='Yes'?'#f4fbf6':'#fff')+';box-shadow:-6px 0 8px -5px rgba(0,0,0,.12)"><button class="btn btn-outline" style="padding:3px 10px;font-size:.7rem" onclick="fbToLead('+i+')">➕ Lead</button> <button class="btn-icon" style="color:var(--red)" title="Delete response" onclick="fbDelete('+i+')">🗑️</button></td>'
    +'</tr>').join('');

  ov.innerHTML='<div style="background:#fff;border-radius:14px;width:min(1080px,96vw);max-height:88vh;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 20px 50px rgba(0,0,0,.3)" onclick="event.stopPropagation()">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;padding:14px 20px;background:#673ab7">'
    +'<div><div style="font-weight:700;font-size:15px;color:#fff">📋 Feedback — '+esc(s.name)+'</div>'
    +'<div style="font-size:12px;color:#d9cdf2">'+(s.date?fmtDate(s.date):'')+'</div></div>'
    +'<div style="display:flex;align-items:center;gap:8px">'
    +(resp.length?'<button onclick="fbExportExcel()" title="Download colorful Excel report" style="background:#1D6F42;border:none;color:#fff;font-weight:600;font-size:12px;padding:7px 14px;border-radius:8px;cursor:pointer;display:flex;align-items:center;gap:5px">\uD83D\uDCE5 Excel Report</button>':'')
    +'<button onclick="document.getElementById(\'sem-fb-overlay\').remove()" style="background:rgba(255,255,255,.15);border:none;width:30px;height:30px;border-radius:50%;font-size:15px;cursor:pointer;color:#fff">\u2715</button></div>'
    +'</div>'
    +'<div style="padding:16px 20px;overflow-y:auto">'
    +cards
    +(haan.length?'<div style="margin-bottom:10px"><button class="btn btn-teal" style="font-size:.78rem" onclick="fbAllHaanToLeads()">➕ Add all \''+'Yes'+'\' responses to Leads ('+haan.length+')</button></div>':'')
    +(resp.length
      ?'<div style="border:1px solid #eee;border-radius:8px;overflow:auto;max-height:48vh"><table style="width:100%;border-collapse:collapse;font-size:12.5px"><thead><tr style="background:#f7f5fb">'
        +'<th style="padding:8px;text-align:left;font-size:11px;color:#673ab7">DATE</th><th style="padding:8px;text-align:left;font-size:11px;color:#673ab7">NAME</th><th style="padding:8px;text-align:left;font-size:11px;color:#673ab7">MOBILE</th><th style="padding:8px;text-align:left;font-size:11px;color:#673ab7">RM</th><th style="padding:8px;text-align:left;font-size:11px;color:#673ab7">EXP.</th><th style="padding:8px;text-align:left;font-size:11px;color:#673ab7">CONTENT</th><th style="padding:8px;text-align:left;font-size:11px;color:#673ab7">INTERESTED</th><th style="padding:8px;text-align:left;font-size:11px;color:#673ab7">PRODUCTS</th><th style="padding:8px;text-align:left;font-size:11px;color:#673ab7">REFERENCES</th><th style="padding:8px;font-size:11px;color:#673ab7;position:sticky;right:0;background:#f7f5fb;box-shadow:-6px 0 8px -5px rgba(0,0,0,.12)">ACTION</th>'
        +'</tr></thead><tbody>'+rows+'</tbody></table></div>'
      :'<div style="text-align:center;padding:30px;color:#999">No feedback received for this seminar yet.<br><small>💬 Use the Feedback Link button to send the link to attendees.</small></div>')
    +'</div></div>';
}

// ── Feedback → colorful Excel report (xlsx-js-style via dnXlsxBook) ──
function fbExportExcel(){
  const list=window._semFbList||[];
  if(!list.length){ toast('No feedback responses to export','error'); return; }
  const sem=window._semFbSeminar||{};
  const UP=a=>a.map(row=>row.map(v=>typeof v==='string'?upc(v):v));

  const haan=list.filter(r=>fbIntLabel(r.interested)==='Yes').length;
  const shayad=list.filter(r=>fbIntLabel(r.interested)==='Maybe').length;
  const nahi=list.filter(r=>r.interested && fbIntLabel(r.interested)!=='Yes' && fbIntLabel(r.interested)!=='Maybe').length;
  const avg=k=>list.length?Math.round(list.reduce((t,r)=>t+Number(r[k]||0),0)/list.length*10)/10:0;

  // Product & topic interest counts
  const countMap=key=>{
    const m={};
    list.forEach(r=>(r[key]||[]).forEach(p=>{ m[p]=(m[p]||0)+1; }));
    return Object.entries(m).sort((a,b)=>b[1]-a[1]);
  };
  const prodCounts=countMap('products');
  const topicCounts=countMap('topics');

  // Colour helpers
  const GREEN={bg:'FFC6EFCE',font:'FF006100'}, AMBER={bg:'FFFFEB9C',font:'FF9C6500'}, RED={bg:'FFFFC7CE',font:'FF9C0006'};
  const starTxt=n=>{ n=Number(n)||0; return n?('\u2605'.repeat(n)+'\u2606'.repeat(5-n)):''; };
  const ratingColor=v=>{ const n=(String(v||'').match(/\u2605/g)||[]).length || (Number(v)||0); return n>=4?GREEN:(n===3?AMBER:(n>0?RED:null)); };
  const intColor=v=>{v=String(v||'').toUpperCase();return v==='YES'?GREEN:(v==='MAYBE'?AMBER:(v?RED:null));};
  const summaryColor=(v,row)=>{
    const m=String(row[0]||'');
    if(/— Yes/i.test(m)) return GREEN;
    if(/— Maybe/i.test(m)) return AMBER;
    if(/— No/i.test(m)) return RED;
    if(/Avg/i.test(m)) return ratingColor(v);
    return null;
  };

  const summaryRows=[
    ['Seminar', sem.name||''],
    ['Date', sem.date?fmtDate(sem.date):''],
    ['Total Responses', list.length],
    ['Avg Experience Rating (/5)', avg('rating')],
    ['Avg Content Rating (/5)', avg('contentRating')],
    ['Interested — Yes ✓', haan],
    ['Interested — Maybe', shayad],
    ['Interested — No', nahi]
  ];
  const totalRefs=list.reduce((t,r)=>t+((r.references||[]).length),0);
  if(totalRefs) summaryRows.push(['References Received', totalRefs]);
  prodCounts.forEach(([p,c])=>summaryRows.push(['Product Interest: '+p, c]));

  const respRows=list.map(r=>[
    r.ts?new Date(r.ts).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'}):'',
    r.name||'', r.mobile||'', r.rm||'',
    starTxt(r.rating), starTxt(r.contentRating),
    fbIntLabel(r.interested),
    (r.products||[]).join(', '),
    (r.references||[]).map(rf=>rf.name+(rf.mobile?' \u2014 '+rf.mobile:'')).join('; ')
  ]);

  const fname='Feedback_'+String(sem.name||'Seminar').replace(/[^a-zA-Z0-9]+/g,'_').replace(/^_+|_+$/g,'').slice(0,40)+'_'+today()+'.xlsx';

  dnXlsxBook(fname,[
    { name:'Summary',
      title:upc('D N Investment — Seminar Feedback Summary: '+(sem.name||'')+(sem.date?' ('+fmtDate(sem.date)+')':'')),
      columns:[
        {header:'METRIC', width:36},
        {header:'VALUE', width:22, align:'center', color:summaryColor}
      ],
      rows:UP(summaryRows)
    },
    { name:'Responses',
      headerHeight:34,
      title:upc('Feedback Responses — '+(sem.name||'')+' | Total: '+list.length),
      columns:[
        {header:'DATE', width:13, align:'center', color:()=>({bg:'FFEFF3F8',font:'FF334155'})},
        {header:'NAME', width:22, color:()=>({bg:'FFF0EBF8',font:'FF4C2889'})},
        {header:'MOBILE', width:13, align:'center', color:()=>({bg:'FFE8F0FE',font:'FF1E40AF'})},
        {header:'RM', width:14, align:'center', color:()=>({bg:'FFE0F7FC',font:'FF0891B2'})},
        {header:'OVERALL EXPERIENCE\nHOW WAS THE SEMINAR? (1\u20135 \u2605)', width:24, align:'center', color:ratingColor},
        {header:'CONTENT QUALITY\nHOW USEFUL WAS THE CONTENT? (1\u20135 \u2605)', width:26, align:'center', color:ratingColor},
        {header:'INTERESTED', width:13, align:'center', color:intColor},
        {header:'PRODUCTS', width:34, color:()=>({bg:'FFF3EEFB',font:'FF5B21B6'})},
        {header:'REFERENCES', width:34, color:()=>({bg:'FFFCE7F3',font:'FF9D174D'})}
      ],
      rows:UP(respRows)
    }
  ]);
  toast('Excel report downloaded!','success');
}

async function fbDelete(idx){
  const r=(window._semFbList||[])[idx];
  if(!r) return;
  if(CU.role!=='admin'){ toast('Only Admin can delete feedback','error'); return; }
  if(!confirm('Permanently delete feedback response from "'+(r.name||'')+'"?')) return;
  try{
    if(r._docId){ await fdb.collection('seminar_feedback').doc(r._docId).delete(); }
    (window._semFbList||[]).splice(idx,1);
    toast('Response deleted','success');
    openSeminarFeedback(); // refresh modal
  }catch(e){ toast('Could not delete — please check your internet','error'); }
}

// Feedback respondent/referrer ka RM dhoondho — pehle seminar attendee se (mobile
// match), phir existing Lead / Equity Client / MF Investor se. Yaani "jis RM ke client
// ne response/reference diya" wahi RM. Admin entry kare tab bhi admin ka naam NAHI aata;
// sirf actual client-owner RM. Kahin na mile to '' (caller non-admin fallback lagata hai).
function rmForFeedbackPerson(r, s){
  if(r && (r.rm||'').trim()) return r.rm.trim();
  const cm=v=>String(v||'').replace(/\D/g,'').slice(-10);
  const mob=cm(r&&r.mobile);
  if(!mob) return '';
  const a=s&&(s.attendees||[]).find(x=>cm(x.mobile)===mob);
  if(a && (a.rm||'').trim()) return a.rm;
  const l=(DB.get('leads')||[]).find(x=>cm(x.mobile)===mob);
  if(l && (l.rm||'').trim()) return l.rm;
  const ec=(DB.get('eq_clients')||[]).find(x=>cm(x.mobile)===mob);
  if(ec && (ec.rm||'').trim()) return ec.rm;
  const mc=(DB.get('mf_clients')||[]).find(x=>cm(x.mobile)===mob);
  if(mc && (mc.rm||'').trim()) return mc.rm;
  return '';
}

async function fbToLead(idx, silent){
  const r=(window._semFbList||[])[idx];
  if(!r) return false;
  const cleanMob=v=>String(v||'').replace(/\D/g,'').slice(-10);
  const mob=cleanMob(r.mobile);
  const leads=DB.get('leads')||[];
  if(mob){
    if(leads.find(l=>cleanMob(l.mobile)===mob)){ if(!silent) toast(r.name+' — already in Leads','error'); return false; }
    if((DB.get('eq_clients')||[]).find(c=>cleanMob(c.mobile)===mob)){ if(!silent) toast(r.name+' — already Equity Client','error'); return false; }
    if((DB.get('mf_clients')||[]).find(c=>cleanMob(c.mobile)===mob)){ if(!silent) toast(r.name+' — already MF Investor','error'); return false; }
  }
  // RM: is respondent ka apna RM (attendee/lead/client se). Admin entry kare tab bhi
  // admin ka naam NAHI — na mile to non-admin entry-karta, warna blank.
  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===currentSeminarId);
  const rm=normRm(rmForFeedbackPerson(r, s) || (CU.role!=='admin' ? (CU.name||'') : ''));
  const rec={
    id:uid(), name:r.name||'', mobile:r.mobile||'', rm,
    source: s&&s.name?s.name:'Seminar Feedback',
    last_call:'', next_call:'', followup_status:'Pending',
    remarks:'Seminar feedback: Interested='+fbIntLabel(r.interested)+((r.products&&r.products.length)?' | Products: '+r.products.join(', '):'')+((r.topics&&r.topics.length)?' | Topic: '+r.topics.join(', '):'')+(r.comments?' | '+String(r.comments).slice(0,90):''),
    created:today(), updated:today()
  };
  await DB.setClient('leads', rec);
  if(!silent){ toast(r.name+' added to Leads!','success'); renderLeadsTable(); updateBadges(); }
  return true;
}

async function fbRefToLead(idx, refIdx){
  const r=(window._semFbList||[])[idx];
  const rf=r&&(r.references||[])[refIdx];
  if(!rf||!rf.name) return;
  const cleanMob=v=>String(v||'').replace(/\D/g,'').slice(-10);
  const mob=cleanMob(rf.mobile);
  const leads=DB.get('leads')||[];
  if(mob){
    if(leads.find(l=>cleanMob(l.mobile)===mob)){ toast(rf.name+' — already in Leads','error'); return; }
    if((DB.get('eq_clients')||[]).find(c=>cleanMob(c.mobile)===mob)){ toast(rf.name+' — already an Equity Client','error'); return; }
    if((DB.get('mf_clients')||[]).find(c=>cleanMob(c.mobile)===mob)){ toast(rf.name+' — already an MF Investor','error'); return; }
  }
  // RM: jis client (referrer r) ne reference diya, uska RM — attendee/lead/client se.
  // Admin entry kare tab bhi admin ka naam NAHI aata; kahin na mile to non-admin
  // entry-karta ka naam, warna blank (admin baad me assign kar de).
  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===currentSeminarId);
  const rm=normRm(rmForFeedbackPerson(r, s) || (CU.role!=='admin' ? (CU.name||'') : ''));
  const rec={
    id:uid(), name:rf.name||'', mobile:rf.mobile||'', rm,
    source:'Reference: '+(r.name||''),
    last_call:'', next_call:'', followup_status:'Pending',
    remarks:'Reference from seminar feedback — referred by '+(r.name||'')+(r.mobile?' ('+r.mobile+')':'')+(s&&s.name?' | Seminar: '+s.name:''),
    created:today(), updated:today()
  };
  DB.setClient('leads', rec).then(()=>{
    toast(rf.name+' added to Leads (reference)!','success');
    renderLeadsTable(); updateBadges();
  });
}

async function fbAllHaanToLeads(){
  const list=window._semFbList||[];
  let added=0, skipped=0;
  for(let i=0;i<list.length;i++){
    if(fbIntLabel(list[i].interested)!=='Yes') continue;
    const ok=await fbToLead(i, true);
    if(ok) added++; else skipped++;
  }
  toast(added+' lead(s) added!'+(skipped?' | '+skipped+' already exist (skip).':''), added>0?'success':'error');
  renderLeadsTable(); updateBadges();
}

// ── Feedback link share (WhatsApp) ──────────────────────────────
function feedbackLinkFor(seminarId){
  return location.origin + '/dninvest-feedback.html' + (seminarId ? ('?s='+encodeURIComponent(seminarId)) : '');
}
function openSeminarQuiz(){
  if(!(typeof CU!=='undefined' && CU && CU.role==='admin')) return;
  closeModal('seminarDetailModal');
  window.open(location.origin + '/dninvest-quiz.html?host=1', '_blank');
}
function shareFeedbackLink(){
  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===currentSeminarId);
  if(!s) return;
  const link=feedbackLinkFor(s.id);
  const msg='Hello! \uD83D\uDE4F\n\nThank you for attending the *D N Investment* seminar "'+(s.name||'')+'".\n\nPlease take a minute to share your feedback:\n'+link+'\n\n- D N Investment, Jamshedpur';
  // Copy link
  try{ navigator.clipboard.writeText(link); }catch(e){}
  toast('Link copied!','success');
  // WhatsApp share (group/broadcast me paste karne ke liye chat picker khulega)
  if(confirm('Link copied!\n\nShare on WhatsApp? (a ready message will open — just pick a group/contact)')){
    window.open('https://wa.me/?text='+encodeURIComponent(msg),'_blank');
  }
}
function sendFeedbackWA(attendeeId){
  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===currentSeminarId);
  if(!s) return;
  const a=(s.attendees||[]).find(x=>x.id===attendeeId);
  if(!a || !a.mobile){ toast('This attendee has no mobile number','error'); return; }
  const link=feedbackLinkFor(s.id);
  const msg='Hello '+(a.name||'')+'! \uD83D\uDE4F\n\nThank you for attending the *D N Investment* seminar.\n\nPlease take a minute to share your feedback:\n'+link+'\n\n- D N Investment, Jamshedpur';
  window.open('https://wa.me/91'+a.mobile+'?text='+encodeURIComponent(msg),'_blank');
}

let _editAttId=null;
function openEditAttendee(attendeeId){
  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===currentSeminarId);
  if(!s) return;
  const a=(s.attendees||[]).find(x=>x.id===attendeeId);
  if(!a) return;

  const isMine = CU.role==='admin' || (a.rm||'').trim().toLowerCase()===(CU.name||'').trim().toLowerCase();
  if(!isMine){ toast('This attendee is not mapped to you','error'); return; }

  _editAttId=attendeeId;
  document.getElementById('editAtt_name').value=a.name||'';
  document.getElementById('editAtt_mobile').value=a.mobile||'';

  // RM dropdown: admin ke liye editable, RM ke liye locked (apna hi naam)
  const rmSel=document.getElementById('editAtt_rm');
  const allRMs=[...new Set([...getSegRMs('equity'),...getSegRMs('mf'),(a.rm||'').trim()])].filter(Boolean).sort((x,y)=>x.localeCompare(y));
  rmSel.innerHTML=allRMs.map(r=>`<option ${r.trim().toLowerCase()===(a.rm||'').trim().toLowerCase()?'selected':''}>${r}</option>`).join('');
  rmSel.disabled = CU.role!=='admin';

  // Client-linked: sync option dikhao
  const syncWrap=document.getElementById('editAtt_syncwrap');
  const note=document.getElementById('editAtt_note');
  if(a.source_id && (a.type==='equity'||a.type==='mf')){
    syncWrap.style.display='block';
    document.getElementById('editAtt_sync').checked=true;
    note.textContent='This attendee is linked to an '+(a.type==='equity'?'Equity':'MF')+' client record.'+(CU.role!=='admin'?' Only Admin can change the RM.':'');
  } else {
    syncWrap.style.display='none';
    note.textContent=(CU.role!=='admin'?'Only Admin can change the RM.':'');
  }

  document.getElementById('editAttendeeModal').classList.add('open');
}

async function saveAttendeeEdit(){
  if(!_editAttId) return;
  const name=document.getElementById('editAtt_name').value.trim();
  const mobile=document.getElementById('editAtt_mobile').value.replace(/\D/g,'');
  const rmSel=document.getElementById('editAtt_rm');
  const rmIfAdmin = CU.role==='admin' ? normRm(rmSel.value) : null;

  if(!name){ toast('Name cannot be empty','error'); return; }
  if(mobile!=='' && mobile.length!==10){ toast('Enter a valid 10-digit mobile (or leave blank)','error'); return; }

  let srcId=null, srcType=null;
  const r = await DB.mutateSeminar(currentSeminarId, sem=>{
    const a=(sem.attendees||[]).find(x=>x.id===_editAttId);
    if(!a) return false;
    a.name=name; a.mobile=mobile;
    a.rm = rmIfAdmin!==null ? rmIfAdmin : a.rm; // non-admin: keep whatever RM is currently on the server
    sem.updated=today();
    srcId=a.source_id; srcType=a.type;
  });
  if(!r.ok || r.aborted) return;

  // Client master sync (name + mobile)
  if(srcId && (srcType==='equity'||srcType==='mf') && document.getElementById('editAtt_sync').checked){
    const coll = srcType==='equity' ? 'eq_clients' : 'mf_clients';
    const list=DB.get(coll)||[];
    const c=list.find(x=>x.id===srcId);
    if(c){
      let changed=false;
      if((c.name||'')!==name){ c.name=name; changed=true; }
      if((c.mobile||'')!==mobile && mobile!==''){ c.mobile=mobile; changed=true; }
      if(changed) await DB.setClient(coll,c);
    }
  }

  closeModal('editAttendeeModal');
  _editAttId=null;
  renderSeminarAttendees();
  renderSeminarsTable();
  toast('Attendee updated!','success');
}

async function removeAttendee(attendeeId){
  await DB.mutateSeminar(currentSeminarId, sem=>{
    sem.attendees=(sem.attendees||[]).filter(x=>x.id!==attendeeId);
    sem.updated=today();
  });
  renderSeminarAttendees();
  renderSeminarsTable();
}

function openAddAttendee(){
  document.getElementById('attendee-search').value='';
  document.getElementById('attendee-search-results').innerHTML='<div style="text-align:center;padding:20px;color:#bbb">Type a name or mobile number to search...</div>';
  document.getElementById('guest_name').value='';
  document.getElementById('guest_mobile').value='';
  const allRMs=[...new Set([...getSegRMs('equity'),...getSegRMs('mf')])];
  const guestRmSel=document.getElementById('guest_rm');
  guestRmSel.innerHTML='<option value="">Select RM</option>'+allRMs.map(r=>`<option ${r===CU.name?'selected':''}>${r}</option>`).join('');
  document.getElementById('addAttendeeModal').classList.add('open');
}

function searchAttendees(){
  const q=document.getElementById('attendee-search').value.trim().toLowerCase();
  const cont=document.getElementById('attendee-search-results');
  if(!q || q.length<2){
    cont.innerHTML='<div style="text-align:center;padding:20px;color:#bbb">Type a name or mobile number to search...</div>';
    return;
  }
  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===currentSeminarId);
  const existingIds=new Set((s?.attendees||[]).map(a=>a.source_id+'_'+a.type));

  // RM can only pick from clients mapped to them; admin sees all
  const myRm=(CU.name||'').trim().toLowerCase();
  const rmFilter = c => CU.role==='admin' || (c.rm||'').trim().toLowerCase()===myRm;
  const eq=(DB.get('eq_clients')||[]).filter(c=>rmFilter(c) && ((c.name||'').toLowerCase().includes(q)||(c.mobile||'').includes(q)));
  const mf=(DB.get('mf_clients')||[]).filter(c=>rmFilter(c) && ((c.name||'').toLowerCase().includes(q)||(c.mobile||'').includes(q)));

  const results=[
    ...eq.slice(0,20).map(c=>({...c,type:'equity'})),
    ...mf.slice(0,20).map(c=>({...c,type:'mf'}))
  ];

  if(!results.length){
    cont.innerHTML='<div style="text-align:center;padding:20px;color:#bbb">No matching clients/investors found'+(CU.role!=='admin'?' <br><small>(only your mapped clients appear in search)</small>':'')+'</div>';
    return;
  }
  let h=`<table><thead><tr><th>Name</th><th>Mobile</th><th>RM</th><th>Type</th><th>Action</th></tr></thead><tbody>`;
  results.forEach(c=>{
    const key=c.id+'_'+c.type;
    const already=existingIds.has(key);
    h+=`<tr>
      <td style="font-weight:600">${c.name}</td>
      <td>${c.mobile||'—'}</td>
      <td>${c.rm||'—'}</td>
      <td><span class="badge ${c.type==='equity'?'b-active':'b-investor'}">${c.type==='equity'?'Equity':'MF'}</span></td>
      <td>${already
        ? `<span style="color:var(--green);font-size:.78rem;font-weight:600">✓ Added</span>`
        : `<button class="btn-icon" onclick="addAttendee('${c.id}','${c.type}')">+ Add</button>`}</td>
      </tr>`;
  });
  h+='</tbody></table>';
  cont.innerHTML=h;
}

// ==== RIGHT-CLICK: Add client to a seminar directly from Equity/MF list ====
let _semCtxEl=null;
function closeClientSeminarMenu(){
  if(_semCtxEl){ _semCtxEl.remove(); _semCtxEl=null; }
  document.removeEventListener('click', closeClientSeminarMenu);
  document.removeEventListener('scroll', closeClientSeminarMenu, true);
}
function showClientSeminarMenu(ev, sourceId, type){
  ev.preventDefault(); ev.stopPropagation();
  closeClientSeminarMenu();
  const src = type==='equity'?(DB.get('eq_clients')||[]).find(c=>c.id===sourceId):(DB.get('mf_clients')||[]).find(c=>c.id===sourceId);
  const t=today();
  // Sirf aaj ya aane wale seminars — jinki date nikal chuki hai unme add nahi
  const list=(DB.get('seminars')||[])
    .filter(s=>(s.date||'')>=t)
    .sort((a,b)=>(a.date||'').localeCompare(b.date||''));  // nearest first
  const m=document.createElement('div');
  m.style.cssText='position:fixed;z-index:99999;background:#fff;border:1px solid var(--border,#ddd);border-radius:10px;box-shadow:0 8px 28px rgba(0,0,0,.18);min-width:240px;max-width:320px;max-height:60vh;overflow:auto;font-size:.82rem;padding:6px';
  let inner=`<div style="padding:7px 10px;font-weight:700;color:var(--navy);border-bottom:1px solid var(--border,#eee);margin-bottom:4px">➕ Add ${src?('“'+src.name+'”'):'client'} to Seminar</div>`;
  if(!list.length){
    inner+='<div style="padding:12px 10px;color:#999">No upcoming seminars — create one first.</div>';
  } else {
    list.forEach(s=>{
      const already=(s.attendees||[]).some(a=>a.source_id===sourceId && a.type===type);
      const upcoming=(s.date||'')>=t;
      inner+=`<div ${already?'':`onclick="addClientToSeminarDirect('${sourceId}','${type}','${s.id}');closeClientSeminarMenu()"`}
        style="padding:8px 10px;border-radius:7px;cursor:${already?'default':'pointer'};display:flex;justify-content:space-between;gap:10px;align-items:center;${already?'opacity:.55':''}"
        ${already?'':`onmouseover="this.style.background='#F0F5FB'" onmouseout="this.style.background='transparent'"`}>
        <span style="font-weight:600">${upcoming?'🟢 ':''}${s.name}</span>
        <span style="color:#888;white-space:nowrap;font-size:.72rem">${fmtDate(s.date)||''}${already?' ✓':''}</span>
      </div>`;
    });
  }
  m.innerHTML=inner;
  document.body.appendChild(m);
  const pad=8, mw=m.offsetWidth, mh=m.offsetHeight;
  let x=ev.clientX, y=ev.clientY;
  if(x+mw+pad>window.innerWidth) x=window.innerWidth-mw-pad;
  if(y+mh+pad>window.innerHeight) y=window.innerHeight-mh-pad;
  m.style.left=Math.max(pad,x)+'px'; m.style.top=Math.max(pad,y)+'px';
  _semCtxEl=m;
  setTimeout(()=>{ document.addEventListener('click', closeClientSeminarMenu); document.addEventListener('scroll', closeClientSeminarMenu, true); },0);
}
async function addClientToSeminarDirect(sourceId, type, seminarId){
  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===seminarId);
  if(!s){ toast('Seminar not found','error'); return; }
  const src = type==='equity'?(DB.get('eq_clients')||[]).find(c=>c.id===sourceId):(DB.get('mf_clients')||[]).find(c=>c.id===sourceId);
  if(!src){ toast('Client not found','error'); return; }
  if(CU.role!=='admin' && (src.rm||'').trim().toLowerCase()!==(CU.name||'').trim().toLowerCase()){
    toast('This client is mapped to '+(src.rm||'another RM')+' — you can only add your own clients','error'); return;
  }
  let already=false;
  const r = await DB.mutateSeminar(seminarId, sem=>{
    sem.attendees=sem.attendees||[];
    if(sem.attendees.some(a=>a.source_id===sourceId && a.type===type)){ already=true; return false; }
    let newId=uid(); while(sem.attendees.some(x=>x.id===newId)) newId=uid();
    sem.attendees.push({ id:newId, source_id:sourceId, type, name:src.name, mobile:src.mobile||'', rm:src.rm||'', status:'Pending' });
    sem.updated=today();
  });
  if(already){ toast(src.name+' already in "'+s.name+'"','error'); return; }
  if(!r.ok) return;
  toast('✓ '+src.name+' → "'+s.name+'"','success');
  if(currentSeminarId===seminarId){ try{ renderSeminarAttendees(); }catch(e){} }
  try{ renderSeminarsTable(); }catch(e){}
}

async function addAttendee(sourceId,type){
  const seminars=DB.get('seminars')||[];
  const s=seminars.find(x=>x.id===currentSeminarId);
  if(!s) return;
  const src=type==='equity'?(DB.get('eq_clients')||[]).find(c=>c.id===sourceId):(DB.get('mf_clients')||[]).find(c=>c.id===sourceId);
  if(!src) return;

  // Hard rule: RM sirf apne mapped client hi add kar sakta hai (kisi bhi segment me)
  if(CU.role!=='admin' && (src.rm||'').trim().toLowerCase()!==(CU.name||'').trim().toLowerCase()){
    toast('This client is mapped to '+(src.rm||'another RM')+' — you can only add your own mapped clients to a seminar','error');
    return;
  }

  const r = await DB.mutateSeminar(currentSeminarId, sem=>{
    sem.attendees = sem.attendees||[];
    if(sem.attendees.some(a=>a.source_id===sourceId && a.type===type)) return false; // already added — checked against the SERVER's latest list, not a stale local one
    let newId=uid();
    while(sem.attendees.some(x=>x.id===newId)) newId=uid();
    sem.attendees.push({ id:newId, source_id:sourceId, type, name:src.name, mobile:src.mobile||'', rm:src.rm||'', status:'Pending' });
    sem.updated=today();
  });
  if(!r.ok) return;
  renderSeminarAttendees();
  renderSeminarsTable();
  searchAttendees(); // refresh "Added" state
}

async function addGuestAttendee(){
  const name=document.getElementById('guest_name').value.trim();
  const mobile=document.getElementById('guest_mobile').value.trim();
  const rm=normRm(document.getElementById('guest_rm').value);
  if(!name){ toast('Please enter the guest name','error'); return; }

  const r = await DB.mutateSeminar(currentSeminarId, sem=>{
    sem.attendees = sem.attendees||[];
    let newId=uid();
    while(sem.attendees.some(x=>x.id===newId)) newId=uid();
    sem.attendees.push({ id:newId, source_id:null, type:'guest', name, mobile, rm:rm||(CU.role!=='admin'?CU.name:''), status:'Pending' });
    sem.updated=today();
  });
  if(!r.ok) return;

  document.getElementById('guest_name').value='';
  document.getElementById('guest_mobile').value='';

  renderSeminarAttendees();
  renderSeminarsTable();
  toast('Guest added!','success');
}


function openAddClient(seg){
  currentEditId=null;
  document.getElementById('clientModalTitle').textContent = seg==='equity'?'Add Equity Client':'Add MF Investor';
  document.getElementById('clientSaveBtn').textContent = seg==='equity'?'Save Client':'Save Investor';
  document.getElementById('clientSaveBtn').dataset.seg = seg;
  document.getElementById('clientModalBody').innerHTML = clientForm(seg, null);
  document.getElementById('clientModal').classList.add('open');
}

function editClient(id, seg){
  currentEditId=id;
  const clients = DB.get(seg==='equity'?'eq_clients':'mf_clients')||[];
  const c=clients.find(x=>x.id===id);
  if(!c) return;
  document.getElementById('clientModalTitle').textContent='Edit '+(seg==='equity'?'Equity Client':'MF Investor');
  document.getElementById('clientSaveBtn').textContent='Update';
  document.getElementById('clientSaveBtn').dataset.seg=seg;
  document.getElementById('clientModalBody').innerHTML=clientForm(seg,c);
  document.getElementById('clientModal').classList.add('open');
}

async function confirmDeleteClient(id, seg, name){
  if(CU.role!=='admin') return;
  if(!confirm(`Delete "${name}"? This cannot be undone.`)) return;
  const key = seg==='equity'?'eq_clients':'mf_clients';
  await DB.deleteClient(key, id);
  closeModal('viewModal');
  toast('Client deleted','success');
  if(seg==='equity') renderEqTable(); else renderMfTable();
  refreshDash(); updateBadges();
}

function rmFieldHtml(seg, c, label){
  const rms = getSegRMs(seg);
  if(CU.role!=='admin'){
    // Staff: for a NEW client, or a client that's already their own, lock the
    // RM field to their own name. For an EXISTING client that belongs to
    // someone else (e.g. editing via Temporary Access while covering an
    // absent colleague's clients), keep the client's real/original RM — the
    // field used to always force-write CU.name here, so simply opening and
    // saving another RM's client (even without touching this field) silently
    // reassigned it to whoever happened to be editing it.
    const lockedName = (c && c.rm && normRm(c.rm) !== normRm(CU.name)) ? c.rm : CU.name;
    return `<div class="form-field"><label>${label}</label>
      <input type="text" value="${lockedName}" disabled style="background:var(--bg);color:var(--gray)">
      <input type="hidden" id="f_rm" value="${lockedName}"></div>`;
  }
  const opts = rms.map(r=>`<option ${c&&c.rm===r?'selected':''}>${r}</option>`).join('');
  return `<div class="form-field"><label>${label}</label><select id="f_rm"><option value="">Select RM</option>${opts}</select></div>`;
}

// Shared "Call Update" section appended to Lead / Equity / MF edit forms.
// Lets the RM optionally log the edit as a call (so call history + count stay accurate).
function callMergeSection(){
  return `
  <div class="form-section" style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
    <button type="button" class="btn btn-success" id="m_call_toggle" style="padding:5px 14px;font-size:.85rem" onclick="toggleCallLogSection(this)">✓ Call will be logged (click to skip)</button>
    <span style="font-size:.72rem;color:var(--gray)">On by default — turn off only if you didn't call</span>
  </div>
  <div id="m_call_fields">
    <input type="checkbox" id="m_log_call" checked style="display:none">
    <div class="form-row">
      <div class="form-field"><label>Call Status</label><select id="m_call_status">
        <option>Done</option><option>No Answer</option><option>Callback Requested</option><option>Pending</option>
      </select></div>
      <div class="form-field"><label>Call Notes</label><input id="m_call_note" placeholder="What was discussed?"></div>
    </div>
  </div>`;
}

// Shows/hides the call-log fields. Default ON (RM usually updates after a call);
// turning it off skips logging the call for this save.
function toggleCallLogSection(btn){
  const div=document.getElementById('m_call_fields');
  const chk=document.getElementById('m_log_call');
  if(!div) return;
  const open = div.style.display==='none';
  div.style.display = open ? '' : 'none';
  if(chk) chk.checked = open;
  if(btn){
    btn.textContent = open ? '✓ Call will be logged (click to skip)' : '📞 Call NOT logged (click to log)';
    btn.classList.toggle('btn-success', open);
    btn.classList.toggle('btn-outline', !open);
  }
}

// If the "Log this update as a call" box is ticked, push a call_logs entry.
// seg: 'lead' | 'equity' | 'mf'. Uses the form's Last/Next call dates.
function maybeLogMergedCall(seg, id, name, rm){
  const chk=document.getElementById('m_log_call');
  if(!chk || !chk.checked) return;
  const status=(document.getElementById('m_call_status')||{value:'Done'}).value||'Done';
  const noteEl=document.getElementById('m_call_note');
  const note=noteEl?noteEl.value.trim():'';
  const lastEl=document.getElementById(seg==='lead'?'l_last_call':'f_last_call');
  const date=(lastEl&&lastEl.value)||today();
  const nextEl=document.getElementById(seg==='lead'?'l_next_call':'f_next_call');
  const nextCall=nextEl?nextEl.value:'';
  DB.addCallLog({
    id:uid(), client_id:id, seg, date, status, note,
    next_call:nextCall, created:today(), ts:new Date().toISOString(),
    client_name:name||'', rm:rm||'', remarks:note, by:CU.name
  });
}

function clientForm(seg, c){
  if(seg==='equity'){
    return `
    <div class="eq-narrow">
    <div class="form-section">Basic Information</div>
    <div class="form-row three">
      <div class="form-field"><label>Client Code</label><input id="f_code" value="${c?.code||''}" placeholder="e.g. 12345"></div>
      <div class="form-field"><label>Client Name *</label><input id="f_name" value="${c?.name||''}" placeholder="Full name"></div>
      <div class="form-field"><label>📱 Registered Number</label><input id="f_mobile" value="${c?.mobile||''}" placeholder="10 digit mobile"></div>
    </div>
    <div class="form-row four">
      <div class="form-field"><label>Email</label><input id="f_email" type="email" value="${c?.email||''}" placeholder="email@example.com"></div>
      <div class="form-field"><label>PAN</label><input id="f_pan" value="${c?.pan||''}" placeholder="ABCDE1234F" style="text-transform:uppercase"></div>
      ${rmFieldHtml('equity', c, 'Equity RM *')}
      <div class="form-field"><label>🎂 Date of Birth</label><input id="f_dob" type="date" value="${c?.dob||''}" max="${today()}"></div>
    </div>
    <div class="form-section">Trading Info</div>
    <div class="form-row three">
      <div class="form-field"><label>Trading Status ${(CU&&CU.role==='admin')?'':'<span style="color:var(--red);font-weight:400;font-size:.7rem">🔒 Admin only</span>'}</label><select id="f_status" ${(CU&&CU.role==='admin')?'':'disabled title="Only Admin can change the status"'}><option ${!c||c.status==='Active'?'selected':''}>Active</option><option ${c?.status==='Inactive'?'selected':''}>Inactive</option><option ${c?.status==='Closed'?'selected':''}>Closed</option></select></div>
      <div class="form-field"><label>Asset Value (₹)</label><input id="f_asset" type="number" value="${c?.asset_value||''}" placeholder="e.g. 500000"></div>
      <div class="form-field"><label>📞 Alternate Number <span style="color:var(--teal);font-weight:400;font-size:.72rem">(RM edit)</span></label><input id="f_alt_mobile" value="${c?.alt_mobile||''}" placeholder="Alternate / secondary mobile"></div>
    </div>
    <div class="form-row three">
      <div class="form-field"><label>Last Trade Date ${(CU&&CU.role==='admin')?'':'<span style="color:var(--red);font-weight:400;font-size:.7rem">🔒 Admin only</span>'}</label><input id="f_last_trade" type="date" value="${c?.last_trade_date||''}" ${(CU&&CU.role==='admin')?'':'disabled title="Only Admin can change the Last Trade Date"'}></div>
      <div class="form-field"><label>Last Trade Month</label><input id="f_last_month" value="${c?.last_trade_month||''}" placeholder="e.g. May-2025"></div>
      <div class="form-field"><label>Last Calling Date</label><input id="f_last_call" type="date" value="${c?.last_call_date||''}"${_lcAttr()}></div>
    </div>
    <div class="form-section">Follow-up</div>
    <div class="form-row three">
      <div class="form-field">
        <label>Next Calling Date</label>
        <input id="f_next_call" type="date" value="${c?.next_call||''}"${_ncAttr()} ${c?.do_not_call?'disabled':''}>
        <label style="display:flex;align-items:center;gap:5px;margin-top:6px;font-size:.8rem;cursor:pointer">
          <input type="checkbox" id="f_do_not_call" onchange="toggleDNC(this)" ${c?.do_not_call?'checked':''}>
          🚫 Do Not Call
        </label>
      </div>
      <div class="form-field"><label>Follow-up Status</label><select id="f_followup">
        <option ${c?.followup_status==='NOT INTERESTED'?'selected':''}>NOT INTERESTED</option>
        <option ${c?.followup_status==='CALL NOT RECEIVED'?'selected':''}>CALL NOT RECEIVED</option>
        <option ${c?.followup_status==='CALL NOT CONNECTED'?'selected':''}>CALL NOT CONNECTED</option>
        <option ${c?.followup_status==='CALL LATER'?'selected':''}>CALL LATER</option>
        <option ${c?.followup_status==='TRADE'?'selected':''}>TRADE</option>
        <option ${c?.followup_status==='INTERESTED IN OTHER PRODUCT'?'selected':''}>INTERESTED IN OTHER PRODUCT</option>
        <option ${c?.followup_status==='POSITIVE'?'selected':''}>POSITIVE</option>
        <option ${c?.followup_status==='WRONG NUMBER'?'selected':''}>WRONG NUMBER</option>
        <option ${c?.followup_status==='OPEN DEMAT ACCOUNT'?'selected':''}>OPEN DEMAT ACCOUNT</option>
        <option ${c?.followup_status==='COME TO OFFICE'?'selected':''}>COME TO OFFICE</option>
        <option ${c?.followup_status==='FUND PROBLEM'?'selected':''}>FUND PROBLEM</option>
        <option ${c?.followup_status==='TRANSFER REQUEST'?'selected':''}>TRANSFER REQUEST</option>
      </select></div>
      <div class="form-field"><label>Remarks</label><input id="f_remarks" value="${c?.remarks||''}" placeholder="Any remarks"></div>
    </div>
    ${callMergeSection()}
    </div>`;
  } else {
    return `
    <div class="form-section">Basic Information</div>
    <div class="form-row three">
      <div class="form-field"><label>Investor Name *</label><input id="f_name" value="${c?.name||''}" placeholder="Full name"></div>
      <div class="form-field"><label>📱 Registered Number</label><input id="f_mobile" value="${c?.mobile||''}" placeholder="10 digit mobile"></div>
      <div class="form-field"><label>PAN</label><input id="f_pan" value="${c?.pan||''}" placeholder="ABCDE1234F" style="text-transform:uppercase">
        <label style="display:flex;align-items:center;gap:6px;margin-top:5px;font-size:.76rem;cursor:pointer">
          <input type="checkbox" id="f_minor" ${c?.is_minor?'checked':''}> 👶 Minor (PAN optional / guardian's PAN)
        </label>
      </div>
    </div>
    <div class="form-row three">
      <div class="form-field"><label>Email</label><input id="f_email" type="email" value="${c?.email||''}" placeholder="email@example.com"></div>
      ${rmFieldHtml('mf', c, 'MF RM *')}
      <div class="form-field"><label>Investor Status ${(CU&&CU.role==='admin')?'':'<span style="color:var(--red);font-weight:400;font-size:.7rem">🔒 Admin only</span>'}</label><select id="f_status" ${(CU&&CU.role==='admin')?'':'disabled title="Only Admin can change the status"'}><option ${!c||c.status==='Investor'?'selected':''}>Investor</option><option ${c?.status==='Prospect'?'selected':''}>Prospect</option></select></div>
    </div>
    <div class="form-row three">
      <div class="form-field"><label>🎂 Date of Birth</label><input id="f_dob" type="date" value="${c?.dob||''}" max="${today()}"></div>
      <div class="form-field"><label>Client Code</label><input id="f_client_id" value="${c?.client_id||''}" placeholder="RTA Client ID"></div>
    </div>
    <div class="form-section">Investment Details</div>
    <div class="form-row three">
      <div class="form-field"><label>AUM (₹)</label><input id="f_aum" type="number" value="${c&&c.aum!=null?(Math.round(c.aum*100)/100):''}" placeholder="e.g. 1000000"></div>
      <div class="form-field"><label>SIP Amount (₹/month)</label><input id="f_sip" type="number" value="${c?.sip_amount||''}" placeholder="e.g. 5000"></div>
      <div class="form-field"><label>SIP Count</label><input id="f_sip_count" type="number" value="${c?.sip_count||''}" placeholder="No. of SIPs"></div>
    </div>
    <div class="form-row three">
      <div class="form-field"><label>Last Investment Date</label><input id="f_last_invest" type="date" value="${c?.last_invest_date||''}"></div>
      <div class="form-field"><label>Last Calling Date</label><input id="f_last_call" type="date" value="${c?.last_call_date||''}"${_lcAttr()}></div>
      <div class="form-field"><label>📞 Alternate Number <span style="color:var(--teal);font-weight:400;font-size:.72rem">(RM edit)</span></label><input id="f_alt_mobile" value="${c?.alt_mobile||''}" placeholder="Alternate / secondary mobile"></div>
    </div>
    <div class="form-section">Follow-up</div>
    <div class="form-row three">
      <div class="form-field">
        <label>Next Calling Date</label>
        <input id="f_next_call" type="date" value="${c?.next_call||''}"${_ncAttr()} ${c?.do_not_call?'disabled':''}>
        <label style="display:flex;align-items:center;gap:5px;margin-top:6px;font-size:.8rem;cursor:pointer">
          <input type="checkbox" id="f_do_not_call" onchange="toggleDNC(this)" ${c?.do_not_call?'checked':''}>
          🚫 Do Not Call
        </label>
      </div>
      <div class="form-field"><label>Follow-up Status</label><select id="f_followup">
        <option ${c?.followup_status==='NOT INTERESTED'?'selected':''}>NOT INTERESTED</option>
        <option ${c?.followup_status==='CALL NOT RECEIVED'?'selected':''}>CALL NOT RECEIVED</option>
        <option ${c?.followup_status==='CALL NOT CONNECTED'?'selected':''}>CALL NOT CONNECTED</option>
        <option ${c?.followup_status==='CALL LATER'?'selected':''}>CALL LATER</option>
        <option ${c?.followup_status==='INTERESTED IN SIP'?'selected':''}>INTERESTED IN SIP</option>
        <option ${c?.followup_status==='INTERESTED IN LUMPSUM'?'selected':''}>INTERESTED IN LUMPSUM</option>
        <option ${c?.followup_status==='INTERESTED IN OTHER PRODUCT'?'selected':''}>INTERESTED IN OTHER PRODUCT</option>
        <option ${c?.followup_status==='COME TO OFFICE'?'selected':''}>COME TO OFFICE</option>
        <option ${c?.followup_status==='FUND PROBLEM'?'selected':''}>FUND PROBLEM</option>
        <option ${c?.followup_status==='TRANSFER REQUEST'?'selected':''}>TRANSFER REQUEST</option>
      </select></div>
      <div class="form-field"><label>Remarks</label><input id="f_remarks" value="${c?.remarks||''}" placeholder="Any remarks"></div>
    </div>
    ${callMergeSection()}`;
  }
}

// Colorful confirm popup for Registered Number change. Returns Promise<boolean>.
function confirmRegNumberChange(oldN, newN){
  return new Promise(resolve=>{
    const ov=document.createElement('div');
    ov.className='modal-overlay open';
    ov.style.zIndex='99999';
    ov.innerHTML=`
      <div class="modal" style="width:430px;max-width:92vw;overflow:hidden">
        <div class="modal-hdr" style="background:linear-gradient(135deg,#dc2626,#f59e0b);color:#fff">
          <h3 style="color:#fff">⚠️ Registered Number Change</h3>
        </div>
        <div class="modal-body" style="text-align:center;padding:22px 20px">
          <p style="font-weight:700;color:var(--navy,#0a1f4d);margin:0 0 16px">You are changing the REGISTERED NUMBER.</p>
          <div style="display:flex;flex-direction:column;gap:10px;margin-bottom:18px">
            <div style="background:#fff1f1;border:1.5px solid #f5b5b5;border-radius:10px;padding:10px 14px;display:flex;justify-content:space-between;align-items:center">
              <span style="color:#dc2626;font-weight:700;font-size:.82rem">OLD</span>
              <span style="color:#dc2626;font-weight:800;font-size:1.1rem;letter-spacing:.5px;text-decoration:line-through">${oldN}</span>
            </div>
            <div style="font-size:1.2rem;color:#9ca3af">↓</div>
            <div style="background:#f0fdf4;border:1.5px solid #a7e3bd;border-radius:10px;padding:10px 14px;display:flex;justify-content:space-between;align-items:center">
              <span style="color:#16a34a;font-weight:700;font-size:.82rem">NEW</span>
              <span style="color:#16a34a;font-weight:800;font-size:1.1rem;letter-spacing:.5px">${newN}</span>
            </div>
          </div>
          <p style="color:var(--gray,#667);font-size:.86rem;margin:0 0 18px">Do you want to save this change?</p>
          <div style="display:flex;gap:10px;justify-content:center">
            <button class="btn btn-outline rc-cancel" style="min-width:110px">Cancel</button>
            <button class="btn rc-ok" style="min-width:130px;background:#16a34a;color:#fff;border:none">✅ Yes, Save</button>
          </div>
        </div>
      </div>`;
    document.body.appendChild(ov);
    const done=v=>{ ov.remove(); resolve(v); };
    ov.querySelector('.rc-ok').onclick=()=>done(true);
    ov.querySelector('.rc-cancel').onclick=()=>done(false);
    ov.addEventListener('click',e=>{ if(e.target===ov) done(false); });
  });
}

async function saveClient(){
  const seg = document.getElementById('clientSaveBtn').dataset.seg;
  const name = (document.getElementById('f_name')||{value:''}).value.trim();
  const rm = normRm((document.getElementById('f_rm')||{value:''}).value.trim());
  if(!name){ toast('Client name is required','error'); return; }
  if(!rm){ toast('Please select RM','error'); return; }

  // DOB mandatory when editing an existing client — hard block, not just a warning.
  if(currentEditId){
    const dobVal = (document.getElementById('f_dob')||{value:''}).value.trim();
    if(!dobVal){
      toast('⚠️ Date of Birth is required — cannot save without DOB', 'error');
      const el = document.getElementById('f_dob');
      if(el){ el.focus(); el.style.border='2px solid var(--red)'; setTimeout(()=>el.style.border='',3000); }
      return;
    }
  }

  // RM call-date lock (Admin-configured). Admin is unrestricted.
  if(CU && CU.role!=='admin'){
    const _cl=effectiveCallLimits();
    const _lc=(document.getElementById('f_last_call')||{value:''}).value;
    const _nc=(document.getElementById('f_next_call')||{value:''}).value;
    const _fd=s=>{ const p=String(s).split('-'); return p.length===3? p[2]+'-'+p[1]+'-'+p[0] : s; };
    if(_lc){ if(_cl.lcMin && _lc<_cl.lcMin){ toast('Last Call date cannot be before '+_fd(_cl.lcMin),'error'); return; }
             if(_cl.lcMax && _lc>_cl.lcMax){ toast('Last Call date cannot be after '+_fd(_cl.lcMax),'error'); return; } }
    if(_nc){ if(_cl.ncMin && _nc<_cl.ncMin){ toast('Next Call date cannot be before '+_fd(_cl.ncMin),'error'); return; }
             if(_cl.ncMax && _nc>_cl.ncMax){ toast('Next Call date cannot be after '+_fd(_cl.ncMax),'error'); return; } }
  }

  // Mandatory field validation
  if(seg==='equity'){
    const code = (document.getElementById('f_code')||{value:''}).value.trim();
    if(!code){
      toast('⚠️ Client Code / UCC is required — cannot save without Client Code','error');
      const el = document.getElementById('f_code');
      if(el){ el.focus(); el.style.border='2px solid var(--red)'; setTimeout(()=>el.style.border='',3000); }
      return;
    }
  } else {
    const pan = (document.getElementById('f_pan')||{value:''}).value.trim();
    const _minor = document.getElementById('f_minor')?.checked;
    if(!pan && !_minor){
      toast('⚠️ PAN Number is required — cannot save without PAN (tick 👶 Minor if no PAN)','error');
      const el = document.getElementById('f_pan');
      if(el){ el.focus(); el.style.border='2px solid var(--red)'; setTimeout(()=>el.style.border='',3000); }
      return;
    }
  }

  const key = seg==='equity'?'eq_clients':'mf_clients';
  const clients = DB.get(key)||[];

  const gv2 = id=>{ const el=document.getElementById(id); return el?el.value.trim():''; };

  // Duplicate check - MF: by PAN, Equity: by Client Code (mobile can be the same for family accounts)
  if(!currentEditId){
    if(seg==='mf'){
      const pan = gv2('f_pan').toUpperCase();
      const _minor = document.getElementById('f_minor')?.checked;
      // A minor has no PAN of their own — their record carries the GUARDIAN's
      // PAN. So a shared PAN between a minor and an adult is correct data, not
      // a duplicate. Skip the check when the record being saved is a minor
      // (below), and never treat an existing minor as the duplicate (!c.is_minor).
      if(pan && !_minor){
        const dup = clients.find(c=>!c.is_minor && c.pan && c.pan.trim().toUpperCase()===pan);
        if(dup){
          toast(`⚠️ MF Investor already exists: "${dup.name}" (PAN: ${dup.pan}) - RM: ${dup.rm||'—'}`,'error');
          return;
        }
      }
    } else {
      // Equity - check by Client Code (if code given)
      const code = gv2('f_code').toUpperCase();
      if(code){
        const dup = clients.find(c=>c.code && c.code.trim().toUpperCase()===code);
        if(dup){
          toast(`⚠️ Equity Client already exists: "${dup.name}" (Code: ${dup.code}) - RM: ${dup.rm||'—'}`,'error');
          return;
        }
      }
    }
  } else {
    // Edit mode - same check but exclude current client
    if(seg==='mf'){
      const pan = gv2('f_pan').toUpperCase();
      const _minor = document.getElementById('f_minor')?.checked;
      if(pan && !_minor){
        // !c.is_minor — a minor sharing this PAN is the guardian's child, not a clash
        const dup = clients.find(c=>c.id!==currentEditId && !c.is_minor && c.pan && c.pan.trim().toUpperCase()===pan);
        if(dup){
          toast(`⚠️ Yeh PAN already exists: "${dup.name}" (PAN: ${dup.pan}) - RM: ${dup.rm||'—'}`,'error');
          return;
        }
      }
    } else {
      const code = gv2('f_code').toUpperCase();
      if(code){
        const dup = clients.find(c=>c.id!==currentEditId && c.code && c.code.trim().toUpperCase()===code);
        if(dup){
          toast(`⚠️ Yeh Client Code already exists: "${dup.name}" (Code: ${dup.code}) - RM: ${dup.rm||'—'}`,'error');
          return;
        }
      }
    }
  }

  let rec;
  let newId = currentEditId;
  if(!newId){
    newId = uid();
    // safety: ensure no collision with existing client ids
    while(clients.some(x=>x.id===newId)) newId = uid();
  }
  // Original mobile is now editable by everyone (admin & RM).
  const _mobileVal = gv2('f_mobile');
  // Alert/confirm when the Registered (original) number is changed while editing.
  if(currentEditId){
    const _exC = clients.find(x=>x.id===currentEditId);
    if(_exC && (_exC.mobile||'') !== _mobileVal){
      const ok = await confirmRegNumberChange(_exC.mobile||'—', _mobileVal||'—');
      if(!ok) return; // abort save; user can review
    }
  }
  if(seg==='equity'){
    rec={
      id: newId,
      code:gv2('f_code'), name, mobile:_mobileVal, alt_mobile:gv2('f_alt_mobile'), pan:gv2('f_pan').toUpperCase(), email:gv2('f_email'), dob:gv2('f_dob'),
      rm, status:gv2('f_status')||'Active',
      asset_value:parseFloat(gv2('f_asset'))||null,
      revenue: (currentEditId ? (clients.find(x=>x.id===currentEditId)?.revenue ?? null) : null),
      last_trade_date:gv2('f_last_trade'), last_trade_month:gv2('f_last_month'),
      last_call_date:gv2('f_last_call'),
      next_call: (document.getElementById('f_do_not_call')?.checked ? '' : gv2('f_next_call')),
      do_not_call: document.getElementById('f_do_not_call')?.checked || false,
      followup_status:gv2('f_followup'), remarks:gv2('f_remarks'),
      created:currentEditId?undefined:today(), updated:today()
    };
  } else {
    rec={
      id: newId,
      name, mobile:_mobileVal, alt_mobile:gv2('f_alt_mobile'), pan:gv2('f_pan').toUpperCase(), email:gv2('f_email'), dob:gv2('f_dob'),
      client_id: gv2('f_client_id'),
      rm, status:gv2('f_status')||'Investor',
      is_minor: document.getElementById('f_minor')?.checked || false,
      aum:parseFloat(gv2('f_aum'))||null,
      sip_amount:parseFloat(gv2('f_sip'))||null,
      sip_count:parseInt(gv2('f_sip_count'))||null,
      sip_details: (currentEditId ? ((DB.get('mf_clients')||[]).find(x=>x.id===currentEditId)?.sip_details ?? null) : null),
      last_invest_date:gv2('f_last_invest'), last_call_date:gv2('f_last_call'),
      next_call: (document.getElementById('f_do_not_call')?.checked ? '' : gv2('f_next_call')),
      do_not_call: document.getElementById('f_do_not_call')?.checked || false,
      followup_status:gv2('f_followup'), remarks:gv2('f_remarks'),
      created:currentEditId?undefined:today(), updated:today()
    };
  }

  let _statusChangeMsg = '';
  if(currentEditId){
    const idx=clients.findIndex(x=>x.id===currentEditId);
    if(idx>=0){
      const old=clients[idx];
      rec.created=old.created||today();
      // Audit log - track changed fields
      const auditFields = seg==='equity'
        ? ['name','mobile','email','dob','rm','status','code','asset_value','revenue','last_trade_date','last_call_date','next_call','followup_status','remarks']
        : ['name','mobile','email','dob','rm','status','pan','client_id','aum','sip_amount','sip_count','last_invest_date','last_call_date','next_call','followup_status','remarks'];
      const changes = [];
      auditFields.forEach(f=>{
        const ov = old[f]===null||old[f]===undefined?'':String(old[f]);
        const nv = rec[f]===null||rec[f]===undefined?'':String(rec[f]);
        if(ov!==nv) changes.push({field:f, old:ov||'—', new:nv||'—'});
      });
      // Call out status (Active/Inactive/Closed/Investor/Prospect) changes by name explicitly,
      // so the person saving gets immediate confirmation of WHICH client's status changed.
      const statusCh = changes.find(ch=>ch.field==='status');
      if(statusCh) _statusChangeMsg = `✅ ${old.name}: status changed ${statusCh.old} → ${statusCh.new}`;
      if(changes.length>0){
        DB.addActivityLog({
          id: uid(),
          type: 'edit',
          seg,
          client_id: currentEditId,
          client_name: old.name,
          rm: old.rm,
          by: CU.name,
          date: new Date().toISOString(),
          changes
        });
      }
    }
  } else {
    rec.created=today();
    // Audit log - new client added
    DB.addActivityLog({
      id: uid(),
      type: 'add',
      seg,
      client_id: rec.id,
      client_name: rec.name,
      rm: rec.rm,
      by: CU.name,
      date: new Date().toISOString(),
      changes: []
    });
  }
  // Optimistic save: setClient() writes localStorage synchronously (and sets the
  // _writing guard) BEFORE its first network await, so we can update the UI
  // instantly instead of waiting 5-10s for the full-list Firestore transaction.
  // The transaction runs in the background and surfaces its own sync-error toast.
  const _savePromise = DB.setClient(key,rec);
  maybeLogMergedCall(seg, rec.id, rec.name, rec.rm);
  closeModal('clientModal');
  toast(_statusChangeMsg || (currentEditId?'Client updated!':'Client added!'),'success');
  const pg = getCurrentPageId();
  // Instantly refresh whatever list is visible — especially the Follow-ups page,
  // which this save path previously never re-rendered, so an edited row lingered
  // in the Today & Overdue list until a manual Refresh.
  if(pg==='eq-followup') renderFollowup('eqf');
  else if(pg==='mf-followup') renderFollowup('mff');
  else if(seg==='equity') renderEqTable();
  else renderMfTable();
  // Heavy/off-screen work (3k-row clients table, dashboard, badges) deferred past
  // the paint so it never blocks the row from updating.
  setTimeout(()=>{
    try{
      if(pg==='eq-followup' || pg==='mf-followup'){ if(seg==='equity') renderEqTable(); else renderMfTable(); }
      refreshDash();
      updateBadges();
    }catch(_){}
  }, 0);
  _savePromise.catch(()=>{});
}

// ══════════════════════════════════════════
// VIEW CLIENT
// ══════════════════════════════════════════
function viewClient(id,seg){
  const clients=DB.get(seg==='equity'?'eq_clients':'mf_clients')||[];
  const c=clients.find(x=>x.id===id);
  if(!c) return;
  const logs=(DB.get('call_logs')||[]).filter(l=>l.client_id===id).sort((a,b)=>b.date.localeCompare(a.date));
  const days=daysDiff(c.last_trade_date);

  document.getElementById('viewModalTitle').textContent=c.name+(seg==='equity'?' — Equity Client':' — MF Investor');
  const st=seg==='equity'?(c.status==='Active'?'b-active':'b-inactive'):(c.status==='Investor'?'b-investor':'b-prospect');

  let body=`
    <div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:18px;align-items:center">
      <span class="badge ${st}" style="font-size:.82rem;padding:4px 12px">${c.status}</span>
      ${c.mobile?`<a href="tel:${c.mobile}" class="btn btn-outline" style="padding:5px 14px">📞 ${c.mobile}</a>`:''}
      ${c.mobile?`<a href="https://wa.me/91${c.mobile}" target="_blank" class="btn btn-success" style="padding:5px 14px">💬 WhatsApp</a>`:''}
      ${c.alt_mobile?`<a href="tel:${c.alt_mobile}" class="btn btn-outline" style="padding:5px 14px" title="Alternate number">📞 ${c.alt_mobile} (Alt)</a>`:''}
      ${c.alt_mobile?`<a href="https://wa.me/91${c.alt_mobile}" target="_blank" class="btn btn-success" style="padding:5px 14px" title="Alternate WhatsApp">💬 Alt</a>`:''}
      ${CU.role!=='backoffice'?`<button class="btn btn-primary" style="padding:5px 14px" onclick="closeModal('viewModal');editClient('${id}','${seg}')">✏️ Edit / Log Call</button>`:''}
    </div>`;

  if(seg==='equity'){
    body+=`<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:18px">
      ${di('Client Code',c.code)}${di('RM',c.rm)}${di('Email',c.email)}
      ${di('🎂 Date of Birth',c.dob?fmtDate(c.dob):null)}
      ${di('Asset Value',c.asset_value?'₹'+fmtNum(c.asset_value):null)}
      ${di('Revenue/Brokerage',c.revenue?'₹'+fmtNum(c.revenue):null)}
      ${di('Last Trade Date',fmtDate(c.last_trade_date)+(days!==null?` <small>(${days} days ago)</small>`:''))}
      ${di('Last Trade Month',c.last_trade_month)}
      ${di('Last Calling Date',fmtDate(c.last_call_date))}
      ${di('Next Calling Date',fmtDate(c.next_call))}
      ${di('Follow-up Status',c.followup_status)}
      ${di('Remarks',c.remarks)}
    </div>`;
  } else {
    body+=`<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:18px">
      ${di('MF RM',c.rm)}${di('Email',c.email)}
      ${di('AUM',c.aum?'₹'+fmtNum(c.aum):null)}
      ${di('SIP Amount',c.sip_amount?'₹'+fmtNum(c.sip_amount)+'/mo':null)}
      ${di('SIP Count',c.sip_count)}
      ${di('Last Investment',fmtDate(c.last_invest_date))}
      ${di('Last Calling Date',fmtDate(c.last_call_date))}
      ${di('Next Calling Date',fmtDate(c.next_call))}
      ${di('Follow-up Status',c.followup_status)}
      ${di('Remarks',c.remarks)}
    </div>`;
  }

  body+=`<div class="form-section">📋 Calling Log (${logs.length})</div>`;
  if(logs.length){
    body+=logs.map(l=>`<div class="call-log-item ${l.seg==='equity'?'eq':'mf'}">
      <div class="call-date">${fmtDate(l.date)}${fmtTime(l.ts)?' · '+fmtTime(l.ts):''} — ${l.seg==='equity'?'Equity':'MF'}</div>
      <div class="call-note">${l.note||'—'}</div>
      <div class="call-status"><span class="badge ${l.status==='Done'?'b-done':'b-pending'}">${l.status}</span>
        ${l.next_call?`→ Next: ${fmtDate(l.next_call)}`:''}
      </div></div>`).join('');
  } else body+='<p style="color:var(--gray);font-size:.82rem">No call logs yet.</p>';

  document.getElementById('viewModalBody').innerHTML=body;
  document.getElementById('viewModal').classList.add('open');
}

function di(label,val){
  return `<div class="form-field"><label>${label}</label><div class="dv" style="font-size:.88rem;font-weight:600;color:var(--navy);padding:4px 0">${val||'<span style="color:#ccc;font-style:italic;font-weight:400">—</span>'}</div></div>`;
}

// Show the FULL detail of a single MF Transaction / Demat entry when its
// client name is clicked in the table (the row truncates fund names and hides
// fields like First Payment, Start Date, remarks, who entered it, etc.).
// Available to RM and Admin alike — no role gate.
function viewMfTxnDetail(entryId){
  const e=getMfBizEntries().find(x=>x.id===entryId);
  if(!e) return;
  const sched = mfTxnHasSchedule(e.type);
  const needsTarget = mfTxnTypeNeedsTarget(e.type);
  let incAmt=0; try{ incAmt = INC.mf(e).amt||0; }catch(_){}
  const incStr = INC.fmt(incAmt) + (INC.isApproved(e)?'':' <span style="font-size:.68rem;background:#fee2e2;color:#b91c1c;border-radius:6px;padding:0 5px">pending</span>');
  document.getElementById('viewModalTitle').textContent='Transaction Detail — '+(e.client_name||'');
  let body=`<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:16px">
      ${di('Date', fmtDate(e.date))}
      ${di('Client', e.client_name)}
      ${di('RM', e.rm)}
      ${di('Type', e.type)}
      ${di('Amount', e.amount!=null?'₹'+brkFmt(e.amount):null)}
      ${di('Incentive', incStr)}
      ${di(needsTarget?'Source Scheme (From)':'Fund Name', e.fund_name)}
      ${needsTarget?di('Target Scheme (To)', e.target_scheme):''}
      ${sched?di(e.type==='SWP'?'First Withdrawal':'First Payment', e.first_payment!=null?'₹'+brkFmt(e.first_payment):null):''}
      ${sched?di('Start Date', fmtDate(e.start_date)):''}
      ${di('Status', e.status||'Pending')}
      ${(e.status==='Declined'&&e.decline_reason)?di('Decline Reason', e.decline_reason):''}
      ${di('Entered By', (e.created_by||'')+(e.created_by_role?' ('+e.created_by_role+')':''))}
      ${di('Entered On', fmtDate(e.created))}
    </div>`;
  if(e.cross_remark){
    body+=`<div class="form-section">💬 Cross-Check Remark</div>
      <p style="font-size:.88rem;color:var(--navy);margin:2px 0">${escapeHtml(e.cross_remark)}</p>
      <p style="font-size:.72rem;color:var(--gray)">— ${escapeHtml(e.cross_remark_by||'')}, ${fmtDate(e.cross_remark_at)}</p>`;
  }
  document.getElementById('viewModalBody').innerHTML=body;
  document.getElementById('viewModal').classList.add('open');
}
function viewDematDetail(entryId){
  const e=getEqDematEntries().find(x=>x.id===entryId);
  if(!e) return;
  let incAmt=0; try{ incAmt = INC.demat(e).amt||0; }catch(_){}
  const incStr = INC.fmt(incAmt) + (INC.isApproved(e)?'':' <span style="font-size:.68rem;background:#fee2e2;color:#b91c1c;border-radius:6px;padding:0 5px">pending</span>');
  document.getElementById('viewModalTitle').textContent='Demat Detail — '+(e.client_name||'');
  let body=`<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:16px">
      ${di('Date', fmtDate(e.date))}
      ${di('Client', e.client_name)}
      ${di('Client Code / UCC', e.client_code)}
      ${di('Trading RM', e.rm)}
      ${di('Opening RM', e.opening_rm||e.rm)}
      ${di('Incentive', incStr)}
      ${di('Status', e.status||'Pending')}
      ${(e.status==='Declined'&&e.decline_reason)?di('Decline Reason', e.decline_reason):''}
      ${di('Entered By', e.created_by)}
      ${di('Entered On', fmtDate(e.created))}
    </div>`;
  if(e.remarks){
    body+=`<div class="form-section">📝 Remarks</div>
      <p style="font-size:.88rem;color:var(--navy);margin:2px 0">${escapeHtml(e.remarks)}</p>`;
  }
  document.getElementById('viewModalBody').innerHTML=body;
  document.getElementById('viewModal').classList.add('open');
}

// ══════════════════════════════════════════
// CALL LOG
// ══════════════════════════════════════════
function viewLeadCalls(id){
  const leads=DB.get('leads')||[];
  const c=leads.find(x=>x.id===id);
  if(!c) return;
  const logs=(DB.get('call_logs')||[]).filter(l=>l.seg==='lead'&&l.client_id===id)
    .sort((a,b)=>(b.date||'').localeCompare(a.date||''));
  document.getElementById('viewModalTitle').textContent=c.name+' — Lead Call History';
  let body=`
    <div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:18px;align-items:center">
      ${c.mobile?`<a href="tel:${c.mobile}" class="btn btn-outline" style="padding:5px 14px">📞 ${c.mobile}</a>`:''}
      ${c.mobile?`<a href="https://wa.me/91${c.mobile}" target="_blank" class="btn btn-success" style="padding:5px 14px">💬 WhatsApp</a>`:''}
      ${c.alt_mobile?`<a href="tel:${c.alt_mobile}" class="btn btn-outline" style="padding:5px 14px" title="Alternate number">📞 ${c.alt_mobile} (Alt)</a>`:''}
      ${c.alt_mobile?`<a href="https://wa.me/91${c.alt_mobile}" target="_blank" class="btn btn-success" style="padding:5px 14px" title="Alternate WhatsApp">💬 Alt</a>`:''}
      <button class="btn btn-primary" style="padding:5px 14px" onclick="closeModal('viewModal');editLead('${id}')">✏️ Edit / Log Call</button>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:18px">
      ${di('RM',c.rm)}${di('Last Call',fmtDate(c.last_call))}${di('Next Call',fmtDate(c.next_call))}
    </div>`;
  body+=`<div class="form-section">📋 Calling Log (${logs.length})</div>`;
  if(logs.length){
    body+=logs.map(l=>`<div class="call-log-item mf">
      <div class="call-date">${fmtDate(l.date)}${fmtTime(l.ts)?' · '+fmtTime(l.ts):''} — by ${l.by||l.rm||'—'}</div>
      <div class="call-note">${l.note||'—'}</div>
      <div class="call-status"><span class="badge ${l.status==='Done'?'b-done':'b-pending'}">${l.status||'—'}</span>
        ${l.next_call?`→ Next: ${fmtDate(l.next_call)}`:''}
      </div></div>`).join('');
  } else body+='<p style="color:var(--gray);font-size:.82rem">No call logs yet.</p>';
  document.getElementById('viewModalBody').innerHTML=body;
  document.getElementById('viewModal').classList.add('open');
}

function logCall(id,seg){
  currentCallTarget={id,seg};
  if(seg==='lead'){
    const leads=DB.get('leads')||[];
    const c=leads.find(x=>x.id===id);
    if(!c) return;
    document.getElementById('callModalTitle').textContent='Log Call — '+c.name+' (Lead)';
    document.getElementById('callModalBody').innerHTML=`
      <div class="form-row">
        <div class="form-field"><label>Call Date</label><input id="cl_date" type="date" value="${_clampLC(today())}"${_lcAttr()}></div>
        <div class="form-field"><label>Call Status</label><select id="cl_status">
          <option>Done</option><option>No Answer</option><option>Callback Requested</option><option>Pending</option></select></div>
      </div>
      <div class="form-row full">
        <div class="form-field"><label>Call Notes</label>
          <textarea id="cl_note" placeholder="What was discussed? Any commitment made?" rows="4"></textarea></div>
      </div>
      <div class="form-row">
        <div class="form-field"><label>Next Call Date</label><input id="cl_next" type="date" value="${_clampNCWork(addDays(today(),7))}"${_ncAttr()}></div>
        <div class="form-field"><label>Follow-up Status</label><select id="cl_followup">
          <option ${c.followup_status==='Pending'?'selected':''}>Pending</option>
          <option ${c.followup_status==='Done'?'selected':''}>Done</option>
          <option ${c.followup_status==='Not Required'?'selected':''}>Not Required</option>
          <option ${c.followup_status==='Not Interested'?'selected':''}>Not Interested</option>
          <option ${c.followup_status==='Call Not Received'?'selected':''}>Call Not Received</option>
          <option ${c.followup_status==='Call Not Connected'?'selected':''}>Call Not Connected</option>
          <option ${c.followup_status==='Call After Some Time'?'selected':''}>Call After Some Time</option>
          <option ${c.followup_status==='Interested In SIP'?'selected':''}>Interested In SIP</option>
          <option ${c.followup_status==='Interested In Lumpsum'?'selected':''}>Interested In Lumpsum</option>
          <option ${c.followup_status==='Interested In Other Product'?'selected':''}>Interested In Other Product</option>
          <option ${c.followup_status==='Open Demat Account'?'selected':''}>Open Demat Account</option>
          <option ${c.followup_status==='Come To Office'?'selected':''}>Come To Office</option>
        </select></div>
      </div>`;
    document.getElementById('callModal').classList.add('open');
    return;
  }
  const clients=DB.get(seg==='equity'?'eq_clients':'mf_clients')||[];
  const c=clients.find(x=>x.id===id);
  if(!c) return;
  document.getElementById('callModalTitle').textContent='Log Call — '+c.name;
  document.getElementById('callModalBody').innerHTML=`
    <div class="form-row">
      <div class="form-field"><label>Call Date</label><input id="cl_date" type="date" value="${_clampLC(today())}"${_lcAttr()}></div>
      <div class="form-field"><label>Call Status</label><select id="cl_status">
        <option>Done</option><option>No Answer</option><option>Callback Requested</option><option>Pending</option></select></div>
    </div>
    <div class="form-row full">
      <div class="form-field"><label>Call Notes</label>
        <textarea id="cl_note" placeholder="What was discussed? Any commitment made?" rows="4"></textarea></div>
    </div>
    <div class="form-row">
      <div class="form-field"><label>Next Call Date</label><input id="cl_next" type="date" value="${_clampNCWork(addDays(today(),7))}"${_ncAttr()}></div>
      <div class="form-field"><label>Follow-up Status</label><select id="cl_followup">${seg==='equity'?`
        <option>NOT INTERESTED</option><option>CALL NOT RECEIVED</option><option>CALL LATER</option>
        <option>TRADE</option><option>INTERESTED IN OTHER PRODUCT</option><option>POSITIVE</option><option>WRONG NUMBER</option><option>OPEN DEMAT ACCOUNT</option>`:`
        <option>NOT INTERESTED</option><option>CALL NOT RECEIVED</option><option>CALL LATER</option>
        <option>INTERESTED IN SIP</option><option>INTERESTED IN LUMPSUM</option><option>INTERESTED IN OTHER PRODUCT</option>`}
      </select></div>
    </div>`;
  document.getElementById('callModal').classList.add('open');
}

async function saveCallLog(){
  if(!currentCallTarget) return;
  const {id,seg}=currentCallTarget;
  const date=document.getElementById('cl_date').value;
  const status=document.getElementById('cl_status').value;
  const note=document.getElementById('cl_note').value.trim();
  const nextCall=document.getElementById('cl_next').value;
  const fuStatus=document.getElementById('cl_followup').value;

  if(seg==='lead'){
    const leads=DB.get('leads')||[];
    const lead=leads.find(x=>x.id===id);
    DB.addCallLog({
      id:uid(), client_id:id, seg:'lead', date, status, note,
      next_call:nextCall, created:today(), ts:new Date().toISOString(),
      client_name: lead?lead.name:'',
      rm: lead?lead.rm:'',
      remarks: note,
      by: CU.name
    });
    if(lead){
      lead.last_call=date;
      lead.next_call=nextCall;
      lead.followup_status=fuStatus;
      if(note) lead.remarks=note;
      lead.updated=today();
      await DB.setClient('leads',lead);
    }
    closeModal('callModal');
    toast('Call logged!','success');
    renderLeadsTable();
    updateBadges();
    return;
  }

  const clients2=DB.get(seg==='equity'?'eq_clients':'mf_clients')||[];
  const clientObj=clients2.find(x=>x.id===id);
  DB.addCallLog({
    id:uid(), client_id:id, seg, date, status, note,
    next_call:nextCall, created:today(), ts:new Date().toISOString(),
    client_name: clientObj?clientObj.name:'',
    rm: clientObj?clientObj.rm:'',
    remarks: note,
    by: CU.name
  });

  // Update client record
  const key=seg==='equity'?'eq_clients':'mf_clients';
  const clients=DB.get(key)||[];
  const idx=clients.findIndex(x=>x.id===id);
  if(idx>=0){
    const oldClient = clients[idx];
    const rm = oldClient.rm;
    // Track changes via call log
    const callChanges = [];
    if(nextCall && nextCall!==oldClient.next_call) callChanges.push({field:'next_call', old:oldClient.next_call||'—', new:nextCall});
    if(fuStatus && fuStatus!==oldClient.followup_status) callChanges.push({field:'followup_status', old:oldClient.followup_status||'—', new:fuStatus});
    if(note && note!==oldClient.remarks) callChanges.push({field:'remarks', old:oldClient.remarks||'—', new:note});
    clients[idx].last_call_date=date;
    clients[idx].next_call=nextCall;
    clients[idx].followup_status=fuStatus;
    if(note) clients[idx].remarks=note;
    clients[idx].rm = rm;
    // Optimistic: setClient writes localStorage + sets the write-guard synchronously
    // before its network await, so the screen updates instantly. The full-list
    // Firestore transaction runs in the background (surfaces its own error toast).
    DB.setClient(key, clients[idx]).catch(()=>{});
    // Save field changes to activity_logs too (so changes are visible in Activity Log)
    if(callChanges.length>0){
      DB.addActivityLog({
        id: uid(), type: 'call_update', seg,
        client_id: id, client_name: oldClient.name, rm,
        by: CU.name, date: new Date().toISOString(),
        changes: callChanges
      });
    }
  }
  closeModal('callModal');
  toast('Call logged!','success');
  const pg = getCurrentPageId();

  // Fast path: if we're on a follow-up page, just remove/refresh the single row
  // instead of re-rendering the whole table (which can be 200+ rows).
  if((pg==='eq-followup' || pg==='mf-followup') && idx>=0){
    const group = pg==='eq-followup' ? 'eqf' : 'mff';
    const rowEl = document.querySelector(`tr[data-client-id="${id}"]`);
    const tab = group==='eqf' ? activeEqfTab : activeMffTab;
    // On Today/Overdue tabs — this client is "done", remove the row instantly
    if(rowEl && (tab==='today' || tab==='overdue')){
      rowEl.style.transition='opacity .2s';
      rowEl.style.opacity='0';
      setTimeout(()=>{ rowEl.remove(); }, 200);
    } else {
      // On "All Pending" or "Upcoming" — full re-render needed since data changed
      renderFollowup(group);
    }
    // Update count badge
    const countEl = document.getElementById(group+'-count');
    if(countEl && rowEl && (tab==='today'||tab==='overdue')){
      const n = parseInt(countEl.textContent)||0;
      if(n>1) countEl.textContent=(n-1)+' client'+(n-1===1?'':'s');
      else countEl.textContent='0 clients';
    }
  } else if(pg==='eq-clients') renderEqTable();
  else if(pg==='mf-clients') renderMfTable();
  else if(pg==='eq-followup') renderFollowup('eqf');
  else if(pg==='mf-followup') renderFollowup('mff');

  // Everything else is off-screen — defer past the current paint so it never blocks.
  setTimeout(()=>{
    try{
      if(pg!=='eq-clients' && seg==='equity') renderEqTable();
      if(pg!=='mf-clients' && seg==='mf') renderMfTable();
      refreshDash();
      updateBadges();
    }catch(_){}
  }, 0);
}

// ══════════════════════════════════════════
// MF BUSINESS TRACKING (Lumpsum/SIP/Switch/Resumption/SIP Stop)
// ══════════════════════════════════════════
let currentBusinessTarget = null;
let editingBusinessId = null;

function openBusinessModal(clientId, clientName){
  currentBusinessTarget = {id: clientId, name: clientName};
  editingBusinessId = null;
  document.getElementById('businessModalTitle').textContent = 'Add Business — ' + clientName;
  const clientWrap=document.getElementById('biz_client_wrap'); if(clientWrap) clientWrap.style.display='none';
  const rmNoteWrap=document.getElementById('biz_rm_note_wrap'); if(rmNoteWrap) rmNoteWrap.style.display='none';
  const mfNoteWrap=document.getElementById('biz_mfdesk_note_wrap'); if(mfNoteWrap) mfNoteWrap.style.display='none';
  ['biz_type','biz_amount','biz_target_fund'].forEach(fid=>{
    const el=document.getElementById(fid);
    if(!el) return;
    el.disabled=false; el.style.background=''; el.style.cursor='';
  });
  document.getElementById('biz_type').value = 'Lumpsum';
  document.getElementById('biz_amount').value = '';
  const bfEl=document.getElementById('biz_fund'); if(bfEl) bfEl.value='';
  const btEl=document.getElementById('biz_target_fund'); if(btEl) btEl.value='';
  const fpEl=document.getElementById('biz_firstpay'); if(fpEl) fpEl.checked=false;
  const sdEl=document.getElementById('biz_startdate'); if(sdEl) sdEl.value='';
  toggleBizTarget();
  document.getElementById('biz_date').value = today();
  document.getElementById('businessModal').classList.add('open');
}

async function saveBusinessEntry(){
  if(!currentBusinessTarget) return;
  const type = document.getElementById('biz_type').value;
  const amount = parseFloat(document.getElementById('biz_amount').value);
  const fundName = (document.getElementById('biz_fund')?.value||'').trim();
  const targetScheme = (document.getElementById('biz_target_fund')?.value||'').trim();
  const date = document.getElementById('biz_date').value;

  if(!amount || amount<=0){ toast('Please enter the amount','error'); return; }
  if(!fundName){ toast('Please enter the fund name','error'); return; }
  if(mfTxnTypeNeedsTarget(type) && !targetScheme){ toast('Please enter the target scheme','error'); return; }
  if(!date){ toast('Please enter the date','error'); return; }

  // SIP / STP / SWP: First Payment is a tick (= amount when done) + Start Date (required)
  const sched = mfTxnHasSchedule(type);
  const startDate = (document.getElementById('biz_startdate')?.value||'').trim();
  const firstPayDone = !!document.getElementById('biz_firstpay')?.checked;
  const firstPay = (sched && firstPayDone) ? amount : null;
  if(sched && !startDate){ toast('Please enter the start date','error'); return; }

  if(editingBusinessId){
    const original = getMfBizEntries().find(e=>e.id===editingBusinessId);
    if(!original){ toast('Entry no longer exists','error'); editingBusinessId=null; closeModal('businessModal'); return; }
    if(CU.role!=='admin' && original.created_by!==CU.name && !hasMfDeskAccess(CU)){ toast('You can only edit entries you created','error'); return; }
    if(CU.role==='admin' && !currentBusinessTarget.id){ toast('Please select a client','error'); return; }

    const finalSched = sched;
    const finalFirstPay = firstPay;

    const r = await DB.updateMfBizEntry('entries', editingBusinessId, fresh=>{
      // Client + RM reassignment: Admin only (the RM dropdown is hidden for
      // everyone else, but this is the save-side guard that actually
      // enforces it — a hidden field alone is only a UI hint).
      if(CU.role==='admin'){
        fresh.client_id = currentBusinessTarget.id;
        fresh.client_name = currentBusinessTarget.name;
        fresh.rm = document.getElementById('biz_client_rm')?.value || '';
      }
      fresh.type = type;
      fresh.amount = amount;
      fresh.fund_name = fundName;
      fresh.target_scheme = mfTxnTypeNeedsTarget(type) ? targetScheme : '';
      fresh.first_payment = finalSched ? finalFirstPay : null;
      fresh.start_date = finalSched ? startDate : '';
      fresh.date = date;
    });
    if(!r.ok || r.aborted){ if(r.aborted) toast('Entry no longer exists','error'); return; }
    learnFundName(fundName);
    if(mfTxnTypeNeedsTarget(type)) learnFundName(targetScheme);
    closeModal('businessModal');
    toast('Business entry updated!','success');
    editingBusinessId = null;
    if(document.getElementById('reportModal').classList.contains('open')) newBusinessMonthlyReport();
    renderMfTxnTable();
    return;
  }

  const mf = DB.get('mf_clients')||[];
  const client = mf.find(c=>c.id===currentBusinessTarget.id);
  const rm = client ? client.rm : (CU.role!=='admin'?CU.name:'');

  const newEntry = {
    id: uid(),
    client_id: currentBusinessTarget.id,
    client_name: currentBusinessTarget.name,
    rm,
    type, amount, fund_name: fundName,
    target_scheme: mfTxnTypeNeedsTarget(type) ? targetScheme : '',
    first_payment: sched ? firstPay : null,
    start_date: sched ? startDate : '',
    date,
    created_by: CU.name,
    created_by_role: CU.role,
    created: today(),
    // RM & MF Desk entries always start as Pending (admin reviews/updates).
    // Admin's own entries are auto-Approved (admin is the approving authority).
    status: CU.role==='admin' ? 'Approved' : 'Pending',
    decline_reason: '',
    cross_remark: '', cross_remark_by: '', cross_remark_at: ''
  };
  DB.appendMfBizEntry('entries', newEntry).then(()=>{ renderMfTxnTable(); });
  learnFundName(fundName);
  if(mfTxnTypeNeedsTarget(type)) learnFundName(targetScheme);
  closeModal('businessModal');
  toast('Business entry saved!','success');
}

// ── Approve / Decline (Admin only) ──
function bizStatusBadge(status){
  const s = status || 'Pending';
  const map = {
    Approved: {bg:'#1D9E7522', col:'#1D9E75', label:'✅ Aprv'},
    Declined: {bg:'#C0392B22', col:'#C0392B', label:'❌ Declined'},
    Pending:  {bg:'#D3940022', col:'#B7950B', label:'⏳ Pend'}
  };
  const m = map[s] || map.Pending;
  return `<span class="badge" style="background:${m.bg};color:${m.col};font-weight:600">${m.label}</span>`;
}

// A user has MF Desk capability either as their sole/primary role ('mf_desk'
// — back-office, no client base of their own), or as an RM who's ALSO been
// granted MF Desk access (mf_desk_access flag) so they can help backfill
// transactions across all RMs while still being a normal RM otherwise.
function hasMfDeskAccess(user){
  if(!user) return false;
  return user.role==='mf_desk' || (user.role==='rm' && user.mf_desk_access===true);
}

// ── Cross-check remarks ──
// A light two-way check between the RM and MF Desk on the same entry, without
// either side needing approve/decline authority (that's Admin-only). Based on
// whether the entry was self-logged by its own RM or logged by someone else
// on that RM's behalf — this works correctly whether the "someone else" is a
// pure MF Desk user or an RM who also has MF Desk access:
//  - Self-logged (rm === created_by) → any other MF-Desk-capable user can
//    leave a remark (e.g. flag that it looks like a duplicate).
//  - Logged by someone else on this RM's behalf → the actual owning RM can
//    leave a remark (e.g. confirm/dispute it).
// Admin can always remark on anything.
function canAddCrossRemark(e){
  if(!CU) return false;
  if(CU.role==='admin') return true;
  const selfEntered = (e.rm||'').trim().toLowerCase() === (e.created_by||'').trim().toLowerCase();
  if(selfEntered){
    return hasMfDeskAccess(CU) && CU.name!==e.created_by;
  }
  return (e.rm||'').trim().toLowerCase() === (CU.name||'').trim().toLowerCase();
}

function addCrossRemark(id){
  const entries = getMfBizEntries();
  const e = entries.find(x=>x.id===id);
  if(!e) return;
  if(!canAddCrossRemark(e)){ toast('You are not allowed to remark on this entry','error'); return; }
  const remark = prompt('Your remark on this entry (e.g. checking if this business belongs to someone else):', e.cross_remark||'');
  if(remark===null) return;
  e.cross_remark = remark.trim();
  e.cross_remark_by = CU.name;
  e.cross_remark_at = today();
  setMfBizEntries(entries);
  toast('Remark saved','success');
  renderMfTxnTable();
  if(document.getElementById('reportModal')?.classList.contains('open')) newBusinessMonthlyReport();
}

// Admin-only: clear/reverse a cross-check remark in one click, without having
// to open the prompt and manually delete the text.
function clearCrossRemark(id){
  if(CU.role!=='admin') return;
  if(!confirm('Clear this cross-check remark?')) return;
  const entries = getMfBizEntries();
  const e = entries.find(x=>x.id===id);
  if(!e) return;
  e.cross_remark = '';
  e.cross_remark_by = '';
  e.cross_remark_at = '';
  setMfBizEntries(entries);
  toast('Remark cleared','success');
  renderMfTxnTable();
  if(document.getElementById('reportModal')?.classList.contains('open')) newBusinessMonthlyReport();
}

function approveBusinessEntry(id){
  if(CU.role!=='admin') return;
  const entries = getMfBizEntries();
  const e = entries.find(x=>x.id===id);
  if(!e) return;
  e.status = 'Approved';
  e.decline_reason = '';
  setMfBizEntries(entries);
  toast('Business approved!','success');
  if(document.getElementById('reportModal')?.classList.contains('open')) newBusinessMonthlyReport();
  renderMfTxnTable();
}

function declineBusinessEntry(id){
  if(CU.role!=='admin') return;
  const reason = prompt('Decline reason (optional):','') || '';
  const entries = getMfBizEntries();
  const e = entries.find(x=>x.id===id);
  if(!e) return;
  e.status = 'Declined';
  e.decline_reason = reason.trim();
  setMfBizEntries(entries);
  toast('Business declined','success');
  if(document.getElementById('reportModal')?.classList.contains('open')) newBusinessMonthlyReport();
  renderMfTxnTable();
}

function markPendingBusinessEntry(id){
  if(CU.role!=='admin') return;
  const entries = getMfBizEntries();
  const e = entries.find(x=>x.id===id);
  if(!e) return;
  e.status = 'Pending';
  e.decline_reason = '';
  setMfBizEntries(entries);
  toast('Status reset to Pending','success');
  if(document.getElementById('reportModal')?.classList.contains('open')) newBusinessMonthlyReport();
  renderMfTxnTable();
}

// ══════════════════════════════════════════
// MF TRANSACTIONS (dedicated entry page — Redemption/Switch/SWP/SIP/Lumpsum)
// All RM-wise, all client-wise. Uses the same 'mf_business' store as the
// per-client "Add Business" button, so both feed the same New Business report.
// ══════════════════════════════════════════
let mfTxnSelectedClient = null;

function getMfBizEntries(){
  const biz = DB.get('mf_business');
  return Array.isArray(biz) ? biz : (biz?.entries || []);
}
function setMfBizEntries(entries){
  const biz = DB.get('mf_business');
  const eqEntries = Array.isArray(biz) ? [] : (biz?.eq_entries||[]);
  DB.set('mf_business', {entries, eq_entries: eqEntries});
}

// Switch & STP move money OUT of one scheme and INTO another, so they need a
// second "Target Scheme" field. Everything else is a single-scheme txn.
function mfTxnTypeNeedsTarget(t){ return t==='Switch' || t==='STP'; }
// New SIP / STP / SWP are recurring — they carry a First Payment (first
// installment/withdrawal) and a Start Date in addition to the txn date.
function mfTxnHasSchedule(t){ return t==='SIP' || t==='STP' || t==='SWP'; }
// SWP label reads "First Withdrawal"; SIP/STP read "First Payment".
function mfFirstPayLabel(t){ return t==='SWP' ? 'First Withdrawal' : 'First Payment'; }
// Checkbox hint text depends on type.
function mfFirstPayHint(t){ return t==='SWP' ? 'Withdrawn (= amount)' : 'Paid (= amount)'; }
// When the First Payment/Withdrawal tick is checked, swap the generic
// "Paid (= amount)" placeholder text for the actual entered amount in
// small green text — a quick visual confirmation of what's being marked
// paid, instead of the same static hint whether ticked or not.
function updateFirstPayHint(prefix){
  const hint=document.getElementById(prefix+'-firstpay-hint') || document.getElementById(prefix+'_firstpay_hint');
  if(!hint) return;
  const checked = (document.getElementById(prefix+'-firstpay') || document.getElementById(prefix+'_firstpay'))?.checked;
  const type = (document.getElementById(prefix+'-type') || document.getElementById(prefix+'_type'))?.value || '';
  const amtEl = document.getElementById(prefix+'-amount') || document.getElementById(prefix+'_amount');
  const amt = parseFloat(amtEl?.value);
  if(checked && amt>0){
    const verb = type==='SWP' ? 'Withdrawn' : 'Paid';
    hint.innerHTML = `<b style="color:var(--green,#16a34a)">✓ ₹${brkFmt(amt)} ${verb}</b>`;
  } else {
    hint.textContent = mfFirstPayHint(type);
  }
}

// Show/hide the Target Scheme field on the MF Transactions form based on Type,
// and relabel the Fund Name field as "Source Scheme (From)" for Switch/STP so
// it's clear which scheme is which.
function toggleMfTxnTarget(){
  const type = document.getElementById('mftxn-type')?.value || '';
  const needs = mfTxnTypeNeedsTarget(type);
  const wrap = document.getElementById('mftxn-target-wrap');
  if(wrap) wrap.style.display = needs ? '' : 'none';
  const fundInput = document.getElementById('mftxn-fund');
  const fundLabel = fundInput ? fundInput.previousElementSibling : null;
  if(fundLabel) fundLabel.textContent = needs ? 'Source Scheme (From) *' : 'Fund Name *';
  if(!needs){
    const t=document.getElementById('mftxn-target-fund'); if(t) t.value='';
    const tr=document.getElementById('mftxn-target-fund-results'); if(tr){ tr.style.display='none'; tr.innerHTML=''; }
  }
  // First Payment + Start Date show only for SIP / STP / SWP
  const sched = mfTxnHasSchedule(type);
  const fpWrap=document.getElementById('mftxn-firstpay-wrap'); if(fpWrap) fpWrap.style.display = sched ? '' : 'none';
  const sdWrap=document.getElementById('mftxn-startdate-wrap'); if(sdWrap) sdWrap.style.display = sched ? '' : 'none';
  const fpLbl=document.getElementById('mftxn-firstpay-label'); if(fpLbl) fpLbl.textContent = mfFirstPayLabel(type);
  const fpHint=document.getElementById('mftxn-firstpay-hint'); if(fpHint) updateFirstPayHint('mftxn');
  if(!sched){
    const fp=document.getElementById('mftxn-firstpay'); if(fp) fp.checked=false;
    const sd=document.getElementById('mftxn-startdate'); if(sd) sd.value='';
  }
}

// Same Target-Scheme show/hide logic for the Add/Edit Business modal.
function toggleBizTarget(){
  const type = document.getElementById('biz_type')?.value || '';
  const needs = mfTxnTypeNeedsTarget(type);
  const wrap = document.getElementById('biz_target_wrap');
  if(wrap) wrap.style.display = needs ? '' : 'none';
  const fundInput = document.getElementById('biz_fund');
  const fundLabel = fundInput ? fundInput.previousElementSibling : null;
  if(fundLabel) fundLabel.textContent = needs ? 'Source Scheme (From) *' : 'Fund Name *';
  if(!needs){
    const t=document.getElementById('biz_target_fund'); if(t) t.value='';
    const tr=document.getElementById('biz-target-fund-results'); if(tr){ tr.style.display='none'; tr.innerHTML=''; }
  }
  // First Payment + Start Date row shows only for SIP / STP / SWP
  const sched = mfTxnHasSchedule(type);
  const schedWrap=document.getElementById('biz_schedule_wrap'); if(schedWrap) schedWrap.style.display = sched ? '' : 'none';
  const fpLbl=document.getElementById('biz_firstpay_label'); if(fpLbl) fpLbl.textContent = mfFirstPayLabel(type);
  const fpHint=document.getElementById('biz_firstpay_hint'); if(fpHint) updateFirstPayHint('biz');
  if(!sched){
    const fp=document.getElementById('biz_firstpay'); if(fp) fp.checked=false;
    const sd=document.getElementById('biz_startdate'); if(sd) sd.value='';
  }
}

// Jumps straight to the MF Transactions page with the Type pre-selected and
// focus on client search — used by the Dashboard "Don't Forget" reminder card
// so logging a Redemption / SIP Stop takes one click instead of three.
function quickMfTxn(type){
  showPage('mf-txns');
  setTimeout(()=>{
    const sel=document.getElementById('mftxn-type');
    if(sel) sel.value=type;
    const search=document.getElementById('mftxn-client-search');
    if(search) search.focus();
  }, 50);
}

function renderMfTxnPage(){
  mfTxnSelectedClient = null;
  if(typeof MFTBULK!=='undefined'){ MFTBULK.sel.clear(); MFTBULK.hideBar(); }
  const sel=document.getElementById('mftxn-client-selected'); if(sel) sel.value='';
  const rmEl=document.getElementById('mftxn-client-rm'); if(rmEl) rmEl.value='';
  const srch=document.getElementById('mftxn-client-search'); if(srch) srch.value='';
  const res=document.getElementById('mftxn-client-results'); if(res){ res.style.display='none'; res.innerHTML=''; }
  const fundEl=document.getElementById('mftxn-fund'); if(fundEl) fundEl.value='';
  const tgtEl=document.getElementById('mftxn-target-fund'); if(tgtEl) tgtEl.value='';
  toggleMfTxnTarget();
  const dateEl=document.getElementById('mftxn-date'); if(dateEl && !dateEl.value) dateEl.value=today();

  // Populate RM filter dropdown (admin / MF Desk sees all MF RMs, RM sees just themself)
  const rmFilter=document.getElementById('mftxn-rm-filter');
  if(rmFilter){
    const isFullAccess = CU.role==='admin' || hasMfDeskAccess(CU);
    const mfRMs = isFullAccess ? getSegRMs('mf') : [CU.name];
    rmFilter.innerHTML='<option value="">All RMs</option>'+mfRMs.map(r=>`<option>${r}</option>`).join('');
    if(!isFullAccess){ rmFilter.value=CU.name; rmFilter.disabled=true; }
  }

  populateMfTxnMonths();
  renderMfTxnTable();

  // Admin can attribute a new transaction to ANY RM — show a dropdown instead of
  // the read-only RM box. RM users keep the read-only box (their own name).
  const rmSel=document.getElementById('mftxn-rm-select');
  const rmRO=document.getElementById('mftxn-client-rm');
  if(rmSel){
    if(CU.role==='admin'){
      rmSel.innerHTML = getSegRMs('mf').map(r=>`<option>${escapeHtml(r)}</option>`).join('');
      rmSel.style.display=''; if(rmRO) rmRO.style.display='none';
    } else {
      rmSel.style.display='none'; if(rmRO) rmRO.style.display='';
    }
  }
}

// Month dropdown me wahi mahine bhare jo data me actually hain (RM ke apne, Admin ke saare).
function populateMfTxnMonths(){
  const sel=document.getElementById('mftxn-month-filter');
  if(!sel) return;
  let entries=getMfBizEntries();
  if(CU.role!=='admin' && !hasMfDeskAccess(CU)){
    entries=entries.filter(e=>(e.rm||'').trim().toLowerCase()===(CU.name||'').trim().toLowerCase());
  }
  const months=[...new Set(entries.map(e=>(e.date||'').slice(0,7)).filter(Boolean))].sort().reverse();
  const cur=sel.value;
  const MNF=['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const label=m=>{const [y,mo]=m.split('-');return MNF[+mo]+' '+y;};
  sel.innerHTML='<option value="">All Months</option>'+months.map(m=>`<option value="${m}">${label(m)}</option>`).join('');
  if(months.includes(cur)){
    sel.value=cur;
  } else if(!sel.dataset.autoDefaulted){
    // Very first time this page loads this session (nothing picked yet, and
    // the user hasn't pressed Clear either — that also lands on "" but is a
    // deliberate choice we shouldn't override). Default to the current
    // month instead of "All Months" so the page opens already scoped to
    // what's relevant today; once the user changes it (or clears it), that
    // choice is respected on every later visit to this page this session.
    const curMonth = today().slice(0,7);
    if(months.includes(curMonth)) sel.value = curMonth;
  }
  sel.dataset.autoDefaulted = '1';
}

// Month chunne pe From/To date clear kar do taaki conflict na ho.
function clearMfTxnFilters(){
  const s=document.getElementById('mftxn-search'); if(s) s.value='';
  const rm=document.getElementById('mftxn-rm-filter'); if(rm) rm.value='';
  const st=document.getElementById('mftxn-status-filter'); if(st) st.value='';
  const tp=document.getElementById('mftxn-type-filter'); if(tp) tp.value='';
  const mo=document.getElementById('mftxn-month-filter'); if(mo) mo.value='';
  const fd=document.getElementById('mftxn-from-date'); if(fd) fd.value='';
  const td=document.getElementById('mftxn-to-date'); if(td) td.value='';
  renderMfTxnTable();
}
function onMfTxnMonthChange(){
  const m=document.getElementById('mftxn-month-filter')?.value||'';
  if(m){
    const fromEl=document.getElementById('mftxn-from-date'); if(fromEl) fromEl.value='';
    const toEl=document.getElementById('mftxn-to-date'); if(toEl) toEl.value='';
  }
  renderMfTxnTable();
}

// ── Fund Name autocomplete (built-in list, no internet dependency) ──
// An earlier version of this pulled live suggestions from a third-party API,
// but that external site proved unreliable (slow/unreachable for some users,
// stuck on "Searching…"). Switched to a bundled list of common Regular Plan
// schemes across the major AMCs instead — it's instant, works with zero
// internet dependency, and never gets stuck. It only covers the commonly
// transacted schemes (not all ~14,000 in India), so the field stays a normal
// text input underneath — if a fund isn't in the list, just type it manually.
const FUND_NAME_LIST = [
  // HDFC
  "HDFC Flexi Cap Fund - Regular Plan - Growth","HDFC Top 100 Fund - Regular Plan - Growth",
  "HDFC Mid-Cap Opportunities Fund - Regular Plan - Growth","HDFC Small Cap Fund - Regular Plan - Growth",
  "HDFC Balanced Advantage Fund - Regular Plan - Growth","HDFC Hybrid Equity Fund - Regular Plan - Growth",
  "HDFC Tax Saver Fund - Regular Plan - Growth","HDFC Index Fund - Sensex Plan - Regular Plan - Growth",
  "HDFC Index Fund - Nifty 50 Plan - Regular Plan - Growth","HDFC Liquid Fund - Regular Plan - Growth",
  "HDFC Money Market Fund - Regular Plan - Growth","HDFC Capital Builder Value Fund - Regular Plan - Growth",
  "HDFC Focused 30 Fund - Regular Plan - Growth","HDFC Multi Cap Fund - Regular Plan - Growth",
  // SBI
  "SBI Bluechip Fund - Regular Plan - Growth","SBI Small Cap Fund - Regular Plan - Growth",
  "SBI Magnum Midcap Fund - Regular Plan - Growth","SBI Contra Fund - Regular Plan - Growth",
  "SBI Equity Hybrid Fund - Regular Plan - Growth","SBI Long Term Equity Fund - Regular Plan - Growth",
  "SBI Focused Equity Fund - Regular Plan - Growth","SBI Magnum Gilt Fund - Regular Plan - Growth",
  "SBI Liquid Fund - Regular Plan - Growth","SBI Healthcare Opportunities Fund - Regular Plan - Growth",
  "SBI Technology Opportunities Fund - Regular Plan - Growth","SBI Banking & Financial Services Fund - Regular Plan - Growth",
  "SBI Nifty Index Fund - Regular Plan - Growth","SBI Multicap Fund - Regular Plan - Growth",
  // ICICI Prudential
  "ICICI Prudential Bluechip Fund - Regular Plan - Growth","ICICI Prudential Flexicap Fund - Regular Plan - Growth",
  "ICICI Prudential Value Discovery Fund - Regular Plan - Growth","ICICI Prudential Equity & Debt Fund - Regular Plan - Growth",
  "ICICI Prudential Balanced Advantage Fund - Regular Plan - Growth","ICICI Prudential Technology Fund - Regular Plan - Growth",
  "ICICI Prudential Banking and Financial Services Fund - Regular Plan - Growth","ICICI Prudential Nifty 50 Index Fund - Regular Plan - Growth",
  "ICICI Prudential Liquid Fund - Regular Plan - Growth","ICICI Prudential Multicap Fund - Regular Plan - Growth",
  "ICICI Prudential Midcap Fund - Regular Plan - Growth","ICICI Prudential Dividend Yield Equity Fund - Regular Plan - Growth",
  "ICICI Prudential All Seasons Bond Fund - Regular Plan - Growth",
  // Axis
  "Axis Bluechip Fund - Regular Plan - Growth","Axis Small Cap Fund - Regular Plan - Growth",
  "Axis Midcap Fund - Regular Plan - Growth","Axis Long Term Equity Fund - Regular Plan - Growth",
  "Axis Focused 25 Fund - Regular Plan - Growth","Axis Flexi Cap Fund - Regular Plan - Growth",
  "Axis Liquid Fund - Regular Plan - Growth","Axis Balanced Advantage Fund - Regular Plan - Growth",
  // Nippon India
  "Nippon India Large Cap Fund - Regular Plan - Growth","Nippon India Small Cap Fund - Regular Plan - Growth",
  "Nippon India Growth Fund - Regular Plan - Growth","Nippon India Multi Cap Fund - Regular Plan - Growth",
  "Nippon India Pharma Fund - Regular Plan - Growth","Nippon India Liquid Fund - Regular Plan - Growth",
  "Nippon India Focused Equity Fund - Regular Plan - Growth","Nippon India Index Fund - Nifty 50 Plan - Regular Plan - Growth",
  // Kotak
  "Kotak Flexicap Fund - Regular Plan - Growth","Kotak Emerging Equity Fund - Regular Plan - Growth",
  "Kotak Bluechip Fund - Regular Plan - Growth","Kotak Small Cap Fund - Regular Plan - Growth",
  "Kotak Balanced Advantage Fund - Regular Plan - Growth","Kotak Equity Hybrid Fund - Regular Plan - Growth",
  "Kotak Tax Saver Fund - Regular Plan - Growth","Kotak Liquid Fund - Regular Plan - Growth",
  // Mirae Asset
  "Mirae Asset Large Cap Fund - Regular Plan - Growth","Mirae Asset Large & Midcap Fund - Regular Plan - Growth",
  "Mirae Asset Emerging Bluechip Fund - Regular Plan - Growth","Mirae Asset Tax Saver Fund - Regular Plan - Growth",
  "Mirae Asset Midcap Fund - Regular Plan - Growth",
  // Parag Parikh
  "Parag Parikh Flexi Cap Fund - Regular Plan - Growth","Parag Parikh Tax Saver Fund - Regular Plan - Growth",
  "Parag Parikh Conservative Hybrid Fund - Regular Plan - Growth",
  // Aditya Birla Sun Life
  "Aditya Birla Sun Life Frontline Equity Fund - Regular Plan - Growth","Aditya Birla Sun Life Flexi Cap Fund - Regular Plan - Growth",
  "Aditya Birla Sun Life Tax Relief 96 - Regular Plan - Growth","Aditya Birla Sun Life Equity Hybrid 95 Fund - Regular Plan - Growth",
  "Aditya Birla Sun Life Mid Cap Fund - Regular Plan - Growth","Aditya Birla Sun Life Liquid Fund - Regular Plan - Growth",
  // UTI
  "UTI Flexi Cap Fund - Regular Plan - Growth","UTI Nifty 50 Index Fund - Regular Plan - Growth",
  "UTI Value Opportunities Fund - Regular Plan - Growth","UTI Mid Cap Fund - Regular Plan - Growth",
  "UTI Dividend Yield Fund - Regular Plan - Growth","UTI Liquid Cash Plan - Regular Plan - Growth",
  // DSP
  "DSP Flexi Cap Fund - Regular Plan - Growth","DSP Midcap Fund - Regular Plan - Growth",
  "DSP Small Cap Fund - Regular Plan - Growth","DSP Tax Saver Fund - Regular Plan - Growth",
  "DSP Equity & Bond Fund - Regular Plan - Growth","DSP Top 100 Equity Fund - Regular Plan - Growth",
  // Quant
  "Quant Active Fund - Regular Plan - Growth","Quant Small Cap Fund - Regular Plan - Growth",
  "Quant Mid Cap Fund - Regular Plan - Growth","Quant Flexi Cap Fund - Regular Plan - Growth",
  "Quant ELSS Tax Saver Fund - Regular Plan - Growth","Quant Infrastructure Fund - Regular Plan - Growth",
  // Tata
  "Tata Flexi Cap Fund - Regular Plan - Growth","Tata Large Cap Fund - Regular Plan - Growth",
  "Tata Mid Cap Growth Fund - Regular Plan - Growth","Tata Small Cap Fund - Regular Plan - Growth",
  "Tata Digital India Fund - Regular Plan - Growth",
  // Canara Robeco
  "Canara Robeco Bluechip Equity Fund - Regular Plan - Growth","Canara Robeco Flexi Cap Fund - Regular Plan - Growth",
  "Canara Robeco Emerging Equities Fund - Regular Plan - Growth","Canara Robeco Equity Tax Saver Fund - Regular Plan - Growth",
  "Canara Robeco Equity Hybrid Fund - Regular Plan - Growth",
  // Franklin Templeton
  "Franklin India Flexi Cap Fund - Regular Plan - Growth","Franklin India Prima Fund - Regular Plan - Growth",
  "Franklin India Equity Income Fund - Regular Plan - Growth","Templeton India Value Fund - Regular Plan - Growth",
  // Motilal Oswal
  "Motilal Oswal Midcap Fund - Regular Plan - Growth","Motilal Oswal Flexi Cap Fund - Regular Plan - Growth",
  "Motilal Oswal Nifty 500 Fund - Regular Plan - Growth",
  // Edelweiss
  "Edelweiss Balanced Advantage Fund - Regular Plan - Growth","Edelweiss Flexi Cap Fund - Regular Plan - Growth",
  "Edelweiss Mid Cap Fund - Regular Plan - Growth",
  // PGIM
  "PGIM India Flexi Cap Fund - Regular Plan - Growth","PGIM India Midcap Opportunities Fund - Regular Plan - Growth",
  // Invesco
  "Invesco India Contra Fund - Regular Plan - Growth","Invesco India Midcap Fund - Regular Plan - Growth",
  "Invesco India Large Cap Fund - Regular Plan - Growth",
  // Bandhan (formerly IDFC)
  "Bandhan Flexi Cap Fund - Regular Plan - Growth","Bandhan Large Cap Fund - Regular Plan - Growth",
  "Bandhan Sterling Value Fund - Regular Plan - Growth",
  // Mahindra Manulife
  "Mahindra Manulife Multi Cap Fund - Regular Plan - Growth","Mahindra Manulife Mid Cap Fund - Regular Plan - Growth",
  // JM Financial
  "JM Flexicap Fund - Regular Plan - Growth","JM Value Fund - Regular Plan - Growth",
  // WhiteOak Capital
  "WhiteOak Capital Flexi Cap Fund - Regular Plan - Growth","WhiteOak Capital Mid Cap Fund - Regular Plan - Growth",
  // Sundaram
  "Sundaram Large and Mid Cap Fund - Regular Plan - Growth","Sundaram Mid Cap Fund - Regular Plan - Growth",
  // Baroda BNP Paribas
  "Baroda BNP Paribas Large Cap Fund - Regular Plan - Growth","Baroda BNP Paribas Flexi Cap Fund - Regular Plan - Growth",
  // 360 ONE / IIFL
  "360 ONE Flexicap Fund - Regular Plan - Growth",
  // LIC
  "LIC MF Large & Mid Cap Fund - Regular Plan - Growth","LIC MF Flexi Cap Fund - Regular Plan - Growth",
  // Liquid/Debt generic (other AMCs commonly used for parking funds)
  "Axis Overnight Fund - Regular Plan - Growth","ICICI Prudential Overnight Fund - Regular Plan - Growth",
  "HDFC Overnight Fund - Regular Plan - Growth","SBI Overnight Fund - Regular Plan - Growth",
];

// Fund names typed manually (because they weren't in the built-in list) get
// remembered here so the NEXT person who needs the same fund finds it as a
// suggestion instead of having to type it out again. Synced via Firestore
// like the other small collections, so it's shared across all RMs live.
function getLearnedFundNames(){
  return DB.get('learned_fund_names') || [];
}

async function learnFundName(name){
  if(!name) return;
  const trimmed = name.trim();
  if(!trimmed) return;
  const existing = getLearnedFundNames();
  const knownLower = new Set([...FUND_NAME_LIST, ...existing].map(n=>n.toLowerCase()));
  if(knownLower.has(trimmed.toLowerCase())) return; // already known — nothing new to learn
  await DB.set('learned_fund_names', [...existing, trimmed]);
}

function searchFundName(inputId, resultsId){
  const input=document.getElementById(inputId);
  const out=document.getElementById(resultsId);
  if(!input||!out) return;
  const q=input.value.trim().toLowerCase();
  if(q.length<2){ out.style.display='none'; out.innerHTML=''; return; }

  // Built-in list + anything learned from earlier manual entries, deduped
  const combined = [...FUND_NAME_LIST, ...getLearnedFundNames()];
  const seen=new Set();
  const allFunds=[];
  for(const n of combined){
    const key=n.toLowerCase();
    if(seen.has(key)) continue;
    seen.add(key);
    allFunds.push(n);
  }

  const matches=allFunds.filter(n=>n.toLowerCase().includes(q)).slice(0,15);

  if(matches.length===0){
    out.innerHTML='<div style="padding:10px;color:var(--gray);font-size:.85rem">No match found — type it and it\'ll be remembered for next time</div>';
    out.style.display='block';
    return;
  }
  out.innerHTML=matches.map(name=>`
    <div style="padding:8px 12px;cursor:pointer;border-bottom:1px solid #f0f0f0;font-size:.85rem" onmouseover="this.style.background='#f7f7f7'" onmouseout="this.style.background='#fff'" onclick="selectFundName('${inputId}','${resultsId}', this.dataset.name)" data-name="${escapeHtml(name)}">
      ${escapeHtml(name)}
    </div>`).join('');
  out.style.display='block';
}

function selectFundName(inputId, resultsId, name){
  const input=document.getElementById(inputId);
  if(input) input.value=name;
  const out=document.getElementById(resultsId);
  if(out){ out.style.display='none'; out.innerHTML=''; }
}

// Close fund-search dropdowns when clicking elsewhere
document.addEventListener('click', e=>{
  [['mftxn-fund','mftxn-fund-results'], ['mftxn-target-fund','mftxn-target-fund-results'], ['biz_fund','biz-fund-results'], ['biz_target_fund','biz-target-fund-results']].forEach(([inputId,resultsId])=>{
    const wrap=document.getElementById(inputId);
    const out=document.getElementById(resultsId);
    if(wrap && out && !wrap.contains(e.target) && !out.contains(e.target)) out.style.display='none';
  });
});

// Admin-only client reassignment inside the Edit Business modal — lets Admin
// fix a transaction that got attributed to the wrong client (and therefore
// the wrong RM) without having to delete and re-enter it.
function searchBizClient(){
  const q=(document.getElementById('biz_client_search').value||'').trim().toLowerCase();
  const out=document.getElementById('biz_client_results');
  if(!out) return;
  if(q.length<2){ out.style.display='none'; out.innerHTML=''; return; }
  const clients=getMfTxnSearchClients();
  const matches=clients.filter(c=>
    (c.name||'').toLowerCase().includes(q) ||
    (c.pan||'').toLowerCase().includes(q) ||
    (c.mobile||'').toLowerCase().includes(q)
  ).slice(0,30);
  if(matches.length===0){
    out.innerHTML='<div style="padding:10px;color:var(--gray);font-size:.85rem">No client found</div>';
    out.style.display='block';
    return;
  }
  out.innerHTML=matches.map(c=>`
    <div style="padding:8px 12px;cursor:pointer;border-bottom:1px solid #f0f0f0" onmouseover="this.style.background='#f7f7f7'" onmouseout="this.style.background='#fff'" onclick="selectBizClient('${c.id}')">
      <div style="font-weight:600;font-size:.88rem">${escapeHtml(c.name||'')}</div>
      <div style="font-size:.75rem;color:var(--gray)">${escapeHtml(c.pan||'—')} · ${escapeHtml(c.mobile||'—')} · RM: ${escapeHtml(c.rm||'—')}</div>
    </div>`).join('');
  out.style.display='block';
}

// Populates the RM dropdown in the Edit Business modal — this RM choice only
// applies to the one transaction entry being edited; it never touches the
// client's actual RM assignment in their profile. Defaults to the given RM
// (the client's real RM, or the entry's currently-stored RM) but stays fully
// editable so Admin can re-attribute just this entry to a different RM.
function populateBizRmDropdown(selectedRm){
  const sel = document.getElementById('biz_client_rm');
  if(!sel) return;
  const rms = getSegRMs('mf');
  let opts = rms.map(r=>`<option value="${escapeHtml(r)}" ${r===selectedRm?'selected':''}>${escapeHtml(r)}</option>`).join('');
  if(selectedRm && !rms.includes(selectedRm)){
    opts += `<option value="${escapeHtml(selectedRm)}" selected>${escapeHtml(selectedRm)} (inactive/unlisted)</option>`;
  }
  sel.innerHTML = opts;
}

function selectBizClient(clientId){
  const clients=getMfTxnSearchClients();
  const c=clients.find(x=>x.id===clientId);
  if(!c) return;
  currentBusinessTarget = {id:c.id, name:c.name};
  const cSel=document.getElementById('biz_client_selected'); if(cSel) cSel.value=c.name;
  populateBizRmDropdown(c.rm||'');
  const cSearch=document.getElementById('biz_client_search'); if(cSearch) cSearch.value='';
  const out=document.getElementById('biz_client_results');
  if(out){ out.style.display='none'; out.innerHTML=''; }
}

document.addEventListener('click', e=>{
  const wrap=document.getElementById('biz_client_search');
  const out=document.getElementById('biz_client_results');
  if(wrap && out && !wrap.contains(e.target) && !out.contains(e.target)) out.style.display='none';
});


// MF Desk (pure role, or an RM granted MF Desk access) needs to search/select
// ANY client across all RMs when logging a transaction on their behalf —
// unlike the shared getMyMfClients() which stays RM-scoped everywhere else
// in the app, this is scoped specifically to the MF Transactions entry form.
function getMfTxnSearchClients(){
  if(CU.role==='admin' || hasMfDeskAccess(CU)) return DB.get('mf_clients')||[];
  return getMyMfClients();
}

function searchMfTxnClient(){
  const q=(document.getElementById('mftxn-client-search').value||'').trim().toLowerCase();
  const out=document.getElementById('mftxn-client-results');
  if(!out) return;
  if(q.length<2){ out.style.display='none'; out.innerHTML=''; return; }

  const clients=getMfTxnSearchClients();
  const matches=clients.filter(c=>
    (c.name||'').toLowerCase().includes(q) ||
    (c.pan||'').toLowerCase().includes(q) ||
    (c.mobile||'').toLowerCase().includes(q)
  ).slice(0,30);

  if(matches.length===0){
    out.innerHTML='<div style="padding:10px;color:var(--gray);font-size:.85rem">No client found</div>';
    out.style.display='block';
    return;
  }
  out.innerHTML=matches.map(c=>`
    <div style="padding:8px 12px;cursor:pointer;border-bottom:1px solid #f0f0f0" onmouseover="this.style.background='#f7f7f7'" onmouseout="this.style.background='#fff'" onclick="selectMfTxnClient('${c.id}')">
      <div style="font-weight:600;font-size:.88rem">${escapeHtml(c.name||'')}</div>
      <div style="font-size:.75rem;color:var(--gray)">${escapeHtml(c.pan||'—')} · ${escapeHtml(c.mobile||'—')} · RM: ${escapeHtml(c.rm||'—')}</div>
    </div>`).join('');
  out.style.display='block';
}

function selectMfTxnClient(clientId){
  const clients=getMfTxnSearchClients();
  const c=clients.find(x=>x.id===clientId);
  if(!c) return;
  mfTxnSelectedClient = {id:c.id, name:c.name, rm:c.rm||''};
  document.getElementById('mftxn-client-selected').value=c.name;
  document.getElementById('mftxn-client-rm').value=c.rm||'—';
  // Admin: default the RM dropdown to the client's RM (can still be overridden).
  const rmSel=document.getElementById('mftxn-rm-select');
  if(rmSel && CU.role==='admin' && c.rm){
    if(![...rmSel.options].some(o=>o.value===c.rm)){ rmSel.insertAdjacentHTML('afterbegin',`<option>${escapeHtml(c.rm)}</option>`); }
    rmSel.value=c.rm;
  }
  document.getElementById('mftxn-client-search').value='';
  const out=document.getElementById('mftxn-client-results');
  out.style.display='none'; out.innerHTML='';
}

// Close the results dropdown when clicking elsewhere
document.addEventListener('click', e=>{
  const wrap=document.getElementById('mftxn-client-search');
  const out=document.getElementById('mftxn-client-results');
  if(wrap && out && !wrap.contains(e.target) && !out.contains(e.target)){
    out.style.display='none';
  }
});

function saveMfTxnEntry(){
  if(!mfTxnSelectedClient){ toast('Please select a client first','error'); return; }
  // Admin may credit the entry to any RM via the dropdown; RM users use their own.
  let rmForEntry = mfTxnSelectedClient.rm;
  const rmSelEl=document.getElementById('mftxn-rm-select');
  if(CU.role==='admin' && rmSelEl && rmSelEl.style.display!=='none' && rmSelEl.value){ rmForEntry = rmSelEl.value; }
  const type=document.getElementById('mftxn-type').value;
  const amount=parseFloat(document.getElementById('mftxn-amount').value);
  const fundName=document.getElementById('mftxn-fund').value.trim();
  const targetScheme=(document.getElementById('mftxn-target-fund')?.value||'').trim();
  const date=document.getElementById('mftxn-date').value;

  if(!amount || amount<=0){ toast('Please enter the amount','error'); return; }
  if(!fundName){ toast('Please enter the fund name','error'); return; }
  if(mfTxnTypeNeedsTarget(type) && !targetScheme){ toast('Please enter the target scheme','error'); return; }
  if(!date){ toast('Please enter the date','error'); return; }

  // SIP / STP / SWP: First Payment is a tick (= amount when done) + Start Date (required)
  const sched = mfTxnHasSchedule(type);
  const startDate = (document.getElementById('mftxn-startdate')?.value||'').trim();
  const firstPayDone = !!document.getElementById('mftxn-firstpay')?.checked;
  const firstPay = (sched && firstPayDone) ? amount : null;
  if(sched && !startDate){ toast('Please enter the start date','error'); return; }

  const newEntry = {
    id: uid(),
    client_id: mfTxnSelectedClient.id,
    client_name: mfTxnSelectedClient.name,
    rm: rmForEntry,
    type, amount, fund_name: fundName,
    target_scheme: mfTxnTypeNeedsTarget(type) ? targetScheme : '',
    first_payment: sched ? firstPay : null,
    start_date: sched ? startDate : '',
    date,
    created_by: CU.name,
    created_by_role: CU.role,
    created: today(),
    // RM & MF Desk entries always start as Pending (admin reviews/updates).
    // Admin's own entries are auto-Approved (admin is the approving authority).
    status: CU.role==='admin' ? 'Approved' : 'Pending',
    decline_reason: '',
    cross_remark: '', cross_remark_by: '', cross_remark_at: ''
  };
  DB.appendMfBizEntry('entries', newEntry).then(()=>{ renderMfTxnTable(); });
  learnFundName(fundName);
  if(mfTxnTypeNeedsTarget(type)) learnFundName(targetScheme);
  toast(`${type} entry saved — ${mfTxnSelectedClient.name}`,'success');

  // Reset form for next entry but keep date/type for fast repeated entry
  mfTxnSelectedClient=null;
  document.getElementById('mftxn-client-selected').value='';
  document.getElementById('mftxn-client-rm').value='';
  document.getElementById('mftxn-client-search').value='';
  document.getElementById('mftxn-amount').value='';
  document.getElementById('mftxn-fund').value='';
  const tgtReset=document.getElementById('mftxn-target-fund'); if(tgtReset) tgtReset.value='';
  const fpReset=document.getElementById('mftxn-firstpay'); if(fpReset) fpReset.checked=false;
  const sdReset=document.getElementById('mftxn-startdate'); if(sdReset) sdReset.value='';
  toggleMfTxnTarget();

  renderMfTxnTable();
}

// Column sorting for the MF Transactions table. Click a header to sort; click
// again to flip the direction. Date & Amount default to descending, text columns
// to ascending.
let mfTxnSort = { key:'date', dir:'desc' };
function setMfTxnSort(key){
  if(mfTxnSort.key===key){ mfTxnSort.dir = mfTxnSort.dir==='asc' ? 'desc' : 'asc'; }
  else { mfTxnSort.key=key; mfTxnSort.dir = (key==='date'||key==='start_date'||key==='amount') ? 'desc' : 'asc'; }
  renderMfTxnTable();
}
function mfTxnSortValue(e, key){
  switch(key){
    case 'client': return (e.client_name||'').toLowerCase();
    case 'rm':     return (e.rm||'').toLowerCase();
    case 'type':   return (e.type||'').toLowerCase();
    case 'fund':   return (e.fund_name||'').toLowerCase();
    case 'amount': return Number(e.amount)||0;
    case 'status': return (e.status||'Pending').toLowerCase();
    case 'start_date': return (e.start_date||'');
    case 'date':
    default:       return (e.date||'');
  }
}

function getFilteredMfTxns(){
  let entries=getMfBizEntries();
  // RM scoping — only plain RMs are restricted to their own entries.
  // Admin and anyone with MF Desk access (pure role or RM+MF Desk) see everyone's.
  // Include entries the RM personally entered (created_by) as well as ones
  // attributed to them (rm) — otherwise a temp-access cover entry (credited
  // to the absent colleague's name) vanishes from the covering RM's own
  // list the instant they save it.
  if(CU.role!=='admin' && !hasMfDeskAccess(CU)){
    const myName=(CU.name||'').trim().toLowerCase();
    entries=entries.filter(e=>(e.rm||'').trim().toLowerCase()===myName || (e.created_by||'').trim().toLowerCase()===myName);
  }
  const q=(document.getElementById('mftxn-search')?.value||'').trim().toLowerCase();
  const rmF=document.getElementById('mftxn-rm-filter')?.value||'';
  const typeF=document.getElementById('mftxn-type-filter')?.value||'';
  const monthF=document.getElementById('mftxn-month-filter')?.value||'';
  const statusF=document.getElementById('mftxn-status-filter')?.value||'';
  const fromD=document.getElementById('mftxn-from-date')?.value||'';
  const toD=document.getElementById('mftxn-to-date')?.value||'';

  if(q) entries=entries.filter(e=>(e.client_name||'').toLowerCase().includes(q)||(e.rm||'').toLowerCase().includes(q)||(e.fund_name||'').toLowerCase().includes(q)||(e.target_scheme||'').toLowerCase().includes(q));
  if(rmF) entries=entries.filter(e=>(e.rm||'')===rmF);
  if(typeF) entries=entries.filter(e=>(e.type||'')===typeF);
  if(monthF) entries=entries.filter(e=>(e.date||'').slice(0,7)===monthF);
  if(statusF) entries=entries.filter(e=>(e.status||'Pending')===statusF);
  if(fromD) entries=entries.filter(e=>e.date>=fromD);
  if(toD) entries=entries.filter(e=>e.date<=toD);

  const sk=mfTxnSort.key, dir=(mfTxnSort.dir==='asc'?1:-1);
  return entries.sort((a,b)=>{
    const va=mfTxnSortValue(a,sk), vb=mfTxnSortValue(b,sk);
    let c;
    if(typeof va==='number' && typeof vb==='number') c=va-vb;
    else c=String(va).localeCompare(String(vb));
    if(c===0) c=(a.date||'').localeCompare(b.date||'')||(a.created||'').localeCompare(b.created||'');
    return c*dir;
  });
}

const MFTXN_TYPE_COLOR = {
  Lumpsum:'#1D9E75', SIP:'#185FA5', 'SIP Stop':'#C0392B', Switch:'#B7950B',
  STP:'#0891B2', Redemption:'#C0392B', SWP:'#D35400', 'Additional Buy':'#059669', 'SIP Bounce Buy':'#D97706', 'SIP Pause':'#7C3AED'
};

function renderMfTxnTable(){
  const wrap=document.getElementById('mftxn-table');
  if(!wrap) return;
  populateMfTxnMonths();
  let entries=getFilteredMfTxns();
  // Column AutoFilter
  entries = CF.applyMftxn(entries);
  const cnt=document.getElementById('mftxn-count');
  if(cnt) cnt.innerHTML=entries.length+' entries · <b>Incentive '+INC.fmt(INC.total('mf',entries))+'</b>';

  if(entries.length===0){
    wrap.innerHTML='<div style="padding:30px;text-align:center;color:var(--gray)">No transactions found</div>';
    MFTBULK.afterRender();
    return;
  }

  const arrow=k=> mfTxnSort.key===k ? (mfTxnSort.dir==='asc'?' ▲':' ▼') : ' ⇅';
  const sTh=(k,label,cfCol,extra='')=>CF.th('mftxn',cfCol,`<span onclick="setMfTxnSort('${k}')" style="cursor:pointer;user-select:none;white-space:nowrap">${label}<span style="color:var(--gray);font-size:.7em">${arrow(k)}</span></span>`);
  const sThNoFilter=(k,label,extra='')=>`<th style="cursor:pointer;user-select:none;white-space:nowrap;${extra}" onclick="setMfTxnSort('${k}')" title="Click to sort">${label}<span style="color:var(--gray);font-size:.7em">${arrow(k)}</span></th>`;

  wrap.innerHTML=`<table>
    <thead><tr>${MFTBULK.th()}${sThNoFilter('date','Date')}${sThNoFilter('start_date','SIP Start')}${sThNoFilter('client','Client')}${sTh('rm','RM','rm')}${sTh('type','Type','type')}${sTh('fund','Fund Name','fund_name')}${sThNoFilter('amount','Amount (₹)','text-align:right')}<th style="text-align:right;white-space:nowrap">Incentive</th><th>Cross-Check</th>${sTh('status','Status','status')}<th></th></tr></thead>
    <tbody>
      ${entries.map(e=>{
        const color=MFTXN_TYPE_COLOR[e.type]||'#777';
        const status = e.status||'Pending';
        const canRemark = canAddCrossRemark(e);
        const remarkCell = e.cross_remark
          ? `<div style="font-size:.78rem;color:var(--navy)">${escapeHtml(e.cross_remark)}</div><div style="font-size:.68rem;color:var(--gray);margin-top:2px">— ${escapeHtml(e.cross_remark_by||'')}, ${fmtDate(e.cross_remark_at)}</div>${canRemark?`<span class="btn-icon" style="cursor:pointer;font-size:.72rem;color:var(--teal)" onclick="addCrossRemark('${e.id}')">✏️ Edit</span>`:''}${CU.role==='admin'?` <span class="btn-icon" style="cursor:pointer;font-size:.72rem;color:var(--red)" onclick="clearCrossRemark('${e.id}')">🗑️ Clear</span>`:''}`
          : (canRemark ? `<span class="btn-icon" style="cursor:pointer;font-size:.78rem;color:var(--teal)" onclick="addCrossRemark('${e.id}')">💬 Rmk</span>` : '<span style="color:var(--gray);font-size:.78rem">—</span>');
        return `<tr>
          ${MFTBULK.td(e.id)}
          <td style="width:68px;font-size:.67rem;white-space:nowrap">${e.date||'—'}</td>
          <td style="width:68px;font-size:.67rem;white-space:nowrap">${e.start_date?fmtDate(e.start_date):'—'}</td>
          <td onclick="viewMfTxnDetail('${e.id}')" style="width:105px;max-width:110px;font-size:.67rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-weight:600;cursor:pointer;color:var(--teal,#0d9488);text-decoration:underline" title="Click to view full transaction detail">${escapeHtml(e.client_name||'—')}</td>
          <td style="width:40px;font-size:.67rem;white-space:nowrap">${escapeHtml(e.rm||'—')}</td>
          <td style="width:62px;white-space:nowrap"><span class="badge" style="background:${color}22;color:${color};font-size:.61rem;padding:1px 5px">${escapeHtml(e.type||'—')}</span></td>
          <td style="width:155px;max-width:160px;font-size:.67rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${escapeHtml(e.fund_name||'')}${e.target_scheme?' → '+escapeHtml(e.target_scheme):''}">${escapeHtml(e.fund_name||'—')}${e.target_scheme?`<span style="font-size:.63rem;color:var(--teal,#0891b2);margin-left:3px">↳ ${escapeHtml(e.target_scheme)}</span>`:''}</td>
          <td style="text-align:right;font-weight:600;width:70px;font-size:.67rem;white-space:nowrap">₹${brkFmt(e.amount)}${e.first_payment?` <span title="${e.type==='SWP'?'First withdrawal received':'First payment received'}" style="display:inline-block;color:#fff;background:var(--green,#16a34a);font-size:.55rem;font-weight:700;line-height:1;border-radius:3px;padding:2px 3px;vertical-align:middle">P</span>`:''}</td>
          ${INC.cell('mf',e)}
          <td style="width:95px;max-width:100px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-size:.65rem">${remarkCell}</td>
          <td>${bizStatusBadge(status)}${status==='Declined'&&e.decline_reason?`<div style="font-size:.7rem;color:var(--red);margin-top:2px">${escapeHtml(e.decline_reason)}</div>`:''}</td>
          <td style="white-space:nowrap">${CU.role==='admin'?`
            ${status!=='Approved'?`<button class="btn-icon" onclick="approveBusinessEntry('${e.id}')" title="Approve" style="color:var(--green)">✅</button>`:''}
            ${status!=='Declined'?`<button class="btn-icon" onclick="declineBusinessEntry('${e.id}')" title="Decline" style="color:var(--red)">❌</button>`:''}
            ${status!=='Pending'?`<button class="btn-icon" onclick="markPendingBusinessEntry('${e.id}')" title="Mark Pending" style="color:var(--gray)">↩️</button>`:''}
            <button class="btn-icon" onclick="editBusinessEntry('${e.id}')" title="Edit (all fields)">✏️</button>
            <button class="btn-icon" onclick="deleteMfTxnEntry('${e.id}')" title="Delete" style="color:var(--red)">🗑️</button>`
          : (hasMfDeskAccess(CU) ? `<button class="btn-icon" onclick="editBusinessEntry('${e.id}')" title="Edit">✏️</button>` : '')}</td>
        </tr>`;
      }).join('')}
    </tbody>
    <tfoot><tr style="font-weight:700;background:var(--bg,#f6f7fb);border-top:2px solid var(--border,#ddd)">
      <td colspan="${CU.role==='admin'?7:6}" style="text-align:right">TOTAL${(document.getElementById('mftxn-rm-filter')?.value)?' — '+escapeHtml(document.getElementById('mftxn-rm-filter').value):''} (${entries.length})</td>
      <td style="text-align:right">₹${brkFmt(entries.reduce((s,e)=>s+(Number(e.amount)||0),0))}</td>
      <td style="text-align:right">${INC.fmt(INC.total('mf',entries))}</td>
      <td colspan="3"></td>
    </tr></tfoot>
  </table>`;
  MFTBULK.afterRender();
}

function deleteMfTxnEntry(id){
  if(CU.role!=='admin') return;
  if(!confirm('Delete this transaction entry?')) return;
  const entries=getMfBizEntries().filter(e=>e.id!==id);
  setMfBizEntries(entries);
  toast('Entry deleted','success');
  renderMfTxnTable();
}

function exportMfTxns(){
  let entries=getFilteredMfTxns();
  let onlySel=false;
  if(typeof MFTBULK!=='undefined' && MFTBULK.sel && MFTBULK.sel.size>0){
    const ids=MFTBULK.sel;
    entries=entries.filter(e=>ids.has(e.id));
    onlySel=true;
  }
  if(entries.length===0){ toast('No data to export','error'); return; }
  const cols=[
    {header:'Date', width:12},
    {header:'Client Name', width:24},
    {header:'RM', width:14},
    {header:'Type', width:14},
    {header:'Fund Name', width:28},
    {header:'Target Scheme', width:28},
    {header:'Amount', width:14, money:true},
    {header:'Incentive', width:14, money:true},
    {header:'Status', width:12, color:dnStatusColor, align:'center'},
    {header:'Cross-Check Remark', width:22}
  ];
  const rows=entries.map(e=>[e.date||'',e.client_name||'',e.rm||'',e.type||'',e.fund_name||'',e.target_scheme||'',e.amount||0,(INC.isApproved(e)?Math.round(INC.mf(e).amt):0),e.status||'Pending',e.cross_remark||'']);
  const totAmt=entries.reduce((s,e)=>s+(Number(e.amount)||0),0);
  const totInc=Math.round(INC.total('mf',entries));
  const totalRow=['TOTAL','','','','','',totAmt,totInc,'', ''];
  dnXlsx('MF_Transactions_'+today()+'.xlsx', 'MF Transactions — '+today(), cols, rows, totalRow);
  toast(onlySel?`Exported ${entries.length} selected`:'Export done!','success');
}

// ══════════════════════════════════════════
// EQUITY: DEMAT ACCOUNT OPENING (dedicated entry page — mirrors MF Transactions)
// Uses the same 'mf_business' store's eq_entries array, so it feeds the same
// "New Business — Demat Accounts" report. Each entry has an Approve/Decline/
// Pending status, same workflow as MF business entries.
// ══════════════════════════════════════════
let eqDematSelectedClient = null;

function getEqDematEntries(){
  const biz = DB.get('mf_business');
  return Array.isArray(biz) ? [] : (biz?.eq_entries || []);
}
function setEqDematEntries(eqEntries){
  const biz = DB.get('mf_business');
  const entries = Array.isArray(biz) ? biz : (biz?.entries||[]);
  DB.set('mf_business', {entries, eq_entries: eqEntries});
}

function renderEqDematPage(){
  eqDematSelectedClient = null;
  const sel=document.getElementById('eqdemat-client-selected'); if(sel) sel.value='';
  const rmEl=document.getElementById('eqdemat-client-rm'); if(rmEl) rmEl.value='';
  const srch=document.getElementById('eqdemat-client-search'); if(srch) srch.value='';
  const codeEl=document.getElementById('eqdemat-code'); if(codeEl) codeEl.value='';
  const res=document.getElementById('eqdemat-client-results'); if(res){ res.style.display='none'; res.innerHTML=''; }
  const dateEl=document.getElementById('eqdemat-date'); if(dateEl && !dateEl.value) dateEl.value=today();

  // Populate RM filter dropdown (admin sees all Equity RMs, RM sees just themself)
  const rmFilter=document.getElementById('eqdemat-rm-filter');
  if(rmFilter){
    const eqRMs = CU.role==='admin' ? getSegRMs('equity') : [CU.name];
    rmFilter.innerHTML='<option value="">All RMs</option>'+eqRMs.map(r=>`<option>${r}</option>`).join('');
    if(CU.role!=='admin'){ rmFilter.value=CU.name; rmFilter.disabled=true; }
  }
  // Populate Opening RM dropdown (who opened the account → gets opening bonus)
  const openSel=document.getElementById('eqdemat-opening-rm');
  if(openSel){
    const allRMs = CU.role==='admin' ? getSegRMs('equity') : [CU.name];
    openSel.innerHTML=allRMs.map(r=>`<option>${r}</option>`).join('');
    openSel.value = CU.name && allRMs.includes(CU.name) ? CU.name : (allRMs[0]||'');
    if(CU.role!=='admin') openSel.disabled=true;
  }

  // Admin can credit the Trading RM (brokerage) to ANY RM — show a dropdown
  // instead of the read-only box. RM users keep the read-only box.
  const tradeSel=document.getElementById('eqdemat-rm-select');
  const tradeRO=document.getElementById('eqdemat-client-rm');
  if(tradeSel){
    if(CU.role==='admin'){
      tradeSel.innerHTML=getSegRMs('equity').map(r=>`<option>${escapeHtml(r)}</option>`).join('');
      tradeSel.style.display=''; if(tradeRO) tradeRO.style.display='none';
    } else {
      tradeSel.style.display='none'; if(tradeRO) tradeRO.style.display='';
    }
  }

  populateEqDematMonths();
  renderEqDematTable();
}

// Demat month dropdown — wahi mahine jo data me hain (RM ke apne, Admin ke saare).
function populateEqDematMonths(){
  const sel=document.getElementById('eqdemat-month-filter');
  if(!sel) return;
  let entries=getEqDematEntries();
  const me=(CU.name||'').trim().toLowerCase();
  if(CU.role!=='admin'){
    entries=entries.filter(e=>(e.rm||'').trim().toLowerCase()===me||(e.opening_rm||'').trim().toLowerCase()===me);
  }
  const months=[...new Set(entries.map(e=>(e.date||'').slice(0,7)).filter(Boolean))].sort().reverse();
  const cur=sel.value;
  const MNF=['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const label=m=>{const [y,mo]=m.split('-');return MNF[+mo]+' '+y;};
  sel.innerHTML='<option value="">All Months</option>'+months.map(m=>`<option value="${m}">${label(m)}</option>`).join('');
  if(months.includes(cur)){
    sel.value=cur;
  } else if(!sel.dataset.autoDefaulted){
    // Same first-load default as the MF Transactions month filter — open
    // straight to the current month instead of "All Months", but only once
    // per session; a later explicit choice (including Clear) is respected.
    const curMonth = today().slice(0,7);
    if(months.includes(curMonth)) sel.value = curMonth;
  }
  sel.dataset.autoDefaulted = '1';
}

// Month chunne pe From/To date clear taaki conflict na ho.
function onEqDematMonthChange(){
  const m=document.getElementById('eqdemat-month-filter')?.value||'';
  if(m){
    const fromEl=document.getElementById('eqdemat-from-date'); if(fromEl) fromEl.value='';
    const toEl=document.getElementById('eqdemat-to-date'); if(toEl) toEl.value='';
  }
  renderEqDematTable();
}

function searchEqDematClient(){
  const q=(document.getElementById('eqdemat-client-search').value||'').trim().toLowerCase();
  const out=document.getElementById('eqdemat-client-results');
  if(!out) return;
  if(q.length<2){ out.style.display='none'; out.innerHTML=''; return; }

  const clients=getMyEqClients();
  const matches=clients.filter(c=>
    (c.name||'').toLowerCase().includes(q) ||
    (c.code||'').toLowerCase().includes(q) ||
    (c.mobile||'').toLowerCase().includes(q)
  ).slice(0,30);

  if(matches.length===0){
    out.innerHTML='<div style="padding:10px;color:var(--gray);font-size:.85rem">No client found</div>';
    out.style.display='block';
    return;
  }
  out.innerHTML=matches.map(c=>`
    <div style="padding:8px 12px;cursor:pointer;border-bottom:1px solid #f0f0f0" onmouseover="this.style.background='#f7f7f7'" onmouseout="this.style.background='#fff'" onclick="selectEqDematClient('${c.id}')">
      <div style="font-weight:600;font-size:.88rem">${escapeHtml(c.name||'')}</div>
      <div style="font-size:.75rem;color:var(--gray)">${escapeHtml(c.code||'—')} · ${escapeHtml(c.mobile||'—')} · RM: ${escapeHtml(c.rm||'—')}</div>
    </div>`).join('');
  out.style.display='block';
}

function selectEqDematClient(clientId){
  const clients=getMyEqClients();
  const c=clients.find(x=>x.id===clientId);
  if(!c) return;
  eqDematSelectedClient = {id:c.id, name:c.name, rm:c.rm||''};
  document.getElementById('eqdemat-client-selected').value=c.name;
  document.getElementById('eqdemat-client-rm').value=c.rm||'—';
  // Admin: default the Opening RM to the client's RM (can change if a different RM opened it)
  if(CU.role==='admin'){
    const tradeSel=document.getElementById('eqdemat-rm-select');
    if(tradeSel && c.rm){
      if(![...tradeSel.options].some(o=>o.value===c.rm)) tradeSel.add(new Option(c.rm,c.rm));
      tradeSel.value=c.rm;
    }
    const openSel=document.getElementById('eqdemat-opening-rm');
    if(openSel && c.rm){
      if(![...openSel.options].some(o=>o.value===c.rm)) openSel.add(new Option(c.rm,c.rm));
      openSel.value=c.rm;
    }
  }
  document.getElementById('eqdemat-client-search').value='';
  if(c.code) document.getElementById('eqdemat-code').value=c.code;
  const out=document.getElementById('eqdemat-client-results');
  out.style.display='none'; out.innerHTML='';
}

// Close the results dropdown when clicking elsewhere
document.addEventListener('click', e=>{
  const wrap=document.getElementById('eqdemat-client-search');
  const out=document.getElementById('eqdemat-client-results');
  if(wrap && out && !wrap.contains(e.target) && !out.contains(e.target)){
    out.style.display='none';
  }
});

function saveEqDematEntry(){
  if(!eqDematSelectedClient){ toast('Please select a client first','error'); return; }
  const code=document.getElementById('eqdemat-code').value.trim().toUpperCase();
  const date=document.getElementById('eqdemat-date').value;
  const remarks=document.getElementById('eqdemat-remarks').value.trim();

  if(!date){ toast('Please enter the date','error'); return; }

  const openingRm=(document.getElementById('eqdemat-opening-rm')?.value||CU.name||'').trim();
  if(!openingRm){ toast('Please select the Opening RM','error'); return; }

  // Admin may credit the brokerage (Trading RM) to any RM via the dropdown.
  let tradingRm = eqDematSelectedClient.rm;
  const tradeSel=document.getElementById('eqdemat-rm-select');
  if(CU.role==='admin' && tradeSel && tradeSel.style.display!=='none' && tradeSel.value){ tradingRm = tradeSel.value; }

  const newEqEntry = {
    id: uid(),
    client_id: eqDematSelectedClient.id,
    client_name: eqDematSelectedClient.name,
    client_code: code,
    rm: tradingRm,
    opening_rm: openingRm,
    type: 'Open Demat Account',
    date, remarks,
    mobile: '',
    created_by: CU.name,
    created: today(),
    status: CU.role==='admin' ? 'Approved' : 'Pending',
    decline_reason: ''
  };
  DB.appendMfBizEntry('eq_entries', newEqEntry).then(()=>{ renderEqDematTable(); });
  toast(`Demat entry saved — ${eqDematSelectedClient.name}`,'success');

  // Reset form for next entry but keep date for fast repeated entry
  eqDematSelectedClient=null;
  document.getElementById('eqdemat-client-selected').value='';
  document.getElementById('eqdemat-client-rm').value='';
  document.getElementById('eqdemat-client-search').value='';
  document.getElementById('eqdemat-code').value='';
  document.getElementById('eqdemat-remarks').value='';

  renderEqDematTable();
}

function getFilteredEqDemat(){
  let entries=getEqDematEntries();
  const me=(CU.name||'').trim().toLowerCase();
  // RM scoping — non-admin sees entries where they are the trading RM OR the opening RM
  if(CU.role!=='admin'){
    entries=entries.filter(e=>(e.rm||'').trim().toLowerCase()===me||(e.opening_rm||'').trim().toLowerCase()===me);
  }
  const q=(document.getElementById('eqdemat-search')?.value||'').trim().toLowerCase();
  const rmF=document.getElementById('eqdemat-rm-filter')?.value||'';
  const statusF=document.getElementById('eqdemat-status-filter')?.value||'';
  const monthF=document.getElementById('eqdemat-month-filter')?.value||'';
  const fromD=document.getElementById('eqdemat-from-date')?.value||'';
  const toD=document.getElementById('eqdemat-to-date')?.value||'';

  if(q) entries=entries.filter(e=>(e.client_name||'').toLowerCase().includes(q)||(e.rm||'').toLowerCase().includes(q)||(e.opening_rm||'').toLowerCase().includes(q)||(e.client_code||'').toLowerCase().includes(q));
  if(rmF) entries=entries.filter(e=>(e.rm||'')===rmF||(e.opening_rm||'')===rmF);
  if(statusF) entries=entries.filter(e=>(e.status||'Pending')===statusF);
  if(monthF) entries=entries.filter(e=>(e.date||'').slice(0,7)===monthF);
  if(fromD) entries=entries.filter(e=>e.date>=fromD);
  if(toD) entries=entries.filter(e=>e.date<=toD);

  return entries.sort((a,b)=>(b.date||'').localeCompare(a.date||'')||(b.created||'').localeCompare(a.created||''));
}

function renderEqDematTable(){
  const wrap=document.getElementById('eqdemat-table');
  if(!wrap) return;
  populateEqDematMonths();
  let entries=getFilteredEqDemat();
  // Column AutoFilter
  entries = CF.applyDemat(entries);
  const cnt=document.getElementById('eqdemat-count');
  if(cnt) cnt.innerHTML=entries.length+' entries · <b>Incentive '+INC.fmt(INC.total('demat',entries))+'</b>';

  if(entries.length===0){
    wrap.innerHTML='<div style="padding:30px;text-align:center;color:var(--gray)">No Demat account entries found</div>';
    return;
  }

  wrap.innerHTML=`<table>
    <thead><tr><th>Date</th><th>Client</th><th>Client Code</th>${CF.th('demat','rm','Trading RM')}${CF.th('demat','rm','Opening RM')}<th style="text-align:right;white-space:nowrap">Incentive</th><th>Remarks</th>${CF.th('demat','status','Status')}<th></th></tr></thead>
    <tbody>
      ${entries.map(e=>{
        const status = e.status||'Pending';
        return `<tr>
          <td>${e.date||'—'}</td>
          <td onclick="viewDematDetail('${e.id}')" style="cursor:pointer;color:var(--teal,#0d9488);text-decoration:underline;font-weight:600" title="Click to view full demat detail">${escapeHtml(e.client_name||'—')}</td>
          <td>${escapeHtml(e.client_code||'—')}</td>
          <td>${escapeHtml(e.rm||'—')}</td>
          <td><span style="background:var(--teal,#0d9488);color:#fff;border-radius:8px;padding:1px 8px;font-size:.75rem;font-weight:600">${escapeHtml(e.opening_rm||e.rm||'—')}</span></td>
          ${INC.cell('demat',e)}
          <td style="font-size:.8rem;color:var(--gray)">${escapeHtml(e.remarks||'—')}</td>
          <td>${bizStatusBadge(status)}${status==='Declined'&&e.decline_reason?`<div style="font-size:.7rem;color:var(--red);margin-top:2px">${escapeHtml(e.decline_reason)}</div>`:''}</td>
          <td style="white-space:nowrap">${CU.role==='admin'?`
            ${status!=='Approved'?`<button class="btn-icon" onclick="approveEqDematEntry('${e.id}')" title="Approve" style="color:var(--green)">✅</button>`:''}
            ${status!=='Declined'?`<button class="btn-icon" onclick="declineEqDematEntry('${e.id}')" title="Decline" style="color:var(--red)">❌</button>`:''}
            ${status!=='Pending'?`<button class="btn-icon" onclick="markPendingEqDematEntry('${e.id}')" title="Mark Pending" style="color:var(--gray)">↩️</button>`:''}
            <button class="btn-icon" onclick="editEqDematEntry('${e.id}')" title="Edit">✏️</button>
            <button class="btn-icon" onclick="deleteEqDematEntry('${e.id}')" title="Delete" style="color:var(--red)">🗑️</button>`:''}</td>
        </tr>`;
      }).join('')}
    </tbody>
    <tfoot><tr style="font-weight:700;background:var(--bg,#f6f7fb);border-top:2px solid var(--border,#ddd)">
      <td colspan="5" style="text-align:right">TOTAL${(document.getElementById('eqdemat-rm-filter')?.value)?' — '+escapeHtml(document.getElementById('eqdemat-rm-filter').value):''} (${entries.length} demat)</td>
      <td style="text-align:right">${INC.fmt(INC.total('demat',entries))}</td>
      <td colspan="2"></td>
    </tr></tfoot>
  </table>`;
}

function approveEqDematEntry(id){
  if(CU.role!=='admin') return;
  const entries = getEqDematEntries();
  const e = entries.find(x=>x.id===id);
  if(!e) return;
  e.status = 'Approved';
  e.decline_reason = '';
  setEqDematEntries(entries);
  toast('Demat entry approved!','success');
  renderEqDematTable();
}

function declineEqDematEntry(id){
  if(CU.role!=='admin') return;
  const reason = prompt('Decline reason (optional):','') || '';
  const entries = getEqDematEntries();
  const e = entries.find(x=>x.id===id);
  if(!e) return;
  e.status = 'Declined';
  e.decline_reason = reason.trim();
  setEqDematEntries(entries);
  toast('Demat entry declined','success');
  renderEqDematTable();
}

function markPendingEqDematEntry(id){
  if(CU.role!=='admin') return;
  const entries = getEqDematEntries();
  const e = entries.find(x=>x.id===id);
  if(!e) return;
  e.status = 'Pending';
  e.decline_reason = '';
  setEqDematEntries(entries);
  toast('Status reset to Pending','success');
  renderEqDematTable();
}

function deleteEqDematEntry(id){
  if(CU.role!=='admin') return;
  if(!confirm('Delete this Demat account entry?')) return;
  const entries = getEqDematEntries().filter(e=>e.id!==id);
  setEqDematEntries(entries);
  toast('Entry deleted','success');
  renderEqDematTable();
}

let editingDematId = null;

function editEqDematEntry(id){
  if(CU.role!=='admin') return;
  const entries = getEqDematEntries();
  const e = entries.find(x=>x.id===id);
  if(!e) return;
  editingDematId = id;
  document.getElementById('dematEditModalTitle').textContent = 'Edit Demat Entry — '+escapeHtml(e.client_name||'');
  document.getElementById('demat_edit_client').value = e.client_name||'';
  document.getElementById('demat_edit_rm').value = e.rm||'—';
  document.getElementById('demat_edit_code').value = e.client_code||'';
  document.getElementById('demat_edit_date').value = e.date||today();
  document.getElementById('demat_edit_remarks').value = e.remarks||'';
  // Populate opening RM dropdown
  const sel = document.getElementById('demat_edit_opening_rm');
  const allRMs = getSegRMs('equity');
  sel.innerHTML = allRMs.map(r=>`<option value="${escapeHtml(r)}" ${r===(e.opening_rm||e.rm)?'selected':''}>${escapeHtml(r)}</option>`).join('');
  if(!allRMs.includes(e.opening_rm||e.rm)){
    sel.innerHTML = `<option value="${escapeHtml(e.opening_rm||e.rm)}" selected>${escapeHtml(e.opening_rm||e.rm)}</option>` + sel.innerHTML;
  }
  document.getElementById('dematEditModal').classList.add('open');
}

function saveDematEdit(){
  if(CU.role!=='admin') return;
  if(!editingDematId) return;
  const date = document.getElementById('demat_edit_date').value;
  if(!date){ toast('Date is required','error'); return; }
  const entries = getEqDematEntries();
  const e = entries.find(x=>x.id===editingDematId);
  if(!e){ toast('Entry not found','error'); return; }
  e.client_code = document.getElementById('demat_edit_code').value.trim().toUpperCase();
  e.date        = date;
  e.remarks     = document.getElementById('demat_edit_remarks').value.trim();
  e.opening_rm  = document.getElementById('demat_edit_opening_rm').value;
  setEqDematEntries(entries);
  toast('Demat entry updated!','success');
  closeModal('dematEditModal');
  editingDematId = null;
  renderEqDematTable();
}

function exportEqDemat(){
  const entries=getFilteredEqDemat();
  if(entries.length===0){ toast('No data to export','error'); return; }
  const cols=[
    {header:'Date', width:12},
    {header:'Client Name', width:24},
    {header:'Client Code', width:14},
    {header:'Trading RM', width:14},
    {header:'Opening RM', width:14},
    {header:'Incentive', width:14, money:true},
    {header:'Remarks', width:28},
    {header:'Status', width:12, color:dnStatusColor, align:'center'}
  ];
  const rows=entries.map(e=>[e.date||'',e.client_name||'',e.client_code||'',e.rm||'',e.opening_rm||e.rm||'',(INC.isApproved(e)?Math.round(INC.demat(e).amt):0),e.remarks||'',e.status||'Pending']);
  const totInc=Math.round(INC.total('demat',entries));
  const totalRow=['TOTAL','','','','',totInc,'',''];
  dnXlsx('Demat_Account_Opening_'+today()+'.xlsx', 'Demat Account Opening — '+today(), cols, rows, totalRow);
  toast('Export done!','success');
}

// ══════════════════════════════════════════
// FOLLOW-UP PAGES
// ══════════════════════════════════════════
function renderFollowup(group){
  try{
  const seg=group==='eqf'?'equity':'mf';
  const tab=group==='eqf'?activeEqfTab:activeMffTab;
  let clients=group==='eqf'?getActiveEqClients():getMyMfClients();
  const t=today(), t7=addDays(t,7);

  // Admin-only RM filter dropdown — populate once, keep selection sticky
  // (same pattern used on No-Trade Alerts and the Trade Activity card).
  const isAdmin = CU.role==='admin';
  const rmSel = document.getElementById(group+'-rm');
  if(rmSel){
    if(isAdmin){
      if(!rmSel.dataset.filled){
        const rms=[...new Set(getSegRMs(seg))].sort((a,b)=>a.localeCompare(b));
        rmSel.innerHTML = '<option value="">👥 All RMs</option>' + rms.map(r=>`<option value="${escapeHtml(r)}">${escapeHtml(r)}</option>`).join('');
        rmSel.dataset.filled = '1';
      }
      rmSel.style.display = '';
    } else {
      rmSel.style.display = 'none';
      rmSel.value = '';
    }
  }
  const rmFilter = (isAdmin && rmSel) ? (rmSel.value||'').trim() : '';
  if(rmFilter) clients = clients.filter(c=>(c.rm||'').trim().toUpperCase()===rmFilter.toUpperCase());

  let data;
  if(tab==='today') data=clients.filter(c=>c.next_call&&c.next_call<=t);
  else if(tab==='overdue') data=clients.filter(c=>c.next_call&&c.next_call<t);
  else if(tab==='upcoming') data=clients.filter(c=>c.next_call&&c.next_call>t&&c.next_call<=t7);
  else data=clients.filter(c=>c.next_call||c.followup_status==='Pending');

  const cont=document.getElementById(group+'-content');
  if(!cont) return;
  const countEl=document.getElementById(group+'-count');

  if(!data.length){
    if(countEl) countEl.textContent='0 clients';
    cont.innerHTML=`<div style="text-align:center;padding:48px;color:var(--gray)">✅ No ${tab} follow-ups</div>`;
    return;
  }
  if(countEl) countEl.textContent=data.length+' client'+(data.length===1?'':'s');

  const tableKey=group+'-fu';
  data = applySort(data, tableKey, {
    name:{get:c=>c.name, type:'str'},
    mobile:{get:c=>c.mobile, type:'str'},
    rm:{get:c=>c.rm, type:'str'},
    next_call:{get:c=>c.next_call, type:'date'},
    last_call_date:{get:c=>c.last_call_date, type:'date'},
    followup_status:{get:c=>c.followup_status, type:'str'},
    remarks:{get:c=>c.remarks, type:'str'},
  });

  let h=`<div class="tbl-wrap"><div class="tbl-scroll"><table><thead><tr>
    ${sortTh('Name',tableKey,'name','str',`()=>renderFollowup('${group}')`)}
    ${sortTh('Mobile',tableKey,'mobile','str',`()=>renderFollowup('${group}')`)}
    ${sortTh('RM',tableKey,'rm','str',`()=>renderFollowup('${group}')`)}
    ${sortTh('Last Call',tableKey,'last_call_date','date',`()=>renderFollowup('${group}')`)}
    ${sortTh('Next Call',tableKey,'next_call','date',`()=>renderFollowup('${group}')`)}
    ${sortTh('Status',tableKey,'followup_status','str',`()=>renderFollowup('${group}')`)}
    ${sortTh('Remarks',tableKey,'remarks','str',`()=>renderFollowup('${group}')`)}
    <th>Actions</th>
    </tr></thead><tbody>`;

  data.forEach(c=>{
    const isOver=c.next_call&&c.next_call<t;
    h+=`<tr class="${isOver?'row-alert':''}" data-client-id="${c.id}">
      <td style="font-weight:600">${c.name}</td>
      <td><a href="tel:${c.mobile}" style="color:var(--navy);text-decoration:none">${c.mobile||'—'}</a></td>
      <td>${c.rm||'—'}</td>
      <td>${fmtDate(c.last_call_date)||'—'}</td>
      <td style="color:${isOver?'var(--red)':'inherit'};font-weight:${isOver?700:400}">${fmtDate(c.next_call)||'—'}</td>
      <td><span class="badge ${c.followup_status==='Done'?'b-done':'b-pending'}">${c.followup_status||'—'}</span></td>
      <td style="max-width:120px;overflow:hidden;text-overflow:ellipsis">${c.remarks||'—'}</td>
      <td>
        <button class="btn-icon" onclick="editClient('${c.id}','${seg==='equity'?'equity':'mf'}')" title="Edit">✏️</button>
        ${c.mobile?`<a href="https://wa.me/91${c.mobile}" target="_blank" class="btn-icon" title="WhatsApp">💬</a>`:''}
      </td></tr>`;
  });
  h+='</tbody></table></div></div>';
  cont.innerHTML=h;
  }catch(e){
    console.error('renderFollowup error:', e);
    const _errCont = document.getElementById('eqnc-content')||document.getElementById('mfnc-content')||document.getElementById('nt-content')||document.getElementById('sip-table')||document.getElementById('seminars-table')||document.getElementById((arguments[0]==='eqf'?'eqf':'mff')+'-content');
    if(_errCont) _errCont.innerHTML = '<div style="text-align:center;padding:36px;color:var(--red)">⚠️ Error loading data: '+(e.message||e)+'</div>';
  }
}

// ══════════════════════════════════════════
// NO-TRADE ALERTS
// ══════════════════════════════════════════
function renderNoTrade(){
  try{
  const days=parseInt(activeNtTab);
  const eq=getActiveEqClients();

  // Admin-only RM filter dropdown — populate once, keep selection sticky
  // (same pattern used on the Trade Activity dashboard card).
  const isAdmin = CU.role==='admin';
  const rmSel = document.getElementById('nt-rm');
  if(rmSel){
    if(isAdmin){
      if(!rmSel.dataset.filled){
        const rms=[...new Set(getSegRMs('equity'))].sort((a,b)=>a.localeCompare(b));
        rmSel.innerHTML = '<option value="">👥 All RMs</option>' + rms.map(r=>`<option value="${escapeHtml(r)}">${escapeHtml(r)}</option>`).join('');
        rmSel.dataset.filled = '1';
      }
      rmSel.style.display = '';
    } else {
      rmSel.style.display = 'none';
      rmSel.value = '';
    }
  }
  const rmFilter = (isAdmin && rmSel) ? (rmSel.value||'').trim() : '';

  let data=eq.filter(c=>daysDiff(c.last_trade_date)>=days)
    .filter(c=>!rmFilter || (c.rm||'').trim().toUpperCase()===rmFilter.toUpperCase())
    .map(c=>({...c,daysAgo:daysDiff(c.last_trade_date)}))
    .sort((a,b)=>a.daysAgo-b.daysAgo);

  const cont=document.getElementById('nt-content');
  if(!cont) return;
  if(!data.length){
    cont.innerHTML=`<div style="text-align:center;padding:48px;color:var(--green)">✅ No clients with ${days}+ days no-trade</div>`;
    return;
  }
  data = applySort(data, 'nt', {
    code:{get:c=>c.code, type:'str'},
    name:{get:c=>c.name, type:'str'},
    mobile:{get:c=>c.mobile, type:'str'},
    rm:{get:c=>c.rm, type:'str'},
    last_trade_date:{get:c=>c.last_trade_date, type:'date'},
    daysAgo:{get:c=>c.daysAgo, type:'num'},
    asset_value:{get:c=>c.asset_value, type:'num'},
  });
  let h=`<p style="margin-bottom:12px;color:var(--gray);font-size:.82rem">${data.length} clients with no trade in ${days}+ days</p>
    <div class="tbl-wrap"><div class="tbl-scroll"><table><thead><tr>
    ${sortTh('Code','nt','code','str','renderNoTrade')}
    ${sortTh('Name','nt','name','str','renderNoTrade')}
    ${sortTh('Mobile','nt','mobile','str','renderNoTrade')}
    ${sortTh('RM','nt','rm','str','renderNoTrade')}
    ${sortTh('Last Trade','nt','last_trade_date','date','renderNoTrade')}
    ${sortTh('Days Since','nt','daysAgo','num','renderNoTrade')}
    ${sortTh('Asset Value','nt','asset_value','num','renderNoTrade')}
    <th>Actions</th>
    </tr></thead><tbody>`;
  data.forEach(c=>{
    const cls=c.daysAgo>=180?'row-alert':c.daysAgo>=90?'row-inactive':'';
    h+=`<tr class="${cls}">
      <td>${c.code||'—'}</td>
      <td style="font-weight:600">${c.name}</td>
      <td><a href="tel:${c.mobile}" style="color:var(--navy);text-decoration:none">${c.mobile||'—'}</a></td>
      <td>${c.rm||'—'}</td>
      <td>${fmtDate(c.last_trade_date)||'Never'}</td>
      <td style="font-weight:700;color:${c.daysAgo>=180?'var(--red)':c.daysAgo>=90?'var(--orange)':'var(--gold)'}">${c.daysAgo}d</td>
      <td>${c.asset_value?'₹'+fmtNum(c.asset_value):'—'}</td>
      <td>
        ${CU.role!=='backoffice'?`<button class="btn-icon" onclick="editClient('${c.id}','equity')" title="Edit">✏️</button>`:''}
        ${c.mobile?`<a href="https://wa.me/91${c.mobile}" target="_blank" class="btn-icon">💬</a>`:''}
      </td></tr>`;
  });
  h+='</tbody></table></div></div>';
  cont.innerHTML=h;
  }catch(e){
    console.error('renderNoTrade error:', e);
    const _errCont = document.getElementById('eqnc-content')||document.getElementById('mfnc-content')||document.getElementById('nt-content')||document.getElementById('sip-table')||document.getElementById('seminars-table')||document.getElementById((arguments[0]==='eqf'?'eqf':'mff')+'-content');
    if(_errCont) _errCont.innerHTML = '<div style="text-align:center;padding:36px;color:var(--red)">⚠️ Error loading data: '+(e.message||e)+'</div>';
  }
}

// ══════════════════════════════════════════
// NO-CALL ALERTS (last_call_date not updated)
// ══════════════════════════════════════════
function renderNoCall(seg){
  try{
  const days = parseInt(seg==='equity'?activeEqncTab:activeMfncTab);
  const clients = seg==='equity'?getActiveEqClients():getMyMfClients();
  let data = clients.filter(c=>{
    if(c.do_not_call) return false; // exclude DNC clients
    const d = daysDiff(c.last_call_date);
    return d===null || d>=days; // never called, or not called in N+ days
  }).map(c=>({...c,daysAgo:daysDiff(c.last_call_date)}))
    .sort((a,b)=>{
      const av=a.daysAgo===null?Infinity:a.daysAgo, bv=b.daysAgo===null?Infinity:b.daysAgo;
      return bv-av;
    });

  // Search filter
  const searchEl = document.getElementById(seg==='equity'?'eqnc-search':'mfnc-search');
  const q = (searchEl ? searchEl.value : '').toLowerCase().trim();
  if(q){
    data = data.filter(c=>{
      const name = (c.name||'').toLowerCase();
      const mobile = (c.mobile||'').toLowerCase();
      const rm = (c.rm||'').toLowerCase();
      const code = (seg==='equity'?(c.code||''):(c.pan||'')).toLowerCase();
      return name.includes(q)||mobile.includes(q)||rm.includes(q)||code.includes(q);
    });
  }

  const contId = seg==='equity'?'eqnc-content':'mfnc-content';
  const cont=document.getElementById(contId);
  if(!cont) return;

  // Update count badge
  const countEl = document.getElementById(seg==='equity'?'eqnc-count':'mfnc-count');
  if(countEl) countEl.textContent = data.length ? data.length+' found' : '';

  if(!data.length){
    cont.innerHTML=`<div style="text-align:center;padding:48px;color:var(--green)">✅ No ${seg==='equity'?'clients':'investors'} with ${days}+ days since last call${q?' matching "'+q+'"':''}</div>`;
    return;
  }
  const label = seg==='equity'?'clients':'investors';
  const idLabel = seg==='equity'?'Code':'PAN';
  const ncKey = seg==='equity'?'eqnc':'mfnc';
  data = applySort(data, ncKey, {
    idval:{get:c=>seg==='equity'?c.code:c.pan, type:'str'},
    name:{get:c=>c.name, type:'str'},
    mobile:{get:c=>c.mobile, type:'str'},
    rm:{get:c=>c.rm, type:'str'},
    last_call_date:{get:c=>c.last_call_date, type:'date'},
    daysAgo:{get:c=>c.daysAgo, type:'num'},
    next_call:{get:c=>c.next_call, type:'date'},
  });
  let h=`<p style="margin-bottom:12px;color:var(--gray);font-size:.82rem">${data.length} ${label} with no call in ${days}+ days (or never called)</p>
    <div class="tbl-wrap"><div class="tbl-scroll"><table><thead><tr>
    ${sortTh(idLabel,ncKey,'idval','str',`()=>renderNoCall('${seg}')`)}
    ${sortTh('Name',ncKey,'name','str',`()=>renderNoCall('${seg}')`)}
    ${sortTh('Mobile',ncKey,'mobile','str',`()=>renderNoCall('${seg}')`)}
    ${sortTh('RM',ncKey,'rm','str',`()=>renderNoCall('${seg}')`)}
    ${sortTh('Last Call',ncKey,'last_call_date','date',`()=>renderNoCall('${seg}')`)}
    ${sortTh('Days Since',ncKey,'daysAgo','num',`()=>renderNoCall('${seg}')`)}
    ${sortTh('Next Call',ncKey,'next_call','date',`()=>renderNoCall('${seg}')`)}
    <th>Actions</th>
    </tr></thead><tbody>`;
  data.forEach(c=>{
    const d=c.daysAgo;
    const cls = d===null||d>=180 ? 'row-alert' : d>=90 ? 'row-inactive' : '';
    const dayLabel = d===null ? 'Never' : d+'d';
    const dayColor = d===null||d>=180 ? 'var(--red)' : d>=90 ? 'var(--orange)' : 'var(--gold)';
    h+=`<tr class="${cls}">
      <td>${(seg==='equity'?c.code:c.pan)||'—'}</td>
      <td style="font-weight:600">${c.name}</td>
      <td><a href="tel:${c.mobile}" style="color:var(--navy);text-decoration:none">${c.mobile||'—'}</a></td>
      <td>${c.rm||'—'}</td>
      <td>${fmtDate(c.last_call_date)||'Never'}</td>
      <td style="font-weight:700;color:${dayColor}">${dayLabel}</td>
      <td>${fmtDate(c.next_call)||'—'}</td>
      <td>
        <button class="btn-icon" onclick="editClient('${c.id}','${seg==='equity'?'equity':'mf'}')" title="Edit">✏️</button>
        ${c.mobile?`<a href="https://wa.me/91${c.mobile}" target="_blank" class="btn-icon" title="WhatsApp">💬</a>`:''}
      </td></tr>`;
  });
  h+='</tbody></table></div></div>';
  cont.innerHTML=h;
  }catch(e){
    console.error('renderNoCall error:', e);
    const _errCont = document.getElementById('eqnc-content')||document.getElementById('mfnc-content')||document.getElementById('nt-content')||document.getElementById('sip-table')||document.getElementById('seminars-table')||document.getElementById((arguments[0]==='eqf'?'eqf':'mff')+'-content');
    if(_errCont) _errCont.innerHTML = '<div style="text-align:center;padding:36px;color:var(--red)">⚠️ Error loading data: '+(e.message||e)+'</div>';
  }
}


function renderSip(){
  try{
  const mf=getMyMfClients();
  const sipClients=mf.filter(c=>c.sip_amount>0);
  const totalAUM=mf.reduce((s,c)=>s+(parseFloat(c.aum)||0),0);
  const totalSIP=sipClients.reduce((s,c)=>s+(parseFloat(c.sip_amount)||0),0);
  const totalCount=sipClients.reduce((s,c)=>s+(parseInt(c.sip_count)||0),0);

  document.getElementById('sipStats').innerHTML=
    `<div class="stat-card teal"><div class="stat-n">${sipClients.length}</div><div class="stat-l">SIP Clients</div></div>`+
    `<div class="stat-card gold"><div class="stat-n">₹${fmtNum(totalSIP)}</div><div class="stat-l">Monthly SIP</div></div>`+
    `<div class="stat-card"><div class="stat-n">${totalCount}</div><div class="stat-l">Total SIP Count</div></div>`+
    `<div class="stat-card purple"><div class="stat-n">₹${fmtNum(totalAUM)}</div><div class="stat-l">Total AUM</div></div>`;

  let sipData = applySort(sipClients, 'sip', {
    name:{get:c=>c.name, type:'str'},
    mobile:{get:c=>c.mobile, type:'str'},
    rm:{get:c=>c.rm, type:'str'},
    status:{get:c=>c.status, type:'str'},
    aum:{get:c=>c.aum, type:'num'},
    sip_amount:{get:c=>c.sip_amount, type:'num'},
    sip_count:{get:c=>c.sip_count, type:'num'},
  });
  if(!_sortState.sip || !_sortState.sip.field) sipData = sipData.slice().sort((a,b)=>(b.sip_amount||0)-(a.sip_amount||0));
  let h=`<table><thead><tr>
    ${sortTh('Name','sip','name','str','renderSip')}
    ${sortTh('Mobile','sip','mobile','str','renderSip')}
    ${sortTh('MF RM','sip','rm','str','renderSip')}
    ${sortTh('Status','sip','status','str','renderSip')}
    ${sortTh('AUM','sip','aum','num','renderSip')}
    ${sortTh('SIP/mo','sip','sip_amount','num','renderSip')}
    ${sortTh('SIP Count','sip','sip_count','num','renderSip')}
    <th>Actions</th></tr></thead><tbody>`;
  sipData.forEach(c=>{
    h+=`<tr class="row-mf">
      <td style="font-weight:600">${c.name}</td>
      <td>${c.mobile||'—'}</td>
      <td>${c.rm||'—'}</td>
      <td><span class="badge b-investor">${c.status}</span></td>
      <td>${c.aum?`<a href="#" onclick="openMfAum('${c.id}');return false" style="text-decoration:none;color:var(--navy)" title="Click for Invested / Gain-Loss / XIRR">₹${fmtNum(c.aum)}</a>`:'—'}</td>
      <td style="font-weight:700;color:var(--teal)">₹${fmtNum(c.sip_amount)}</td>
      <td>${sipCntCell(c)}</td>
      <td><button class="btn-icon" onclick="viewClient('${c.id}','mf')">👁</button>
          <button class="btn-icon" onclick="editClient('${c.id}','mf')">✏️</button></td></tr>`;
  });
  h+='</tbody></table>';
  document.getElementById('sip-table').innerHTML=`<div class="tbl-wrap"><div class="tbl-scroll">${h}</div></div>`;
  }catch(e){
    console.error('renderSip error:', e);
    const _errCont = document.getElementById('eqnc-content')||document.getElementById('mfnc-content')||document.getElementById('nt-content')||document.getElementById('sip-table')||document.getElementById('seminars-table')||document.getElementById((arguments[0]==='eqf'?'eqf':'mff')+'-content');
    if(_errCont) _errCont.innerHTML = '<div style="text-align:center;padding:36px;color:var(--red)">⚠️ Error loading data: '+(e.message||e)+'</div>';
  }
}

// ══════════════════════════════════════════
// REPORTS
// ══════════════════════════════════════════
function renderReports(){
  const eqCards=[
    {icon:'👥',title:'RM-wise Client List',desc:'Clients grouped by Equity RM',fn:'rmEqReport'},
    {icon:'✅',title:'Active/Inactive Report',desc:'Trading status breakdown',fn:'statusEqReport'},
    {icon:'📅',title:'Last Trade Date Report',desc:'Clients sorted by last trade',fn:'lastTradeReport'},
    {icon:'⚠️',title:'No Trade — 30 Days',desc:'Clients inactive 30+ days',fn:'noTrade30'},
    {icon:'🔴',title:'No Trade — 60 Days',desc:'Clients inactive 60+ days',fn:'noTrade60'},
    {icon:'⚫',title:'No Trade — 90/180 Days',desc:'Clients inactive 90/180+ days',fn:'noTrade90'},
    {icon:'📞',title:'Follow-up Pending',desc:'Clients with pending follow-ups',fn:'fuPendingEq'},
    {icon:'💰',title:'Revenue Report',desc:'RM-wise brokerage/revenue',fn:'revenueReport'},
    {icon:'🆕',title:'New Business — Demat Accounts',desc:'Demat accounts converted from Leads (month-wise)',fn:'eqNewBusinessReport'},
    {icon:'🚫',title:'DNC List — Equity',desc:'Do Not Call clients (Equity)',fn:'showDncReportEq'},
  ];
  const mfCards=[
    {icon:'🏦',title:'RM-wise Investor List',desc:'Investors grouped by MF RM',fn:'rmMfReport'},
    {icon:'📈',title:'AUM Report',desc:'RM-wise AUM summary',fn:'aumReport'},
    {icon:'🔄',title:'SIP Report',desc:'SIP amount and count by RM',fn:'sipReport'},
    {icon:'🧲',title:'Investor vs Prospect',desc:'Status breakdown',fn:'statusMfReport'},
    {icon:'📞',title:'MF Follow-up Pending',desc:'MF clients with pending calls',fn:'fuPendingMf'},
    {icon:'💼',title:'New Business (Month-wise)',desc:'Lumpsum/SIP/Switch/Resumption entries by month',fn:'newBusinessMonthlyReport'},
    {icon:'🚫',title:'DNC List — MF',desc:'Do Not Call investors (MF)',fn:'showDncReportMf'},
  ];
  const coCards=[
    {icon:'📊',title:'RM Performance',desc:'Combined EQ+MF per RM',fn:'rmPerf'},
    {icon:'🔀',title:'RM Shift History',desc:'Clients moved between RMs — date & RM wise',fn:'rmShiftReport'},
  ];
  document.getElementById('eq-reports').innerHTML=eqCards.map(r=>reportCard(r)).join('');
  document.getElementById('mf-reports').innerHTML=mfCards.map(r=>reportCard(r)).join('');
  document.getElementById('co-reports').innerHTML=coCards.map(r=>reportCard(r)).join('');
}

function reportCard(r){
  return `<div class="report-card" onclick="${r.fn}()">
    <div class="r-icon">${r.icon}</div>
    <h4>${r.title}</h4><p>${r.desc}</p></div>`;
}

let _reportSort = {col:null, dir:1};

function showReport(title, headers, rows){
  currentReportData={title,headers,rows};
  _reportSort = {col:null, dir:1};
  document.getElementById('reportModalTitle').textContent=title;
  renderReportTable();
  document.getElementById('reportModal').classList.add('open');
}

function sortReportCol(colIndex){
  if(_reportSort.col===colIndex) _reportSort.dir = -_reportSort.dir;
  else { _reportSort.col=colIndex; _reportSort.dir=1; }
  renderReportTable();
}

function renderReportTable(){
  let {headers,rows}=currentReportData;
  rows = rows.slice();
  if(_reportSort.col!=null){
    const col=_reportSort.col, dir=_reportSort.dir;
    rows.sort((a,b)=>{
      let va=a[col], vb=b[col];
      const na=parseFloat((va==null?'':va).toString().replace(/[^0-9.\-]/g,''));
      const nb=parseFloat((vb==null?'':vb).toString().replace(/[^0-9.\-]/g,''));
      if(!isNaN(na) && !isNaN(nb) && (va==null||va==='' || /[0-9]/.test(va)) && (vb==null||vb===''||/[0-9]/.test(vb))){
        return dir*(na-nb);
      }
      va=(va==null?'':va).toString().toLowerCase(); vb=(vb==null?'':vb).toString().toLowerCase();
      return dir*va.localeCompare(vb, undefined, {numeric:true});
    });
  }
  let h=`<p style="color:var(--gray);font-size:.8rem;margin-bottom:12px">${rows.length} records</p>`;
  h+=`<div class="tbl-wrap"><div class="tbl-scroll"><table><thead><tr>${headers.map((hd,i)=>{
    const arrow = _reportSort.col===i ? (_reportSort.dir===1?' ▲':' ▼') : '';
    return `<th onclick="sortReportCol(${i})" style="cursor:pointer" title="Click to sort">${hd}${arrow}</th>`;
  }).join('')}</tr></thead><tbody>`;
  rows.forEach(r=>{ h+=`<tr>${r.map(c=>`<td>${c!=null?c:'—'}</td>`).join('')}</tr>`; });
  h+='</tbody></table></div></div>';
  document.getElementById('reportModalBody').innerHTML=h;
}

function exportReportCSV(){
  const {title,headers,rows}=currentReportData;
  if(!headers||!headers.length){ toast('No data to export','error'); return; }
  const moneyRe=/aum|sip|revenue|brokerage|turnover|amount|asset|business|salary|value/i;
  const cols=headers.map(h=>{
    const money=moneyRe.test(h);
    return {header:h, money, align: money?'right':'left',
            color: /status/i.test(h)?dnStatusColor:undefined};
  });
  const cleanRows=rows.map(r=>r.map((c,i)=> cols[i].money ? c : (c==null?'':c)));
  const name=(title||'Report').replace(/[^a-z0-9]+/gi,'_').replace(/^_|_$/g,'').slice(0,40)||'Report';
  dnXlsx(name+'_'+today()+'.xlsx', title||'Report', cols, cleanRows);
  toast('Export done!','success');
}

function rmEqReport(){
  const eq=getActiveEqClients();
  const rms={};
  eq.forEach(c=>{ if(!rms[c.rm]) rms[c.rm]=[]; rms[c.rm].push(c); });
  const rows=[];
  Object.entries(rms).sort().forEach(([rm,cls])=>{
    cls.forEach(c=>rows.push([rm,c.code||'',c.name,c.mobile||'',c.status,c.asset_value?'₹'+fmtNum(c.asset_value):'',fmtDate(c.last_trade_date)||'']));
  });
  showReport('RM-wise Equity Client List',['RM','Code','Name','Mobile','Status','Asset Value','Last Trade'],rows);
}

function statusEqReport(){
  const eq=getActiveEqClients();
  const rows=eq.sort((a,b)=>a.status?.localeCompare(b.status)).map(c=>[c.rm,c.code||'',c.name,c.mobile||'',c.status,fmtDate(c.last_trade_date)||'',daysDiff(c.last_trade_date)||'']);
  showReport('Active/Inactive Report',['RM','Code','Name','Mobile','Status','Last Trade','Days'],rows);
}

function lastTradeReport(){
  const eq=getActiveEqClients().filter(c=>c.last_trade_date).sort((a,b)=>a.last_trade_date.localeCompare(b.last_trade_date));
  const rows=eq.map(c=>[c.rm,c.name,c.mobile||'',fmtDate(c.last_trade_date),daysDiff(c.last_trade_date),c.status,c.asset_value?'₹'+fmtNum(c.asset_value):'']);
  showReport('Last Trade Date Report',['RM','Name','Mobile','Last Trade','Days Ago','Status','Asset'],rows);
}

function noTrade30(){ ntReport(30); }
function noTrade60(){ ntReport(60); }
function noTrade90(){ ntReport(90); }

// ══════════════════════════════════════════
// SQUARE-OFF (T+5) — daily admin upload, RM-filtered view
// Stored as a single array in crm_data/squareoff (overwritten daily → stays light)
// ══════════════════════════════════════════
// Square-off rows carry a DEALER name straight from the uploaded file. That
// name is unreliable — it can be blank, misspelt, or an old/short form that
// doesn't match any RM in the CRM, and then the row falls through to nobody.
// So we resolve each row's RM by CLIENT CODE against eq_clients (the code is
// the one field the broker file always gets right), and only fall back to the
// file's dealer name when the code can't be matched.
// Done here, in the single accessor, so the table, RM pill, RM dropdown,
// filter and Excel export all agree without touching each of them.
function sqCodeRmMap(){
  const m = new Map();
  (DB.get('eq_clients')||[]).forEach(c=>{
    const code = String(c.code||'').trim().toUpperCase();
    const rm   = String(c.rm||'').trim().toUpperCase();
    if(code && rm) m.set(code, rm);
  });
  return m;
}
function getSquareoff(){
  const rows = DB.get('squareoff')||[];
  if(!rows.length) return rows;
  const codeMap = sqCodeRmMap();
  if(!codeMap.size) return rows;                 // clients not synced yet — leave as-is
  // Names that actually exist as an RM in the CRM
  const knownRMs = new Set([...codeMap.values()]);
  return rows.map(r=>{
    const code = String(r.code||'').trim().toUpperCase();
    const byCode = code ? codeMap.get(code) : '';
    if(!byCode) return r;                         // code unknown — keep file's dealer
    const fileDealer = String(r.dealer||'').trim().toUpperCase();
    // Code wins whenever the file's dealer is blank or isn't a real RM.
    if(!fileDealer || !knownRMs.has(fileDealer)) return {...r, dealer:byCode, _rmBy:'code'};
    return r;
  });
}
function sqMyRows(){
  const rows=getSquareoff();
  if(!(CU) ) return [];
  // VIEW scope stays "own clients only" even for RMs granted upload rights
  // (risk_upload / backoffice_access) — that permission lets them upload the
  // office-wide file, but they should still only SEE their own clients here,
  // same as any other RM. Full office view is reserved for actual admin and
  // the standalone Back Office role (who have no "own" clients of their own).
  if(CU.role==='admin' || CU.role==='backoffice') return rows;
  const tempDealers = (typeof getTempAccessDealers==='function') ? getTempAccessDealers('eq') : [];
  const myRMs=[...new Set([...(CU.eq_dealers||[CU.name]),...(CU.mf_dealers||[CU.name]),...tempDealers])].map(d=>String(d||'').trim().toUpperCase()).filter(Boolean);
  return rows.filter(r=>myRMs.includes(String(r.dealer||'').trim().toUpperCase()));
}
function sqFmtD(d){ if(!d) return '—'; const x=new Date(d); return isNaN(x)?String(d):x.toLocaleDateString('en-IN',{day:'2-digit',month:'short'}); }
function sqToISO(v){
  if(v==null || v==='') return '';
  const pad=n=>String(n).padStart(2,'0');
  // 1) Raw Excel serial number (what we get since we read WITHOUT cellDates) —
  //    pure UTC-days arithmetic, zero dependency on browser timezone.
  if(typeof v==='number' && !isNaN(v)){
    const utcDays = Math.floor(v - 25569); // days since Unix epoch (handles 1900 leap-bug correctly)
    const d = new Date(utcDays*86400*1000);
    return `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}`;
  }
  // 2) Already a JS Date object — read it back via UTC getters (matches how it was built)
  if(v instanceof Date){
    if(isNaN(v)) return '';
    return `${v.getUTCFullYear()}-${pad(v.getUTCMonth()+1)}-${pad(v.getUTCDate())}`;
  }
  // 3) Plain text — pull Y-M-D straight out of the string, no Date() timezone parsing
  const s=String(v).trim();
  let m=s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if(m) return `${m[1]}-${pad(m[2])}-${pad(m[3])}`;
  m=s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/); // DD/MM/YYYY
  if(m) return `${m[3]}-${pad(m[2])}-${pad(m[1])}`;
  const d=new Date(s);
  if(!isNaN(d)) return `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}`;
  return s;
}
function sqUpload(input){
  if(!canUploadSquareoff()){ toast('Only admin/back-office/risk-upload RMs can upload','error'); input.value=''; return; }
  const file=input.files[0]; if(!file){ return; }
  const reader=new FileReader();
  reader.onload=function(e){
    try{
      // raw:true — CSV files me SheetJS khud hi date-jaisi text (jaise "03/08/2026")
      // ko number/date bana deta hai apne heuristic se, jo ambiguous DD/MM ko
      // MM/DD samajh leta hai (din <=12 hone par). Isse "3 Aug" chup-chaap "8 Mar"
      // ban jaata tha. raw:true is auto-conversion ko band karta hai, taaki humara
      // apna DD/MM/YYYY parser (sqToISO) neeche asli text par sahi se chale.
      const wb=XLSX.read(e.target.result,{type:'binary',raw:true});
      const out=[];
      wb.SheetNames.forEach(sn=>{
        const ws=wb.Sheets[sn];
        const arr=XLSX.utils.sheet_to_json(ws,{defval:''});
        arr.forEach(r=>{
          // Needle-priority matching: har needle ke liye pehle exact header,
          // phir partial. Jo pehla needle non-empty value de, wahi lete hain.
          // (Column order nahi, needle order decide karta hai — isse
          //  "Debit for Actual Shortage" jaise galat column nahi pakdte.)
          const _keys = Object.keys(r);
          const _norm = k => String(k).toLowerCase().replace(/[^a-z0-9]/g,'');
          const _val = k => { const v = r[k]; return (v===null||v===undefined||String(v).trim()==='') ? null : v; };
          const pick=(...needles)=>{
            let firstHit = null;
            for(const n of needles){
              for(const k of _keys){ if(_norm(k)===n){ const v=_val(k); if(v!==null) return v; if(firstHit===null) firstHit=''; } }
              for(const k of _keys){ const hk=_norm(k); if(hk!==n && hk.includes(n)){ const v=_val(k); if(v!==null) return v; if(firstHit===null) firstHit=''; } }
            }
            return firstHit===null ? '' : firstHit;
          };
          const name=String(pick('name','clientname','custname')||'').replace(/\s+/g,' ').trim();
          const code=String(pick('clientcode','ucccode','ucc','clientid','code')||'').trim();
          // Dealer khali ho to RM / Sales Person naam fallback (dusre office ki
          // file me 'Dealer Name' blank hota hai aur naam 'R M' column me hota hai)
          const dealer=String(pick('dealername','dealer','rm','relationshipmanager','salespersname','salesperson')||'').trim();
          // Debit: 'Bal More than 5/90 Days' hi asli square-off amount hai —
          // 'Debit for Actual Shortage' (aksar 0) se pehle isko dekho
          const debit=parseFloat(String(pick('sqoffdebit','balmorethan5','balmorethan','sqoffamount','sqoff','debitamount','debit')||'0').replace(/[,\s]/g,''))||0;
          // t5 date — 'T-5/90 AcBal' (amount) se bachne ke liye bare 't5' nahi
          const t5=sqToISO(pick('t590daydate','f590date','t590date','t5date','90daydate','tminus5','t5day'));
          // sq date — 'Square Off Days' (count) se bachne ke liye bare 'squareoff' nahi
          const sqDate=sqToISO(pick('squareoffdate','sqroffdate','sqoffdate','sqrdate','sqdate'));
          if(!code && !name) return;
          // ₹100 se neeche (99.99 tak) ka debit ignore — chhoti amount pe square-off nahi
          if(debit < 100) return;
          out.push({code,name,dealer,debit,t5,sqDate});
        });
      });
      if(!out.length){ toast('No rows found in file','error'); input.value=''; return; }
      // MERGE mode: purana data mitta nahi. Key = Client Code + Square-off Date.
      // Same key wali row nayi file se overwrite ho jati hai, baaki add hoti hain.
      // Isse alag-alag office ki files alag-alag upload ki ja sakti hain.
      // Sirf beeti hui sq-date wali rows (aaj se pehle) hata di jati hain, taaki
      // purani dates jama na hon.
      const keyOf = r => String(r.code||r.name||'').trim().toUpperCase()+'|'+(r.sqDate||'');
      const merged = new Map();
      (DB.get('squareoff')||[]).forEach(r=>merged.set(keyOf(r), r));   // old rows first
      out.forEach(r=>merged.set(keyOf(r), r));                          // new rows on top
      const todayISO = today();
      const finalRows = [...merged.values()].filter(r=>!r.sqDate || r.sqDate >= todayISO);
      const dropped = merged.size - finalRows.length;
      DB.set('squareoff', finalRows);
      const uploadedDates = [...new Set(out.map(r=>r.sqDate).filter(Boolean))];
      let msg = 'Square-off merged — file se '+out.length+' rows ('+uploadedDates.map(sqFmtD).join(', ')+'), total '+finalRows.length+' clients';
      if(dropped) msg += ', '+dropped+' with an old date removed';
      toast(msg,'success');
      renderSquareoff(); updateBadges(); sqUpdateBell();
    }catch(err){ console.error(err); toast('Upload failed: '+err.message,'error'); }
    input.value='';
  };
  reader.readAsBinaryString(file);
}
function sqClear(){
  if(!canUploadSquareoff()) return;
  if(!confirm('Clear all square-off data?')) return;
  DB.set('squareoff', []); renderSquareoff(); updateBadges(); toast('Square-off cleared','success');
}
function renderSquareoff(){
  const canUpload=canUploadSquareoff();
  document.querySelectorAll('.admin-only-sq').forEach(el=>el.style.display=canUpload?'inline-flex':'none');

  let rows=sqMyRows();
  const search=((document.getElementById('sq-search')||{value:''}).value||'').toLowerCase().trim();

  // Full office-wide VIEW (see + filter by every RM) is reserved for actual
  // admin and the standalone Back Office role. RMs granted upload rights
  // (risk_upload / backoffice_access) can upload the whole file but their own
  // view here stays scoped to their own clients — sqMyRows() already enforces
  // that, so this flag only controls the "All RMs" dropdown behaviour.
  const isFullView = !!(CU && (CU.role==='admin' || CU.role==='backoffice'));

  // RM dropdown: full-view users always see it (all RMs in data). Everyone
  // else sees it only when their visible data spans more than one RM (own +
  // a temp-access absent RM), so they can pick "just mine" vs "the absent RM's".
  const rmSel=document.getElementById('sq-rm');
  const myRMsInData=[...new Set(rows.map(r=>String(r.dealer||'').trim().toUpperCase()).filter(Boolean))].sort();
  const showRmSel = isFullView || myRMsInData.length>1;
  if(rmSel) rmSel.style.display = showRmSel ? '' : 'none';
  const rmF=(document.getElementById('sq-rm')||{value:''}).value;

  if(rmSel && showRmSel){
    const prev=rmSel.value;
    const optList = isFullView
      ? [...new Set(getSquareoff().map(r=>String(r.dealer||'').trim().toUpperCase()).filter(Boolean))].sort()
      : myRMsInData;
    rmSel.innerHTML='<option value="">All RMs</option>'+optList.map(r=>`<option${r===prev?' selected':''}>${r}</option>`).join('');
  }

  if(search) rows=rows.filter(r=>(r.name||'').toLowerCase().includes(search)||(r.code||'').toLowerCase().includes(search));
  if(showRmSel && rmF) rows=rows.filter(r=>String(r.dealer||'').trim().toUpperCase()===rmF);

  const tbl=document.getElementById('sq-table'); if(!tbl) return;
  const stripElEmpty=document.getElementById('sq-stat-strip');
  if(!getSquareoff().length){
    tbl.innerHTML=`<tr><td style="text-align:center;padding:32px;color:var(--gray)">No square-off file uploaded yet.${canUpload?" Upload today's file above.":''}</td></tr>`;
    if(stripElEmpty) stripElEmpty.innerHTML='';
    return;
  }
  if(!rows.length){
    tbl.innerHTML=`<tr><td style="text-align:center;padding:32px;color:var(--gray)">🎉 No square-off clients for you right now.</td></tr>`;
    if(stripElEmpty) stripElEmpty.innerHTML='';
    return;
  }

  // Distinct dates across visible rows, earliest (most urgent) first
  const dateKeys=[...new Set(rows.map(r=>r.sqDate).filter(Boolean))].sort();
  const todayStr=today();
  const palette=['#dc2626','#ea580c','#ca8a04','#0891b2','#7c3aed','#be185d'];
  const dateColor={}; dateKeys.forEach((d,i)=>dateColor[d]=palette[i%palette.length]);

  // Pivot: one row per client code (fallback name if code blank)
  const byClient={};
  rows.forEach(r=>{
    const key=(r.code||'').trim() || ('N:'+(r.name||'').trim().toLowerCase());
    if(!byClient[key]) byClient[key]={code:r.code,name:r.name,dealer:r.dealer,vals:{}};
    byClient[key].vals[r.sqDate] = (byClient[key].vals[r.sqDate]||0) + (r.debit||0);
    // keep freshest name/dealer if it varies between files
    byClient[key].name = r.name || byClient[key].name;
    byClient[key].dealer = r.dealer || byClient[key].dealer;
  });
  let clients=Object.values(byClient);
  // Sort by total debit desc
  clients.forEach(c=>{ c.total=Object.values(c.vals).reduce((s,v)=>s+v,0); });
  clients.sort((a,b)=>b.total-a.total);

  const total=clients.reduce((s,c)=>s+c.total,0);
  const cntEl=document.getElementById('sq-count');
  if(cntEl) cntEl.textContent='';

  // RM pill colors — fixed premium deep tones (muted, no neon). Each RM gets its
  // own distinct shade; unknown names fall back to a deep hashed palette.
  const rmFixed={
    'ROHIT':'#4338CA',   // indigo
    'RIYA':'#BE185D',    // rose
    'RAJU':'#9F1239',    // wine
    'BHARAT':'#047857',  // emerald
    'KHOKHAN':'#0F766E', // teal
    'PUJA':'#0369A1',    // deep sky
    'KOMAL':'#7E22CE',   // plum
    'MEGHA':'#B45309',   // bronze
    'ANJALI':'#475569'   // slate
  };
  const rmFallback=['#334155','#1E4620','#4C1D95','#831843','#155E75','#7C2D12','#365314','#5B21B6','#134E4A','#701A75'];
  const rmColor={};
  const rmColorFor=name=>{
    const key=String(name||'—').trim().toUpperCase();
    if(!rmColor[key]){
      if(rmFixed[key]){ rmColor[key]=rmFixed[key]; }
      else{ let h=0; for(let i=0;i<key.length;i++) h=(h*31+key.charCodeAt(i))>>>0; rmColor[key]=rmFallback[h%rmFallback.length]; }
    }
    return rmColor[key];
  };

  // Stat strip — clean cards with a thin top accent bar instead of flooded colour
  let stripHtml=`<div class="sq-stat-chip" style="--chip-accent:#132039">
      <span class="n">${clients.length}</span><span class="l">Clients</span></div>
    <div class="sq-stat-chip" style="--chip-accent:#0d9488">
      <span class="n">₹${Math.round(total).toLocaleString('en-IN')}</span><span class="l">Total Debit</span></div>`;
  dateKeys.forEach(dk=>{
    const dTotal=clients.reduce((s,c)=>s+(c.vals[dk]||0),0);
    const dCount=clients.filter(c=>c.vals[dk]!=null).length;
    const isToday=dk===todayStr;
    stripHtml+=`<div class="sq-stat-chip" style="--chip-accent:${dateColor[dk]}">
      <span class="n" style="color:${dateColor[dk]}">₹${Math.round(dTotal).toLocaleString('en-IN')}</span>
      <span class="l">${sqFmtD(dk)}${isToday?' · TODAY':''} — ${dCount} clients</span></div>`;
  });
  const stripEl=document.getElementById('sq-stat-strip');
  if(stripEl) stripEl.innerHTML=stripHtml;

  // Header stays on-brand navy for every column; each date column gets a thin
  // coloured top accent bar + coloured dot/amount instead of a flooded tint.
  let html='<thead><tr><th style="min-width:100px">Client Code</th><th style="min-width:180px">Client Name</th><th style="min-width:110px;text-align:center">RM</th>';
  dateKeys.forEach(dk=>{
    const isToday=dk===todayStr;
    const dTotal=clients.reduce((s,c)=>s+(c.vals[dk]||0),0);
    const dCount=clients.filter(c=>c.vals[dk]!=null).length;
    html+=`<th style="min-width:160px;padding-top:8px;padding-bottom:8px;text-align:center;
        box-shadow:inset 0 3px 0 ${dateColor[dk]}">
      <div class="sq-date-pill" style="justify-content:center;color:#fff"><span class="sq-date-dot" style="background:${dateColor[dk]}"></span>${sqFmtD(dk)}${isToday?' · TODAY':''}</div>
      <div style="font-size:1.02rem;font-weight:800;color:#fff;margin-top:2px;letter-spacing:.2px;text-align:center">₹${Math.round(dTotal).toLocaleString('en-IN')}</div>
      <div style="font-size:.68rem;font-weight:500;color:#8fa0bd;text-transform:none;letter-spacing:0;margin-top:1px;text-align:center">${dCount} clients</div>
    </th>`;
  });
  html+='</tr></thead><tbody>';
  clients.forEach(c=>{
    const rc=rmColorFor(c.dealer);
    html+=`<tr>
      <td><span class="sq-code">${c.code||'—'}</span></td>
      <td><span class="sq-name">${c.name||'—'}</span></td>
      <td style="text-align:center"><span class="sq-rm-pill" style="color:${rc};background:${rc}14;border-color:${rc}">${c.dealer||'—'}</span></td>`;
    dateKeys.forEach(dk=>{
      const v=c.vals[dk];
      if(v==null){
        html+=`<td style="text-align:center"><span class="sq-amt-empty">—</span></td>`;
      }else{
        const big=v>=50000;
        html+=`<td style="text-align:center;${big?'background:#fdf6e8':''}">
          <span class="sq-amt${big?' sq-big':''}">₹${v.toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2})}</span></td>`;
      }
    });
    html+=`</tr>`;
  });
  html+='</tbody>';
  tbl.innerHTML=html;
}
function sqUpdateBell(){
  const rows=sqMyRows();
  const n=new Set(rows.map(r=>(r.code||'').trim()||('N:'+(r.name||'').trim().toLowerCase()))).size;
  const mBadge=document.getElementById('mnav-sq-badge');
  if(mBadge){ mBadge.textContent=n>99?'99+':n; mBadge.style.display=n>0?'':'none'; }
  const bell=document.getElementById('dnSqBell'); if(!bell) return;
  const badge=document.getElementById('dnSqBellCount');
  if(badge){ badge.textContent=n>99?'99+':n; }
  bell.style.display='inline-flex';
}
function sqExport(){
  const isAdmin=canUploadSquareoff();
  let rows=sqMyRows();
  if(!rows.length){ toast('No data to export','error'); return; }
  const dateKeys=[...new Set(rows.map(r=>r.sqDate).filter(Boolean))].sort();
  const byClient={};
  rows.forEach(r=>{
    const key=(r.code||'').trim() || ('N:'+(r.name||'').trim().toLowerCase());
    if(!byClient[key]) byClient[key]={code:r.code,name:r.name,dealer:r.dealer,vals:{}};
    byClient[key].vals[r.sqDate]=(byClient[key].vals[r.sqDate]||0)+(r.debit||0);
    byClient[key].name=r.name||byClient[key].name;
    byClient[key].dealer=r.dealer||byClient[key].dealer;
  });
  let clients=Object.values(byClient);
  clients.forEach(c=>{ c.total=Object.values(c.vals).reduce((s,v)=>s+v,0); });
  clients.sort((a,b)=>b.total-a.total);

  const cols=[{header:'Client Code',width:14},{header:'Client Name',width:32},{header:'RM',width:12}]
    .concat(dateKeys.map(dk=>({header:sqFmtD(dk),money:true,width:15})));
  const data=clients.map(c=>{
    const row=[c.code,c.name,c.dealer];
    dateKeys.forEach(dk=>row.push(c.vals[dk]!=null?c.vals[dk]:''));
    return row;
  });
  const totalRow=['','','TOTAL'].concat(dateKeys.map(dk=>clients.reduce((s,c)=>s+(c.vals[dk]||0),0)));
  dnXlsx('Square-off_'+today()+'.xlsx','Square-off (T+5)',cols,data,totalRow);
}
function ntReport(d){
  const rows=getActiveEqClients().filter(c=>daysDiff(c.last_trade_date)>=d)
    .sort((a,b)=>daysDiff(b.last_trade_date)-daysDiff(a.last_trade_date))
    .map(c=>[c.rm,c.code||'',c.name,c.mobile||'',fmtDate(c.last_trade_date)||'Never',daysDiff(c.last_trade_date)||'',c.asset_value?'₹'+fmtNum(c.asset_value):'']);
  showReport(`No-Trade ${d}+ Days Report`,['RM','Code','Name','Mobile','Last Trade','Days','Asset Value'],rows);
}

function fuPendingEq(){
  const rows=getActiveEqClients().filter(c=>c.followup_status==='Pending'||c.next_call).sort((a,b)=>(a.next_call||'').localeCompare(b.next_call||''))
    .map(c=>[c.rm,c.name,c.mobile||'',fmtDate(c.last_call_date)||'',fmtDate(c.next_call)||'',c.followup_status||'',c.remarks||'']);
  showReport('Follow-up Pending — Equity',['RM','Name','Mobile','Last Call','Next Call','Status','Remarks'],rows);
}

function revenueReport(){
  const eq=getActiveEqClients();
  const rms={};
  eq.forEach(c=>{ if(!rms[c.rm]) rms[c.rm]={count:0,rev:0,aum:0}; rms[c.rm].count++; rms[c.rm].rev+=(c.revenue||0); rms[c.rm].aum+=(c.asset_value||0); });
  const rows=Object.entries(rms).sort().map(([rm,d])=>[rm,d.count,'₹'+fmtNum(d.aum),'₹'+fmtNum(d.rev)]);
  showReport('Revenue Report',['RM','Client Count','Total AUM','Total Revenue'],rows);
}

function rmMfReport(){
  const mf=getMyMfClients();
  const rows=mf.sort((a,b)=>a.rm?.localeCompare(b.rm)).map(c=>[c.rm,c.name,c.mobile||'',c.status,c.aum?'₹'+fmtNum(c.aum):'',c.sip_amount?'₹'+fmtNum(c.sip_amount):'',c.sip_count||'']);
  showReport('RM-wise MF Investor List',['RM','Name','Mobile','Status','AUM','SIP/mo','SIP Count'],rows);
}

function aumReport(){
  const mf=getMyMfClients();
  const rms={};
  mf.forEach(c=>{ if(!rms[c.rm]) rms[c.rm]={count:0,aum:0,inv:0}; rms[c.rm].count++; rms[c.rm].aum+=(c.aum||0); rms[c.rm].inv+=(c.status==='Investor'?1:0); });
  const rows=Object.entries(rms).sort((a,b)=>b[1].aum-a[1].aum).map(([rm,d])=>[rm,d.count,d.inv,'₹'+fmtNum(d.aum)]);
  showReport('AUM Report',['RM','Total Clients','Investors','Total AUM'],rows);
}

function sipReport(){
  const mf=getMyMfClients();
  const rms={};
  mf.forEach(c=>{ if(!rms[c.rm]) rms[c.rm]={count:0,sip:0,sipC:0}; rms[c.rm].count++; rms[c.rm].sip+=(c.sip_amount||0); rms[c.rm].sipC+=(c.sip_count||0); });
  const rows=Object.entries(rms).sort((a,b)=>b[1].sip-a[1].sip).map(([rm,d])=>[rm,d.count,'₹'+fmtNum(d.sip),d.sipC]);
  showReport('SIP Report',['RM','Total Investors','Monthly SIP','Total SIP Count'],rows);
}

function statusMfReport(){
  const rows=getMyMfClients().sort((a,b)=>a.status?.localeCompare(b.status)).map(c=>[c.rm,c.name,c.mobile||'',c.status,c.aum?'₹'+fmtNum(c.aum):'',c.sip_amount?'₹'+fmtNum(c.sip_amount):'']);
  showReport('Investor vs Prospect',['RM','Name','Mobile','Status','AUM','SIP/mo'],rows);
}

function fuPendingMf(){
  const rows=getMyMfClients().filter(c=>c.followup_status==='Pending'||c.next_call).sort((a,b)=>(a.next_call||'').localeCompare(b.next_call||''))
    .map(c=>[c.rm,c.name,c.mobile||'',fmtDate(c.last_call_date)||'',fmtDate(c.next_call)||'',c.followup_status||'',c.remarks||'']);
  showReport('Follow-up Pending — MF',['RM','Name','Mobile','Last Call','Next Call','Status','Remarks'],rows);
}

let _bizReportEntries = [];
let _bizReportSort = {col:null, dir:1};

const BIZ_REPORT_HEADERS = ['Month','RM','Client','Fund Name','Date','Lumpsum','SIP','SIP Stop','Switch','STP','Redemption','SWP','Additional Buy','SIP Bounce Buy','SIP Pause','Remarks'];
const BIZ_REPORT_TYPECOL = {Lumpsum:0, SIP:1, 'SIP Stop':2, Switch:3, STP:4, Redemption:5, SWP:6, 'Additional Buy':7, 'SIP Bounce Buy':8, 'SIP Pause':9};

function bizReportRowCells(e){
  const amts = new Array(8).fill('');
  const idx = BIZ_REPORT_TYPECOL[e.type];
  if(idx!==undefined) amts[idx] = '₹'+fmtNum(e.amount);
  const ym = (e.date||'').slice(0,7);
  const [y,m]=ym.split('-');
  const monthLabel = m ? MN[parseInt(m)]+'-'+y : '';
  return [monthLabel, e.rm||'—', e.client_name||'—', e.fund_name||'—', fmtDate(e.date)||'—', ...amts, e.remarks||'—'];
}

function newBusinessMonthlyReport(){
  const biz = DB.get('mf_business');
  let entries = Array.isArray(biz) ? biz : (biz?.entries || []);
  // Staff sees only their own entries. Admin and anyone with MF Desk access see everyone's.
  if(CU.role!=='admin' && !hasMfDeskAccess(CU)){
    const myNames = (CU.mf_dealers||[CU.name]).map(d=>d.trim().toUpperCase());
    entries = entries.filter(e=>myNames.includes((e.rm||'').trim().toUpperCase()) || e.created_by===CU.name);
  }
  _bizReportEntries = entries;
  _bizReportSort = {col:null, dir:1};
  document.getElementById('reportModalTitle').textContent='New Business (Month-wise)';
  document.getElementById('reportModal').classList.add('open');
  renderBizReportTable();
}

function sortBizReportCol(colIndex){
  if(_bizReportSort.col===colIndex) _bizReportSort.dir = -_bizReportSort.dir;
  else { _bizReportSort.col=colIndex; _bizReportSort.dir=1; }
  renderBizReportTable();
}

function renderBizReportTable(){
  let entries = _bizReportEntries.slice();
  entries.sort((a,b)=>(b.date||'').localeCompare(a.date||'')); // default: most recent first

  if(_bizReportSort.col!=null){
    const col=_bizReportSort.col, dir=_bizReportSort.dir;
    entries.sort((a,b)=>{
      const ca=bizReportRowCells(a)[col], cb=bizReportRowCells(b)[col];
      const na=parseFloat((ca||'').toString().replace(/[^0-9.\-]/g,''));
      const nb=parseFloat((cb||'').toString().replace(/[^0-9.\-]/g,''));
      if(!isNaN(na) && !isNaN(nb) && /[0-9]/.test(ca||'') && /[0-9]/.test(cb||'')) return dir*(na-nb);
      return dir*(ca||'').toString().toLowerCase().localeCompare((cb||'').toString().toLowerCase(), undefined, {numeric:true});
    });
  }

  // CSV export uses the same sorted entries (no action buttons in export)
  currentReportData = { headers: BIZ_REPORT_HEADERS, rows: entries.map(e=>bizReportRowCells(e)) };

  let h=`<p style="color:var(--gray);font-size:.8rem;margin-bottom:12px">${entries.length} records</p>`;
  const tblHeaders=[...BIZ_REPORT_HEADERS,'Cross-Check','Status','Actions'];
  h+=`<div class="tbl-wrap"><div class="tbl-scroll"><table><thead><tr>${tblHeaders.map((hd,i)=>{
    if(i>=BIZ_REPORT_HEADERS.length) return `<th>${hd}</th>`;
    const arrow = _bizReportSort.col===i ? (_bizReportSort.dir===1?' ▲':' ▼') : '';
    return `<th onclick="sortBizReportCol(${i})" style="cursor:pointer" title="Click to sort">${hd}${arrow}</th>`;
  }).join('')}</tr></thead><tbody>`;

  if(!entries.length){
    h+=`<tr><td colspan="${tblHeaders.length}" style="text-align:center;padding:24px;color:#bbb">No business entries yet</td></tr>`;
  }

  entries.forEach(e=>{
    const cells = bizReportRowCells(e);
    const status = e.status||'Pending';
    const canRemark = canAddCrossRemark(e);
    const remarkCell = e.cross_remark
      ? `<div style="font-size:.78rem;color:var(--navy)">${escapeHtml(e.cross_remark)}</div><div style="font-size:.68rem;color:var(--gray);margin-top:2px">— ${escapeHtml(e.cross_remark_by||'')}, ${fmtDate(e.cross_remark_at)}</div>${canRemark?`<span class="btn-icon" style="cursor:pointer;font-size:.72rem;color:var(--teal)" onclick="addCrossRemark('${e.id}')">✏️ Edit</span>`:''}${CU.role==='admin'?` <span class="btn-icon" style="cursor:pointer;font-size:.72rem;color:var(--red)" onclick="clearCrossRemark('${e.id}')">🗑️ Clear</span>`:''}`
      : (canRemark ? `<span class="btn-icon" style="cursor:pointer;font-size:.78rem;color:var(--teal)" onclick="addCrossRemark('${e.id}')">💬 Rmk</span>` : '<span style="color:var(--gray);font-size:.78rem">—</span>');
    const canEdit = CU.role==='admin' || e.created_by===CU.name || hasMfDeskAccess(CU);
    h+=`<tr>${cells.map(c=>`<td>${c!=null&&c!==''?c:'—'}</td>`).join('')}`;
    h+=`<td style="min-width:140px">${remarkCell}</td>`;
    h+=`<td>${bizStatusBadge(status)}${status==='Declined'&&e.decline_reason?`<div style="font-size:.7rem;color:var(--red);margin-top:2px">${escapeHtml(e.decline_reason)}</div>`:''}</td>`;
    h+=`<td style="white-space:nowrap">
      ${CU.role==='admin'&&status!=='Approved'?`<button class="btn-icon" onclick="approveBusinessEntry('${e.id}')" title="Approve" style="color:var(--green)">✅</button>`:''}
      ${CU.role==='admin'&&status!=='Declined'?`<button class="btn-icon" onclick="declineBusinessEntry('${e.id}')" title="Decline" style="color:var(--red)">❌</button>`:''}
      ${CU.role==='admin'&&status!=='Pending'?`<button class="btn-icon" onclick="markPendingBusinessEntry('${e.id}')" title="Mark Pending" style="color:var(--gray)">↩️</button>`:''}
      ${canEdit?`<button class="btn-icon" onclick="editBusinessEntry('${e.id}')" title="Edit">✏️</button>`:''}
      ${CU.role==='admin'?`<button class="btn-icon" onclick="deleteBusinessEntry('${e.id}')" title="Delete" style="color:var(--red)">🗑️</button>`:''}
    </td>`;
    h+='</tr>';
  });
  h+='</tbody></table></div></div>';
  document.getElementById('reportModalBody').innerHTML=h;
}

function editBusinessEntry(id){
  const entries = getMfBizEntries();
  const e = entries.find(x=>x.id===id);
  if(!e) return;
  if(CU.role!=='admin' && e.created_by!==CU.name && !hasMfDeskAccess(CU)){ toast('You can only edit entries you created','error'); return; }
  currentBusinessTarget = {id: e.client_id, name: e.client_name};
  editingBusinessId = id;
  document.getElementById('businessModalTitle').textContent = 'Edit Business — '+e.client_name;

  // Client + RM reassignment row: Admin only. MF Desk access does NOT extend
  // to changing who a transaction is attributed to — that stays exactly the
  // same as before this feature: Amount/Type/Target Scheme/Fund Name/Dates
  // are open to edit, but Client and RM are locked/hidden for everyone
  // except Admin.
  const clientWrap=document.getElementById('biz_client_wrap');
  if(clientWrap) clientWrap.style.display = CU.role==='admin' ? '' : 'none';
  const rmNoteWrap=document.getElementById('biz_rm_note_wrap');
  if(rmNoteWrap) rmNoteWrap.style.display = CU.role==='admin' ? '' : 'none';
  const mfNoteWrap = document.getElementById('biz_mfdesk_note_wrap');
  if(mfNoteWrap) mfNoteWrap.style.display = (hasMfDeskAccess(CU) && CU.role!=='admin') ? '' : 'none';
  const cSel=document.getElementById('biz_client_selected'); if(cSel) cSel.value=e.client_name||'';
  populateBizRmDropdown(e.rm||'');
  const cSearch=document.getElementById('biz_client_search'); if(cSearch) cSearch.value='';
  const cResults=document.getElementById('biz_client_results'); if(cResults){ cResults.style.display='none'; cResults.innerHTML=''; }

  document.getElementById('biz_type').value = e.type;
  document.getElementById('biz_amount').value = e.amount;
  const bfEl=document.getElementById('biz_fund'); if(bfEl) bfEl.value = e.fund_name||'';
  const btEl=document.getElementById('biz_target_fund'); if(btEl) btEl.value = e.target_scheme||'';
  const fpEl=document.getElementById('biz_firstpay'); if(fpEl) fpEl.checked = !!(e.first_payment);
  const sdEl=document.getElementById('biz_startdate'); if(sdEl) sdEl.value = e.start_date||'';
  toggleBizTarget();
  document.getElementById('biz_date').value = e.date;

  // Nothing is locked/disabled anymore for MF Desk — full edit rights on
  // every field except Delete, which stays Admin-only (its button is simply
  // never rendered for non-admins, elsewhere).
  ['biz_type','biz_amount','biz_target_fund'].forEach(fid=>{
    const el=document.getElementById(fid);
    if(!el) return;
    el.disabled = false; el.style.background=''; el.style.cursor='';
  });

  document.getElementById('businessModal').classList.add('open');
}

function deleteBusinessEntry(id){
  if(CU.role!=='admin') return;
  if(!confirm('Delete this business entry? This cannot be undone.')) return;
  const biz = DB.get('mf_business');
  const entries = (Array.isArray(biz) ? biz : (biz?.entries||[])).filter(e=>e.id!==id);
  const eqEntries = Array.isArray(biz) ? [] : (biz?.eq_entries||[]);
  DB.set('mf_business', {entries, eq_entries: eqEntries});
  toast('Entry deleted','success');
  newBusinessMonthlyReport();
}

function eqNewBusinessReport(){
  const biz = DB.get('mf_business');
  const entries = Array.isArray(biz) ? [] : (biz?.eq_entries||[]);
  const isAdmin = CU && CU.role==='admin';

  const filtered = isAdmin ? entries : entries.filter(e=>(e.rm||'').toLowerCase()===(CU.name||'').toLowerCase());

  if(!filtered.length){
    toast('No Demat account entries found. Convert Leads.','error');
    return;
  }

  // Month-wise grouping
  const monthAgg = {};
  filtered.forEach(e=>{
    const month = (e.date||'').slice(0,7); // YYYY-MM
    if(!monthAgg[month]) monthAgg[month]={month, count:0, rms:{}};
    monthAgg[month].count++;
    const rm = e.rm||'—';
    if(!monthAgg[month].rms[rm]) monthAgg[month].rms[rm]=0;
    monthAgg[month].rms[rm]++;
  });

  // Flat rows: Date | Client Name | Client Code | RM | Type | Remarks | Status
  const rows = filtered
    .slice()
    .sort((a,b)=>(b.date||'').localeCompare(a.date||''))
    .map(e=>[
      fmtDate(e.date)||e.date||'—',
      e.client_name||'—',
      e.client_code||'—',
      e.rm||'—',
      e.type||'Open Demat Account',
      e.remarks||'—',
      e.status||'Pending'
    ]);

  showReport(
    `New Business — Demat Accounts (${filtered.length} entries)`,
    ['Date','Client Name','Client Code','RM','Type','Remarks','Status'],
    rows
  );
}

function rmPerf(){
  const eq=getActiveEqClients(), mf=getMyMfClients();
  const rms={};
  eq.forEach(c=>{ if(!rms[c.rm]) rms[c.rm]={eqC:0,mfC:0,aum:0,sip:0,rev:0}; rms[c.rm].eqC++; rms[c.rm].rev+=(c.revenue||0); });
  mf.forEach(c=>{ if(!rms[c.rm]) rms[c.rm]={eqC:0,mfC:0,aum:0,sip:0,rev:0}; rms[c.rm].mfC++; rms[c.rm].aum+=(c.aum||0); rms[c.rm].sip+=(c.sip_amount||0); });
  const rows=Object.entries(rms).sort().map(([rm,d])=>[rm,d.eqC,d.mfC,'₹'+fmtNum(d.aum),'₹'+fmtNum(d.sip),'₹'+fmtNum(d.rev)]);
  showReport('RM Performance Report',['RM','EQ Clients','MF Investors','Total AUM','Monthly SIP','Revenue'],rows);
}

// Whenever a client's RM changes — via the bulk "Set RM"/"Unmap" bar (BULK.setRm/
// unmap in dninvest-app-2.js, logs type:'bulk_rm_update') or a single client Edit
// form where the RM dropdown was changed (saveClient, logs type:'edit' with a 'rm'
// entry in .changes) — an activity_log entry already gets written. This report
// just reads those entries back out, filtered to RM changes only, so every shift
// is visible date-wise and RM-wise without digging through the full Activity Log.
function rmShiftReport(){
  const logs = DB.get('activity_logs')||[];
  const isAdmin = CU && CU.role==='admin';
  const myName = (CU && CU.name||'').toLowerCase();
  const segLbl = s => (s==='eq'||s==='equity') ? 'Equity' : (s==='mf' ? 'MF' : (s||'—'));
  let entries = [];
  logs.forEach(l=>{
    if(l.type!=='bulk_rm_update' && l.type!=='edit') return;
    const ch = (l.changes||[]).find(c=>c.field==='rm');
    if(!ch) return;
    entries.push({
      date: (l.date||'').slice(0,10), // ISO — used for sort/filter; formatted only at display time
      client: l.client_name||'—',
      seg: segLbl(l.seg),
      from: ch.old||'(unmapped)',
      to: ch.new||'(unmapped)',
      by: l.by||'—'
    });
  });
  if(!isAdmin){
    entries = entries.filter(e=> e.from.toLowerCase()===myName || e.to.toLowerCase()===myName);
  }
  if(!entries.length){ toast('No RM shift history found yet','error'); return; }
  entries.sort((a,b)=> b.date.localeCompare(a.date));
  const rows = entries.map(e=>[fmtDate(e.date)||e.date, e.client, e.seg, e.from, e.to, e.by]);
  showReport(
    `RM Shift History (${entries.length} changes)`,
    ['Date','Client Name','Segment','From RM','To RM','Changed By'],
    rows
  );
}

// ══════════════════════════════════════════
// EXPORT
// ══════════════════════════════════════════
function exportCSV(seg){
  const bseg = seg==='equity' ? 'eq' : 'mf';
  const key  = seg==='equity' ? 'eq_clients' : 'mf_clients';
  let clients, onlySel=false;
  if(typeof BULK!=='undefined' && BULK.sel && BULK.sel[bseg] && BULK.sel[bseg].size>0){
    const ids=BULK.sel[bseg];
    clients=(DB.get(key)||[]).filter(c=>ids.has(c.id));
    onlySel=true;
  } else {
    clients=seg==='equity'?getActiveEqClients():getMyMfClients();
  }
  if(!clients.length){ toast('No data to export','error'); return; }
  let cols, rows, title, fname;
  if(seg==='equity'){
    cols=[
      {header:'Code',width:12},{header:'Name',width:22},{header:'Mobile',width:13},
      {header:'Email',width:24},{header:'RM',width:14},{header:'Status',width:12,align:'center',color:dnStatusColor},
      {header:'Asset Value',width:14,money:true},{header:'Revenue',width:13,money:true},
      {header:'Last Trade Date',width:14},{header:'Last Trade Month',width:14},
      {header:'Last Call Date',width:14},{header:'Next Call Date',width:14},
      {header:'Follow-up Status',width:16},{header:'Remarks',width:26}
    ];
    rows=clients.map(c=>[c.code||'',c.name,c.mobile||'',c.email||'',c.rm,c.status,c.asset_value||0,c.revenue||0,c.last_trade_date||'',c.last_trade_month||'',c.last_call_date||'',c.next_call||'',c.followup_status||'',c.remarks||'']);
    title='Equity Clients'; fname='equity_clients_';
  } else {
    cols=[
      {header:'Name',width:22},{header:'Mobile',width:13},{header:'Email',width:24},
      {header:'RM',width:14},{header:'Status',width:12,align:'center',color:dnStatusColor},
      {header:'AUM',width:14,money:true},{header:'SIP Amount',width:13,money:true},{header:'SIP Count',width:10,num:true},
      {header:'Last Invest Date',width:14},{header:'Last Call Date',width:14},
      {header:'Next Call Date',width:14},{header:'Follow-up Status',width:16},{header:'Remarks',width:26}
    ];
    rows=clients.map(c=>[c.name,c.mobile||'',c.email||'',c.rm,c.status,c.aum||0,c.sip_amount||0,c.sip_count||0,c.last_invest_date||'',c.last_call_date||'',c.next_call||'',c.followup_status||'',c.remarks||'']);
    title='MF Investors'; fname='mf_investors_';
  }
  dnXlsx(fname+today()+'.xlsx', title+' — '+today(), cols, rows);
  toast(onlySel?`Exported ${clients.length} selected`:'Export done!','success');
}

// ══════════════════════════════════════════
// ADMIN
// ══════════════════════════════════════════
// Admin: force a user out of the CRM right now (e.g. they left on leave with the
// browser still open) — their next real-time sync tick logs them out automatically,
// and their HR attendance "out" time gets stamped at THIS moment, not whenever
// they'd otherwise have manually logged out.
async function forceLogoutUser(userId){
  const users = DB.get('users')||[];
  const u = users.find(x=>x.id===userId);
  if(!u) return;
  if(!confirm(`Log ${u.name} out of the CRM now?\n\nTheir HR "Out Time" will also be saved with the current time.`)) return;

  try{
    const docRef = fdb.collection('crm_data').doc('force_logout');
    const snap = await docRef.get();
    const flags = (snap.exists && snap.data() && snap.data().data) ? snap.data().data : {};
    flags[u.username] = Date.now();
    await docRef.set({ data:flags, updated:new Date().toISOString() });
  }catch(e){ toast('Force-logout flag could not be saved: '+e.message, 'error'); return; }

  // Stamp HR out-time right now, same name-matching rule as check-in.
  try{
    const HR_NAMES = ['Puja','Rohit','Raju','Komal','Riya','Bharat','Khokhan','Megha','Anjali'];
    const rawName = String(u.name || u.username || '').trim();
    const match = HR_NAMES.find(n =>
      n.toLowerCase() === rawName.toLowerCase() ||
      n.toLowerCase() === rawName.split(' ')[0].toLowerCase() ||
      n.toLowerCase() === String(u.username||'').toLowerCase());
    const hrName = match || rawName;
    const td = today();
    const outDate = new Date();
    const outTime = String(outDate.getHours()).padStart(2,'0')+':'+String(outDate.getMinutes()).padStart(2,'0');
    const attRef = fdb.collection('hr_data').doc('attendance');
    await fdb.runTransaction(async (tx)=>{
      const doc = await tx.get(attRef);
      let latest = (doc.exists && doc.data() && doc.data().data) ? doc.data().data : {};
      if(!latest[hrName]) latest[hrName]=[];
      const idx = latest[hrName].findIndex(r=>r.date===td);
      if(idx>=0) latest[hrName][idx].out = outTime;
      else latest[hrName].push({date:td, in:'', out:outTime, status:'Present'});
      tx.set(attRef, {data:latest, updated:new Date().toISOString()});
    });
  }catch(e){ console.log('[FORCE LOGOUT] HR out-time save error:', e.message); }

  toast(`✅ ${u.name} has been logged out, out-time saved`, 'success');
}

function renderAdmin(){
  const users=DB.get('users')||[];
  let h=users.map(u=>`
    <div class="user-card">
      <div class="user-avatar">${u.name[0].toUpperCase()}</div>
      <div class="user-info">
        <div class="user-name">${u.name} ${u.left_company?'<span style="color:#fff;background:#7c2d12;font-size:.72rem;padding:1px 6px;border-radius:6px;margin-left:4px">🚶 LEFT COMPANY</span>':''}${!u.left_company&&!u.active?'<span style="color:var(--red);font-size:.75rem">(Inactive)</span>':''}${u.role!=='admin'&&!u.left_company&&u.manualOverride?'<span style="color:var(--orange);font-size:.72rem;margin-left:4px">⚙️ Manual</span>':''}${u.role!=='admin'&&!u.left_company&&!u.manualOverride?'<span style="color:var(--gray);font-size:.72rem;margin-left:4px">🕐 Auto</span>':''}</div>
        <div class="user-role">@${u.username} · ${u.role==='admin'?'Admin':u.role==='mf_desk'?'MF Desk':u.role==='backoffice'?'Back Office':'RM'}${u.role==='rm'&&u.mf_desk_access?' <span style="color:var(--teal);font-weight:600">+ MF Desk access</span>':''}${u.role==='rm'&&u.risk_upload?' <span style="color:#d97706;font-weight:600">+ Risk/Square-off</span>':''}${u.role==='rm'&&u.backoffice_access?' <span style="color:#7c3aed;font-weight:600">+ Back Office</span>':''}</div>
        <div class="user-role" style="margin-top:2px">${u.mobile?'📱 '+u.mobile:'<span style="color:var(--gray);font-style:italic">No mobile</span>'}</div>
        <div class="user-role" style="margin-top:2px">${u.email?'✉️ '+u.email:''}</div>
        ${u.role==='rm'?`<div class="user-role" style="margin-top:2px">${u.pin?'🔢 PIN set':'🔢 No PIN'}</div>`:''}
        ${(()=>{ const today=new Date().toISOString().split('T')[0]; const ta=(u.tempAccess||[]).filter(t=>t.expiry>=today); if(!ta.length) return ''; const users2=DB.get('users')||[]; const names=ta.map(t=>{ const nm=(users2.find(x=>x.id===t.absentUserId)||{}).name||''; return t.expiry===today ? nm : `${nm} (until ${t.expiry})`; }).join(', '); return `<div class="user-role" style="margin-top:2px;color:var(--teal)">🔄 Temp: ${names}</div>`; })()}
        <div class="user-segs">${(u.segments||[]).map(s=>`<span class="badge ${s==='equity'?'b-eq':'b-mf'}">${s==='equity'?'Equity':'MF'}</span>`).join('')}</div>
      </div>
      <div style="display:flex;gap:6px;flex-wrap:wrap">
        <button class="btn btn-outline" onclick="editUser('${u.id}')">Edit</button>
        ${u.role!=='admin'&&!u.left_company?`<button class="btn" onclick="forceLogoutUser('${u.id}')" style="background:#d97706;color:#fff;border:none" title="Force logout now — also stamps HR out-time">🚪 Force Logout</button>`:''}
        ${u.role!=='admin'&&!u.left_company?`<button class="btn" onclick="openTempAccessModal('${u.id}')" style="background:var(--teal);color:#fff;border:none" title="Temp Access">🔄 Temp</button>`:''}
        ${u.role!=='admin'&&!u.left_company?`<button class="btn ${u.active===false?'btn-success':'btn-danger'}" onclick="toggleUser('${u.id}')">${u.active===false?'Activate':'Deactivate'}</button>`:''}
        ${u.role!=='admin'&&!u.left_company?`<button class="btn" onclick="markAsLeft('${u.id}')" style="background:#7c2d12;color:#fff;border:none" title="Mark this employee as left the company — blocks login, keeps all records">🚶 Mark as Left</button>`:''}
        ${u.role!=='admin'&&u.left_company?`<button class="btn btn-success" onclick="reactivateUser('${u.id}')" title="Reactivate — restores normal login/auto-schedule">↩️ Reactivate</button>`:''}
        <button class="btn btn-danger" onclick="deleteUser('${u.id}')">Delete</button>
      </div>
    </div>`).join('');
  document.getElementById('user-list').innerHTML=h;
}

function openAddUser(){
  document.getElementById('userModalTitle').textContent='Add User';
  document.getElementById('userModalBody').innerHTML=userForm(null);
  document.getElementById('userModal').classList.add('open');
}

function editUser(id){
  const user=(DB.get('users')||[]).find(u=>u.id===id);
  if(!user) return;
  document.getElementById('userModalTitle').textContent='Edit User: '+user.name;
  document.getElementById('userModalBody').innerHTML=userForm(user);
  document.getElementById('userModal').classList.add('open');
}

function userForm(u){
  return `
    <div class="form-row">
      <div class="form-field"><label>Full Name *</label><input id="uf_name" value="${u?.name||''}"></div>
      <div class="form-field"><label>Username *</label><input id="uf_uname" value="${u?.username||''}" ${u?'readonly':''}></div>
    </div>
    <div class="form-row">
      <div class="form-field"><label>Mobile Number</label><input id="uf_mobile" type="tel" value="${u?.mobile||''}" placeholder="10 digit mobile" maxlength="10"></div>
      <div class="form-field"><label>Email</label><input id="uf_email" type="email" value="${u?.email||''}" placeholder="email@example.com"></div>
    </div>
    <div class="form-row">
      <div class="form-field"><label>Password *</label><input id="uf_pwd" type="password" placeholder="${u?'Leave blank to keep current':'Enter password'}"></div>
    </div>
    <div class="form-row">
      <div class="form-field"><label>Role</label><select id="uf_role" onchange="onUserRoleChange(this)">
        <option ${u?.role==='admin'?'selected':''} value="admin">Admin</option>
        <option ${u?.role==='rm'||!u?'selected':''} value="rm">RM</option>
        <option ${u?.role==='mf_desk'?'selected':''} value="mf_desk">MF Desk (MF Transactions only)</option>
        <option ${u?.role==='backoffice'?'selected':''} value="backoffice">Back Office (Equity/MF Import + Risk/Square-off upload only, no RM work)</option>
      </select></div>
      <div class="form-field" id="uf_pin_wrap" style="${(!u||u.role==='rm')?'':'display:none'}">
        <label>4-Digit PIN (RM only)</label>
        <input id="uf_pin" type="password" maxlength="4" inputmode="numeric" pattern="[0-9]{4}" value="" placeholder="${u?'Leave blank to keep current':'Enter 4-digit PIN'}">
      </div>
    </div>
    <div class="form-field" id="uf_mfdesk_wrap" style="${(!u||u.role==='rm')?'':'display:none'}">
      <label style="display:flex;align-items:center;gap:8px;text-transform:none;letter-spacing:0;font-size:.88rem;font-weight:600">
        <input type="checkbox" id="uf_mfdesk_access" ${u?.mf_desk_access?'checked':''}>
        Also give MF Desk access (can view/enter MF Transactions for any RM, in addition to their own)
      </label>
    </div>
    <div class="form-field" id="uf_risk_wrap" style="${(!u||u.role==='rm')?'':'display:none'}">
      <label style="display:flex;align-items:center;gap:8px;text-transform:none;letter-spacing:0;font-size:.88rem;font-weight:600">
        <input type="checkbox" id="uf_risk_upload" ${u?.risk_upload?'checked':''}>
        Allow Broker RMS Risk file + Square-off (T+5) upload (office-wide daily broker files)
      </label>
    </div>
    <div class="form-field" id="uf_backoffice_wrap" style="${(!u||u.role==='rm')?'':'display:none'}">
      <label style="display:flex;align-items:center;gap:8px;text-transform:none;letter-spacing:0;font-size:.88rem;font-weight:600">
        <input type="checkbox" id="uf_backoffice_access" ${u?.backoffice_access?'checked':''}>
        Also give Back Office access (Equity/MF Import Excel maps to ALL clients office-wide, in addition to their own RM work)
      </label>
    </div>
    <div class="form-field"><label>Segments Access</label>
      <div style="display:flex;gap:12px;margin-top:6px">
        <label style="display:flex;align-items:center;gap:6px;font-size:.88rem;text-transform:none;letter-spacing:0">
          <input type="checkbox" id="uf_eq" ${(u?.segments||[]).includes('equity')||!u?'checked':''}>Equity</label>
        <label style="display:flex;align-items:center;gap:6px;font-size:.88rem;text-transform:none;letter-spacing:0">
          <input type="checkbox" id="uf_mf" ${(u?.segments||[]).includes('mf')||!u?'checked':''}>Mutual Fund</label>
      </div>
    </div>
    <input type="hidden" id="uf_id" value="${u?.id||''}">`;
}

async function saveUser(){
  const id=document.getElementById('uf_id').value;
  const name=document.getElementById('uf_name').value.trim();
  const uname=document.getElementById('uf_uname').value.trim().toLowerCase();
  const pwd=document.getElementById('uf_pwd').value;
  const role=document.getElementById('uf_role').value;
  const pinVal=document.getElementById('uf_pin')?.value||'';
  const mfDeskAccess = role==='rm' && !!document.getElementById('uf_mfdesk_access')?.checked;
  const riskUpload = role==='rm' && !!document.getElementById('uf_risk_upload')?.checked;
  const backofficeAccess = role==='rm' && !!document.getElementById('uf_backoffice_access')?.checked;
  if(role==='rm' && pinVal && !/^[0-9]{4}$/.test(pinVal)){
    toast('PIN must be 4 digits (0-9)','error'); return;
  }
  const segs=[];
  if(document.getElementById('uf_eq').checked) segs.push('equity');
  if(document.getElementById('uf_mf').checked) segs.push('mf');

  if(!name||!uname){ toast('Name and username required','error'); return; }
  if(!segs.length){ toast('Select at least one segment','error'); return; }
  if(!id && !pwd){ toast('Password required for new user','error'); return; }

  const r = await DB.mutateUsers(users=>{
    if(id){
      const idx=users.findIndex(u=>u.id===id);
      if(idx>=0){
        const mob=document.getElementById('uf_mobile').value.trim();
        const eml=document.getElementById('uf_email').value.trim();
        users[idx]={...users[idx],name,role,segments:segs,mobile:mob,email:eml,mf_desk_access:mfDeskAccess,risk_upload:riskUpload,backoffice_access:backofficeAccess};
        if(pwd) users[idx].password=pwd;
        if(role==='rm' && pinVal) users[idx].pin=pinVal;
      }
    } else {
      if(users.find(u=>u.username===uname)){ toast('Username already exists','error'); return false; }
      const mob2=document.getElementById('uf_mobile').value.trim();
      const eml2=document.getElementById('uf_email').value.trim();
      const newUser={id:uid(),username:uname,password:pwd,name,role,segments:segs,mobile:mob2,email:eml2,active:true,mf_desk_access:mfDeskAccess,risk_upload:riskUpload,backoffice_access:backofficeAccess};
      if(role==='rm' && pinVal) newUser.pin=pinVal;
      users.push(newUser);
    }
  });
  if(!r.ok || r.aborted) return;
  closeModal('userModal');
  toast(id?'User updated!':'User added!','success');
  renderAdmin();
  populateRmDropdowns();
}

async function toggleUser(id){
  const users=DB.get('users')||[];
  const u = users.find(x=>x.id===id);
  if(!u) return;
  const newActive = !u.active;
  // Manual override: set manualOverride flag with timestamp
  // Auto-schedule will resume at next scheduled boundary
  await DB.mutateUsers(users=>{
    const idx=users.findIndex(u=>u.id===id);
    if(idx<0) return false;
    users[idx].active = newActive;
    users[idx].manualOverride = true;
    users[idx].manualOverrideTime = new Date().toISOString();
    users[idx].manualOverrideActive = newActive;
  });
  renderAdmin();
  toast((newActive ? '✅ Activated' : '🔴 Deactivated') + ' — will resume at the next auto-schedule boundary', 'success');
}

// "Mark as Left" is a distinct, sticky state from the daily auto-schedule
// Deactivate above. It permanently blocks login and is never touched by
// runAutoSchedule/runLateAbsentCheck (unlike manualOverride, which clears
// itself once the day/window boundary passes). Clients, history, and
// incentive records are untouched — only login/CRM access is locked.
async function markAsLeft(id){
  const users=DB.get('users')||[];
  const u = users.find(x=>x.id===id);
  if(!u) return;
  if(!confirm(`Mark ${u.name} as left the company? Their login will be blocked immediately. Client/history records are kept safe — you can Reactivate anytime.`)) return;
  await DB.mutateUsers(users=>{
    const idx=users.findIndex(u=>u.id===id);
    if(idx<0) return false;
    users[idx].left_company = true;
    users[idx].left_company_date = new Date().toISOString();
    users[idx].active = false;
    users[idx].manualOverride = true; // extra belt-and-suspenders lock, left_company is the real guard
    users[idx].manualOverrideTime = new Date().toISOString();
    users[idx].manualOverrideActive = false;
  });
  renderAdmin();
  toast(`🚶 ${u.name} marked as left. Login blocked.`, 'success');
}

async function reactivateUser(id){
  const users=DB.get('users')||[];
  const u = users.find(x=>x.id===id);
  if(!u) return;
  await DB.mutateUsers(users=>{
    const idx=users.findIndex(u=>u.id===id);
    if(idx<0) return false;
    users[idx].left_company = false;
    users[idx].left_company_date = null;
    users[idx].manualOverride = false; // let auto-schedule take over again normally
    users[idx].active = false; // stays inactive until the next auto-schedule boundary, same as a fresh user
  });
  renderAdmin();
  toast(`↩️ ${u.name} reactivated — back on normal auto-schedule`, 'success');
}

async function deleteUser(id){
  const users=DB.get('users')||[];
  const idx=users.findIndex(u=>u.id===id);
  if(idx<0) return;
  const u=users[idx];

  if(u.role==='admin' && users.filter(x=>x.role==='admin').length<=1){
    toast('Cannot delete the only Admin account','error');
    return;
  }

  // Warn if this RM still has clients assigned (clients themselves are NOT deleted —
  // they keep their existing RM name on the record, just no longer have a logged-in user).
  const eqCount=(DB.get('eq_clients')||[]).filter(c=>(c.rm||'').trim().toUpperCase()===(u.name||'').trim().toUpperCase()).length;
  const mfCount=(DB.get('mf_clients')||[]).filter(c=>(c.rm||'').trim().toUpperCase()===(u.name||'').trim().toUpperCase()).length;
  let msg='Delete '+u.name+' ('+u.username+')?\n\nThis cannot be undone.';
  if(eqCount||mfCount){
    msg+='\n\n⚠️ This RM still has '+eqCount+' Equity and '+mfCount+' MF client(s) assigned. Their client records will NOT be deleted, but reassign clients to another RM first if needed.';
  }
  if(!confirm(msg)) return;

  await DB.mutateUsers(users=>{
    const i=users.findIndex(x=>x.id===id);
    if(i<0) return false;
    users.splice(i,1);
  });
  renderAdmin();
  populateRmDropdowns();
  toast(u.name+' has been deleted','success');
}

async function fixRmCasing(){
  if(CU.role!=='admin'){ toast('Only Admin can do this','error'); return; }
  if(!confirm('This will scan all Equity & MF clients and leads, and fix any RM names that are stored with inconsistent capitalization (e.g. "riya" -> "Riya"), merging them into one entry on dashboards & reports.\n\nThis only changes how names are capitalized — no client data is deleted. Continue?')) return;

  let changedCount=0;

  for(const key of ['eq_clients','mf_clients','leads']){
    const list = DB.get(key)||[];
    let anyChange=false;
    list.forEach(c=>{
      if(c.rm){
        const fixed=normRm(c.rm);
        if(fixed!==c.rm){ c.rm=fixed; anyChange=true; changedCount++; }
      }
    });
    if(anyChange) await DB.set(key, list);
  }

  // Seminars: attendee rm field too
  const seminars = DB.get('seminars')||[];
  let seminarChanged=false;
  seminars.forEach(s=>{
    (s.attendees||[]).forEach(a=>{
      if(a.rm){
        const fixed=normRm(a.rm);
        if(fixed!==a.rm){ a.rm=fixed; seminarChanged=true; changedCount++; }
      }
    });
  });
  if(seminarChanged) await DB.set('seminars', seminars);

  toast(changedCount>0 ? `✅ Fixed casing on ${changedCount} record(s)! Refreshing...` : '✅ All RM names were already consistent — nothing to fix.', 'success');
  if(changedCount>0) setTimeout(()=>{ refreshDash(); }, 800);
}

// ── Auto Schedule ──
// Mon-Fri: 8:00 AM – 7:00 PM IST (active window)
// Saturday: 9:00 AM – 3:00 PM IST (shorter window, same as HR attendance rule)
// Sunday: always inactive (no active window)
// Stock Exchange holidays (from HR Portal's holiday calendar, hr_data/holidays):
// treated exactly like Sunday — RMs auto-deactivate on both portals for that day.
function getISTHour(){
  // IST = UTC+5:30
  const now = new Date();
  const utc = now.getTime() + now.getTimezoneOffset()*60000;
  const ist = new Date(utc + 5.5*3600000);
  return ist.getHours() + ist.getMinutes()/60;
}

function getISTDayOfWeek(){
  // 0=Sunday .. 6=Saturday, computed in IST (not local browser time)
  const now = new Date();
  const utc = now.getTime() + now.getTimezoneOffset()*60000;
  const ist = new Date(utc + 5.5*3600000);
  return ist.getDay();
}

// Today's date (YYYY-MM-DD) in IST — used to match the HR holiday calendar.
function getISTDateStr(){
  const now = new Date();
  const utc = now.getTime() + now.getTimezoneOffset()*60000;
  const ist = new Date(utc + 5.5*3600000);
  return ist.getFullYear()+'-'+String(ist.getMonth()+1).padStart(2,'0')+'-'+String(ist.getDate()).padStart(2,'0');
}

// Cached holiday set from HR Portal (hr_data/holidays). Refreshed periodically
// so a holiday declared in HR auto-flows here without a CRM reload.
let _holidaySet = new Set();
async function refreshHolidaySet(){
  try{
    const holDoc = await fdb.collection('hr_data').doc('holidays').get();
    const holidays = (holDoc.exists && holDoc.data() && holDoc.data().data) ? holDoc.data().data : [];
    _holidaySet = new Set(holidays.map(h=>h.date));
  }catch(e){ /* keep last known set on error */ }
}
function isHolidayToday(){ return _holidaySet.has(getISTDateStr()); }

// Returns {start, end} active-window boundaries (in IST decimal hours) for
// the given day-of-week. Sunday (and holidays, handled in runAutoSchedule)
// return null (no active window at all).
function getActiveWindowForDay(dow){
  if(dow===0) return null;           // Sunday — always inactive
  if(dow===6) return {start:9, end:15}; // Saturday — 9:00 AM to 3:00 PM
  return {start:8, end:19};          // Mon-Fri — 8:00 AM to 7:00 PM
}

// TRUE if the current IST moment lies inside today's working window.
// Login uses this directly instead of trusting the stored `active` flag,
// which can be stale if the auto-schedule hasn't run yet on this device.
function isWithinActiveWindowNow(){
  const win = isHolidayToday() ? null : getActiveWindowForDay(getISTDayOfWeek());
  if(!win) return false;
  const h = getISTHour();
  return h >= win.start && h < win.end;
}

// Pure/read-only version of the auto-schedule decision — used to cheaply
// check locally (no Firestore hit) whether this minute's tick would
// actually change anything, before paying for a transaction read+write.
function _wouldAutoScheduleChange(users, istHour, shouldBeActive){
  return (users||[]).some(u=>{
    if(u.role === 'admin') return false;
    if(u.left_company) return false; // departed employees — never touched by auto-schedule
    if(u.manualOverride){
      const overrideTime = new Date(u.manualOverrideTime);
      const utc = overrideTime.getTime() + overrideTime.getTimezoneOffset()*60000;
      const istO = new Date(utc + 5.5*3600000);
      const overrideHour = istO.getHours() + istO.getMinutes()/60;
      const overrideDow = istO.getDay();
      const overrideWin = isHolidayToday() ? null : getActiveWindowForDay(overrideDow);
      const dayChanged = getISTDateStr() !== (isNaN(istO) ? '' :
        istO.getFullYear()+'-'+String(istO.getMonth()+1).padStart(2,'0')+'-'+String(istO.getDate()).padStart(2,'0'));
      let crossedBoundary = dayChanged;
      if(!dayChanged && overrideWin){
        crossedBoundary = (overrideHour < overrideWin.start && istHour >= overrideWin.start) ||
                           (overrideHour >= overrideWin.start && overrideHour < overrideWin.end && istHour >= overrideWin.end);
      }
      return crossedBoundary;
    }
    return (u.active !== false) !== shouldBeActive;
  });
}

function runAutoSchedule(){
  const istHour = getISTHour();
  const dow = getISTDayOfWeek();
  // Holiday → behave exactly like Sunday (no active window, RMs deactivate).
  const win = isHolidayToday() ? null : getActiveWindowForDay(dow);
  const shouldBeActive = win ? (istHour >= win.start && istHour < win.end) : false;

  // Cheap local-only check FIRST — this runs every minute in every open tab
  // of every user, so touching Firestore here unconditionally (which the
  // transaction-safe rewrite did) meant a fresh document read every single
  // minute all day, for everyone — the #1 driver behind a sudden jump in the
  // Firebase bill. A schedule boundary is only actually crossed a handful of
  // times a day, so almost every one of these ticks needs zero Firestore
  // contact at all.
  const localUsers = DB.get('users')||[];
  if(!_wouldAutoScheduleChange(localUsers, istHour, shouldBeActive)) return;

  DB.mutateUsers(users=>{
    let changed = false;
    users.forEach((u, idx) => {
      if(u.role === 'admin') return; // admins skip
      if(u.left_company) return; // departed employees — never touched by auto-schedule
      if(u.manualOverride){
        // Check if next scheduled boundary has passed → clear override
        const overrideTime = new Date(u.manualOverrideTime);
        const utc = overrideTime.getTime() + overrideTime.getTimezoneOffset()*60000;
        const istO = new Date(utc + 5.5*3600000);
        const overrideHour = istO.getHours() + istO.getMinutes()/60;
        const overrideDow = istO.getDay();
        const overrideWin = isHolidayToday() ? null : getActiveWindowForDay(overrideDow);
        // Has the day changed since the override, or has the active window
        // (start/end) for today been crossed since the override was set?
        const dayChanged = getISTDateStr() !== (isNaN(istO) ? '' :
          istO.getFullYear()+'-'+String(istO.getMonth()+1).padStart(2,'0')+'-'+String(istO.getDate()).padStart(2,'0'));
        let crossedBoundary = dayChanged;
        if(!dayChanged && overrideWin){
          crossedBoundary = (overrideHour < overrideWin.start && istHour >= overrideWin.start) ||
                             (overrideHour >= overrideWin.start && overrideHour < overrideWin.end && istHour >= overrideWin.end);
        }
        if(crossedBoundary){
          users[idx].manualOverride = false;
          users[idx].active = shouldBeActive;
          changed = true;
        }
        return; // still in manual override, don't change
      }
      // Auto mode
      if((u.active !== false) !== shouldBeActive){
        users[idx].active = shouldBeActive;
        changed = true;
      }
    });
    if(!changed) return false; // nothing to change — skip the write entirely
  }).then(r=>{ if(r.ok && !r.aborted) renderAdmin && renderAdmin(); });
}

// Auto-schedule check every minute (cheap — see _wouldAutoScheduleChange
// above, only touches Firestore when a boundary is actually crossed).
// Holiday calendar itself is refreshed separately, every 15 min — a holiday
// is a rare, planned event, so it doesn't need minute-level freshness, and
// re-reading that doc every single minute in every open tab all day was
// pure waste on top of the auto-schedule cost.
setInterval(runAutoSchedule, 60000);
setInterval(refreshHolidaySet, 15*60000);

// ── Late-Login Auto-Absent (Mon-Fri: 10:00 AM, Saturday: 10:30 AM) ──
// On working days (same Mon-Sat window as the auto-schedule, no Sunday/holiday
// handling needed since CRM is closed for RMs then anyway), if an RM has not
// logged in by 10:00 AM IST, they are auto-deactivated AND marked "Absent" in
// the HR Portal's attendance for the day. If they log in later that day, their
// status flips to "Late" and they're reactivated immediately (see
// recordHrAttendanceOnCrmLogin). Runs from whichever app (CRM or HR Portal) is
// opened first — both share the same Firestore project.
// Per-tab flag (resets on reload, not persisted) — once a run finds nothing
// left to check for today, later minute-ticks in THIS tab skip Firestore
// entirely instead of re-fetching holidays+attendance every minute for the
// rest of the day. (A user's own login is handled by the normal login flow,
// not this check, so once nobody is left unresolved there's truly nothing
// more for this function to do until the date changes.)
let _lateAbsentResolvedForDate = null;
async function runLateAbsentCheck(){
  try{
    if(typeof fdb==='undefined') return;
    const istHour = getISTHour();
    const dow = getISTDayOfWeek();
    if(dow===0) return; // Sunday — skip
    const absentCutoff = (dow===6) ? 10.75 : 10; // Saturday=10:45 AM, Mon-Fri=10:00 AM
    if(istHour < absentCutoff) return;

    const td = today();
    if(_lateAbsentResolvedForDate===td) return; // already confirmed fully resolved this session
    if(isHolidayToday()){ _lateAbsentResolvedForDate=td; return; } // uses the shared _holidaySet — no separate Firestore read needed here

    const HR_NAMES = ['Puja','Rohit','Raju','Komal','Riya','Bharat','Khokhan','Megha','Anjali'];

    const attDocRef = fdb.collection('hr_data').doc('attendance');
    const attSnap = await attDocRef.get();
    const attData = (attSnap.exists && attSnap.data() && attSnap.data().data) ? attSnap.data().data : {};

    let usersChanged=false;
    const usersList = DB.get('users') || [];
    const idsToMarkAbsent = []; // collected during the loop, applied via a single safe mutateUsers() at the end
    let anyStillUnresolved = false;

    for(const u of usersList){
      if(u.role==='admin') continue;
      if(u.left_company) continue; // departed employees — never touched by late-absent check
      if(u.lateAbsentMarked===td) continue; // already handled today, don't repeat

      const rawName = String(u.name || u.username || '').trim();
      const hrName = HR_NAMES.find(n =>
        n.toLowerCase() === rawName.toLowerCase() ||
        n.toLowerCase() === rawName.split(' ')[0].toLowerCase() ||
        n.toLowerCase() === String(u.username||'').toLowerCase()
      ) || rawName;

      const hasLoggedInToday = (attData[hrName]||[]).some(r=>r.date===td);
      if(hasLoggedInToday) continue; // already logged in today, leave alone

      // Not logged in by 10 AM — deactivate + mark Absent
      idsToMarkAbsent.push(u.id);
      usersChanged = true;
      anyStillUnresolved = true; // this write could fail (see catch below) — worth re-checking next minute

      try{
        await fdb.runTransaction(async (tx)=>{
          const doc = await tx.get(attDocRef);
          let latest = (doc.exists && doc.data() && doc.data().data) ? doc.data().data : {};
          if(!latest[hrName]) latest[hrName]=[];
          const aidx = latest[hrName].findIndex(r=>r.date===td);
          const record = {date:td, in:'', out:'', status:'Absent'};
          if(aidx>=0) latest[hrName][aidx]=record; else latest[hrName].push(record);
          tx.set(attDocRef, {data:latest, updated:new Date().toISOString()});
        });
      }catch(e){ console.log('Late-absent attendance write error:', hrName, e); }
    }

    if(usersChanged){
      await DB.mutateUsers(fresh=>{
        let changed=false;
        idsToMarkAbsent.forEach(id=>{
          const idx=fresh.findIndex(x=>x.id===id);
          if(idx<0 || fresh[idx].lateAbsentMarked===td) return; // already marked (e.g. by another tab) — don't redo
          fresh[idx].active = false;
          fresh[idx].manualOverride = false; // this is an auto action, not a manual one
          fresh[idx].lateAbsentMarked = td;
          changed=true;
        });
        if(!changed) return false;
      });
      renderAdmin && renderAdmin();
    }
    if(!anyStillUnresolved) _lateAbsentResolvedForDate = td; // nothing left to check today — skip Firestore for the rest of the day in this tab
  }catch(e){
    console.log('runLateAbsentCheck error:', e);
  }
}
setInterval(runLateAbsentCheck, 60000);
runLateAbsentCheck(); // also run once immediately on load


// ══════════════════════════════════════════
// ANNOUNCEMENT
// ══════════════════════════════════════════
let _annImageData; // undefined = unchanged, '' = removed, dataURL = new image

function renderAnnouncementAdmin(){
  const ann = DB.get('announcement');
  _annImageData = undefined;
  const txt=document.getElementById('ann-text');
  const prev=document.getElementById('ann-img-preview');
  const meta=document.getElementById('ann-meta');
  if(!txt) return;
  txt.value = ann?.text || '';

  // Populate RM checkbox list
  const users=(DB.get('users')||[]).filter(u=>u.active!==false);
  const listEl=document.getElementById('ann-users-list');
  const selectedUsers = (ann?.target?.type==='users') ? (ann.target.usernames||[]) : [];
  listEl.innerHTML = users.map(u=>`
    <label style="display:flex;align-items:center;gap:5px;font-size:.85rem;font-weight:400;text-transform:none;letter-spacing:0">
      <input type="checkbox" class="ann-user-chk" value="${u.username}" ${selectedUsers.includes(u.username)?'checked':''}> ${u.name}
    </label>`).join('');

  // Set target select
  const targetSel=document.getElementById('ann-target');
  let targetVal='all';
  if(ann?.target?.type==='segment') targetVal='seg-'+ann.target.segment;
  else if(ann?.target?.type==='users') targetVal='users';
  targetSel.value=targetVal;
  onAnnTargetChange();

  if(ann?.image){
    prev.innerHTML = `<div class="ann-imgwrap"><img src="${ann.image}"><button class="ann-rm" onclick="removeAnnouncementImage()" title="Remove image">×</button></div>`;
  } else {
    prev.innerHTML = '';
  }
  if(ann?.date){
    let tgtLabel='All RMs';
    if(ann.target?.type==='segment') tgtLabel = ann.target.segment==='equity'?'Equity Team':'Mutual Fund Team';
    else if(ann.target?.type==='users') tgtLabel = (ann.target.usernames||[]).map(un=>{const u=users.find(x=>x.username===un); return u?u.name:un;}).join(', ') || '—';
    meta.textContent = `Last sent: ${fmtDate(ann.date)} by ${ann.by||'Admin'} → ${tgtLabel}`;
  } else {
    meta.textContent = 'No announcement currently active.';
  }
  document.getElementById('ann-img-input').value='';
}

function onAnnTargetChange(){
  const v=document.getElementById('ann-target').value;
  document.getElementById('ann-users-wrap').style.display = (v==='users')?'':'none';
}

function handleAnnouncementImage(event){
  const file = event.target.files[0];
  if(!file) return;
  if(!file.type.startsWith('image/')){ toast('Please select an image file','error'); return; }
  const reader = new FileReader();
  reader.onload = function(e){
    const img = new Image();
    img.onload = function(){
      const MAX = 700;
      let w = img.width, h = img.height;
      if(w > MAX || h > MAX){
        if(w > h){ h = Math.round(h * MAX / w); w = MAX; }
        else { w = Math.round(w * MAX / h); h = MAX; }
      }
      const canvas = document.createElement('canvas');
      canvas.width = w; canvas.height = h;
      const ctx = canvas.getContext('2d');
      ctx.drawImage(img, 0, 0, w, h);
      _annImageData = canvas.toDataURL('image/jpeg', 0.7);
      document.getElementById('ann-img-preview').innerHTML =
        `<div class="ann-imgwrap"><img src="${_annImageData}"><button class="ann-rm" onclick="removeAnnouncementImage()" title="Remove image">×</button></div>`;
    };
    img.src = e.target.result;
  };
  reader.readAsDataURL(file);
}

function removeAnnouncementImage(){
  _annImageData = '';
  document.getElementById('ann-img-preview').innerHTML='';
  document.getElementById('ann-img-input').value='';
}

// ══════════════════════════════════════════
// SPECIAL OFFER — poster image (stored as base64 in Firestore,
// same pattern as the announcement image — no external storage needed)
// ══════════════════════════════════════════
let _offerImageData; // undefined = unchanged, '' = removed, dataURL = new image

function handleOfferImage(event){
  const file = event.target.files[0];
  if(!file) return;
  if(!file.type.startsWith('image/')){ toast('Please select an image file','error'); return; }
  const reader = new FileReader();
  reader.onload = function(e){
    const img = new Image();
    img.onload = function(){
      const MAX = 800; // keep the Firestore doc small — several offers share one document
      let w = img.width, h = img.height;
      if(w > MAX || h > MAX){
        if(w > h){ h = Math.round(h * MAX / w); w = MAX; }
        else { w = Math.round(w * MAX / h); h = MAX; }
      }
      const canvas = document.createElement('canvas');
      canvas.width = w; canvas.height = h;
      const ctx = canvas.getContext('2d');
      ctx.drawImage(img, 0, 0, w, h);
      _offerImageData = canvas.toDataURL('image/jpeg', 0.65);
      document.getElementById('offer-img-preview').innerHTML =
        `<div class="ann-imgwrap"><img src="${_offerImageData}"><button class="ann-rm" type="button" onclick="removeOfferImage()" title="Remove image">×</button></div>`;
    };
    img.src = e.target.result;
  };
  reader.readAsDataURL(file);
}

function removeOfferImage(){
  _offerImageData = '';
  document.getElementById('offer-img-preview').innerHTML='';
  document.getElementById('offer-img-input').value='';
}

// Resets the poster-image field. Pass an existing dataURL (edit mode) to
// show it as the current image, or call with no args to clear the field.
function resetOfferImageState(existingUrl){
  _offerImageData = undefined;
  const inp = document.getElementById('offer-img-input'); if(inp) inp.value='';
  const prev = document.getElementById('offer-img-preview');
  if(prev){
    prev.innerHTML = existingUrl
      ? `<div class="ann-imgwrap"><img src="${existingUrl}"><button class="ann-rm" type="button" onclick="removeOfferImage()" title="Remove image">×</button></div>`
      : '';
  }
}

// ── Browser Notification Helpers ──
function requestNotifPermission(){
  if(!('Notification' in window)) return;
  if(Notification.permission === 'default'){
    Notification.requestPermission();
  }
}

function sendBrowserNotif(title, body, iconUrl){
  if(!('Notification' in window)) return;
  if(Notification.permission !== 'granted') return;
  try {
    const n = new Notification(title, {
      body: body || '',
      icon: iconUrl || '/favicon.ico',
      tag: 'dninvest-ann',
      renotify: true
    });
    n.onclick = function(){ window.focus(); n.close(); };
    setTimeout(()=>n.close(), 8000);
  } catch(e){ console.warn('Notification error', e); }
}

function sendAnnouncement(){
  const text = document.getElementById('ann-text').value.trim();
  const existing = DB.get('announcement');
  let image;
  if(_annImageData === '') image = '';
  else if(typeof _annImageData === 'string') image = _annImageData;
  else image = existing?.image || '';

  if(!text && !image){ toast('Type a message or add an image','error'); return; }

  const targetVal = document.getElementById('ann-target').value;
  let target = {type:'all'};
  let tgtLabel = 'All RMs';
  if(targetVal==='seg-equity'){ target = {type:'segment', segment:'equity'}; tgtLabel='Equity Team'; }
  else if(targetVal==='seg-mf'){ target = {type:'segment', segment:'mf'}; tgtLabel='Mutual Fund Team'; }
  else if(targetVal==='users'){
    const usernames=[...document.querySelectorAll('.ann-user-chk:checked')].map(c=>c.value);
    if(!usernames.length){ toast('Select at least one RM','error'); return; }
    target = {type:'users', usernames};
    const users=DB.get('users')||[];
    tgtLabel = usernames.map(un=>{const u=users.find(x=>x.username===un); return u?u.name:un;}).join(', ');
  }

  const ann = {
    id: uid(),
    text,
    image,
    date: new Date().toISOString(),
    by: CU.name,
    target
  };
  DB.set('announcement', ann);
  // Permanent history record (image not stored in history — Firestore doc size safety)
  addCommHistory({
    id:'H'+Date.now()+Math.random().toString(36).slice(2,6),
    kind:'announcement', refId:ann.id,
    title:'', text:ann.text||'', hasImage:!!ann.image,
    target:tgtLabel, by:CU.name, date:ann.date
  });
  toast('Announcement sent to: '+tgtLabel,'success');
  // Browser notification for admin preview (self-test)
  sendBrowserNotif('📢 DN Investment', ann.text ? ann.text.substring(0,80) : 'New Announcement');
  renderAnnouncementAdmin();
}

function clearAnnouncement(){
  if(!confirm('Clear the current announcement for all RMs?')) return;
  DB.set('announcement', null);
  toast('Announcement cleared','success');
  renderAnnouncementAdmin();
}

// ══════════════════════════════════════════
// MEETING AGENDA — shared board where every RM and Admin can drop in an
// Idea / Problem / Target ahead of the office meeting. 'meeting_agenda' holds
// the current/active items (merge-on-write, like client records, so two RMs
// adding at the same moment don't clobber each other). When Admin clears the
// board for a new meeting, the current items are archived as one dated cycle
// into 'meeting_agenda_archive' instead of being deleted outright.
// ══════════════════════════════════════════
const MA_TYPES = {
  idea:    {label:'Idea',    icon:'💡'},
  problem: {label:'Problem', icon:'⚠️'},
  target:  {label:'Target',  icon:'🎯'}
};

function submitAgendaItem(){
  const type = document.getElementById('ma-type').value;
  const textEl = document.getElementById('ma-text');
  const text = (textEl.value||'').trim();
  if(!text){ toast('Please type your point first','error'); return; }
  const item = {
    id: uid(),
    type,
    text,
    author: CU.name,
    createdAt: new Date().toISOString()
  };
  DB.setClient('meeting_agenda', item);
  textEl.value = '';
  toast('Added to agenda','success');
  renderMeetingAgenda();
}

async function deleteAgendaItem(id){
  if(!confirm('Remove this item from the agenda?')) return;
  await DB.deleteClient('meeting_agenda', id);
  renderMeetingAgenda();
}

async function clearMeetingAgenda(){
  const items = DB.get('meeting_agenda') || [];
  if(items.length === 0){ toast('Agenda is already empty','error'); return; }
  if(!confirm(`Clear all ${items.length} item(s) for a new meeting?\n\nThey will be saved under Past Meetings, not deleted.`)) return;
  const archive = DB.get('meeting_agenda_archive') || [];
  archive.unshift({
    id: uid(),
    clearedAt: new Date().toISOString(),
    clearedBy: CU.name,
    items
  });
  await DB.set('meeting_agenda_archive', archive);
  await DB.set('meeting_agenda', []);
  toast('Agenda cleared for the next meeting','success');
  renderMeetingAgenda();
}

function toggleMaHistory(id){
  const el = document.getElementById('ma-hist-'+id);
  if(el) el.classList.toggle('open');
}

function renderMeetingAgenda(){
  const board = document.getElementById('ma-board');
  if(!board) return;
  const items = DB.get('meeting_agenda') || [];

  const adminActions = document.getElementById('ma-admin-actions');
  if(adminActions) adminActions.style.display = CU.role==='admin' ? 'flex' : 'none';
  const historyCard = document.getElementById('ma-history-card');
  if(historyCard) historyCard.style.display = CU.role==='admin' ? '' : 'none';

  board.innerHTML = Object.keys(MA_TYPES).map(type=>{
    const meta = MA_TYPES[type];
    const list = items.filter(i=>i.type===type).sort((a,b)=>(b.createdAt||'').localeCompare(a.createdAt||''));
    const body = list.length
      ? list.map(i=>`
        <div class="ma-item">
          <div class="ma-item-text">${escapeHtml(i.text)}</div>
          <div class="ma-item-meta">
            <span>👤 ${escapeHtml(i.author)} · ${fmtDate(i.createdAt)}</span>
            ${(CU.role==='admin' || i.author===CU.name) ? `<span class="ma-item-del" onclick="deleteAgendaItem('${i.id}')">🗑️ Remove</span>` : ''}
          </div>
        </div>`).join('')
      : `<div class="ma-empty">No ${meta.label.toLowerCase()}s yet</div>`;
    return `
      <div class="ma-col ${type}">
        <div class="ma-col-hdr">
          <span class="ma-col-title">${meta.icon} ${meta.label}s</span>
          <span class="ma-col-count">${list.length}</span>
        </div>
        ${body}
      </div>`;
  }).join('');

  if(CU.role==='admin'){
    const histList = document.getElementById('ma-history-list');
    if(histList){
      const archive = DB.get('meeting_agenda_archive') || [];
      if(archive.length===0){
        histList.innerHTML = `<div class="ma-empty">No past meetings cleared yet</div>`;
      } else {
        histList.innerHTML = archive.map(cycle=>`
          <div class="ma-history-entry">
            <div class="ma-history-hdr" onclick="toggleMaHistory('${cycle.id}')">
              <span>🗓️ Cleared ${fmtDate(cycle.clearedAt)} by ${escapeHtml(cycle.clearedBy)} — ${cycle.items.length} item(s)</span>
              <span>▾</span>
            </div>
            <div class="ma-history-body" id="ma-hist-${cycle.id}">
              ${cycle.items.map(i=>`
                <div class="ma-item" style="margin-bottom:8px">
                  <span class="ma-history-tag ${i.type}">${MA_TYPES[i.type].icon} ${MA_TYPES[i.type].label}</span>
                  <span class="ma-item-text" style="display:inline">${escapeHtml(i.text)}</span>
                  <div class="ma-item-meta"><span>👤 ${escapeHtml(i.author)} · ${fmtDate(i.createdAt)}</span></div>
                </div>`).join('')}
            </div>
          </div>`).join('');
      }
    }
  }
}

// Show popup if there's an unseen announcement for the current user
// ─── SPECIAL OFFERS / CONTESTS ───────────────────────────────────────────────
// Admin publishes offers (Monsoon Dhamaka, Diwali Offer, Demat/SIP contest...)
// from the Announcements page. Stored in crm_data/special_offers. RMs see a
// festive popup on every login/refresh while the offer is live — and instantly
// via the real-time listener when Admin publishes.
function getSpecialOffers(){ return DB.get('special_offers') || []; }
function saveSpecialOffersList(list){ DB.set('special_offers', list); }

const OFFER_THEMES = {
  festival: {emoji:'🪔', float:['🪔','✨','🎆','🌟','🎊'], grad:'linear-gradient(135deg,#7A1E00 0%,#C1440E 45%,#E8930C 100%)', ribbon:'#FFD54F'},
  monsoon:  {emoji:'🌧️', float:['🌧️','⚡','☔','💧','🌈'], grad:'linear-gradient(135deg,#0B2B5B 0%,#155FA0 50%,#37B6E9 100%)', ribbon:'#8FE3FF'},
  contest:  {emoji:'🏆', float:['🏆','🥇','🎯','⭐','🎉'], grad:'linear-gradient(135deg,#3B0764 0%,#7C3AED 55%,#C084FC 100%)', ribbon:'#FDE68A'},
  money:    {emoji:'💰', float:['💰','💸','🪙','✨','🤑'], grad:'linear-gradient(135deg,#064E3B 0%,#059669 55%,#34D399 100%)', ribbon:'#FEF08A'},
  rocket:   {emoji:'🚀', float:['🚀','🎯','🔥','⭐','💥'], grad:'linear-gradient(135deg,#111C3D 0%,#3B2E7E 55%,#E0447C 100%)', ribbon:'#F9A8D4'}
};

function populateOfferTarget(){
  const listEl=document.getElementById('offer-users-list');
  if(!listEl) return;
  const prev={};
  listEl.querySelectorAll('.offer-user-chk').forEach(c=>{ prev[c.value]=c.checked; });
  const users=(DB.get('users')||[]).filter(u=>u.active!==false && u.role!=='admin');
  listEl.innerHTML = users.map(u=>`
    <label style="display:flex;align-items:center;gap:5px;font-size:.85rem;font-weight:400;text-transform:none;letter-spacing:0">
      <input type="checkbox" class="offer-user-chk" value="${u.username}" ${prev[u.username]?'checked':''}> ${u.name}
    </label>`).join('');
}

function onOfferTargetChange(){
  const v=document.getElementById('offer-target').value;
  document.getElementById('offer-users-wrap').style.display = (v==='users')?'':'none';
}

function saveSpecialOffer(){
  if(!CU || CU.role!=='admin'){ toast('Only admin can publish offers','error'); return; }
  const title=(document.getElementById('offer-title').value||'').trim();
  const msg=(document.getElementById('offer-msg').value||'').trim();
  const theme=document.getElementById('offer-theme').value||'festival';
  const targetVal=document.getElementById('offer-target').value||'ALL';
  let target={type:'all'};
  if(targetVal==='seg-equity') target={type:'segment', segment:'equity'};
  else if(targetVal==='seg-mf') target={type:'segment', segment:'mf'};
  else if(targetVal==='users'){
    const usernames=Array.from(document.querySelectorAll('.offer-user-chk:checked')).map(c=>c.value);
    if(!usernames.length){ toast('Please select at least one RM','error'); return; }
    target={type:'users', usernames};
  }
  const from=document.getElementById('offer-from').value||today();
  const to=document.getElementById('offer-to').value||'';

  // If a poster image is attached (new upload, or an existing one kept during
  // edit), the image itself can carry the offer — title/message become optional.
  const existingOffer = editingOfferId ? getSpecialOffers().find(x=>x.id===editingOfferId) : null;
  const willHaveImage = !!_offerImageData || (_offerImageData===undefined && existingOffer && !!existingOffer.image);
  if(!title && !willHaveImage){ toast('Please enter the offer title, or add a poster image','error'); return; }
  if(!msg && !willHaveImage){ toast('Please enter the offer details / message, or add a poster image','error'); return; }
  if(to && to<from){ toast('Valid To cannot be earlier than Valid From','error'); return; }

  // _offerImageData: undefined = leave unchanged (edit only), '' = removed, dataURL = new image
  const list=getSpecialOffers();
  if(editingOfferId){
    const o=list.find(x=>x.id===editingOfferId);
    if(o){
      Object.assign(o,{title,msg,theme,target,from,to,updated:new Date().toISOString(),updatedBy:CU.name});
      if(_offerImageData!==undefined) o.image=_offerImageData;
    }
    saveSpecialOffersList(list);
    // Keep the history record in sync with the edited offer
    if(o){
      const hl=getCommHistory();
      const h=hl.find(x=>x.kind==='offer' && x.refId===o.id);
      if(h){ Object.assign(h,{title:o.title,text:o.msg,theme:o.theme,from:o.from,to:o.to,target:offerTargetLabel(o),edited:new Date().toISOString(),hasImage:!!o.image}); saveCommHistory(hl); }
      else addCommHistory({id:'H'+Date.now()+Math.random().toString(36).slice(2,6),kind:'offer',refId:o.id,title:o.title,text:o.msg,theme:o.theme,from:o.from,to:o.to,target:offerTargetLabel(o),by:CU.name,date:o.created||new Date().toISOString(),edited:new Date().toISOString(),hasImage:!!o.image});
    }
    cancelOfferEdit();
    renderOffersAdmin();
    toast('💾 Offer updated! RMs will see the new version');
    return;
  }
  const newOffer={id:'OF'+Date.now(), title, msg, theme, target, from, to, active:true, created:new Date().toISOString(), by:CU.name, image:_offerImageData||''};
  list.push(newOffer);
  saveSpecialOffersList(list);
  // Permanent history record
  addCommHistory({
    id:'H'+Date.now()+Math.random().toString(36).slice(2,6),
    kind:'offer', refId:newOffer.id,
    title, text:msg, theme, from, to,
    target:offerTargetLabel(newOffer), by:CU.name, date:newOffer.created, hasImage:!!newOffer.image
  });
  document.getElementById('offer-title').value='';
  document.getElementById('offer-msg').value='';
  resetOfferImageState();
  renderOffersAdmin();
  toast('🎉 Offer published! All targeted RMs will see the popup');
}

let editingOfferId=null;
function editOffer(id){
  const o=getSpecialOffers().find(x=>x.id===id);
  if(!o) return;
  editingOfferId=id;
  document.getElementById('offer-title').value=o.title||'';
  document.getElementById('offer-msg').value=o.msg||'';
  document.getElementById('offer-theme').value=o.theme||'festival';
  document.getElementById('offer-from').value=o.from||'';
  document.getElementById('offer-to').value=o.to||'';
  // Target: map stored target back to the select + checkboxes
  const t=o.target;
  let tv='ALL';
  if(t && typeof t==='object'){
    if(t.type==='segment') tv='seg-'+t.segment;
    else if(t.type==='users') tv='users';
  } else if(typeof t==='string' && t!=='ALL'){ tv='users'; }
  document.getElementById('offer-target').value=tv;
  populateOfferTarget();
  if(tv==='users'){
    const usernames=(t && t.usernames) ? t.usernames : [];
    document.querySelectorAll('.offer-user-chk').forEach(c=>{ c.checked=usernames.includes(c.value); });
  }
  onOfferTargetChange();
  resetOfferImageState(o.image||'');
  const pb=document.getElementById('offer-publish-btn'); if(pb) pb.innerHTML='💾 Update Offer';
  const cb=document.getElementById('offer-cancel-btn'); if(cb) cb.style.display='';
  const card=document.getElementById('offer-admin-card'); if(card) card.scrollIntoView({behavior:'smooth',block:'start'});
  document.getElementById('offer-title').focus();
}

function cancelOfferEdit(){
  editingOfferId=null;
  document.getElementById('offer-title').value='';
  document.getElementById('offer-msg').value='';
  document.getElementById('offer-to').value='';
  document.getElementById('offer-target').value='ALL';
  onOfferTargetChange();
  resetOfferImageState();
  const pb=document.getElementById('offer-publish-btn'); if(pb) pb.innerHTML='🎉 Publish Offer';
  const cb=document.getElementById('offer-cancel-btn'); if(cb) cb.style.display='none';
}

function toggleOffer(id){
  const list=getSpecialOffers();
  const o=list.find(x=>x.id===id); if(!o) return;
  o.active=!o.active;
  saveSpecialOffersList(list);
  renderOffersAdmin();
  toast(o.active?'▶ Offer resumed — popup will show again':'⏸ Offer paused — popup stopped for RMs');
}

function deleteOffer(id){
  if(!confirm('Delete this offer?\n\n(Its record will remain safely in 📜 History below.)')) return;
  saveSpecialOffersList(getSpecialOffers().filter(x=>x.id!==id));
  // Mark the history record as deleted (record stays in history)
  const hl=getCommHistory();
  const h=hl.find(x=>x.kind==='offer' && x.refId===id);
  if(h){ h.deletedOn=new Date().toISOString(); h.deletedBy=CU.name; saveCommHistory(hl); }
  if(editingOfferId===id) cancelOfferEdit();
  renderOffersAdmin();
  renderCommHistory();
}

function offerIsLive(o){
  if(!o.active) return false;
  const d=today();
  if(o.from && d<o.from) return false;
  if(o.to && d>o.to) return false;
  return true;
}

// Target check — same style as announcements. Also supports legacy string
// targets ('ALL' or an RM name) from older offers.
function offerAppliesToMe(o){
  const t=o.target;
  if(!t || t==='ALL') return true;
  if(typeof t==='string') return t===CU.name;
  if(t.type==='all') return true;
  if(t.type==='segment') return (CU.segments||[]).includes(t.segment);
  if(t.type==='users') return (t.usernames||[]).includes(CU.username);
  return true;
}

function offerTargetLabel(o){
  const t=o.target;
  if(!t || t==='ALL') return 'All RMs';
  if(typeof t==='string') return t;
  if(t.type==='all') return 'All RMs';
  if(t.type==='segment') return t.segment==='equity'?'Equity Team':'Mutual Fund Team';
  if(t.type==='users'){
    const users=DB.get('users')||[];
    return (t.usernames||[]).map(un=>{const u=users.find(x=>x.username===un); return u?u.name:un;}).join(', ')||'—';
  }
  return 'All RMs';
}

function renderOffersAdmin(){
  const box=document.getElementById('offers-admin-list');
  if(!box) return;
  const list=getSpecialOffers();
  if(!list.length){ box.innerHTML='<p style="font-size:.78rem;color:var(--text3)">No offers yet.</p>'; return; }
  box.innerHTML='<div style="font-size:.72rem;font-weight:600;color:var(--text2);text-transform:uppercase;margin-bottom:6px">Published offers</div>'+
    list.slice().reverse().map(o=>{
      const th=OFFER_THEMES[o.theme]||OFFER_THEMES.festival;
      const live=offerIsLive(o);
      const d=today();
      let badge;
      if(live) badge='<span style="font-size:.62rem;color:#fff;background:var(--green,#059669);border-radius:10px;padding:1px 8px;margin-left:4px">LIVE — RMs will see popup</span>';
      else if(!o.active) badge='<span style="font-size:.62rem;color:var(--text3);background:var(--border,#ddd);border-radius:10px;padding:1px 8px;margin-left:4px">PAUSED</span>';
      else if(o.from && d<o.from) badge='<span style="font-size:.62rem;color:#9C6500;background:#FFEB9C;border-radius:10px;padding:1px 8px;margin-left:4px">⏳ STARTS '+o.from+' — no popup yet</span>';
      else badge='<span style="font-size:.62rem;color:#9C0006;background:#FFC7CE;border-radius:10px;padding:1px 8px;margin-left:4px">EXPIRED</span>';
      const thumb = o.image ? `<img src="${o.image}" style="width:44px;height:44px;object-fit:cover;border-radius:8px;border:1px solid var(--border,#e3e8ef);flex-shrink:0">` : '';
      return `<div style="display:flex;align-items:center;gap:10px;padding:8px 10px;border:1px solid var(--border,#e3e8ef);border-radius:10px;margin-bottom:6px">
        ${thumb}
        <span style="font-size:20px">${th.emoji}</span>
        <div style="flex:1;min-width:0">
          <div style="font-size:.85rem;font-weight:600">${o.title||'🖼️ Poster Offer'} ${badge}</div>
          <div style="font-size:.74rem;color:var(--text3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${o.msg||(o.image?'(poster image only — no text)':'')}</div>
          <div style="font-size:.68rem;color:var(--text3)">🎯 ${offerTargetLabel(o)} · 📅 ${o.from||'—'} → ${o.to||'no end'}</div>
        </div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end">
          <button onclick="editOffer('${o.id}')" style="background:#2563EB;color:#fff;border:none;font-size:.72rem;font-weight:600;padding:6px 10px;border-radius:8px;cursor:pointer;white-space:nowrap">✏️ Edit</button>
          <button onclick="toggleOffer('${o.id}')" style="background:${o.active?'#D97706':'#059669'};color:#fff;border:none;font-size:.72rem;font-weight:600;padding:6px 10px;border-radius:8px;cursor:pointer;white-space:nowrap">${o.active?'⏸ Pause':'▶ Resume'}</button>
          <button onclick="deleteOffer('${o.id}')" style="background:#DC2626;color:#fff;border:none;font-size:.72rem;font-weight:600;padding:6px 10px;border-radius:8px;cursor:pointer;white-space:nowrap">🗑 Delete</button>
        </div>
      </div>`;
    }).join('');
}

// ══════════════════════════════════════════════════════════════════
// COMMUNICATION HISTORY (📜) — permanent record of every announcement
// & offer sent to RMs. Stored in crm_data/comm_history (admin-written).
// Images are NOT stored here (Firestore 1MB doc limit) — only a 📷 flag.
// ══════════════════════════════════════════════════════════════════
function getCommHistory(){ return DB.get('comm_history')||[]; }
function saveCommHistory(list){
  list.sort((a,b)=>String(b.date||'').localeCompare(String(a.date||'')));
  DB.set('comm_history', list.slice(0,400)); // keep newest 400
}
function addCommHistory(entry){
  const l=getCommHistory(); l.push(entry); saveCommHistory(l);
  renderCommHistory();
}

function chEsc(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

function chFiltered(){
  const kindF=(document.getElementById('ch-kind')||{value:''}).value;
  const q=((document.getElementById('ch-search')||{value:''}).value||'').toLowerCase().trim();
  let list=getCommHistory();
  if(kindF) list=list.filter(h=>h.kind===kindF);
  if(q) list=list.filter(h=>((h.title||'')+' '+(h.text||'')+' '+(h.target||'')+' '+(h.by||'')).toLowerCase().includes(q));
  return list;
}

function renderCommHistory(){
  const card=document.getElementById('comm-history-card');
  if(!card) return;
  if(!CU || CU.role!=='admin'){ card.style.display='none'; return; }
  card.style.display='';
  const box=document.getElementById('comm-history-list');
  if(!box) return;
  const list=chFiltered();
  if(!list.length){
    box.innerHTML='<p style="font-size:.78rem;color:var(--text3)">No history yet. New announcements and offers will be recorded here automatically.</p>';
    updateChDelBtn();
    return;
  }
  const rows=list.map(h=>{
    const isOffer=h.kind==='offer';
    const th=isOffer ? (OFFER_THEMES[h.theme]||OFFER_THEMES.festival) : null;
    const icon=isOffer ? (th?th.emoji:'🎁') : '📢';
    const kindBadge=isOffer
      ? '<span style="background:#f3e8ff;color:#6d28d9;padding:1px 7px;border-radius:8px;font-size:.64rem;font-weight:700">OFFER</span>'
      : '<span style="background:#dbeafe;color:#1d4ed8;padding:1px 7px;border-radius:8px;font-size:.64rem;font-weight:700">ANNOUNCEMENT</span>';
    const when=h.date ? new Date(h.date).toLocaleString('en-IN',{day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'}) : '—';
    const extra=[];
    if(h.hasImage) extra.push('📷 Image');
    if(isOffer && (h.from||h.to)) extra.push('📅 '+(h.from||'—')+' → '+(h.to||'no end'));
    if(h.edited) extra.push('✏️ Edited '+new Date(h.edited).toLocaleDateString('en-IN',{day:'2-digit',month:'short'}));
    if(h.deletedOn) extra.push('<span style="color:#DC2626;font-weight:600">🗑 Offer deleted '+new Date(h.deletedOn).toLocaleDateString('en-IN',{day:'2-digit',month:'short'})+'</span>');
    return `<div style="display:flex;align-items:flex-start;gap:10px;padding:8px 10px;border:1px solid var(--border,#e3e8ef);border-radius:10px;margin-bottom:6px${h.deletedOn?';opacity:.75':''}">
      <input type="checkbox" class="ch-chk" value="${chEsc(h.id)}" onchange="updateChDelBtn()" style="margin-top:4px;width:16px;height:16px;cursor:pointer">
      <span style="font-size:18px">${icon}</span>
      <div style="flex:1;min-width:0">
        <div style="font-size:.82rem;font-weight:600">${kindBadge} ${chEsc(h.title||(h.text?h.text.slice(0,60):'')||'—')}</div>
        ${h.title && h.text ? '<div style="font-size:.74rem;color:var(--text3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="'+chEsc(h.text)+'">'+chEsc(h.text)+'</div>' : ''}
        <div style="font-size:.68rem;color:var(--text3);margin-top:2px">🎯 ${chEsc(h.target||'All RMs')} · 🕐 ${when} · by ${chEsc(h.by||'Admin')}${extra.length?' · '+extra.join(' · '):''}</div>
      </div>
    </div>`;
  }).join('');
  box.innerHTML=`<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
      <label style="display:flex;align-items:center;gap:6px;font-size:.72rem;color:var(--text2);cursor:pointer;font-weight:600">
        <input type="checkbox" id="ch-all" onchange="document.querySelectorAll('.ch-chk').forEach(c=>c.checked=this.checked);updateChDelBtn()" style="width:15px;height:15px;cursor:pointer"> Select All
      </label>
      <span style="font-size:.72rem;color:var(--text3)">${list.length} record(s)</span>
    </div>`+rows;
  updateChDelBtn();
}

function updateChDelBtn(){
  const n=document.querySelectorAll('.ch-chk:checked').length;
  const btn=document.getElementById('ch-del-btn');
  const cnt=document.getElementById('ch-del-count');
  if(cnt) cnt.textContent=n;
  if(btn) btn.style.display = n>0 ? '' : 'none';
}

function deleteSelectedHistory(){
  if(!CU || CU.role!=='admin'){ toast('Admin only','error'); return; }
  const ids=Array.from(document.querySelectorAll('.ch-chk:checked')).map(c=>c.value);
  if(!ids.length){ toast('Select at least one record','error'); return; }
  if(!confirm(ids.length+' history record(s) will be PERMANENTLY deleted from history.\n\nThis cannot be undone. Continue?')) return;
  const idSet=new Set(ids);
  saveCommHistory(getCommHistory().filter(h=>!idSet.has(h.id)));
  renderCommHistory();
  toast('🗑 '+ids.length+' history record(s) deleted','success');
}

function exportCommHistory(){
  const list=chFiltered();
  if(!list.length){ toast('No history to export','error'); return; }
  const kindColor=v=>{ v=String(v||'').toLowerCase();
    if(v==='offer') return {bg:'FFE9D5FF',font:'FF6D28D9'};
    if(v==='announcement') return {bg:'FFDBEAFE',font:'FF1D4ED8'};
    return null; };
  const rows=list.map(h=>[
    h.date?new Date(h.date).toLocaleString('en-IN'):'—',
    h.kind==='offer'?'Offer':'Announcement',
    h.title||'—',
    h.text||'—',
    h.target||'All RMs',
    h.kind==='offer'?(h.theme||'—'):'—',
    h.from||'—', h.to||'—',
    h.by||'Admin',
    h.deletedOn?('Deleted '+new Date(h.deletedOn).toLocaleDateString('en-IN')):(h.edited?'Edited':'Active/Sent'),
    h.hasImage?'Yes':'—'
  ]);
  const cols=[
    {header:'Date & Time',width:20},{header:'Type',width:14,align:'center',color:kindColor},
    {header:'Title',width:28},{header:'Message',width:45},{header:'Target',width:20},
    {header:'Theme',width:12,align:'center'},{header:'Valid From',width:12,align:'center'},
    {header:'Valid To',width:12,align:'center'},{header:'By',width:12},
    {header:'Status',width:16,align:'center'},{header:'Image',width:8,align:'center'}
  ];
  dnXlsx(`Announcement_Offer_History_${new Date().toISOString().slice(0,10)}.xlsx`, '📜 Announcements & Offers History — '+new Date().toLocaleDateString('en-IN'), cols, rows);
  toast('Export done!','success');
}

// ── Festive popup for RMs — shows on every login/refresh while offer is live ──
let offerPopupShownIds=null;
function showOfferPopupIfAny(force){
  if(!CU || CU.role==='admin') return; // popup is for RMs only
  const live=getSpecialOffers().filter(o=>offerIsLive(o) && offerAppliesToMe(o));
  if(!live.length) return;
  const ids=live.map(o=>o.id).join(',');
  if(!force && offerPopupShownIds===ids) return;
  offerPopupShownIds=ids;
  const old=document.getElementById('offer-popup-overlay'); if(old) old.remove();
  const esc=s=>String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

  const el=document.createElement('div');
  el.id='offer-popup-overlay';

  // Pure poster mode: every live offer has an image and no title/message —
  // show just the poster(s), full size, no themed card/confetti behind it.
  const posterOnly = live.every(o=>o.image && !(o.title||'').trim() && !(o.msg||'').trim());
  if(posterOnly){
    const imgs=live.map(o=>`<img src="${esc(o.image)}" style="width:100%;max-height:80vh;object-fit:contain;border-radius:14px;display:block;box-shadow:0 12px 40px rgba(0,0,0,.5);margin-bottom:14px">`).join('');
    el.innerHTML=`
      <div class="offer-poster-only">
        ${imgs}
        <button onclick="document.getElementById('offer-popup-overlay').remove()" class="offer-close-btn">Got it! 🎉</button>
      </div>`;
    document.body.appendChild(el);
    return;
  }

  const th=OFFER_THEMES[live[0].theme]||OFFER_THEMES.festival;
  const floats=th.float.concat(th.float).map((e,i)=>`<span class="offer-float" style="left:${(i*13+5)%95}%;animation-delay:${(i*0.7).toFixed(1)}s;animation-duration:${(5+(i%4)).toFixed(1)}s">${e}</span>`).join('');
  const cards=live.map(o=>{
    const t=OFFER_THEMES[o.theme]||OFFER_THEMES.festival;
    const posterHtml = o.image ? `<img src="${esc(o.image)}" style="width:100%;max-height:60vh;object-fit:contain;border-radius:10px;margin-top:10px;display:block;box-shadow:0 6px 18px rgba(0,0,0,.25)">` : '';
    const titleHtml = (o.title||'').trim() ? `<div style="font-size:17px;font-weight:800;color:#fff;letter-spacing:.3px">${t.emoji} ${esc(o.title)}</div>` : '';
    const msgHtml = (o.msg||'').trim() ? `<div style="font-size:13.5px;color:rgba(255,255,255,.95);margin-top:6px;line-height:1.5;white-space:pre-wrap">${esc(o.msg)}</div>` : '';
    return `<div style="background:rgba(255,255,255,.14);backdrop-filter:blur(3px);border:1px solid rgba(255,255,255,.35);border-radius:14px;padding:14px 16px;margin-top:12px;text-align:left">
      ${titleHtml}
      ${msgHtml}
      ${posterHtml}
      <div style="font-size:11px;color:${th.ribbon};margin-top:8px;font-weight:600">⏳ Valid: ${o.from||'today'}${o.to?' to '+o.to:' — until further notice'}</div>
    </div>`;
  }).join('');
  el.innerHTML=`
    <div class="offer-popup-card" style="background:${th.grad}">
      <div class="offer-float-layer">${floats}</div>
      <div style="position:relative;z-index:2">
        <div class="offer-big-emoji">${th.emoji}</div>
        <div style="font-size:12px;letter-spacing:3px;color:${th.ribbon};font-weight:700;text-transform:uppercase">✦ Special Offer ✦</div>
        ${cards}
        <button onclick="document.getElementById('offer-popup-overlay').remove()" class="offer-close-btn">Got it! 🎉</button>
      </div>
    </div>`;
  document.body.appendChild(el);
}

(function injectOfferCss(){
  const css=document.createElement('style');
  css.textContent=`
  #offer-popup-overlay{position:fixed;inset:0;background:rgba(10,14,25,.62);z-index:9999;display:flex;align-items:center;justify-content:center;padding:18px;animation:offerFade .25s ease}
  .offer-popup-card{position:relative;overflow:hidden;width:100%;max-width:440px;max-height:88vh;overflow-y:auto;border-radius:22px;padding:26px 22px;text-align:center;box-shadow:0 24px 70px rgba(0,0,0,.5);animation:offerPop .45s cubic-bezier(.2,1.4,.4,1)}
  .offer-poster-only{position:relative;width:100%;max-width:460px;max-height:92vh;overflow-y:auto;display:flex;flex-direction:column;align-items:center;animation:offerPop .45s cubic-bezier(.2,1.4,.4,1)}
  .offer-big-emoji{font-size:52px;line-height:1;animation:offerBounce 1.6s ease-in-out infinite}
  .offer-float-layer{position:absolute;inset:0;z-index:1;pointer-events:none}
  .offer-float{position:absolute;top:105%;font-size:20px;opacity:.85;animation-name:offerFloatUp;animation-iteration-count:infinite;animation-timing-function:linear}
  .offer-close-btn{margin-top:16px;background:#fff;color:#222;border:none;font-weight:800;font-size:14px;padding:11px 26px;border-radius:30px;cursor:pointer;box-shadow:0 6px 18px rgba(0,0,0,.3)}
  .offer-close-btn:active{transform:scale(.96)}
  @keyframes offerFade{from{opacity:0}to{opacity:1}}
  @keyframes offerPop{from{transform:scale(.7);opacity:0}to{transform:scale(1);opacity:1}}
  @keyframes offerBounce{0%,100%{transform:translateY(0)}50%{transform:translateY(-8px)}}
  @keyframes offerFloatUp{from{transform:translateY(0) rotate(0deg)}to{transform:translateY(-130vh) rotate(30deg)}}`;
  document.head.appendChild(css);
})();

function checkAnnouncement(){
  if(!CU) return;
  // Special offer popup — shows on every login/refresh while any offer is live
  try{ showOfferPopupIfAny(false); }catch(e){}
  // Request browser notification permission on first check (RM side)
  if(CU.role !== 'admin') requestNotifPermission();
  const ann = DB.get('announcement');
  if(!ann || (!ann.text && !ann.image)) return;
  if(!announcementAppliesToMe(ann)) return;
  const seenKey = 'dninvest_ann_seen_' + CU.username;
  const seen = localStorage.getItem(seenKey);
  if(seen === ann.id) return;
  // Send browser notification (works even if CRM is minimized)
  if(CU.role !== 'admin'){
    sendBrowserNotif('📢 DN Investment - Announcement', ann.text ? ann.text.substring(0,100) : 'New Announcement from Admin');
  }
  showAnnouncementPopup(ann);
}

function announcementAppliesToMe(ann){
  const target = ann.target || {type:'all'};
  if(target.type==='all') return true;
  if(target.type==='segment') return (CU.segments||[]).includes(target.segment);
  if(target.type==='users') return (target.usernames||[]).includes(CU.username);
  return true;
}

function showAnnouncementPopup(ann){
  let h='';
  if(ann.image) h += `<img class="ann-pop-img" src="${ann.image}">`;
  if(ann.text) h += `<div class="ann-pop-text">${escapeHtml(ann.text).replace(/\n/g,'<br>')}</div>`;
  if(ann.date) h += `<div class="ann-pop-meta">— ${ann.by||'Admin'}, ${fmtDate(ann.date)}</div>`;
  document.getElementById('announcementPopupBody').innerHTML = h;
  document.getElementById('announcementPopup').dataset.annId = ann.id;
  document.getElementById('announcementPopup').classList.add('open');
}

function closeAnnouncementPopup(){
  const el = document.getElementById('announcementPopup');
  const id = el.dataset.annId;
  if(id && CU){ localStorage.setItem('dninvest_ann_seen_' + CU.username, id); }
  el.classList.remove('open');
}

// ── Bottom-nav 📢 Alerts button ─────────────────────────────────────────────
// Admin → opens the Announcements page. RM → re-shows any live offer popup
// and the current announcement (even if already marked seen).
function openAnnAlerts(){
  if(CU && CU.role==='admin'){ showPage('announcements'); return; }
  let shown=false;
  try{
    const live=getSpecialOffers().filter(o=>offerIsLive(o) && offerAppliesToMe(o));
    if(live.length){ showOfferPopupIfAny(true); shown=true; }
  }catch(e){}
  try{
    const ann=DB.get('announcement');
    if(ann && (ann.text||ann.image) && announcementAppliesToMe(ann)){ showAnnouncementPopup(ann); shown=true; }
  }catch(e){}
  if(!shown) toast('No active announcement or offer right now');
}

// ── Followup Alert Popup ────────────────────────────────────────────────────
function checkFollowupAlert(){
  // Follow-up popup disabled — the dashboard Follow-ups card shows today's pending list.
  return;
}


function closeFollowupAlert(){
  // popup removed
}
// ───────────────────────────────────────────────────────────────────────────

// ══════════════════════════════════════════
// TWO-WAY RM <-> ADMIN MESSAGING
// ══════════════════════════════════════════

// Structure: rm_messages = array of threads
// thread = { id, rmUsername, rmName, messages: [{id, from, fromName, text, date, isAdmin}] }

function cleanOldMessages(threads){
  const cutoff = new Date(Date.now() - 24*60*60*1000).toISOString();
  return threads.map(t => ({
    ...t,
    messages: (t.messages||[]).filter(m => m.date > cutoff)
  })).filter(t => t.messages.length > 0);
}

function getRmMessages(){
  const raw = DB.get('rm_messages') || [];
  return raw;
}

function getRmMessagesClean(){
  // Returns threads with 24h-old messages removed
  const raw = DB.get('rm_messages') || [];
  return cleanOldMessages(raw);
}

function getMyThread(){
  if(!CU) return null;
  const threads = getRmMessages();
  return threads.find(t => t.rmUsername === CU.username) || null;
}

function updateMsgBadge(){
  if(!CU) return;
  if(CU.role === 'admin'){
    // Admin: count unread RM messages (not yet seen)
    const threads = getRmMessages();
    const seenKey = 'dninvest_admin_msg_seen';
    const lastSeen = localStorage.getItem(seenKey) || '';
    let unread = 0;
    threads.forEach(t => {
      (t.messages||[]).forEach(m => {
        if(!m.isAdmin && m.id > lastSeen) unread++;
      });
    });
    const badge = document.getElementById('admin-inbox-badge-nav');
    if(badge){ badge.textContent = unread; badge.style.display = unread > 0 ? '' : 'none'; }
    const inboxBadge = document.getElementById('inbox-unread-badge');
    if(inboxBadge){ inboxBadge.style.display = unread > 0 ? '' : 'none'; }
    return;
  }
  // RM: count unread admin replies
  const thread = getMyThread();
  const seenKey = 'dninvest_msg_seen_' + CU.username;
  const lastSeen = localStorage.getItem(seenKey) || '';
  let unread = 0;
  if(thread){
    thread.messages.forEach(m => {
      if(m.isAdmin && m.id > lastSeen) unread++;
    });
  }
  const badge = document.getElementById('nb-msg');
  if(badge){
    badge.textContent = unread;
    badge.style.display = unread > 0 ? '' : 'none';
  }
}

function sendRmMessage(){
  if(!CU || CU.role === 'admin') return;
  const text = (document.getElementById('rm-msg-text').value || '').trim();
  if(!text){ toast('Message likhein', 'error'); return; }
  const threads = getRmMessages();
  let thread = threads.find(t => t.rmUsername === CU.username);
  const msgId = uid();
  const newMsg = { id: msgId, from: CU.username, fromName: CU.name, text, date: new Date().toISOString(), isAdmin: false };
  if(thread){
    thread.messages.push(newMsg);
  } else {
    thread = { id: uid(), rmUsername: CU.username, rmName: CU.name, messages: [newMsg] };
    threads.push(thread);
  }
  DB.set('rm_messages', cleanOldMessages(threads));
  document.getElementById('rm-msg-text').value = '';
  toast('Message sent to Admin ✓', 'success');
  renderRmMessages();
}

function adminReplyToRm(rmUsername){
  const inp = document.getElementById('admin-reply-' + rmUsername);
  const text = (inp ? inp.value : '').trim();
  if(!text){ toast('Reply likhein', 'error'); return; }
  const threads = getRmMessages();
  const thread = threads.find(t => t.rmUsername === rmUsername);
  if(!thread){ toast('Thread not found', 'error'); return; }
  const newMsg = { id: uid(), from: CU.username, fromName: CU.name, text, date: new Date().toISOString(), isAdmin: true };
  thread.messages.push(newMsg);
  DB.set('rm_messages', cleanOldMessages(threads));
  if(inp) inp.value = '';
  toast('Reply sent ✓', 'success');
  renderInbox();
}

function renderRmMessages(){
  const thread = getMyThread();
  const seenKey = 'dninvest_msg_seen_' + CU.username;
  // Mark all admin messages as seen
  if(thread){
    const lastMsg = thread.messages.filter(m=>m.isAdmin).slice(-1)[0];
    if(lastMsg) localStorage.setItem(seenKey, lastMsg.id);
  }
  updateMsgBadge();
  const el = document.getElementById('rm-conversation');
  if(!el) return;
  if(!thread || !thread.messages.length){
    el.innerHTML = '<em style="color:var(--gray);font-size:.85rem">No messages yet. Message the Admin.</em>';
    return;
  }
  el.innerHTML = thread.messages.map(m => {
    const side = m.isAdmin ? 'recv' : 'sent';
    const label = m.isAdmin ? '🔴 Admin' : '🟢 You';
    return `<div class="msg-wrap ${side}">
      <div class="msg-bubble ${side}">${escapeHtml(m.text).replace(/\n/g,'<br>')}</div>
      <div class="msg-meta">${label} · ${fmtDate(m.date)}</div>
    </div>`;
  }).join('');
  el.scrollTop = el.scrollHeight;
}

function renderInbox(){
  const el = document.getElementById('admin-inbox-list');
  const card = document.getElementById('admin-inbox-card');
  if(!el || !card) return;
  card.style.display = '';
  // Use 24h-cleaned threads
  const threads = getRmMessagesClean();
  // Update unread badge
  const unreadBadge = document.getElementById('inbox-unread-badge');
  const totalUnread = threads.reduce((acc,t)=>{
    return acc + (t.messages||[]).filter(m=>!m.isAdmin && !m.adminRead).length;
  }, 0);
  if(unreadBadge) unreadBadge.style.display = totalUnread > 0 ? '' : 'none';
  if(!threads.length){
    el.innerHTML = '<em style="color:var(--gray)">No messages from RMs yet.</em>';
    return;
  }
  // Remember active tab
  const prevActive = el.querySelector('.rm-tab-btn.active');
  const activeUser = prevActive ? prevActive.dataset.rm : threads[0].rmUsername;
  // Seen key for admin — track which msg IDs have been seen
  const adminSeenKey = 'dninvest_admin_seen_msgs';
  const seenSet = new Set(JSON.parse(localStorage.getItem(adminSeenKey)||'[]'));
  // Build tab bar — highlight tab if has new (unseen) RM messages
  const tabsHtml = threads.map((t,i) => {
    const newMsgs = (t.messages||[]).filter(m=>!m.isAdmin && !seenSet.has(m.id));
    const unread = newMsgs.length;
    const badgeHtml = unread > 0 ? `<span class="rm-tab-unread">${unread}</span>` : '';
    const isActive = t.rmUsername === activeUser;
    const newClass = unread > 0 ? ' has-new' : '';
    return `<button class="rm-tab-btn${isActive?' active':''}${newClass}" data-rm="${t.rmUsername}" onclick="switchInboxTab('${t.rmUsername}')">${t.rmName}${badgeHtml}</button>`;
  }).join('');
  // Build panels
  const panelsHtml = threads.map(t => {
    const msgs = t.messages || [];
    const isActive = t.rmUsername === activeUser;
    const msgsHtml = msgs.length ? msgs.map(m => {
      const side = m.isAdmin ? 'sent' : 'recv';
      const label = m.isAdmin ? '🔴 Admin' : `🟢 ${m.fromName}`;
      // New unseen RM messages get highlight class
      const isNew = !m.isAdmin && !seenSet.has(m.id);
      const newCls = isNew ? ' new-msg' : '';
      return `<div class="msg-wrap ${side}${isNew ? ' msg-new' : ''}">
        <div class="msg-bubble ${side}${newCls}">${escapeHtml(m.text).replace(/\n/g,'<br>')}</div>
        <div class="msg-meta">${label} · ${fmtDate(m.date)}</div>
      </div>`;
    }).join('') : `<div class="rm-empty">No messages yet.</div>`;
    return `<div class="rm-tab-panel${isActive?' active':''}" id="rm-panel-${t.rmUsername}">
      <div class="rm-chat-box" id="rm-chatbox-${t.rmUsername}">${msgsHtml}</div>
      <div class="rm-reply-row">
        <input type="text" id="admin-reply-${t.rmUsername}" placeholder="Reply to ${t.rmName}..." onkeydown="if(event.key==='Enter')adminReplyToRm('${t.rmUsername}')">
        <button class="btn btn-primary" style="padding:7px 14px;font-size:.82rem" onclick="adminReplyToRm('${t.rmUsername}')">Reply</button>
      </div>
    </div>`;
  }).join('');
  el.innerHTML = `<div class="rm-tab-bar">${tabsHtml}</div>${panelsHtml}`;
  // Scroll active chat to bottom
  const activeChatBox = document.getElementById('rm-chatbox-' + activeUser);
  if(activeChatBox) activeChatBox.scrollTop = activeChatBox.scrollHeight;
}

function switchInboxTab(rmUsername){
  document.querySelectorAll('.rm-tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.rm-tab-panel').forEach(p => p.classList.remove('active'));
  const btn = document.querySelector(`.rm-tab-btn[data-rm="${rmUsername}"]`);
  const panel = document.getElementById('rm-panel-' + rmUsername);
  if(btn){
    btn.classList.add('active');
    btn.classList.remove('has-new');
    const badge = btn.querySelector('.rm-tab-unread'); if(badge) badge.remove();
  }
  if(panel) panel.classList.add('active');
  const chatBox = document.getElementById('rm-chatbox-' + rmUsername);
  if(chatBox) chatBox.scrollTop = chatBox.scrollHeight;
  // Mark all messages in this thread as seen
  const adminSeenKey = 'dninvest_admin_seen_msgs';
  const seenArr = JSON.parse(localStorage.getItem(adminSeenKey)||'[]');
  const seenSet = new Set(seenArr);
  const threads = getRmMessagesClean();
  const t = threads.find(x => x.rmUsername === rmUsername);
  if(t) (t.messages||[]).forEach(m => { if(!m.isAdmin) seenSet.add(m.id); });
  // Trim seen list to last 500 IDs
  const newSeen = [...seenSet].slice(-500);
  localStorage.setItem(adminSeenKey, JSON.stringify(newSeen));
  // Remove highlight animation from seen messages
  if(panel) panel.querySelectorAll('.msg-new').forEach(el => el.classList.remove('msg-new'));
  if(panel) panel.querySelectorAll('.new-msg').forEach(el => el.classList.remove('new-msg'));
}


function openMsgPopup(){
  if(!CU || CU.role==='admin') return;
  document.getElementById('msgAdminPopup').classList.add('open');
  renderRmMessages();
}

function closeMsgPopup(){
  document.getElementById('msgAdminPopup').classList.remove('open');
}

// Check if admin has replied to this RM — show popup notification
function checkRmReply(){
  if(!CU) return;
  if(CU.role==='admin'){
    // Admin: check for new RM messages
    const threads = getRmMessages();
    const seenKey = 'dninvest_admin_msg_seen';
    const lastSeen = localStorage.getItem(seenKey) || '';
    let newMsgs = [];
    threads.forEach(t => {
      (t.messages||[]).forEach(m => {
        if(!m.isAdmin && m.id > lastSeen) newMsgs.push({...m, rmName: t.rmName});
      });
    });
    if(!newMsgs.length) return;
    const last = newMsgs[newMsgs.length-1];
    showAdminMsgNotif(last);
    return;
  }
  // RM: check for new admin replies
  const thread = getMyThread();
  if(!thread) return;
  const seenKey = 'dninvest_msg_seen_' + CU.username;
  const lastSeen = localStorage.getItem(seenKey) || '';
  const newReplies = thread.messages.filter(m => m.isAdmin && m.id > lastSeen);
  if(!newReplies.length) return;
  const last = newReplies[newReplies.length-1];
  showRmReplyNotif(last);
}

function showAdminMsgNotif(msg){
  const body = document.getElementById('announcementPopupBody');
  const popup = document.getElementById('announcementPopup');
  body.innerHTML = `<div style="text-align:left">
    <div style="font-size:.8rem;color:var(--gray);margin-bottom:8px">✉️ <b>${escapeHtml(msg.rmName)}</b> sent a message:</div>
    <div class="msg-bubble recv" style="max-width:100%;background:var(--navy);color:#fff">${escapeHtml(msg.text).replace(/\n/g,'<br>')}</div>
    <div style="font-size:.72rem;color:var(--gray);margin-top:8px;text-align:right">${fmtDate(msg.date)}</div>
  </div>`;
  popup.dataset.annId = 'adminnotif_' + msg.id;
  const closeBtn = popup.querySelector('.modal-footer .btn');
  if(closeBtn){
    closeBtn.onclick = function(){
      // Mark all current RM messages as seen for admin
      const allThreads = getRmMessages();
      let lastId = '';
      allThreads.forEach(t => {
        (t.messages||[]).forEach(m => { if(!m.isAdmin && m.id > lastId) lastId = m.id; });
      });
      if(lastId) localStorage.setItem('dninvest_admin_msg_seen', lastId);
      popup.classList.remove('open');
      updateMsgBadge();
      if(getCurrentPageId()==='admin') renderInbox();
    };
  }
  popup.classList.add('open');
}

function showRmReplyNotif(msg){
  // Re-use announcement popup with different styling
  const body = document.getElementById('announcementPopupBody');
  const popup = document.getElementById('announcementPopup');
  body.innerHTML = `<div style="text-align:left">
    <div style="font-size:.8rem;color:var(--gray);margin-bottom:8px">📨 Admin's reply:</div>
    <div class="msg-bubble recv" style="max-width:100%">${escapeHtml(msg.text).replace(/\n/g,'<br>')}</div>
    <div style="font-size:.72rem;color:var(--gray);margin-top:8px;text-align:right">${fmtDate(msg.date)}</div>
  </div>`;
  popup.dataset.annId = 'rmreply_' + msg.id;
  // Override close button to mark rm reply as seen
  const closeBtn = popup.querySelector('.modal-footer .btn');
  if(closeBtn){
    closeBtn.onclick = function(){
      const thread2 = getMyThread();
      if(thread2){
        const lastAdminMsg = thread2.messages.filter(m=>m.isAdmin).slice(-1)[0];
        if(lastAdminMsg) localStorage.setItem('dninvest_msg_seen_' + CU.username, lastAdminMsg.id);
      }
      popup.classList.remove('open');
      updateMsgBadge();
    };
  }
  popup.classList.add('open');
}


let _taTargetUserId = null;

function openTempAccessModal(userId){
  if(!CU || CU.role!=='admin'){ toast('Only admin can do this','error'); return; }
  _taTargetUserId = userId;
  const users = DB.get('users')||[];
  const targetUser = users.find(u=>u.id===userId);
  if(!targetUser){ toast('User not found','error'); return; }

  document.getElementById('ta-rm-name').textContent = targetUser.name;

  // Show all other RMs as options (not admin, not self)
  const otherRMs = users.filter(u=>u.role!=='admin' && u.id!==userId);
  const today = new Date().toISOString().split('T')[0];
  // "Currently active" = not yet expired (expiry could be today or a future date)
  const activeEntries = (targetUser.tempAccess||[]).filter(t=>t.expiry>=today);
  const existing = activeEntries.map(t=>t.absentUserId);

  let listHtml = '';
  otherRMs.forEach(u=>{
    const checked = existing.includes(u.id) ? 'checked' : '';
    listHtml += `<label style="display:flex;align-items:center;gap:8px;cursor:pointer;padding:6px 10px;border-radius:8px;border:1px solid var(--border);background:${checked?'rgba(0,150,136,.08)':'#fff'}">
      <input type="checkbox" value="${u.id}" ${checked} onchange="this.closest('label').style.background=this.checked?'rgba(0,150,136,.08)':'#fff'">
      <span><b>${u.name}</b> <span style="color:var(--gray);font-size:.78rem">(${(u.eq_dealers||[]).concat(u.mf_dealers||[]).filter((v,i,a)=>a.indexOf(v)===i).join(', ')})</span></span>
    </label>`;
  });
  document.getElementById('ta-rm-list').innerHTML = listHtml || '<em style="color:var(--gray)">No other RM found</em>';

  // Default the date picker: keep the latest chosen expiry if any active access exists, else today
  const dateInput = document.getElementById('ta-expiry-date');
  dateInput.min = today;
  const maxExpiry = activeEntries.reduce((m,t)=> t.expiry>m ? t.expiry : m, today);
  dateInput.value = maxExpiry;

  // Show current access with each RM's actual expiry date
  if(activeEntries.length){
    const rows = activeEntries.map(t=>{
      const nm = (users.find(u=>u.id===t.absentUserId)||{}).name || t.absentUserId;
      const untilTxt = t.expiry===today ? 'today' : `until ${t.expiry}`;
      return `${nm} (${untilTxt})`;
    }).join(', ');
    document.getElementById('ta-current').innerHTML = `✅ Current temp access: <b>${rows}</b>`;
  } else {
    document.getElementById('ta-current').textContent = '';
  }

  document.getElementById('tempAccessModal').classList.add('open');
}

async function saveTempAccess(){
  const today = new Date().toISOString().split('T')[0];
  const expiryInput = document.getElementById('ta-expiry-date');
  let expiry = expiryInput ? expiryInput.value : today;
  if(!expiry || expiry < today) expiry = today; // never allow a past date
  const checkboxes = document.querySelectorAll('#ta-rm-list input[type=checkbox]');
  const allShownIds = Array.from(checkboxes).map(cb=>cb.value);
  const selected = [];
  checkboxes.forEach(cb=>{
    if(cb.checked) selected.push({absentUserId: cb.value, expiry});
  });

  let targetName='', grantedNames='', finalTempAccess=null;
  const r = await DB.mutateUsers(users=>{
    const idx = users.findIndex(u=>u.id===_taTargetUserId);
    if(idx<0) return false;
    // Drop any existing entries for RMs shown in this modal (they're being re-set by this save);
    // entries for RMs not shown here (shouldn't normally happen) are left untouched.
    const existing = (users[idx].tempAccess||[]).filter(t=>!allShownIds.includes(t.absentUserId));
    users[idx].tempAccess = [...existing, ...selected];
    targetName = users[idx].name;
    grantedNames = selected.map(s=>(users.find(u=>u.id===s.absentUserId)||{}).name||'').join(', ');
    finalTempAccess = users[idx].tempAccess;
  });
  if(!r.ok || r.aborted){ if(r.aborted) toast('User not found','error'); return; }

  // Update CU if granting to logged-in user
  if(CU.id === _taTargetUserId){
    CU.tempAccess = finalTempAccess;
    // Dropdown turant refresh ho, warna temp RM ka option reload tak nahi aata
    try{ populateRmDropdowns(); }catch(e){}
  }

  closeModal('tempAccessModal');
  renderAdmin();
  const untilTxt = expiry===today ? 'for today' : `until ${expiry}`;
  if(selected.length){
    toast(`✅ ${targetName} has been given access to ${grantedNames} ${untilTxt}!`, 'success');
  } else {
    toast(`🔄 Removed ${targetName}'s temporary access`, 'info');
  }
}

// Check & auto-expire temp access on load
async function cleanExpiredTempAccess(){
  const today = new Date().toISOString().split('T')[0];
  await DB.mutateUsers(users=>{
    let changed = false;
    users.forEach((u,i)=>{
      if(!u.tempAccess?.length) return;
      const valid = u.tempAccess.filter(t=>t.expiry>=today);
      if(valid.length !== u.tempAccess.length){ users[i].tempAccess=valid; changed=true; }
    });
    if(!changed) return false; // nothing expired — skip the write entirely
  });
}


function openChangeCredModal(){
  if(!CU) return;
  document.getElementById('cc_current').value='';
  document.getElementById('cc_newpass').value='';
  document.getElementById('cc_newpin').value='';
  document.getElementById('cc_err').style.display='none';
  // Show PIN field only for RMs
  document.getElementById('cc_pin_wrap').style.display = CU.role==='rm' ? '' : 'none';
  document.getElementById('changeCredModal').classList.add('open');
}

async function saveChangeCred(){
  if(!CU) return;
  const current = document.getElementById('cc_current').value;
  const newPass = document.getElementById('cc_newpass').value;
  const newPin  = document.getElementById('cc_newpin').value;
  const errEl   = document.getElementById('cc_err');

  // Verify current password
  if(current !== CU.password){
    errEl.textContent = '❌ Current password is incorrect';
    errEl.style.display = '';
    return;
  }
  // Validate new PIN if entered
  if(newPin && !/^[0-9]{4}$/.test(newPin)){
    errEl.textContent = '❌ PIN must be 4 digits (0-9)';
    errEl.style.display = '';
    return;
  }
  if(!newPass && !newPin){
    errEl.textContent = '❌ Change something — password or PIN';
    errEl.style.display = '';
    return;
  }

  const r = await DB.mutateUsers(users=>{
    const idx = users.findIndex(u => u.id === CU.id);
    if(idx<0) return false;
    if(newPass) users[idx].password = newPass;
    if(newPin && CU.role==='rm') users[idx].pin = newPin;
  });
  if(!r.ok || r.aborted){ toast('User not found','error'); return; }

  // Update persistent session so auto-login keeps working with the new credential
  if(newPass){ CU.password = newPass; localStorage.setItem('dninvest_session', JSON.stringify({username:CU.username, password:newPass, at:Date.now()})); }
  if(newPin && CU.role==='rm'){ CU.pin = newPin; localStorage.setItem('dninvest_session', JSON.stringify({username:CU.username, password:newPin, at:Date.now()})); }

  closeModal('changeCredModal');
  toast('✅ Password/PIN updated!', 'success');
}


function onUserRoleChange(sel){
  const pinWrap = document.getElementById('uf_pin_wrap');
  if(pinWrap) pinWrap.style.display = sel.value==='rm' ? '' : 'none';
  const mfDeskWrap = document.getElementById('uf_mfdesk_wrap');
  if(mfDeskWrap) mfDeskWrap.style.display = sel.value==='rm' ? '' : 'none';
  const riskWrap = document.getElementById('uf_risk_wrap');
  if(riskWrap) riskWrap.style.display = sel.value==='rm' ? '' : 'none';
  const boWrap = document.getElementById('uf_backoffice_wrap');
  if(boWrap) boWrap.style.display = sel.value==='rm' ? '' : 'none';
}


function toggleDNC(cb){
  const dateInput = document.getElementById('f_next_call');
  if(!dateInput) return;
  if(cb.checked){
    dateInput.value = '';
    dateInput.disabled = true;
  } else {
    dateInput.disabled = false;
  }
}


function showDncReportEq(){ showDncReport('equity'); }
function showDncReportMf(){ showDncReport('mf'); }

function showDncReport(seg){
  // Use getActiveEqClients/getMyMfClients so RM sees only their own clients
  const data = seg==='equity'
    ? getActiveEqClients().filter(c=>c.do_not_call)
    : getMyMfClients().filter(c=>c.do_not_call);
  if(!data.length){ toast('No DNC clients found','info'); return; }
  const cols = seg==='equity'
    ? ['Code','Name','Mobile','RM','Follow-up','Remarks']
    : ['Name','Mobile','PAN','RM','AUM','Follow-up','Remarks'];
  const rows = data.map(c=> seg==='equity'
    ? [c.code||'—', c.name, c.mobile||'—', c.rm||'—', c.followup_status||'—', c.remarks||'—']
    : [c.name, c.mobile||'—', c.pan||'—', c.rm||'—', c.aum?'₹'+fmtNum(c.aum):'—', c.followup_status||'—', c.remarks||'—']
  );
  showReport('🚫 DNC Clients — '+(seg==='equity'?'Equity':'MF')+' ('+data.length+')', cols, rows);
}


function escapeHtml(str){
  const d = document.createElement('div');
  d.textContent = str;
  return d.innerHTML;
}

// ══════════════════════════════════════════
// PAGINATION
// ══════════════════════════════════════════
function renderPg(tab,tot,cur){
  const el=document.getElementById(tab+'-pg');
  const pages=Math.ceil(tot/PG_SIZE);
  if(!el||pages<=1){if(el)el.innerHTML='';return;}
  let h=`<button class="pg-btn" onclick="gp('${tab}',${cur-1})" ${cur===1?'disabled':''}>‹</button>`;
  let rng=[];
  for(let i=1;i<=pages;i++){
    if(i===1||i===pages||Math.abs(i-cur)<=2) rng.push(i);
    else if(rng[rng.length-1]!=='...') rng.push('...');
  }
  rng.forEach(r=>{ if(r==='...') h+=`<span class="pg-info">…</span>`; else h+=`<button class="pg-btn ${r===cur?'active':''}" onclick="gp('${tab}',${r})">${r}</button>`; });
  h+=`<button class="pg-btn" onclick="gp('${tab}',${cur+1})" ${cur===pages?'disabled':''}>›</button>`;
  h+=`<span class="pg-info">${cur}/${pages} (${tot})</span>`;
  el.innerHTML=h;
}

function gp(tab,p){
  const tot=tab==='eq'?eqFiltered.length:tab==='mf'?mfFiltered.length:leadsFiltered.length;
  const pages=Math.ceil(tot/PG_SIZE);
  if(p<1||p>pages) return;
  if(tab==='eq'){eqPage=p;renderEqTable();}
  else if(tab==='mf'){mfPage=p;renderMfTable();}
  else{leadsPage=p;renderLeadsTable();}
}

// ══════════════════════════════════════════
// UTILS
// ══════════════════════════════════════════
function closeModal(id){ document.getElementById(id).classList.remove('open'); }

// ══ SIP DETAILS POPUP (SIP Cnt pe click) ══
function sipCntCell(c){
  const n = parseInt(c.sip_count)||0;
  if(!n) return '—';
  const has = Array.isArray(c.sip_details) && c.sip_details.length;
  if(!has) return n;
  return `<span onclick="showSipDetails('${c.id}')" title="Click for scheme-wise SIP details"
    style="cursor:pointer;background:var(--teal,#0d9488);color:#fff;border-radius:6px;padding:1px 8px;font-weight:700;font-size:.78rem;display:inline-block">${n}</span>`;
}

function showSipDetails(id){
  const list = DB.get('mf_clients')||[];
  const c = list.find(x=>x.id===id);
  if(!c) return;
  const d = Array.isArray(c.sip_details)?c.sip_details:[];
  if(!d.length){ toast('SIP details are not available — please import the Running SIP Report','error'); return; }
  const total = d.reduce((s,x)=>s+(parseFloat(x.amount)||0),0);
  document.getElementById('sipDetailTitle').innerHTML = `SIP Details — ${c.name} <span style="font-weight:500;opacity:.7">(${d.length} SIP${d.length>1?'s':''})</span>`;
  let h = `<div style="overflow:auto"><table class="tbl" style="width:100%;font-size:.85rem">
    <thead><tr>
      <th style="text-align:left">Scheme</th>
      <th style="text-align:right;white-space:nowrap">SIP Amount</th>
      <th style="text-align:center;white-space:nowrap">SIP Date</th>
    </tr></thead><tbody>`;
  d.forEach(x=>{
    h += `<tr>
      <td style="text-align:left">${x.scheme||'—'}${(x.folio||x.freq||x.trxn)?`<div style="font-size:.7rem;opacity:.6">${[x.folio?'Folio: '+x.folio:'', x.freq||'', x.trxn?'Trxn: '+x.trxn:''].filter(Boolean).join(' · ')}</div>`:''}</td>
      <td style="text-align:right;font-weight:700;color:var(--teal,#0d9488);white-space:nowrap">₹${fmtNum(x.amount||0)}</td>
      <td style="text-align:center;white-space:nowrap">${x.day||'—'}</td>
    </tr>`;
  });
  h += `</tbody><tfoot><tr style="font-weight:800;border-top:2px solid var(--teal,#0d9488)">
      <td style="text-align:left">TOTAL (${d.length})</td>
      <td style="text-align:right;white-space:nowrap">₹${fmtNum(total)}</td>
      <td></td>
    </tr></tfoot></table></div>`;
  document.getElementById('sipDetailBody').innerHTML = h;
  document.getElementById('sipDetailModal').classList.add('open');
}

let toastTimer;
function toast(msg,type=''){
  const el=document.getElementById('toast');
  el.textContent=msg; el.className='show '+(type||'');
  clearTimeout(toastTimer);
  toastTimer=setTimeout(()=>el.className='',3000);
}
// ══ IMPORT SYSTEM ══
var importData = { aum: null, sip: null, contact: null, activeTab: 'aum' };

function openImportModal(){
  importData = { aum: null, sip: null, contact: null, activeTab: 'aum' };
  document.getElementById('importModal').classList.add('open');
}

function switchImportTab(tab){
  importData.activeTab = tab;
  ['aum','sip','both','contact'].forEach(t=>{
    document.getElementById('imp-'+t).style.display = t===tab?'block':'none';
    document.getElementById('imp-tab-'+t).classList.toggle('active', t===tab);
  });
  checkImportReady();
}

function checkImportReady(){
  const btn = document.getElementById('importBtn');
  const tab = importData.activeTab;
  if(tab==='aum' && importData.aum) btn.disabled=false;
  else if(tab==='sip' && importData.sip) btn.disabled=false;
  else if(tab==='both' && importData.aum && importData.sip) btn.disabled=false;
  else if(tab==='contact' && importData.contact) btn.disabled=false;
  else btn.disabled=true;
}

function readExcel(file, cb){
  const reader = new FileReader();
  reader.onload = function(e){
    try{
      const wb = XLSX.read(e.target.result, {type:'binary', cellDates:true});
      const ws = wb.Sheets[wb.SheetNames[0]];
      const rows = XLSX.utils.sheet_to_json(ws, {header:1, defval:''});
      cb(null, rows);
    }catch(err){ cb(err); }
  };
  reader.readAsBinaryString(file);
}

// Parse the "AUM By Client" report.
// HEADER-BASED, deliberately. This used to read fixed column positions
// (pan=r[2], inv=r[3], aum=r[6]) which matched the OLD report layout:
//   S.No | Client Name | PAN | Inv. Amt | Div. Paid | Div. ReInv | AUM
// The RTA later inserted a "Client ID" column at position 2, shifting every
// column after it one to the right. The old code kept reading blindly, so
// Client IDs were written into the PAN field (that's where junk PANs like
// "2537656" came from) and AUM silently read Div. ReInv instead. Never trust
// positions in this file again — read the header row.
function parseAumExcel(rows){
  const wanted = {
    name:    ['clientname','name','investorname','clientnames'],
    pan:     ['pan','pannumber','pancard','panno','pancardno'],
    client_id: ['clientid','clientcode','clientno','clientidno'],
    aum:     ['aum','currentvalue','marketvalue','closingbalance','currentamt'],
    inv_amt: ['invamt','invamount','investmentamount','investedamount','purchaseamount','investment'],
    // Performance columns, shown when the AUM cell is clicked.
    // NOTE: "Today's P/L" and "Today's P/L %" BOTH normalise to "todayspl",
    // so they can't be told apart by header alone — and they go stale daily
    // anyway. Deliberately not imported.
    div_paid:  ['divpaid','dividendpaid'],
    div_reinv: ['divreinv','dividendreinv','divreinvest'],
    avg_days:  ['avgdays','averagedays'],
    gain_loss: ['gainloss','gain','profitloss'],
    abs_rtn:   ['absrtn','absreturn','absolutereturn'],
    xirr:      ['xirr','xirrpct'],
    rm:      ['rm','relationshipmanager','dealer','dealername','employeename'],
    sno:     ['sno','srno','serialno','slno']
  };
  let hdrIdx=-1, colMap={};
  for(let i=0; i<Math.min(rows.length,15); i++){
    const map={};
    (rows[i]||[]).forEach((cell,ci)=>{
      const h=normHdr(cell);
      for(const [f,vars] of Object.entries(wanted)){
        if(vars.includes(h) && map[f]===undefined) map[f]=ci;
      }
    });
    // Name + AUM are the minimum we need to call this a real header row.
    if(map.name!==undefined && map.aum!==undefined){ hdrIdx=i; colMap=map; break; }
  }
  // No header found -> return null so the caller aborts with a clear message.
  // (The old code assumed row 7 here and imported garbage instead.)
  if(hdrIdx===-1) return null;

  const isPan = v => /^[A-Z]{5}[0-9]{4}[A-Z]$/.test(String(v||'').trim().toUpperCase());
  const num = v => Math.round((parseFloat(String(v==null?'':v).replace(/[,\s₹]/g,''))||0)*100)/100;
  const at = (r,f) => colMap[f]!==undefined ? r[colMap[f]] : '';

  const data = rows.slice(hdrIdx+1).filter(r=>{
    if(!r || !r.some(c=>c!=='' && c!=null)) return false;
    // Data rows carry a serial number; totals/footer rows don't.
    if(colMap.sno!==undefined) return String(r[colMap.sno]||'').trim().match(/^\d+$/);
    return String(at(r,'name')||'').trim() !== '';
  });

  const out = data.map(r=>{
    const rawPan = String(at(r,'pan')||'').trim().toUpperCase();
    return {
      sno: colMap.sno!==undefined ? r[colMap.sno] : '',
      name: String(at(r,'name')||'').trim(),
      // Only accept a real PAN. Anything else (Client ID, folio, blank) is
      // dropped rather than written over a client's good PAN.
      pan: isPan(rawPan) ? rawPan : '',
      // Client ID is the RTA's own stable key. We keep it because it's the only
      // field that survives name changes — and because the wrong-column bug
      // wrote these very IDs into the PAN field, it's how we find those records.
      client_id: String(at(r,'client_id')||'').trim(),
      inv_amt: num(at(r,'inv_amt')),
      aum: num(at(r,'aum')),
      div_paid:  num(at(r,'div_paid')),
      div_reinv: num(at(r,'div_reinv')),
      avg_days:  num(at(r,'avg_days')),
      gain_loss: num(at(r,'gain_loss')),
      abs_rtn:   num(at(r,'abs_rtn')),
      xirr:      num(at(r,'xirr')),
      rm: String(at(r,'rm')||'').trim()
    };
  }).filter(r=>r.name);

  out._badPan = data.length - out.filter(r=>r.pan).length;
  return out;
}

// Parse the "Running SIP Report".
// HEADER-BASED for the same reason parseAumExcel is: this used to read fixed
// positions (amt=r[6], cnt=r[7], scheme=r[2], date=r[9]). Those happen to be
// right for today's layout, but the AUM report proved the RTA will insert a
// column without warning — and when that happens, parseFloat() on the wrong
// column quietly yields 0 instead of failing loudly. Read the header.
//
// Name column looks like:  "SURESH SAHU [AOFPS2567B] [1854321]"
//                            name        PAN          Client ID
// Minors have an empty PAN bracket: "Saanvi Agarwal [] [4993137]".
function parseSipExcel(rows){
  const wanted = {
    sno:        ['sno','srno','serialno','slno'],
    name:       ['name','clientname','investorname'],
    scheme:     ['scheme','schemename','fundname','fund'],
    folio:      ['foliono','folio','folionumber'],
    freq:       ['sipfrequency','frequency'],
    amount:     ['sipmonthlyamount','monthlyamount'],
    reg_amount: ['sipregisteredamount','registeredamount'],
    count:      ['sipcount'],
    day:        ['sipdate'],
    trxn:       ['siptrxnno','siptransactionno','siptrxn','trxnno','transactionno']
  };
  let hdrIdx=-1, colMap={};
  for(let i=0; i<Math.min(rows.length,15); i++){
    const map={};
    (rows[i]||[]).forEach((cell,ci)=>{
      const h=normHdr(cell);
      for(const [f,vars] of Object.entries(wanted)){
        if(vars.includes(h) && map[f]===undefined) map[f]=ci;
      }
    });
    if(map.name!==undefined && (map.amount!==undefined || map.reg_amount!==undefined)){
      hdrIdx=i; colMap=map; break;
    }
  }
  if(hdrIdx===-1) return null;   // caller aborts — never guess positions

  // "SIP Monthly Amount" is the one we want. Fall back to the registered
  // amount only if the monthly column is missing entirely.
  const amtCol = colMap.amount!==undefined ? colMap.amount : colMap.reg_amount;
  const usedRegAmt = colMap.amount===undefined;
  const at = (r,f) => colMap[f]!==undefined ? r[colMap[f]] : '';
  const num = v => Math.round((parseFloat(String(v==null?'':v).replace(/[,\s₹]/g,''))||0)*100)/100;

  const data = rows.slice(hdrIdx+1).filter(r=>{
    if(!r || !r.some(c=>c!=='' && c!=null)) return false;
    if(colMap.sno!==undefined) return String(r[colMap.sno]||'').trim().match(/^\d+$/);
    return String(at(r,'name')||'').trim() !== '';
  });

  const sipMap = {};
  data.forEach(r => {
    const nameRaw = String(at(r,'name')||'');
    const name = nameRaw.replace(/\[.*?\]/g,'').replace(/\s+/g,' ').trim();
    const panMatch = nameRaw.match(/\[([A-Z]{5}\d{4}[A-Z])\]/);
    const pan = panMatch ? panMatch[1] : '';        // minors: empty bracket -> ''
    const cidMatch = nameRaw.match(/\[(\d{4,})\]/);   // 2nd bracket = Client ID
    const clientId = cidMatch ? cidMatch[1] : '';
    const amt = num(r[amtCol]);
    const cnt = parseInt(at(r,'count'))||0;
    // Bucket by Client ID first — it is unique per person. A minor can carry the
    // guardian's PAN, so keying by PAN would merge guardian+minor SIPs (double amount/count).
    const key = clientId ? ('CID:'+clientId) : (pan || name.toUpperCase());
    if(!key) return;
    if(!sipMap[key]){ sipMap[key] = {name, pan, client_id:clientId, sip_amount:0, sip_count:0, sip_details:[]}; }
    // PAN alias ONLY when there is no Client ID — otherwise a shared PAN (guardian↔minor)
    // would point two different people at the same bucket and double their totals.
    if(!clientId && pan && !sipMap[pan]) sipMap[pan] = sipMap[key];
    sipMap[key].sip_amount += amt;
    sipMap[key].sip_count  += cnt;
    sipMap[key].sip_details.push({
      scheme: String(at(r,'scheme')||'').trim(),
      amount: amt,
      day:    String(at(r,'day')||'').trim(),
      folio:  String(at(r,'folio')||'').trim(),
      freq:   String(at(r,'freq')||'').trim(),
      trxn:   String(at(r,'trxn')||'').trim()
    });
  });
  Object.values(sipMap).forEach(v => v.sip_details.sort((a,b)=>(b.amount||0)-(a.amount||0)));
  Object.defineProperty(sipMap, '_usedRegAmt', {value:usedRegAmt, enumerable:false});
  return sipMap;
}

function handleAumFile(input){
  const file = input.files[0];
  if(!file) return;
  readExcel(file, function(err, rows){
    if(err){ toast('AUM file read error: '+err.message,'error'); return; }
    const parsed = parseAumExcel(rows);
    if(!parsed || !parsed.length){
      importData.aum = null;
      document.getElementById('aum-preview').innerHTML =
        `<div style="background:var(--red2,#fee);color:var(--red,#c00);padding:10px;border-radius:8px;font-size:.85rem;font-weight:600">
         ❌ Header row not found (Client Name + AUM columns are required). Please check the file format — import stopped.</div>`;
      checkImportReady(); return;
    }
    importData.aum = parsed;
    const warn = parsed._badPan ? `<div style="color:var(--orange,#c60);font-size:.8rem;font-weight:600;margin-top:4px">⚠️ ${parsed._badPan} rows have no valid PAN — those clients' PAN will be left untouched</div>` : '';
    document.getElementById('aum-preview').innerHTML = 
      `<div style="background:var(--green2);color:var(--green);padding:10px;border-radius:8px;font-size:.85rem;font-weight:600">
       ✅ ${parsed.length} clients found in AUM file</div>${warn}`;
    checkImportReady();
  });
}

function handleSipFile(input){
  const file = input.files[0];
  if(!file) return;
  readExcel(file, function(err, rows){
    if(err){ toast('SIP file read error: '+err.message,'error'); return; }
    const parsedS = parseSipExcel(rows);
    if(!parsedS || !Object.keys(parsedS).length){
      importData.sip = null;
      document.getElementById('sip-preview').innerHTML =
        `<div style="background:var(--red2,#fee);color:var(--red,#c00);padding:10px;border-radius:8px;font-size:.85rem;font-weight:600">
         ❌ Header row not found (Name + SIP Monthly Amount columns are required). Please check the file format — import stopped.</div>`;
      checkImportReady(); return;
    }
    importData.sip = parsedS;
    const cnt = Object.keys(parsedS).length;
    const rw = parsedS._usedRegAmt ? `<div style="color:var(--orange,#c60);font-size:.8rem;font-weight:600;margin-top:4px">⚠️ "SIP Monthly Amount" column not found — used "SIP Registered Amount" instead</div>` : '';
    document.getElementById('sip-preview').innerHTML = 
      `<div style="background:var(--green2);color:var(--green);padding:10px;border-radius:8px;font-size:.85rem;font-weight:600">
       ✅ ${cnt} SIP clients found</div>${rw}`;
    checkImportReady();
  });
}

function handleBothFile(type, input){
  const file = input.files[0];
  if(!file) return;
  document.getElementById('both-'+type+'-name').textContent = file.name;
  readExcel(file, function(err, rows){
    if(err){ toast('Error: '+err.message,'error'); return; }
    if(type==='aum'){
      const parsedB = parseAumExcel(rows);
      if(!parsedB || !parsedB.length){
        importData.aum = null;
        document.getElementById('both-preview').innerHTML +=
          `<div style="color:var(--red,#c00);font-size:.82rem;font-weight:600">❌ AUM: header row not found — please check the format</div>`;
        checkImportReady(); return;
      }
      importData.aum = parsedB;
      document.getElementById('both-preview').innerHTML += 
        `<div style="color:var(--green);font-size:.82rem;font-weight:600">✅ AUM: ${parsedB.length} clients${parsedB._badPan?' (⚠️ '+parsedB._badPan+' without a valid PAN)':''}</div>`;
    } else {
      const parsedSB = parseSipExcel(rows);
      if(!parsedSB || !Object.keys(parsedSB).length){
        importData.sip = null;
        document.getElementById('both-preview').innerHTML +=
          `<div style="color:var(--red,#c00);font-size:.82rem;font-weight:600">❌ SIP: header row not found — please check the format</div>`;
        checkImportReady(); return;
      }
      importData.sip = parsedSB;
      const cnt = Object.keys(parsedSB).length;
      document.getElementById('both-preview').innerHTML += 
        `<div style="color:var(--green);font-size:.82rem;font-weight:600">✅ SIP: ${cnt} clients${parsedSB._usedRegAmt?' (⚠️ Registered Amount used)':''}</div>`;
    }
    checkImportReady();
  });
}

// Normalise a DOB cell (Excel serial, Date, or various string formats) → yyyy-mm-dd
function _dP2(x){ return String(x).padStart(2,'0'); }
function normDob(v){
  if(v===undefined||v===null||v==='') return '';
  if(v instanceof Date && !isNaN(v)) return v.getFullYear()+'-'+_dP2(v.getMonth()+1)+'-'+_dP2(v.getDate());
  // ── Compact YYYYMMDD (broker files: 19561009 → 1956-10-09) ──
  const _c = String(v).trim();
  if(['','0','null','na','n/a','-','00000000'].includes(_c.toLowerCase())) return '';
  if(/^(19|20)\d{6}$/.test(_c)){
    const _mo=+_c.slice(4,6), _da=+_c.slice(6,8);
    if(_mo>=1 && _mo<=12 && _da>=1 && _da<=31) return _c.slice(0,4)+'-'+_c.slice(4,6)+'-'+_c.slice(6,8);
  }
  if(typeof v==='number' && v>0 && v<60000){
    const d=new Date(Math.round((v-25569)*86400*1000));
    if(!isNaN(d)) return d.getUTCFullYear()+'-'+_dP2(d.getUTCMonth()+1)+'-'+_dP2(d.getUTCDate());
  }
  let s=String(v).trim(); if(!s) return '';
  let m=s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);        if(m) return m[1]+'-'+_dP2(m[2])+'-'+_dP2(m[3]);
  m=s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);            if(m) return m[3]+'-'+_dP2(m[2])+'-'+_dP2(m[1]);
  m=s.match(/^(\d{1,2})[-\s]([A-Za-z]{3,})[-\s](\d{4})$/);
  if(m){ const mo=['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'].indexOf(m[2].slice(0,3).toLowerCase()); if(mo>=0) return m[3]+'-'+_dP2(mo+1)+'-'+_dP2(m[1]); }
  const d=new Date(s); if(!isNaN(d)) return d.getFullYear()+'-'+_dP2(d.getMonth()+1)+'-'+_dP2(d.getDate());
  return '';
}
// Parse a simple Name/Mobile/PAN/DOB sheet (auto-detect header columns)
function parseContactExcel(rows){
  let hdrIdx=-1, colMap={};
  const wanted = {
    name: ['name','clientname','investorname'],
    mobile: ['mobile','mobileno','phone','contactno','mobilenumber'],
    pan: ['pan','pannumber','pancard'],
    dob: ['dob','dateofbirth','birthdate','birthday','dateofbirthdob','invdob']
  };
  for(let i=0;i<Math.min(rows.length,10);i++){
    const row = rows[i];
    const map = {};
    row.forEach((cell,ci)=>{
      const h = normHdr(cell);
      for(const [field, variants] of Object.entries(wanted)){
        if(variants.includes(h) && map[field]===undefined) map[field]=ci;
      }
    });
    if(map.name!==undefined && (map.mobile!==undefined || map.pan!==undefined || map.dob!==undefined)){
      hdrIdx=i; colMap=map; break;
    }
  }
  if(hdrIdx===-1) return null;
  return rows.slice(hdrIdx+1)
    .filter(r=>r.some(c=>c!==''))
    .map(r=>({
      name: colMap.name!==undefined ? String(r[colMap.name]||'').trim() : '',
      mobile: colMap.mobile!==undefined ? String(r[colMap.mobile]||'').replace(/\D/g,'').slice(-10) : '',
      pan: colMap.pan!==undefined ? String(r[colMap.pan]||'').trim().toUpperCase() : '',
      dob: colMap.dob!==undefined ? normDob(r[colMap.dob]) : ''
    }))
    .filter(r=>r.name);
}

function handleContactFile(input){
  const file = input.files[0];
  if(!file) return;
  readExcel(file, function(err, rows){
    if(err){ toast('File read error: '+err.message,'error'); return; }
    const data = parseContactExcel(rows);
    if(!data){
      toast('Header row not found. Excel must have Name and Mobile/PAN columns.','error');
      return;
    }
    importData.contact = data;
    document.getElementById('contact-preview').innerHTML =
      `<div style="background:var(--green2);color:var(--green);padding:10px;border-radius:8px;font-size:.85rem;font-weight:600">
       ✅ ${data.length} rows found</div>`;
    checkImportReady();
  });
}

async function doContactImport(){
  const data = importData.contact;
  if(!data || !data.length) return;
  const existing = DB.get('mf_clients')||[];
  const byPanGroup = {};
  const byNameGroup = {};
  // PAN is usually unique, but a minor's folio is often registered under their
  // guardian's PAN — so the SAME PAN can legitimately belong to several client
  // records (the guardian + each child). Always group by PAN (never assume 1:1),
  // and use the name to pick the right one within a shared-PAN group.
  existing.forEach(c=>{
    if(c.pan){
      const pk = c.pan.trim().toUpperCase();
      if(!byPanGroup[pk]) byPanGroup[pk] = [];
      byPanGroup[pk].push(c);
    }
    if(c.name){
      const nk = c.name.trim().toUpperCase();
      if(!byNameGroup[nk]) byNameGroup[nk] = [];
      byNameGroup[nk].push(c);
    }
  });

  let updated=0, notFound=0, ambiguous=0;
  const touched=[];
  // If multiple rows share the same name, keep the first mobile/pan found
  const seen = new Set();
  const ambigNames = new Set();
  data.forEach(row=>{
    const key = row.name.trim().toUpperCase();
    const rowPan = (row.pan||'').trim().toUpperCase();
    let ex = null;

    // 1) PAN match — narrow by name too when the PAN is shared (guardian + minors case),
    // since a child's name still differs from their guardian's/siblings' names.
    if(rowPan && byPanGroup[rowPan]){
      const panGroup = byPanGroup[rowPan];
      if(panGroup.length===1){
        ex = panGroup[0];
      } else {
        const narrowedByName = panGroup.filter(c=>c.name && c.name.trim().toUpperCase()===key);
        if(narrowedByName.length===1) ex = narrowedByName[0];
      }
    }

    // 2) Fall back to name matching if PAN alone didn't resolve it.
    if(!ex){
      const group = byNameGroup[key] || [];
      if(group.length===1){
        ex = group[0];
      } else if(group.length>1){
        // CRM has multiple investors with this exact name — only proceed if this
        // row's PAN narrows it down to exactly one of them; otherwise stay skipped,
        // same as before, rather than risk updating the wrong person.
        if(rowPan){
          const narrowed = group.filter(c=>c.pan && c.pan.trim().toUpperCase()===rowPan);
          if(narrowed.length===1) ex = narrowed[0];
        }
        if(!ex){ ambiguous++; ambigNames.add(row.name.trim()); return; }
      }
    }

    if(ex){
      let changed=false;
      if(row.mobile && ex.mobile!==row.mobile){ ex.mobile=row.mobile; changed=true; }
      if(row.pan && ex.pan!==row.pan){ ex.pan=row.pan; changed=true; }
      if(row.dob && ex.dob!==row.dob){ ex.dob=row.dob; changed=true; }
      if(changed && !seen.has(ex.id)){
        ex.updated = today();
        touched.push(ex);
        seen.add(ex.id);
        updated++;
      }
    } else {
      notFound++;
    }
  });

  await DB.setClientsBulk('mf_clients', touched);
  closeModal('importModal');
  let cmsg = `✅ Contact info updated! ${updated} updated`;
  if(notFound) cmsg += `, ${notFound} not matched`;
  if(ambiguous) cmsg += `, ${ambiguous} skipped (duplicate name, no match via PAN either)`;
  toast(cmsg, 'success');
  renderMfTable(); refreshDash(); updateBadges();

  // Duplicate naam wale rows skip hue (PAN se bhi resolve nahi hue) — inhe manually update karna padega
  if(ambigNames.size){
    setTimeout(()=>{
      showReport(`Duplicate Names — Skipped (${ambigNames.size} names / ${ambiguous} rows)`,
        ['#','Naam','Wajah'],
        [...ambigNames].map((n,i)=>[i+1, n, 'The CRM has 1+ investors with this same name, and a PAN match could not confirm which one — please update manually']));
    }, 600);
  }
}

async function doImport(){
  if(importData.activeTab==='contact'){ await doContactImport(); return; }
  const tab = importData.activeTab;
  const existing = DB.get('mf_clients')||[];
  // Naam ko normalize karo: uppercase + extra spaces hatao. Isse same aadmi ki
  // formatting-difference (double space, case) se nayi duplicate row nahi banti.
  const nmKey = s => String(s||'').toUpperCase().replace(/\s+/g,' ').trim();
  const mob10 = m => String(m||'').replace(/\D/g,'').slice(-10);
  const validPan = p => /^[A-Z]{5}[0-9]{4}[A-Z]$/.test(String(p||'').trim().toUpperCase());

  // Snapshot each client's SIP list BEFORE any mutation — needed to diff
  // old vs new (added / updated / closed) for the SIP upload summary.
  const oldSipById = {};
  if(importData.sip) existing.forEach(c=>{ oldSipById[c.id] = Array.isArray(c.sip_details) ? c.sip_details.slice() : []; });
  
  // Build lookup from existing clients
  // Match on identity, in order of how much we trust it:
  //   1. Client ID  — RTA's stable key. Also catches records poisoned by the
  //                   old wrong-column bug, whose `pan` field literally holds
  //                   a Client ID.
  //   2. PAN        — exact, once the data is clean. MINORS EXCLUDED from this
  //                   map — a minor legally carries the GUARDIAN's PAN, so
  //                   PAN-matching would misfile the minor's AUM onto the
  //                   guardian. Minors match via Client ID / Mobile instead.
  //   3. Mobile     — 10-digit match. AUM file me mobile nahi hota, par contact/
  //                   dusre imports me hota hai — tab kaam aata hai. Zero risk.
  //   4. Name       — ONLY when it is unique on both sides. Kai naam (3 alag RAJ
  //                   KUMAR) alag-alag logon ke hain; unhe naam se match karna ek
  //                   ka data doosre pe likh dega. Isliye skip + report.
  const existingMap = {}, byClientId = {}, byMobile = {}, nameMap = {}, nameCount = {};
  // How many existing clients (guardian + any minors) share each PAN. A minor
  // legally carries the guardian's PAN, so a family can have 2+ CRM records
  // on the very same PAN. If a file row's Client ID doesn't resolve (e.g. the
  // minor's own Client ID was never captured), falling back to PAN alone
  // would silently attribute that row to whichever one record happens to sit
  // in existingMap — usually the guardian — even when the row was actually
  // the minor's own transaction. panOwnerCount lets the PAN-fallback below
  // detect "this PAN belongs to more than one person" and refuse to guess.
  const panOwnerCount = {};
  existing.forEach(c => {
    const p = String(c.pan||'').trim().toUpperCase();
    if(p && /^[A-Z]{5}[0-9]{4}[A-Z]$/.test(p)){
      panOwnerCount[p] = (panOwnerCount[p]||0)+1;
      if(!c.is_minor) existingMap[p] = c;
    }
    if(c.client_id) byClientId[String(c.client_id).trim()] = c;
    if(/^\d{4,}$/.test(p) && !c.is_minor) byClientId[p] = c;      // legacy: Client ID stuck in the PAN field
    const m = mob10(c.mobile); if(m.length===10) byMobile[m] = c;
    const n = nmKey(c.name);
    if(n){ nameCount[n] = (nameCount[n]||0)+1; nameMap[n] = c; }
  });
  // How often each name appears in the uploaded file itself (AUM tab; SIP-only
  // tab builds its own count just below since it's a separate object shape)
  const fileNameCount = {};
  (importData.aum||[]).forEach(r => {
    const n = nmKey(r.name);
    if(n) fileNameCount[n] = (fileNameCount[n]||0)+1;
  });
  const ambigRows = [];      // couldn't be identified safely
  const mfChangeLogBatch = []; // Invested Amount changes collected here, written in ONE transaction at the end (see addMfChangeLogBatch)
  const claimed = new Set(); // guard: never let two file rows write the same client
  
  // Compact performance snapshot kept on the MF client so the AUM cell can be
  // clicked open. Short keys on purpose — this rides along in the mf_clients
  // blob doc, which every open tab re-downloads on change.
  const _aumDetail = row => ({
    inv: row.inv_amt||0, dp: row.div_paid||0, dr: row.div_reinv||0,
    ad: row.avg_days||0, gl: row.gain_loss||0, ar: row.abs_rtn||0,
    xirr: row.xirr||0, on: today()
  });

  let updated = 0, added = 0;
  const sipApplied = new Set();   // client ids that got fresh SIP data this import
  let sipAdded = 0, sipUpdated = 0, sipClosed = 0, sipClientsCleared = 0;
  const newClients = [];
  
  if(importData.aum){
    importData.aum.forEach(row => {
      const upName = nmKey(row.name);
      // Match order: Client ID → PAN → Mobile → Name.
      // PAN is skipped when panOwnerCount[pan] > 1 — a guardian+minor family
      // sharing one PAN — because without a Client ID we can't safely tell
      // whose transaction this row actually is; guessing "the guardian" would
      // silently overwrite the guardian's record with the minor's numbers (or
      // vice versa). That case falls through to Name matching / ambiguous
      // instead, same as if PAN had found nothing.
      const panIsUnambiguous = row.pan && (panOwnerCount[row.pan]||0) <= 1;
      // Same guard applies to pulling in this row's SIP data (sip_amount/
      // sip_count/sip_details from the separately-parsed SIP report) — this
      // used to fall back to `importData.sip[row.pan]` unconditionally, which
      // is exactly the same guardian/minor mix-up bug as the client-match
      // above, just one step further down: even after `ex` correctly resolved
      // to the right person, their record could still get the OTHER family
      // member's SIP numbers glued on via this PAN-keyed lookup.
      const sipInfo = importData.sip
        ? (importData.sip['CID:'+row.client_id] || (panIsUnambiguous && importData.sip[row.pan]) || importData.sip[upName] || {})
        : {};

      let ex = (row.client_id && byClientId[String(row.client_id).trim()])
            || (panIsUnambiguous && existingMap[row.pan])
            || (row.mobile && byMobile[mob10(row.mobile)])
            || null;
      if(!ex){
        if(nameCount[upName] === 1 && fileNameCount[upName] === 1){
          ex = nameMap[upName];                       // unique on both sides — safe
        } else if(nameCount[upName] > 1 || fileNameCount[upName] > 1){
          ambigRows.push(row); return;                // several people share this name
        }
        // else: nobody by this name -> genuinely new client, fall through to add
      }
      if(ex && claimed.has(ex.id)){ ambigRows.push(row); return; }
      if(ex) claimed.add(ex.id);
      
      if(ex){
        // Update existing
        const oldAumBeforeUpdate = parseFloat(ex.aum)||0; // capture BEFORE it gets overwritten below — needed to correctly detect genuine first-ever investors
        if(row.aum || row.aum===0) ex.aum = row.aum;   // 0 is a real value, not "skip"
        ex.pan = row.pan || ex.pan;                     // parser only ever returns a valid PAN or ''
        if(row.mobile && !ex.mobile) ex.mobile = mob10(row.mobile); // mobile mile aur khaali ho to bhar do
        if(row.client_id) ex.client_id = row.client_id; // lock in the stable key
        {
          const hadAumDetail = !!ex.aum_detail;
          const newInv = parseFloat(row.inv_amt)||0;
          const _td = today();
          // Same-day re-uploads must all diff against YESTERDAY's figure, not
          // against each other. Without this, uploading a corrected/completed
          // file 2-3 times in one day (common when the source file comes in
          // partial) would compare the 2nd upload against the 1st upload's
          // possibly-incomplete numbers and log a fake addition/redemption.
          // `invested_baseline_*` is the Invested Amount as it stood at the
          // END of the last DIFFERENT calendar day — it's set once (the first
          // import of a new day) and then left untouched through every
          // further same-day re-upload, so every re-upload today keeps
          // comparing against that same fixed starting point. The log entry
          // itself (id = clientId+today) naturally gets refined/overwritten
          // by each re-upload rather than duplicated.
          if(hadAumDetail && ex.invested_baseline_date!==_td){
            ex.invested_baseline_inv = parseFloat(ex.aum_detail.inv)||0;
            ex.invested_baseline_date = _td;
          }
          const oldInv = (ex.invested_baseline_date===_td) ? (parseFloat(ex.invested_baseline_inv)||0) : null;
          // Only track a change when we have a GENUINE previous Invested Amount
          // to compare against — no guessing/estimating. A guessed number
          // isn't a real purchase or redemption amount, so if there's no
          // baseline yet, this import silently establishes one (aum_detail
          // set just below) and tracking starts cleanly from tomorrow.
          // Threshold of ₹1: the RTA's file sometimes carries paisa-level
          // rounding noise between two exports of the SAME investment (e.g.
          // ₹5,50,000.00 one day, ₹5,50,000.30 the next) with no real
          // transaction behind it. Below ₹1 isn't a purchase/redemption —
          // it's just export noise — so it's not logged as a change (avoids
          // meaningless "▲ +₹0" / "▼ -₹0" rows in the Additions/Redemptions list).
          const isFirstEver = !hadAumDetail && oldAumBeforeUpdate===0; // no prior AUM detail AND zero AUM before — genuinely never invested until now
          if(isFirstEver && newInv>0){
            // First-ever investment for this (pre-existing) client record — counts as
            // a new-investor "win" too, same as a brand-new client added below.
            ex.prev_invested = 0;
            ex.invested_change_amt = newInv;
            ex.invested_change_date = _td;
            ex.invested_baseline_inv = 0; ex.invested_baseline_date = _td;
            mfChangeLogBatch.push({id:ex.id+'__'+_td, date:_td, clientId:ex.id, name:ex.name||'', rm:ex.rm||'', prevInvested:0, newInvested:newInv, delta:newInv});
          } else if(oldInv!==null && Math.abs(newInv - oldInv) >= 1){
            ex.prev_invested = oldInv;
            ex.invested_change_amt = newInv - oldInv;
            ex.invested_change_date = _td;
            mfChangeLogBatch.push({id:ex.id+'__'+_td, date:_td, clientId:ex.id, name:ex.name||'', rm:ex.rm||'', prevInvested:oldInv, newInvested:newInv, delta:newInv-oldInv});
          }
        }
        ex.aum_detail = _aumDetail(row);
        if(sipInfo.sip_amount) ex.sip_amount = sipInfo.sip_amount;
        if(sipInfo.sip_count) ex.sip_count = sipInfo.sip_count;
        if(sipInfo.sip_details) ex.sip_details = sipInfo.sip_details;
        if(importData.sip && (sipInfo.sip_details||sipInfo.sip_amount||sipInfo.sip_count)) sipApplied.add(ex.id);
        ex.status = 'Investor';
        newClients.push(ex);
        updated++;
      } else {
        // Add new
        let rm = row.rm || '';
        if(CU.role!=='admin' && CU.role!=='backoffice' && !CU.backoffice_access) rm = CU.name;
        const nid = uid();
        const newInv = parseFloat(row.inv_amt)||0;
        newClients.push({
          id: nid,
          name: row.name, pan: row.pan, client_id: row.client_id||'', mobile:(row.mobile?mob10(row.mobile):''), email:'', rm,
          status: 'Investor',
          aum: row.aum,
          aum_detail: _aumDetail(row),
          // New investor's first invested amount counts as a "win" too — mark it
          // so today's Win/Loss card (mfWins) picks it up as "+MF New" on add day.
          prev_invested: 0,
          invested_change_amt: newInv,
          invested_change_date: newInv>0 ? today() : '',
          sip_amount: sipInfo.sip_amount||null,
          sip_count: sipInfo.sip_count||null,
          sip_details: sipInfo.sip_details||null,
          last_invest_date:'', last_call_date:'', next_call:'',
          followup_status:'', remarks:'', created: today(), updated: today()
        });
        if(newInv>0){
          const _td=today();
          mfChangeLogBatch.push({id:nid+'__'+_td, date:_td, clientId:nid, name:row.name||'', rm:rm||'', prevInvested:0, newInvested:newInv, delta:newInv});
        }
        if(importData.sip && (sipInfo.sip_details||sipInfo.sip_amount||sipInfo.sip_count)) sipApplied.add(nid);
        added++;
      }
    });
  } else if(importData.sip){
    // SIP only — same match order as AUM: Client ID → PAN → Name.
    // (No Mobile here — the SIP report file doesn't carry a mobile column.)
    const sipFileNameCount = {};
    Object.values(importData.sip).forEach(row => {
      const n = nmKey(row.name);
      if(n) sipFileNameCount[n] = (sipFileNameCount[n]||0)+1;
    });
    Object.values(importData.sip).forEach(row => {
      const upName = nmKey(row.name);
      const rowPan = validPan(row.pan) ? String(row.pan).trim().toUpperCase() : '';
      // Same guardian+minor PAN-sharing guard as the AUM import above — don't
      // fall back to a shared PAN without a Client ID to disambiguate.
      const panIsUnambiguous = rowPan && (panOwnerCount[rowPan]||0) <= 1;
      let ex = (row.client_id && byClientId[String(row.client_id).trim()])
            || (panIsUnambiguous && existingMap[rowPan])
            || null;
      if(!ex){
        // Naam se match SIRF jab dono taraf unique ho — warna same-naam alag
        // log ka SIP ghus jata / duplicate ban jata.
        if(nameCount[upName] === 1 && sipFileNameCount[upName] === 1){
          ex = nameMap[upName];
        } else if(nameCount[upName] > 1 || sipFileNameCount[upName] > 1){
          return;                                     // several people share this name — skip
        }
      }
      if(ex){
        ex.sip_amount = row.sip_amount;
        ex.sip_count = row.sip_count;
        ex.sip_details = row.sip_details||null;
        sipApplied.add(ex.id);
        newClients.push(ex);
        updated++;
      } else {
        const nid = uid();
        newClients.push({
          id: nid, name: row.name, pan: row.pan, client_id: row.client_id||'', mobile:'', email:'', rm: (CU.role!=='admin'&&CU.role!=='backoffice'&&!CU.backoffice_access)?CU.name:(row.rm||''),
          status:'Prospect', aum:null, sip_amount:row.sip_amount, sip_count:row.sip_count, sip_details:row.sip_details||null,
          last_invest_date:'', last_call_date:'', next_call:'',
          followup_status:'', remarks:'', created:today(), updated:today()
        });
        sipApplied.add(nid);
        added++;
      }
    });
    // (existing clients not present in SIP file are left untouched - no need to re-push)
  }

  // ── SIP RECONCILIATION (Trxn-based) ──────────────────────────────
  // Running SIP Report hamesha POORA SIP book hota hai. Isliye jo SIP file me
  // nahi aaya = closed maana jayega. Do kaam:
  //   1. Jis existing client ko upar SIP data nahi mila (sipApplied me nahi),
  //      use strong key (Client ID / valid PAN) se dhoondo. Mila to apply karo;
  //      warna, agar uske purane SIP the, unhe CLOSE (clear) kar do. Naam se
  //      match NAHI karte — same-naam alag log ka SIP galti se apply/clear ho sakta.
  //   2. Har client ka old vs new sip_details diff → added / updated / closed.
  //      Detail key = SIP Trxn No. (jab available ho), warna Folio+Scheme.
  //      NOTE: is update ke baad PEHLI import me purane stored SIPs me Trxn No.
  //      nahi hoga → us ek baar closed/added count thoda inflated dikh sakta hai;
  //      agli import se exact ho jayega (tab tak har SIP pe Trxn No. store ho chuka hoga).
  if(importData.sip){
    const sm = importData.sip;
    const touchedIds = new Set(newClients.map(c=>c.id));

    // (1) Apply-or-close for clients the loops above didn't already handle
    existing.forEach(c=>{
      if(sipApplied.has(c.id)) return;
      const s = (c.client_id && sm['CID:'+String(c.client_id).trim()])
             || (validPan(c.pan) && sm[String(c.pan).trim().toUpperCase()])
             || null;
      const hadSips = (oldSipById[c.id]||[]).length>0 || Number(c.sip_count)>0;
      if(s){
        c.sip_details = s.sip_details||[]; c.sip_count = s.sip_count||0; c.sip_amount = s.sip_amount||0;
        sipApplied.add(c.id);
        if(!touchedIds.has(c.id)){ newClients.push(c); touchedIds.add(c.id); }
      } else if(hadSips){
        c.sip_details = []; c.sip_count = 0; c.sip_amount = 0;
        sipClientsCleared++;
        if(!touchedIds.has(c.id)){ newClients.push(c); touchedIds.add(c.id); }
      }
    });

    // (2) Per-SIP diff for the summary
    const dkeys = arr => {
      const out = new Map();
      (arr||[]).forEach(x=>{
        const base = (x.trxn && String(x.trxn).trim())
          ? 'T:'+String(x.trxn).trim()
          : 'F:'+String(x.folio||'').trim()+'|'+String(x.scheme||'').trim().toUpperCase();
        let k = base, i = 1;
        while(out.has(k)){ k = base+'#'+(i++); }   // same folio+scheme repeated
        out.set(k, x);
      });
      return out;
    };
    const finalById = {};
    existing.forEach(c=> finalById[c.id] = c);
    newClients.forEach(c=> finalById[c.id] = c);
    Object.keys(finalById).forEach(id=>{
      const oldD = oldSipById[id] || [];
      const newD = Array.isArray(finalById[id].sip_details) ? finalById[id].sip_details : [];
      if(!oldD.length && !newD.length) return;
      const ok = dkeys(oldD), nk = dkeys(newD);
      nk.forEach((nx,k)=>{
        if(!ok.has(k)){ sipAdded++; }
        else {
          const ox = ok.get(k);
          if((Number(ox.amount)||0)!==(Number(nx.amount)||0) || String(ox.day||'')!==String(nx.day||'')) sipUpdated++;
        }
      });
      ok.forEach((ox,k)=>{ if(!nk.has(k)) sipClosed++; });
    });
  }
  
  // newClients contains only touched/new records; setClientsBulk merges
  // them into the existing local array and writes only these docs to Firestore.
  await DB.setClientsBulk('mf_clients', newClients);
  // One single transaction for every Invested Amount change detected this
  // import (see addMfChangeLogBatch) — not N separate ones racing each other.
  await DB.addMfChangeLogBatch(mfChangeLogBatch);
  closeModal('importModal');
  let _imsg = `✅ Import done! ${updated} updated + ${added} new clients`;
  if(importData.sip){
    const _b = [];
    if(sipAdded)   _b.push(`${sipAdded} new`);
    if(sipUpdated) _b.push(`${sipUpdated} update`);
    if(sipClosed)  _b.push(`${sipClosed} closed`);
    _imsg += ` · SIP: ` + (_b.length ? _b.join(' · ') : 'no change')
           + (sipClientsCleared ? ` (${sipClientsCleared} client fully closed)` : '');
  }
  if(ambigRows.length) _imsg += ` — ${ambigRows.length} rows skipped (same name, 1+ match)`;
  toast(_imsg, 'success');
  renderMfTable(); refreshDash(); updateBadges();
  // Anything we refused to guess at, hand back to the user with the Client ID
  // and PAN so it can be fixed by hand.
  if(ambigRows.length){
    setTimeout(()=>{
      showReport(`Same Name — Skipped (${ambigRows.length} rows)`,
        ['#','Naam','Client ID','PAN','AUM','Wajah'],
        ambigRows.map((r,i)=>[i+1, r.name, r.client_id||'—', r.pan||'—',
          (r.aum||0).toLocaleString('en-IN'),
          'There is more than 1 investor with this name — please update manually']));
    }, 600);
  }
}

// ══════════════════════════════════════════
// MF DUPLICATE MERGE — review-based (admin/backoffice)
// Group key = valid PAN ELSE 10-digit mobile ELSE normalized naam. Isse alag-PAN
// wale same-naam log ALAG group me rehte hain (galat merge se bachao). Admin har
// group tick karke merge karta hai; AUM ka sabse bada value rakha jata hai (double
// nahi), baaki records delete hote hain aur unke khaali fields survivor me bharte hain.
// ══════════════════════════════════════════
function _mfDupKey(c){
  const p = String(c.pan||'').trim().toUpperCase();
  if(/^[A-Z]{5}[0-9]{4}[A-Z]$/.test(p)) return 'P:'+p;
  const m = String(c.mobile||'').replace(/\D/g,'').slice(-10);
  if(m.length===10) return 'M:'+m;
  const n = String(c.name||'').toUpperCase().replace(/\s+/g,' ').trim();
  return n ? 'N:'+n : '';
}
function findMfDupGroups(){
  // Minors are excluded from duplicate-scanning entirely — a minor legally
  // carries the guardian's PAN (and often the guardian's mobile too, since
  // a child doesn't have their own phone), so grouping them by PAN/mobile
  // makes a real guardian+minor pair look like the same person duplicated.
  // "Merge Selected" deletes every non-primary record in a group, so without
  // this exclusion a minor's own investor record could get permanently
  // deleted as a false "duplicate" of their parent.
  const arr = (DB.get('mf_clients')||[]).filter(c=>!c.is_minor);
  const map = {};
  arr.forEach(c=>{ const k=_mfDupKey(c); if(!k) return; (map[k]=map[k]||[]).push(c); });
  return Object.keys(map).filter(k=>map[k].length>1).map(k=>({key:k, recs:map[k]}));
}
function _mfPrimary(recs){
  const score=c=>(
    (String(c.pan||'').match(/^[A-Z]{5}[0-9]{4}[A-Z]$/)?8:0)
    + (c.client_id?4:0)
    + (String(c.mobile||'').replace(/\D/g,'').length>=10?2:0)
    + (Number(c.aum)>0?1:0)
  );
  return recs.slice().sort((a,b)=> score(b)-score(a) || String(a.created||'').localeCompare(String(b.created||'')) )[0];
}
function openMfDupMerge(){
  if(CU.role!=='admin' && CU.role!=='backoffice' && !CU.backoffice_access){ toast('This tool is for admin only','error'); return; }
  const esc = v => String(v==null?'':v).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const groups = findMfDupGroups();
  const ov=document.createElement('div');
  ov.id='mfDupOverlay';
  ov.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:9999;display:flex;align-items:flex-start;justify-content:center;overflow:auto;padding:20px';
  let body;
  if(!groups.length){
    body='<div style="padding:34px;text-align:center"><div style="font-size:2rem">✅</div><div style="font-size:1.05rem;font-weight:700;margin-top:8px">No Duplicates Found</div><div style="color:#64748b;font-size:.85rem;margin-top:6px">There are no MF investors sharing the same PAN / mobile / name.</div><div style="margin-top:16px"><button class="btn btn-outline" onclick="document.getElementById(\'mfDupOverlay\').remove()">Band Karein</button></div></div>';
  } else {
    let rows='';
    groups.forEach((g,gi)=>{
      const prim=_mfPrimary(g.recs);
      const kind = g.key[0]==='P'?'PAN':(g.key[0]==='M'?'Mobile':'Naam');
      let cards='';
      g.recs.forEach(c=>{
        const isP=c.id===prim.id;
        const m=String(c.mobile||'').replace(/\D/g,'').slice(-10);
        cards+='<tr style="background:'+(isP?'#ecfdf5':'#fff')+'">'
          +'<td style="padding:5px 8px;border-top:1px solid #eee;white-space:nowrap">'+(isP?'<span style="color:#059669;font-weight:700">✔ Rakhenge</span>':'<span style="color:#b91c1c">Hatega</span>')+'</td>'
          +'<td style="padding:5px 8px;border-top:1px solid #eee">'+esc(c.name||'—')+'</td>'
          +'<td style="padding:5px 8px;border-top:1px solid #eee">'+esc(c.pan||'—')+'</td>'
          +'<td style="padding:5px 8px;border-top:1px solid #eee">'+(m||'—')+'</td>'
          +'<td style="padding:5px 8px;border-top:1px solid #eee;text-align:right">'+(Number(c.aum)>0?'₹'+fmtNum(Number(c.aum)):'—')+'</td>'
          +'<td style="padding:5px 8px;border-top:1px solid #eee">'+esc(c.rm||'—')+'</td>'
          +'</tr>';
      });
      rows+='<div style="border:1px solid #e5e7eb;border-radius:10px;margin-bottom:12px;overflow:hidden">'
        +'<label style="display:flex;align-items:center;gap:8px;padding:8px 12px;background:#f8fafc;cursor:pointer;font-weight:600;font-size:.85rem">'
        +'<input type="checkbox" class="mfdup-chk" data-key="'+esc(g.key)+'" checked> Group '+(gi+1)+' — '+g.recs.length+' records (same '+kind+') — merge</label>'
        +'<table style="width:100%;border-collapse:collapse;font-size:.8rem">'
        +'<tr style="background:#f1f5f9;font-size:.72rem;color:#475569"><td style="padding:5px 8px">STATUS</td><td style="padding:5px 8px">NAAM</td><td style="padding:5px 8px">PAN</td><td style="padding:5px 8px">MOBILE</td><td style="padding:5px 8px;text-align:right">AUM</td><td style="padding:5px 8px">RM</td></tr>'
        +cards+'</table></div>';
    });
    body='<div style="padding:16px 18px;border-bottom:1px solid #eee;display:flex;justify-content:space-between;align-items:center">'
      +'<div style="font-weight:800;font-size:1.05rem">🔀 Duplicate MF Investors — Review & Merge</div>'
      +'<button onclick="document.getElementById(\'mfDupOverlay\').remove()" style="border:none;background:#f1f5f9;border-radius:8px;width:30px;height:30px;cursor:pointer;font-size:1rem">✕</button></div>'
      +'<div style="padding:10px 18px;font-size:.8rem;color:#7c5e10;background:#fffbeb;border-bottom:1px solid #fde68a">✅ The record kept is the one with the most information. For AUM groups, the <b>largest</b> value is kept (it won\'t be doubled). The other records will be deleted, and their blank fields (PAN/mobile/RM/remarks) will be filled in on the survivor. Uncheck any group you don\'t want merged.</div>'
      +'<div style="padding:16px 18px">'+rows
      +'<div style="display:flex;gap:10px;justify-content:flex-end;margin-top:8px">'
      +'<button class="btn btn-outline" onclick="document.getElementById(\'mfDupOverlay\').remove()">Cancel</button>'
      +'<button class="btn btn-teal" onclick="mergeMfDupsSelected()">✔ Merge Selected</button></div></div>';
  }
  ov.innerHTML='<div style="background:#fff;border-radius:14px;width:min(760px,96vw);max-height:90vh;overflow:auto;box-shadow:0 20px 50px rgba(0,0,0,.3)">'+body+'</div>';
  ov.addEventListener('click',e=>{ if(e.target===ov) ov.remove(); });
  document.body.appendChild(ov);
}
async function mergeMfDupsSelected(){
  const keys=[...document.querySelectorAll('.mfdup-chk')].filter(x=>x.checked).map(x=>x.getAttribute('data-key'));
  if(!keys.length){ toast('No group selected','error'); return; }
  const groups=findMfDupGroups().filter(g=>keys.includes(g.key));
  if(!groups.length){ toast('Nothing found','error'); return; }
  const val=v=>String(v||'').trim();
  const survivors=[], delIds=[]; let totalDel=0;
  groups.forEach(g=>{
    const prim=_mfPrimary(g.recs);
    const survivor=Object.assign({}, prim);
    let maxAum=Number(prim.aum)||0, maxSipAmt=Number(prim.sip_amount)||0, maxSipCnt=Number(prim.sip_count)||0;
    g.recs.forEach(c=>{
      if(c.id===prim.id) return;
      ['pan','client_id','mobile','email','rm','remarks','followup_status','next_call','last_call_date','last_invest_date','sip_details','aum_detail'].forEach(f=>{
        if(!val(survivor[f]) && val(c[f])) survivor[f]=c[f];
      });
      maxAum=Math.max(maxAum, Number(c.aum)||0);
      maxSipAmt=Math.max(maxSipAmt, Number(c.sip_amount)||0);
      maxSipCnt=Math.max(maxSipCnt, Number(c.sip_count)||0);
      delIds.push(c.id); totalDel++;
    });
    survivor.aum=maxAum;
    if(maxSipAmt) survivor.sip_amount=maxSipAmt;
    if(maxSipCnt) survivor.sip_count=maxSipCnt;
    survivor.updated=today();
    survivors.push(survivor);
  });
  if(!confirm('Confirm: '+groups.length+' group(s) will be merged, '+totalDel+' duplicate record(s) will be deleted (their info will be added to the survivor). Proceed?')) return;
  try{
    await DB.setClientsBulk('mf_clients', survivors);
    await DB.deleteClientsBulk('mf_clients', delIds);
    const ovx=document.getElementById('mfDupOverlay'); if(ovx) ovx.remove();
    toast('✅ '+groups.length+' group(s) merged, '+totalDel+' duplicate(s) removed','success');
    renderMfTable(); refreshDash(); updateBadges();
  }catch(e){ toast('Merge failed: '+(e&&e.message||e),'error'); }
}

// ══════════════════════════════════════════
// EQUITY BAD-IMPORT REVIEW — review-based (admin only)
// Finds equity clients that look like they came from a wrongly-uploaded
// MF AUM file (Client ID/Client Name/AUM columns get auto-mapped to
// Code/Name/Asset Value, and since the MF file has no Last Trade Date,
// every unmatched row becomes a brand-new "Active" equity client).
// Signal used: added TODAY, with NO last_trade_date at all — a real
// broker equity file always carries a Last Trade Date, so this is a very
// safe marker. Nothing is deleted without the admin reviewing & confirming.
// ══════════════════════════════════════════
function findEqBadImportCandidates(){
  const td = today();
  const arr = DB.get('eq_clients')||[];
  return arr.filter(c => c.created===td && !c.last_trade_date);
}
function openEqBadImportReview(){
  if(CU.role!=='admin'){ toast('This tool is for admin only','error'); return; }
  const esc = v => String(v==null?'':v).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const cands = findEqBadImportCandidates();
  const ov=document.createElement('div');
  ov.id='eqBadImportOverlay';
  ov.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:9999;display:flex;align-items:flex-start;justify-content:center;overflow:auto;padding:20px';
  let body;
  if(!cands.length){
    body='<div style="padding:34px;text-align:center"><div style="font-size:2rem">✅</div><div style="font-size:1.05rem;font-weight:700;margin-top:8px">Nothing Found</div><div style="color:#64748b;font-size:.85rem;margin-top:6px">No equity client added today has a blank Last Trade Date.</div><div style="margin-top:16px"><button class="btn btn-outline" onclick="document.getElementById(\'eqBadImportOverlay\').remove()">Close</button></div></div>';
  } else {
    const rows = cands.map(c=>
      '<tr>'
      +'<td style="padding:5px 8px;border-top:1px solid #eee"><input type="checkbox" class="eqbad-chk" data-id="'+esc(c.id)+'" checked></td>'
      +'<td style="padding:5px 8px;border-top:1px solid #eee">'+esc(c.name||'—')+'</td>'
      +'<td style="padding:5px 8px;border-top:1px solid #eee">'+esc(c.code||'—')+'</td>'
      +'<td style="padding:5px 8px;border-top:1px solid #eee">'+esc(c.mobile||'—')+'</td>'
      +'<td style="padding:5px 8px;border-top:1px solid #eee;text-align:right">'+(c.asset_value?'₹'+fmtNum(c.asset_value):'—')+'</td>'
      +'<td style="padding:5px 8px;border-top:1px solid #eee">'+esc(c.rm||'—')+'</td>'
      +'</tr>'
    ).join('');
    body='<div style="padding:16px 18px;border-bottom:1px solid #eee;display:flex;justify-content:space-between;align-items:center">'
      +'<div style="font-weight:800;font-size:1.05rem">🧹 Today\'s Suspicious Equity Additions — Review</div>'
      +'<button onclick="document.getElementById(\'eqBadImportOverlay\').remove()" style="border:none;background:#f1f5f9;border-radius:8px;width:30px;height:30px;cursor:pointer;font-size:1rem">✕</button></div>'
      +'<div style="padding:10px 18px;font-size:.8rem;color:#7c5e10;background:#fffbeb;border-bottom:1px solid #fde68a">⚠️ These '+cands.length+' equity clients were added today but have a completely blank Last Trade Date — a real broker file always carries a Last Trade Date, so these look like they came from a wrongly-uploaded file (e.g. an MF AUM file). Uncheck any you don\'t want to delete, then confirm below.</div>'
      +'<div style="padding:16px 18px">'
      +'<table style="width:100%;border-collapse:collapse;font-size:.8rem">'
      +'<tr style="background:#f1f5f9;font-size:.72rem;color:#475569"><td style="padding:5px 8px"><input type="checkbox" id="eqbad-all" checked onchange="document.querySelectorAll(\'.eqbad-chk\').forEach(x=>x.checked=this.checked)"></td><td style="padding:5px 8px">NAAM</td><td style="padding:5px 8px">CODE</td><td style="padding:5px 8px">MOBILE</td><td style="padding:5px 8px;text-align:right">ASSET VALUE</td><td style="padding:5px 8px">RM</td></tr>'
      +rows+'</table>'
      +'<div style="display:flex;gap:10px;justify-content:flex-end;margin-top:14px">'
      +'<button class="btn btn-outline" onclick="document.getElementById(\'eqBadImportOverlay\').remove()">Cancel</button>'
      +'<button class="btn btn-teal" style="background:#dc2626;border-color:#dc2626" onclick="deleteEqBadImportSelected()">🗑️ Delete Selected</button></div></div>';
  }
  ov.innerHTML='<div style="background:#fff;border-radius:14px;width:min(760px,96vw);max-height:90vh;overflow:auto;box-shadow:0 20px 50px rgba(0,0,0,.3)">'+body+'</div>';
  ov.addEventListener('click',e=>{ if(e.target===ov) ov.remove(); });
  document.body.appendChild(ov);
}
async function deleteEqBadImportSelected(){
  const ids=[...document.querySelectorAll('.eqbad-chk')].filter(x=>x.checked).map(x=>x.getAttribute('data-id'));
  if(!ids.length){ toast('Nothing selected','error'); return; }
  if(!confirm('Confirm: '+ids.length+' equity client(s) will be permanently deleted. This cannot be undone. Proceed?')) return;
  try{
    await DB.deleteClientsBulk('eq_clients', ids);
    const ovx=document.getElementById('eqBadImportOverlay'); if(ovx) ovx.remove();
    toast('✅ '+ids.length+' client(s) deleted','success');
    renderEqTable(); refreshDash(); updateBadges();
  }catch(e){ toast('Delete failed: '+(e&&e.message||e),'error'); }
}

// ══════════════════════════════════════════
// EQUITY IMPORT (Asset Value + Last Trade Date)
// ══════════════════════════════════════════
var eqImportData = null;

function openEqImportModal(){
  eqImportData = null;
  document.getElementById('eq-preview').innerHTML='';
  document.getElementById('eqImportBtnGo').disabled=true;
  document.getElementById('eq-file').value='';
  if(CU.role!=='admin' && CU.role!=='backoffice' && !CU.backoffice_access){
    document.getElementById('eqImportDesc1').innerHTML =
      `Upload an Excel file with <b>Asset Value</b> and <b>Last Trade Date</b>. Matching will be done on <b>Client Code</b> (not mobile — family accounts can share the same mobile) — if a match is found, the existing client will be updated; if not, a new client will be added under <b>your (${CU.name}) RM name</b>.`;
    document.getElementById('eqImportDesc2').innerHTML =
      `Expected columns (header names flexible/auto-detect): <b>Code</b> (or Client Code), <b>Name</b>, <b>Mobile</b>, <b>Asset Value</b> (or Holding/Portfolio Value), <b>Last Trade Date</b> (or Last Trade)`;
  } else {
    document.getElementById('eqImportDesc1').innerHTML =
      `Upload an Excel file with <b>Asset Value</b> and <b>Last Trade Date</b>. Matching will be done on <b>Client Code</b> (not mobile — family accounts can share the same mobile) — if a match is found, the existing client will be updated; if not, a <b>new client will be added</b>.`;
    document.getElementById('eqImportDesc2').innerHTML =
      `Expected columns (header names flexible/auto-detect): <b>Code</b> (or Client Code), <b>Name</b>, <b>Mobile</b>, <b>RM</b> (required for new clients), <b>Asset Value</b> (or Holding/Portfolio Value), <b>Last Trade Date</b> (or Last Trade)<br>
       <span style="color:var(--teal)">This box also auto-detects the daily <b>Broker RMS Risk file</b> and the <b>Square-off (T+5)</b> file — just drop either one here, no need to go anywhere else.</span>`;
  }
  document.getElementById('eqImportModal').classList.add('open');
}

// Normalize a header string for flexible matching
function normHdr(s){ return String(s||'').toLowerCase().replace(/[^a-z0-9]/g,''); }

// Broker file ka Account Status ("Active" / "InActive" / "Closed") -> CRM status
function normAcStatus(v){
  const s = String(v||'').toLowerCase().replace(/[^a-z]/g,'');
  if(s==='closed'||s==='close'||s==='closedaccount') return 'Closed';
  if(s==='inactive'||s==='dormant') return 'Inactive';
  if(s==='active') return 'Active';
  return '';
}

// ── CRM ka asli status: LAST TRADE DATE se ─────────────────────────────────
// Broker ka "Account Status" sirf ye batata hai ki account khula hai ya band —
// trading activity nahi. Isliye file me 2017/2023 ke last trade wale bhi
// "Active" aate hain, aur import unhe CRM me Inactive se Active kar deta tha.
// Ab rule: Closed => Closed (broker se). Baaki sab me last trade 1 saal se
// purana => Inactive, warna Active. Last trade blank ho to status chhedte nahi.
const EQ_INACTIVE_DAYS = 365;
function deriveEqStatus(brokerStatus, lastTrade){
  if(brokerStatus==='Closed') return 'Closed';
  // daysDiff() hi use karo — wahi parser table ke "6299D AGO" me chalta hai.
  // Pehle yahan sirf YYYY-MM-DD parse hota tha, isliye purane records
  // (2009-04-17T00:00:00Z / 17-Apr-2009 jaise) chupchaap skip ho jate the.
  const days = daysDiff(lastTrade);
  if(days===null) return '';                   // pata nahi -> mat badlo
  return days > EQ_INACTIVE_DAYS ? 'Inactive' : 'Active';
}

// Try to parse an Excel date value (could be Date object, serial number, or string)
// All paths return local date (YYYY-MM-DD) without UTC timezone shift
function parseExcelDate(val){
  if(!val) return '';
  function localYMD(d){ if(isNaN(d)) return ''; return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0'); }
  if(val instanceof Date) return localYMD(val);
  if(typeof val === 'number'){
    // Excel serial — shift to noon UTC to avoid day-boundary rollback in IST
    const d = new Date(Math.round((val - 25569)*86400*1000) + 12*3600*1000);
    if(!isNaN(d)) return localYMD(d);
  }
  const s = String(val).trim();
  // yyyy-mm-dd already local
  if(/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  // dd-mmm-yyyy e.g. 25-Jun-2026
  const m1=s.match(/^(\d{1,2})[-\/]([A-Za-z]+)[-\/](\d{4})$/);
  if(m1){ const mn={jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12}; const mo=mn[m1[2].toLowerCase().slice(0,3)]; if(mo) return m1[3]+'-'+String(mo).padStart(2,'0')+'-'+String(m1[1]).padStart(2,'0'); }
  // dd/mm/yyyy
  const m2=s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if(m2) return m2[3]+'-'+String(m2[2]).padStart(2,'0')+'-'+String(m2[1]).padStart(2,'0');
  // fallback
  const d = new Date(s);
  if(!isNaN(d) && s.match(/\d{4}/)) return localYMD(d);
  return '';
}

// ══════════════════════════════════════════
// BROKER RMS RISK FILE (daily .xls) — import & display
// Stores a map { [cltCode]: {ac_bal, stk_val, tot_expo, risk_val, risk_pct} }
// under DB key 'eq_risk'. Signs are FLIPPED on import so holdings/credits
// read as positive and debits/shortfalls read as negative (broker RMS shows
// the reverse). Latest file replaces the previous day's data.
// ══════════════════════════════════════════
let _eqRiskCache=null;
function getEqRisk(){
  if(_eqRiskCache) return _eqRiskCache;
  _eqRiskCache = DB.get('eq_risk') || {code:{}, updated:'', count:0};
  return _eqRiskCache;
}
function clearEqRiskCache(){ _eqRiskCache=null; }
function eqRiskFor(code){
  if(!code) return null;
  const r = getEqRisk();
  return (r.code && r.code[String(code).trim()]) || null;
}
// Detect + import the RMS file. Returns true if the file was an RMS risk file.
function tryImportRiskFile(rows){
  // locate header row containing the RMS signature columns
  let hdr=-1, m={};
  const want = {
    code:['cltcode','clientcode','cltcd'],
    name:['cltname','clientname'],
    ac_bal:['acbal','accountbalance','ledgerbalance'],
    stk_val:['stkval'],
    tot_expo:['totexpo','totalexposure'],
    risk_val:['riskval'],
    risk_pct:['risk','riskpct','risk%']
  };
  for(let i=0;i<Math.min(rows.length,15);i++){
    const map={};
    (rows[i]||[]).forEach((cell,ci)=>{
      const h=normHdr(cell);
      for(const [f,vars] of Object.entries(want)){
        if(vars.includes(h) && map[f]===undefined) map[f]=ci;
      }
    });
    // Signature: must have Clt Code + Risk Val + Stk Val to qualify as RMS file
    if(map.code!==undefined && map.risk_val!==undefined && map.stk_val!==undefined){
      hdr=i; m=map; break;
    }
  }
  if(hdr===-1) return false; // not an RMS file → let normal import handle it

  // Permission gate: admin, Back Office, or an RM explicitly granted risk_upload by admin.
  if(!(CU && (CU.role==='admin' || CU.role==='backoffice' || CU.risk_upload===true))){
    const pv0 = document.getElementById('eq-preview');
    if(pv0) pv0.innerHTML =
      `<div style="background:#fff3f3;color:var(--red,#dc2626);padding:12px;border-radius:8px;font-size:.85rem;font-weight:600;border:1px solid #f5c2c2">
         🔒 This is a Broker RMS Risk file. You don't have permission to upload it.<br>
         <span style="font-weight:500;color:var(--gray)">Please ask the admin to enable "Allow Broker RMS Risk file upload" for your account.</span>
       </div>`;
    const goBtn0=document.getElementById('eqImportBtnGo');
    if(goBtn0) goBtn0.disabled = true;
    return true; // handled (blocked) — don't fall through to normal import
  }

  const num = v => { const n=parseFloat(String(v==null?'':v).replace(/[₹,%\s]/g,'')); return isNaN(n)?0:n; };
  const codeMap = {};
  let cnt=0;
  rows.slice(hdr+1).forEach(r=>{
    const code = String(r[m.code]==null?'':r[m.code]).trim();
    if(!code || !/\d/.test(code)) return; // skip blanks/subtotals
    codeMap[code] = {
      // SIGN FLIP on balance-type fields (holdings/credit → +, debit → −)
      ac_bal:   -num(r[m.ac_bal]),
      stk_val:  -num(r[m.stk_val]),
      risk_val: -num(r[m.risk_val]),
      // exposure & risk% kept as-is (already positive in the file)
      tot_expo: m.tot_expo!==undefined ? num(r[m.tot_expo]) : 0,
      risk_pct: m.risk_pct!==undefined ? num(r[m.risk_pct]) : 0
    };
    cnt++;
  });

  eqImportData = null; // this is not a client-list import
  const payload = {code:codeMap, updated:new Date().toISOString(), count:cnt};
  const pv = document.getElementById('eq-preview');
  if(pv) pv.innerHTML =
    `<div style="background:#eef6ff;color:var(--navy,#0a1f4d);padding:12px;border-radius:8px;font-size:.85rem;font-weight:600;border:1px solid #cfe0ff">
       ⚠️ Broker RMS Risk file detected — <b>${cnt}</b> clients' risk data ready.<br>
       <span style="font-weight:500;color:var(--gray)">Values sign-adjusted (holdings shown +, debits −). This replaces the previous day's risk data.</span>
       <div style="margin-top:10px"><button class="btn btn-teal" onclick='doRiskImport(${JSON.stringify(payload).replace(/'/g,"&#39;")})'>Import Risk Data</button></div>
     </div>`;
  const goBtn=document.getElementById('eqImportBtnGo');
  if(goBtn) goBtn.disabled = true; // normal import button stays off for RMS files
  return true;
}
// eq_risk ka data 3000+ clients ka hai. Agar poora nested map (code:{...}) native
// Firestore fields me store karein to har numeric field index hota hai → 40,000
// index-entries/doc limit cross → "too many index entries for entity .../eq_risk"
// sync error. Fix: bade code-map ko EK JSON STRING me store karo (poora doc = 1
// index entry). localStorage me normal object shape rakhte hain, isliye getEqRisk()
// / eqRiskFor() bilkul waise hi chalte hain. Read-time rehydration syncFromFirebase
// me handle hota hai (codeJson → code).
// MERGE MODE: naye file ke clients ko office-wide risk map me MERGE karta hai —
// jo codes file me hain sirf wahi update/add hote hain, baaki SABHI RMs ke purane
// clients ka data waisa hi rehta hai. Isliye Raju apni (sirf apne clients wali)
// broker RMS file upload kare to bhi kisi aur RM ka data wipe nahi hota; har RM
// ke upload se poore office ka risk data cumulatively fresh banta rehta hai.
// (Pehle poora doc replace hota tha → RM ka partial file baaki sab udaa deta tha.)
function saveEqRisk(payload){
  DB.setLocal('eq_risk', payload); // local: normal shape {code:{...}, updated, count}
  try{
    if(typeof fdb!=='undefined'){
      fdb.collection('crm_data').doc('eq_risk').set({
        data:{ codeJson: JSON.stringify(payload.code||{}), updated:payload.updated||'', count:payload.count||0 },
        updated:new Date().toISOString()
      }).then(()=>console.log('Firebase synced: eq_risk (compact,', payload.count,'clients)'))
        .catch(e=>{ console.log('Firebase error eq_risk:',e); toast('Sync error: '+e.message,'error'); });
    }
  }catch(e){}
}
function doRiskImport(payload){
  saveEqRisk(payload);
  clearEqRiskCache();
  closeModal('eqImportModal');
  toast(`✅ Risk data imported for ${payload.count} clients`, 'success');
  try{ renderEqTable(); }catch(e){}
}
// Formats a signed rupee value compactly with colour (green +, red −).
function fmtRiskMoney(n){
  if(n==null || n===0) return '<span style="color:#999">—</span>';
  const abs=Math.abs(n);
  const s = abs>=10000000 ? (abs/10000000).toFixed(2)+'Cr'
          : abs>=100000  ? (abs/100000).toFixed(2)+'L'
          : abs>=1000    ? (abs/1000).toFixed(1)+'K'
          : Math.round(abs);
  const pos=n>0;
  return `<span style="color:${pos?'var(--green,#16a34a)':'var(--red,#dc2626)'};font-weight:700">${pos?'+':'−'}₹${s}</span>`;
}
// MF AUM cell -> portfolio breakdown. Same idea as openEqRisk() on the equity
// side. Data comes from the AUM By Client import (aum_detail).
function openMfAum(id){
  const c = (DB.get('mf_clients')||[]).find(x=>x.id===id);
  const body = document.getElementById('mfAumModalBody');
  if(!c){ return; }
  const d = c.aum_detail;
  const row = (label,val) => `<div style="display:flex;justify-content:space-between;padding:11px 4px;border-bottom:1px solid #eef1f6">
    <span style="color:var(--gray);font-size:.86rem">${label}</span><span style="font-size:.95rem">${val}</span></div>`;
  const signed = (n,pct) => {
    if(n===null || n===undefined || n==='') return '—';
    const pos = Number(n) >= 0;
    const col = pos ? 'var(--green,#16a34a)' : 'var(--red,#dc2626)';
    const txt = pct ? Math.abs(Number(n)).toFixed(2)+'%' : '₹'+fmtNum(Math.abs(Number(n)));
    return `<b style="color:${col}">${pos?'+':'−'}${txt}</b>`;
  };
  const hdr = `<div style="font-weight:700;font-size:1.05rem;margin-bottom:4px">${escapeHtml(c.name||'')}</div>
    <div style="color:var(--gray);font-size:.78rem;margin-bottom:10px">PAN: ${escapeHtml(c.pan||'—')}${c.client_id?' • Client ID: '+escapeHtml(String(c.client_id)):''}</div>`;
  if(!d){
    body.innerHTML = hdr + `${row('AUM', c.aum?'<b>₹'+fmtNum(c.aum)+'</b>':'—')}
      <div style="margin-top:12px;color:var(--gray);font-size:.84rem">For more detail (Invested, Gain/Loss, XIRR), upload the AUM By Client report via MF → Import Excel.</div>`;
  } else {
    body.innerHTML = hdr +
      row('Invested', d.inv?'₹'+fmtNum(d.inv):'—') +
      row('Current AUM', c.aum?'<b>₹'+fmtNum(c.aum)+'</b>':'—') +
      row('Gain / Loss', signed(d.gl)) +
      row('Abs. Return', signed(d.ar, true)) +
      row('XIRR', signed(d.xirr, true)) +
      (d.dp ? row('Dividend Paid', '₹'+fmtNum(d.dp)) : '') +
      (d.dr ? row('Dividend Re-Inv', '₹'+fmtNum(d.dr)) : '') +
      (d.ad ? row('Avg. Days', fmtNum(d.ad)) : '') +
      `<div style="margin-top:10px;font-size:.72rem;color:#999">As per last uploaded AUM By Client report${d.on?' • '+fmtDate(d.on):''}</div>`;
  }
  document.getElementById('mfAumModal').classList.add('open');
}

function openEqRisk(code, name){
  const r = eqRiskFor(code);
  const body = document.getElementById('eqRiskModalBody');
  if(!r){
    body.innerHTML = `<div style="padding:8px 2px"><div style="font-weight:700;font-size:1rem;margin-bottom:6px">${escapeHtml(name||'')}</div>
      <div style="color:var(--gray)">No risk data for this client. Upload today's Broker RMS Risk file from Equity → Import.</div></div>`;
  } else {
    const riskColor = r.risk_pct>=90?'var(--red,#dc2626)':r.risk_pct>=50?'var(--orange,#ea580c)':'var(--green,#16a34a)';
    const rowHtml=(label,val)=>`<div style="display:flex;justify-content:space-between;padding:11px 4px;border-bottom:1px solid #eef1f6">
      <span style="color:var(--gray);font-size:.86rem">${label}</span><span style="font-size:.95rem">${val}</span></div>`;
    body.innerHTML = `
      <div style="font-weight:700;font-size:1.05rem;margin-bottom:4px">${escapeHtml(name||'')}</div>
      <div style="color:var(--gray);font-size:.78rem;margin-bottom:10px">Clt Code: ${escapeHtml(String(code))}</div>
      ${rowHtml('Ac Balance', fmtRiskMoney(r.ac_bal))}
      ${rowHtml('Stock Value', fmtRiskMoney(r.stk_val))}
      ${rowHtml('Total Exposure', r.tot_expo?`₹${fmtNum(r.tot_expo)}`:'—')}
      ${rowHtml('Risk Value', fmtRiskMoney(r.risk_val))}
      ${rowHtml('Risk %', `<b style="color:${riskColor}">${(r.risk_pct||0).toFixed(2)}%</b>`)}
      <div style="margin-top:10px;font-size:.72rem;color:#999">As per last uploaded RMS file${getEqRisk().updated?' • '+fmtDate(getEqRisk().updated.split('T')[0]):''}</div>`;
  }
  document.getElementById('eqRiskModal').classList.add('open');
}

// Strip country code / non-digits from a mobile number → keep last 10 digits.
// e.g. "919632817805" or "+91 96328-17805" → "9632817805"
function cleanMobileNo(v){
  let d = String(v||'').replace(/\D/g,'');
  if(d.length>10) d = d.slice(-10);   // drops leading 91 (or 0) country/trunk code
  return d;
}

// One-time cleanup: strip country code (91) from mobile of all EXISTING clients.
async function fixAllMobiles(){
  if(CU.role!=='admin') return;
  const keys=['eq_clients','mf_clients'];
  let total=0;
  const preview={};
  keys.forEach(k=>{
    const list=DB.get(k)||[];
    let n=0;
    list.forEach(c=>{
      const m=String(c.mobile||'').replace(/\D/g,'');
      const a=String(c.alt_mobile||'').replace(/\D/g,'');
      if(m.length>10 || a.length>10) n++;
    });
    preview[k]=n; total+=n;
  });
  if(total===0){ toast('✅ All mobile numbers are already clean — no 91 country code found','success'); return; }
  if(!confirm(`${total} clients have a country code (91) in their mobile number.\n\nEquity: ${preview.eq_clients||0}\nMF: ${preview.mf_clients||0}\n\nRemove the 91 from all of them and keep the last 10 digits? (Alternate number too)`)) return;
  let fixed=0;
  for(const k of keys){
    const list=DB.get(k)||[];
    const touched=[];
    list.forEach(c=>{
      let ch=false;
      const m=String(c.mobile||'').replace(/\D/g,'');
      if(m.length>10){ c.mobile=m.slice(-10); ch=true; }
      const a=String(c.alt_mobile||'').replace(/\D/g,'');
      if(a.length>10){ c.alt_mobile=a.slice(-10); ch=true; }
      if(ch){ touched.push(c); fixed++; }
    });
    if(touched.length) await DB.setClientsBulk(k, touched);
  }
  toast(`✅ ${fixed} clients' mobile numbers fixed (91 removed)`,'success');
  renderEqTable(); if(typeof renderMfTable==='function') renderMfTable(); refreshDash(); updateBadges();
}

// ── Fix Status (Last Trade) — Admin only ───────────────────────────────────
// Import sirf un clients ko chhoo sakta hai jo broker file me hote hain. Jo
// client file me hi nahi aaya (purane/dormant account), wo CRM me Active hi
// pada reh jata tha chahe uska last trade 5 saal purana ho. Ye button CRM ke
// apne last_trade_date se poore Equity base ka status dobara banata hai.
// Closed clients ko haath nahi lagate — wo broker se hi decide hote hain.
async function fixStatusByLastTrade(){
  if(CU.role!=='admin') return;
  const list = DB.get('eq_clients')||[];
  const toFix = [];
  let nBlankSkippedClosed=0, nBadDate=0;
  list.forEach(c=>{
    if(c.status==='Closed') return;              // don't touch Closed
    if(!c.last_trade_date){
      // Kabhi trade nahi kiya (ya date missing) — inhe bhi Inactive maano, jab tak Closed na ho.
      if(c.status!=='Inactive') toFix.push({c, from:c.status||'—', to:'Inactive'});
      else nBlankSkippedClosed++;
      return;
    }
    // daysDiff() wahi function hai jo table me "6299D AGO" dikhata hai — isse
    // har format (YYYY-MM-DD / 17-Apr-2009 / Date object) sahi parse hota hai.
    const days = daysDiff(c.last_trade_date);
    if(days===null){ nBadDate++; return; }       // date could not be parsed
    const want = days > EQ_INACTIVE_DAYS ? 'Inactive' : 'Active';
    if(want!==c.status) toFix.push({c, from:c.status||'—', to:want});
  });
  if(nBadDate) console.log('Fix Status: date parse fail ->', nBadDate, 'clients');
  if(!toFix.length){
    toast(`✅ All clients' status is already correct${nBadDate?` (${nBadDate} had a date that couldn't be parsed)`:''}`,'success');
    return;
  }
  const nInact = toFix.filter(x=>x.to==='Inactive').length;
  const nAct   = toFix.filter(x=>x.to==='Active').length;
  const nBlankInact = toFix.filter(x=>x.to==='Inactive' && !x.c.last_trade_date).length;
  if(!confirm(`${toFix.length} clients' status will be updated:\n\n`+
              `🟡 ${nInact} will be marked Inactive (${nBlankInact} have a blank last trade, the rest haven't traded in 1 year)\n`+
              `🟢 ${nAct} will be marked Active (traded within the last 1 year)\n\n`+
              `Left untouched: Closed clients`+
              `${nBadDate?`, ${nBadDate} whose date couldn't be parsed`:''}.\n\nProceed with the update?`)) return;
  const touched=[];
  toFix.forEach(x=>{ x.c.status = x.to; touched.push(x.c); });
  await DB.setClientsBulk('eq_clients', touched);
  DB.addActivityLog(toFix.map(x=>({ id:uid(), type:'edit', seg:'equity',
    client_id:x.c.id, client_name:x.c.name, rm:x.c.rm, by:CU.name,
    date:new Date().toISOString(),
    changes:[{field:'status', old:x.from, new:x.to}] })));
  toast(`✅ ${toFix.length} clients' status updated (${nInact} Inactive, ${nAct} Active)`,'success');
  renderEqTable(); refreshDash(); updateBadges();
}

// Entry point for the Equity "Import Excel" file input. Peeks at every sheet's
// header row first: if it matches the Square-off (T+5) file signature
// (Code + Dealer + Sq off Debit/Square Off Date), hands off to sqUpload() —
// which correctly merges ALL sheets/dates — instead of the normal single-sheet
// equity-client import. This lets Back Office upload T+5 files from the same
// Import Excel button without needing a separate page.
function handleEqFileEntry(input){
  const file = input.files[0];
  if(!file) return;
  const reader = new FileReader();
  reader.onload = function(e){
    try{
      const wb = XLSX.read(e.target.result, {type:'binary'});
      let isSquareoff = false;
      for(const sn of wb.SheetNames){
        const arr = XLSX.utils.sheet_to_json(wb.Sheets[sn], {header:1, defval:''});
        if(!arr.length) continue;
        const hdrRow = (arr[0]||[]).map(h=>String(h).toLowerCase().replace(/[^a-z0-9]/g,''));
        const hasCode = hdrRow.includes('code');
        const hasDealer = hdrRow.includes('dealer');
        const hasSqSignal = hdrRow.some(h=>h.includes('sqoffdebit')) || hdrRow.some(h=>h.includes('squareoffdate'));
        if(hasCode && hasDealer && hasSqSignal){ isSquareoff = true; break; }
      }
      if(isSquareoff){
        if(!canUploadSquareoff()){
          toast('Square-off (T+5) files can only be uploaded by admin/back-office/risk-upload RMs','error');
          input.value=''; return;
        }
        sqUpload(input); // re-reads the same file; handles multi-sheet date-merge itself
        setTimeout(()=>{ const m=document.getElementById('eqImportModal'); if(m) m.classList.remove('open'); }, 300);
        return;
      }
    }catch(err){ /* detection failed silently — fall through to normal import */ }
    handleEqFile(input);
  };
  reader.readAsBinaryString(file);
}

function handleEqFile(input){
  const file = input.files[0];
  if(!file) return;
  readExcel(file, function(err, rows){
    if(err){ toast('File read error: '+err.message,'error'); return; }

    // ── AUTO-DETECT: Broker RMS Risk file ──────────────────────────────────
    // Recognised by its signature header columns (Clt Code + Risk Val etc).
    // If found, import risk metrics instead of the normal client sheet.
    if(tryImportRiskFile(rows)) return;

    // ── GUARD: reject an MF AUM/SIP file dropped here by mistake ───────────
    // The MF "AUM By Client" report has "Client Name"+"Client ID"+"AUM"
    // columns, which auto-detect (below) would otherwise happily map onto
    // Name/Code/Asset Value — and since that file has NO Last Trade Date,
    // every unmatched Client ID gets added as a brand-new "Active" equity
    // client. Block it outright instead, with a clear message.
    for(let i=0;i<Math.min(rows.length,15);i++){
      const hdrRow = (rows[i]||[]).map(h=>normHdr(h));
      const hasMfSignature = hdrRow.some(h=>['invamt','investedamt','xirr','divpaid','divreinv','abrsrtn','absrtn'].includes(h));
      if(hasMfSignature){
        toast('⚠️ This looks like an MF AUM/SIP file (Inv. Amt / XIRR / Div. Paid column found) — upload it under MUTUAL FUND → Import Excel, not Equity Import.','error');
        return;
      }
    }

    // Find header row - look for a row containing recognizable column names
    let hdrIdx = -1, colMap = {};
    const wanted = {
      code: ['code','clientcode','clientid','ucccode','ucc'],
      mobile: ['mobile','mobileno','phone','contactno','mobilenumber'],
      email: ['email','emailid','emailaddress','mailid','mail'],
      dob: ['dob','dateofbirth','birthdate','birthday','dateofbirthdob'],
      name: ['name','clientname','investorname'],
      asset_value: ['assetvalue','holdingvalue','portfoliovalue','marketvalue','currentvalue','aum','asset'],
      last_trade_date: ['lasttradedate','lasttrade','lasttradingdate','lastdealdate'],
      status: ['accountstatus','acstatus','clientstatus','status','tradingstatus']
    };
    if(CU.role==='admin') wanted.rm = ['rm','relationshipmanager','dealer','dealername','employeename'];
    for(let i=0;i<Math.min(rows.length,15);i++){
      const row = rows[i];
      const map = {};
      row.forEach((cell,ci)=>{
        const h = normHdr(cell);
        for(const [field, variants] of Object.entries(wanted)){
          if(variants.includes(h) && map[field]===undefined) map[field]=ci;
        }
      });
      if(map.name!==undefined && (map.code!==undefined || map.mobile!==undefined)){
        hdrIdx=i; colMap=map; break;
      }
    }
    if(hdrIdx===-1){
      toast('Header row not found. Excel must have Code/Mobile and Name columns.','error');
      return;
    }
    const data = rows.slice(hdrIdx+1)
      .filter(r=>r.some(c=>c!==''))
      .map(r=>{
        const lt = colMap.last_trade_date!==undefined ? parseExcelDate(r[colMap.last_trade_date]) : '';
        const bs = colMap.status!==undefined ? normAcStatus(r[colMap.status]) : '';
        return {
        code: colMap.code!==undefined ? String(r[colMap.code]||'').trim() : '',
        mobile: colMap.mobile!==undefined ? cleanMobileNo(r[colMap.mobile]) : '',
        email: colMap.email!==undefined ? String(r[colMap.email]||'').trim().toLowerCase() : '',
        dob: colMap.dob!==undefined ? parseExcelDate(r[colMap.dob]) : '',
        name: colMap.name!==undefined ? String(r[colMap.name]||'').trim() : '',
        rm: colMap.rm!==undefined ? normRm(String(r[colMap.rm]||'').trim()) : '',
        asset_value: colMap.asset_value!==undefined ? (parseFloat(String(r[colMap.asset_value]).replace(/[₹,]/g,''))||null) : null,
        last_trade_date: lt,
        broker_status: bs,
        // Status file ke "Account Status" se nahi, LAST TRADE se banta hai
        status: deriveEqStatus(bs, lt)
        };
      })
      .filter(r=>r.code||r.mobile||r.name);

    eqImportData = data;
    const nClosed = data.filter(r=>r.status==='Closed').length;
    const nInact  = data.filter(r=>r.status==='Inactive').length;
    const nAct    = data.filter(r=>r.status==='Active').length;
    const nUnk    = data.filter(r=>!r.status).length;
    // Broker "Active" bol raha hai par 1 saal se trade nahi — inhe Inactive kiya
    const nFlip   = data.filter(r=>r.broker_status==='Active' && r.status==='Inactive').length;
    document.getElementById('eq-preview').innerHTML =
      `<div style="background:var(--green2);color:var(--green);padding:10px;border-radius:8px;font-size:.85rem;font-weight:600">
       ✅ ${data.length} rows found (matched by Code/Mobile)</div>` +
      (colMap.last_trade_date!==undefined
        ? `<div style="background:#FEF6E7;color:#92400E;padding:8px 10px;border-radius:8px;font-size:.8rem;font-weight:600;margin-top:6px">
           📅 Status will be based on <b>Last Trade Date</b> (older than 1 year = Inactive):<br>
           🟢 ${nAct} Active &nbsp;·&nbsp; 🟡 ${nInact} Inactive &nbsp;·&nbsp; 🔴 ${nClosed} Closed${nUnk?` &nbsp;·&nbsp; ⚪ ${nUnk} last trade blank (status waisa hi rahega)`:''}
           ${nFlip?`<br>⚠️ Of these, the file says <b>${nFlip}</b> are "Active" but haven't traded in 1 year — they were kept Inactive.`:''}
           <br>New Closed accounts will not be added; existing clients will be marked Closed.</div>`
        : `<div style="background:#FEE2E2;color:#991B1B;padding:8px 10px;border-radius:8px;font-size:.8rem;font-weight:600;margin-top:6px">
           ⚠️ Last Trade Date column not found — status will not be updated.</div>`);
    document.getElementById('eqImportBtnGo').disabled = data.length===0;
  });
}

async function doEqImport(){
  if(!eqImportData || !eqImportData.length) return;
  const existing = DB.get('eq_clients')||[];
  const byCode = {}, byName = {};
  existing.forEach(c=>{
    if(c.code) byCode[String(c.code).trim().toUpperCase()] = c;
    // Duplicate naam wale clients ko AMBIGUOUS mark karo — inko naam se
    // match karna khatarnak hai (ek hi naam ke 2-7 alag codes hote hain).
    if(c.name){
      const nk = String(c.name).trim().toUpperCase();
      byName[nk] = (byName[nk] === undefined) ? c : 'AMBIGUOUS';
    }
  });
  // ids already used (to avoid collision with new uids)
  const usedIds = new Set(existing.map(c=>c.id));

  let updated=0, added=0, skipped=0, closedSkipped=0, closedMarked=0;
  const touched = [], statusFixes = [];
  eqImportData.forEach(row=>{
    // ── MATCHING (fixed) ────────────────────────────────────────────────
    // Pehle jo logic tha: code na mile to naam se match kar leta tha. Bug ye
    // tha ki ek hi naam ke kai alag accounts hote hain (e.g. NITESH KUMAR ke
    // 4 codes: 225870/1539468 Active, 1788150/3335199 Closed). Closed wale
    // code ki row CRM me na milne par naam se fallback hota tha aur Active
    // wale Nitesh pe Closed chipak jaata tha. File me 141 duplicate naam
    // hain jinme 75 ka status mixed hai → 75 clients tak galat ho sakte the.
    // Fix: agar row me CODE hai to SIRF code se match karo. Code diya hai
    // par CRM me nahi mila = ye alag account hai → naya add hoga (ya Closed
    // hone par skip). Naam-fallback sirf tab jab row me code hi na ho, aur
    // tab bhi sirf unique naam ke liye.
    let ex = null;
    if(row.code){
      ex = byCode[String(row.code).trim().toUpperCase()] || null;
    } else if(row.name){
      const nm = byName[String(row.name).trim().toUpperCase()];
      if(nm && nm !== 'AMBIGUOUS') ex = nm;
    }

    // Closed account jo CRM me hai hi nahi -> post hi mat karo
    if(!ex && row.status==='Closed'){ closedSkipped++; return; }

    if(ex){
      if(row.status){
        const old = ex.status || '(blank)';
        if(old !== row.status){
          statusFixes.push([ex.rm||'—', ex.code||'', ex.name||'', old, row.status]);
          if(row.status==='Closed') closedMarked++;
        }
        ex.status = row.status;                // Active / Inactive / Closed file se
      }
      if(row.asset_value!==null) ex.asset_value = row.asset_value;
      if(row.last_trade_date && row.last_trade_date!==ex.last_trade_date){
        // Comeback-trade highlight: colour the Name for the day this trade
        // shows up, based on how long the client had gone without trading.
        const gapDays = ex.last_trade_date ? daysBetween(ex.last_trade_date, row.last_trade_date) : null;
        if(gapDays!==null && gapDays>=180) ex.comeback_tag='yellow';
        else if(gapDays!==null && gapDays>=90) ex.comeback_tag='green';
        else if(gapDays!==null && gapDays>=30) ex.comeback_tag='blue';
        else ex.comeback_tag='';
        ex.comeback_date = row.last_trade_date;   // tag only applies while this stays the latest trade date
        ex.last_trade_date = row.last_trade_date;
      }
      if(row.email) ex.email = row.email;      // always overwrite email from file
      if(row.mobile) ex.mobile = row.mobile;   // update mobile (country code stripped)
      if(row.dob) ex.dob = row.dob;            // fill DOB if present in file
      ex.updated = today();
      touched.push(ex);
      updated++;
    } else if(row.name){
      // New client - staff must use their own RM; admin (and Back Office,
      // uploading the whole office's file) can leave RM blank / use the
      // file's RM column if not present in the Excel.
      let rm = row.rm || '';
      if(CU.role!=='admin' && CU.role!=='backoffice' && !CU.backoffice_access) rm = CU.name;
      let id = uid();
      while(usedIds.has(id)) id = uid();
      usedIds.add(id);
      const rec = {
        id, code:row.code, name:row.name, mobile:row.mobile, email:row.email||'', dob:row.dob||'',
        rm, status: row.status || 'Active',
        asset_value: row.asset_value, revenue:null,
        last_trade_date: row.last_trade_date, last_trade_month:'',
        last_call_date:'', next_call:'', followup_status:'', remarks:'',
        created: today(), updated: today()
      };
      touched.push(rec);
      added++;
    } else {
      skipped++;
    }
  });

  await DB.setClientsBulk('eq_clients', touched);
  closeModal('eqImportModal');
  let msg = `✅ Import done! ${updated} updated, ${added} new added`;
  if(statusFixes.length) msg += `, ${statusFixes.length} status corrected`;
  if(closedSkipped) msg += `, ${closedSkipped} Closed skipped (not added)`;
  if(skipped) msg += `, ${skipped} skipped (no name)`;
  toast(msg, 'success');
  renderEqTable(); refreshDash(); updateBadges();

  // Import se jo bhi status auto-correct hua (jaise 1 saal purana last trade
  // -> Inactive) usko bhi activity_logs me daalo — warna wo sirf yahan ke
  // one-time popup me dikhta tha aur RM/Admin ke "Changed Clients" date-wise
  // history me kabhi nahi aata tha.
  if(statusFixes.length){
    DB.addActivityLog(statusFixes.map(([rm,code,name,oldStatus,newStatus])=>({
      id: uid(), type:'edit', seg:'equity',
      client_id: (byCode[String(code).trim().toUpperCase()]||{}).id || '',
      client_name: name, rm, by: CU.name, date: new Date().toISOString(),
      changes: [{field:'status', old:oldStatus, new:newStatus}]
    })));
  }

  // Jo status file se theek hue (RM ne galat mark kiya tha) — unki report dikhao
  if(statusFixes.length){
    setTimeout(()=>{
      showReport(`Status Corrections — ${statusFixes.length} clients (${closedMarked} Closed mark)`,
        ['RM','Code','Name','Was in CRM as','From Last Trade'], statusFixes);
    }, 600);
  }
}

// ══ EQUITY BULK UPDATE — RM / PAN / DOB ek hi file se (Admin only) ══
// Client Code zaroori. RM / PAN / DOB me se jo column file me mile, sirf wahi
// field update hota hai. Naye client add nahi hote, baaki fields touch nahi hote.
let eqBulkData = null;      // {code, rm?, pan?, dob?}[]
let eqBulkFields = [];      // file me kaunse field mile

function openEqBulkModal(){
  if(CU.role!=='admin') return;
  eqBulkData = null; eqBulkFields = [];
  document.getElementById('eq-bulk-preview').innerHTML='';
  document.getElementById('eqBulkBtnGo').disabled=true;
  document.getElementById('eq-bulk-file').value='';
  document.getElementById('eqBulkModal').classList.add('open');
}

function downloadEqBulkTemplate(){
  const wsData = [
    ['Client Code','Client Name (reference only)','New RM','PAN','DOB'],
    ['EQ12345','Example: Ramesh Kumar','Komal','ABCDE1234F','19561009'],
    ['EQ67890','Example: Sunita Devi','Bharat','',''],
    ['EQ11111','Example: DOB update only','','','09/10/1956']
  ];
  const ws = XLSX.utils.aoa_to_sheet(wsData);
  ws['!cols'] = [{wch:18},{wch:32},{wch:16},{wch:16},{wch:14}];
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Bulk Update');
  XLSX.writeFile(wb, 'Equity_Bulk_Update_Template.xlsx');
}

function handleEqBulkFile(input){
  const file = input.files[0];
  if(!file) return;
  readExcel(file, function(err, rows){
    if(err){ toast('File read error: '+err.message,'error'); return; }
    const wanted = {
      code: ['code','clientcode','clientid','ucccode','ucc'],
      rm:   ['newrm','rm','relationshipmanager','dealer','dealername','employeename'],
      pan:  ['pan','pancard','panno','pannumber','pancardno','pancardnumber'],
      dob:  ['dob','dateofbirth','birthdate','birthday','dateofbirthdob','invdob','dobdate']
    };
    let hdrIdx=-1, colMap={};
    for(let i=0;i<Math.min(rows.length,15);i++){
      const map = {};
      rows[i].forEach((cell,ci)=>{
        const h = normHdr(cell);
        for(const [field, variants] of Object.entries(wanted)){
          if(variants.includes(h) && map[field]===undefined) map[field]=ci;
        }
      });
      // Code + kam se kam ek updatable field mile tabhi header row maano
      if(map.code!==undefined && (map.rm!==undefined || map.pan!==undefined || map.dob!==undefined)){
        hdrIdx=i; colMap=map; break;
      }
    }
    if(hdrIdx===-1){
      toast('Header row not found. The Excel file must have a Client Code column plus at least one of RM / PAN / DOB.','error');
      return;
    }

    eqBulkFields = ['rm','pan','dob'].filter(f=>colMap[f]!==undefined);
    const validRms = new Set((DB.get('users')||DEFAULT_USERS).filter(u=>u.role!=='admin').map(u=>u.name));
    const panRe = /^[A-Z]{5}[0-9]{4}[A-Z]$/;
    const _blank = ['','0','null','na','n/a','-','00000000'];
    const todayStr = today();

    const data = [];
    const seen = new Set();
    let dupSkipped=0, badRm=0, badPanFmt=0;
    const badDates = [];
    const counts = {rm:0, pan:0, dob:0};

    rows.slice(hdrIdx+1).filter(r=>r.some(c=>c!=='')).forEach(r=>{
      const code = String(r[colMap.code]||'').trim();
      if(!code) return;
      const ck = code.toUpperCase();
      if(seen.has(ck)){ dupSkipped++; return; }   // same code repeat — pehli row hi
      const row = {code};

      if(colMap.rm!==undefined){
        const rm = normRm(String(r[colMap.rm]||'').trim());
        if(rm){
          if(validRms.has(rm)){ row.rm = rm; counts.rm++; }
          else badRm++;
        }
      }
      if(colMap.pan!==undefined){
        const pan = String(r[colMap.pan]||'').trim().toUpperCase();
        if(pan && !_blank.includes(pan.toLowerCase())){
          row.pan = pan; counts.pan++;
          if(!panRe.test(pan)) badPanFmt++;
        }
      }
      if(colMap.dob!==undefined){
        const rawDob = r[colMap.dob];
        const rawStr = String(rawDob==null?'':rawDob).trim().toLowerCase();
        if(!_blank.includes(rawStr)){
          const dob = normDob(rawDob);
          if(!dob) badDates.push([code, String(rawDob), 'Date could not be parsed']);
          else if(dob > todayStr) badDates.push([code, String(rawDob), 'Future date']);
          else if(dob < '1900-01-01') badDates.push([code, String(rawDob), 'Before 1900']);
          else { row.dob = dob; counts.dob++; }
        }
      }

      if(row.rm || row.pan || row.dob){ seen.add(ck); data.push(row); }
    });

    if(!data.length){
      toast('No valid rows found (Client Code plus at least one of RM / PAN / DOB is required).','error');
      return;
    }

    eqBulkData = data;
    const labels = {rm:'RM', pan:'PAN', dob:'DOB'};
    const found = eqBulkFields.map(f=>`<b>${labels[f]}</b> (${counts[f]})`).join(', ');
    let html = `<div style="background:var(--green2);color:var(--green);padding:10px;border-radius:8px;font-size:.85rem;font-weight:600">
       ✅ ${data.length} rows — will be updated: ${found}</div>`;
    if(dupSkipped){
      html += `<div style="background:#eef6ff;color:#1d4ed8;padding:8px 10px;border-radius:8px;font-size:.8rem;margin-top:8px;font-weight:600">
       ℹ️ ${dupSkipped} duplicate Client Code row(s) — the first row was used.</div>`;
    }
    if(badRm){
      html += `<div style="background:#fdecea;color:var(--red);padding:10px;border-radius:8px;font-size:.8rem;margin-top:8px">
       ⚠️ ${badRm} row(s) have an RM name that doesn't match the CRM's list — that RM will be skipped (other fields will still update). The name must be exact (e.g. Komal, Bharat).</div>`;
    }
    if(badPanFmt){
      html += `<div style="background:#fff7e6;color:var(--orange);padding:10px;border-radius:8px;font-size:.8rem;margin-top:8px">
       ⚠️ ${badPanFmt} PAN doesn't look like the standard format (ABCDE1234F) — it will be uploaded as-is, please double-check.</div>`;
    }
    if(badDates.length){
      html += `<div style="background:#fff7e6;color:var(--orange);padding:10px;border-radius:8px;font-size:.8rem;margin-top:8px;font-weight:600">
       ⚠️ ${badDates.length} row(s) have an invalid DOB — that DOB will be skipped.
       <a href="#" onclick="showEqBulkBadDobReport();return false;" style="color:var(--teal);text-decoration:underline;margin-left:6px">Dekho</a></div>`;
      window._eqBulkBadDob = badDates;
    } else {
      window._eqBulkBadDob = null;
    }
    document.getElementById('eq-bulk-preview').innerHTML = html;
    document.getElementById('eqBulkBtnGo').disabled = false;
  });
}

function showEqBulkBadDobReport(){
  if(!window._eqBulkBadDob) return;
  showReport(`Invalid DOB rows (${window._eqBulkBadDob.length})`,
    ['Client Code','File ki value','Wajah'], window._eqBulkBadDob);
}

async function doEqBulkUpdate(){
  if(CU.role!=='admin') return;
  if(!eqBulkData || !eqBulkData.length) return;
  const existing = DB.get('eq_clients')||[];
  const byCode = {};
  existing.forEach(c=>{ if(c.code) byCode[String(c.code).trim().toUpperCase()] = c; });

  const upd = {rm:0, pan:0, dob:0};
  let unchanged=0;
  const touched = [], newLogs = [], notFoundRows = [];

  eqBulkData.forEach(row=>{
    const ex = byCode[row.code.trim().toUpperCase()];
    if(!ex){ notFoundRows.push([row.code, row.rm||'—', row.pan||'—', row.dob?fmtDate(row.dob):'—']); return; }
    const changes = [];

    if(row.rm && ex.rm!==row.rm){
      changes.push({field:'rm', old:ex.rm||'—', new:row.rm});
      ex.rm = row.rm; upd.rm++;
    }
    if(row.pan){
      const oldPan = (ex.pan||'').trim().toUpperCase();
      if(oldPan!==row.pan){
        changes.push({field:'pan', old:oldPan||'—', new:row.pan});
        ex.pan = row.pan; upd.pan++;
      }
    }
    if(row.dob){
      const oldDob = (ex.dob||'').trim();
      if(oldDob!==row.dob){
        changes.push({field:'dob', old:oldDob||'—', new:row.dob});
        ex.dob = row.dob; upd.dob++;
      }
    }

    if(!changes.length){ unchanged++; return; }
    ex.updated = today();
    touched.push(ex);
    newLogs.push({
      id: uid(), type: 'bulk_update', seg:'equity',
      client_id: ex.id, client_name: ex.name, rm: ex.rm||'',
      by: CU.name, date: new Date().toISOString(),
      changes
    });
  });

  if(touched.length) await DB.setClientsBulk('eq_clients', touched);
  if(newLogs.length) await DB.addActivityLog(newLogs);

  closeModal('eqBulkModal');
  const parts = [];
  if(upd.rm) parts.push(`RM: ${upd.rm}`);
  if(upd.pan) parts.push(`PAN: ${upd.pan}`);
  if(upd.dob) parts.push(`DOB: ${upd.dob}`);
  let msg = `✅ Bulk Update done! ${touched.length} client(s) updated` + (parts.length?` (${parts.join(', ')})`:'');
  if(unchanged) msg += `, ${unchanged} already same`;
  if(notFoundRows.length) msg += `, ${notFoundRows.length} Client Code not found`;
  toast(msg, touched.length>0?'success':'error');
  renderEqTable(); refreshDash(); updateBadges();

  if(notFoundRows.length){
    setTimeout(()=>{
      showReport(`Client Code Not Found — ${notFoundRows.length} rows (skipped)`,
        ['Client Code','RM','PAN','DOB'], notFoundRows);
    }, 600);
  }
}

// ══ MF BULK DOB UPDATE (Admin only) ══
// Broker client-list exports have Name as "NAME [PAN] [CLIENT CODE]" (same
// bracket format as the Running SIP Report), followed by a DOB column. This
// parses that format directly — no need to split it into separate columns.
let mfBulkDobData = null;
let mfBulkDobBad = [];

function openMfBulkDobModal(){
  if(CU.role!=='admin') return;
  mfBulkDobData = null; mfBulkDobBad = [];
  document.getElementById('mf-bulk-dob-preview').innerHTML='';
  document.getElementById('mfBulkDobBtnGo').disabled=true;
  document.getElementById('mf-bulk-dob-file').value='';
  document.getElementById('mfBulkDobModal').classList.add('open');
}

function parseMfDobFile(rows){
  const wanted = {
    name: ['name','clientname','investorname'],
    dob:  ['dob','dateofbirth','birthdate','birthday']
  };
  let hdrIdx=-1, colMap={};
  for(let i=0; i<Math.min(rows.length,15); i++){
    const map={};
    (rows[i]||[]).forEach((cell,ci)=>{
      const h=normHdr(cell);
      for(const [f,vars] of Object.entries(wanted)){
        if(vars.includes(h) && map[f]===undefined) map[f]=ci;
      }
    });
    if(map.name!==undefined && map.dob!==undefined){ hdrIdx=i; colMap=map; break; }
  }
  if(hdrIdx===-1) return null;   // caller aborts — never guess positions

  const data=[], bad=[];
  rows.slice(hdrIdx+1).forEach(r=>{
    if(!r || !r.some(c=>c!=='' && c!=null)) return;
    const nameRaw = String(r[colMap.name]||'');
    if(!nameRaw.trim()) return;
    // Same bracket parsing as the Running SIP Report: "NAME [PAN] [CLIENT CODE]"
    const name = nameRaw.replace(/\[.*?\]/g,'').replace(/\s+/g,' ').trim();
    const panMatch = nameRaw.match(/\[([A-Z]{5}\d{4}[A-Z])\]/);
    const pan = panMatch ? panMatch[1] : '';
    const cidMatch = nameRaw.match(/\[(\d{4,})\]/);
    const clientId = cidMatch ? cidMatch[1] : '';
    if(!pan && !clientId){ bad.push([name||'—', '—', 'No PAN/Client Code in name — cannot match']); return; }

    const rawDob = String(r[colMap.dob]==null?'':r[colMap.dob]).trim();
    if(!rawDob || rawDob==='0000-00-00'){ bad.push([name, clientId||pan, rawDob||'(blank)']); return; }
    const dob = normDob(rawDob);
    if(!dob || dob.slice(0,4)==='0000'){ bad.push([name, clientId||pan, rawDob]); return; }

    data.push({name, pan, client_id:clientId, dob});
  });
  return {data, bad};
}

function handleMfBulkDobFile(input){
  const file = input.files[0];
  if(!file) return;
  readExcel(file, function(err, rows){
    if(err){ toast('File read error: '+err.message,'error'); return; }
    const parsed = parseMfDobFile(rows);
    if(!parsed || !parsed.data.length){
      toast('The "Name" and "DOB" columns could not be found in the file, or no valid rows were found','error');
      document.getElementById('mfBulkDobBtnGo').disabled=true;
      return;
    }
    // De-dup by client_id (or PAN if no client_id) — last row wins, same as Eq bulk
    const byKey = {};
    let dupSkipped = 0;
    parsed.data.forEach(row=>{
      const key = row.client_id || row.pan;
      if(byKey[key]) dupSkipped++;
      byKey[key] = row;
    });
    mfBulkDobData = Object.values(byKey);
    mfBulkDobBad = parsed.bad;

    let html = `<div style="background:var(--green2);color:var(--green);padding:10px;border-radius:8px;font-size:.85rem;font-weight:600">
       ✅ ${mfBulkDobData.length} row(s) — DOB will be updated</div>`;
    if(dupSkipped){
      html += `<div style="background:#eef6ff;color:#1d4ed8;padding:8px 10px;border-radius:8px;font-size:.8rem;margin-top:8px;font-weight:600">
       ℹ️ ${dupSkipped} duplicate row(s) — the last one was used.</div>`;
    }
    if(mfBulkDobBad.length){
      html += `<div style="background:#fff7e6;color:var(--orange);padding:10px;border-radius:8px;font-size:.8rem;margin-top:8px;font-weight:600">
       ⚠️ ${mfBulkDobBad.length} row(s) skipped (invalid DOB or PAN/Client Code missing).
       <a href="#" onclick="showMfBulkDobBadReport();return false;" style="color:var(--teal);text-decoration:underline;margin-left:6px">Dekho</a></div>`;
    }
    document.getElementById('mf-bulk-dob-preview').innerHTML = html;
    document.getElementById('mfBulkDobBtnGo').disabled = false;
  });
}

function showMfBulkDobBadReport(){
  if(!mfBulkDobBad.length) return;
  showReport(`Skipped rows (${mfBulkDobBad.length})`, ['Name','PAN / Client Code','Wajah'], mfBulkDobBad);
}

async function doMfBulkDobUpdate(){
  if(CU.role!=='admin') return;
  if(!mfBulkDobData || !mfBulkDobData.length) return;
  const existing = DB.get('mf_clients')||[];
  const byClientId = {}, byPan = {};
  existing.forEach(c=>{
    if(c.client_id) byClientId[String(c.client_id).trim()] = c;
    const p = String(c.pan||'').trim().toUpperCase();
    if(p && !c.is_minor) byPan[p] = c;   // minors can carry a guardian's PAN — never match on it
  });

  let updated=0, unchanged=0;
  const touched=[], newLogs=[], notFoundRows=[];

  mfBulkDobData.forEach(row=>{
    const ex = (row.client_id && byClientId[row.client_id]) || (row.pan && byPan[row.pan]) || null;
    if(!ex){ notFoundRows.push([row.name, row.client_id||row.pan||'—', fmtDate(row.dob)]); return; }
    const oldDob = (ex.dob||'').trim();
    if(oldDob===row.dob){ unchanged++; return; }
    ex.dob = row.dob;
    ex.updated = today();
    updated++;
    touched.push(ex);
    newLogs.push({
      id: uid(), type:'bulk_update', seg:'mf',
      client_id: ex.id, client_name: ex.name, rm: ex.rm||'',
      by: CU.name, date: new Date().toISOString(),
      changes: [{field:'dob', old:oldDob||'—', new:row.dob}]
    });
  });

  if(touched.length) await DB.setClientsBulk('mf_clients', touched);
  if(newLogs.length) await DB.addActivityLog(newLogs);

  closeModal('mfBulkDobModal');
  let msg = `✅ Bulk DOB Update done! ${updated} investor(s) updated`;
  if(unchanged) msg += `, ${unchanged} already same`;
  if(notFoundRows.length) msg += `, ${notFoundRows.length} not found`;
  toast(msg, updated>0?'success':'error');
  renderMfTable(); refreshDash(); updateBadges();

  if(notFoundRows.length){
    setTimeout(()=>{
      showReport(`PAN / Client Code Not Found — ${notFoundRows.length} rows (skipped)`,
        ['Name','PAN / Client Code','DOB'], notFoundRows);
    }, 600);
  }
}


// ══ TABLE SORTING (Universal) ══
window._sortState = {};
function makeSortable(tableSelector){
  const tbl = document.querySelector(tableSelector);
  if(!tbl) return;
  const ths = tbl.querySelectorAll('thead th');
  ths.forEach((th, i) => {
    if(th.textContent.trim() === 'Actions') return;
    th.style.cursor = 'pointer';
    th.title = 'Click to sort';
    th.onclick = () => {
      const tbody = tbl.querySelector('tbody');
      const rows = Array.from(tbody.querySelectorAll('tr'));
      const key = tableSelector + '-' + i;
      const dir = window._sortState[key] === 1 ? -1 : 1;
      window._sortState[key] = dir;
      rows.sort((a, b) => {
        let at = a.cells[i] ? a.cells[i].textContent.trim() : '';
        let bt = b.cells[i] ? b.cells[i].textContent.trim() : '';
        // Try date parse (dd-mmm-yyyy)
        const da = Date.parse(at), db = Date.parse(bt);
        if(!isNaN(da) && !isNaN(bt) && at.match(/\d{2}-[A-Za-z]{3}-\d{4}/) && bt.match(/\d{2}-[A-Za-z]{3}-\d{4}/)){
          return dir * (da - db);
        }
        // Try numeric (strip currency symbols, commas, K/L/Cr)
        const parseNum = (s) => {
          s = s.replace(/[₹,\s]/g,'');
          let mult = 1;
          if(s.endsWith('Cr')){ mult=10000000; s=s.slice(0,-2); }
          else if(s.endsWith('L')){ mult=100000; s=s.slice(0,-1); }
          else if(s.endsWith('K')){ mult=1000; s=s.slice(0,-1); }
          const n = parseFloat(s);
          return isNaN(n) ? null : n*mult;
        };
        const na = parseNum(at), nb = parseNum(bt);
        if(na !== null && nb !== null){
          return dir * (na - nb);
        }
        return dir * at.localeCompare(bt, undefined, {numeric:true, sensitivity:'base'});
      });
      rows.forEach(r => tbody.appendChild(r));
      // Update header arrows
      ths.forEach((h, j) => {
        h.textContent = h.textContent.replace(/ [▲▼]/g, '');
        if(j === i) h.textContent += (dir === 1 ? ' ▲' : ' ▼');
      });
    };
  });
}

// NOTE: eq-table and mf-table now use data-level sorting (sortEqTable/sortMfTable)
// defined alongside their render functions, so the generic DOM-reorder
// makeSortable() below is only applied to follow-up/no-trade/sip tables.

// For follow-up / no-trade / sip tabs, hook into tab switching
function sortAllVisibleTables(){
  document.querySelectorAll('.page.active table, .tab-panel.active table, #eqf-content table, #mff-content table, #nt-content table, #sip-table table').forEach((tbl, idx) => {
    let sel = null;
    if(tbl.closest('#eqf-content')) sel = '#eqf-content table';
    else if(tbl.closest('#mff-content')) sel = '#mff-content table';
    else if(tbl.closest('#nt-content')) sel = '#nt-content table';
    else if(tbl.closest('#sip-table')) sel = '#sip-table table';
    if(sel) makeSortable(sel);
  });
}

// Hook into tab clicks for follow-up/no-trade/sip pages
document.addEventListener('click', function(e){
  if(e.target.classList.contains('tab-item')){
    setTimeout(sortAllVisibleTables, 100);
  }
  if(e.target.closest('.nav-item')){
    setTimeout(sortAllVisibleTables, 150);
  }
});

// Initial sort setup after page load
setTimeout(sortAllVisibleTables, 1000);

// ══════════════════════════════════════════
// COLUMN RESIZE (drag column borders to resize)
// ══════════════════════════════════════════
function enableColumnResize(table){
  if(table.dataset.resizableInit) return;
  table.dataset.resizableInit='1';
  const ths = table.querySelectorAll('thead th');
  ths.forEach(th=>{
    if(th.querySelector('.col-resize-handle')) return;
    th.style.position = th.style.position || 'sticky';
    const handle=document.createElement('div');
    handle.className='col-resize-handle';
    th.appendChild(handle);

    const startResize=(startX)=>{
      const startWidth=th.offsetWidth;
      if(table.style.tableLayout!=='fixed'){
        ths.forEach(t=>{ t.style.width=t.offsetWidth+'px'; });
        table.style.tableLayout='fixed';
      }
      handle.classList.add('active');
      return startWidth;
    };

    handle.addEventListener('mousedown', function(e){
      e.preventDefault(); e.stopPropagation();
      const startX=e.pageX;
      const startWidth=startResize(startX);
      function onMove(ev){
        const newWidth=Math.max(36, startWidth + (ev.pageX-startX));
        th.style.width=newWidth+'px';
      }
      function onUp(){
        handle.classList.remove('active');
        document.removeEventListener('mousemove',onMove);
        document.removeEventListener('mouseup',onUp);
      }
      document.addEventListener('mousemove',onMove);
      document.addEventListener('mouseup',onUp);
    });

    handle.addEventListener('touchstart', function(e){
      e.stopPropagation();
      const touch=e.touches[0];
      const startX=touch.pageX;
      const startWidth=startResize(startX);
      function onMove(ev){
        const t=ev.touches[0];
        const newWidth=Math.max(36, startWidth + (t.pageX-startX));
        th.style.width=newWidth+'px';
      }
      function onEnd(){
        handle.classList.remove('active');
        document.removeEventListener('touchmove',onMove);
        document.removeEventListener('touchend',onEnd);
      }
      document.addEventListener('touchmove',onMove,{passive:true});
      document.addEventListener('touchend',onEnd);
    },{passive:true});
  });
}

function scanForResizableTables(root){
  (root||document).querySelectorAll('.tbl-scroll table, #seminar-attendees-table table').forEach(t=>{
    if(!t.dataset.resizableInit) enableColumnResize(t);
  });
}

// Observe content area for new/re-rendered tables
const colResizeObserver = new MutationObserver(muts=>{
  for(const m of muts){
    if(m.addedNodes && m.addedNodes.length){
      m.addedNodes.forEach(node=>{
        if(node.nodeType!==1) return;
        if(node.tagName==='TABLE' && !node.dataset.resizableInit) enableColumnResize(node);
        else if(node.querySelectorAll) scanForResizableTables(node);
      });
    }
  }
});
colResizeObserver.observe(document.body, {childList:true, subtree:true});

// Initial scan
setTimeout(()=>scanForResizableTables(document), 500);

// Debug helper - type checkFollowups() in browser console
window.checkFollowups = function(){
  const eq = DB.get('eq_clients')||[];
  const t = today();
  const withNextCall = eq.filter(c=>c.next_call);
  const due = eq.filter(c=>c.next_call && c.next_call<=t);
  console.log('Total eq_clients:', eq.length);
  console.log('With next_call set:', withNextCall.length);
  console.log('Due today or overdue (<='+t+'):', due.length);
  if(withNextCall.length>0) console.log('Sample next_call values:', withNextCall.slice(0,5).map(c=>c.name+': '+c.next_call));
  return {total:eq.length, withNextCall:withNextCall.length, due:due.length};
};

/* ── Generic click-to-sort for every data table (any <table> with a <thead>) ──
   • Click a column header to sort; click again to flip ascending/descending.
   • Auto-detects numbers (₹/commas/%), dates (dd-mm-yyyy or yyyy-mm-dd) and text.
   • Headers that already have their own onclick sorter (e.g. MF Transactions,
     New Business report) are left untouched — no double-sorting.
   • Empty/action columns are not clickable. TOTAL / footer rows stay pinned at
     the bottom. This is a view-only convenience; a table re-render resets it. */
(function(){
  if(window.__genericSortInit) return; window.__genericSortInit=true;
  const st=document.createElement('style');
  st.textContent='table thead th{cursor:pointer;user-select:none}';
  (document.head||document.documentElement).appendChild(st);

  function parse(s){
    const n=s.replace(/[₹,%]/g,'').replace(/\s/g,'');
    if(n!=='' && /[0-9]/.test(n) && !isNaN(n)) return {t:'num', v:parseFloat(n)};
    let m=s.match(/^(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})$/);
    if(m) return {t:'str', v:m[3]+m[2].padStart(2,'0')+m[1].padStart(2,'0')};
    m=s.match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})$/);
    if(m) return {t:'str', v:m[1]+m[2].padStart(2,'0')+m[3].padStart(2,'0')};
    return {t:'str', v:s.toLowerCase()};
  }

  document.addEventListener('click', function(e){
    const th=e.target.closest('th'); if(!th) return;
    if(!th.closest('thead')) return;
    const table=th.closest('table'); if(!table) return;
    if(th.hasAttribute('onclick')) return;                 // has its own sorter
    const ths=Array.from(th.parentElement.children);
    const col=ths.indexOf(th); if(col<0) return;
    if(!th.textContent.replace(/[▲▼⇅]/g,'').trim()) return; // empty / action column
    const tbody=table.tBodies[0]; if(!tbody) return;

    let dir='asc';
    if(table.getAttribute('data-gs-col')===String(col))
      dir=table.getAttribute('data-gs-dir')==='asc'?'desc':'asc';
    table.setAttribute('data-gs-col',col);
    table.setAttribute('data-gs-dir',dir);
    const d=dir==='asc'?1:-1;

    const pinned=[], list=[];
    Array.from(tbody.rows).forEach(r=>{
      const f=(r.cells[0]?r.cells[0].textContent:'').trim().toUpperCase();
      if(r.classList.contains('no-sort')||f.startsWith('TOTAL')||f.startsWith('GRAND')) pinned.push(r);
      else list.push(r);
    });
    list.sort((a,b)=>{
      const pa=parse((a.cells[col]?a.cells[col].textContent:'').trim());
      const pb=parse((b.cells[col]?b.cells[col].textContent:'').trim());
      let c;
      if(pa.t==='num'&&pb.t==='num') c=pa.v-pb.v;
      else c=String(pa.v).localeCompare(String(pb.v),undefined,{numeric:true});
      return c*d;
    });
    list.forEach(r=>tbody.appendChild(r));
    pinned.forEach(r=>tbody.appendChild(r));

    ths.forEach((h,i)=>{
      const ex=h.querySelector('.gs-arrow'); if(ex) ex.remove();
      if(h.hasAttribute('onclick')) return;
      if(!h.textContent.replace(/[▲▼⇅]/g,'').trim()) return;
      const sp=document.createElement('span');
      sp.className='gs-arrow';
      sp.style.cssText='color:#9aa;font-size:.7em;margin-left:3px';
      sp.textContent = i===col ? (dir==='asc'?'▲':'▼') : '';
      h.appendChild(sp);
    });
  });
})();

