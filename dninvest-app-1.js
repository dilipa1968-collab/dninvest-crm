
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
  // mf_clients hit Firestore's 1,048,576-byte single-document hard limit
  // (1,183,867 bytes) on 20-Aug-2026, right after adding per-scheme AUM
  // breakdown (aum_schemes) — that extra data pushed a doc that was already
  // close to the ceiling over it. 8 shards mirrors eq_clients' safety margin:
  // ~918 clients today → ~115/shard → well under 200 KB/shard even as
  // aum_schemes/sip_details keep growing.
  mf_clients: 8,
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
  // Split a full array into per-shard arrays. Dedupes by id first — same id
  // always hashes to the same shard, so if the source array already had two
  // entries with the identical id (e.g. baked into the legacy pre-shard doc
  // from some earlier double-save), splitting it blindly would faithfully
  // carry that duplication into the shard doc, where it would then persist
  // forever (every subsequent read/merge would keep serving 2 copies of the
  // same client). Last one in the source array wins — for a genuine exact
  // duplicate the two copies are identical anyway, so which one survives
  // doesn't matter; for anything else this is a strict superset of what a
  // plain array-to-shards copy would have produced.
  _splitShards(key,arr){
    const n = SHARD_CFG[key];
    const parts = Array.from({length:n},()=>[]);
    const byId = {}; const order = [];
    (arr||[]).forEach(r=>{ if(!r) return; if(byId[r.id]===undefined) order.push(r.id); byId[r.id]=r; });
    order.forEach(id=>{ const r=byId[id]; parts[this._shardOf(key,r.id)].push(r); });
    return parts;
  },
  // Flatten the in-memory shard cache back into one array. Also dedupes by
  // id as a read-side safety net — see _splitShards above for why an id
  // could in principle repeat, and _readShards below for how an already-
  // corrupted shard doc gets silently cleaned up the next time it's written.
  _mergeShards(key){
    const parts = this._shardCache[key]||[];
    const flat = [].concat(...parts.map(p=>p||[]));
    const byId = {}; const order = [];
    flat.forEach(r=>{ if(!r) return; if(byId[r.id]===undefined) order.push(r.id); byId[r.id]=r; });
    return order.map(id=>byId[id]);
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
      batch.set(this._shardRef(key,i), {data:DB._clean(p), updated:new Date().toISOString(), shard:i, count:p.length});
    });
    await batch.commit();
    this._shardCache[key] = parts;
    return parts;
  },
  // One-time migration: if no shard doc exists yet but the legacy oversized
  // document does, copy it into shards. Deterministic + idempotent, so it's
  // safe even if two browsers do it at the same instant. The legacy doc is
  // intentionally LEFT IN PLACE as a backup (it is simply never read again).
  //
  // _migratePromise cache (added 20-Aug-2026): the OLD version of this check
  // — "does at least one shard doc already have data?" — had a dangerous
  // race. A single targeted write (_setClientSharded/_setBulkSharded/
  // deleteClient, all of which touch only ONE shard) could create/populate
  // just that one shard doc BEFORE the full legacy→shards migration ever
  // ran. The next _ensureMigrated call would then see "a shard has data" and
  // conclude migration was already done — permanently skipping the real
  // migration, so every OTHER client (everyone not in that one narrow write)
  // would silently vanish from every shard-backed read from then on. Fix:
  // every sharded write path below now awaits _ensureMigrated(key) FIRST,
  // and this function caches its in-flight/completed promise per key so the
  // real migration is guaranteed to run to completion before any targeted
  // shard write is allowed to happen, no matter which caller gets there
  // first or how many call it concurrently.
  _migratePromise: {},
  async _ensureMigrated(key){
    if(this._migratePromise[key]) return this._migratePromise[key];
    this._migratePromise[key] = (async ()=>{
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
    })();
    return this._migratePromise[key];
  },
  // Recursively strips `undefined` from any value before it's sent to
  // Firestore. Firestore's SDK REJECTS THE ENTIRE WRITE if ANY field
  // anywhere in the payload — however deeply nested, e.g. one missing
  // `folio` inside one entry of an `aum_schemes` array — is `undefined`.
  // localStorage silently tolerates undefined (JSON.stringify just drops
  // those keys), so this class of bug saves fine locally, looks fine in
  // the browser that made the change, and then the WHOLE collection
  // silently fails to sync to Firestore — which is exactly what makes it
  // so easy to introduce and so confusing to diagnose (works until refresh,
  // "hat jaata hai" on reload because the reload is reading Firestore's
  // last-successful — i.e. older — copy). Run every write payload through
  // this before calling .set()/tx.set()/batch.set(), no exceptions.
  // (Re-added 20-Aug-2026 — this function was present in an earlier session
  // but was missing from the file at the start of this session, most likely
  // lost in a deploy/rollback the same way the "Mark as Left" button was.)
  _clean(v){
    if(Array.isArray(v)) return v.map(x=> x===undefined ? null : this._clean(x));
    if(v && typeof v==='object' && !(v instanceof Date)){
      const out={};
      Object.keys(v).forEach(k=>{
        const cv=v[k];
        if(cv===undefined) return;   // drop the key entirely
        out[k]=this._clean(cv);
      });
      return out;
    }
    return v;
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
        fdb.collection('crm_data').doc(key).set({data:DB._clean(val),updated:new Date().toISOString()})
          .then(()=>console.log('Firebase synced:',key))
          .catch(e=>{ console.log('Firebase error:',e); toast('Sync error: '+e.message,'error'); });
      }
    }catch(e){}
  },
  // Set just the local copy without writing to Firebase
  setLocal(key,val){
    if(this._mem) this._mem[key] = val;   // keep cache in sync
    try{ localStorage.setItem('dninvest_'+key,JSON.stringify(val)); }catch(e){}
    // getCrmSchemeNames() (fund-name dropdown source) caches its scan of
    // every mf_clients[].sip_details[]/aum_schemes[] scheme name, since
    // scanning ~1000+ clients on every keystroke would be slow — but that
    // means a fresh AUM/SIP import's newly-added scheme names (e.g. found
    // 20-Aug-2026: "Abakkus Large and Mid Cap Fund" missing from the
    // dropdown right after import) silently didn't show up until a full
    // page reload recomputed the cache from scratch. Invalidating here
    // catches every mf_clients write path that goes through setLocal.
    if(key==='mf_clients' && typeof _crmSchemeNamesCache!=='undefined') _crmSchemeNamesCache=null;
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
        tx.set(docRef, {data:DB._clean(finalData), updated:new Date().toISOString()});
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
        tx.set(docRef, {data:DB._clean(finalData), updated:new Date().toISOString()});
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
        tx.set(docRef, {data:DB._clean(finalData), updated:new Date().toISOString()});
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
        tx.set(docRef, {data:DB._clean(finalData), updated:new Date().toISOString()});
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
        tx.set(docRef, {data:DB._clean(finalData), updated:new Date().toISOString()});
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
        tx.set(docRef, {data:DB._clean(finalData), updated:new Date().toISOString()});
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
        tx.set(docRef, {data:DB._clean(finalData), updated:new Date().toISOString()});
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
        tx.set(docRef, {data:DB._clean(finalData), updated:new Date().toISOString()});
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
    await this._ensureMigrated(key);   // guarantee full migration before a targeted write
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
        tx.set(ref, {data:DB._clean(latest), updated:new Date().toISOString(), shard:si, count:latest.length});
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
        tx.set(docRef, {data:DB._clean(latest), updated:new Date().toISOString()});
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
        tx.set(docRef, {data:DB._clean(latest), updated:new Date().toISOString()});
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
        tx.set(docRef, {data:DB._clean(latest), updated:new Date().toISOString()});
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
  // Generic version of mutateSeminar/mutateUsers above, for any OTHER simple
  // flat array stored as a single crm_data/<key> doc (no per-record id, not
  // sharded) — added 20-Aug-2026 for learned_fund_names, whose writes were
  // still using the old unsafe "read local copy, then blind DB.set(whole
  // array)" pattern. Two RMs saving transactions with different new fund-
  // name spellings close together would race: whichever save's write landed
  // second silently discarded the first RM's newly-learned name entirely
  // (classic lost-update — the exact bug class the 6/7-Aug fix addressed for
  // seminars/users/mf_business, this call site just wasn't converted at the
  // time). `mutate(arr)` gets a deep-cloned copy to modify in place; return
  // `false` to abort without writing (e.g. "nothing changed").
  async mutateArray(key, mutate){
    let localArr = this.get(key)||[];
    const clone = JSON.parse(JSON.stringify(localArr));
    if(mutate(clone)!==false) this.setLocal(key, clone);

    if(typeof fdb==='undefined') return {ok:false, error:'Offline — could not connect to Firebase'};
    const docRef = fdb.collection('crm_data').doc(key);
    let finalData=null, aborted=false;
    this._writing[key] = (this._writing[key]||0) + 1;
    try{
      await fdb.runTransaction(async (tx)=>{
        const doc = await tx.get(docRef);
        let latest = (doc.exists && doc.data() && doc.data().data) ? doc.data().data : (this.get(key)||[]);
        latest = JSON.parse(JSON.stringify(latest));
        const res = mutate(latest);
        if(res===false){ aborted=true; return; }
        tx.set(docRef, {data:DB._clean(latest), updated:new Date().toISOString()});
        finalData = latest;
      });
      if(finalData) this.setLocal(key, finalData);
      return {ok:true, aborted};
    }catch(e){
      console.log('mutateArray error ('+key+'):', e);
      toast('Sync error: '+e.message,'error');
      return {ok:false, error:e.message};
    } finally{ this._writing[key] = Math.max(0,(this._writing[key]||1)-1); }
  },
  // Delete a single client record (merge-on-write delete)
  async deleteClient(key, id){
    let arr = (this.get(key)||[]).filter(c=>c.id!==id);
    this.setLocal(key, arr);

    if(this._isSharded(key) && typeof fdb!=='undefined'){
      await this._ensureMigrated(key);   // guarantee full migration before a targeted write
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
          tx.set(ref, {data:DB._clean(latest), updated:new Date().toISOString(), shard:si, count:latest.length});
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
          tx.set(docRef, {data:DB._clean(latest), updated:new Date().toISOString()});
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
    await this._ensureMigrated(key);   // guarantee full migration before a targeted write
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
          tx.set(refs[k], {data:DB._clean(out), updated:new Date().toISOString(), shard:i, count:out.length});
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
      await this._ensureMigrated(key);   // guarantee full migration before a targeted write
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
            tx.set(refs[k], {data:DB._clean(out), updated:new Date().toISOString(), shard:i, count:out.length});
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
        tx.set(ref, {data:DB._clean(latest), updated:new Date().toISOString()});
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
          tx.set(docRef, {data:DB._clean(latest), updated:new Date().toISOString()});
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
            // Update the in-memory cache FIRST, unconditionally — this is what
            // DB.get() actually reads from within a session, so correctness
            // must never depend on the localStorage cache below succeeding.
            // (Root-caused 20-Aug-2026: eq_clients/mf_clients — especially
            // mf_clients once per-scheme aum_schemes/sip_details were added —
            // can be big enough that JSON.stringify(merged) blows past the
            // browser's ~5-10MB localStorage-per-origin quota. That threw an
            // uncaught QuotaExceededError right here, which — because _mem
            // was never touched in this branch at all — meant the correctly-
            // fetched, complete Firestore data was thrown away entirely, and
            // DB.get() kept serving whatever smaller/older snapshot was still
            // sitting in localStorage from before the quota was breached.
            // localStorage is now best-effort only: a fast warm-start cache,
            // never the source of truth for a key that's already synced from
            // Firestore this session.)
            if(!this._mem) this._mem = {};
            this._mem[key] = merged;
            if(key==='mf_clients' && typeof _crmSchemeNamesCache!=='undefined') _crmSchemeNamesCache=null;
            try{
              localStorage.setItem('dninvest_'+key, JSON.stringify(merged));
              console.log('Loaded from Firebase (sharded):',key, merged.length,'records',
                          this._shardCache[key].map(p=>p.length));
            }catch(e){
              console.log('localStorage cache skipped for',key,'(quota exceeded) — using in-memory only:',e);
            }
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
let eqPage=1, mfPage=1, leadsPage=1, mfpPage=1;
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
// Normalizes a Client ID for matching (RTA imports, MF client records).
// Strips whitespace, a trailing ".0"/".00" (SheetJS sometimes reads a
// numeric-looking Excel cell as a float), and leading zeros (Excel/SheetJS
// silently drops them from a General/Number-formatted cell, e.g. a source
// file with "0004708417" becomes the number 4708417 on read) — so an ID
// stored one way in an existing client record still matches the same ID
// read a different way from a fresh import file, instead of silently
// failing to match and creating a duplicate client.
function normCid(v){
  return String(v==null?'':v).trim().replace(/\.0+$/,'').replace(/^0+(?=\d)/,'');
}

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
      tx.set(docRef, {data:DB._clean(latest), updated:new Date().toISOString()});
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
      tx.set(docRef, {data:DB._clean(latest), updated:new Date().toISOString()});
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
            tx.set(docRef, {data:DB._clean(latest), updated:new Date().toISOString()});
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
              tx.set(docRef, {data:DB._clean(latest), updated:new Date().toISOString()});
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
          tx.set(docRef, {data:DB._clean(latest), updated:new Date().toISOString()});
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

// Opens the personal Asset & Net Worth Tracker in a new tab. This is a
// standalone, localStorage-only tool (no Firebase/login), so — unlike HR —
// there's no autologin token to generate; it just opens the file directly.
function openAssetTracker(){
  window.open('dninvest-assets.html', '_blank');
}



// Clear stale local data cache without logging out (24-Aug-2026 — mobile
// browser was showing old/stuck data; clearing the browser's cache fixed it,
// which meant a stale localStorage cache — NOT a Firestore sync problem —
// was the culprit. This button does the same fix as manually clearing the
// browser cache, in one tap: wipes every 'dninvest_*' cached-data key
// (client lists, snapshots, sort state, etc.) but deliberately KEEPS
// 'dninvest_session' so the person doesn't get logged out for this, then
// reloads so DB.syncFromFirebase() rebuilds every cache fresh from Firestore.
function clearCrmCache(){
  if(!confirm('Cache clear karke page reload hoga. Continue?')) return;
  Object.keys(localStorage).forEach(k=>{
    if(k.startsWith('dninvest_') && k!=='dninvest_session') localStorage.removeItem(k);
  });
  location.reload();
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
    if(typeof fdb!=='undefined') await fdb.collection('shared_control').doc('call_limits').set({data:DB._clean(d), updated:new Date().toISOString()});
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
            // 24-Aug-2026 fix: this listener updated localStorage but never
            // invalidated DB._mem['rm_messages'] — since DB.get() returns
            // straight from _mem when present (see DB.get above) without
            // re-reading localStorage, renderInbox()/checkRmReply() kept
            // rendering the OLD cached thread list even though the fresh
            // data had already landed in localStorage. Looked exactly like
            // "message only shows after a refresh" (which resets _mem from
            // scratch) — because that's exactly what was happening.
            if(!DB._mem) DB._mem={};
            DB._mem['rm_messages'] = newData;
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
                if(!DB._mem) DB._mem={};
                DB._mem[key]=merged;
                try{ localStorage.setItem('dninvest_'+key, JSON.stringify(merged)); }catch(e){ console.log('localStorage cache skipped for',key,'(quota exceeded):',e); }
                console.log('Real-time merge:', key);
                if(key==='call_logs' && getCurrentPageId()==='leads') renderLeadsTable();
                if(getCurrentPageId()==='activity-log') renderActivityLog();
                refreshDash(); updateBadges();
              }
              return;
            }
            if(JSON.stringify(newData) !== JSON.stringify(existing)){
              if(!DB._mem) DB._mem={};
              DB._mem[key]=newData;
              try{ localStorage.setItem('dninvest_'+key, JSON.stringify(newData)); }catch(e){ console.log('localStorage cache skipped for',key,'(quota exceeded):',e); }
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
        // Same DB._mem-invalidation bug as rm_messages (24-Aug-2026 fix) —
        // clearEqRiskCache() below only clears the SEPARATE _eqRiskCache;
        // getEqRisk() falls through to DB.get('eq_risk'), which was still
        // returning the stale DB._mem value since only localStorage was
        // being updated here.
        if(!DB._mem) DB._mem={};
        DB._mem['eq_risk'] = norm;
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

      // ── mf_clients/eq_clients/leads/seminars: BALANCED sync (21-Aug-2026,
      // replaces the "everyone always instant" version above). Realtime
      // onSnapshot listeners are only kept attached while THIS tab is the
      // visible/foreground tab (Page Visibility API) — a tab minimized or
      // sitting behind another window detaches them entirely, so it stops
      // generating billed Firestore reads for edits happening elsewhere.
      // A background tab instead falls back to a plain poll every 4 min.
      // The moment the tab is switched back to / brought to front, it
      // re-attaches realtime AND does one immediate catch-up poll, so
      // whatever happened while it was hidden shows up right away — not
      // after waiting out the rest of the 4-min window.
      // rm_messages/announcement/comm_history/force_logout/call_limits are
      // untouched below — small single docs and/or need to stay instant
      // (security kill-switch, RM↔Admin messaging) regardless of tab
      // visibility, so they keep their own onSnapshot as before.
      {
        const SHARD_KEYS = ['eq_clients','mf_clients','leads','seminars'].filter(k=>DB._isSharded(k));
        window._shardUnsubs = window._shardUnsubs || [];
        window._shardBgTimer = window._shardBgTimer || null;

        async function _pollShardedClientData(){
          for(const key of SHARD_KEYS){
            try{
              const parts = await DB._ensureMigrated(key);
              DB._shardCache[key] = parts.map(p=>p||[]);
              const merged = DB._mergeShards(key);
              let existingArr = null;
              try{ existingArr = JSON.parse(localStorage.getItem('dninvest_'+key)||'[]'); }catch(e){}
              if(Array.isArray(existingArr) && existingArr.length===merged.length){
                const ex=existingArr;
                if(ex[0]&&merged[0]&&ex[0].id===merged[0].id&&ex[ex.length-1]&&merged[merged.length-1]&&ex[ex.length-1].id===merged[merged.length-1].id) continue;
              }
              if(!DB._mem) DB._mem = {};
              DB._mem[key] = merged;
              if(key==='mf_clients' && typeof _crmSchemeNamesCache!=='undefined') _crmSchemeNamesCache=null;
              try{ localStorage.setItem('dninvest_'+key, JSON.stringify(merged)); }
              catch(e){ console.log('localStorage cache skipped for',key,'(quota exceeded) — using in-memory only:',e); }
              _afterRealtime(key);
            }catch(e){ console.log('poll failed for',key,e); }
          }
        }

        function _attachShardedRealtime(){
          if(window._shardUnsubs.length) return;   // already attached
          SHARD_KEYS.forEach(key=>{
            const n = SHARD_CFG[key];
            if(!DB._shardCache[key]) DB._shardCache[key] = Array.from({length:n},()=>[]);
            if(!DB._shardSeen[key])  DB._shardSeen[key]  = new Set();
            for(let i=0;i<n;i++){
              const unsub = DB._shardRef(key,i).onSnapshot(doc=>{
                if(doc.metadata.hasPendingWrites) return;
                if(DB._writing[key]>0) return;
                if(!(doc.exists && doc.data() && Array.isArray(doc.data().data))) return;
                DB._shardCache[key][i] = doc.data().data;
                DB._shardSeen[key].add(i);
                if(DB._shardSeen[key].size < n) return;   // wait for every shard to report in once
                const merged = DB._mergeShards(key);
                let existingArr = null;
                try{ existingArr = JSON.parse(localStorage.getItem('dninvest_'+key)||'[]'); }catch(e){}
                if(Array.isArray(existingArr) && existingArr.length===merged.length){
                  const ex=existingArr;
                  if(ex[0]&&merged[0]&&ex[0].id===merged[0].id&&ex[ex.length-1]&&merged[merged.length-1]&&ex[ex.length-1].id===merged[merged.length-1].id) return;
                }
                if(!DB._mem) DB._mem = {};
                DB._mem[key] = merged;
                if(key==='mf_clients' && typeof _crmSchemeNamesCache!=='undefined') _crmSchemeNamesCache=null;
                try{ localStorage.setItem('dninvest_'+key, JSON.stringify(merged)); }
                catch(e){ console.log('localStorage cache skipped for',key,'(quota exceeded) — using in-memory only:',e); }
                _afterRealtime(key);
              });
              window._shardUnsubs.push(unsub);
            }
          });
        }

        function _detachShardedRealtime(){
          window._shardUnsubs.forEach(u=>{ try{ u(); }catch(e){} });
          window._shardUnsubs = [];
          SHARD_KEYS.forEach(key=>{ DB._shardSeen[key] = new Set(); });   // re-arm the "wait for all shards" gate for next attach
        }

        function _applyVisibilityMode(){
          if(document.visibilityState==='visible'){
            if(window._shardBgTimer){ clearInterval(window._shardBgTimer); window._shardBgTimer=null; }
            _pollShardedClientData();   // catch up immediately on whatever happened while hidden
            _attachShardedRealtime();
          } else {
            _detachShardedRealtime();
            if(!window._shardBgTimer) window._shardBgTimer = setInterval(_pollShardedClientData, 4*60000); // every 4 min while hidden
          }
        }

        document.addEventListener('visibilitychange', _applyVisibilityMode);
        _applyVisibilityMode();   // set initial mode for this load
      }

      ['eq_clients','mf_clients','leads','seminars'].forEach(key=>{
        if(DB._isSharded(key)) return;   // handled above (balanced realtime/poll) — only leads/seminars land here, they aren't sharded
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
  // SIP Tracker — removed from the sidebar entirely (20-Aug-2026, user
  // request: "sip tracker ki jarurat nahi hai pura hata de left menu se
  // bhi"). Forced hidden unconditionally, overriding the hasMf-based show
  // logic above — the underlying nav link and page markup still live in
  // index.html (not touched this session, only app-1.js was uploaded), so
  // if a full code-level removal from index.html is wanted later, that file
  // needs to be shared too. For now this makes the item disappear for every
  // role, with no route to reach the page.
  { const sipNav=document.getElementById('nav-mf-sip'); if(sipNav) sipNav.style.display='none'; }
  // MF Prospects: admin always sees it; a regular RM only sees it if explicitly
  // granted access (mf_prospects_access) — Admin decides who gets this, same
  // pattern as risk_upload/backoffice_access/mf_desk_access.
  const mfpNav=document.getElementById('nav-mf-prospects');
  if(mfpNav) mfpNav.style.display=(CU.role==='admin' || (hasMf && CU.mf_prospects_access===true))?'flex':'none';
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

  // Asset & Net Worth Tracker shortcut button — admin only
  const assetBtn = document.getElementById('assetTrackerTopBtn');
  if(assetBtn) assetBtn.style.display = (CU.role==='admin') ? '' : 'none';

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
  else if(id==='mf-prospects'){ mfpPage=1; renderMfProspects(); }
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
// Redesigned 22-Aug-2026 to match the D N Investment brand (logo_new.png) —
// black + gold theme, actual hexagon "DN" emblem drawn onto the card canvas
// (cropped from the logo, background matted to transparent so it composites
// cleanly onto the card's own black gradient) instead of the old plain-text
// brand line. Embedded as a base64 PNG below so the card generator has zero
// external asset dependency (same single-file approach as the rest of the
// app) — loaded once and cached in _dnEmblemImgCache for every card after.
const _DN_EMBLEM_B64 = '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';
let _dnEmblemImgCache = null;
function _getDnEmblemImg(cb){
  if(_dnEmblemImgCache && _dnEmblemImgCache.complete){ cb(_dnEmblemImgCache); return; }
  const img = new Image();
  img.onload = ()=>{ _dnEmblemImgCache = img; cb(img); };
  img.onerror = ()=>{ cb(null); };   // draw the card without the emblem rather than not at all
  img.src = 'data:image/png;base64,'+_DN_EMBLEM_B64;
}
function birthdayCardImage(idx){
  const rec=_bdayCards[idx]||{};
  const name=(rec.name||'Friend').trim();
  const num=rec.num||'';
  const first=name.split(' ')[0]||name;
  _getDnEmblemImg(function(emblemImg){ _drawBirthdayCard(emblemImg, name, num, first); });
}
function _drawBirthdayCard(emblemImg, name, num, first){
  const W=1080,H=1080;
  const cv=document.createElement('canvas'); cv.width=W; cv.height=H; const g=cv.getContext('2d');
  const GOLD='#c9942a', GOLD_L='#f5d98a', GOLD_PALE='#fdf6e3', CREAM='#f3e9d2', INK='#c9c2b0';
  function rr(x,y,w,h,r){g.beginPath();g.moveTo(x+r,y);g.arcTo(x+w,y,x+w,y+h,r);g.arcTo(x+w,y+h,x,y+h,r);g.arcTo(x,y+h,x,y,r);g.arcTo(x,y,x+w,y,r);g.closePath();}
  // black gradient background (matched to the logo's own black, not flat)
  const bg=g.createLinearGradient(0,0,W,H);
  bg.addColorStop(0,'#161410'); bg.addColorStop(0.5,'#0b0a09'); bg.addColorStop(1,'#1a160e');
  g.fillStyle=bg; g.fillRect(0,0,W,H);
  // soft gold glow behind the centre
  const glow=g.createRadialGradient(W/2,H*0.42,60,W/2,H*0.42,H*0.72);
  glow.addColorStop(0,'rgba(201,148,42,.14)'); glow.addColorStop(1,'rgba(201,148,42,0)');
  g.fillStyle=glow; g.fillRect(0,0,W,H);
  // vector balloons, gold/cream tones
  function balloon(x,y,rw,rh,c1,c2,tilt){
    g.save(); g.translate(x,y); g.rotate(tilt);
    const q=g.createLinearGradient(-rw,-rh,rw,rh); q.addColorStop(0,c1); q.addColorStop(1,c2);
    g.fillStyle=q; g.beginPath(); g.ellipse(0,0,rw,rh,0,0,7); g.fill();
    g.fillStyle='rgba(255,255,255,.22)'; g.beginPath(); g.ellipse(-rw*.32,-rh*.36,rw*.20,rh*.28,-0.5,0,7); g.fill();
    g.fillStyle=c2; g.beginPath(); g.moveTo(-9,rh); g.lineTo(9,rh); g.lineTo(0,rh+16); g.closePath(); g.fill();
    g.strokeStyle='rgba(245,217,138,.35)'; g.lineWidth=2; g.beginPath(); g.moveTo(0,rh+16);
    g.bezierCurveTo(26,rh+90,-26,rh+150,6,rh+215); g.stroke();
    g.restore();
  }
  balloon(150,220,50,62,GOLD_L,GOLD,-0.16);
  balloon(254,168,42,53,CREAM,GOLD_L,0.12);
  balloon(930,220,50,62,CREAM,GOLD_L,0.16);
  balloon(826,168,42,53,GOLD_L,GOLD,-0.12);
  // gold/cream confetti, kept clear of the centre content column
  function conf(x,y,w,h,c,rot){ g.save(); g.translate(x,y); g.rotate(rot); g.fillStyle=c; g.fillRect(-w/2,-h/2,w,h); g.restore(); }
  const cc=[GOLD,GOLD_L,CREAM,'rgba(255,255,255,.7)'];
  let _seed=11; const rnd=()=>{ _seed=(_seed*16807)%2147483647; return _seed/2147483647; };
  for(let i=0;i<44;i++){ const x=rnd()*W, y=rnd()*H;
    if(x>150&&x<930&&y>90&&y<960) continue;
    g.globalAlpha=.4+rnd()*.35; conf(x,y,7+rnd()*7,3+rnd()*4,cc[i%4],rnd()*3.14); }
  g.globalAlpha=1;
  // double gold frame + corner dots, echoing the emblem's own hexagon border
  g.strokeStyle=GOLD; g.lineWidth=7; rr(40,40,W-80,H-80,40); g.stroke();
  g.strokeStyle='rgba(245,217,138,.55)'; g.lineWidth=1.5; rr(58,58,W-116,H-116,32); g.stroke();
  g.fillStyle=GOLD;
  [[58,58,1,1],[W-58,58,-1,1],[58,H-58,1,-1],[W-58,H-58,-1,-1]].forEach(c=>{
    g.save(); g.translate(c[0],c[1]); g.scale(c[2],c[3]);
    g.beginPath(); g.arc(30,30,5,0,7); g.fill(); g.restore(); });
  // ── D N Investment hexagon emblem, drawn from the actual logo ──
  let emblemBottom = 150;
  if(emblemImg){
    const embW=190, embH=embW*emblemImg.height/emblemImg.width;
    g.drawImage(emblemImg, W/2-embW/2, 118, embW, embH);
    emblemBottom = 118+embH;
  }
  // HAPPY BIRTHDAY, letter-spaced, gold
  const hbY = emblemBottom+52;
  g.textAlign='center';
  g.fillStyle=GOLD_L; g.font='500 27px Georgia, serif';
  (function(t,y){ const sp=8; let tot=0,i;
    for(i=0;i<t.length;i++) tot+=g.measureText(t[i]).width+sp; tot-=sp;
    let x=W/2-tot/2; g.textAlign='left';
    for(i=0;i<t.length;i++){ g.fillText(t[i],x,y); x+=g.measureText(t[i]).width+sp; }
    g.textAlign='center'; })('HAPPY BIRTHDAY',hbY);
  g.strokeStyle='rgba(201,148,42,.7)'; g.lineWidth=2;
  g.beginPath(); g.moveTo(W/2-272,hbY-10); g.lineTo(W/2-212,hbY-10); g.moveTo(W/2+212,hbY-10); g.lineTo(W/2+272,hbY-10); g.stroke();
  g.fillStyle=GOLD; [W/2-200,W/2+200].forEach(x=>{ g.beginPath(); g.arc(x,hbY-10,4,0,7); g.fill(); });
  // NAME — cream, auto-fit (shrink, then wrap to 2 lines)
  g.fillStyle=CREAM;
  const nm=name.toUpperCase();
  const maxNameW=W-300;
  const nameY = hbY+114;
  let fs=76; g.font='500 '+fs+'px Georgia, serif';
  if(g.measureText(nm).width<=maxNameW){
    g.fillText(nm, W/2, nameY);
  } else {
    while(g.measureText(nm).width>maxNameW && fs>52){ fs-=2; g.font='500 '+fs+'px Georgia, serif'; }
    if(g.measureText(nm).width<=maxNameW){
      g.fillText(nm, W/2, nameY);
    } else {
      const words=nm.split(' '); let l1='',l2='';
      for(const w of words){ const t=(l1?l1+' ':'')+w;
        if(g.measureText(t).width<=maxNameW && !l2) l1=t; else l2=(l2?l2+' ':'')+w; }
      if(!l2){ l2=l1; l1=''; }
      fs=56; g.font='500 '+fs+'px Georgia, serif';
      while((g.measureText(l1).width>maxNameW||g.measureText(l2).width>maxNameW)&&fs>38){ fs-=2; g.font='500 '+fs+'px Georgia, serif'; }
      if(l1){ g.fillText(l1, W/2, nameY-26); g.fillText(l2, W/2, nameY-26+fs+6); }
      else  { g.fillText(l2, W/2, nameY); }
    }
  }
  // divider with gold diamond
  const divY = nameY+64;
  g.strokeStyle=GOLD; g.lineWidth=2.5;
  g.beginPath(); g.moveTo(W/2-110,divY); g.lineTo(W/2-24,divY); g.moveTo(W/2+24,divY); g.lineTo(W/2+110,divY); g.stroke();
  g.save(); g.translate(W/2,divY); g.rotate(Math.PI/4); g.fillStyle=GOLD; g.fillRect(-7,-7,14,14); g.restore();
  // message
  g.fillStyle=INK; g.font='32px Georgia, serif';
  g.fillText('Dear '+first+', wishing you a day filled with joy', W/2, divY+64);
  g.fillText('and a year of health, happiness', W/2, divY+108);
  g.fillText('and prosperity.', W/2, divY+152);
  // brand line — logo already carries the mark, so this stays small & simple
  const brandY = divY+192;
  g.strokeStyle='rgba(245,217,138,.25)'; g.lineWidth=1;
  g.beginPath(); g.moveTo(W/2-150,brandY-24); g.lineTo(W/2+150,brandY-24); g.stroke();
  g.fillStyle=GOLD_L; g.font='500 34px Georgia, serif'; g.fillText('D N INVESTMENT', W/2, brandY+18);
  g.fillStyle='#a89a78'; g.font='20px Georgia, serif'; g.fillText('Jamshedpur  ·  Trusted Wealth Partner', W/2, brandY+48);
  // bottom gold ribbon, black text
  g.fillStyle=GOLD; rr(W/2-320,940,640,58,29); g.fill();
  g.fillStyle='#161410'; g.font='500 26px Georgia, serif';
  g.fillText('Many Many Happy Returns of the Day', W/2, 978);

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

  // No-call alerts (Equity) — "never called" only, same definition as renderNoCall()
  const noCallEq = activeEq.filter(c=>daysDiff(c.last_call_date)===null)
    .map(c=>({...c,days:daysDiff(c.created)}))
    .sort((a,b)=>{const av=a.days===null?Infinity:a.days, bv=b.days===null?Infinity:b.days; return bv-av;})
    .slice(0,8);
  document.getElementById('noCallEqAlert').innerHTML = noCallEq.length
    ? noCallEq.map(c=>{
        const cls=c.days===null||c.days>=180?'r180':c.days>=90?'r90':'r60';
        const label=c.days===null?'Never called':c.days+'d never-called';
        return `<div class="alert-row ${cls}"><span>${c.name}</span><span>${label}</span></div>`;
      }).join('')
    : '<p style="color:var(--green);font-size:.82rem;padding:8px 0">✅ No alerts</p>';

  // No-call alerts (MF) — "never called" only, same definition as renderNoCall()
  const noCallMf = mf.filter(c=>daysDiff(c.last_call_date)===null)
    .map(c=>({...c,days:daysDiff(c.created)}))
    .sort((a,b)=>{const av=a.days===null?Infinity:a.days, bv=b.days===null?Infinity:b.days; return bv-av;})
    .slice(0,8);
  document.getElementById('noCallMfAlert').innerHTML = noCallMf.length
    ? noCallMf.map(c=>{
        const cls=c.days===null||c.days>=180?'r180':c.days>=90?'r90':'r60';
        const label=c.days===null?'Never called':c.days+'d never-called';
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
  // "Never called" only — mirrors renderNoCall()'s definition, not last_call_date-based
  document.getElementById('nb-eqnc').textContent=activeEq.filter(c=>{
    if(daysDiff(c.last_call_date)!==null) return false;
    const sinceAdded=daysDiff(c.created); return sinceAdded===null||sinceAdded>=60;
  }).length;
  document.getElementById('nb-mfnc').textContent=mf.filter(c=>{
    if(daysDiff(c.last_call_date)!==null) return false;
    const sinceAdded=daysDiff(c.created); return sinceAdded===null||sinceAdded>=60;
  }).length;
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
// Debounced wrapper for the search box only (21-Aug-2026, "fast redraw" fix)
// — oninput fired filterEq() on every single keystroke, and each call does a
// full filter+sort+innerHTML rebuild over the whole client list, which was
// visibly janky for a fast typer. onchange callers (status/RM/date filters,
// which fire rarely) and internal programmatic callers still call filterEq()
// directly for an immediate result — only the search input's oninput was
// switched (in index.html) to call this instead, so typing waits 200ms after
// the last keystroke before actually re-rendering.
let _filterEqT;
function filterEqDebounced(){ clearTimeout(_filterEqT); _filterEqT=setTimeout(filterEq,200); }
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
  ['search','status','rm','comeback','followup-filter','badge','last-call-from','last-call-to','next-call-from','next-call-to','last-trade-from','last-trade-to','last-biz-from','last-biz-to'].forEach(f=>{
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
  // Quick lookup so we can flag equity clients who are ALSO an MF investor.
  // PAN is the reliable match (unique, doesn't change) — mobile is only a
  // fallback for records where PAN is missing on one side, since some
  // clients have a different mobile number saved against their MF account
  // vs their Equity account (updated one side but not the other).
  // Also build a PAN/mobile → MF RM lookup so the "M" badge tooltip can show
  // WHICH RM handles that client's MF account, not just "yes, they're an MF
  // investor" — genuinely useful for an Equity RM wondering who to loop in.
  const _mfClientsForMatch = getMyMfClients()||[];
  const _mfPanSet = new Set(_mfClientsForMatch.map(c=>String(c.pan||'').trim().toUpperCase()).filter(Boolean));
  const _mfMobileSet = new Set(_mfClientsForMatch.map(c=>String(c.mobile||'').trim()).filter(Boolean));
  const _mfRmByPan = {}, _mfRmByMobile = {};
  _mfClientsForMatch.forEach(c=>{
    const pan=String(c.pan||'').trim().toUpperCase(), mob=String(c.mobile||'').trim();
    if(pan && !_mfRmByPan[pan]) _mfRmByPan[pan]=c.rm||'—';
    if(mob && !_mfRmByMobile[mob]) _mfRmByMobile[mob]=c.rm||'—';
  });
  const _mfRmFor = c => _mfRmByPan[String(c.pan||'').trim().toUpperCase()] || _mfRmByMobile[String(c.mobile||'').trim()] || '—';
  // Parse the RMS risk map ONCE per render (was being re-parsed per comparison
  // during sort → tens of thousands of JSON.parse of a 2700-entry object → hang).
  const _riskMap = (getEqRisk().code) || {};
  const _riskOf = code => (code ? _riskMap[String(code).trim()] : null) || null;
  const q=(document.getElementById('eq-search')||{value:''}).value.toLowerCase();
  const st=(document.getElementById('eq-status')||{value:''}).value;
  const rm=(document.getElementById('eq-rm')||{value:''}).value;
  const comebackFilter=(document.getElementById('eq-comeback')||{value:''}).value;
  const fu=(document.getElementById('eq-followup-filter')||{value:''}).value;
  const badgeFilter=(document.getElementById('eq-badge')||{value:''}).value;
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
  if(badgeFilter){
    data=data.filter(c=>{
      const isM = _mfPanSet.has(String(c.pan||'').trim().toUpperCase()) || _mfMobileSet.has(String(c.mobile||'').trim());
      const isH = (c.asset_value>=500000);
      if(badgeFilter==='M') return isM;
      if(badgeFilter==='H') return isH;
      if(badgeFilter==='MH') return isM && isH;
      if(badgeFilter==='NONE') return !isM; // "Not MF Client" — ignore HNI status
      return true;
    });
  }
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
      <td><span style="display:inline-block;min-width:60px">${c.code||'—'}</span>${(_mfPanSet.has(String(c.pan||'').trim().toUpperCase())||_mfMobileSet.has(String(c.mobile||'').trim()))?`<span class="badge-tip" data-tip="MF RM: ${escapeHtml(_mfRmFor(c))}" style="font-size:.62rem;background:#0d9488;color:#fff;border-radius:4px;padding:0 4px;font-weight:700;vertical-align:middle;cursor:help">M</span>`:''}</td>
      <td style="font-weight:600;cursor:context-menu" oncontextmenu="showClientSeminarMenu(event,'${c.id}','equity')" title="Right-click → Add to Seminar">${c.name}${(c.asset_value>=500000)?'<span class="badge-tip" data-tip="HNI — Asset Value ≥ ₹5L" style="margin-left:4px;font-size:.65rem;background:#7c3aed;color:#fff;border-radius:4px;padding:0 4px;font-weight:700;vertical-align:middle;cursor:help">H</span>':''}</td>
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
let _filterMfT;
function filterMfDebounced(){ clearTimeout(_filterMfT); _filterMfT=setTimeout(filterMf,200); }
function sortMfTable(colIndex){
  if(mfSortField===colIndex) mfSortDir = -mfSortDir;
  else { mfSortField=colIndex; mfSortDir=1; }
  mfPage=1;
  _saveSortState();
  renderMfTable();
}

function changePageSize(tab, val){
  PG_SIZE = parseInt(val)||50;
  const eqSel=document.getElementById('eq-pagesize'), mfSel=document.getElementById('mf-pagesize'), mfpSel=document.getElementById('mfp-pagesize');
  if(eqSel) eqSel.value=val;
  if(mfSel) mfSel.value=val;
  if(mfpSel) mfpSel.value=val;
  if(tab==='eq'){ eqPage=1; renderEqTable(); }
  else if(tab==='mfp'){ mfpPage=1; renderMfProspects(); }
  else { mfPage=1; renderMfTable(); }
}

function renderMfTable(){
  if(mfSortField===null){
    try{ const _ss=JSON.parse(localStorage.getItem('dninvest_sort_state')||'{}');
      if(_ss.mfField!==undefined && _ss.mfField!==null){ mfSortField=_ss.mfField; mfSortDir=_ss.mfDir||1; } }catch(e){}
  }
  let data=getMyMfClients();
  // Mirror of the Equity page's "M" badge: flag MF investors who are ALSO an
  // equity client, matched by PAN (reliable) with mobile as fallback. Using
  // "E" here (not "M") since "M" already means "also an MF investor" on the
  // Equity page — reusing it here would be confusing on a page that's
  // already all-MF.
  // NOTE: match against ALL equity clients (DB.get), not getMyEqClients().
  // getMyEqClients() is scoped to the logged-in RM's own equity book, but the
  // whole point of this badge is to flag a cross-sell / already-has-equity
  // case even when that equity account sits with a DIFFERENT RM (that's what
  // the "Equity RM: ..." tooltip is for). Scoping it to "my" clients made the
  // badge invisible to every non-admin RM whenever the match was on someone
  // else's book — admin only saw it because getMyEqClients() happens to
  // return everyone for role==='admin'.
  const _eqClientsForMatch = DB.get('eq_clients')||[];
  const _eqPanSet = new Set(_eqClientsForMatch.map(c=>String(c.pan||'').trim().toUpperCase()).filter(Boolean));
  const _eqMobileSet = new Set(_eqClientsForMatch.map(c=>String(c.mobile||'').trim()).filter(Boolean));
  const _eqRmByPan = {}, _eqRmByMobile = {};
  _eqClientsForMatch.forEach(c=>{
    const pan=String(c.pan||'').trim().toUpperCase(), mob=String(c.mobile||'').trim();
    if(pan && !_eqRmByPan[pan]) _eqRmByPan[pan]=c.rm||'—';
    if(mob && !_eqRmByMobile[mob]) _eqRmByMobile[mob]=c.rm||'—';
  });
  const _eqRmFor = c => _eqRmByPan[String(c.pan||'').trim().toUpperCase()] || _eqRmByMobile[String(c.mobile||'').trim()] || '—';
  const q=(document.getElementById('mf-search')||{value:''}).value.toLowerCase();
  const st=(document.getElementById('mf-status')||{value:''}).value;
  const rm=(document.getElementById('mf-rm')||{value:''}).value;
  const fu=(document.getElementById('mf-followup-filter')||{value:''}).value;
  const badgeFilterMf=(document.getElementById('mf-badge')||{value:''}).value;
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
  if(badgeFilterMf){
    data=data.filter(c=>{
      const isE = _eqPanSet.has(String(c.pan||'').trim().toUpperCase()) || _eqMobileSet.has(String(c.mobile||'').trim());
      const isH = (c.aum>=300000);
      if(badgeFilterMf==='E') return isE;
      if(badgeFilterMf==='H') return isH;
      if(badgeFilterMf==='EH') return isE && isH;
      if(badgeFilterMf==='NONE') return !isE; // "Not Equity Client" — ignore HNI status
      return true;
    });
  }
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
      <td style="font-weight:600;cursor:context-menu" oncontextmenu="showClientSeminarMenu(event,'${c.id}','mf')" title="Right-click → Add to Seminar"><span style="display:inline-block;width:20px;text-align:left">${(_eqPanSet.has(String(c.pan||'').trim().toUpperCase())||_eqMobileSet.has(String(c.mobile||'').trim()))?`<span class="badge-tip" data-tip="Equity RM: ${escapeHtml(_eqRmFor(c))}" style="font-size:.65rem;background:#2563eb;color:#fff;border-radius:4px;padding:0 4px;font-weight:700;vertical-align:middle;cursor:help">E</span>`:''}</span>${c.name}${(c.aum>=300000)?'<span class="badge-tip" data-tip="HNI — AUM ≥ ₹3L" style="margin-left:4px;font-size:.65rem;background:#0d9488;color:#fff;border-radius:4px;padding:0 4px;font-weight:700;vertical-align:middle;cursor:help">H</span>':''}</td>
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
  if(!(await dangerConfirm(`Delete lead "${name}"? This cannot be undone.`))) return;
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
  if(!(await dangerConfirm(`Delete seminar "${name}"? This cannot be undone.`))) return;
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
  if(!(await dangerConfirm('Permanently delete feedback response from "'+(r.name||'')+'"?'))) return;
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
  if(!(await dangerConfirm(`Delete "${name}"? This cannot be undone.`))) return;
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
// A light note-taking check on an entry, without approve/decline authority
// (that's Admin-only).
//  - MF-Desk-capable user (pure MF Desk role, or an RM granted MF Desk
//    access) can see every RM's entries (see getFilteredMfTxns) — so they get
//    the same blanket ability as Admin to leave a remark on ANY entry,
//    including their own data entry. (Previously self-entered rows were
//    excluded "no point cross-checking your own entry", but that produced an
//    inconsistent, hard-to-explain pattern — RMK showing on some rows and not
//    others depending on who happened to key it in. Treating MF Desk like
//    Admin here removes that inconsistency.)
//  - A plain RM (no desk access) can only remark on entries logged by
//    someone else on their behalf (e.g. MF Desk backfilled it for them) — a
//    chance to confirm/dispute it. They can't remark on their own self-logged
//    entries, and can't see/remark on other RMs' entries at all.
// Admin can always remark on anything.
function canAddCrossRemark(e){
  if(!CU) return false;
  if(CU.role==='admin') return true;
  if(hasMfDeskAccess(CU)) return true;
  const selfEntered = (e.rm||'').trim().toLowerCase() === (e.created_by||'').trim().toLowerCase();
  if(selfEntered) return false;
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
  // — Bulk-imported from AUM By Scheme report (19-Aug-2026) — 414 additional real scheme names, deduped against the hand-curated list above
  "Nippon India Small Cap Fund - Growth","Tata Business Cycle Fund - Regular Plan - Growth",
  "ICICI Prudential India Opportunities Fund - Growth","HDFC Mid Cap Fund - Regular Plan - Growth",
  "ICICI Prudential Innovation Fund - Regular Plan - Growth","Motilal Oswal Large and Mid Cap Fund - Regular Plan - Growth",
  "ICICI Prudential Dynamic Asset Allocation Active FOF - Growth","Aditya Birla Sun Life Value Fund - Regular Plan - Growth",
  "HSBC Value Fund - Regular Plan - Growth","SBI Large Cap Fund - Regular Plan - Growth",
  "PGIM India Mid Cap Fund - Regular Plan - Growth","Nippon India Large Cap Fund - Growth",
  "WhiteOak Capital Special Opportunities Fund - Regular Plan - Growth","Nippon India Growth Mid Cap Fund - Growth",
  "HDFC Balanced Advantage Fund - Regular Plan - IDCW","WhiteOak Capital Multi Asset Allocation Fund - Regular Plan - Growth",
  "ICICI Prudential Balanced Advantage Fund - Regular - Growth","Motilal Oswal Multi Cap Fund - Regular Plan - Growth",
  "WhiteOak Capital Multi Cap Fund - Regular Plan - Growth","ICICI Prudential Flexicap Fund - Growth",
  "Axis Large and Mid Cap Fund - Regular Plan - Growth","Nippon India ELSS Tax Saver Fund - Growth",
  "Axis ELSS Tax Saver Fund - Regular Plan - Growth","Canara Robeco Large Cap Fund - Regular Plan - Growth",
  "DSP Mid Cap Fund - Regular Plan - Growth","HSBC Mid Cap Fund - Regular Plan - Growth",
  "HDFC Defence Fund - Regular Plan - Growth","WhiteOak Capital Large and Mid Cap Fund - Regular Plan - Growth",
  "Tata Mid Cap Fund - Regular Plan - Growth","DSP ELSS Tax Saver Fund - Regular Plan - Growth",
  "Edelweiss Small Cap Fund - Regular Plan - Growth","Nippon India Retirement Fund - Wealth Creation Scheme - Growth",
  "Tata Multicap Fund - Regular Plan - Growth","ICICI Prudential Large Cap Fund - Regular Plan - Growth",
  "Edelweiss Large & Mid Cap Fund - Regular Plan - Growth","Tata Balanced Advantage Fund - Regular Plan - Growth",
  "Nippon India Innovation Fund - Regular Plan - Growth","Nippon India Balanced Advantage Fund - Growth",
  "SBI Large & Midcap Fund - Regular Plan - Growth","ITI Balanced Advantage Fund - Regular Plan - Growth",
  "ITI Small Cap Fund - Regular Plan - Growth","Aditya Birla Sun Life ELSS Tax Saver Fund - Regular Plan - Growth",
  "Kotak Multi Asset Omni FOF - Regular Plan - Growth","HDFC Value Fund - Regular Plan - Growth",
  "Tata Large and Mid Cap Fund - Regular Plan - Growth","Motilal Oswal Small Cap Fund - Regular Plan - Growth",
  "Axis Focused Fund - Regular Plan - Growth","HDFC Childrens Fund (Lock-in) - Regular Plan - Growth",
  "ICICI Prudential Technology Fund - Growth","Abakkus Small Cap Fund - Regular Plan - Growth",
  "Edelweiss US Technology Equity Fund of Fund - Regular Plan - Growth","Kotak Flexi Cap Fund - Regular Plan - Growth",
  "ICICI Prudential Multi-Asset Fund - Growth","Mirae Asset Large and Mid Cap Fund - Regular Plan - Growth",
  "Nippon India Multi Asset Allocation Fund - Regular Plan - Growth","Bajaj Finserv Flexi Cap Fund - Regular Plan - Growth",
  "DSP Multi Asset Allocation Fund - Regular Plan - Growth","HSBC Small Cap Fund - Regular Plan - Growth",
  "Canara Robeco ELSS Tax Saver - Regular Plan - Growth","Edelweiss Multi Asset Omni Fund of Fund - Regular Plan - Growth",
  "DSP Large and Mid Cap Fund - Regular Plan - Growth","DSP Healthcare Fund - Regular Plan - Growth",
  "Tata Banking and Financial Services Fund - Regular Plan - Growth","Edelweiss Aggressive Hybrid Fund - Regular Plan - Growth",
  "Canara Robeco Large and Mid Cap Fund - Regular Plan - Growth","SBI Gold Fund - Regular Plan - Growth",
  "Nippon India Gold Savings Fund - Growth","Union Small Cap Fund - Regular Plan - Growth",
  "Kotak Mid Cap Fund - Regular Plan - Growth","Union Multicap Fund - Regular Plan - Growth",
  "ICICI Prudential Business Cycle Fund - Growth","Aditya Birla Sun Life Digital India Fund - Regular Plan - Growth",
  "Motilal Oswal Mid Cap Fund - Regular Plan - Growth","HDFC Manufacturing Fund - Regular Plan - Growth",
  "Bandhan Small Cap Fund - Regular Plan - Growth","Tata India Innovation Fund - Regular Plan - Growth",
  "Nippon India Pharma Fund - Growth","Nippon India Flexi Cap Fund - Regular Plan - Growth",
  "HDFC ELSS Tax Saver - Regular Plan - Growth","ICICI Prudential Smallcap Fund - Growth",
  "Nippon India Focused Fund - Growth","Nippon India Ultra Short Duration Fund - Growth",
  "Kotak Multicap Fund - Regular Plan - Growth","Nippon India Aggressive Hybrid Fund - Growth",
  "Kotak Multi Asset Allocation Fund - Regular Plan - Growth","UTI Value Fund - Regular Plan - Growth",
  "SBI Innovative Opportunities Fund - Regular Plan - Growth","Nippon India Retirement Fund - Income Generation Scheme - Growth",
  "ITI Multi Cap Fund - Regular Plan - Growth","Kotak Business Cycle Fund - Regular Plan - Growth",
  "Nippon India Multi Cap Fund - Growth","Tata Housing Opportunities Fund - Regular Plan - Growth",
  "ICICI Prudential Manufacturing Fund - Growth","Edelweiss Large Cap Fund - Regular Plan - Growth",
  "Axis Multi Asset Allocation Fund - Regular Plan - Growth","Tata Focused Fund - Regular Plan - Growth",
  "Quant Mid Cap Fund - Growth","Helios Flexi Cap Fund - Regular Plan - Growth",
  "Tata Multi Asset Allocation Fund - Regular Plan - Growth","SBI Mid Cap Fund - Regular Plan - Growth",
  "ICICI Prudential PSU Equity Fund - Regular Plan - Growth","UTI Multi Cap Fund - Regular Plan - Growth",
  "Nippon India Silver ETF Fund of Fund - Regular Plan - Growth","Franklin India Small Cap Fund - Growth",
  "Motilal Oswal ELSS Tax Saver Fund - Regular Plan - Growth","Edelweiss Multi Cap Fund - Regular Plan - Growth",
  "ICICI Prudential Large and Mid Cap Fund - Growth","Axis Large Cap Fund - Regular Plan - Growth",
  "ICICI Prudential Midcap Fund - Growth","Kotak Large and Mid Cap Fund - Regular Plan - Growth",
  "Tata ELSS Fund - Regular Plan - Growth","Kotak ELSS Tax Saver Fund - Regular Plan - Growth",
  "UTI Large and Mid Cap Fund - Regular Plan - Growth","Tata Retirement Savings Fund - Moderate Plan - Regular Plan - Growth",
  "Aditya Birla Sun Life Focused Fund - Regular Plan - Growth","Aditya Birla Sun Life Balanced Advantage Fund - Regular Plan - Growth",
  "WhiteOak Capital ELSS Tax Saver Fund - Regular Plan - Growth","HSBC ELSS Tax Saver Fund - Regular Plan - Growth",
  "Aditya Birla Sun Life Consumption Fund - Regular Plan - Growth","Altiva Hybrid Long-Short Fund - Regular Plan - Growth",
  "Aditya Birla Sun Life Large and Mid Cap Fund - Regular Plan - Growth","HDFC Childrens Fund - Regular Plan - Growth",
  "ITI Banking and Financial Services Fund - Regular Plan - Growth","LIC MF Small Cap Fund - Regular Plan - Growth",
  "PGIM India Small Cap Fund - Regular Plan - Growth","Canara Robeco Small Cap Fund - Regular Plan - Growth",
  "Nippon India US Equity Opportunites Fund - Growth","Bandhan Large and Mid Cap Fund - Regular Plan - Growth",
  "Sundaram Multi Factor Fund - Regular Plan - Growth","ICICI Prudential Silver ETF FOF - Growth",
  "ICICI Prudential Value Fund - Regular Plan - Growth","ICICI Prudential Childrens Fund - Regular Plan - Growth",
  "Quant Multi Cap Fund - Regular Plan - Growth","HSBC Aggressive Hybrid Fund - Regular Plan - Growth",
  "Bajaj Finserv Small Cap Fund - Regular Plan - Growth","Helios Small Cap Fund - Regular Plan - Growth",
  "Bandhan Infrastructure Fund - Regular Plan - Growth","Axis Business Cycles Fund - Regular Plan - Growth",
  "Invesco India Mid Cap Fund - Regular Plan - Growth","Baroda BNP Paribas Mid Cap Fund - Regular Plan - Growth",
  "LIC MF Manufacturing Fund - Regular Plan - Growth","Axis Consumption Fund - Regular Plan - Growth",
  "Aditya Birla Sun Life PSU Equity Fund - Regular Plan - Growth","Mirae Asset Diversified Equity Allocator Passive FOF - Regular Plan - Growth",
  "Aditya Birla Sun Life Large Cap Fund - Regular Plan - Growth","HSBC Infrastructure Fund - Regular Plan - Growth",
  "Canara Robeco Focused Fund - Regular Plan - Growth","Union ELSS Tax Saver Fund - Regular Plan - Growth",
  "Mirae Asset ELSS Tax Saver Fund - Regular Plan - Growth","WhiteOak Capital Aggressive Hybrid Fund Regular Growth",
  "DSP Dynamic Asset Allocation Fund - Regular Plan - Growth","ICICI Prudential ELSS Tax Saver Fund - Regular Plan - Growth",
  "UTI Quant Fund - Regular Plan - Growth","Union Innovation & Opportunities Fund - Regular Plan - Growth",
  "HSBC Multi Cap Fund - Regular Plan - Growth","Sundaram Multi Cap Fund - Regular Plan - Growth",
  "WhiteOak Capital Quality Equity Fund - Regular Plan - Growth","SBI Energy Opportunities Fund - Regular Plan - Growth",
  "Nippon India Value Fund - Growth","Aditya Birla Sun Life Nifty India Defence Index Fund - Regular Plan - Growth",
  "Tata Ultra Short Term Fund - Regular Plan - Growth","Bajaj Finserv Balanced Advantage Fund - Regular Plan - Growth",
  "Union Flexi Cap Fund - Regular Plan - Growth","Bandhan Focused Fund - Regular Plan - Growth",
  "Baroda BNP Paribas Small Cap Fund - Regular Plan - Growth","Nippon India Active Momentum Fund - Regular Plan - Growth",
  "Axis Multicap Fund - Regular Plan - Growth","ICICI Prudential US Bluechip Equity Fund - Regular - Growth",
  "HSBC Multi Asset Allocation Fund - Regular Plan - Growth","ICICI Prudential Energy Opportunities Fund - Regular Plan - Growth",
  "Nippon India Liquid Fund - Growth","Nippon India Vision Large and Mid Cap Fund - Growth",
  "HDFC Large Cap Fund - Regular Plan - Growth","SBI Automotive Opportunities Fund - Regular Plan - Growth",
  "SBI Focused Fund - Regular Plan - Growth","Motilal Oswal Manufacturing Fund - Regular Plan - Growth",
  "Franklin India Dividend Yield Fund - Growth","Abakkus Large and Mid Cap Fund - Regular Plan - Growth",
  "ICICI Prudential Corporate Bond Fund - Growth","SBI Quant Fund - Regular Plan - Growth",
  "HSBC Equity Savings Fund - Regular Plan - Growth","Nippon India Multi Asset Omni FOF - Regular Plan - Growth",
  "ICICI Prudential Infrastructure Fund - Growth","WhiteOak Capital Balanced Advantage Fund - Regular Plan - Growth",
  "HSBC Balanced Advantage Fund - Regular Plan - Growth","Bandhan Innovation Fund - Regular Plan - Growth",
  "UTI Multi Asset Allocation Fund - Regular Plan - Growth","Bank of India Flexi Cap Fund - Regular Plan - Growth",
  "SBI ESG Exclusionary Strategy Fund - Regular Plan - Growth","Parag Parikh ELSS Tax Saver Fund - Regular Plan - Growth",
  "HDFC Large and Mid Cap Fund - Regular Plan - Growth","Tata Value Fund - Regular Plan - Growth",
  "Quant Small Cap Fund - Growth","SBI Flexi Cap Fund - Regular Plan - Growth",
  "ITI Value Fund - Regular Plan - Growth","Sundaram Consumption Fund - Regular Plan - Growth",
  "HSBC Consumption Fund - Regular Plan - Growth","DSP Aggressive Hybrid Fund - Regular Plan - IDCW",
  "ICICI Prudential ESG Exclusionary Strategy Fund - Regular Plan - Growth","Edelweiss Balanced Advantage Fund - Regular Plan - Monthly IDCW",
  "Helios Mid Cap Fund - Regular Plan - Growth","Invesco India Multicap Fund - Regular Plan - Growth",
  "WhiteOak Capital Digital Bharat Fund - Regular Plan - Growth","Axis Momentum Fund - Regular Plan - Growth",
  "DSP Aggressive Hybrid Fund - Regular Plan - Growth","Franklin India Flexi Cap Fund - Growth",
  "Motilal Oswal Balanced Advantage Fund - Regular Plan - Growth","Union Balanced Advantage Fund - Regular Plan - Growth",
  "Tata India Consumer Fund - Regular Plan - Growth","Bajaj Finserv Multi Cap Fund - Regular Plan - Growth",
  "Nippon India Consumption Fund - Growth","ICICI Prudential Ultra Short Term Fund - Regular Plan - Growth",
  "Franklin India Focused Equity Fund - Growth","DSP Multi Asset Omni Fund of Funds - Regular Plan - Growth",
  "Aditya Birla Sun Life Small Cap Fund - Regular Plan - Growth","PGIM India ELSS Tax Saver Fund - Regular Plan - Growth",
  "UTI MNC Fund - Regular Plan - Growth","Franklin India Opportunities Fund - Growth",
  "Kotak Large Cap Fund - Regular Plan - Growth","LIC MF Multi Cap Fund - Regular Plan - Growth",
  "Axis Nifty Smallcap 50 Index Fund - Regular Plan - Growth","ICICI Prudential Transportation and Logistics Fund - Regular Plan - Growth",
  "Quant Aggressive Hybrid Fund - Regular Plan - Growth","Aditya Birla Sun Life MNC Fund - Regular Plan - Growth",
  "Mirae Asset Small Cap Fund - Regular Plan - Growth","Tata Aggressive Hybrid Fund - Regular Plan - Monthly IDCW",
  "HDFC Multi Asset Active FOF - Regular Plan - Growth","HDFC Retirement Savings Fund - Equity Plan - Regular Plan - Growth",
  "Aditya Birla Sun Life Bal Bhavishya Yojna - Regular Plan - Growth","Union Largecap Fund - Regular Plan - Growth",
  "Axis Equity Savings Fund - Regular Plan - Growth","Aditya Birla Sun Life MNC Fund - Regular Plan - IDCW",
  "SBI Infrastructure Fund - Regular Plan - Growth","Tata Retirement Savings Fund - Progresive Plan - Regular Plan - Growth",
  "DSP Large Cap Fund - Regular Plan - Growth","ICICI Prudential Banking and Financial Services Fund - Growth",
  "Axis Children's Fund - No Lock-in - Regular Plan - Growth","DSP India Tiger Fund - Regular Plan - Growth",
  "Quant Manufacturing Fund - Regular Plan - Growth","Mirae Asset Multicap Fund - Regular Plan - Growth",
  "Edelweiss ELSS Tax Saver Fund - Regular Plan - Growth","ICICI Prudential Equity and Debt Fund - Growth",
  "Helios Large and Mid Cap Fund - Regular Plan - Growth","HDFC Technology Fund - Regular Plan - Growth",
  "UTI Transportation and Logistics Fund - Regular Plan - IDCW","Tata Nifty Auto Index Fund - Regular Plan - Growth",
  "ICICI Prudential Gold ETF FOF - Growth","Kotak Gold Silver Passive FOF - Regular Plan - Growth",
  "ICICI Prudential Active Momentum Fund - Regular Plan - Growth","Aditya Birla Sun Life Quant Fund - Regular Plan - Growth",
  "Quant Infrastructure Fund - Growth","ICICI Prudential Savings Fund - Growth",
  "ITI ELSS Tax Saver Fund - Regular Plan - Growth","ITI Flexi Cap Fund - Regular Plan - Growth",
  "LIC MF Low Duration Fund - Regular Plan - Growth","Motilal Oswal Business Cycle Fund - Regular Plan - Growth",
  "Mirae Asset Multi Asset Allocation Fund - Regular Plan - Growth","Union Business Cycle Fund - Regular Plan - Growth",
  "Kotak Special Opportunities Fund - Regular Plan - Growth","Union Mid Cap Fund - Regular Plan - Growth",
  "HSBC Large Cap Fund - Regular Plan - Growth","Bandhan Value Fund - Regular Plan - Growth",
  "Sundaram ELSS Tax Saver Fund - Regular Plan - Growth","Sundaram Focused Fund - Regular Plan - Growth",
  "Edelweiss NIFTY Large Mid Cap 250 Index Fund - Regular Plan - Growth","Aditya Birla Sun Life Multi Asset Allocation Fund - Regular Plan - Growth",
  "Nippon India MNC Fund - Regular Plan - Growth","UTI Transportation and Logistics Fund - Regular Plan - Growth",
  "HSBC Business Cycles Fund - Regular Plan - Growth","ITI Mid Cap Fund - Regular Plan - Growth",
  "Quant Multi Asset Allocation Fund - Regular Plan - Growth","HSBC Large and Mid Cap Fund - Regular Plan - Growth",
  "Bandhan Multi Cap Fund - Regular Plan - Growth","Canara Robeco Mid Cap Fund - Regular Plan - Growth",
  "PGIM India Large Cap Fund - Growth","HDFC Mid Cap Fund - Regular Plan - IDCW",
  "DSP US Specific Equity Omni FOF - Regular Plan - Growth","SBI ELSS Tax Saver Fund - Regular Plan - Growth",
  "HDFC Consumption Fund - Regular Plan - Growth","Edelweiss Consumption Fund - Regular Plan - Growth",
  "SBI Consumption Opportunities Fund - Regular Plan - Growth","Quant Focused Fund - Growth",
  "Axis Gold Fund - Regular Plan - Growth","Motilal Oswal Consumption Fund - Regular Plan - Growth",
  "Bandhan Business Cycle Fund - Regular Plan - Growth","Bajaj Finserv Multi Asset Allocation Fund - Regular Plan - Growth",
  "ICICI Prudential Quant Fund - Growth","Union Value Fund - Regular Plan - Growth",
  "Franklin India Large and Mid Cap Fund - Growth","Edelweiss Business Cycle Fund - Regular Plan - Growth",
  "Tata Corporate Bond Fund - Regular Plan - Growth","ICICI Prudential Housing Opportunities Fund - Regular Plan - Growth",
  "Union Multi Asset Allocation Fund - Regular Plan - Growth","Axis Global Equity Alpha Fund of Fund - Regular Plan - Growth",
  "Aditya Birla Sun Life Manufacturing Equity Fund - Regular Plan - Growth","UTI India Consumer Fund - Regular Plan - Growth",
  "ICICI Prudential Pharma Healthcare and Diagnostics (P.H.D) Fund - Growth","HDFC Business Cycle Fund - Regular Plan - Growth",
  "Invesco India Small Cap Fund - Regular Plan - Growth","ITI Large and Mid Cap Fund - Regular Plan - Growth",
  "Baroda BNP Paribas Multi Asset Fund - Regular Plan - Growth","SBI Balanced Advantage Fund - Regular Plan - Growth",
  "UTI Small Cap Fund - Regular Plan - Growth","Abakkus Flexi Cap Fund - Regular Plan - Growth",
  "HSBC Ultra Short Duration Fund - Regular Plan - Growth","Bandhan Ultra Short Duration Fund - Regular Plan - Growth",
  "UTI Infrastructure Fund - Regular Plan - Growth","Bandhan ELSS Tax Saver Fund - Regular Plan - Growth",
  "Bajaj Finserv Large Cap Fund - Regular Plan - Growth","Motilal Oswal Developed Market Ex US ETFs Overseas Equity Passive FOF - Regular Plan - Growth",
  "LIC MF Mid Cap Fund - Regular Plan - Growth","Invesco India ELSS Tax Saver Fund - Regular Plan - Growth",
  "HDFC Pharma and Healthcare Fund - Regular Plan - Growth","ICICI Prudential Equity and Debt Fund - Monthly IDCW",
  "Bandhan Multi Factor Fund - Regular Plan - Growth","Quant Flexi Cap Fund - Growth",
  "Abakkus Liquid Fund - Regular Plan - Growth","UTI BSE Sensex Index Fund - Regular Plan - Growth",
  "Motilal Oswal Large Cap Fund - Regular Plan - Growth","DSP Ultra Short Fund - Regular Plan - Growth",
  "DSP Nifty 50 Equal Weight Index Fund - Regular Plan - Growth","PGIM India Large and Mid Cap Fund - Regular Plan - Growth",
  "Sundaram Services Fund - Regular Plan - Growth","Kotak Gold Fund - Regular Plan - Growth",
  "Sundaram Flexi Cap Fund - Regular Plan - Growth","Motilal Oswal Digital India Fund - Regular Plan - Growth",
  "Nippon India Nifty 500 Equal Weight Index Fund - Regular Plan - Growth","ITI Large Cap Fund - Regular Plan - Growth",
  "Motilal Oswal Nifty India Defence Index Fund - Regular Plan - Growth","HSBC India Export Opportunities Fund - Regular Plan - Growth",
  "SBI PSU Fund - Regular Plan - Growth","Bandhan Gold ETF FOF - Regular Plan - Growth",
  "UTI Balanced Advantage Fund - Regular Plan - Growth","Nippon India Low Duration Fund - Growth Option",
  "Motilal Oswal Gold and Silver Passive Fund of Funds - Regular Plan - Growth","Tata Nifty500 Multicap Infrastructure 50:30:20 Index Fund - Regular Plan - Growth",
  "HDFC Housing Opportunities Fund - Regular Plan - Growth","SBI MNC Fund - Regular Plan - Growth",
  "Motilal Oswal Liquid Fund - Regular Plan - Growth","Bajaj Finserv Large and Mid Cap Fund - Regular Plan - Growth",
  "ICICI Prudential Nifty LargeMidcap 250 Index Fund - Regular Plan - Growth","HDFC BSE India Sector Leaders Index Fund - Regular Plan - Growth",
  "Nippon India Large Cap Fund - IDCW","Aditya Birla Sun Life Nifty Midcap 150 Index Fund - Regular Plan - Growth",
  "Kotak US Specific Equity Passive FOF - Regular Plan - Growth","Mahindra Manulife Manufacturing Fund - Regular Plan - Growth",
  "Kotak Infrastructure And Economic Reform Fund - Standard - Regular Plan - Growth","Axis Silver Fund of Fund - Regular Plan - Growth",
  "Edelweiss Greater China Equity Off-Shore Fund - Regular Plan - Growth","WhiteOak Capital Large Cap Fund - Regular Plan - Growth",
  "UTI Banking and Financial Services Fund - Regular Plan - Growth","Invesco India Balanced Advantage Fund - Regular Plan - Growth",
  "UTI Healthcare Fund - Regular Plan - Growth","Tata Gold ETF Fund of Fund - Regular Plan - Growth",
  "Bank of India Multi Cap Fund - Regular Plan - Growth","Sundaram Balanced Advantage Fund - Regular Plan - Growth",
  "Baroda BNP Paribas Small Cap Fund - Regular Plan - IDCW","LIC MF Large Cap Fund - Regular Plan - Growth",
  "Invesco India Largecap Fund - Regular Plan - Growth","ICICI Prudential Bharat 22 FOF - Growth",
  "Tata Resources and Energy Fund - Regular Plan - Growth","Motilal Oswal Nifty 500 Momentum 50 Index Fund - Regular Plan - Growth",
  "ICICI Prudential Commodities Fund - Growth","Mahindra Manulife Small Cap Fund - Regular Plan - Growth",
  "ICICI Prudential Bharat Consumption Fund - Growth","Kotak Savings Fund - Regular Plan - Growth",
  "Tata Nifty200 Alpha 30 Index Fund - Regular Plan - Growth","Tata Childrens Fund - Regular Plan - Growth",
  "DSP Multicap Fund - Regular Plan - Growth","SBI Childrens Fund - Investment Plan - Regular Plan - Growth",
  "Axis India Manufacturing Fund - Regular Plan - Growth","ITI Ultra Short Duration Fund - Regular Plan - Growth",
  "Tata BSE Quality Index Fund - Regular Plan - Growth","Bandhan Silver ETF FOF - Regular Plan - Growth",
  "ICICI Prudential Multi-Asset Active FOF - Growth","DSP Silver ETF Fund of Fund - Regular Plan - Growth",
  "Bank of India Business Cycle Fund - Regular Plan - Growth","SBI ESG Exclusionary Strategy Fund - Regular Plan - IDCW",
  "Axis Greater China Equity Fund of Fund - Regular Plan - Growth","LIC MF Value Fund - Regular Plan - Growth",
  "Aditya Birla Sun Life Silver ETF Fund of Fund - Regular Plan - Growth","Tata India Pharma and Healthcare Fund - Regular Plan - Growth",
  "Motilal Oswal Special Opportunities Fund - Regular Plan - Growth","Franklin India Multi Cap Fund - Regular Plan - Growth",
  "SBI Banking and PSU Fund - Regular Plan - Growth","HDFC Nifty 50 Index Fund - Regular Plan - Growth",
  "Tata Aggressive Hybrid Fund - Regular Plan - Growth","ITI Business Cycle Fund - Regular Plan - Growth",
  "Franklin India Liquid Fund - Super Institutional Plan - Growth","Edelweiss Focused Fund - Regular Plan - Growth",
  "Quant Consumption Fund - Regular Plan - Growth","LIC MF ULIS (Regular Contribution 15 Years - Monthly) - Regular Plan - IDCW Reinvestment",
  "Bandhan Midcap Fund - Regular Plan - Growth","Invesco India Flexi Cap Fund - Regular Plan - Growth",
  "Baroda BNP Paribas Balanced Advantage Fund - Regular Plan - Growth","Kotak International REIT Overseas Equity Omni FOF - Regular Plan - Growth",
  "Quant Momentum Fund - Regular Plan - Growth","Mahindra Manulife Banking and Financial Services Fund - Regular Plan - Growth",
  "Canara Robeco Multi Cap Fund - Regular Plan - Growth","Franklin India Balanced Advantage Fund - Growth",
  "ICICI Prudential Strategic Metal and Energy Equity Fund of Fund - Regular Plan - Growth","ICICI Prudential NASDAQ 100 Index Fund - Regular Plan - Growth",
  "Aditya Birla Sun Life Savings Fund - Regular Plan - Growth","HDFC Ultra Short Term Fund - Regular Plan - Growth",
  "Edelweiss Gold ETF FOF - Regular Plan - Growth","ICICI Prudential All Seasons Bond Fund - Growth",
  "Nippon India Balanced Advantage Fund - IDCW","Mirae Asset Health Care Fund - Regular Plan - Growth",
  "Edelweiss Gold and Silver ETF Fund of Fund - Regular Plan - Growth","Sundaram Ultra Short Duration Fund - Regular Plan - Growth",
  "Aditya Birla Sun Life Credit Risk Fund - Regular Plan - Growth","Aditya Birla Sun Life ELSS Tax Saver Fund - Regular Plan - IDCW",
  "Nippon India Aggressive Hybrid Fund - Segregated Portfolio 2 - Growth","Nippon India Aggressive Hybrid Fund - Segregated Portfolio 2 - IDCW",];

// Fund names typed manually (because they weren't in the built-in list) get
// remembered here so the NEXT person who needs the same fund finds it as a
// suggestion instead of having to type it out again. Synced via Firestore
// like the other small collections, so it's shared across all RMs live.
function getLearnedFundNames(){
  return DB.get('learned_fund_names') || [];
}

// Every unique scheme name already sitting in the CRM's own MF client SIP
// records (c.sip_details[].scheme — populated by the SIP report import, see
// "SIP Details" modal). These are REAL scheme names the RMs already deal
// with, in the exact spelling/format the SIP import uses — a much better
// autocomplete source than the generic built-in list for schemes this office
// actually has live SIPs in. Cached per page-load since mf_clients can be a
// few thousand records; recomputed if it comes back empty (e.g. data synced
// in after the first computation).
let _crmSchemeNamesCache = null;
function getCrmSchemeNames(){
  if(_crmSchemeNamesCache && _crmSchemeNamesCache.length) return _crmSchemeNamesCache;
  const seen = new Set();
  const names = [];
  const scanList = (list) => (Array.isArray(list)?list:[]).forEach(d=>{
    const scheme = String(d.scheme||'').trim();
    if(!scheme) return;
    // Some SIP/AUM-report imports produced a truncated/mis-parsed "scheme"
    // value (e.g. just the word "GROWTH" instead of the full scheme name) —
    // a genuine fund name is always reasonably long and almost always
    // contains the word "fund", so this filters that garbage out rather
    // than surfacing it as a suggestion.
    if(scheme.length < 15) return;
    if(!/fund/i.test(scheme)) return;
    const key = scheme.toLowerCase();
    if(seen.has(key)) return;
    seen.add(key);
    names.push(scheme);
  });
  (DB.get('mf_clients')||[]).forEach(c=>{
    scanList(c.sip_details);
    scanList(c.aum_schemes); // lumpsum/non-SIP holdings, from a per-scheme AUM import
  });
  _crmSchemeNamesCache = names;
  return names;
}

// Which schemes THIS specific client already holds (from their own
// sip_details) — used to put their existing funds at the very top of the
// Fund Name suggestions when adding a transaction for them, since an
// "Additional Buy"/"Redemption"/"Switch" almost always targets a fund the
// client is already in, not some random new scheme.
function getClientSchemeNames(clientId){
  if(!clientId) return [];
  const c = (DB.get('mf_clients')||[]).find(x=>x.id===clientId);
  if(!c) return [];
  const seen=new Set(), names=[];
  const addFrom = list => (Array.isArray(list)?list:[]).forEach(d=>{
    const scheme=String(d.scheme||'').trim();
    if(!scheme || scheme.length<15 || !/fund/i.test(scheme)) return;
    const key=scheme.toLowerCase();
    if(seen.has(key)) return;
    seen.add(key);
    names.push(scheme);
  });
  addFrom(c.sip_details);
  addFrom(c.aum_schemes); // lumpsum/non-SIP holdings, from a per-scheme AUM import
  return names;
}
// Maps a Fund Name input's id to the currently-selected client's id for that
// same form, so searchFundName() knows whose funds to prioritize.
function _clientIdForFundInput(inputId){
  if(inputId==='mftxn-fund' || inputId==='mftxn-target-fund') return mfTxnSelectedClient?.id || null;
  if(inputId==='biz_fund' || inputId==='biz_target_fund') return currentBusinessTarget?.id || null;
  return null;
}

async function learnFundName(name){
  if(!name) return;
  const trimmed = name.trim();
  if(!trimmed) return;
  // Transaction-safe (20-Aug-2026, see DB.mutateArray above for why): this
  // used to read the local cache and blind-overwrite the whole array — if
  // two RMs saved a transaction with a different NEW fund-name spelling
  // around the same time, the second write silently erased the first RM's
  // learned name. That's why "Merge Fund Names" was reporting no duplicates
  // even with genuinely different-spelled saved transactions on record —
  // several of the actually-typed variants had never survived into
  // learned_fund_names at all.
  await DB.mutateArray('learned_fund_names', arr=>{
    const knownLower = new Set([...FUND_NAME_LIST, ...arr, ...getCrmSchemeNames()].map(n=>n.toLowerCase()));
    if(knownLower.has(trimmed.toLowerCase())) return false; // already known — nothing new to learn, abort write
    arr.push(trimmed);
  });
}

// ── FUND NAME DUPLICATE MERGE ──────────────────────────────────────────────
// RMs typing free-text over months produce spelling/casing drift on the SAME
// real fund ("helio mid cap fund" / "helios mid cap fund" / "HELIOS SMALL
// CAP" vs "...FUND") — this finds those clusters and lets Admin merge them:
// picks one canonical spelling, removes the rest from learned_fund_names, and
// rewrites any past mf_business transaction that used a variant so
// historical records/reports stay consistent too.

function _levenshtein(a, b){
  const m=a.length, n=b.length;
  if(m===0) return n; if(n===0) return m;
  let prev=new Array(n+1); for(let j=0;j<=n;j++) prev[j]=j;
  for(let i=1;i<=m;i++){
    const cur=[i];
    for(let j=1;j<=n;j++){
      cur[j] = a[i-1]===b[j-1] ? prev[j-1] : 1+Math.min(prev[j-1], prev[j], cur[j-1]);
    }
    prev=cur;
  }
  return prev[n];
}
// Strips punctuation/spaces and common filler words (Fund/Plan/Growth/etc.)
// so two spellings of the same scheme collapse to (near-)identical keys,
// while genuinely different schemes (different cap-size, different AMC)
// stay distinct.
function _fundLooseKey(name){
  const noise = new Set(['fund','regular','plan','direct','growth','option','reinvestment','reinvest','payout','idcw','dividend','scheme','the','of','and']);
  return String(name||'').toLowerCase().split(/[^a-z0-9]+/).filter(t=>t && !noise.has(t)).join('');
}
// Every distinct fund name/target-scheme actually sitting in saved
// mf_business transactions — the real ground truth of what's been typed and
// saved, independent of learned_fund_names (whose writes had a lost-update
// race bug until 20-Aug-2026; see DB.mutateArray/learnFundName above — some
// genuinely-saved spelling variants never made it into that list at all).
// Used only for duplicate-detection below, not as an autocomplete source.
function getTxnFundNames(){
  const seen = new Set(); const names = [];
  const biz = DB.get('mf_business') || {entries:[]};
  (biz.entries||[]).forEach(e=>{
    [e.fund_name, e.target_scheme].forEach(v=>{
      const s = String(v||'').trim();
      if(!s) return;
      const k = s.toLowerCase();
      if(seen.has(k)) return;
      seen.add(k); names.push(s);
    });
  });
  return names;
}
function findFundNameDupGroups(){
  // Fold every candidate through the fund-name alias table BEFORE clustering
  // — same logic searchFundName() already uses for the dropdown. Without
  // this, a name already merged/deleted by a prior "Merge Fund Names" pass
  // still physically exists inside client records (sip_details/aum_schemes
  // are never rewritten in place — see mergeFundDupsSelected comments) or
  // inside FUND_NAME_LIST, so the very next scan pulled it back in raw and
  // re-clustered it as a "new" duplicate — the merge looked like it silently
  // undid itself. Folding through aliasMap here makes the scanner respect
  // its own past merges/deletes, same as the dropdown already does.
  const aliasMap = {};
  (DB.get('fund_name_aliases')||[]).forEach(a=>{ if(a && a.v) aliasMap[a.v]=a.c; });
  const isDeleted = n => aliasMap[String(n||'').toLowerCase()] === '';
  const canon = n => aliasMap[String(n||'').toLowerCase()] || n;

  const learned = [...new Set([...getLearnedFundNames(), ...getTxnFundNames()].filter(n=>!isDeleted(n)).map(canon))];
  const referenceSeen = new Set();
  const reference = [];
  [...FUND_NAME_LIST, ...getCrmSchemeNames()].filter(n=>!isDeleted(n)).map(canon).forEach(n=>{
    const k=n.toLowerCase(); if(referenceSeen.has(k)) return; referenceSeen.add(k); reference.push(n);
  });
  if(!learned.length) return [];

  const all = [
    ...reference.map(name=>({name, isRef:true})),
    ...learned.map(name=>({name, isRef:false})),
  ];
  all.forEach(e=>{ e.key=_fundLooseKey(e.name); });

  // Bucket by first 3 chars of the loose key so we only Levenshtein-compare
  // plausible neighbours (n could be 600-1000+, full O(n²) would be slow).
  const buckets={};
  all.forEach((e,i)=>{
    const b=e.key.slice(0,3)||'~';
    (buckets[b] ||= []).push(i);
  });

  const parent=all.map((_,i)=>i);
  const find=x=>{ while(parent[x]!==x){ parent[x]=parent[parent[x]]; x=parent[x]; } return x; };
  const union=(a,b)=>{ const ra=find(a), rb=find(b); if(ra!==rb) parent[ra]=rb; };

  Object.values(buckets).forEach(idxs=>{
    for(let i=0;i<idxs.length;i++){
      for(let j=i+1;j<idxs.length;j++){
        const ei=all[idxs[i]], ej=all[idxs[j]];
        if(!ei.key || !ej.key) continue;
        if(ei.key===ej.key){ union(idxs[i],idxs[j]); continue; }
        if(Math.abs(ei.key.length-ej.key.length)<=2 && ei.key.length>=6 && ej.key.length>=6){
          if(_levenshtein(ei.key, ej.key)<=2) union(idxs[i],idxs[j]);
        }
      }
    }
  });

  const clusters={};
  all.forEach((e,i)=>{ const r=find(i); (clusters[r] ||= []).push(e); });

  const groups=[];
  Object.values(clusters).forEach(members=>{
    if(members.length<2) return;
    // No longer requires at least one "learned" (actually-typed/saved) member
    // — a cluster made entirely of reference-list spellings (built-in
    // FUND_NAME_LIST + real scheme names pulled from client SIP/AUM data)
    // is just as real a duplicate for the dropdown's purposes, even if no
    // transaction happens to have used one of these exact spellings yet
    // (20-Aug-2026: e.g. "Abakkus Small Cap Fund - Regular Plan - Growth" /
    // "ABAKKUS SMALL CAP FUND - Regular Growth" / two Flexi Cap variants —
    // all reference-only, all clearly the same funds). Similarly no longer
    // blocks on 2+ distinct reference spellings in one cluster — that used
    // to be a safety guard against two genuinely different funds fuzzy-
    // matching together, but the clustering step above (3-char bucket +
    // length-gated Levenshtein ≤2 on noise-stripped loose keys) is already
    // conservative enough to make that rare, and the review screen now lets
    // admin see every member and either uncheck a wrong group entirely or
    // pick a different survivor via radio button — a second layer of safety
    // that makes this guard's cost (silently hiding real reference-only
    // duplicates like the Abakkus case) no longer worth paying.
    const canonical = members.slice().sort((a,b)=> b.name.length - a.name.length)[0].name;
    const variants = members.filter(m=>m.name.toLowerCase()!==canonical.toLowerCase()).map(m=>m.name);
    if(!variants.length) return;
    groups.push({ canonical, variants: [...new Set(variants)] });
  });
  return groups;
}

function _fundDupRadioChanged(gi){
  document.querySelectorAll(`input[name="funddup-radio-${gi}"]`).forEach(r=>{
    const label = r.closest('label');
    const span = label ? label.querySelector('span') : null;
    if(!label || !span) return;
    if(r.checked){ label.style.background='#f0fdf4'; span.style.color='#166534'; span.style.fontWeight='700'; }
    else { label.style.background=''; span.style.color='#b91c1c'; span.style.fontWeight='400'; }
  });
}

function openFundDupMerge(){
  if(CU.role!=='admin' && CU.role!=='backoffice' && !CU.backoffice_access){ toast('This tool is for admin only','error'); return; }
  const esc = v => String(v==null?'':v).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const groups = findFundNameDupGroups();
  // Every custom-typed fund name currently reachable from the suggestion
  // dropdown (learned_fund_names ∪ names actually saved on a transaction) —
  // listed here so Admin can delete a genuinely wrong/garbage entry outright
  // (a typo nobody should keep, not a real duplicate of anything to merge
  // into). Excludes anything already hidden via a prior delete.
  const aliasMap = {};
  (DB.get('fund_name_aliases')||[]).forEach(a=>{ if(a && a.v) aliasMap[a.v]=a.c; });
  const allCustom = [...new Set([...getLearnedFundNames(), ...getTxnFundNames()])]
    .filter(n=>aliasMap[n.toLowerCase()]!=='')   // '' = already deleted
    .sort((a,b)=>a.localeCompare(b));
  const ov=document.createElement('div');
  ov.id='fundDupOverlay';
  ov.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:9999;display:flex;align-items:flex-start;justify-content:center;overflow:auto;padding:20px';

  // ── Section 1: duplicate groups (merge, pick survivor via radio) ──
  let rows='';
  if(groups.length){
    groups.forEach((g,gi)=>{
      const allMembers = [g.canonical, ...g.variants];
      const memberRows = allMembers.map((m,mi)=>{
        const isCanon = m===g.canonical;
        return `<label style="display:flex;align-items:center;gap:8px;padding:5px 8px;border-radius:6px;cursor:pointer;font-size:.83rem;${isCanon?'background:#f0fdf4':''}">
          <input type="radio" name="funddup-radio-${gi}" class="funddup-radio" data-idx="${gi}" value="${esc(m)}" ${isCanon?'checked':''} onchange="_fundDupRadioChanged(${gi})">
          <span style="${isCanon?'color:#166534;font-weight:700':'color:#b91c1c'}">${esc(m)}</span>
        </label>`;
      }).join('');
      rows+=`<div style="border:1px solid #e5e7eb;border-radius:10px;margin-bottom:12px;overflow:hidden">
        <label style="display:flex;align-items:center;gap:8px;padding:8px 12px;background:#f8fafc;cursor:pointer;font-weight:600;font-size:.85rem">
          <input type="checkbox" class="funddup-chk" data-idx="${gi}" checked> Group ${gi+1} — ${allMembers.length} name(s), pick which one to keep
        </label>
        <div style="padding:10px 14px">
          <div style="font-size:.72rem;color:#64748b;margin-bottom:4px">Choose which spelling survives — the rest merge into it (past transactions using them get rewritten too):</div>
          ${memberRows}
        </div>
      </div>`;
    });
  }
  const groupsSection = groups.length
    ? '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">'
      +   '<div style="font-weight:700;font-size:.9rem">Duplicate groups found ('+groups.length+')</div>'
      +   '<label style="display:flex;align-items:center;gap:5px;font-size:.78rem;color:#64748b;cursor:pointer;font-weight:400"><input type="checkbox" id="funddup-all" checked onchange="document.querySelectorAll(\'.funddup-chk\').forEach(c=>c.checked=this.checked)"> Select all groups</label>'
      + '</div>'
      +'<div style="padding:8px 12px;font-size:.78rem;color:#7c5e10;background:#fffbeb;border:1px solid #fde68a;border-radius:8px;margin-bottom:12px">✅ Click a name in any group to pick it as the one that survives — green = currently selected to KEEP, red = will be merged into it. Merging removes the others from the suggestion list AND rewrites any past transaction (Fund Name / Switch target) that used them, so historical records stay consistent. Uncheck a group to skip merging it.</div>'
      + '<div style="max-height:320px;overflow:auto;padding-right:4px">' + rows + '</div>'
      + '<div style="display:flex;gap:10px;justify-content:flex-end;margin:10px 0 24px">'
      + '<button class="btn btn-teal" onclick="mergeFundDupsSelected()">✔ Merge Selected</button></div>'
    : '<div style="padding:16px;text-align:center;background:#f0fdf4;border-radius:10px;margin-bottom:24px"><div style="font-size:1.5rem">✅</div><div style="font-weight:700;font-size:.92rem;margin-top:4px">No Duplicate Fund Names Found</div><div style="color:#64748b;font-size:.8rem;margin-top:2px">All typed-in fund names look distinct.</div></div>';

  // ── Section 2: delete wrong/garbage names outright ──
  const delSection = !allCustom.length ? '' :
    '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">'
    +   '<div style="font-weight:700;font-size:.9rem">All custom-typed fund names ('+allCustom.length+') — delete wrong ones</div>'
    +   '<label style="display:flex;align-items:center;gap:5px;font-size:.78rem;color:#64748b;cursor:pointer;font-weight:400"><input type="checkbox" id="funddel-all" onchange="document.querySelectorAll(\'.funddel-chk\').forEach(c=>c.checked=this.checked)"> Select all</label>'
    + '</div>'
    + '<div style="border:1px solid #e5e7eb;border-radius:10px;max-height:260px;overflow:auto">'
    + allCustom.map(n=>`<div style="display:flex;align-items:center;gap:10px;padding:7px 12px;border-bottom:1px solid #f0f0f0;font-size:.83rem">
        <input type="checkbox" class="funddel-chk" value="${esc(n)}">
        <span style="flex:1">${esc(n)}</span>
        <button title="Delete this fund name from suggestions" onclick="deleteFundNameSuggestion('${esc(n).replace(/'/g,"\\'")}')" style="border:none;background:#fef2f2;color:#b91c1c;border-radius:6px;width:26px;height:26px;cursor:pointer;font-size:.9rem;flex-shrink:0">✕</button>
      </div>`).join('')
    + '</div>'
    + '<div style="display:flex;justify-content:flex-end;margin:8px 0 24px">'
    + '<button class="btn" style="background:#fef2f2;color:#b91c1c;border:1px solid #fecaca" onclick="deleteFundNameSuggestionsBulk()">🗑 Delete Selected</button></div>';

  // ── Section 3: restore anything previously merged-away or deleted ──
  const aliasList = (DB.get('fund_name_aliases')||[]).slice().sort((a,b)=>a.v.localeCompare(b.v));
  const restoreSection = !aliasList.length ? '' :
    '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">'
    +   '<div style="font-weight:700;font-size:.9rem">Previously merged/deleted names ('+aliasList.length+') — restore if needed</div>'
    +   '<label style="display:flex;align-items:center;gap:5px;font-size:.78rem;color:#64748b;cursor:pointer;font-weight:400"><input type="checkbox" id="fundrestore-all" onchange="document.querySelectorAll(\'.fundrestore-chk\').forEach(c=>c.checked=this.checked)"> Select all</label>'
    + '</div>'
    + '<div style="border:1px solid #e5e7eb;border-radius:10px;max-height:260px;overflow:auto">'
    + aliasList.map(a=>`<div style="display:flex;align-items:center;gap:10px;padding:7px 12px;border-bottom:1px solid #f0f0f0;font-size:.83rem">
        <input type="checkbox" class="fundrestore-chk" value="${esc(a.v)}">
        <span style="flex:1">${esc(a.v)} ${a.c==='' ? '<span style="color:#b91c1c">— deleted</span>' : `<span style="color:#64748b">→ merged into "${esc(a.c)}"</span>`}</span>
        <button title="Restore — show this in the dropdown again" onclick="restoreFundNameAlias('${esc(a.v).replace(/'/g,"\\'")}')" style="border:none;background:#f0fdf4;color:#166534;border-radius:6px;padding:4px 10px;cursor:pointer;font-size:.78rem;flex-shrink:0;white-space:nowrap">↩ Restore</button>
      </div>`).join('')
    + '</div>'
    + '<div style="display:flex;justify-content:flex-end;margin-top:8px">'
    + '<button class="btn" style="background:#f0fdf4;color:#166534;border:1px solid #bbf7d0" onclick="restoreFundNameAliasesBulk()">↩ Restore Selected</button></div>';

  const header='<div style="padding:16px 18px;border-bottom:1px solid #eee;display:flex;justify-content:space-between;align-items:center;flex-shrink:0">'
    +'<div style="font-weight:800;font-size:1.05rem">🔀 Fund Names — Merge Duplicates / Delete</div>'
    +'<button onclick="document.getElementById(\'fundDupOverlay\').remove()" style="border:none;background:#f1f5f9;border-radius:8px;width:30px;height:30px;cursor:pointer;font-size:1rem">✕</button></div>';
  const footer='<div style="padding:12px 18px;border-top:1px solid #eee;display:flex;gap:10px;justify-content:flex-end;flex-shrink:0">'
    +'<button class="btn btn-outline" onclick="document.getElementById(\'fundDupOverlay\').remove()">Close</button></div>';
  // Header and footer stay pinned; only the middle (all three sections)
  // scrolls as one — each section also keeps its own internal scroll cap
  // (see max-height above) so a long list within a section doesn't force
  // the whole modal to grow past the screen.
  ov.innerHTML='<div style="background:#fff;border-radius:14px;width:min(760px,96vw);max-height:88vh;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 20px 50px rgba(0,0,0,.3)" onclick="event.stopPropagation()">'
    + header
    + '<div style="padding:16px 18px;overflow:auto;flex:1">'+groupsSection+delSection+restoreSection+'</div>'
    + footer
    + '</div>';
  ov.addEventListener('click',e=>{ if(e.target===ov) ov.remove(); });
  document.body.appendChild(ov);
}

// Deletes one fund name outright from every suggestion source — not a merge
// (nothing takes its place), for a genuinely wrong/garbage typed name that
// shouldn't be remembered at all. Removes it from learned_fund_names, and
// records a '' (hide) sentinel in fund_name_aliases so searchFundName()
// filters it out even if it also lives inside some client's own
// sip_details/aum_schemes scheme data (getCrmSchemeNames) — that underlying
// client record is left untouched, only the suggestion-dropdown visibility
// is affected. Does NOT touch past mf_business transactions that already
// used this name (deleting a suggestion isn't the same as saying those
// historical entries were wrong).
async function _deleteFundNameSuggestions(names){
  const keys = names.map(n=>n.toLowerCase());
  await DB.mutateArray('learned_fund_names', arr=>{
    const before = arr.length;
    for(let i=arr.length-1;i>=0;i--){ if(keys.includes(arr[i].toLowerCase())) arr.splice(i,1); }
    if(arr.length===before) return false;
  });
  await DB.mutateArray('fund_name_aliases', arr=>{
    let changed=false;
    keys.forEach(key=>{
      const existing = arr.find(a=>a.v===key);
      if(existing){ if(existing.c!==''){ existing.c=''; changed=true; } }
      else { arr.push({v:key, c:''}); changed=true; }
    });
    if(!changed) return false;
  });
}
async function deleteFundNameSuggestion(name){
  if(!(await dangerConfirm(`Delete "${name}" from fund-name suggestions? This won't change any past transaction that already used it.`))) return;
  await _deleteFundNameSuggestions([name]);
  toast(`Deleted "${name}" from suggestions`,'success');
  document.getElementById('fundDupOverlay')?.remove();
  openFundDupMerge();
}
async function deleteFundNameSuggestionsBulk(){
  const names = [...document.querySelectorAll('.funddel-chk:checked')].map(c=>c.value);
  if(!names.length){ toast('No fund names selected','error'); return; }
  if(!(await dangerConfirm(`Delete ${names.length} fund name(s) from suggestions? This won't change any past transaction that already used them.`))) return;
  await _deleteFundNameSuggestions(names);
  toast(`Deleted ${names.length} fund name(s) from suggestions`,'success');
  document.getElementById('fundDupOverlay')?.remove();
  openFundDupMerge();
}

// Undo one entry from fund_name_aliases — a variant that got merged into a
// canonical, or deleted outright (c==='' sentinel). Found 20-Aug-2026: an
// admin merging/deleting during testing can catch a genuinely still-in-use
// scheme name (e.g. "Abakkus Large and Mid Cap Fund", which a client
// actually holds) in the same sweep as real duplicates — since the alias
// table is what makes a merge/delete "stick" for the dropdown regardless of
// source, removing the entry here is the only way to bring that name back.
async function restoreFundNameAlias(variantLower){
  await DB.mutateArray('fund_name_aliases', arr=>{
    const idx = arr.findIndex(a=>a.v===variantLower);
    if(idx<0) return false;
    arr.splice(idx,1);
  });
  _crmSchemeNamesCache = null;
  toast('Restored — it will show in the dropdown again','success');
  document.getElementById('fundDupOverlay')?.remove();
  openFundDupMerge();
}
async function restoreFundNameAliasesBulk(){
  const vals = [...document.querySelectorAll('.fundrestore-chk:checked')].map(c=>c.value);
  if(!vals.length){ toast('No names selected','error'); return; }
  await DB.mutateArray('fund_name_aliases', arr=>{
    const before = arr.length;
    for(let i=arr.length-1;i>=0;i--){ if(vals.includes(arr[i].v)) arr.splice(i,1); }
    if(arr.length===before) return false;
  });
  _crmSchemeNamesCache = null;
  toast(`Restored ${vals.length} fund name(s)`,'success');
  document.getElementById('fundDupOverlay')?.remove();
  openFundDupMerge();
}

async function mergeFundDupsSelected(){
  const groups = findFundNameDupGroups();
  const checked = [...document.querySelectorAll('.funddup-chk')].filter(x=>x.checked).map(x=>parseInt(x.dataset.idx));
  if(!checked.length){ toast('No group selected','error'); return; }

  // Build the final list of {canonical, variants} from whichever radio the
  // admin picked as the survivor in each group (defaults to the
  // auto-suggested canonical if nothing was changed).
  const selectedGroups = checked.map(idx=>{
    const picked = document.querySelector(`input[name="funddup-radio-${idx}"]:checked`);
    const canonical = picked ? picked.value : groups[idx].canonical;
    const allMembers = [groups[idx].canonical, ...groups[idx].variants];
    const variants = allMembers.filter(m=>m!==canonical);
    return { canonical, variants };
  }).filter(g=>g.canonical && g.variants.length);

  const totalVariants = selectedGroups.reduce((s,g)=>s+g.variants.length,0);
  if(!(await dangerConfirm(`${selectedGroups.length} group(s), ${totalVariants} variant name(s) will be merged into their canonical spelling — permanently rewriting any past transactions that used them. Proceed?`))) return;

  // Build one lowercase-variant → canonical map across all selected groups.
  const rewriteMap = {};
  selectedGroups.forEach(g=>{
    g.variants.forEach(v=>{ rewriteMap[v.toLowerCase()] = g.canonical; });
  });

  // 1) Clean learned_fund_names — drop every variant; keep canonical only if
  // it isn't already covered by the built-in/CRM-scheme reference list.
  const existingLearned = getLearnedFundNames();
  const referenceLower = new Set([...FUND_NAME_LIST, ...getCrmSchemeNames()].map(n=>n.toLowerCase()));
  let newLearned = existingLearned.filter(n=>!(n.toLowerCase() in rewriteMap));
  selectedGroups.forEach(g=>{
    if(!referenceLower.has(g.canonical.toLowerCase()) && !newLearned.some(n=>n.toLowerCase()===g.canonical.toLowerCase())){
      newLearned.push(g.canonical);
    }
  });
  await DB.set('learned_fund_names', newLearned);

  // 1b) Persist the variant→canonical mapping permanently (20-Aug-2026).
  // Some variants live only inside mf_clients[].sip_details[]/aum_schemes[]
  // (real scheme-name spellings straight from RTA SIP/AUM report imports,
  // NOT learned_fund_names) — the fund-name search box's dropdown
  // (searchFundName) also pulls candidates from there via getCrmSchemeNames(),
  // so removing a variant from learned_fund_names alone doesn't stop it
  // reappearing in the dropdown. Rewriting every client's stored scheme
  // records directly is riskier (touches thousands of records, and those
  // exact strings are also displayed elsewhere as historical portfolio
  // data), so instead every future dropdown build consults this alias table
  // and folds any variant into its canonical spelling before dedup —
  // regardless of which source it came from.
  await DB.mutateArray('fund_name_aliases', arr=>{
    let changed=false;
    Object.keys(rewriteMap).forEach(v=>{
      const c = rewriteMap[v];
      const existing = arr.find(a=>a.v===v);
      if(existing){ if(existing.c!==c){ existing.c=c; changed=true; } }
      else { arr.push({v, c}); changed=true; }
    });
    if(!changed) return false;
  });

  // 2) Rewrite past mf_business transactions using a variant name.
  const biz = DB.get('mf_business') || {entries:[], eq_entries:[]};
  let fixedCount = 0;
  const newEntries = (biz.entries||[]).map(e=>{
    let changed=false;
    const ne = {...e};
    if(ne.fund_name && rewriteMap[ne.fund_name.toLowerCase()]){ ne.fund_name = rewriteMap[ne.fund_name.toLowerCase()]; changed=true; }
    if(ne.target_scheme && rewriteMap[ne.target_scheme.toLowerCase()]){ ne.target_scheme = rewriteMap[ne.target_scheme.toLowerCase()]; changed=true; }
    if(changed) fixedCount++;
    return ne;
  });
  await DB.set('mf_business', {entries:newEntries, eq_entries: biz.eq_entries||[]});

  _crmSchemeNamesCache = null; // stale after this — force recompute next lookup
  document.getElementById('fundDupOverlay')?.remove();
  toast(`✅ Merged ${totalVariants} variant name(s) across ${selectedGroups.length} group(s) — ${fixedCount} past transaction(s) updated`, 'success');
}

function searchFundName(inputId, resultsId){
  const input=document.getElementById(inputId);
  const out=document.getElementById(resultsId);
  if(!input||!out) return;
  const q=input.value.trim().toLowerCase();
  const clientId = _clientIdForFundInput(inputId);
  const clientFunds = getClientSchemeNames(clientId);
  // Normally wait for 2+ characters before searching — but if a client is
  // already selected on this form, show THEIR existing funds immediately on
  // focus (empty query), since picking one of those is the common case for
  // an Additional Buy / Redemption / Switch and shouldn't require typing.
  if(q.length<2 && !(clientFunds.length && document.activeElement===input)){
    out.style.display='none'; out.innerHTML=''; return;
  }

  // Escape to <html> (documentElement), same fix as the badge tooltip: the
  // app's own zoom control sets `document.body.style.zoom`, and any
  // position:fixed element left as a body DESCENDANT still inherits that
  // zoomed coordinate space (mismatched against real mouse/viewport pixels)
  // — AND being nested inside the page's normal flow, it can still get
  // visually squeezed/clipped by whatever sits below it (like this table),
  // which is why mouse-wheel scrolling over the list was landing on the
  // table underneath instead of the dropdown itself. Moving it out to
  // <html> once (first time it's shown) sidesteps both problems — real
  // fixed positioning, and a genuinely top-level element for scroll/click
  // hit-testing.
  if(out.parentElement !== document.documentElement){
    document.documentElement.appendChild(out);
    out.style.position='fixed';
    out.style.margin='0';
    out.dataset.anchorInput=inputId;
  }
  const r=input.getBoundingClientRect();
  out.style.left=r.left+'px';
  out.style.width=r.width+'px';
  const maxH=260;
  if(r.bottom+8+maxH > window.innerHeight){
    out.style.top='';
    out.style.bottom=(window.innerHeight-r.top+4)+'px'; // not enough room below → show above the input
  } else {
    out.style.bottom='';
    out.style.top=(r.bottom+4)+'px';
  }

  // Client's own existing funds first (they're who this transaction is
  // for — almost always the right answer), then the rest of the CRM's real
  // scheme names, then the generic built-in list, then anything manually
  // learned — deduped, client funds keep priority position even if they'd
  // otherwise also appear further down one of the other lists.
  //
  // Every candidate is folded through the fund-name alias table (built by
  // "Merge Fund Names", 20-Aug-2026) BEFORE dedup — a variant spelling
  // sitting inside a client's own SIP/AUM records (getCrmSchemeNames) isn't
  // rewritten in place, so without this the dropdown would keep resurfacing
  // it forever even after an admin merged it away.
  const aliasMap = {};
  (DB.get('fund_name_aliases')||[]).forEach(a=>{ if(a && a.v) aliasMap[a.v]=a.c; });
  // c==='' is the delete sentinel (deleteFundNameSuggestion) — drop those
  // entirely rather than mapping through canon(), which would just return
  // the original name unchanged for a falsy/empty alias value.
  const isDeleted = n => aliasMap[String(n||'').toLowerCase()] === '';
  const canon = n => aliasMap[String(n||'').toLowerCase()] || n;
  const combined = [...clientFunds, ...getCrmSchemeNames().filter(n=>!isDeleted(n)), ...FUND_NAME_LIST.filter(n=>!isDeleted(n)), ...getLearnedFundNames().filter(n=>!isDeleted(n))]
    .map(canon);
  const clientFundKeys = new Set(clientFunds.map(n=>canon(n).toLowerCase()));
  const seen=new Set();
  const allFunds=[];
  for(const n of combined){
    const key=n.toLowerCase();
    if(seen.has(key)) continue;
    seen.add(key);
    allFunds.push(n);
  }

  const qFiltered = q.length>=2 ? allFunds.filter(n=>n.toLowerCase().includes(q)) : allFunds;
  // With an empty query, cap the client's-own-funds view to a sane length;
  // once they start typing, the normal 15-match cap applies as before.
  const matches = qFiltered.slice(0, q.length>=2 ? 15 : Math.min(clientFunds.length, 15));

  // onmousedown preventDefault() on each result item, below, stops the browser's
  // default "mousedown blurs the currently focused element" behavior — without
  // it, clicking a result briefly blurs the input BEFORE the click/onclick
  // fires, and since this input re-runs searchFundName() on focus/blur-adjacent
  // events, the list could re-render (or another closer elsewhere could fire)
  // and wipe the results out from under the click, making it feel like the
  // list "closes before I can select anything".

  if(matches.length===0){
    out.innerHTML='<div style="padding:10px;color:var(--gray);font-size:.85rem">No match found — type it and it\'ll be remembered for next time</div>';
    out.style.display='block';
    return;
  }
  out.innerHTML=matches.map(name=>{
    const isClientFund = clientFundKeys.has(name.toLowerCase());
    return `
    <div style="padding:8px 12px;cursor:pointer;border-bottom:1px solid #f0f0f0;font-size:.85rem;${isClientFund?'background:#f0fdf4':''}" onmouseover="this.style.background='#f7f7f7'" onmouseout="this.style.background='${isClientFund?'#f0fdf4':'#fff'}'" onmousedown="event.preventDefault()" onclick="selectFundName('${inputId}','${resultsId}', this.dataset.name)" data-name="${escapeHtml(name)}">
      ${isClientFund?'<span style="color:#16a34a;font-weight:700;margin-right:5px" title="Client already holds this">★</span>':''}${escapeHtml(name)}
    </div>`;
  }).join('');
  out.style.display='block';
}

function selectFundName(inputId, resultsId, name){
  const input=document.getElementById(inputId);
  if(input) input.value=name;
  const out=document.getElementById(resultsId);
  if(out){ out.style.display='none'; out.innerHTML=''; }
}

// Close fund-search dropdowns when clicking elsewhere
// ── Global badge tooltip (M/H/E badges) ──────────────────────────────────
// A CSS-only ::after tooltip gets silently clipped whenever the badge sits
// inside a scrolling container (tbl-scroll) — the tooltip box tries to render
// outside the container's bounds and the container's own overflow just cuts
// it off, so it never becomes visible. Rendering it as a position:fixed
// element appended straight to <body> sidesteps that entirely: it's
// positioned relative to the viewport, not any scrolling ancestor, so it can
// never be clipped no matter where in a table the badge is.
// Positioned off the mouse cursor (not the badge's own bounding rect) — for
// a badge in the FIRST couple of table rows, the badge sits close enough to
// the sticky top bar + table header that "flip below if too close to the
// viewport edge" never triggered (there was room before the viewport edge,
// just not before the header), so the tooltip rendered overlapping the
// header. A generous 60px top margin (not 4px) avoids that regardless of
// exactly how tall the sticky header is.
let _badgeTipEl = null, _badgeTipArrowEl = null;
document.addEventListener('mouseover', e=>{
  const t = e.target.closest && e.target.closest('.badge-tip');
  if(!t || !t.dataset.tip) return;
  if(_badgeTipEl) _badgeTipEl.remove();
  if(_badgeTipArrowEl) _badgeTipArrowEl.remove();
  const tip = document.createElement('div');
  tip.textContent = t.dataset.tip;
  tip.style.cssText = 'position:fixed;background:#111827;color:#fff;border:1.5px solid #dc2626;'
    + 'padding:5px 9px;border-radius:6px;font-size:.72rem;font-weight:700;white-space:nowrap;'
    + 'z-index:99999;box-shadow:0 4px 12px rgba(0,0,0,.35);pointer-events:none;';
  // Appended to <html> (documentElement), NOT <body> — the app's own zoom
  // control (dnAppZoom, see dninvest-app-2.js) sets `document.body.style.zoom`
  // to fit more columns on screen. Chrome's `zoom` CSS property applies to
  // everything inside body INCLUDING position:fixed children — so a tooltip
  // appended to body gets its top/left values reinterpreted in the zoomed
  // coordinate space, while mouse coordinates (e.clientX/clientY) stay in
  // real, unzoomed viewport pixels. That mismatch is exactly why the tooltip
  // kept landing far from the cursor at zoom levels other than 100%. <html>
  // itself is never zoomed, so anything appended there positions correctly
  // regardless of the current zoom level.
  document.documentElement.appendChild(tip);
  _badgeTipEl = tip;
  const arrow = document.createElement('div');
  arrow.style.cssText = 'position:fixed;width:0;height:0;border-left:5px solid transparent;'
    + 'border-right:5px solid transparent;z-index:99999;pointer-events:none;';
  document.documentElement.appendChild(arrow);
  _badgeTipArrowEl = arrow;

  const tr = tip.getBoundingClientRect();
  const mx = e.clientX, my = e.clientY;
  const gap = 12;
  let top = my - tr.height - gap;
  let flipped = false;
  if(top < 60){ top = my + gap + 6; flipped = true; } // 60px margin clears the sticky topbar/header, not just the viewport edge
  let left = mx - tr.width/2;
  if(left < 4) left = 4;
  if(left + tr.width > window.innerWidth - 4) left = window.innerWidth - 4 - tr.width;
  tip.style.top = top+'px';
  tip.style.left = left+'px';

  const arrowLeft = Math.max(left+6, Math.min(mx-5, left+tr.width-16));
  if(!flipped){
    arrow.style.top = (top+tr.height)+'px';
    arrow.style.left = arrowLeft+'px';
    arrow.style.borderTop = '5px solid #dc2626';
  } else {
    arrow.style.top = (top-5)+'px';
    arrow.style.left = arrowLeft+'px';
    arrow.style.borderBottom = '5px solid #dc2626';
  }
});
document.addEventListener('mouseout', e=>{
  const t = e.target.closest && e.target.closest('.badge-tip');
  if(!t) return;
  if(_badgeTipEl){ _badgeTipEl.remove(); _badgeTipEl=null; }
  if(_badgeTipArrowEl){ _badgeTipArrowEl.remove(); _badgeTipArrowEl=null; }
});

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

async function deleteMfTxnEntry(id){
  if(CU.role!=='admin') return;
  if(!(await dangerConfirm('Delete this transaction entry? This cannot be undone.'))) return;
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

async function deleteEqDematEntry(id){
  if(CU.role!=='admin') return;
  if(!(await dangerConfirm('Delete this Demat account entry? This cannot be undone.'))) return;
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
  // "No-Call Alert" = client has NEVER been called, full stop — clients who
  // were called at some point (even 60+/90+/180+ days ago) no longer belong
  // here; that's follow-up/re-engagement territory, not "never contacted".
  // Within "never called", the 60+/90+/180+ tabs now tier by how long the
  // client has existed in the CRM without ever getting a first call (using
  // c.created), so the tabs still mean something instead of all three
  // showing the identical "never called" list.
  let data = clients.filter(c=>{
    if(c.do_not_call) return false; // exclude DNC clients
    if(daysDiff(c.last_call_date)!==null) return false; // has a real last-call date — was called before, not a "never called" case
    const sinceAdded = daysDiff(c.created);
    return sinceAdded===null || sinceAdded>=days;
  }).map(c=>({...c,daysAgo:daysDiff(c.created)}))
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
    cont.innerHTML=`<div style="text-align:center;padding:48px;color:var(--green)">✅ No ${seg==='equity'?'clients':'investors'} added ${days}+ days ago that are still never-called${q?' matching "'+q+'"':''}</div>`;
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
    last_call_date:{get:c=>c.created, type:'date'},
    daysAgo:{get:c=>c.daysAgo, type:'num'},
    next_call:{get:c=>c.next_call, type:'date'},
  });
  let h=`<p style="margin-bottom:12px;color:var(--gray);font-size:.82rem">${data.length} ${label} never called, added ${days}+ days ago</p>
    <div class="tbl-wrap"><div class="tbl-scroll"><table><thead><tr>
    ${sortTh(idLabel,ncKey,'idval','str',`()=>renderNoCall('${seg}')`)}
    ${sortTh('Name',ncKey,'name','str',`()=>renderNoCall('${seg}')`)}
    ${sortTh('Mobile',ncKey,'mobile','str',`()=>renderNoCall('${seg}')`)}
    ${sortTh('RM',ncKey,'rm','str',`()=>renderNoCall('${seg}')`)}
    ${sortTh('Added On',ncKey,'last_call_date','date',`()=>renderNoCall('${seg}')`)}
    ${sortTh('Days Never-Called',ncKey,'daysAgo','num',`()=>renderNoCall('${seg}')`)}
    ${sortTh('Next Call',ncKey,'next_call','date',`()=>renderNoCall('${seg}')`)}
    <th>Actions</th>
    </tr></thead><tbody>`;
  data.forEach(c=>{
    const d=c.daysAgo;
    const cls = d===null||d>=180 ? 'row-alert' : d>=90 ? 'row-inactive' : '';
    const dayLabel = d===null ? '—' : d+'d';
    const dayColor = d===null||d>=180 ? 'var(--red)' : d>=90 ? 'var(--orange)' : 'var(--gold)';
    h+=`<tr class="${cls}">
      <td>${(seg==='equity'?c.code:c.pan)||'—'}</td>
      <td style="font-weight:600">${c.name}</td>
      <td><a href="tel:${c.mobile}" style="color:var(--navy);text-decoration:none">${c.mobile||'—'}</a></td>
      <td>${c.rm||'—'}</td>
      <td>${fmtDate(c.created)||'—'}</td>
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


// ── MF PROSPECTS — Equity clients not yet an MF investor. Cross-cutting,
// read-only view into the Equity list: sourced from ALL equity clients
// (unscoped by Equity RM — an MF RM has no eq_dealers), filtered down to
// "not already an MF investor" using the same PAN/mobile matching used for
// the M/E badges elsewhere. Nothing here ever writes to the equity record;
// "+ Add as MF Investor" only opens the normal Add Investor form, pre-filled,
// which creates a brand-new independent MF record on save.
function renderMfProspects(){
  if(!(CU.role==='admin' || CU.mf_prospects_access===true)){
    document.getElementById('mfp-table').innerHTML = `<div style="text-align:center;padding:48px;color:var(--gray)">🔒 You don't have access to this page. Ask Admin to grant MF Prospects access.</div>`;
    document.getElementById('mfp-pg').innerHTML='';
    document.getElementById('mfp-count').textContent='';
    return;
  }
  // Populate the Equity-RM filter dropdown once (cheap, small list) — every
  // Equity RM name, so Admin (or a restricted RM, within their allowed set)
  // can narrow the prospect list down to one Equity RM's book.
  const rmSel=document.getElementById('mfp-rm');
  if(rmSel && rmSel.options.length<=1){
    let rmNames = getSegRMs('equity');
    if(CU.role!=='admin' && Array.isArray(CU.mf_prospects_eq_rms) && CU.mf_prospects_eq_rms.length){
      const allowed=new Set(CU.mf_prospects_eq_rms.map(r=>r.trim().toUpperCase()));
      rmNames = rmNames.filter(r=>allowed.has(String(r).trim().toUpperCase()));
    }
    rmSel.innerHTML='<option value="">All Equity RMs</option>'+rmNames.map(r=>`<option value="${r}">${r}</option>`).join('');
  }

  const eqAll = DB.get('eq_clients')||[];
  const mfAll = DB.get('mf_clients')||[];
  const mfPanSet = new Set(mfAll.map(c=>String(c.pan||'').trim().toUpperCase()).filter(Boolean));
  const mfMobileSet = new Set(mfAll.map(c=>String(c.mobile||'').trim()).filter(Boolean));

  let data = eqAll.filter(c=>{
    if(c.do_not_call) return false;
    if(c.status==='Closed') return false;
    const isMf = mfPanSet.has(String(c.pan||'').trim().toUpperCase()) || mfMobileSet.has(String(c.mobile||'').trim());
    return !isMf;
  });

  // Admin can restrict a non-admin RM to only certain Equity RMs' clients
  // (mf_prospects_eq_rms). Empty/unset = no restriction (see everyone's).
  if(CU.role!=='admin' && Array.isArray(CU.mf_prospects_eq_rms) && CU.mf_prospects_eq_rms.length){
    const allowed = new Set(CU.mf_prospects_eq_rms.map(r=>r.trim().toUpperCase()));
    data = data.filter(c=>allowed.has(String(c.rm||'').trim().toUpperCase()));
  }

  const q=(document.getElementById('mfp-search')||{value:''}).value.trim().toLowerCase();
  if(q){ data=data.filter(c=>(c.name||'').toLowerCase().includes(q)||(c.mobile||'').includes(q)); }

  const rmFilter=(document.getElementById('mfp-rm')||{value:''}).value;
  if(rmFilter){ data=data.filter(c=>String(c.rm||'').trim().toUpperCase()===rmFilter.trim().toUpperCase()); }

  data = data.slice().sort((a,b)=>(a.name||'').localeCompare(b.name||''));

  const cont=document.getElementById('mfp-table');
  const nb=document.getElementById('nb-mfprospects'); if(nb) nb.textContent=data.length;
  document.getElementById('mfp-count').textContent = data.length+' prospects';

  if(!data.length){
    cont.innerHTML = `<div style="text-align:center;padding:48px;color:var(--green)">✅ No equity clients left to convert${q?' matching "'+q+'"':''}</div>`;
    document.getElementById('mfp-pg').innerHTML='';
    return;
  }

  const rows = data.slice((mfpPage-1)*PG_SIZE, mfpPage*PG_SIZE);
  let h=`<table><thead><tr>
    <th>Name</th><th>Mobile</th><th>Equity RM</th><th>Last Trade</th><th>Actions</th>
    </tr></thead><tbody>`;
  rows.forEach(c=>{
    h+=`<tr>
      <td style="font-weight:600">${c.name}</td>
      <td><a href="tel:${c.mobile}" style="color:var(--navy);text-decoration:none">${c.mobile||'—'}</a></td>
      <td>${c.rm||'—'}</td>
      <td>${fmtDate(c.last_trade_date)||'—'}</td>
      <td><button class="btn btn-outline" style="font-size:.72rem;padding:5px 10px" onclick="addAsMfInvestor('${c.id}')">➕ Add as MF Investor</button></td>
    </tr>`;
  });
  h+='</tbody></table>';
  cont.innerHTML=h;

  const pages=Math.ceil(data.length/PG_SIZE);
  const pg=document.getElementById('mfp-pg');
  if(pages<=1){ pg.innerHTML=''; }
  else{
    let ph=`<button class="pg-btn" onclick="gpMfp(${mfpPage-1})" ${mfpPage===1?'disabled':''}>‹</button>`;
    let rng=[];
    for(let i=1;i<=pages;i++){ if(i===1||i===pages||Math.abs(i-mfpPage)<=2) rng.push(i); else if(rng[rng.length-1]!=='...') rng.push('...'); }
    rng.forEach(r=>{ if(r==='...') ph+=`<span class="pg-info">…</span>`; else ph+=`<button class="pg-btn ${r===mfpPage?'active':''}" onclick="gpMfp(${r})">${r}</button>`; });
    ph+=`<button class="pg-btn" onclick="gpMfp(${mfpPage+1})" ${mfpPage===pages?'disabled':''}>›</button>`;
    ph+=`<span class="pg-info">${mfpPage}/${pages} (${data.length})</span>`;
    pg.innerHTML=ph;
  }
}
function filterMfp(){ mfpPage=1; renderMfProspects(); }
let _filterMfpT;
function filterMfpDebounced(){ clearTimeout(_filterMfpT); _filterMfpT=setTimeout(filterMfp,200); }
function gpMfp(p){ if(p<1) return; mfpPage=p; renderMfProspects(); }
function resetMfpFilters(){
  const s=document.getElementById('mfp-search'); if(s) s.value='';
  const r=document.getElementById('mfp-rm'); if(r) r.value='';
  mfpPage=1;
  renderMfProspects();
}

// Opens the normal "Add MF Investor" form, pre-filled from an equity client's
// details. currentEditId stays null, so Save creates a brand-new, independent
// MF record — the equity client (and its RM) is never modified.
function addAsMfInvestor(eqClientId){
  const eq = (DB.get('eq_clients')||[]).find(x=>x.id===eqClientId);
  if(!eq){ toast('Client not found','error'); return; }
  currentEditId=null;
  const pseudo = { name:eq.name, mobile:eq.mobile, pan:eq.pan, dob:eq.dob, email:eq.email, status:'Investor' };
  document.getElementById('clientModalTitle').textContent='Add MF Investor';
  document.getElementById('clientSaveBtn').textContent='Save Investor';
  document.getElementById('clientSaveBtn').dataset.seg='mf';
  document.getElementById('clientModalBody').innerHTML=clientForm('mf', pseudo);
  document.getElementById('clientModal').classList.add('open');
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
    'ANJALI':'#475569',  // slate
    'SHYAM':'#C2410C'    // burnt orange — added 20-Aug-2026, user reported his
                          // name wasn't getting its own highlight color like the
                          // original 9 RMs (he joined later, 17-Aug-2026, and was
                          // never added to this map, so he fell back to the
                          // generic hashed palette instead of a dedicated shade)
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

async function deleteBusinessEntry(id){
  if(CU.role!=='admin') return;
  if(!(await dangerConfirm('Delete this business entry? This cannot be undone.'))) return;
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
      tx.set(attRef, {data:DB._clean(latest), updated:new Date().toISOString()});
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
        <div class="user-role">@${u.username} · ${u.role==='admin'?'Admin':u.role==='mf_desk'?'MF Desk':u.role==='backoffice'?'Back Office':'RM'}${u.role==='rm'&&u.mf_desk_access?' <span style="color:var(--teal);font-weight:600">+ MF Desk access</span>':''}${u.role==='rm'&&u.risk_upload?' <span style="color:#d97706;font-weight:600">+ Risk/Square-off</span>':''}${u.role==='rm'&&u.backoffice_access?' <span style="color:#7c3aed;font-weight:600">+ Back Office</span>':''}${u.role==='rm'&&u.mf_prospects_access?' <span style="color:#2563eb;font-weight:600">+ MF Prospects</span>':''}</div>
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
    <div class="form-field" id="uf_mfprospects_wrap" style="${(!u||u.role==='rm')?'':'display:none'}">
      <label style="display:flex;align-items:center;gap:8px;text-transform:none;letter-spacing:0;font-size:.88rem;font-weight:600;cursor:pointer">
        <input type="checkbox" id="uf_mfprospects_access" onchange="document.getElementById('uf_mfprospects_rms_wrap').style.display=this.checked?'':'none'" ${u?.mf_prospects_access?'checked':''}>
        Also give MF Prospects access (see Equity clients who aren't MF investors yet, and convert them)
      </label>
      <div id="uf_mfprospects_rms_wrap" style="display:${u?.mf_prospects_access?'':'none'};margin-top:8px;padding:10px 12px;background:var(--bg);border-radius:8px">
        <div style="font-size:.78rem;color:var(--gray);margin-bottom:6px">Which Equity RM's clients should show up here? (leave all unchecked = show every Equity RM's clients)</div>
        <div style="display:flex;flex-wrap:wrap;gap:10px">
          ${getSegRMs('equity').map(rmName=>`
            <label style="display:flex;align-items:center;gap:5px;font-size:.8rem;text-transform:none;letter-spacing:0;cursor:pointer">
              <input type="checkbox" class="uf-mfp-eqrm" value="${rmName}" ${(u?.mf_prospects_eq_rms||[]).includes(rmName)?'checked':''}> ${rmName}
            </label>`).join('')}
        </div>
      </div>
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
  const mfProspectsAccess = role==='rm' && !!document.getElementById('uf_mfprospects_access')?.checked;
  const mfProspectsEqRms = mfProspectsAccess ? Array.from(document.querySelectorAll('.uf-mfp-eqrm:checked')).map(x=>x.value) : [];
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
        users[idx]={...users[idx],name,role,segments:segs,mobile:mob,email:eml,mf_desk_access:mfDeskAccess,risk_upload:riskUpload,backoffice_access:backofficeAccess,mf_prospects_access:mfProspectsAccess,mf_prospects_eq_rms:mfProspectsEqRms};
        if(pwd) users[idx].password=pwd;
        if(role==='rm' && pinVal) users[idx].pin=pinVal;
      }
    } else {
      if(users.find(u=>u.username===uname)){ toast('Username already exists','error'); return false; }
      const mob2=document.getElementById('uf_mobile').value.trim();
      const eml2=document.getElementById('uf_email').value.trim();
      const newUser={id:uid(),username:uname,password:pwd,name,role,segments:segs,mobile:mob2,email:eml2,active:true,mf_desk_access:mfDeskAccess,risk_upload:riskUpload,backoffice_access:backofficeAccess,mf_prospects_access:mfProspectsAccess,mf_prospects_eq_rms:mfProspectsEqRms};
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
          tx.set(attDocRef, {data:DB._clean(latest), updated:new Date().toISOString()});
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

async function deleteOffer(id){
  if(!(await dangerConfirm('Delete this offer? Its record will remain safely in History below.'))) return;
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

async function deleteSelectedHistory(){
  if(!CU || CU.role!=='admin'){ toast('Admin only','error'); return; }
  const ids=Array.from(document.querySelectorAll('.ch-chk:checked')).map(c=>c.value);
  if(!ids.length){ toast('Select at least one record','error'); return; }
  if(!(await dangerConfirm(ids.length+' history record(s) will be PERMANENTLY deleted from history. This cannot be undone. Continue?'))) return;
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
  // Request browser notification permission on first check. Was RM-only
  // before 20-Aug-2026 — Admin's browser never even asked for permission,
  // so an incoming RM message could only ever show the in-page popup
  // (invisible if Admin has the CRM tab in the background, which is the
  // common case — see showAdminMsgNotif below). Now requested for everyone.
  requestNotifPermission();
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
  // OS-level browser notification (20-Aug-2026 fix) — works even if the CRM
  // tab is in the background/minimized. Before this, an incoming RM message
  // only showed an in-page popup, which Admin never saw if focused on
  // another tab — RM would send a message and Admin had no way of knowing
  // until told separately (e.g. on call/WhatsApp).
  sendBrowserNotif('✉️ ' + (msg.rmName||'RM') + ' sent a message', msg.text ? msg.text.substring(0,100) : '');
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
// Big red confirmation modal for genuinely destructive delete actions
// (24-Aug-2026) — replaces the plain browser confirm() popup, which is easy
// to click through on autopilot, with a deliberately loud, hard-to-miss
// warning. Returns a Promise<boolean> — callers must `await` it. Used
// wherever a delete happens in the app, whether the person doing it is an
// RM or Admin.
function dangerConfirm(message, opts){
  opts = opts || {};
  return new Promise(resolve=>{
    const ov = document.createElement('div');
    ov.style.cssText = 'position:fixed;inset:0;z-index:100001;background:rgba(0,0,0,.55);display:flex;align-items:center;justify-content:center;padding:16px';
    ov.innerHTML = `
      <div style="background:#fff;border-radius:16px;max-width:420px;width:100%;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,.35)">
        <div style="background:linear-gradient(135deg,#dc2626,#991b1b);color:#fff;padding:20px 22px;display:flex;align-items:center;gap:12px">
          <div style="font-size:32px;line-height:1">⚠️</div>
          <div style="font-weight:800;font-size:1.1rem">${opts.title||'Confirm Delete'}</div>
        </div>
        <div style="padding:20px 22px;font-size:.92rem;color:#1f2937;line-height:1.5">${escapeHtml(message)}</div>
        <div style="display:flex;gap:10px;padding:16px 22px 22px;justify-content:flex-end">
          <button id="_dcCancel" style="padding:10px 18px;border:1.5px solid #e2e8f0;border-radius:9px;background:#fff;color:#374151;font-weight:700;cursor:pointer;font-size:.85rem">Cancel</button>
          <button id="_dcOk" style="padding:10px 18px;border:none;border-radius:9px;background:#dc2626;color:#fff;font-weight:800;cursor:pointer;font-size:.85rem">${opts.okLabel||'🗑️ Delete'}</button>
        </div>
      </div>`;
    document.body.appendChild(ov);
    const close = ok => { document.body.removeChild(ov); resolve(ok); };
    ov.querySelector('#_dcCancel').onclick = ()=>close(false);
    ov.querySelector('#_dcOk').onclick = ()=>close(true);
    ov.addEventListener('click', ev=>{ if(ev.target===ov) close(false); });
  });
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
    scheme:  ['schemename','scheme','fundname','fund'],
    folio:   ['foliono','folio','folionumber'],
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
  const nmKey = s => String(s||'').toUpperCase().replace(/\s+/g,' ').trim();

  const data = rows.slice(hdrIdx+1).filter(r=>{
    if(!r || !r.some(c=>c!=='' && c!=null)) return false;
    // Data rows carry a serial number; totals/footer rows don't.
    if(colMap.sno!==undefined) return String(r[colMap.sno]||'').trim().match(/^\d+$/);
    return String(at(r,'name')||'').trim() !== '';
  });

  const flatRows = data.map(r=>{
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
      client_id: normCid(at(r,'client_id')),
      scheme: String(at(r,'scheme')||'').trim(),
      folio:  String(at(r,'folio')||'').trim(),
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

  // ── GROUP multi-scheme rows into one entry per client ──────────────────
  // Some AUM reports ("AUM By Client", per-holding) give ONE ROW PER SCHEME,
  // so the same client appears on several consecutive rows (different Scheme
  // Name/Folio each time). The rest of the app (the CRM-merge step right
  // after this, and every mf_clients record) expects ONE entry per client
  // with their TOTAL aum/inv_amt/etc — without this grouping, only the first
  // scheme-row of a multi-scheme client actually reached the merge step
  // (the merge's own "claimed" guard silently dropped every OTHER scheme-row
  // of that same client into the ambiguous pile), so a client's imported AUM
  // silently reflected just ONE of their several holdings, not their total.
  // Reports with only one row per client (no scheme-name column at all) are
  // completely unaffected — each becomes its own single-row "group".
  const grouped = {};
  const order = [];
  const seenKeys = new Set();
  flatRows.forEach(r=>{
    const key = r.client_id ? ('CID:'+r.client_id) : (r.pan || nmKey(r.name));
    const isFirst = !seenKeys.has(key);
    if(isFirst){
      seenKeys.add(key);
      grouped[key] = {...r, aum_schemes:[], _maxSchemeAum: r.aum};
      order.push(key);
    } else {
      const g = grouped[key];
      // Second+ row for this client — accumulate totals, keep the first row's
      // "current snapshot" fields (avg_days/abs_rtn/xirr are ratios, not
      // additive — represented by whichever holding is largest, updated below).
      g.inv_amt   += r.inv_amt;
      g.aum       += r.aum;
      g.div_paid  += r.div_paid;
      g.div_reinv += r.div_reinv;
      g.gain_loss += r.gain_loss;
      if(!g.pan && r.pan) g.pan = r.pan;
      if(!g.rm && r.rm) g.rm = r.rm;
      if(r.aum > g._maxSchemeAum){
        g.avg_days = r.avg_days; g.abs_rtn = r.abs_rtn; g.xirr = r.xirr;
        g._maxSchemeAum = r.aum;
      }
    }
    if(r.scheme) grouped[key].aum_schemes.push({scheme:r.scheme, folio:r.folio, aum:r.aum});
  });
  const out = order.map(k=>{ const g=grouped[k]; delete g._maxSchemeAum; return g; });

  out._badPan = out.length - out.filter(r=>r.pan).length;
  // If the file never had a PAN column at all (as opposed to individual
  // blank/invalid PAN cells), say so plainly — that's a report-format fact,
  // not a per-row data-quality problem, and the old wording ("rows have no
  // valid PAN") read as an alarming data-quality warning either way.
  out._noPanColumn = colMap.pan===undefined;
  out._rowCount = flatRows.length;
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
    const clientId = cidMatch ? normCid(cidMatch[1]) : '';
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
    const multiScheme = parsed.reduce((s,r)=>s+(r.aum_schemes&&r.aum_schemes.length>1?1:0),0);
    let warn = '';
    if(parsed._noPanColumn){
      warn = `<div style="color:var(--gray,#666);font-size:.8rem;margin-top:4px">ℹ️ This report doesn't include a PAN column — clients will be matched by Client ID / Name instead, and PAN won't be changed.</div>`;
    } else if(parsed._badPan){
      warn = `<div style="color:var(--orange,#c60);font-size:.8rem;font-weight:600;margin-top:4px">⚠️ ${parsed._badPan} clients have no valid PAN — those clients' PAN will be left untouched</div>`;
    }
    const schemeNote = multiScheme ? `<div style="color:var(--gray,#666);font-size:.8rem;margin-top:4px">📋 ${multiScheme} client(s) hold multiple schemes — their AUM/Invested figures are summed across all of them (was: only their first scheme's number, before this fix).</div>` : '';
    document.getElementById('aum-preview').innerHTML = 
      `<div style="background:var(--green2);color:var(--green);padding:10px;border-radius:8px;font-size:.85rem;font-weight:600">
       ✅ ${parsed.length} clients found in AUM file${parsed._rowCount>parsed.length?` (from ${parsed._rowCount} scheme-level rows)`:''}</div>${warn}${schemeNote}`;
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
      const panNoteB = parsedB._noPanColumn ? ' (no PAN column in this report — matched by Client ID/Name)' : (parsedB._badPan?' (⚠️ '+parsedB._badPan+' without a valid PAN)':'');
      document.getElementById('both-preview').innerHTML += 
        `<div style="color:var(--green);font-size:.82rem;font-weight:600">✅ AUM: ${parsedB.length} clients${panNoteB}</div>`;
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
    if(c.client_id) byClientId[normCid(c.client_id)] = c;
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

      let ex = (row.client_id && byClientId[normCid(row.client_id)])
            || (panIsUnambiguous && existingMap[row.pan])
            || (row.mobile && byMobile[mob10(row.mobile)])
            || null;
      if(!ex){
        if(nameCount[upName] === 1 && fileNameCount[upName] === 1){
          ex = nameMap[upName];                       // unique on both sides — safe
        } else if(row.client_id){
          // This row carries its own Client ID — the RTA's authoritative key
          // — so a same-name collision (another person in the DB, or another
          // row in this same file, happens to share this exact name) is no
          // longer a real ambiguity: we know precisely which account this
          // row is, we just don't have it filed under this Client ID yet.
          // Falls through to "add as new" below instead of being silently
          // skipped. If this genuinely is the same person as one of the
          // same-named existing records (their Client ID was simply never
          // captured before), Merge Duplicates — which now treats Client ID
          // as authoritative, 20-Aug-2026 — will correctly fold the two
          // back together once both carry a Client ID to compare.
        } else if(nameCount[upName] > 1 || fileNameCount[upName] > 1){
          ambigRows.push(row); return;                // several people share this name, no Client ID to break the tie
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
        // Per-scheme breakdown (from a report that had a Scheme Name column) —
        // this is what feeds the "client already holds this fund" (★) hint in
        // the Fund Name autocomplete when adding a transaction for them.
        if(row.aum_schemes && row.aum_schemes.length) ex.aum_schemes = row.aum_schemes;
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
          aum_schemes: row.aum_schemes && row.aum_schemes.length ? row.aum_schemes : null,
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
      let ex = (row.client_id && byClientId[normCid(row.client_id)])
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
      const s = (c.client_id && sm['CID:'+normCid(c.client_id)])
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
  const validPan = c => { const p=String(c.pan||'').trim().toUpperCase(); return /^[A-Z]{5}[0-9]{4}[A-Z]$/.test(p) ? p : ''; };
  const cid      = c => String(c.client_id||'').trim();
  const mob      = c => String(c.mobile||'').replace(/\D/g,'').slice(-10);
  const nm       = c => String(c.name||'').toUpperCase().replace(/\s+/g,' ').trim();

  const parent = arr.map((_,i)=>i);
  const find = i => { while(parent[i]!==i){ parent[i]=parent[parent[i]]; i=parent[i]; } return i; };
  // safeUnion refuses to link two records whose Client ID OR PAN are BOTH
  // present and DIFFER — those are the two most authoritative identifiers
  // a real-world RTA record carries, so two different (present, valid)
  // values on either one is conclusive proof these are two different real
  // people, no matter what weaker signal (shared mobile / same name) is
  // trying to link them. Found 20-Aug-2026: a first version of this
  // function unioned on ANY shared key including mobile/name alone, which
  // grouped genuinely different family members sharing one household phone
  // number (e.g. "AKHILESH KUMAR" PAN AMSPM6908R + "POONAM KUMARI SAH" PAN
  // ARCPP8357Q, same mobile) as if they were the same duplicated client —
  // "Merge Selected" would have deleted one real person's entire investment
  // record. This guard makes that impossible.
  const conflicts = (i,j) => {
    const ci=cid(arr[i]), cj=cid(arr[j]);
    if(ci && cj && ci!==cj) return true;
    const pi=validPan(arr[i]), pj=validPan(arr[j]);
    if(pi && pj && pi!==pj) return true;
    return false;
  };
  const union = (i,j) => { if(conflicts(i,j)) return; const ri=find(i), rj=find(j); if(ri!==rj) parent[ri]=rj; };

  // Pass 1 — Client ID: the single most authoritative, RTA-assigned key.
  // Two records sharing a non-empty Client ID are certainly the same
  // investor account; union unconditionally (conflicts() only exists to
  // guard weaker signals below, not this one).
  const byCid = {};
  arr.forEach((c,i)=>{ const k=cid(c); if(!k) return; if(byCid[k]===undefined) byCid[k]=i; else { const ri=find(i),rj=find(byCid[k]); if(ri!==rj) parent[ri]=rj; } });
  // Pass 2 — valid PAN, same guard rule (safe: PAN vs PAN never conflicts
  // with itself, and Client ID mismatches are already blocked by conflicts()).
  const byPan = {};
  arr.forEach((c,i)=>{ const k=validPan(c); if(!k) return; if(byPan[k]===undefined) byPan[k]=i; else union(i, byPan[k]); });
  // Pass 3 — mobile AND exact name BOTH matching together. Mobile alone is
  // unsafe (household/family sharing one number); name alone is unsafe (two
  // unrelated people can share a common name). Requiring both at once is a
  // much stronger signal that this is genuinely the same person recorded
  // twice, and conflicts() still blocks it if either side's Client ID/PAN
  // proves otherwise.
  const byMobName = {};
  arr.forEach((c,i)=>{
    const m=mob(c), n=nm(c);
    if(m.length!==10 || !n) return;
    const k=m+'|'+n;
    if(byMobName[k]===undefined) byMobName[k]=i; else union(i, byMobName[k]);
  });

  const groups = {};
  arr.forEach((c,i)=>{ const r=find(i); (groups[r]=groups[r]||[]).push(c); });
  return Object.keys(groups).filter(k=>groups[k].length>1).map(k=>({key:'G'+k, recs:groups[k]}));
}
function _mfPrimary(recs){
  const score=c=>(
    (String(c.pan||'').match(/^[A-Z]{5}[0-9]{4}[A-Z]$/)?8:0)
    + (c.client_id?4:0)
    + (String(c.mobile||'').replace(/\D/g,'').length>=10?2:0)
    + (Number(c.aum)>0?1:0)
  );
  // Prefer whichever duplicate carries the MOST RECENT AUM By Client import
  // date (aum_detail.on) as the survivor — that's the record holding the
  // freshest Invested/Gain-Loss/XIRR figures AND, since 19-Aug-2026, the
  // freshest per-scheme Fund-wise Breakup (aum_schemes). Previously this
  // sorted on completeness-score first and earliest `created` as the
  // tiebreak — for two records with an otherwise-identical PAN/client_id/
  // mobile (a genuine same-client duplicate), that tiebreak kept the OLDER
  // record and silently discarded the newer one's fund-wise breakup. Falls
  // back to the completeness score, then earliest `created`, only when
  // neither record has an aum_detail.on date to compare.
  return recs.slice().sort((a,b)=>
    String((b.aum_detail&&b.aum_detail.on)||'').localeCompare(String((a.aum_detail&&a.aum_detail.on)||''))
    || score(b)-score(a)
    || String(a.created||'').localeCompare(String(b.created||''))
  )[0];
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
      // Groups can now be linked by more than one shared signal at once
      // (union-find, see findMfDupGroups) — show whichever signal(s) are
      // actually common across every record in this group, not a single
      // fixed label derived from the internal group key.
      const pans=new Set(g.recs.map(c=>String(c.pan||'').trim().toUpperCase()).filter(p=>/^[A-Z]{5}[0-9]{4}[A-Z]$/.test(p)));
      const cids=new Set(g.recs.map(c=>String(c.client_id||'').trim()).filter(Boolean));
      const kind = (cids.size===1 && g.recs.every(c=>String(c.client_id||'').trim())) ? 'Client ID'
                 : (pans.size===1 && g.recs.every(c=>/^[A-Z]{5}[0-9]{4}[A-Z]$/.test(String(c.pan||'').trim().toUpperCase()))) ? 'PAN'
                 : 'Mobile + Naam';
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
      ['pan','client_id','mobile','email','rm','remarks','followup_status','next_call','last_call_date','last_invest_date','sip_details','aum_detail','aum_schemes'].forEach(f=>{
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
  if(!(await dangerConfirm(groups.length+' group(s) will be merged, '+totalDel+' duplicate record(s) will be permanently deleted (their info will be added to the survivor). Proceed?'))) return;
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
  if(!(await dangerConfirm(ids.length+' equity client(s) will be permanently deleted. This cannot be undone. Proceed?'))) return;
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
  // ── Fund-wise (scheme-wise) AUM breakup ──
  // Populated during AUM import whenever the source report had a per-scheme
  // row (see aum_schemes in the import routine). Shown here as its own table
  // so RMs can see exactly which funds make up a client's total AUM, same as
  // the scheme-wise SIP breakup shown in showSipDetails().
  const schemes = Array.isArray(c.aum_schemes) ? c.aum_schemes.filter(x=>x && x.scheme) : [];
  let schemeHtml = '';
  if(schemes.length){
    const sorted = schemes.slice().sort((a,b)=>(parseFloat(b.aum)||0)-(parseFloat(a.aum)||0));
    const totalScheme = sorted.reduce((s,x)=>s+(parseFloat(x.aum)||0),0);
    schemeHtml = `<div style="margin-top:14px;font-weight:700;font-size:.88rem">Fund-wise Breakup (${sorted.length})</div>
      <div style="overflow:auto;margin-top:4px"><table class="tbl" style="width:100%;font-size:.85rem">
        <thead><tr>
          <th style="text-align:left">Scheme</th>
          <th style="text-align:right;white-space:nowrap">AUM</th>
        </tr></thead><tbody>`;
    sorted.forEach(x=>{
      schemeHtml += `<tr>
        <td style="text-align:left">${escapeHtml(x.scheme||'—')}${x.folio?`<div style="font-size:.7rem;opacity:.6">Folio: ${escapeHtml(String(x.folio))}</div>`:''}</td>
        <td style="text-align:right;font-weight:700;white-space:nowrap">₹${fmtNum(x.aum||0)}</td>
      </tr>`;
    });
    schemeHtml += `</tbody><tfoot><tr style="font-weight:800;border-top:2px solid var(--teal,#0d9488)">
        <td style="text-align:left">TOTAL (${sorted.length})</td>
        <td style="text-align:right;white-space:nowrap">₹${fmtNum(totalScheme)}</td>
      </tr></tfoot></table></div>`;
  }
  if(!d){
    body.innerHTML = hdr + `${row('AUM', c.aum?'<b>₹'+fmtNum(c.aum)+'</b>':'—')}
      <div style="margin-top:12px;color:var(--gray);font-size:.84rem">For more detail (Invested, Gain/Loss, XIRR), upload the AUM By Client report via MF → Import Excel.</div>` + schemeHtml;
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
      `<div style="margin-top:10px;font-size:.72rem;color:#999">As per last uploaded AUM By Client report${d.on?' • '+fmtDate(d.on):''}</div>` +
      schemeHtml;
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
    const clientId = cidMatch ? normCid(cidMatch[1]) : '';
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
    if(c.client_id) byClientId[normCid(c.client_id)] = c;
    const p = String(c.pan||'').trim().toUpperCase();
    if(p && !c.is_minor) byPan[p] = c;   // minors can carry a guardian's PAN — never match on it
  });

  let updated=0, unchanged=0;
  const touched=[], newLogs=[], notFoundRows=[];

  mfBulkDobData.forEach(row=>{
    const ex = (row.client_id && byClientId[normCid(row.client_id)]) || (row.pan && byPan[row.pan]) || null;
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

