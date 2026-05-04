
const API='';
let currentPage='dashboard',tourSteps=[],tourIdx=0,graphNetwork=null,tickerEvents=[];
let sessionTimer=300,sessionInterval=null,idleTimeout=null;
let undoTimers={};
let piiRevealed=false,piiCountdown=null;

// ═══════════════════════════════════════════════════════════════
//  Toast System
// ═══════════════════════════════════════════════════════════════
function showToast(msg,type='info',duration=3000){
  const c=document.getElementById('toast-container');
  const t=document.createElement('div');t.className=`toast ${type}`;t.innerHTML=msg;
  c.appendChild(t);setTimeout(()=>t.remove(),duration);
}

// ═══════════════════════════════════════════════════════════════
//  RBAC
// ═══════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════
//  Session Timeout
// ═══════════════════════════════════════════════════════════════
function resetSession(){
  sessionTimer=300;
  document.getElementById('session-lock').classList.remove('show');
}
function startSessionTimer(){
  if(sessionInterval)clearInterval(sessionInterval);
  sessionInterval=setInterval(()=>{
    sessionTimer--;
    const m=Math.floor(sessionTimer/60),s=sessionTimer%60;
    document.getElementById('session-timer').textContent=`Session: ${m}:${s.toString().padStart(2,'0')}`;
    if(sessionTimer<=0){
      document.getElementById('session-lock').classList.add('show');
      clearInterval(sessionInterval);
    }
  },1000);
}
document.addEventListener('click',()=>{resetSession()});
document.addEventListener('keydown',()=>{resetSession()});
document.addEventListener('mousemove',()=>{if(idleTimeout)clearTimeout(idleTimeout);idleTimeout=setTimeout(()=>{},1000)});
document.getElementById('session-lock').addEventListener('click',()=>{resetSession();startSessionTimer()});
startSessionTimer();

// ═══════════════════════════════════════════════════════════════
//  Navigation
// ═══════════════════════════════════════════════════════════════
document.querySelectorAll('.nav-item').forEach(el=>{
  el.addEventListener('click',()=>{
    if(el.classList.contains('locked'))return;
    const p=el.dataset.page;navigateTo(p);
  });
});

function navigateTo(p){
  currentPage=p;
  document.querySelectorAll('.nav-item').forEach(n=>n.classList.toggle('active',n.dataset.page===p));
  document.querySelectorAll('.page').forEach(pg=>pg.classList.toggle('active',pg.id==='page-'+p));
  // Log access
  fetch('/api/access-logs',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({role:'Admin',action:'page_view',entity:p})});
  if(p==='dashboard')loadDashboard();
  else if(p==='clusters')loadClusters();
  else if(p==='evidence')loadEvidence();
  else if(p==='pii')loadPII();
  else if(p==='abi')loadActivity();
  else if(p==='query')loadQuery();
  else if(p==='reviews')loadReviews();
  else if(p==='graph')loadGraph();
  else if(p==='health')loadHealth();
  else if(p==='history')loadHistory();
  else if(p==='calibration')loadCalibration();
  else if(p==='access_log')loadAccessLog();
}

// ═══════════════════════════════════════════════════════════════
//  Update Review Badge Helper
// ═══════════════════════════════════════════════════════════════
function updateReviewBadge() {
  const badge = document.getElementById('review-badge');
  if(!badge) return;
  fetch('/api/stats').then(r=>r.json()).then(s=>{
    if(s.pending_reviews > 0){
      badge.style.display = 'inline-block';
      badge.textContent = s.pending_reviews;
    }else{
      badge.style.display = 'none';
    }
  }).catch(e=>console.error('Badge update error:', e));
}

// ═══════════════════════════════════════════════════════════════
//  SSE Live Ticker
// ═══════════════════════════════════════════════════════════════
let evtSource=null;
let sseRetries=0;
const maxSSERetries=5;
const sseDelays=[1000,2000,4000,8000,16000];

function connectSSE(){
  if(evtSource)evtSource.close();
  evtSource=new EventSource('/api/events');
  sseRetries=0;
  evtSource.onmessage=e=>{try{const data=JSON.parse(e.data);addTickerEvent(data);sseRetries=0;}catch(x){}};
  ['pipeline','ingest','tokenize','blocking','auto_link','review','safety','reject','abi','complete','error','review_decided','review_undone','sync','config'].forEach(t=>{
    evtSource.addEventListener(t,e=>{try{addTickerEvent(JSON.parse(e.data))}catch(x){}});
  });
  evtSource.onerror=(e)=>{logger.warn(`SSE error - retry ${sseRetries+1}/${maxSSERetries}`);if(sseRetries<maxSSERetries){const delay=sseDelays[sseRetries]||16000;sseRetries++;setTimeout(connectSSE,delay);}else{showToast('📶 Connection lost. Click Dashboard to refresh.','warning');}};
}

function addTickerEvent(ev){
  tickerEvents.push(ev);
  if(tickerEvents.length>30)tickerEvents.shift();
  const bar=document.getElementById('ticker');bar.classList.remove('empty');
  const inner=document.getElementById('ticker-inner');
  inner.innerHTML=tickerEvents.slice(-15).map(e=>{
    const c=e.type==='auto_link'?'auto':e.type==='review'||e.type==='review_decided'?'review':e.type==='safety'||e.type==='error'||e.type==='reject'?'safety':'info';
    return `<span class="ticker-chip ${c}">${e.message}</span>`;
  }).join('');
  if(ev.type==='complete'||ev.type==='error'){
    setTimeout(()=>{loadDashboard();updateReviewBadge()},500);
    document.getElementById('btn-run').disabled=false;
    document.getElementById('btn-run').textContent='⚡ Run Pipeline';
    document.getElementById('processing-status').innerHTML='';
  }
  if(ev.type==='review_decided'){
    setTimeout(()=>{if(currentPage==='dashboard')loadDashboard();updateReviewBadge()},300);
  }
}
connectSSE();

// ═══════════════════════════════════════════════════════════════
//  Pipeline
// ═══════════════════════════════════════════════════════════════
async function runPipeline(){
  try{
    console.log('🚀 runPipeline called. null:', null);
    
    const btn=document.getElementById('btn-run');
    const statusDiv=document.getElementById('processing-status');
    if(!btn){console.error('Button not found');return}
    btn.disabled=true;btn.textContent='⏳ Running...';
    statusDiv.innerHTML='<div class="processing-indicator"><span class="dot"></span>Pipeline running (60s timeout)</div>';
    const timeoutMs=60000;
    let timeoutHandle=null;
    console.log('📡 Sending POST request to /api/pipeline/run');
    const timeoutPromise=new Promise((_,reject)=>{timeoutHandle=setTimeout(()=>{reject(new Error('Pipeline timeout (60s)'))},timeoutMs);});
    const fetchPromise=fetch('/api/pipeline/run',{method:'POST',headers:{'Content-Type':'application/json'}});
    const response=await Promise.race([fetchPromise,timeoutPromise]);
    console.log('✅ Response received:', response.status, response.statusText);
    if(!response.ok)throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    clearTimeout(timeoutHandle);
    console.log('⏱️ Waiting 2s for pipeline to start processing...');
    setTimeout(()=>{loadDashboard();updateReviewBadge();},2000);
    showToast('⚡ Pipeline started — processing in background','success');
  }catch(e){
    console.error('❌ Pipeline error:', e);
    const btn=document.getElementById('btn-run');
    const statusDiv=document.getElementById('processing-status');
    if(btn){btn.disabled=false;btn.textContent='⚡ Run Pipeline'}
    if(statusDiv){statusDiv.innerHTML=''}
    showToast(`❌ ${e.message}`,'error');
  }
}

// ═══════════════════════════════════════════════════════════════
//  Dashboard
// ═══════════════════════════════════════════════════════════════
async function loadDashboard(){
  try{
    const s=await(await fetch('/api/stats')).json();
    const sp=await(await fetch('/api/sparklines')).json();
    const sg=document.getElementById('stats-grid');
    sg.innerHTML=[
      statCard('UBIDs','🆔',s.total_ubids,s.delta_ubids,'--accent'),
      statCard('Records','📄',s.total_records,0,'--cyan'),
      statCard('Departments','🏛️',s.departments,0,'--purple'),
      statCard('Auto-Linked','✅',s.auto_linked,0,'--green'),
      statCard('In Review','⚠️',s.pending_reviews,0,'--yellow'),
      statCard('Safety Blocked','🛡️',s.blocked_by_safety,0,'--red'),
      statCard('Avg Confidence','📊',(s.avg_confidence*100).toFixed(1)+'%',0,'--accent'),
      statCard('Rollbacks','↩',s.rollback_count||0,0,'--yellow'),
    ].join('');
    // ABI grid with sparklines
    const ag=document.getElementById('abi-grid');
    ag.innerHTML=`
      <div class="abi-tile abi-active card" onclick="navigateTo('abi');document.getElementById('abi-status-filter').value='ACTIVE'"><div class="count">${s.active_count}</div><div class="abi-label">Active</div><div class="abi-delta">${deltaStr(s.delta_active)}</div><div class="sparkline-mini">${miniSparkline(sp.active,'#34d399')}</div></div>
      <div class="abi-tile abi-dormant card" onclick="navigateTo('abi');document.getElementById('abi-status-filter').value='DORMANT'"><div class="count">${s.dormant_count}</div><div class="abi-label">Dormant</div><div class="abi-delta">${deltaStr(s.delta_dormant)}</div><div class="sparkline-mini">${miniSparkline(sp.dormant,'#fbbf24')}</div></div>
      <div class="abi-tile abi-closed card" onclick="navigateTo('abi');document.getElementById('abi-status-filter').value='CLOSED'"><div class="count">${s.closed_count}</div><div class="abi-label">Closed</div><div class="abi-delta">${deltaStr(s.delta_closed)}</div><div class="sparkline-mini">${miniSparkline(sp.closed,'#f87171')}</div></div>
      <div class="abi-tile abi-uncertain card" onclick="navigateTo('abi');document.getElementById('abi-status-filter').value='UNCERTAIN'"><div class="count">${s.uncertain_count}</div><div class="abi-label">Uncertain</div><div class="abi-delta">${deltaStr(s.delta_uncertain)}</div><div class="sparkline-mini">${miniSparkline(sp.uncertain,'#a78bfa')}</div></div>`;
    // Donut
    const total=s.active_count+s.dormant_count+s.closed_count+s.uncertain_count;
    if(total>0)document.getElementById('donut-wrap').innerHTML=renderDonut(s.active_count,s.dormant_count,s.closed_count,s.uncertain_count);
    loadHeatmap();
    // Alerts
    const alerts=await(await fetch('/api/alerts')).json();
    document.getElementById('alerts-list').innerHTML=alerts.length?alerts.slice(0,6).map(a=>`<div class="alert-item ${a.severity}"><div class="alert-dot"></div><div class="alert-body"><h5>${a.title}</h5><p>${a.description}</p></div><button class="btn btn-sm" onclick="ackAlert('${a.alert_id}',this)" ${a.acknowledged?'disabled':''}>${a.acknowledged?'✓':'Ack'}</button></div>`).join(''):'<div style="color:var(--muted);font-size:13px;padding:10px">No alerts yet</div>';
    updateReviewBadge();
  }catch(e){console.error(e)}
}

function miniSparkline(data,color){
  if(!data||!data.length)return '';
  const w=80,h=20,max=Math.max(...data,1),min=Math.min(...data,0);
  const pts=data.map((v,i)=>`${(i/(data.length-1||1))*w},${h-((v-min)/(max-min||1))*h}`).join(' ');
  return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}"><polyline points="${pts}" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" opacity="0.7"/></svg>`;
}

function statCard(label,icon,val,delta,color){
  const d=delta>0?`<div class="delta up">+${delta} vs last</div>`:delta<0?`<div class="delta down">${delta} vs last</div>`:'';
  return `<div class="stat-card card"><div class="label">${icon} ${label}</div><div class="value" style="color:var(${color})">${val}</div>${d}</div>`;
}
function deltaStr(d){return d>0?`+${d} vs last`:d<0?`${d} vs last`:'—'}

function renderDonut(a,d,c,u){
  const t=a+d+c+u;if(!t)return'';
  const pcts=[a/t,d/t,c/t,u/t],colors=['#34d399','#fbbf24','#f87171','#a78bfa'],labels=['Active','Dormant','Closed','Uncertain'],vals=[a,d,c,u];
  let offset=0,paths='';
  pcts.forEach((p,i)=>{
    if(p>0){const dash=p*251.2,gap=251.2-dash;paths+=`<circle cx="60" cy="60" r="40" fill="none" stroke="${colors[i]}" stroke-width="18" stroke-dasharray="${dash} ${gap}" stroke-dashoffset="${-offset}" opacity="0.85"/>`;offset+=dash;}
  });
  const legend=labels.map((l,i)=>`<div class="donut-legend-item"><span class="dot" style="background:${colors[i]}"></span>${l}: <strong>${vals[i]}</strong> (${(pcts[i]*100).toFixed(0)}%)</div>`).join('');
  return `<svg class="donut-svg" viewBox="0 0 120 120">${paths}<text x="60" y="56" text-anchor="middle" fill="white" font-size="18" font-weight="800">${t}</text><text x="60" y="70" text-anchor="middle" fill="#94a3b8" font-size="8" font-weight="600">TOTAL</text></svg><div class="donut-legend">${legend}</div>`;
}

// ═══════════════════════════════════════════════════════════════
//  Karnataka District Heatmap
// ═══════════════════════════════════════════════════════════════
async function loadHeatmap(){
  try{
    const districts=await(await fetch('/api/districts')).json();
    if(!districts.length||!districts.some(d=>d.total>0)){document.getElementById('heatmap-wrap').innerHTML='<div style="color:var(--muted);font-size:13px;text-align:center;padding:20px">Run pipeline to see district data</div>';return}
    const grid=Array.from({length:8},()=>Array(6).fill(null));
    districts.forEach(d=>{if(d.row>=0&&d.row<8&&d.col>=0&&d.col<6){if(!grid[d.row][d.col])grid[d.row][d.col]=d;else{for(let dr=-1;dr<=1;dr++)for(let dc=-1;dc<=1;dc++){const nr=d.row+dr,nc=d.col+dc;if(nr>=0&&nr<8&&nc>=0&&nc<6&&!grid[nr][nc]){grid[nr][nc]=d;return}}}}});
    let html='<div class="heatmap-grid" style="grid-template-columns:repeat(6,1fr);grid-template-rows:repeat(8,1fr)">';
    for(let r=0;r<8;r++){for(let c=0;c<6;c++){const d=grid[r][c];
      if(d){
        const intensity=d.total>0?Math.min(1,d.total/4):0;
        const bg=d.active>=d.dormant&&d.active>=d.closed&&d.active>=d.uncertain?`rgba(52,211,153,${0.12+intensity*0.5})`:d.dormant>=d.active&&d.dormant>=d.closed?`rgba(251,191,36,${0.12+intensity*0.5})`:d.closed>=d.active&&d.closed>=d.dormant?`rgba(248,113,113,${0.12+intensity*0.5})`:`rgba(167,139,250,${0.12+intensity*0.5})`;
        const bc=d.total>0?(d.active>=d.dormant?'rgba(52,211,153,0.5)':d.dormant>=d.active?'rgba(251,191,36,0.5)':'rgba(248,113,113,0.5)'):'transparent';
        html+=`<div class="heatmap-cell" style="background:${bg};border-color:${bc}" onclick="heatmapDrill('${d.name}')"><div class="hm-name">${d.name.length>12?d.code:d.name}</div><div class="hm-count">${d.total||'·'}</div>`;
        if(d.total>0){html+=`<div class="hm-tip"><strong style="font-size:13px;display:block;margin-bottom:6px">${d.name}</strong><div class="hm-tip-row"><span><span class="hm-dot" style="background:var(--green)"></span>Active</span><strong>${d.active}</strong></div><div class="hm-tip-row"><span><span class="hm-dot" style="background:var(--yellow)"></span>Dormant</span><strong>${d.dormant}</strong></div><div class="hm-tip-row"><span><span class="hm-dot" style="background:var(--red)"></span>Closed</span><strong>${d.closed}</strong></div><div class="hm-tip-row"><span><span class="hm-dot" style="background:var(--purple)"></span>Uncertain</span><strong>${d.uncertain}</strong></div><div style="margin-top:6px;padding-top:6px;border-top:1px solid var(--border);font-size:10px;color:var(--accent)">Click to filter →</div></div>`}
        html+=`</div>`;
      }else{html+=`<div class="heatmap-cell" style="opacity:0.15;background:var(--glass)"><div class="hm-name" style="opacity:0.3">·</div></div>`}
    }}
    html+='</div><div class="heatmap-legend"><span><span class="hl-swatch" style="background:rgba(52,211,153,0.6)"></span>Active</span><span><span class="hl-swatch" style="background:rgba(251,191,36,0.6)"></span>Dormant</span><span><span class="hl-swatch" style="background:rgba(248,113,113,0.6)"></span>Closed</span><span><span class="hl-swatch" style="background:rgba(167,139,250,0.6)"></span>Uncertain</span></div>';
    document.getElementById('heatmap-wrap').innerHTML=html;
  }catch(e){console.error('Heatmap error:',e)}
}
function heatmapDrill(districtName){
  navigateTo('abi');
  // Actually filter Activity Status page by district
  setTimeout(()=>{
    document.getElementById('abi-status-filter').value='';
    loadActivity(districtName);
  },100);
}

async function ackAlert(id,btn){await fetch(`/api/alerts/${id}/ack`,{method:'POST'});btn.disabled=true;btn.textContent='✓'}

async function updateReviewBadge(){
  try{const r=await(await fetch('/api/reviews?status=pending')).json();const b=document.getElementById('review-badge');if(r.length>0){b.style.display='inline';b.textContent=r.length}else{b.style.display='none'}}catch(e){}
}

// ═══════════════════════════════════════════════════════════════
//  Global Search + Ctrl+K
// ═══════════════════════════════════════════════════════════════
document.addEventListener('keydown',e=>{
  if((e.ctrlKey||e.metaKey)&&e.key==='k'){e.preventDefault();openSearchOverlay()}
  if(e.key==='/'&&!['INPUT','TEXTAREA','SELECT'].includes(document.activeElement.tagName)){e.preventDefault();openSearchOverlay()}
  if(e.key==='Escape')closeSearchOverlay();
});
function openSearchOverlay(){document.getElementById('search-overlay').classList.add('show');document.getElementById('cmd-search-input').value='';document.getElementById('cmd-search-input').focus();document.getElementById('cmd-search-results').innerHTML=''}
function closeSearchOverlay(){document.getElementById('search-overlay').classList.remove('show')}
let cmdSearchTimeout;
async function cmdSearch(q){
  clearTimeout(cmdSearchTimeout);
  const box=document.getElementById('cmd-search-results');
  if(!q||q.length<2){box.innerHTML='<div style="color:var(--muted);font-size:12px;text-align:center;padding:20px">Type to search...</div>';return}
  cmdSearchTimeout=setTimeout(async()=>{
    const r=await(await fetch(`/api/search?q=${encodeURIComponent(q)}`)).json();
    if(r.results.length){
      box.innerHTML=r.results.map(x=>`<div style="display:flex;align-items:center;gap:12px;padding:10px 12px;border-bottom:1px solid var(--border);cursor:pointer" onclick="closeSearchOverlay();openCluster('${x.ubid}')"><div style="flex:1"><div style="font-weight:700;font-size:13px">${x.name}</div><div style="font-size:11px;color:var(--muted)">${x.ubid.slice(0,20)}… · ${x.reason}</div></div>${statusBadge(x.abi_status)}${riskBadge(x.risk)}</div>`).join('');
    }else box.innerHTML='<div style="color:var(--muted);font-size:12px;text-align:center;padding:20px">No results found</div>';
  },200);
}

let searchTimeout;
async function globalSearch(q){
  clearTimeout(searchTimeout);
  const box=document.getElementById('search-results');
  if(!q||q.length<2){box.style.display='none';return}
  searchTimeout=setTimeout(async()=>{
    const r=await(await fetch(`/api/search?q=${encodeURIComponent(q)}`)).json();
    if(r.results.length){
      box.style.display='block';
      box.innerHTML=`<div class="card"><div class="card-title">Search Results (${r.results.length})</div><table><tr><th>UBID</th><th>Name</th><th>Match</th><th>Status</th><th>Risk</th><th></th></tr>${r.results.map(x=>`<tr><td style="font-family:monospace;font-size:11px">${x.ubid.slice(0,16)}…</td><td>${x.name}</td><td><span class="badge badge-blue">${x.reason}</span></td><td>${statusBadge(x.abi_status)}</td><td>${riskBadge(x.risk)}</td><td><button class="btn btn-sm" onclick="openCluster('${x.ubid}')">View</button></td></tr>`).join('')}</table></div>`;
    }else{box.style.display='block';box.innerHTML='<div class="card"><div style="color:var(--muted);font-size:13px;padding:10px">No results found</div></div>'}
  },300);
}

// ═══════════════════════════════════════════════════════════════
//  Clusters
// ═══════════════════════════════════════════════════════════════
async function loadClusters(){
  const data=await(await fetch('/api/clusters')).json();
  const activity=await(await fetch('/api/activity')).json();
  const abiMap={};activity.forEach(a=>abiMap[a.ubid]=a);
  document.getElementById('clusters-table').innerHTML=`<div class="card"><table><tr><th>UBID</th><th>Udyam</th><th>Business Name</th><th>Records</th><th>Depts</th><th>Risk</th><th>ABI</th><th>Security</th><th>SyncBridge</th><th></th></tr>${data.map(c=>{
    const abi=abiMap[c.ubid];const udyam=c.department_records?.find(r=>r.udyam)?.udyam||'—';
    const secFlag=c.security_flag||'🟢 Clean';
    const secBadge=secFlag.includes('Flagged')?'badge-red':secFlag.includes('Watch')?'badge-yellow':'badge-green';
    return`<tr><td style="font-family:monospace;font-size:11px;cursor:pointer;color:var(--accent)" onclick="openCluster('${c.ubid}')">${c.ubid.slice(0,20)}…</td><td style="font-size:11px">${udyam}</td><td>${c.business_name}</td><td>${c.total_records}</td><td>${c.departments_covered}</td><td>${riskBadge(c.risk_score||0)}</td><td>${statusBadge(abi?.status||'UNKNOWN')}</td><td><span class="badge ${secBadge}">${secFlag}</span></td><td>${c.theme2?.ready?'<span class="badge badge-green">✓ Ready</span>':'<span class="badge badge-yellow">'+(c.theme2?.score||0)+'/5</span>'}</td><td><button class="btn btn-sm" onclick="openCluster('${c.ubid}')">Explore</button></td></tr>`}).join('')}</table></div>`;
}

async function openCluster(ubid){
  const c=await(await fetch(`/api/cluster-detail?ubid=${encodeURIComponent(ubid)}`)).json();
  // Log access
  fetch('/api/access-logs',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({role:'Admin',action:'entity_accessed',entity:ubid})});
  const ct=document.getElementById('cluster-detail-content');
  const abi=c.abi;
  let html=`<h3>${c.business_name}</h3><div style="font-family:monospace;font-size:11px;color:var(--muted);margin-bottom:16px">${c.ubid}</div>`;
  // Tabs
  html+=`<div class="tab-bar" id="modal-tabs"><button class="tab-btn active" onclick="showModalTab('classification')">ABI Classification</button><button class="tab-btn" onclick="showModalTab('risk')">Risk Breakdown</button><button class="tab-btn" onclick="showModalTab('records')">Dept Records</button><button class="tab-btn" onclick="showModalTab('evidence')">Link Evidence</button><button class="tab-btn" onclick="showModalTab('timeline')">Timeline</button></div>`;

  // Tab: Classification
  html+=`<div class="modal-tab" id="mtab-classification">`;
  if(abi){html+=`<div class="card" style="margin-bottom:12px"><div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap">${statusBadge(abi.status)} <span style="font-size:13px">Confidence: <strong>${(abi.confidence*100).toFixed(0)}%</strong></span><span style="font-size:12px;color:var(--muted)">${abi.rule_override}</span></div></div>`}
  if(abi?.signals?.length){html+=`<div class="card"><div class="card-title">Activity Signals</div>${abi.signals.map(s=>`<div style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid var(--border)"><span class="badge badge-blue">${deptLabel(s.dept)}</span><span style="font-size:12px;font-weight:600">${s.type}</span><span style="font-size:11px;color:var(--muted)">${s.date}</span><span style="font-size:11px;flex:1">${s.detail}</span></div>`).join('')}</div>`}
  html+=`</div>`;

  // Tab: Risk
  html+=`<div class="modal-tab" id="mtab-risk" style="display:none">`;
  if(c.risk_factors?.length){html+=`<div class="card"><div class="card-title">Risk Breakdown (Score: ${c.risk_score})</div>${c.risk_factors.map(f=>`<div style="display:flex;align-items:center;gap:10px;padding:6px 0;border-bottom:1px solid var(--border)"><span class="badge badge-${f.severity==='high'?'red':f.severity==='medium'?'yellow':'blue'}">${f.contribution}</span><span style="font-size:12px">${f.factor}</span></div>`).join('')}</div>`}
  if(c.security_flag){html+=`<div class="card" style="margin-top:12px"><div class="card-title">Security Assessment</div><div style="display:flex;align-items:center;gap:10px;padding:8px 0"><span style="font-size:18px">${c.security_flag}</span></div>${(c.security_reasons||[]).map(r=>`<div style="font-size:12px;color:var(--muted);padding:4px 0">• ${r}</div>`).join('')}</div>`}
  html+=`</div>`;

  // Tab: Records
  html+=`<div class="modal-tab" id="mtab-records" style="display:none"><div class="card"><div class="card-title">Department Records (${c.department_records?.length||0})</div><table><tr><th>Dept</th><th>Record ID</th><th>Name</th><th>PAN</th><th>Udyam</th><th>Status</th></tr>${(c.department_records||[]).map(r=>`<tr><td><span class="badge badge-blue">${deptLabel(r.department)}</span></td><td style="font-family:monospace;font-size:10px">${r.record_id}</td><td>${r.business_name}</td><td>${r.pan||'—'}</td><td style="font-size:11px">${r.udyam||'—'}</td><td>${r.status||'—'}</td></tr>`).join('')}</table></div></div>`;

  // Tab: Evidence
  html+=`<div class="modal-tab" id="mtab-evidence" style="display:none">`;
  if(c.evidence?.length){html+=c.evidence.map(e=>`<div class="card" style="margin-bottom:12px"><div style="display:flex;align-items:center;gap:8px;margin-bottom:6px"><strong style="font-size:13px">${e.record_a}</strong><span style="color:var(--muted)">↔</span><strong style="font-size:13px">${e.record_b}</strong>${decisionBadge(e.decision)}<span style="font-weight:700;color:var(--accent)">${(e.confidence*100).toFixed(0)}%</span></div><div class="xai-list">${(e.xai_factors||[]).map(x=>`<div class="xai-row"><span class="feat">${x.feature}</span><div class="bar-track"><div class="bar-val ${x.direction}" style="width:${Math.min(100,x.bar_pct)}%"></div></div><span class="val" style="color:${x.direction==='positive'?'var(--green)':'var(--red)'}">${x.contribution>0?'+':''}${x.contribution}</span></div>`).join('')}</div>${e.reviewer_notes?`<div style="margin-top:8px;padding:8px;background:rgba(52,211,153,0.1);border-radius:8px;font-size:11px">📝 Reviewer: "${e.reviewer_notes}" — ${e.reviewer||''}</div>`:''}</div>`).join('')}
  else{html+=`<div style="color:var(--muted);font-size:13px;padding:10px">No link evidence</div>`}
  html+=`</div>`;

  // Tab: Timeline
  html+=`<div class="modal-tab" id="mtab-timeline" style="display:none">`;
  if(c.timeline?.length){
    html+=`<div style="display:flex;justify-content:flex-end;margin-bottom:8px"><button class="btn btn-sm" onclick="exportTimeline()">📄 Export PDF</button></div>`;
    html+=`<div class="timeline-wall">`;
    html+=c.timeline.map(t=>{
      const actionColors={record_ingested:'var(--cyan)',auto_linked:'var(--green)',sent_to_review:'var(--yellow)',abi_classified:'var(--purple)',reviewer_merged:'var(--green)',reviewer_approved:'var(--green)',reviewer_separated:'var(--red)',reviewer_deferred:'var(--muted)',reviewer_undo:'var(--yellow)'};
      const actionIcons={record_ingested:'📥',auto_linked:'✅',sent_to_review:'⚠️',abi_classified:'📊',reviewer_merged:'👤✓',reviewer_approved:'👤✓',reviewer_separated:'👤✗',reviewer_deferred:'👤⏸',reviewer_undo:'↩'};
      const color=actionColors[t.action]||'var(--accent)';const icon=actionIcons[t.action]||t.icon||'⚙️';
      const actorClass=t.actor==='SYSTEM'?'actor-system':t.actor&&t.actor!=='SYSTEM'?'actor-reviewer':'';
      let e=`<div class="tl-entry tl-${t.action}"><div class="tl-time">${new Date(t.timestamp).toLocaleString()}</div><div class="tl-action" style="color:${color}">${icon} ${t.action.replace(/_/g,' ').toUpperCase()}</div><div class="tl-details">${t.details}</div>`;
      e+=`<div class="tl-actor ${actorClass}">by ${t.actor}</div>`;
      if(t.before&&t.after)e+=`<div class="tl-diff"><span class="before">${t.before}</span><span>→</span><span class="after">${t.after}</span></div>`;
      e+=`</div>`;return e;
    }).join('');
    html+=`</div>`;
  }else{html+=`<div style="color:var(--muted);font-size:13px;padding:10px">No timeline events yet</div>`}
  html+=`</div>`;

  ct.innerHTML=html;
  document.getElementById('cluster-modal').classList.add('show');
}
function showModalTab(tab){
  document.querySelectorAll('.modal-tab').forEach(t=>t.style.display='none');
  document.querySelectorAll('#modal-tabs .tab-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('mtab-'+tab).style.display='block';
  event.target.classList.add('active');
}
function exportTimeline(){window.print()}
function closeModal(){document.getElementById('cluster-modal').classList.remove('show')}

// ═══════════════════════════════════════════════════════════════
//  Evidence
// ═══════════════════════════════════════════════════════════════
async function loadEvidence(){
  const f=document.getElementById('evidence-filter').value;
  const data=await(await fetch('/api/evidence')).json();
  const filtered=f?data.filter(e=>e.decision===f):data;
  document.getElementById('evidence-list').innerHTML=filtered.length?filtered.map(e=>`<div class="card fade-in" style="margin-bottom:12px">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">
      <strong>${e.record_a}</strong><span style="color:var(--muted)">↔</span><strong>${e.record_b}</strong>
      <span class="badge badge-blue">${deptLabel(e.dept_a)}</span><span class="badge badge-blue">${deptLabel(e.dept_b)}</span>
      ${decisionBadge(e.decision)}
      <span style="font-weight:700;color:var(--accent);margin-left:auto">${(e.confidence*100).toFixed(1)}%</span>
    </div>
    <div class="conf-bar"><div class="conf-fill" style="width:${e.confidence*100}%;background:${e.confidence>=0.92?'var(--green)':e.confidence>=0.65?'var(--yellow)':'var(--red)'}"></div></div>
    ${e.safety_blocks?.length?`<div style="margin-top:8px;font-size:11px;color:var(--red)">🛡️ ${e.safety_blocks.join('; ')}</div>`:''}
    <div class="xai-list">${(e.xai_factors||[]).map(x=>`<div class="xai-row"><span class="feat">${x.feature}</span><div class="bar-track"><div class="bar-val ${x.direction}" style="width:${Math.min(100,x.bar_pct)}%"></div></div><span class="val" style="color:${x.direction==='positive'?'var(--green)':'var(--red)'}">${x.contribution>0?'+':''}${x.contribution}</span></div>`).join('')}
    </div>
    ${e.reviewer_notes?`<div style="margin-top:8px;padding:8px;background:rgba(52,211,153,0.1);border-radius:8px;font-size:11px">📝 ${e.reviewer}: "${e.reviewer_notes}" → ${e.reviewer_decision}</div>`:''}
  </div>`).join(''):'<div class="card"><div style="color:var(--muted);font-size:13px;padding:10px">No evidence yet — run the pipeline</div></div>';
}

// ═══════════════════════════════════════════════════════════════
//  PII with Masking Toggle
// ═══════════════════════════════════════════════════════════════
async function loadPII(){
  const reveal=piiRevealed;
  const data=await(await fetch(`/api/records?reveal=${reveal}`)).json();
  const masked=data.length?data[0].masked:true;
  document.getElementById('pii-reveal-btn').innerHTML=masked?'🔓 Reveal PII':'🔒 Mask PII';
  document.getElementById('pii-status-text').textContent=masked?'PII masked by default':'⚠️ PII revealed — auto-masks in 30s';
  document.getElementById('pii-table').innerHTML=data.length?`<div class="card"><div class="card-title">Raw → Tokenized Comparison ${masked?'<span class="badge badge-green" style="margin-left:8px">🔒 Masked</span>':'<span class="badge badge-red" style="margin-left:8px">⚠️ Revealed</span>'}</div><table><tr><th>Dept</th><th>Name</th><th>PAN (raw)</th><th>PAN (token)</th><th>GSTIN (raw)</th><th>GSTIN (token)</th></tr>${data.map(r=>`<tr><td><span class="badge badge-blue">${deptLabel(r.raw.department)}</span></td><td>${r.raw.business_name}</td><td style="font-family:monospace;font-size:11px">${r.raw.pan||'—'}</td><td style="font-family:monospace;font-size:10px;color:var(--green)">${r.tokenized.pan_token?.slice(0,16)||'—'}…</td><td style="font-family:monospace;font-size:11px">${r.raw.gstin||'—'}</td><td style="font-family:monospace;font-size:10px;color:var(--green)">${r.tokenized.gstin_token?.slice(0,16)||'—'}…</td></tr>`).join('')}</table></div>`:'<div class="card"><div style="color:var(--muted);font-size:13px;padding:10px">Run pipeline first</div></div>';
}
async function togglePIIReveal(){
  if(!piiRevealed){
    
    await fetch('/api/pii/reveal',{method:'POST'});
    piiRevealed=true;showToast('⚠️ PII revealed — auto-masks in 30s','warning');
    if(piiCountdown)clearTimeout(piiCountdown);
    piiCountdown=setTimeout(()=>{piiRevealed=false;if(currentPage==='pii')loadPII();showToast('🔒 PII auto-masked','info')},30000);
  }else{
    await fetch('/api/pii/hide',{method:'POST'});
    piiRevealed=false;if(piiCountdown)clearTimeout(piiCountdown);
    showToast('🔒 PII masked','info');
  }
  loadPII();
}

// ═══════════════════════════════════════════════════════════════
//  Activity (ABI)
// ═══════════════════════════════════════════════════════════════
async function loadActivity(districtFilter){
  const st=document.getElementById('abi-status-filter').value;
  const mo=document.getElementById('abi-months-filter').value;
  let url='/api/activity?';
  if(st)url+=`status=${st}&`;if(mo)url+=`months=${mo}&`;
  if(districtFilter)url+=`district=${encodeURIComponent(districtFilter)}&`;
  const data=await(await fetch(url)).json();
  document.getElementById('abi-list').innerHTML=data.length?data.map(a=>`<div class="card fade-in" style="margin-bottom:12px">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">
      <strong style="font-size:15px">${a.business_name}</strong>
      ${statusBadge(a.status)}
      <span style="font-weight:700;font-size:13px;color:var(--accent)">${(a.confidence*100).toFixed(0)}% confidence</span>
      <span style="margin-left:auto;font-size:12px;color:var(--muted)">${a.district||''} · ${a.departments_active} depts active</span>
    </div>
    <div style="font-size:12px;color:var(--muted);margin-bottom:8px">${a.rule_override}</div>
    <div style="display:flex;gap:16px;flex-wrap:wrap;font-size:12px;margin-bottom:10px">
      <span>📅 Last: <strong>${a.last_event_type}</strong> (${a.last_event_date})</span>
      <span>🏛️ Dept: <strong>${deptLabel(a.last_event_dept)}</strong></span>
      <span>⏱️ <strong>${a.months_since_activity}mo</strong> since activity</span>
    </div>
    <div class="card-title" style="margin-top:8px">Evidence Signals</div>
    ${a.signals.map(s=>`<div style="display:flex;align-items:center;gap:10px;padding:6px 0;border-bottom:1px solid var(--border);font-size:12px"><span class="badge badge-blue">${deptLabel(s.dept)}</span><span style="font-weight:600">${s.type}</span><span style="color:var(--muted)">${s.date}</span><span style="flex:1">${s.detail}</span></div>`).join('')}
  </div>`).join(''):'<div class="card"><div style="color:var(--muted);font-size:13px;padding:10px">No activity data — run pipeline</div></div>';
}

// ═══════════════════════════════════════════════════════════════
//  Query
// ═══════════════════════════════════════════════════════════════
async function loadQuery(){
  const guided=await(await fetch('/api/guided')).json();
  document.getElementById('guided-chips').innerHTML=guided.map(g=>`<div class="chip" onclick="runGuidedQuery('${g.id}','${encodeURIComponent(JSON.stringify(g.params))}')">${g.label}</div>`).join('');
}
async function runNLQuery(){
  const q=document.getElementById('nl-input').value;if(!q)return;
  const data=await(await fetch(`/api/nl-query?q=${encodeURIComponent(q)}`)).json();
  const cyDiv=document.getElementById('query-cypher');
  if(data.cypher){cyDiv.style.display='block';cyDiv.innerHTML=`<div class="card"><div class="card-title">Interpreted as: ${data.interpreted.join(' + ')}</div><div class="cypher-block">${data.cypher}<button class="copy-btn" onclick="copyCypher(this)">📋 Copy</button></div></div>`}
  renderQueryResults(data.results,data.count);
}
async function runGuidedQuery(id,paramsEnc){
  const params=JSON.parse(decodeURIComponent(paramsEnc));
  const qs=Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join('&');
  const data=await(await fetch(`/api/query?${qs}`)).json();
  const cyDiv=document.getElementById('query-cypher');
  const guided=await(await fetch('/api/guided')).json();
  const g=guided.find(x=>x.id===id);
  if(g){cyDiv.style.display='block';cyDiv.innerHTML=`<div class="card"><div class="card-title">Cypher Translation</div><div class="cypher-block">${g.cypher}<button class="copy-btn" onclick="copyCypher(this)">📋 Copy</button></div></div>`}
  renderQueryResults(data.results,data.count);
}
function renderQueryResults(results,count){
  const div=document.getElementById('query-results');
  div.innerHTML=`<div class="card" style="margin-top:12px"><div class="card-title">${count} Result${count!==1?'s':''}</div>${results.length?`<table><tr><th>UBID</th><th>Name</th><th>Records</th><th>Depts</th><th>ABI</th><th>Risk</th><th></th></tr>${results.map(r=>`<tr><td style="font-family:monospace;font-size:11px">${(r.ubid||'').slice(0,16)}…</td><td>${r.name||r.business_name||''}</td><td>${r.records||r.total_records||0}</td><td>${r.depts||r.departments_covered||0}</td><td>${statusBadge(r.abi_status||'UNKNOWN')}</td><td>${riskBadge(r.risk||r.risk_score||0)}</td><td><button class="btn btn-sm" onclick="openCluster('${r.ubid}')">View</button></td></tr>`).join('')}</table>`:'<div style="color:var(--muted);font-size:13px;padding:10px">No matching businesses</div>'}</div>`;
}
function copyCypher(btn){
  const block=btn.parentElement;const text=block.textContent.replace('📋 Copy','').trim();
  navigator.clipboard.writeText(text);btn.textContent='✓ Copied';setTimeout(()=>btn.textContent='📋 Copy',2000);
}
function exportQueryResults(){window.open('/api/export/query-results')}

// ═══════════════════════════════════════════════════════════════
//  Reviews with Toast + Animation + Undo
// ═══════════════════════════════════════════════════════════════
async function loadReviews(){
  const st=document.getElementById('review-status-filter').value;
  const data=await(await fetch(`/api/reviews?status=${st}`)).json();
  const rs=await(await fetch('/api/reviewer-stats')).json();
  document.getElementById('reviewer-stats-bar').innerHTML=`<div style="display:flex;gap:12px;font-size:11px;color:var(--muted)"><span>Reviewed: <strong style="color:var(--text)">${rs.total_reviewed}</strong></span><span>Merges: <strong style="color:var(--green)">${rs.merges}</strong></span><span>Separated: <strong style="color:var(--red)">${rs.separations}</strong></span><span>Accuracy: <strong style="color:var(--accent)">${(rs.accuracy*100).toFixed(0)}%</strong></span></div>`;
  if(!data.length&&st==='pending'){
    document.getElementById('review-list').innerHTML='<div class="card"><div class="queue-empty"><div class="emoji">🎉</div><div style="font-size:16px;font-weight:700;margin-bottom:4px">Queue cleared!</div><div style="font-size:13px;color:var(--muted)">All pairs have been reviewed</div></div></div>';
    return;
  }
  document.getElementById('review-list').innerHTML=data.length?data.map(r=>renderReviewItem(r)).join(''):'<div class="card"><div style="color:var(--muted);font-size:13px;padding:10px">No items in queue</div></div>';
}

function renderReviewItem(r){
  const isPending=r.status==='pending';
  const fieldsA=r.rec_a_details||{};const fieldsB=r.rec_b_details||{};
  const compareFields=['business_name','pan','gstin','udyam','district','pincode','phone','email','legal_status','sector','status'];
  return `<div class="card fade-in" style="margin-bottom:16px" id="review-${r.review_id}">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px">
      <span style="font-weight:800;font-size:15px">${r.review_id}</span>
      ${decisionBadge(r.status)}
      <span style="font-weight:700;color:var(--accent)">${(r.confidence*100).toFixed(1)}%</span>
      <span style="font-size:12px;color:var(--muted)">Risk: ${riskBadge(r.risk_score||0)}</span>
      ${r.reviewer?`<span style="margin-left:auto;font-size:11px;color:var(--muted)">by ${r.reviewer}</span>`:''}
    </div>
    <div class="review-pair">
      <div class="review-side card" style="border-color:rgba(56,189,248,0.3)">
        <h4><span class="badge badge-blue">${deptLabel(r.dept_a)}</span> ${r.record_a}</h4>
        ${compareFields.map(f=>fieldsA[f]?`<div class="field-row"><span class="fl">${f}</span><span class="fv ${fieldsA[f]===fieldsB[f]?'field-match':'field-mismatch'}">${fieldsA[f]}</span></div>`:'').join('')}
      </div>
      <div class="review-side card" style="border-color:rgba(167,139,250,0.3)">
        <h4><span class="badge badge-purple">${deptLabel(r.dept_b)}</span> ${r.record_b}</h4>
        ${compareFields.map(f=>fieldsB[f]?`<div class="field-row"><span class="fl">${f}</span><span class="fv ${fieldsA[f]===fieldsB[f]?'field-match':'field-mismatch'}">${fieldsB[f]}</span></div>`:'').join('')}
      </div>
    </div>
    <div class="card-title">XAI Feature Contributions</div>
    <div class="xai-list">${(r.xai_factors||[]).map(x=>`<div class="xai-row"><span class="feat">${x.feature}</span><div class="bar-track"><div class="bar-val ${x.direction}" style="width:${Math.min(100,x.bar_pct)}%"></div></div><span class="val" style="color:${x.direction==='positive'?'var(--green)':'var(--red)'}">${x.contribution>0?'+':''}${x.contribution}</span></div>`).join('')}</div>
    ${isPending?`<div class="review-actions">
      <button class="btn btn-green" onclick="decideReview('${r.review_id}','merged')">✓ Merge<span class="shortcut">[M]</span></button>
      <button class="btn btn-red" onclick="decideReview('${r.review_id}','separated')">✗ Separate<span class="shortcut">[S]</span></button>
      <button class="btn" onclick="decideReview('${r.review_id}','deferred')">⏸ Defer<span class="shortcut">[D]</span></button>
      <input style="flex:1;background:var(--card);border:1px solid var(--border);border-radius:8px;padding:7px 12px;color:var(--text);font-size:12px;font-family:inherit" id="notes-${r.review_id}" placeholder="Reviewer notes...">
    </div>`:`<div style="font-size:12px;color:var(--muted);margin-top:8px">${r.reviewer_notes?'Notes: '+r.reviewer_notes:''}</div>
    ${r.status==='merged'||r.status==='separated'?`<div class="undo-bar" id="undo-${r.review_id}"><button class="btn btn-sm" onclick="undoReview('${r.review_id}')">↩ Undo</button><span>Decision can be undone within 60s</span><span class="countdown" id="countdown-${r.review_id}">60</span></div>`:''}`}
  </div>`;
}

async function decideReview(rid,decision){
  
  const notes=document.getElementById('notes-'+rid)?.value||'';
  const res=await(await fetch(`/api/reviews/${rid}/decide`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({decision,notes,reviewer:'Admin'})})).json();
  // Toast feedback
  const toastMap={merged:'✅ Merged — Decision recorded',separated:'✗ Separated — Decision recorded',deferred:'⏸ Deferred — Moved to bottom of queue'};
  const toastType={merged:'success',separated:'info',deferred:'warning'};
  showToast(toastMap[decision]||'Decision recorded',toastType[decision]||'info');
  // Slide-out animation
  const card=document.getElementById('review-'+rid);
  if(card){card.classList.add('review-card-exit');setTimeout(()=>card.remove(),500)}
  // Update dashboard counters live
  updateReviewBadge();
  if(currentPage==='dashboard')setTimeout(loadDashboard,500);
  // Start undo timer for merge/separate
  if(decision==='merged'||decision==='separated'){
    undoTimers[rid]={time:60,interval:null};
    // Reload to show undo button after animation
    setTimeout(()=>{
      if(currentPage==='reviews')loadReviews();
    },600);
  }else{
    setTimeout(()=>{if(currentPage==='reviews')loadReviews()},600);
  }
}

async function undoReview(rid){
  const res=await(await fetch(`/api/reviews/${rid}/undo`,{method:'POST'})).json();
  if(res.ok){showToast('↩ Decision rolled back','warning');loadReviews();updateReviewBadge()}
  else showToast('Cannot undo — time expired','error');
}

// Keyboard shortcuts
document.addEventListener('keydown',e=>{
  if(currentPage!=='reviews')return;
  if(['INPUT','TEXTAREA','SELECT'].includes(document.activeElement.tagName))return;
  const pending=document.querySelector('[id^="review-REV"]');
  if(!pending)return;
  const rid=pending.id.replace('review-','');
  if(e.key==='m'||e.key==='M')decideReview(rid,'merged');
  else if(e.key==='s'||e.key==='S'){if(!e.ctrlKey)decideReview(rid,'separated')}
  else if(e.key==='d'||e.key==='D')decideReview(rid,'deferred');
});

// ═══════════════════════════════════════════════════════════════
//  Graph Network
// ═══════════════════════════════════════════════════════════════
async function loadGraph(){
  const data=await(await fetch('/api/graph-data')).json();
  if(!data.nodes.length){document.getElementById('graph-container').innerHTML='<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--muted)">Run pipeline to see graph</div>';return}
  const nodes=new vis.DataSet(data.nodes.map(n=>{
    let isBusiness=n.type!=='dept';let faCode='\uf15c';let iconSize=20;
    if(isBusiness){faCode=n.full_name?.toLowerCase().includes('factor')||n.full_name?.toLowerCase().includes('mill')?'\uf275':'\uf1ad';iconSize=25+(n.records*6)}
    else{if(n.department==='commercial_taxes')faCode='\uf53c';else if(n.department==='factories_board')faCode='\uf013';else if(n.department==='shops_establishments')faCode='\uf54f';else if(n.department==='labour_dept')faCode='\uf508';else if(n.department==='revenue_dept')faCode='\uf0d6';iconSize=18}
    return{id:n.id,label:isBusiness?n.label:n.label.split(' ')[0],shape:'icon',icon:{face:'"Font Awesome 6 Free"',code:faCode,size:iconSize,color:isBusiness?n.color:'#94a3b8',weight:'900'},font:{color:isBusiness?'#f8fafc':'#94a3b8',size:isBusiness?12:9,face:'Inter',background:isBusiness?'rgba(15,23,42,0.8)':'transparent'},shadow:{enabled:true,color:isBusiness?n.color:'rgba(0,0,0,0.5)',size:isBusiness?20:10,x:0,y:0},_data:n};
  }));
  const edges=new vis.DataSet(data.edges.map(e=>({from:e.from,to:e.to,width:e.width*1.5,color:{color:e.color,highlight:'#fff',hover:e.color},dashes:e.dashes||false,label:e.label||'',font:{color:'#cbd5e1',size:10,face:'Inter',background:'rgba(15,23,42,0.9)'},smooth:{type:'continuous'}})));
  const container=document.getElementById('graph-container');
  graphNetwork=new vis.Network(container,{nodes,edges},{
    physics:{solver:'barnesHut',barnesHut:{gravitationalConstant:-3000,centralGravity:0.03,springLength:120,springConstant:0.04,damping:0.09,avoidOverlap:0.1},stabilization:{iterations:250,fit:true}},
    interaction:{hover:true,tooltipDelay:100,zoomView:true,dragNodes:true,dragView:true,hideEdgesOnDrag:true},edges:{arrows:{to:false}}
  });
  graphNetwork.on('click',params=>{
    if(params.nodes.length){
      const nd=nodes.get(params.nodes[0]);const d=nd._data;
      if(d&&d.type!=='dept'){
        document.getElementById('graph-detail').innerHTML=`<div class="card-title">Node Detail</div><div style="font-weight:700;font-size:14px;margin-bottom:8px">${d.full_name||d.label}</div><div style="font-size:12px;color:var(--muted);margin-bottom:12px"><div>Status: ${statusBadge(d.status)}</div><div style="margin:4px 0">Records: ${d.records} | Depts: ${d.depts}</div><div>Risk: ${riskBadge(d.risk)}</div></div><button class="btn btn-sm btn-primary" onclick="openCluster('${d.id}')">Explore Entity Profile</button>`;
      }
    }
  });
}

// ═══════════════════════════════════════════════════════════════
//  Dept Health
// ═══════════════════════════════════════════════════════════════
async function loadHealth(){
  const data=await(await fetch('/api/dept-health')).json();
  const icons={'commercial_taxes':'🧾','factories_board':'🏭','shops_establishments':'🏪','labour_dept':'👷','revenue_dept':'💰'};
  const colors={'Good':'var(--green)','Fair':'var(--yellow)','Stale':'var(--red)','Unknown':'var(--muted)'};
  document.getElementById('health-list').innerHTML=data.length?data.map(d=>`<div class="card dept-card fade-in">
    <div class="dept-icon" style="background:${colors[d.quality]}22;border:1px solid ${colors[d.quality]}44;font-size:22px">${icons[d.department]||'📁'}</div>
    <div class="dept-info"><div class="dept-name">${deptLabel(d.department)}</div><div class="dept-meta">${d.records} records • Match rate: ${(d.match_rate*100).toFixed(0)}% • Synced: ${d.last_sync}</div></div>
    <div style="text-align:right"><div style="font-size:11px;color:var(--muted);margin-bottom:4px">Quality: <strong style="color:${colors[d.quality]}">${d.quality}</strong></div><div class="quality-bar"><div class="quality-fill" style="width:${d.quality_score}%;background:${colors[d.quality]}"></div></div></div>
    <button class="btn btn-sm" onclick="triggerSync('${d.department}',this)">🔄 Sync</button>
  </div>`).join(''):'<div class="card"><div style="color:var(--muted);font-size:13px;padding:10px">Run pipeline first</div></div>';
}
async function triggerSync(dept,btn){
  btn.disabled=true;btn.textContent='Syncing...';
  await fetch(`/api/dept-sync?department=${dept}`,{method:'POST'});
  showToast(`🔄 ${dept.replace(/_/g,' ')} synced successfully`,'success');
  setTimeout(()=>loadHealth(),500);
}

// ═══════════════════════════════════════════════════════════════
//  History
// ═══════════════════════════════════════════════════════════════
async function loadHistory(){
  const data=await(await fetch('/api/pipeline/history')).json();
  document.getElementById('history-table').innerHTML=data.length?`<div class="card"><table><tr><th>Run</th><th>Status</th><th>Started</th><th>Duration</th><th>Records</th><th>Pairs</th><th>Auto</th><th>Review</th><th>Safety</th><th>Active</th><th>Dormant</th><th>Closed</th><th>Uncertain</th><th></th></tr>${data.map(r=>`<tr style="cursor:pointer" onclick="drillRun('${r.run_id}')"><td style="font-weight:700">${r.run_id}</td><td>${r.status==='completed'?'<span class="badge badge-green">✓</span>':'<span class="badge badge-red">✗</span>'}</td><td style="font-size:11px">${new Date(r.started_at).toLocaleString()}</td><td style="font-size:11px">${r.duration_ms}ms</td><td>${r.records_processed}</td><td>${r.pairs_analyzed}</td><td style="color:var(--green);font-weight:700">${r.auto_linked}</td><td style="color:var(--yellow);font-weight:700">${r.sent_to_review}</td><td style="color:var(--red);font-weight:700">${r.blocked_by_safety}</td><td>${r.abi_active}</td><td>${r.abi_dormant}</td><td>${r.abi_closed}</td><td>${r.abi_uncertain}</td><td><button class="btn btn-sm" onclick="event.stopPropagation();drillRun('${r.run_id}')">Details</button></td></tr>`).join('')}</table></div>`:'<div class="card"><div style="color:var(--muted);font-size:13px;padding:10px">No pipeline runs yet</div></div>';
}
async function drillRun(runId){
  const r=await(await fetch(`/api/pipeline/run-detail?run_id=${runId}`)).json();
  const ct=document.getElementById('cluster-detail-content');
  let html=`<h3>${r.run_id} — Pipeline Run Detail</h3><div style="font-size:12px;color:var(--muted);margin-bottom:16px">Started: ${new Date(r.started_at).toLocaleString()} | Duration: ${r.duration_ms}ms | Status: ${r.status}</div>`;
  html+=`<div class="card" style="margin-bottom:12px"><div class="card-title">Summary</div><div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;text-align:center">${[['Records',r.records_processed,'--cyan'],['Auto-linked',r.auto_linked,'--green'],['Review',r.sent_to_review,'--yellow'],['Blocked',r.blocked_by_safety,'--red']].map(([l,v,c])=>`<div><div style="font-size:24px;font-weight:800;color:var(${c})">${v}</div><div style="font-size:10px;color:var(--muted);text-transform:uppercase">${l}</div></div>`).join('')}</div></div>`;
  if(r.pair_details?.length){html+=`<div class="card" style="margin-bottom:12px"><div class="card-title">Pair Decisions</div><table><tr><th>Record A</th><th>Record B</th><th>Confidence</th><th>Decision</th></tr>${r.pair_details.map(p=>`<tr><td>${p.a}</td><td>${p.b}</td><td style="font-weight:700">${(p.conf*100).toFixed(0)}%</td><td>${decisionBadge(p.decision)}</td></tr>`).join('')}</table></div>`}
  if(r.events?.length){html+=`<div class="card"><div class="card-title">Event Log (${r.events.length})</div>${r.events.map(e=>`<div style="padding:4px 0;font-size:12px;border-bottom:1px solid var(--border)">${e.message}</div>`).join('')}</div>`}
  ct.innerHTML=html;
  document.getElementById('cluster-modal').classList.add('show');
}

// ═══════════════════════════════════════════════════════════════
//  Calibration with Interactive Slider
// ═══════════════════════════════════════════════════════════════
async function loadCalibration(){
  try{
    const data=await(await fetch('/api/calibration')).json();
    if(!data.total_pairs){document.getElementById('cal-panel').innerHTML='<div class="card-title">Auto-Link Threshold</div><div style="padding:16px;color:var(--muted);font-size:13px">Run pipeline first</div>';return}
    const currentVal=(data.current_auto_threshold*100).toFixed(0);
    document.getElementById('cal-panel').innerHTML=`<div class="card-title">Auto-Link Threshold — Interactive</div><div class="cal-slider-wrap">
      <div style="display:flex;align-items:center;gap:16px;margin-bottom:12px"><span style="font-size:13px">Auto-link threshold:</span><strong style="font-size:24px;color:var(--green)" id="cal-val">${currentVal}%</strong></div>
      <input type="range" class="cal-slider" id="cal-range" min="30" max="98" value="${currentVal}" oninput="updateCalPreview(this.value)">
      <div class="cal-info"><span>30% (Aggressive)</span><span>98% (Ultra-conservative)</span></div>
      <div id="cal-risk-indicator" style="margin-top:12px"></div>
      <div style="margin-top:16px;display:flex;gap:8px"><button class="btn btn-primary" onclick="applyThreshold()">Apply Threshold</button><button class="btn" onclick="document.getElementById('cal-range').value=${currentVal};updateCalPreview(${currentVal})">Reset</button></div>
    </div>`;
    updateCalPreview(currentVal);
    document.getElementById('cost-panel').innerHTML=`<div class="card-title">Decision Economics</div><div style="padding:16px">
      <div style="font-size:14px;margin-bottom:12px">Estimated cost per false positive merge:</div>
      <div style="font-size:32px;font-weight:900;color:var(--red);margin-bottom:8px">${data.false_positive_cost}</div>
      <div style="font-size:12px;color:var(--muted)">Includes: audit investigation + data rollback + compliance penalty + trust erosion</div>
      <div style="margin-top:16px;padding:12px;background:rgba(52,211,153,0.1);border:1px solid rgba(52,211,153,0.2);border-radius:10px;font-size:12px;color:var(--green)">✓ Conservative threshold active — safety blocks prevent high-cost errors</div></div>`;
    // Threshold table with highlighted current
    document.getElementById('cal-table').innerHTML=`<table><tr><th>Threshold</th><th>Auto-Link</th><th>Review</th><th>Reject</th><th>FP Risk</th><th>Visual</th></tr>${data.data.map(d=>{
      const pctA=d.auto_link/Math.max(d.total,1)*100,pctR=d.review/Math.max(d.total,1)*100;
      const fpLabel=d.fp_risk>50?'HIGH':d.fp_risk>25?'MEDIUM':'LOW';
      const fpColor=d.fp_risk>50?'var(--red)':d.fp_risk>25?'var(--yellow)':'var(--green)';
      return`<tr ${d.threshold===data.current_auto_threshold?'style="background:rgba(129,140,248,0.1)"':''}><td style="font-weight:700">${(d.threshold*100).toFixed(0)}%${d.threshold===data.current_auto_threshold?' ← current':''}</td><td style="color:var(--green)">${d.auto_link}</td><td style="color:var(--yellow)">${d.review}</td><td style="color:var(--red)">${d.reject}</td><td style="color:${fpColor};font-weight:700">${fpLabel}</td><td><div style="display:flex;height:8px;border-radius:4px;overflow:hidden;width:120px"><div style="width:${pctA}%;background:var(--green)"></div><div style="width:${pctR}%;background:var(--yellow)"></div><div style="flex:1;background:var(--red)"></div></div></td></tr>`}).join('')}</table>`;
  }catch(e){console.error(e)}
}
function updateCalPreview(val){
  document.getElementById('cal-val').textContent=val+'%';
  const risk=val<50?'HIGH':val<70?'MEDIUM':val<85?'LOW':'VERY LOW';
  const riskColor=val<50?'var(--red)':val<70?'var(--yellow)':val<85?'var(--green)':'var(--green)';
  const warn=val<70?`<div class="cal-warning" style="background:rgba(248,113,113,0.1);border:1px solid rgba(248,113,113,0.3);color:var(--red)">⚠️ Below safe threshold — false positive risk increases significantly</div>`:'';
  document.getElementById('cal-risk-indicator').innerHTML=`<div style="display:flex;align-items:center;gap:8px;font-size:13px"><span>Estimated false positive risk:</span><strong style="color:${riskColor};font-size:16px">${risk}</strong></div>${warn}`;
}
async function applyThreshold(){
  
  const val=document.getElementById('cal-range').value/100;
  await fetch('/api/calibration/apply',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({auto_threshold:val})});
  showToast(`⚙️ Threshold updated to ${(val*100).toFixed(0)}%`,'success');
  loadCalibration();
}

// ═══════════════════════════════════════════════════════════════
//  Access Log
// ═══════════════════════════════════════════════════════════════
async function loadAccessLog(){
  const data=await(await fetch('/api/access-logs')).json();
  document.getElementById('access-log-table').innerHTML=`<div class="card"><div class="card-title">Data Access Audit Trail (${data.length} entries)</div>${data.length?`<table><tr><th>Timestamp</th><th>Actor</th><th>Action</th><th>Entity/Screen</th><th>IP</th></tr>${data.slice(0,100).map(l=>`<tr><td style="font-size:11px;font-family:monospace">${new Date(l.timestamp).toLocaleString()}</td><td><span class="badge badge-blue">${l.actor}</span></td><td style="font-size:12px">${l.action}</td><td style="font-size:12px">${l.entity}</td><td style="font-size:11px;color:var(--muted)">${l.ip}</td></tr>`).join('')}</table>`:'<div style="color:var(--muted);font-size:13px;padding:10px">No access logs yet</div>'}</div>`;
}

// ═══════════════════════════════════════════════════════════════
//  Tour — Complete 7-Step with Navigation + Spotlight
// ═══════════════════════════════════════════════════════════════
async function startTour(){
  tourSteps=await(await fetch('/api/tour')).json();tourIdx=0;showTourStep();
  document.getElementById('tour-overlay').classList.add('show');
}
function showTourStep(){
  const s=tourSteps[tourIdx];
  document.getElementById('tour-step-num').textContent=`Step ${s.step} of ${tourSteps.length}`;
  document.getElementById('tour-title').textContent=s.title;
  document.getElementById('tour-desc').textContent=s.desc;
  document.getElementById('tour-dots').innerHTML=tourSteps.map((_,i)=>`<div class="tour-dot ${i===tourIdx?'active':''}"></div>`).join('');
  document.getElementById('tour-prev').style.display=tourIdx===0?'none':'inline-block';
  document.getElementById('tour-next').textContent=tourIdx===tourSteps.length-1?'Finish':'Next →';
  // Navigate and spotlight
  const target=s.target;
  if(s.action==='navigate'&&target){
    navigateTo(target);
    setTimeout(()=>{
      // Spotlight specific elements per step
      document.querySelectorAll('.spotlight-ring').forEach(e=>e.classList.remove('spotlight-ring'));
      if(s.step===3){// UBID Clusters — spotlight first row
        const row=document.querySelector('#page-clusters table tr:nth-child(2)');
        if(row)row.classList.add('spotlight-ring');
      }else if(s.step===5){// Review Queue — spotlight first review
        const rev=document.querySelector('[id^="review-REV"]');
        if(rev)rev.classList.add('spotlight-ring');
      }else if(s.step===6){// Activity Status — spotlight UNCERTAIN card
        const cards=document.querySelectorAll('#abi-list .card');
        cards.forEach(c=>{if(c.textContent.includes('UNCERTAIN'))c.classList.add('spotlight-ring')});
      }else if(s.step===7){// Calibration — spotlight current row
        const rows=document.querySelectorAll('#cal-table tr');
        rows.forEach(r=>{if(r.textContent.includes('← current'))r.classList.add('spotlight-ring')});
      }
    },300);
  }
  if(s.action==='click'&&target==='#btn-run')runPipeline();
}
function tourNext(){
  if(tourIdx<tourSteps.length-1){tourIdx++;showTourStep()}
  else endTour();
}
function tourPrev(){if(tourIdx>0){tourIdx--;showTourStep()}}
function endTour(){
  document.getElementById('tour-overlay').classList.remove('show');
  document.querySelectorAll('.spotlight-ring').forEach(e=>e.classList.remove('spotlight-ring'));
}

// ═══════════════════════════════════════════════════════════════
//  Helpers
// ═══════════════════════════════════════════════════════════════
function statusBadge(s){
  const m={ACTIVE:'badge-green',DORMANT:'badge-yellow',CLOSED:'badge-red',UNCERTAIN:'badge-purple',UNKNOWN:'badge-blue'};
  return`<span class="badge ${m[s]||'badge-blue'}">${s}</span>`;
}
function decisionBadge(d){
  const m={auto_link:['badge-green','Auto-Linked'],review:['badge-yellow','Review'],reject:['badge-red','Rejected'],merged:['badge-green','Merged'],separated:['badge-red','Separated'],deferred:['badge-purple','Deferred'],pending:['badge-yellow','Pending']};
  const x=m[d]||['badge-blue',d];return`<span class="badge ${x[0]}">${x[1]}</span>`;
}
function riskBadge(r){
  const c=r>=50?'badge-red':r>=25?'badge-yellow':'badge-green';
  return`<span class="badge ${c}">${r}</span>`;
}
function deptLabel(d){
  const m={commercial_taxes:'Comm. Taxes',factories_board:'Factories',shops_establishments:'Shops & Est.',labour_dept:'Labour',revenue_dept:'Revenue'};
  return m[d]||d;
}
function exportCSV(type){window.open(`/api/export/${type}`)}

// Initial load — must await role before dashboard
(async()=>{
  console.log('🔄 Initializing application...');
  try{
        await loadDashboard();
    console.log('✅ Dashboard loaded');
  }catch(e){
    console.error('❌ Initialization error:', e);
  }
})();
