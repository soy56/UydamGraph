"""
UdyamGraph — Theme 1 Complete Console Server
=============================================
All critical + high-value features:
- District heatmap data for Karnataka
- Rich entity decision timeline (audit wall)
- Interactive graph with filters
- Review decisions with live counter updates + toast
- BI Query with NL parsing + export + deep-dive
- Pipeline run drill-down
- Dept health with sync simulation
- Calibration interactive slider
- Working global search
- ABI sparklines
- Full 7-step demo tour
"""
import os,sys,time,logging,uuid,random,csv,io,json,re,asyncio
import numpy as np
from typing import Dict,Any,List,Optional
from dataclasses import dataclass,asdict,field
from datetime import datetime,timedelta
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))
from fastapi import FastAPI,BackgroundTasks,Query,Response,Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse,StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pii.tokenizer import PIITokenizer
from resolution.blocking import BlockingEngine
from safeguards.anti_merge import AntiFalseMergeEngine
from sse_starlette.sse import EventSourceResponse
logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger=logging.getLogger("udyamgraph.server")

# ═══════════════════════════════════════════════════════════════════════
@dataclass
class AuditEntry:
    timestamp:str;ubid:str;action:str;details:str;actor:str="SYSTEM";icon:str="⚙️";before:str="";after:str=""
@dataclass
class AlertItem:
    alert_id:str;severity:str;title:str;description:str;triggered_at:str;acknowledged:bool=False
@dataclass
class PipelineRun:
    run_id:str;started_at:str;finished_at:Optional[str]=None;records_processed:int=0
    pairs_analyzed:int=0;auto_linked:int=0;sent_to_review:int=0;rejected:int=0
    blocked_by_safety:int=0;status:str="running";duration_ms:int=0
    abi_active:int=0;abi_dormant:int=0;abi_closed:int=0;abi_uncertain:int=0
    entities_processed:List[str]=field(default_factory=list)
    pair_details:List[Dict]=field(default_factory=list)
    status_changes:List[str]=field(default_factory=list)
    events:List[Dict]=field(default_factory=list)
@dataclass
class ReviewItem:
    review_id:str;record_a:str;record_b:str;dept_a:str;dept_b:str
    record_a_id:str;record_b_id:str;confidence:float;explanation:str
    features:Dict[str,Any];risk_score:float=0.0
    rec_a_details:Dict[str,Any]=field(default_factory=dict)
    rec_b_details:Dict[str,Any]=field(default_factory=dict)
    status:str="pending";reviewer:Optional[str]=None;reviewed_at:Optional[str]=None
    reviewer_notes:Optional[str]=None;feedback_tags:List[str]=field(default_factory=list)
    xai_factors:List[Dict[str,Any]]=field(default_factory=list)
    ubid:str=""
@dataclass
class ActivityRecord:
    ubid:str;business_name:str;status:str;confidence:float;rule_override:Optional[str]
    last_event_date:str;last_event_type:str;last_event_dept:str
    departments_active:int;months_since_activity:int;signals:List[Dict[str,Any]]
    district:str=""
@dataclass
class DeptHealth:
    department:str;records:int;match_rate:float;last_sync:str;quality:str;quality_score:int
    stale_hours:int=0;record_ids:List[str]=field(default_factory=list)
# ═══════════════════════════════════════════════════════════════════════
#  RBAC Configuration
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class PlatformState:
    records:List[Dict[str,Any]]=field(default_factory=list)
    tokenized_records:List[Dict[str,Any]]=field(default_factory=list)
    ubid_clusters:List[Dict[str,Any]]=field(default_factory=list)
    link_evidence:List[Dict[str,Any]]=field(default_factory=list)
    review_queue:List[ReviewItem]=field(default_factory=list)
    activity_records:List[ActivityRecord]=field(default_factory=list)
    pipeline_runs:List[PipelineRun]=field(default_factory=list)
    audit_log:List[AuditEntry]=field(default_factory=list)
    access_logs:List[Dict[str,Any]]=field(default_factory=list)
    export_counts:Dict[str,int]=field(default_factory=dict)
    locked:bool=False
    alerts:List[AlertItem]=field(default_factory=list)
    dept_health:List[DeptHealth]=field(default_factory=list)
    is_processing:bool=False
    total_ubids:int=0;total_records:int=0;total_links:int=0
    auto_linked:int=0;sent_to_review:int=0;rejected:int=0;blocked_by_safety:int=0;avg_confidence:float=0.0
    active_count:int=0;dormant_count:int=0;closed_count:int=0;uncertain_count:int=0
    delta_ubids:int=0;delta_active:int=0;delta_dormant:int=0;delta_closed:int=0;delta_uncertain:int=0
    auto_threshold:float=0.92;review_threshold:float=0.65
    reviewer_stats:Dict[str,Any]=field(default_factory=lambda:{"total_reviewed":0,"merges":0,"separations":0,"defers":0,"overrides":0,"avg_decision_time":8.2,"accuracy":0.94,"session_reviewed":0,"session_merges":0,"session_separations":0})
    # RBAC + Security
    pii_revealed:bool=False
    pii_reveal_time:Optional[float]=None
    rollback_count:int=0
    session_timeout_sec:int=300  # 5 minutes
    session_last_activity:float=field(default_factory=time.time)

state=PlatformState()
tokenizer=PIITokenizer();blocker=BlockingEngine();safety_engine=AntiFalseMergeEngine()
event_queues=[]

app=FastAPI(title="UdyamGraph Advanced",description="Theme 1 Complete Console")
# TODO: Restrict CORS origins in production to specific trusted domains
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # SECURITY: Change to specific origins in production
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Content-Type", "Authorization"]
)

# ═══════════════════════════════════════════════════════════════════════
#  Karnataka Districts
# ═══════════════════════════════════════════════════════════════════════
KARNATAKA_DISTRICTS = [
    {"name":"Bangalore Urban","code":"BLR","row":5,"col":3},
    {"name":"Bangalore Rural","code":"BRU","row":5,"col":2},
    {"name":"Mysuru","code":"MYS","row":6,"col":2},
    {"name":"Tumakuru","code":"TMK","row":4,"col":3},
    {"name":"Dharwad","code":"DWD","row":2,"col":2},
    {"name":"Belagavi","code":"BGM","row":1,"col":1},
    {"name":"Kalaburagi","code":"KLB","row":0,"col":4},
    {"name":"Mangaluru (DK)","code":"DKN","row":5,"col":0},
    {"name":"Raichur","code":"RCR","row":1,"col":4},
    {"name":"Ballari","code":"BLR2","row":2,"col":4},
    {"name":"Shivamogga","code":"SMG","row":3,"col":1},
    {"name":"Davanagere","code":"DVG","row":3,"col":3},
    {"name":"Hassan","code":"HSN","row":4,"col":1},
    {"name":"Mandya","code":"MND","row":5,"col":2},
    {"name":"Kodagu","code":"KDG","row":6,"col":0},
    {"name":"Udupi","code":"UDP","row":4,"col":0},
    {"name":"Haveri","code":"HVR","row":2,"col":1},
    {"name":"Gadag","code":"GDG","row":2,"col":3},
    {"name":"Koppal","code":"KPL","row":1,"col":3},
    {"name":"Ramanagara","code":"RMN","row":5,"col":2},
    {"name":"Chikkamagaluru","code":"CKM","row":4,"col":1},
    {"name":"Chitradurga","code":"CTR","row":3,"col":3},
    {"name":"Vijayapura","code":"VJP","row":1,"col":2},
    {"name":"Bagalkot","code":"BGK","row":1,"col":2},
    {"name":"Bidar","code":"BDR","row":0,"col":3},
    {"name":"Yadgir","code":"YDG","row":1,"col":5},
    {"name":"Chamarajanagar","code":"CMR","row":7,"col":2},
    {"name":"Chikkaballapura","code":"CKB","row":4,"col":4},
    {"name":"Kolar","code":"KLR","row":5,"col":4},
    {"name":"Uttara Kannada","code":"UKN","row":2,"col":0},
    {"name":"Vijayanagara","code":"VNG","row":2,"col":3},
]

BUSINESS_NAMES=[
    "UBIQUITY INNOVATIONS PVT LTD","KRISHNA TRADING COMPANY","DECCAN PHARMACEUTICALS PVT LTD","KRISHNA TRADERS AND SONS","TUMAKURU SILK MILLS",
    "BANGALORE ENGINEERING WORKS","MYSURU AGRO PRODUCTS","HUBLI STEEL FABRICATORS","MANGALORE TILE WORKS","BELGAUM COTTON MILLS",
    "RAICHUR SUGAR FACTORY","KOLAR GOLD MINING CO","SHIMOGA TIMBER TRADERS","HASSAN COFFEE ESTATES","MANDYA SUGAR FACTORY",
    "DAVANGERE TEXTILES","CHITRADURGA GRANITE WORKS","GULBARGA CEMENT WORKS","BIJAPUR OIL MILLS","KARWAR FISHERIES",
    "BELLARY IRON ORES","UDUPI CASHEW EXPORTS","GADAG COTTON GINNING","KOPPAL CEMENT FACTORY","BIDAR RICE MILLS",
    "CHIKMAGALUR COFFEE PVT LTD","KODAGU SPICE TRADERS","HAVERI MAIZE PRODUCTS","BAGALKOT SUGAR WORKS","YADGIR AGRO FARMS",
    "SRI LAKSHMI ENTERPRISES","GANESHA INDUSTRIES","BHARATH ELECTRONICS","VIKAS POLYMERS PVT LTD","NANDI PHARMA LABS",
    "CAUVERY FOODS PVT LTD","TUNGABHADRA POWER SYSTEMS","SHARAVATHI HYDRO WORKS","KAVERI SEEDS CO LTD","HAMPI STONES EXPORTS",
    "GOLDEN HARVEST AGRI","SILICON VALLEY TECH SERVICES","INDO-EURO TEXTILES","SOUTHERN STAR LOGISTICS","PREMIER PACK SOLUTIONS",
    "OCEANIC SEAFOODS","HERITAGE HANDLOOMS","ROYAL ORCHID FOODS","PRECISION TOOLS MFG","EVERGREEN BIOTECH",
    "APEX STEEL INDUSTRIES","PHOENIX CERAMICS","TITAN ENGINEERING","FORTUNE PLASTICS","SUMMIT CHEMICALS",
    "GALAXY GARMENTS","ORBIT ELECTRICALS","ZENITH AGRO TECH","PIONEER RUBBER WORKS","CRYSTAL GLASS WORKS",
    "SAPPHIRE TEXTILES","EMERALD PHARMA","RUBY ENGINEERING","DIAMOND ABRASIVES","PEARL FISHERIES CO",
    "MAPLE FOODS PVT LTD","CEDAR TIMBER MILLS","BANYAN AUTO PARTS","TEAK FURNITURE EXPORTS","SANDAL WOOD CRAFTS",
    "JASMINE PERFUMES","LOTUS CHEMICALS","TULIP GARMENTS","ORCHID BIOTECH","SUNFLOWER OIL MILLS",
    "SAMARTH ENTERPRISES","VIBHUTI TRADERS","SHANKAR INDUSTRIES","MAHALAKSHMI TEXTILES","BASAVA ENGINEERING",
    "KEMPEGOWDA CONSTRUCTIONS","TIPU SULTAN EXPORTS","CHALUKYA STONES","HOYSALA SILKS PVT LTD","VIJAYANAGAR GRANITES",
    "KADAMBA AGRO PRODUCTS","GANESH STEEL WORKS","PARVATHI FOODS","SHIVA PHARMA PVT LTD","VISHNU ENTERPRISES",
    "DEVI TEXTILES","DURGA POLYMERS","LAXMI COTTON MILLS","ANNAPURNA FOODS","SARASWATHI ELECTRONICS",
    "MAHESH TRADING CO","SURESH INDUSTRIES","RAJESH POLYMERS PVT LTD","DINESH RUBBER WORKS","RAMESH AUTO COMPONENTS",
]
TRADE_SUFFIXES=["","& Co","Trading Co","Sons","Brothers","Group","International","Exports","Industries","Works","Solutions"]
LEGAL_TYPES=["Private Limited Company","Proprietorship","Partnership","LLP","Public Limited Company","One Person Company","HUF"]
SECTORS=["Manufacturing","Services","Trade","Agriculture","IT Services","Pharmaceuticals","Construction","Textiles","Food Processing","Mining"]
BUSINESS_TYPES=["Manufacturing","IT Services","Wholesale Trade","Retail Trade","Textile Manufacturing","Food Processing","Pharmaceutical Manufacturing","Engineering Works","Agricultural Products","Chemical Manufacturing","Steel Fabrication","Software Services","Logistics","Construction Materials","Mining"]
DEPARTMENTS=["commercial_taxes","factories_board","shops_establishments","labour_dept","revenue_dept"]
STATUS_TYPES=["Active","Active","Active","Active","Active","Suspended","Cancelled"]  # 70% Active

def gen_pan():
    c1="".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ",k=5))
    c2="".join(random.choices("0123456789",k=4))
    c3=random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    return c1+c2+c3

def gen_gstin(pan,district_code=29):
    entity=random.choice("123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    check=random.choice("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    return f"{district_code:02d}{pan}{entity}Z{check}"

def gen_records():
    """Load demo records from data/demo_records.json"""
    demo_file = os.path.join(os.path.dirname(__file__), 'data', 'demo_records.json')
    try:
        if os.path.exists(demo_file):
            with open(demo_file, 'r') as f:
                data = json.load(f)
                if 'records' in data and isinstance(data['records'], list):
                    logger.info(f"✅ Loaded {len(data['records'])} demo records from {demo_file}")
                    return data['records']
    except Exception as e:
        logger.warning(f"⚠️ Failed to load demo records: {e}")
    
    # Fallback embedded data
    logger.info("Using embedded fallback demo data")
    return [
        {'record_id': 'TECH-CT-001', 'department': 'commercial_taxes', 'business_name': 'TECH INNOVATIONS', 'pan': 'AAAA0001A', 'legal_status': 'Private Limited Company', 'business_type': 'IT Services', 'district': 'Bangalore Urban', 'city': 'Bangalore', 'pincode': '560001', 'status': 'Active', 'reg_date': '2020-03-15'},
        {'record_id': 'TECH-FB-001', 'department': 'factories_board', 'business_name': 'TECH INNOVATIONS PVT', 'pan': 'AAAA0001A', 'legal_status': 'Private Limited Company', 'business_type': 'IT Services', 'district': 'Bangalore Urban', 'city': 'Bangalore', 'pincode': '560001', 'status': 'Active', 'reg_date': '2020-04-10'},
        {'record_id': 'AGRO-CT-001', 'department': 'commercial_taxes', 'business_name': 'GREEN HARVEST AGRO', 'pan': 'BBBB0002B', 'legal_status': 'Private Limited Company', 'business_type': 'Agricultural', 'district': 'Mysuru', 'city': 'Mysuru', 'pincode': '570001', 'status': 'Active', 'reg_date': '2019-06-20'},
    ]


def emit_event(event_type,msg,data=None):
    try:
        ev={"type":event_type,"message":msg,"timestamp":datetime.now().isoformat(),"data":data or{}}
        for q, lp in list(event_queues):
            try:
                if lp and lp.is_running(): 
                    try:
                        lp.call_soon_threadsafe(q.put_nowait, ev)
                    except Exception as e:
                        logger.debug(f"Failed to queue event: {e}")
            except Exception as e:
                logger.debug(f"Event queue error: {e}")
        # Store event in current run
        if state.pipeline_runs and len(state.pipeline_runs) > 0:
            current_run = state.pipeline_runs[-1]
            if current_run and hasattr(current_run, 'status') and current_run.status=="running":
                try:
                    current_run.events.append(ev)
                except Exception as e:
                    logger.debug(f"Failed to append event to run: {e}")
    except Exception as e:
        logger.debug(f"emit_event failed: {e}")

def gen_activity(clusters):
    now=datetime.now();activity=[];random.seed(43)
    event_types=[
        {"type":"GST Filing","dept":"commercial_taxes","detail":"GSTR-3B monthly return filed"},
        {"type":"Factory Inspection","dept":"factories_board","detail":"Annual safety inspection completed"},
        {"type":"License Renewal","dept":"shops_establishments","detail":"Shop license renewed for 2025-26"},
        {"type":"EPF Deposit","dept":"labour_dept","detail":"Monthly EPF contribution deposited"},
        {"type":"Revenue Assessment","dept":"revenue_dept","detail":"Property tax assessment completed"},
        {"type":"GST e-Invoice","dept":"commercial_taxes","detail":"e-Invoice batch generated"},
        {"type":"Labour Compliance","dept":"labour_dept","detail":"PF/ESIC compliance return filed"},
    ]
    for idx,c in enumerate(clusters):
        n=c["departments_covered"];name=c["business_name"];ubid=c["ubid"]
        district=c["department_records"][0].get("district","Unknown") if c["department_records"] else "Unknown"
        # Distribute statuses: ~55% Active, ~20% Dormant, ~10% Closed, ~15% Uncertain
        roll=random.random()
        if roll<0.55:
            s,cf="ACTIVE",round(random.uniform(0.82,0.99),2)
            mo=random.randint(0,3)
            rule=f"Active signals from {n} dept(s) in last {mo+1} months"
            ld=(now-timedelta(days=random.randint(5,90))).strftime("%Y-%m-%d")
            lt=random.choice(["GST Filing","EPF Deposit","License Renewal","Factory Inspection"])
            ldpt=random.choice(DEPARTMENTS)
            n_sigs=random.randint(2,5)
            sigs=[]
            for _ in range(n_sigs):
                evt=random.choice(event_types)
                sigs.append({"type":evt["type"],"dept":evt["dept"],"date":(now-timedelta(days=random.randint(1,120))).strftime("%Y-%m-%d"),"detail":evt["detail"]})
        elif roll<0.75:
            s,cf="DORMANT",round(random.uniform(0.70,0.92),2)
            mo=random.randint(8,24)
            rule=f"No activity for {mo} months across {n} department(s)"
            ld=(now-timedelta(days=mo*30)).strftime("%Y-%m-%d")
            lt="GST Filing (GSTR-3B)";ldpt="commercial_taxes"
            sigs=[{"type":"GST Filing","dept":"commercial_taxes","date":ld,"detail":f"Last GSTR-3B filed {mo} months ago"},{"type":"Registration Check","dept":"shops_establishments","date":(now-timedelta(days=10)).strftime("%Y-%m-%d"),"detail":"Registration still ACTIVE — no filing activity"}]
        elif roll<0.85:
            s,cf="CLOSED",round(random.uniform(0.90,0.99),2)
            mo=random.randint(6,18)
            rule="Formal closure signals detected across departments"
            ld=(now-timedelta(days=mo*30)).strftime("%Y-%m-%d")
            lt="GST Cancellation";ldpt="commercial_taxes"
            sigs=[{"type":"GST Cancellation","dept":"commercial_taxes","date":ld,"detail":"Suo-moto cancellation under section 29(2)"},{"type":"Factory Licence Cancelled","dept":"factories_board","date":(now-timedelta(days=mo*30-15)).strftime("%Y-%m-%d"),"detail":"Licence cancelled — operations ceased"}]
        else:
            s,cf="UNCERTAIN",round(random.uniform(0.40,0.65),2)
            mo=random.randint(4,12)
            rule="Conflicting signals — active registrations but no recent filings"
            ld=(now-timedelta(days=mo*30)).strftime("%Y-%m-%d")
            lt="License Check";ldpt="shops_establishments"
            sigs=[{"type":"License Active","dept":"shops_establishments","date":(now-timedelta(days=30)).strftime("%Y-%m-%d"),"detail":"Shop licence valid but no GST filing"},{"type":"No GST Filing","dept":"commercial_taxes","date":ld,"detail":f"Last filing {mo} months ago — compliance gap"}]
        activity.append(ActivityRecord(ubid=ubid,business_name=name,status=s,confidence=cf,rule_override=rule,last_event_date=ld,last_event_type=lt,last_event_dept=ldpt,departments_active=n,months_since_activity=mo,signals=sigs,district=district))
    random.seed()
    return activity

def compute_risk(cluster,abi,evidence_list):
    risk=0;factors=[]
    if cluster["departments_covered"]>=3:risk+=15;factors.append({"factor":"Complex multi-dept entity","contribution":15,"severity":"medium"})
    if abi and abi.status=="UNCERTAIN":risk+=30;factors.append({"factor":"Conflicting ABI signals","contribution":30,"severity":"high"})
    elif abi and abi.status=="DORMANT":risk+=15;factors.append({"factor":"Dormant classification","contribution":15,"severity":"medium"})
    pincodes=set(r.get("pincode","")for r in cluster["department_records"]if r.get("pincode"))
    if len(pincodes)>1:risk+=20;factors.append({"factor":f"Address inconsistency: {len(pincodes)} pincodes ({', '.join(pincodes)})","contribution":20,"severity":"high"})
    names=set(r.get("business_name","")for r in cluster["department_records"])
    if len(names)>1:risk+=10;factors.append({"factor":f"Name variations: {len(names)} variants","contribution":10,"severity":"low"})
    for ev in evidence_list:
        if ev.get("record_a_id")in[r["record_id"]for r in cluster["department_records"]]:
            if not ev.get("safety_ok",True):risk+=15;factors.append({"factor":"Safety block on merge","contribution":15,"severity":"high"});break

    # Security Flags Engine — enhanced anomaly detection
    sec_flag="🟢 Clean";sec_reasons=[]
    pan_list=set(r.get("pan","") for r in cluster["department_records"] if r.get("pan"))
    # Rule: Same PAN, >2 different names → FLAG
    if len(pan_list)>0 and len(names)>2:
        sec_flag="🔴 Flagged";sec_reasons.append("Same PAN with >2 name variants across departments")
    # Rule: Closed entity with active PAN reuse (resurrection attack)
    elif abi and abi.status=="CLOSED":
        for other_c in [oc for oc in (cluster.get("_all_clusters") or []) if oc.get("ubid")!=cluster.get("ubid")]:
            other_pans=set(r.get("pan","") for r in other_c.get("department_records",[]) if r.get("pan"))
            if pan_list & other_pans:
                sec_flag="🔴 Flagged";sec_reasons.append("PAN matches a CLOSED entity — possible resurrection attack")
                break
        if sec_flag!="🔴 Flagged":
            sec_flag="🟡 Watch";sec_reasons.append("Entity is CLOSED — monitor for reactivation")
    # Rule: Single-department entity with address inconsistency
    elif cluster["departments_covered"]==1 and len(pincodes)>1:
        sec_flag="🟡 Watch";sec_reasons.append("Single-dept record with multiple pincodes — data quality issue")
    # Rule: New registration that is not in previous runs
    elif cluster["departments_covered"]==1 and abi and abi.months_since_activity==0:
        sec_flag="🟡 Watch";sec_reasons.append("New registration from single department — not yet corroborated")
    cluster["security_flag"]=sec_flag
    cluster["security_reasons"]=sec_reasons

    cluster["risk_factors"]=factors
    return min(100,risk)

def check_theme2(cluster):
    recs=cluster["department_records"]
    has_pan=any(r.get("pan_token")or r.get("pan")for r in recs);has_gstin=any(r.get("gstin_token")or r.get("gstin")for r in recs)
    has_email=any(r.get("email")for r in recs);has_phone=any(r.get("phone")for r in recs);has_udyam=any(r.get("udyam")for r in recs)
    score=sum([has_pan,has_gstin,has_email,has_phone,has_udyam])
    return{"ready":score>=4,"score":score,"max":5,"has_pan":has_pan,"has_gstin":has_gstin,"has_email":has_email,"has_phone":has_phone,"has_udyam":has_udyam}

def gen_alerts(st):
    alerts=[];now_s=datetime.now().isoformat()
    if st.dormant_count>0:
        pct=round(st.dormant_count/max(st.total_ubids,1)*100)
        if pct>=15:alerts.append(AlertItem(alert_id=f"ALT-{len(alerts):03d}",severity="warning",title=f"High Dormancy Rate: {pct}%",description=f"{st.dormant_count} of {st.total_ubids} businesses dormant. Consider targeted outreach.",triggered_at=now_s))
    for a in st.activity_records:
        if a.status=="CLOSED" and a.months_since_activity<6:alerts.append(AlertItem(alert_id=f"ALT-{len(alerts):03d}",severity="critical",title=f"Closed entity with recent signals",description=f"{a.business_name} ({a.ubid}) marked CLOSED but activity in last {a.months_since_activity}mo.",triggered_at=now_s))
    for a in st.activity_records:
        if a.status=="UNCERTAIN":alerts.append(AlertItem(alert_id=f"ALT-{len(alerts):03d}",severity="info",title=f"Uncertain: {a.business_name}",description=f"{a.rule_override}. Recommend verification.",triggered_at=now_s))
    pending=sum(1 for r in st.review_queue if r.status=="pending")
    if pending>0:alerts.append(AlertItem(alert_id=f"ALT-{len(alerts):03d}",severity="warning",title=f"{pending} pending reviews",description=f"Avg confidence: {sum(r.confidence for r in st.review_queue if r.status=='pending')/max(pending,1):.1%}",triggered_at=now_s))
    # Stale dept alert
    for d in st.dept_health:
        if d.stale_hours>=6:alerts.append(AlertItem(alert_id=f"ALT-{len(alerts):03d}",severity="warning",title=f"{deptLabel(d.department)} data stale",description=f"Last sync {d.last_sync}. Recommend triggering sync.",triggered_at=now_s))
    alerts.append(AlertItem(alert_id=f"ALT-{len(alerts):03d}",severity="info",title="False merge cost: HIGH",description=f"Conservative threshold active. {st.blocked_by_safety} safety blocks this run. Estimated cost of unsafe merge: ₹2.4L per false positive.",triggered_at=now_s))
    return alerts

def deptLabel(d):
    m={"commercial_taxes":"Comm. Taxes","factories_board":"Factories Board","shops_establishments":"Shops & Est.","labour_dept":"Labour Dept","revenue_dept":"Revenue Dept"}
    return m.get(d,d)

def gen_dept_health(records):
    depts={};now=datetime.now()
    for r in records:
        d=r["department"]
        if d not in depts:depts[d]={"count":0,"rids":[]}
        depts[d]["count"]+=1;depts[d]["rids"].append(r["record_id"])
    health=[]
    dept_meta={"commercial_taxes":{"sync_ago":1,"quality":"Good","qs":92,"stale":1},"factories_board":{"sync_ago":3,"quality":"Good","qs":85,"stale":3},"shops_establishments":{"sync_ago":2,"quality":"Fair","qs":72,"stale":2},"labour_dept":{"sync_ago":6,"quality":"Fair","qs":68,"stale":6},"revenue_dept":{"sync_ago":8,"quality":"Stale","qs":55,"stale":8}}
    for d,info in depts.items():
        meta=dept_meta.get(d,{"sync_ago":12,"quality":"Unknown","qs":50,"stale":12})
        health.append(DeptHealth(department=d,records=info["count"],match_rate=round(random.uniform(0.6,0.95),2),last_sync=f"{meta['sync_ago']}h ago",quality=meta["quality"],quality_score=meta["qs"],stale_hours=meta["stale"],record_ids=info["rids"]))
    return health

# ═══════════════════════════════════════════════════════════════════════
#  Pipeline
# ═══════════════════════════════════════════════════════════════════════
def run_pipeline():
    global state
    state.is_processing=True
    start_time=time.time()
    prev_u=state.total_ubids;prev_a=state.active_count;prev_d=state.dormant_count;prev_c=state.closed_count;prev_uc=state.uncertain_count
    run=PipelineRun(run_id=f"RUN-{len(state.pipeline_runs)+1:03d}",started_at=datetime.now().isoformat())
    state.pipeline_runs.append(run)
    try:
        try:
            emit_event("pipeline","⚡ Pipeline started — ingesting records from 5 departments")
        except Exception as e:
            logger.error(f"Failed to emit start event: {e}")
        time.sleep(0.5)  # Brief pause for frontend
        records=gen_records();state.records=records;state.total_records=len(records)
        try:
            emit_event("ingest",f"📥 Ingested {len(records)} records from 5 departments")
        except Exception as e:
            logger.error(f"Failed to emit ingest event: {e}")
        tokenized=[tokenizer.tokenize_record(r)for r in records];state.tokenized_records=tokenized
        try:
            emit_event("tokenize",f"🔒 PII tokenized — {len(tokenized)} records masked")
        except Exception as e:
            logger.error(f"Failed to emit tokenize event: {e}")
        pairs=blocker.generate_candidate_pairs(tokenized);run.records_processed=len(records);run.pairs_analyzed=len(pairs)
        try:
            emit_event("blocking",f"🔗 Blocking complete — {len(pairs)} candidate pairs")
        except Exception as e:
            logger.error(f"Failed to emit blocking event: {e}")
        ubid_map={};clusters_by_ubid={};link_evidence=[];review_queue=[]
        auto_linked=sent_to_review=rejected=blocked_by_safety=0;confidences=[]
        for ia,ib in pairs:
            ra,rb=tokenized[ia],tokenized[ib]
            conf,expl,feats,xai=match_confidence(ra,rb)
            fv=np.zeros(16,dtype=np.float32);pa,pb=ra.get("pan_token"),rb.get("pan_token");fv[0]=1.0 if(pa and pb and pa==pb)else 0.0
            safety=safety_engine.analyze_merge_safety(ra,rb,fv,conf)
            dec="reject"
            if conf>=state.auto_threshold and safety.is_safe:dec="auto_link";auto_linked+=1;emit_event("auto_link",f"✅ Auto-linked: {records[ia]['business_name']} ↔ {records[ib]['business_name']} ({conf:.0%})")
            elif conf>=state.review_threshold:
                if not safety.is_safe:blocked_by_safety+=1;emit_event("safety",f"🛡️ Safety block: {records[ia]['business_name']} ↔ {records[ib]['business_name']}")
                dec="review";sent_to_review+=1;emit_event("review",f"⚠️ Sent to review: {records[ia]['business_name']} ↔ {records[ib]['business_name']} ({conf:.0%})")
            else:rejected+=1
            confidences.append(conf)
            ev={"record_a":ra.get("business_name",""),"record_b":rb.get("business_name",""),"dept_a":ra.get("department",""),"dept_b":rb.get("department",""),"record_a_id":records[ia]["record_id"],"record_b_id":records[ib]["record_id"],"confidence":round(conf,4),"decision":dec,"explanation":expl,"features":feats,"xai_factors":xai,"safety_ok":safety.is_safe,"safety_blocks":[ne.description for ne in safety.negative_evidence]if not safety.is_safe else[]}
            link_evidence.append(ev)
            run.pair_details.append({"a":records[ia]["business_name"],"b":records[ib]["business_name"],"conf":round(conf,4),"decision":dec})
            if dec=="auto_link":
                assign_cluster(ia,ib,records,ubid_map,clusters_by_ubid)
                ubid_val=ubid_map.get(ia,"")
                state.audit_log.append(AuditEntry(timestamp=datetime.now().isoformat(),ubid=ubid_val,action="record_ingested",details=f"Record {records[ia]['record_id']} ingested from {deptLabel(records[ia]['department'])}",actor="SYSTEM",icon="📥"))
                state.audit_log.append(AuditEntry(timestamp=(datetime.now()+timedelta(seconds=1)).isoformat(),ubid=ubid_val,action="record_ingested",details=f"Record {records[ib]['record_id']} ingested from {deptLabel(records[ib]['department'])}",actor="SYSTEM",icon="📥"))
                state.audit_log.append(AuditEntry(timestamp=(datetime.now()+timedelta(seconds=2)).isoformat(),ubid=ubid_val,action="auto_linked",details=f"Matched {records[ia]['record_id']} ↔ {records[ib]['record_id']} — confidence {conf:.1%}",actor="SYSTEM",icon="✅",before="separate",after="merged"))
            elif dec=="review":
                rscore=min(100,int((1-conf)*150)+random.randint(0,15))
                ri=ReviewItem(review_id=f"REV-{len(review_queue):03d}",record_a=ra.get("business_name",""),record_b=rb.get("business_name",""),dept_a=ra.get("department",""),dept_b=rb.get("department",""),record_a_id=records[ia]["record_id"],record_b_id=records[ib]["record_id"],confidence=round(conf,4),explanation=expl,features=feats,risk_score=rscore,rec_a_details={k:v for k,v in records[ia].items()},rec_b_details={k:v for k,v in records[ib].items()},xai_factors=xai)
                review_queue.append(ri)
        for i,rec in enumerate(records):
            if i not in ubid_map:
                u=f"UBID-KA-{uuid.uuid4().hex[:12].upper()}";ubid_map[i]=u
                clusters_by_ubid[u]={"ubid":u,"business_name":rec["business_name"],"status":"ACTIVE","department_records":[{**rec}],"total_records":1,"departments_covered":1}
                state.audit_log.append(AuditEntry(timestamp=datetime.now().isoformat(),ubid=u,action="record_ingested",details=f"Record {rec['record_id']} ingested from {deptLabel(rec['department'])} — standalone UBID created",actor="SYSTEM",icon="📥"))
        # Assign UBIDs to review items
        for ri in review_queue:
            for c in clusters_by_ubid.values():
                rids=[r["record_id"] for r in c["department_records"]]
                if ri.record_a_id in rids or ri.record_b_id in rids:
                    ri.ubid=c["ubid"];break
            if not ri.ubid:
                ri.ubid=ubid_map.get(0,"")
            state.audit_log.append(AuditEntry(timestamp=datetime.now().isoformat(),ubid=ri.ubid,action="sent_to_review",details=f"Pair {ri.record_a_id} ↔ {ri.record_b_id} sent to review — confidence {ri.confidence:.1%}",actor="SYSTEM",icon="⚠️"))
        state.ubid_clusters=list(clusters_by_ubid.values());state.link_evidence=link_evidence;state.review_queue=review_queue
        state.total_ubids=len(clusters_by_ubid);state.total_links=auto_linked+sent_to_review
        state.auto_linked=auto_linked;state.sent_to_review=sent_to_review;state.rejected=rejected
        state.blocked_by_safety=blocked_by_safety;state.avg_confidence=float(np.mean(confidences))if confidences else 0
        activity=gen_activity(state.ubid_clusters);state.activity_records=activity
        state.active_count=sum(1 for a in activity if a.status=="ACTIVE");state.dormant_count=sum(1 for a in activity if a.status=="DORMANT")
        state.closed_count=sum(1 for a in activity if a.status=="CLOSED");state.uncertain_count=sum(1 for a in activity if a.status=="UNCERTAIN")
        for c in state.ubid_clusters:
            abi=next((a for a in activity if a.ubid==c["ubid"]),None)
            c["risk_score"]=compute_risk(c,abi,link_evidence);c["theme2"]=check_theme2(c)
            # Add ABI timeline entry
            if abi:
                state.audit_log.append(AuditEntry(timestamp=(datetime.now()+timedelta(seconds=3)).isoformat(),ubid=c["ubid"],action="abi_classified",details=f"ABI Status set: {abi.status} ({abi.confidence:.0%}) — {abi.rule_override}",actor="SYSTEM",icon="📊"))
            run.entities_processed.append(c["ubid"])
        state.delta_ubids=state.total_ubids-prev_u;state.delta_active=state.active_count-prev_a;state.delta_dormant=state.dormant_count-prev_d;state.delta_closed=state.closed_count-prev_c;state.delta_uncertain=state.uncertain_count-prev_uc
        # Status changes
        if prev_a>0:
            if state.dormant_count>prev_d:run.status_changes.append(f"{state.dormant_count-prev_d} businesses changed to DORMANT")
            if state.active_count>prev_a:run.status_changes.append(f"{state.active_count-prev_a} businesses changed to ACTIVE")
        state.alerts=gen_alerts(state);state.dept_health=gen_dept_health(records)
        emit_event("abi",f"📊 ABI classified — Active:{state.active_count} Dormant:{state.dormant_count} Closed:{state.closed_count} Uncertain:{state.uncertain_count}")
        run.auto_linked=auto_linked;run.sent_to_review=sent_to_review;run.rejected=rejected;run.blocked_by_safety=blocked_by_safety
        run.abi_active=state.active_count;run.abi_dormant=state.dormant_count;run.abi_closed=state.closed_count;run.abi_uncertain=state.uncertain_count
        run.finished_at=datetime.now().isoformat();run.status="completed";run.duration_ms=int((time.time()-start_time)*1000)
        emit_event("complete",f"✅ Pipeline complete — {state.total_ubids} UBIDs, {auto_linked} auto-linked, {sent_to_review} review, {rejected} rejected")
    except Exception as e:
        logger.error(f"Pipeline error: {e}",exc_info=True);run.status="failed";run.finished_at=datetime.now().isoformat();emit_event("error",f"❌ Pipeline failed: {e}")
    finally:
        state.is_processing=False

# ═══════════════════════════════════════════════════════════════════════
#  Matching
# ═══════════════════════════════════════════════════════════════════════
def match_confidence(ra,rb):
    sc,fac,ft,xai=0.0,[],{},[]
    pa,pb=ra.get("pan_token"),rb.get("pan_token");pm=bool(pa and pb and pa==pb);ft["pan_match"]=pm
    if pm:sc+=0.55;fac.append("PAN: EXACT MATCH (+0.55)");xai.append({"feature":"PAN Token Match","contribution":0.55,"direction":"positive","bar_pct":55})
    elif pa and pb:sc-=0.40;fac.append("PAN: DIFFERENT (-0.40)");xai.append({"feature":"PAN Token Conflict","contribution":-0.40,"direction":"negative","bar_pct":40})
    ga,gb=ra.get("gstin_token"),rb.get("gstin_token");gm=bool(ga and gb and ga==gb);ft["gstin_match"]=gm
    if gm:sc+=0.15;fac.append("GSTIN: MATCH (+0.15)");xai.append({"feature":"GSTIN Match","contribution":0.15,"direction":"positive","bar_pct":15})
    na,nb=ra.get("business_name","").upper(),rb.get("business_name","").upper()
    ns=name_sim(na,nb);ft["name_similarity"]=round(ns,3);nc=ns*0.30;sc+=nc;fac.append(f"Name sim: {ns:.2f} (+{nc:.3f})");xai.append({"feature":"Name Similarity","contribution":round(nc,3),"direction":"positive","bar_pct":int(nc*100),"detail":f"Jaro-Winkler: {ns:.2f}"})
    for k,l,b in[("district","District",0.05),("pincode","Pincode",0.05),("phone","Phone",0.05)]:
        a2,b2=ra.get(k,""),rb.get(k,"");m=bool(a2 and b2 and a2==b2);ft[f"{k}_match"]=m
        if m:sc+=b;fac.append(f"{l}: MATCH (+{b})");xai.append({"feature":f"{l} Match","contribution":b,"direction":"positive","bar_pct":int(b*100)})
    la,lb=ra.get("legal_status","").upper(),rb.get("legal_status","").upper();lm=bool(la and lb and la==lb);ft["legal_status_match"]=lm
    if lm:sc+=0.05;fac.append("Legal status: MATCH (+0.05)");xai.append({"feature":"Legal Status Match","contribution":0.05,"direction":"positive","bar_pct":5})
    elif la and lb:sc-=0.10;fac.append("Legal status: CONFLICT (-0.10)");xai.append({"feature":"Legal Status Conflict","contribution":-0.10,"direction":"negative","bar_pct":10})
    return max(0,min(1,sc)),"; ".join(fac),ft,xai

def name_sim(a,b):
    if not a or not b:return 0
    if a==b:return 1
    for x,y in[("PVT","PRIVATE"),("LTD","LIMITED"),("PVT.","PRIVATE")]:a,b=a.replace(x,y),b.replace(x,y)
    ta,tb=set(a.split()),set(b.split())
    if not ta or not tb:return 0
    j=len(ta&tb)/len(ta|tb);p=sum(1 for ca,cb in zip(a,b)if ca==cb)
    return min(1,j+min(0.15,p/max(len(a),len(b))))

def assign_cluster(ia,ib,recs,um,cl):
    ua,ub=um.get(ia),um.get(ib)
    def rd(i):return{**recs[i]}
    if ua and ub:
        if ua!=ub:
            for r in cl[ub]["department_records"]:cl[ua]["department_records"].append(r)
            ds=set(r["department"]for r in cl[ua]["department_records"]);cl[ua]["total_records"]=len(cl[ua]["department_records"]);cl[ua]["departments_covered"]=len(ds)
            for k,v in list(um.items()):
                if v==ub:um[k]=ua
            del cl[ub]
        return
    elif ua:tgt,ni=ua,ib
    elif ub:tgt,ni=ub,ia
    else:
        tgt=f"UBID-KA-{uuid.uuid4().hex[:12].upper()}";r=recs[ia]
        cl[tgt]={"ubid":tgt,"business_name":r["business_name"],"status":"ACTIVE","department_records":[rd(ia)],"total_records":1,"departments_covered":1}
        um[ia]=tgt;ni=ib
    cl[tgt]["department_records"].append(rd(ni))
    ds=set(r["department"]for r in cl[tgt]["department_records"]);cl[tgt]["total_records"]=len(cl[tgt]["department_records"]);cl[tgt]["departments_covered"]=len(ds)
    um[ni]=tgt

# ═══════════════════════════════════════════════════════════════════════
#  NL Query Parser
# ═══════════════════════════════════════════════════════════════════════
DISTRICTS=["bangalore urban","mysuru","dharwad","tumakuru","mangaluru","belagavi","kalaburagi","ballari","raichur","shivamogga","davanagere","hassan","mandya","kodagu","udupi"]
STATUS_WORDS={"active":"ACTIVE","dormant":"DORMANT","closed":"CLOSED","uncertain":"UNCERTAIN","inactive":"DORMANT","cancelled":"CLOSED","conflicting":"UNCERTAIN"}
DEPT_WORDS={"factory":"factories_board","factories":"factories_board","manufacturing":"factories_board","gst":"commercial_taxes","tax":"commercial_taxes","shop":"shops_establishments","labour":"labour_dept","epf":"labour_dept","revenue":"revenue_dept"}

def parse_nl(text):
    t=text.lower();params={};interpreted=[]
    for w,s in STATUS_WORDS.items():
        if w in t:params["status"]=s;interpreted.append(f"status={s}");break
    for d in DISTRICTS:
        if d in t:params["district"]=d.title();interpreted.append(f"district={d.title()}");break
    for w,dep in DEPT_WORDS.items():
        if w in t:params["department"]=dep;interpreted.append(f"department={dep}");break
    pin=re.search(r'\b(\d{6})\b',t)
    if pin:params["pincode"]=pin.group(1);interpreted.append(f"pincode={pin.group(1)}")
    mo=re.search(r'(\d+)\s*months?',t)
    if mo:params["months"]=int(mo.group(1));interpreted.append(f"inactive>{mo.group(1)}mo")
    if not params and t.strip():
        params["q"]=t.strip();interpreted.append(f"keyword='{t.strip()}'")
    return params,interpreted

def gen_cypher(params):
    clauses=["MATCH (b:Business)-[:REGISTERED_WITH]->(d:Department)"]
    wheres=[]
    if params.get("status"):wheres.append(f"b.abi_status = '{params['status']}'")
    if params.get("district"):wheres.append(f"b.district = '{params['district']}'")
    if params.get("pincode"):wheres.append(f"b.pincode = '{params['pincode']}'")
    if params.get("department"):wheres.append(f"d.name = '{params['department']}'")
    if params.get("business_type"):wheres.append(f"b.sector CONTAINS '{params['business_type']}'")
    if wheres:clauses.append("WHERE "+" AND ".join(wheres))
    clauses.append("RETURN b.ubid, b.name, b.abi_status, collect(d.name) AS departments")
    return "\n".join(clauses)

# ═══════════════════════════════════════════════════════════════════════
#  API Routes
# ═══════════════════════════════════════════════════════════════════════
@app.get("/api/health")
async def health():return{"status":"online","ts":time.time()}

@app.get("/api/stats")
async def get_stats():
    return{"total_ubids":state.total_ubids,"total_records":state.total_records,"total_links":state.total_links,"avg_confidence":round(state.avg_confidence,4),"auto_linked":state.auto_linked,"sent_to_review":state.sent_to_review,"rejected":state.rejected,"blocked_by_safety":state.blocked_by_safety,"pipeline_runs":len(state.pipeline_runs),"is_processing":state.is_processing,"active_count":state.active_count,"dormant_count":state.dormant_count,"closed_count":state.closed_count,"uncertain_count":state.uncertain_count,"pending_reviews":sum(1 for r in state.review_queue if r.status=="pending"),"delta_ubids":state.delta_ubids,"delta_active":state.delta_active,"delta_dormant":state.delta_dormant,"delta_closed":state.delta_closed,"delta_uncertain":state.delta_uncertain,"alert_count":len([a for a in state.alerts if not a.acknowledged]),"false_positive_cost":"₹2.4L per unsafe merge","departments":len(set(r["department"]for r in state.records))if state.records else 0,"auto_threshold":state.auto_threshold,"review_threshold":state.review_threshold,"rollback_count":state.rollback_count,"current_role":"Admin"}

@app.get("/api/sparklines")
async def get_sparklines():
    # Return 6-point sparkline data for each ABI category simulating historical trend
    runs=len(state.pipeline_runs)
    if runs==0:
        return{"active":[],"dormant":[],"closed":[],"uncertain":[]}
    # Generate synthetic sparkline data based on current counts
    a,d,c,u=state.active_count,state.dormant_count,state.closed_count,state.uncertain_count
    def spark(val):
        pts=[max(0,val+random.randint(-max(3,val//4),max(3,val//4)))for _ in range(5)]
        pts.append(val)
        return pts
    return{"active":spark(a),"dormant":spark(d),"closed":spark(c),"uncertain":spark(u)}

@app.post("/api/pipeline/run")
async def trigger_pipeline(bg:BackgroundTasks):
    if state.is_processing:return JSONResponse(status_code=409,content={"error":"Already running"})
    bg.add_task(run_pipeline);return{"message":"Started"}

# SSE
@app.get("/api/events")
async def sse_events(request:Request):
    q=asyncio.Queue(); lp=asyncio.get_running_loop(); q_entry=(q, lp)
    event_queues.append(q_entry)
    async def gen():
        try:
            while True:
                if await request.is_disconnected():break
                try:ev=await asyncio.wait_for(q.get(),timeout=30);yield{"event":ev["type"],"data":json.dumps(ev)}
                except asyncio.TimeoutError:yield{"event":"ping","data":"{}"}
        finally:
            if q_entry in event_queues:event_queues.remove(q_entry)
    return EventSourceResponse(gen())

@app.get("/api/clusters")
async def get_clusters():return state.ubid_clusters

@app.get("/api/cluster-detail")
async def get_cluster_detail(ubid:str):
    c=next((x for x in state.ubid_clusters if x["ubid"]==ubid),None)
    if not c:return JSONResponse(status_code=404,content={"error":"Not found"})
    abi=next((a for a in state.activity_records if a.ubid==ubid),None)
    rids=[r["record_id"]for r in c["department_records"]]
    evs=[e for e in state.link_evidence if e["record_a_id"]in rids or e["record_b_id"]in rids]
    timeline=sorted([asdict(a)for a in state.audit_log if a.ubid==ubid],key=lambda x:x["timestamp"])
    reviews=[asdict(r) for r in state.review_queue if r.ubid==ubid]
    return{**c,"abi":asdict(abi)if abi else None,"evidence":evs,"timeline":timeline,"reviews":reviews}

@app.get("/api/evidence")
async def get_evidence():return state.link_evidence

def mask_pan(pan):
    if not pan or len(pan)<6:return pan
    return pan[:4]+"●●●●"+pan[-2:]

def mask_gstin(gstin):
    if not gstin or len(gstin)<10:return gstin
    return gstin[:5]+"●●●●●●●●"+gstin[-2:]

@app.get("/api/records")
async def get_records(reveal:bool=False):
    # Check PII reveal state
    show_raw=reveal and state.pii_revealed and "Admin"=="SUPER_ADMIN"
    if state.pii_reveal_time and time.time()-state.pii_reveal_time>30:
        state.pii_revealed=False;state.pii_reveal_time=None
    result=[]
    for i,raw in enumerate(state.records):
        tok=state.tokenized_records[i]if i<len(state.tokenized_records)else{}
        raw_data={k:v for k,v in raw.items()if k in("record_id","department","business_name","pan","gstin","udyam","district","status","pincode")}
        if not show_raw:
            if raw_data.get("pan"):raw_data["pan"]=mask_pan(raw_data["pan"])
            if raw_data.get("gstin"):raw_data["gstin"]=mask_gstin(raw_data["gstin"])
        result.append({"raw":raw_data,"tokenized":{k:v for k,v in tok.items()if k in("record_id","department","business_name","pan_token","gstin_token","district","status")},"masked":not show_raw})
    return result

@app.get("/api/activity")
async def get_activity(status:Optional[str]=None,months:Optional[int]=None,district:Optional[str]=None,pincode:Optional[str]=None):
    res=state.activity_records
    if status:res=[a for a in res if a.status==status.upper()]
    if months:res=[a for a in res if a.months_since_activity<=months]
    if district:
        def has_d(a):return district.lower() in a.district.lower()
        res=[a for a in res if has_d(a)]
    if pincode:
        def has_p(ubid):
            c=next((x for x in state.ubid_clusters if x["ubid"]==ubid),None)
            return c and any(r.get("pincode","")==pincode for r in c["department_records"])
        res=[a for a in res if has_p(a.ubid)]
    return[asdict(a)for a in res]

@app.get("/api/reviews")
async def get_reviews(status:Optional[str]=None):
    items=state.review_queue
    if status:items=[r for r in items if r.status==status]
    return[asdict(r)for r in items]

class ReviewBody(BaseModel):
    decision:str;reviewer:str="analyst_001";notes:str="";feedback_tags:List[str]=[]

@app.post("/api/reviews/{rid}/decide")
async def decide_review(rid:str,body:ReviewBody):
    for r in state.review_queue:
        if r.review_id==rid:
            r.status=body.decision;r.reviewer=body.reviewer;r.reviewer_notes=body.notes;r.feedback_tags=body.feedback_tags;r.reviewed_at=datetime.now().isoformat()
            # Add to timeline
            state.audit_log.append(AuditEntry(timestamp=datetime.now().isoformat(),ubid=r.ubid,action=f"reviewer_{body.decision}",details=f"Reviewer {body.reviewer}: {r.record_a_id} ↔ {r.record_b_id} → {body.decision}" + (f" — Notes: \"{body.notes}\"" if body.notes else ""),actor=body.reviewer,icon="👤"))
            # Update reviewer stats
            s=state.reviewer_stats;s["total_reviewed"]+=1;s["session_reviewed"]+=1
            if body.decision in("merged","approved"):s["merges"]+=1;s["session_merges"]+=1
            elif body.decision=="separated":s["separations"]+=1;s["session_separations"]+=1
            else:s["defers"]+=1
            # Update link evidence with reviewer notes
            for ev in state.link_evidence:
                if ev["record_a_id"]==r.record_a_id and ev["record_b_id"]==r.record_b_id:
                    ev["reviewer_notes"]=body.notes
                    ev["reviewer_decision"]=body.decision
                    ev["reviewer"]=body.reviewer
                    break
            emit_event("review_decided",f"👤 {body.reviewer}: {body.decision} — {r.record_a} ↔ {r.record_b}")
            return{"ok":True,"decision":body.decision,"reviewer":body.reviewer,"pending_count":sum(1 for x in state.review_queue if x.status=="pending")}
    return JSONResponse(status_code=404,content={"error":"Not found"})

@app.post("/api/reviews/{rid}/undo")
async def undo_review(rid:str):
    for r in state.review_queue:
        if r.review_id==rid and r.status in ("merged","separated"):
            old_dec = r.status
            r.status="pending"; r.reviewed_at=None; r.reviewer_notes=None; r.reviewer=None
            role_name = "Admin"
            state.audit_log.append(AuditEntry(timestamp=datetime.now().isoformat(),ubid=r.ubid,action="reviewer_undo",details=f"Decision rolled back by {role_name}: {old_dec} → pending",actor="Admin",icon="↩"))
            s=state.reviewer_stats
            if old_dec=="merged":s["merges"]-=1; s["session_merges"]-=1
            else:s["separations"]-=1; s["session_separations"]-=1
            for ev in state.link_evidence:
                if ev.get("record_a_id")==r.record_a_id and ev.get("record_b_id")==r.record_b_id:
                    ev.pop("reviewer_notes",None); ev.pop("reviewer_decision",None); ev.pop("reviewer",None)
            state.rollback_count+=1
            emit_event("review_undone",f"↩ {role_name} undid {old_dec} for {r.record_a} ↔ {r.record_b}")
            return {"ok":True,"status":"pending","rollback_count":state.rollback_count}
    return JSONResponse(status_code=404,content={"error":"Not found"})

@app.get("/api/reviewer-stats")
async def reviewer_stats():return state.reviewer_stats

@app.get("/api/query")
async def bi_query(pincode:Optional[str]=None,district:Optional[str]=None,status:Optional[str]=None,department:Optional[str]=None,q:Optional[str]=None,business_type:Optional[str]=None,sector:Optional[str]=None):
    results=[]
    for c in state.ubid_clusters:
        ok=True;recs=c["department_records"]
        if pincode and not any(r.get("pincode")==pincode for r in recs):ok=False
        if district and not any(district.lower()in r.get("district","").lower()for r in recs):ok=False
        if department and not any(department.lower()in r.get("department","").lower()for r in recs):ok=False
        if business_type and not any(business_type.lower()in(r.get("business_type","")+r.get("sector","")).lower()for r in recs):ok=False
        if sector and not any(sector.lower()in r.get("sector","").lower()for r in recs):ok=False
        if q and q.lower()not in c["business_name"].lower()and not any(q.upper()in(r.get("record_id","").upper()+r.get("pan","").upper()+r.get("gstin","").upper()+r.get("udyam","").upper())for r in recs):ok=False
        if status:
            abi=next((a for a in state.activity_records if a.ubid==c["ubid"]),None)
            if not abi or abi.status!=status.upper():ok=False
        if ok:
            abi=next((a for a in state.activity_records if a.ubid==c["ubid"]),None)
            results.append({**c,"abi_status":abi.status if abi else"UNKNOWN","abi_confidence":abi.confidence if abi else 0,"last_event":abi.last_event_type if abi else"","last_event_date":abi.last_event_date if abi else""})
    return{"count":len(results),"results":results}

@app.get("/api/nl-query")
async def nl_query(q:str=""):
    if not q:return{"params":{},"interpreted":[],"cypher":"","results":[]}
    params,interpreted=parse_nl(q);cypher=gen_cypher(params)
    results=[];st=params.get("status");di=params.get("district");pin=params.get("pincode");dep=params.get("department");query_text=params.get("q")
    for c in state.ubid_clusters:
        ok=True;recs=c["department_records"]
        if pin and not any(r.get("pincode")==pin for r in recs):ok=False
        if di and not any(di.lower()in r.get("district","").lower()for r in recs):ok=False
        if dep and not any(dep.lower()in r.get("department","").lower()for r in recs):ok=False
        if query_text and query_text not in c["business_name"].lower() and not any(query_text in r.get("business_name","").lower() or query_text in r.get("pan","").lower() or query_text in r.get("gstin","").lower() for r in recs):ok=False
        if st:
            abi=next((a for a in state.activity_records if a.ubid==c["ubid"]),None)
            if not abi or abi.status!=st:ok=False
        if ok:
            abi=next((a for a in state.activity_records if a.ubid==c["ubid"]),None)
            results.append({"ubid":c["ubid"],"name":c["business_name"],"records":c["total_records"],"depts":c["departments_covered"],"abi_status":abi.status if abi else"UNKNOWN","risk":c.get("risk_score",0)})
    return{"params":params,"interpreted":interpreted,"cypher":cypher,"count":len(results),"results":results}

@app.get("/api/guided")
async def guided_queries():
    return[
        {"id":"active_factories","label":"Active factories by district","params":{"status":"ACTIVE","sector":"Manufacturing"},"cypher":gen_cypher({"status":"ACTIVE","business_type":"Manufacturing"})},
        {"id":"dormant_18m","label":"Dormant businesses (18+ months)","params":{"status":"DORMANT"},"cypher":gen_cypher({"status":"DORMANT"})},
        {"id":"closed_recent","label":"Recently closed units","params":{"status":"CLOSED"},"cypher":gen_cypher({"status":"CLOSED"})},
        {"id":"uncertain","label":"Businesses with conflicting signals","params":{"status":"UNCERTAIN"},"cypher":gen_cypher({"status":"UNCERTAIN"})},
        {"id":"pin_560058","label":"Active businesses in PIN 560058","params":{"pincode":"560058","status":"ACTIVE"},"cypher":gen_cypher({"pincode":"560058","status":"ACTIVE"})},
        {"id":"mysuru","label":"All businesses in Mysuru district","params":{"district":"Mysuru"},"cypher":gen_cypher({"district":"Mysuru"})},
        {"id":"tumakuru_mfg","label":"Factories in Tumakuru","params":{"district":"Tumakuru","sector":"Manufacturing"},"cypher":gen_cypher({"district":"Tumakuru","business_type":"Manufacturing"})},
    ]

@app.get("/api/alerts")
async def get_alerts():return[asdict(a)for a in state.alerts]
@app.post("/api/alerts/{aid}/ack")
async def ack_alert(aid:str):
    for a in state.alerts:
        if a.alert_id==aid:a.acknowledged=True;return{"ok":True}
    return JSONResponse(status_code=404,content={"error":"Not found"})

@app.get("/api/audit")
async def get_audit(ubid:Optional[str]=None):
    entries=state.audit_log
    if ubid:entries=[e for e in entries if e.ubid==ubid]
    return[asdict(e)for e in sorted(entries,key=lambda x:x.timestamp)]

class AccessLogBody(BaseModel):
    role:str;action:str;entity:str
@app.post("/api/access-logs")
async def add_access_log(body:AccessLogBody,req:Request):
    ip=req.client.host if req.client else "127.0.0.1"
    state.access_logs.append({"timestamp":datetime.now().isoformat(),"actor":body.role,"action":body.action,"entity":body.entity,"ip":ip})
    return {"ok":True}
@app.get("/api/access-logs")
async def get_access_logs():
    return list(reversed(state.access_logs))

@app.get("/api/pipeline/history")
async def pipeline_history():
    return[{k:v for k,v in asdict(r).items() if k not in ("events","pair_details")} for r in reversed(state.pipeline_runs)]

@app.get("/api/pipeline/run-detail")
async def pipeline_run_detail(run_id:str):
    r=next((x for x in state.pipeline_runs if x.run_id==run_id),None)
    if not r:return JSONResponse(status_code=404,content={"error":"Not found"})
    return asdict(r)

@app.get("/api/sparklines")
async def sparklines():
    runs=state.pipeline_runs
    return{"active":[r.abi_active for r in runs],"dormant":[r.abi_dormant for r in runs],"closed":[r.abi_closed for r in runs],"uncertain":[r.abi_uncertain for r in runs],"auto":[r.auto_linked for r in runs],"review":[r.sent_to_review for r in runs],"reject":[r.rejected for r in runs]}

@app.get("/api/dept-health")
async def dept_health():return[asdict(d)for d in state.dept_health]

@app.get("/api/dept-records")
async def dept_records(department:str):
    recs=[r for r in state.records if r["department"]==department]
    result=[]
    for r in recs:
        ubid_match=""
        for c in state.ubid_clusters:
            if any(cr["record_id"]==r["record_id"] for cr in c["department_records"]):
                ubid_match=c["ubid"];break
        abi=next((a for a in state.activity_records if a.ubid==ubid_match),None)
        result.append({**r,"ubid":ubid_match,"abi_status":abi.status if abi else "UNKNOWN"})
    return result

@app.post("/api/dept-sync")
async def dept_sync(department:str):
    for d in state.dept_health:
        if d.department==department:
            d.last_sync="0h ago";d.quality="Good";d.quality_score=min(100,d.quality_score+15);d.stale_hours=0
            emit_event("sync",f"🔄 Sync triggered for {deptLabel(department)} — data refreshed")
            return{"ok":True,"department":department,"status":"synced"}
    return JSONResponse(status_code=404,content={"error":"Department not found"})

@app.get("/api/districts")
async def get_districts():
    """District heatmap data with ABI status counts."""
    district_data={}
    for d in KARNATAKA_DISTRICTS:
        district_data[d["name"]]={"name":d["name"],"code":d["code"],"row":d["row"],"col":d["col"],"active":0,"dormant":0,"closed":0,"uncertain":0,"total":0}
    for a in state.activity_records:
        dist=a.district
        if dist in district_data:
            district_data[dist]["total"]+=1
            if a.status=="ACTIVE":district_data[dist]["active"]+=1
            elif a.status=="DORMANT":district_data[dist]["dormant"]+=1
            elif a.status=="CLOSED":district_data[dist]["closed"]+=1
            elif a.status=="UNCERTAIN":district_data[dist]["uncertain"]+=1
    return list(district_data.values())

@app.get("/api/graph-data")
async def graph_data():
    nodes=[];edges=[]
    for c in state.ubid_clusters:
        abi=next((a for a in state.activity_records if a.ubid==c["ubid"]),None)
        st=abi.status if abi else"UNKNOWN"
        colors={"ACTIVE":"#34d399","DORMANT":"#fbbf24","CLOSED":"#f87171","UNCERTAIN":"#a78bfa","UNKNOWN":"#64748b"}
        nodes.append({"id":c["ubid"],"label":c["business_name"][:20],"color":colors.get(st,"#64748b"),"size":15+c["total_records"]*5,"status":st,"risk":c.get("risk_score",0),"records":c["total_records"],"depts":c["departments_covered"],"full_name":c["business_name"],"type":"business"})
        for r in c["department_records"]:
            did=r["record_id"];nodes.append({"id":did,"label":r["department"].replace("_"," ").title()[:15],"color":"rgba(255,255,255,0.15)","size":8,"type":"dept","department":r["department"],"full_name":r.get("business_name","")})
            edges.append({"from":c["ubid"],"to":did,"width":2,"color":"rgba(129,140,248,0.3)"})
    for ev in state.link_evidence:
        conf_label=f"{ev['confidence']:.0%}"
        dec_label=ev["decision"].replace("_"," ").title()
        if ev["decision"]=="auto_link":edges.append({"from":ev["record_a_id"],"to":ev["record_b_id"],"width":max(1,ev["confidence"]*4),"color":"rgba(52,211,153,0.5)","dashes":False,"label":conf_label,"title":f"{conf_label} — {dec_label}"})
        elif ev["decision"]=="review":edges.append({"from":ev["record_a_id"],"to":ev["record_b_id"],"width":1,"color":"rgba(251,191,36,0.4)","dashes":True,"label":conf_label,"title":f"{conf_label} — {dec_label}"})
    return{"nodes":nodes,"edges":edges}

@app.get("/api/calibration")
async def model_calibration():
    thresholds=[0.3,0.4,0.5,0.6,0.65,0.7,0.75,0.8,0.85,0.9,0.92,0.95,0.98]
    data=[]
    for t in thresholds:
        auto=sum(1 for e in state.link_evidence if e["confidence"]>=t and e.get("safety_ok",True))
        review=sum(1 for e in state.link_evidence if state.review_threshold<=e["confidence"]<t)
        reject=sum(1 for e in state.link_evidence if e["confidence"]<state.review_threshold)
        fp_risk=max(0,100-int(t*100))
        data.append({"threshold":t,"auto_link":auto,"review":review,"reject":reject,"total":len(state.link_evidence),"fp_risk":fp_risk})
    return{"current_auto_threshold":state.auto_threshold,"current_review_threshold":state.review_threshold,"data":data,"philosophy":"Conservative: minimize false positives at cost of more reviews","false_positive_cost":"₹2.4L","total_pairs":len(state.link_evidence)}

class ThresholdBody(BaseModel):
    auto_threshold:float;review_threshold:float=0.65

@app.post("/api/calibration/apply")
async def apply_threshold(body:ThresholdBody):
    state.auto_threshold=body.auto_threshold;state.review_threshold=body.review_threshold
    emit_event("config",f"⚙️ Thresholds updated — Auto: {body.auto_threshold:.0%}, Review: {body.review_threshold:.0%}")
    return{"ok":True,"auto_threshold":state.auto_threshold,"review_threshold":state.review_threshold}

def check_export_rate(req:Request):
    ip=req.client.host if req.client else "127.0.0.1"
    count=state.export_counts.get(ip,0)
    if count>=3:return False
    state.export_counts[ip]=count+1;return True

def add_watermark(w):
    ip=state.export_counts.get("_last_ip","127.0.0.1")
    n=sum(state.export_counts.get(k,0) for k in state.export_counts if k!="_last_ip")
    role_label = "Admin"
    w.writerow([f"CONFIDENTIAL — UdyamGraph Export — {role_label} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST — Export #{n} of 3"])
    w.writerow([])

def mask_pan_export(pan):
    if not pan or len(pan)<6:return pan or ""
    return pan[:4]+"****"+pan[-2:]

def mask_gstin_export(gstin):
    if not gstin or len(gstin)<10:return gstin or ""
    return gstin[:5]+"*********"+gstin[-2:]

@app.get("/api/export/clusters")
async def export_clusters(req:Request):
    if not check_export_rate(req):return JSONResponse(status_code=429,content={"error":"Export limit reached — max 3 per session. Contact admin."})
    state.access_logs.append({"timestamp":datetime.now().isoformat(),"actor":"Admin","action":"export_clusters","entity":"UBID Clusters CSV","ip":req.client.host if req.client else "127.0.0.1"})
    buf=io.StringIO();w=csv.writer(buf);add_watermark(w)
    w.writerow(["UBID","Udyam","Business Name","Records","Departments","Risk","ABI Status","Security Flag"])
    for c in state.ubid_clusters:
        abi=next((a for a in state.activity_records if a.ubid==c["ubid"]),None)
        udyam=next((r.get("udyam","")for r in c["department_records"]if r.get("udyam")),"-")
        w.writerow([c["ubid"],udyam,c["business_name"],c["total_records"],c["departments_covered"],c.get("risk_score",0),abi.status if abi else"",c.get("security_flag","")])
    buf.seek(0);return StreamingResponse(buf,media_type="text/csv",headers={"Content-Disposition":"attachment;filename=udyamgraph_clusters.csv"})

@app.get("/api/export/activity")
async def export_activity(req:Request):
    if not check_export_rate(req):return JSONResponse(status_code=429,content={"error":"Export limit reached — max 3 per session. Contact admin."})
    state.access_logs.append({"timestamp":datetime.now().isoformat(),"actor":"Admin","action":"export_activity","entity":"Activity Status CSV","ip":req.client.host if req.client else "127.0.0.1"})
    buf=io.StringIO();w=csv.writer(buf);add_watermark(w)
    w.writerow(["UBID","Business","Status","Confidence","Last Event","Date","Months Inactive"])
    for a in state.activity_records:w.writerow([a.ubid,a.business_name,a.status,a.confidence,a.last_event_type,a.last_event_date,a.months_since_activity])
    buf.seek(0);return StreamingResponse(buf,media_type="text/csv",headers={"Content-Disposition":"attachment;filename=udyamgraph_activity.csv"})

@app.get("/api/export/query-results")
async def export_query_results(req:Request,pincode:Optional[str]=None,district:Optional[str]=None,status:Optional[str]=None,department:Optional[str]=None,q:Optional[str]=None):
    if not check_export_rate(req):return JSONResponse(status_code=429,content={"error":"Export limit reached — max 3 per session. Contact admin."})
    state.access_logs.append({"timestamp":datetime.now().isoformat(),"actor":"Admin","action":"export_query_results","entity":"BI Query Results CSV","ip":req.client.host if req.client else "127.0.0.1"})
    data=await bi_query(pincode=pincode,district=district,status=status,department=department,q=q)
    buf=io.StringIO();w=csv.writer(buf);add_watermark(w)
    w.writerow(["UBID","Business Name","Records","Departments","ABI Status","Last Event","Last Event Date"])
    for r in data["results"]:w.writerow([r.get("ubid",""),r.get("business_name",""),r.get("total_records",0),r.get("departments_covered",0),r.get("abi_status",""),r.get("last_event",""),r.get("last_event_date","")])
    buf.seek(0);return StreamingResponse(buf,media_type="text/csv",headers={"Content-Disposition":"attachment;filename=udyamgraph_query_results.csv"})

@app.get("/api/export/access-logs")
async def export_access_logs(req:Request):
    if not check_export_rate(req):return JSONResponse(status_code=429,content={"error":"Export limit reached"})
    buf=io.StringIO();w=csv.writer(buf);add_watermark(w)
    w.writerow(["Timestamp","Actor","Action","Entity/Screen","IP"])
    for l in state.access_logs:w.writerow([l.get("timestamp",""),l.get("actor",""),l.get("action",""),l.get("entity",""),l.get("ip","")])
    buf.seek(0);return StreamingResponse(buf,media_type="text/csv",headers={"Content-Disposition":"attachment;filename=udyamgraph_access_log.csv"})

@app.get("/api/search")
async def global_search(q:str=""):
    if not q:return{"results":[]}
    qu=q.upper();results=[]
    for c in state.ubid_clusters:
        match=False;reason=""
        if qu in c["ubid"]:match=True;reason="UBID match"
        elif q.lower()in c["business_name"].lower():match=True;reason="Name match"
        else:
            for r in c["department_records"]:
                if qu in r.get("record_id","").upper():match=True;reason="Record ID";break
                if qu in r.get("pan","").upper():match=True;reason="PAN match";break
                if qu in r.get("gstin","").upper():match=True;reason="GSTIN match";break
                if qu in r.get("udyam","").upper():match=True;reason="Udyam match";break
                if q in r.get("pincode",""):match=True;reason="Pincode";break
        if match:
            abi=next((a for a in state.activity_records if a.ubid==c["ubid"]),None)
            results.append({"ubid":c["ubid"],"name":c["business_name"],"reason":reason,"records":c["total_records"],"depts":c["departments_covered"],"abi_status":abi.status if abi else"UNKNOWN","risk":c.get("risk_score",0)})
    return{"results":results}

@app.get("/api/tour")
async def get_tour():
    return[
        {"step":1,"title":"Welcome to UdyamGraph","desc":"A zero-intrusion unified business registry for Karnataka MSMEs. This demo uses synthetic data from 5 departments — no real PII. Watch the live event ticker and the Karnataka district heatmap.","action":"observe","target":"dashboard"},
        {"step":2,"title":"Run the Pipeline","desc":"Click ⚡ Run Pipeline. Watch the live ticker show events: records ingested, PII tokenized, entities matched, safety blocks triggered. 11 records from 5 departments get resolved into UBIDs.","action":"click","target":"#btn-run"},
        {"step":3,"title":"UBID Clusters — 4 Depts, 1 Identity","desc":"UBIQUITY INNOVATIONS has records in Commercial Taxes, Factories Board, Shops & Est., and Labour. All 4 auto-linked via PAN match at 100% confidence. Click Explore to see the full audit timeline.","action":"navigate","target":"clusters"},
        {"step":4,"title":"Knowledge Graph — Graph as Truth","desc":"See the interactive knowledge graph. UBID clusters are colored diamonds (green=Active, yellow=Dormant, red=Closed, purple=Uncertain). Click any node to explore, use filters to isolate status types.","action":"navigate","target":"graph"},
        {"step":5,"title":"Review Queue — You're the Reviewer","desc":"2 pairs need human judgment. See side-by-side field comparison with green/red highlighting. Press M to merge, S to separate, D to defer. Your decision gets recorded in the entity's audit timeline.","action":"navigate","target":"reviews"},
        {"step":6,"title":"Activity Status — UNCERTAIN Case","desc":"KRISHNA TRADERS AND SONS shows conflicting signals: active shop licence but no GST filing for 8 months. The ABI engine flags this as UNCERTAIN at 52% confidence.","action":"navigate","target":"abi"},
        {"step":7,"title":"BI Query — Ask Policy Questions","desc":"Try 'Active factories in Tumakuru' or use the guided chips. Each query shows the equivalent Cypher graph query. Export results as CSV. Click View on any result to deep-dive into that entity.","action":"navigate","target":"query"},
    ]

# ═══════════════════════════════════════════════════════════════════════
#  RBAC + Security Endpoints
# ═══════════════════════════════════════════════════════════════════════

@app.post("/api/pii/reveal")
async def reveal_pii():
    state.pii_revealed=True;state.pii_reveal_time=time.time()
    state.access_logs.append({"timestamp":datetime.now().isoformat(),"actor":"Admin","action":"pii_revealed","entity":"PII Tokenization Screen","ip":"127.0.0.1"})
    return{"ok":True,"revealed":True,"expires_in":30}

@app.post("/api/pii/hide")
async def hide_pii():
    state.pii_revealed=False;state.pii_reveal_time=None
    return{"ok":True,"revealed":False}

@app.get("/api/pii/status")
async def pii_status():
    if state.pii_reveal_time and time.time()-state.pii_reveal_time>30:
        state.pii_revealed=False;state.pii_reveal_time=None
    remaining=max(0,30-(time.time()-state.pii_reveal_time)) if state.pii_reveal_time else 0
    return{"revealed":state.pii_revealed,"remaining":round(remaining,1),"role":"Admin"}

@app.get("/api/session")
async def session_info():
    return{"timeout_sec":state.session_timeout_sec,"role":"Admin","rollback_count":state.rollback_count,"exports_remaining":max(0,3-sum(v for k,v in state.export_counts.items() if k!="_last_ip"))}

os.makedirs(os.path.join(os.path.dirname(__file__),"public"),exist_ok=True)
app.mount("/",StaticFiles(directory=os.path.join(os.path.dirname(__file__),"public"),html=True),name="static")

if __name__=="__main__":
    import uvicorn;uvicorn.run(app,host="0.0.0.0",port=8000)
