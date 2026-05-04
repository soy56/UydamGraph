"""
UdyamGraph — Pipeline
=====================

Production-ready pipeline for processing business records.
Supports JSON/CSV file input or mock data generation.

Usage:
    # Process real data
    python pipeline.py --input data/records.json
    python pipeline.py --input data/records.csv
    
    # Generate and process mock data
    python pipeline.py --mock
    python pipeline.py --mock --mock-count 50
"""

import logging
import json
import csv
import argparse
import time
from typing import Dict, Any, List
from pathlib import Path

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import config
from pii.tokenizer import PIITokenizer
from resolution.blocking import BlockingEngine
from safeguards.anti_merge import AntiFalseMergeEngine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("udyamgraph.pipeline")


class CleanPipeline:
    """
    Production pipeline without mock data.
    Processes real business records from files or APIs.
    """
    
    def __init__(self):
        self.tokenizer = PIITokenizer()
        self.blocker = BlockingEngine()
        self.safety_engine = AntiFalseMergeEngine()
        
    def load_records_from_json(self, filepath: str) -> List[Dict[str, Any]]:
        """Load business records from JSON file."""
        logger.info(f"Loading records from {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Handle both array and object with 'records' key
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict) and 'records' in data:
            records = data['records']
        else:
            raise ValueError("JSON must be an array or object with 'records' key")
            
        logger.info(f"Loaded {len(records)} records")
        return records
    
    def load_records_from_csv(self, filepath: str) -> List[Dict[str, Any]]:
        """Load business records from CSV file."""
        logger.info(f"Loading records from {filepath}")
        
        records = []
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(dict(row))
                
        logger.info(f"Loaded {len(records)} records")
        return records
    
    def process(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process business records through the pipeline.
        
        Args:
            records: List of business records with fields:
                - record_id: Unique identifier
                - business_name: Company name
                - pan: PAN number (optional)
                - gstin: GSTIN (optional)
                - department: Source department
                - district: Location
                - pincode: Postal code
                
        Returns:
            Dictionary with processing results
        """
        start_time = time.time()
        
        logger.info("=" * 70)
        logger.info("  UdyamGraph Pipeline - Processing Started")
        logger.info("=" * 70)
        
        # Step 1: Tokenize PII
        logger.info(f"\n[1/3] Tokenizing PII for {len(records)} records...")
        tokenized_records = []
        
        for record in records:
            tokenized = self.tokenizer.tokenize_record(record)
            tokenized_records.append(tokenized)
            
        logger.info(f"✓ Tokenized {len(tokenized_records)} records")
        
        # Step 2: Generate candidate pairs
        logger.info(f"\n[2/3] Generating candidate pairs via blocking...")
        pairs = self.blocker.generate_candidate_pairs(tokenized_records)
        
        possible_pairs = len(records) * (len(records) - 1) // 2
        reduction = 100 * (1 - len(pairs) / max(possible_pairs, 1))
        
        logger.info(f"✓ Generated {len(pairs)} candidate pairs")
        logger.info(f"  Reduction: {possible_pairs} possible → {len(pairs)} actual ({reduction:.1f}% reduction)")
        
        if not pairs:
            logger.info("\n✓ No candidate pairs found. All records are distinct.")
            return {
                "status": "success",
                "records_processed": len(records),
                "candidate_pairs": 0,
                "matches": [],
                "processing_time": time.time() - start_time
            }
        
        # Step 3: Safety checks
        logger.info(f"\n[3/3] Running safety checks on {len(pairs)} pairs...")
        
        matches = []
        blocked = []
        
        for idx_a, idx_b in pairs:
            rec_a = tokenized_records[idx_a]
            rec_b = tokenized_records[idx_b]
            
            # Simple feature vector for safety check
            import numpy as np
            features = np.zeros(16, dtype=np.float32)
            
            # Check PAN token match
            pan_a = rec_a.get("pan_token")
            pan_b = rec_b.get("pan_token")
            features[0] = 1.0 if (pan_a and pan_b and pan_a == pan_b) else 0.0
            
            # Run safety check
            safety = self.safety_engine.analyze_merge_safety(
                rec_a, rec_b, features, 0.85
            )
            
            match_info = {
                "record_a_id": rec_a.get("record_id", "unknown"),
                "record_b_id": rec_b.get("record_id", "unknown"),
                "business_name_a": rec_a.get("business_name", ""),
                "business_name_b": rec_b.get("business_name", ""),
                "department_a": rec_a.get("department", ""),
                "department_b": rec_b.get("department", ""),
                "pan_match": bool(features[0]),
                "is_safe": safety.is_safe,
                "confidence": safety.confidence
            }
            
            if safety.is_safe:
                matches.append(match_info)
            else:
                match_info["blocked_reason"] = safety.negative_evidence[0].description if safety.negative_evidence else "Safety check failed"
                blocked.append(match_info)
        
        logger.info(f"✓ Safety checks complete")
        logger.info(f"  Safe matches: {len(matches)}")
        logger.info(f"  Blocked: {len(blocked)}")
        
        # Summary
        processing_time = time.time() - start_time
        
        logger.info("\n" + "=" * 70)
        logger.info("  Pipeline Complete")
        logger.info("=" * 70)
        logger.info(f"  Records processed: {len(records)}")
        logger.info(f"  Candidate pairs: {len(pairs)}")
        logger.info(f"  Safe matches: {len(matches)}")
        logger.info(f"  Blocked: {len(blocked)}")
        logger.info(f"  Processing time: {processing_time:.2f}s")
        logger.info("=" * 70)
        
        return {
            "status": "success",
            "records_processed": len(records),
            "candidate_pairs": len(pairs),
            "safe_matches": matches,
            "blocked_matches": blocked,
            "processing_time": processing_time
        }


# ─── Mock Data Generation Functions ──────────────────────────────────────────

KARNATAKA_DISTRICTS = [
    {"name":"Bangalore Urban","code":"BLR"},{"name":"Bangalore Rural","code":"BRU"},
    {"name":"Mysuru","code":"MYS"},{"name":"Tumakuru","code":"TMK"},
    {"name":"Dharwad","code":"DWD"},{"name":"Belagavi","code":"BGM"},
    {"name":"Kalaburagi","code":"KLB"},{"name":"Mangaluru (DK)","code":"DKN"},
    {"name":"Raichur","code":"RCR"},{"name":"Ballari","code":"BLR2"},
    {"name":"Shivamogga","code":"SMG"},{"name":"Davanagere","code":"DVG"},
    {"name":"Hassan","code":"HSN"},{"name":"Mandya","code":"MND"},
    {"name":"Kodagu","code":"KDG"},{"name":"Udupi","code":"UDP"},
    {"name":"Haveri","code":"HVR"},{"name":"Chitradurga","code":"CTA"},
    {"name":"Bidar","code":"BID"},{"name":"Vijayapura","code":"BJP"},
    {"name":"Bagalkot","code":"BGK"},{"name":"Koppal","code":"KPL"},
    {"name":"Gadag","code":"GDG"},{"name":"Ramanagara","code":"RMN"},
    {"name":"Yadgir","code":"YDR"},{"name":"Chikkamagaluru","code":"CKM"},
    {"name":"Chamarajanagar","code":"CMR"},{"name":"Chikkaballapura","code":"CKB"},
    {"name":"Kolar","code":"KLR"},{"name":"Uttara Kannada","code":"UKN"},
    {"name":"Vijayanagara","code":"VNG"},
]

BUSINESS_NAMES = [
    "UBIQUITY INNOVATIONS PVT LTD","KRISHNA TRADING COMPANY","DECCAN PHARMACEUTICALS PVT LTD",
    "KRISHNA TRADERS AND SONS","TUMAKURU SILK MILLS","BANGALORE ENGINEERING WORKS",
    "MYSURU AGRO PRODUCTS","HUBLI STEEL FABRICATORS","MANGALORE TILE WORKS",
    "BELGAUM COTTON MILLS","RAICHUR SUGAR FACTORY","KOLAR GOLD MINING CO",
    "SHIMOGA TIMBER TRADERS","HASSAN COFFEE ESTATES","MANDYA SUGAR FACTORY",
    "DAVANGERE TEXTILES","CHITRADURGA GRANITE WORKS","GULBARGA CEMENT WORKS",
    "BIJAPUR OIL MILLS","KARWAR FISHERIES","BELLARY IRON ORES","UDUPI CASHEW EXPORTS",
    "GADAG COTTON GINNING","KOPPAL CEMENT FACTORY","BIDAR RICE MILLS",
    "CHIKMAGALUR COFFEE PVT LTD","KODAGU SPICE TRADERS","HAVERI MAIZE PRODUCTS",
    "BAGALKOT SUGAR WORKS","YADGIR AGRO FARMS","SRI LAKSHMI ENTERPRISES",
    "GANESHA INDUSTRIES","BHARATH ELECTRONICS","VIKAS POLYMERS PVT LTD",
    "NANDI PHARMA LABS","CAUVERY FOODS PVT LTD","TUNGABHADRA POWER SYSTEMS",
    "SHARAVATHI HYDRO WORKS","KAVERI SEEDS CO LTD","HAMPI STONES EXPORTS",
    "GOLDEN HARVEST AGRI","SILICON VALLEY TECH SERVICES","INDO-EURO TEXTILES",
    "SOUTHERN STAR LOGISTICS","PREMIER PACK SOLUTIONS","OCEANIC SEAFOODS",
    "HERITAGE HANDLOOMS","ROYAL ORCHID FOODS","PRECISION TOOLS MFG",
    "EVERGREEN BIOTECH","APEX STEEL INDUSTRIES","PHOENIX CERAMICS",
    "TITAN ENGINEERING","FORTUNE PLASTICS","SUMMIT CHEMICALS","GALAXY GARMENTS",
    "ORBIT ELECTRICALS","ZENITH AGRO TECH","PIONEER RUBBER WORKS",
    "CRYSTAL GLASS WORKS","SAPPHIRE TEXTILES","EMERALD PHARMA","RUBY ENGINEERING",
    "DIAMOND ABRASIVES","PEARL FISHERIES CO","MAPLE FOODS PVT LTD",
    "CEDAR TIMBER MILLS","BANYAN AUTO PARTS","TEAK FURNITURE EXPORTS",
    "SANDAL WOOD CRAFTS","JASMINE PERFUMES","LOTUS CHEMICALS","TULIP GARMENTS",
    "ORCHID BIOTECH","SUNFLOWER OIL MILLS","SAMARTH ENTERPRISES","VIBHUTI TRADERS",
    "SHANKAR INDUSTRIES","MAHALAKSHMI TEXTILES","BASAVA ENGINEERING",
    "KEMPEGOWDA CONSTRUCTIONS","TIPU SULTAN EXPORTS","CHALUKYA STONES",
    "HOYSALA SILKS PVT LTD","VIJAYANAGAR GRANITES","KADAMBA AGRO PRODUCTS",
    "GANESH STEEL WORKS","PARVATHI FOODS","SHIVA PHARMA PVT LTD",
    "VISHNU ENTERPRISES","DEVI TEXTILES","DURGA POLYMERS","LAXMI COTTON MILLS",
    "ANNAPURNA FOODS","SARASWATHI ELECTRONICS","MAHESH TRADING CO",
    "SURESH INDUSTRIES","RAJESH POLYMERS PVT LTD","DINESH RUBBER WORKS",
    "RAMESH AUTO COMPONENTS",
]

LEGAL_TYPES = [
    "Private Limited Company","Proprietorship","Partnership","LLP",
    "Public Limited Company","One Person Company","HUF"
]

BUSINESS_TYPES = [
    "Manufacturing","IT Services","Wholesale Trade","Retail Trade",
    "Textile Manufacturing","Food Processing","Pharmaceutical Manufacturing",
    "Engineering Works","Agricultural Products","Chemical Manufacturing",
    "Steel Fabrication","Software Services","Logistics","Construction Materials","Mining"
]

DEPARTMENTS = [
    "commercial_taxes","factories_board","shops_establishments",
    "labour_dept","revenue_dept"
]


def gen_pan():
    """Generate a random PAN number."""
    import random
    c1 = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=5))
    c2 = "".join(random.choices("0123456789", k=4))
    c3 = random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    return c1 + c2 + c3


def gen_gstin(pan, state_code=29):
    """Generate a GSTIN from a PAN."""
    import random
    entity = random.choice("123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    check = random.choice("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    return f"{state_code:02d}{pan}{entity}Z{check}"


def generate_synthetic_karnataka_records(count: int = 150) -> List[Dict[str, Any]]:
    """
    Generate synthetic Karnataka business records for testing.
    
    Args:
        count: Number of records to generate (default: 150)
        
    Returns:
        List of synthetic business records
    """
    import random
    
    records = []
    entities = []
    
    # Generate unique entities (about 1/3 of record count)
    num_entities = max(count // 3, 10)
    for i in range(num_entities):
        district = random.choice(KARNATAKA_DISTRICTS)
        pan = gen_pan()
        gstin = gen_gstin(pan, 29)
        
        entities.append({
            "name": random.choice(BUSINESS_NAMES),
            "pan": pan,
            "gstin": gstin,
            "legal": random.choice(LEGAL_TYPES),
            "btype": random.choice(BUSINESS_TYPES),
            "district": district,
            "pincode": f"{560000 + i:06d}"
        })
    
    # Generate records from these entities (multiple departments per entity)
    while len(records) < count:
        ent = random.choice(entities)
        dept = random.choice(DEPARTMENTS)
        dist_code = ent["district"]["code"]
        rid = f"{dept.upper()[:2]}-{dist_code}-{random.randint(2020,2025)}-{random.randint(10000,99999)}"
        
        rec = {
            "record_id": rid,
            "department": dept,
            "business_name": ent["name"],
            "pan": ent["pan"],
            "legal_status": ent["legal"],
            "business_type": ent["btype"],
            "district": ent["district"]["name"],
            "city": ent["district"]["name"],
            "pincode": ent["pincode"],
            "status": "Active",
            "reg_date": f"{random.randint(2018,2025)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        }
        
        if dept == "commercial_taxes":
            rec["gstin"] = ent["gstin"]
        
        records.append(rec)
    
    return records


def main():
    """Main entry point for the clean pipeline."""
    parser = argparse.ArgumentParser(
        description="UdyamGraph Pipeline - Process business records"
    )
    parser.add_argument(
        '--input',
        type=str,
        help='Input file path (JSON or CSV)',
        required=False
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output file path for results (JSON)',
        required=False
    )
    parser.add_argument(
        '--mock',
        action='store_true',
        help='Generate and process mock data (150 synthetic records)',
        default=False
    )
    parser.add_argument(
        '--mock-count',
        type=int,
        help='Number of mock records to generate (default: 150)',
        default=150
    )
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = CleanPipeline()
    
    # Load records
    if args.mock:
        # Generate mock data
        logger.info(f"Generating {args.mock_count} synthetic Karnataka business records...")
        records = generate_synthetic_karnataka_records(args.mock_count)
        logger.info(f"Generated {len(records)} mock records")
    elif args.input:
        input_path = Path(args.input)
        
        if not input_path.exists():
            logger.error(f"Input file not found: {args.input}")
            return
        
        # Load based on file extension
        if input_path.suffix.lower() == '.json':
            records = pipeline.load_records_from_json(args.input)
        elif input_path.suffix.lower() == '.csv':
            records = pipeline.load_records_from_csv(args.input)
        else:
            logger.error(f"Unsupported file format: {input_path.suffix}")
            logger.error("Supported formats: .json, .csv")
            return
    else:
        # No input file - show usage
        logger.info("=" * 70)
        logger.info("  UdyamGraph Clean Pipeline")
        logger.info("=" * 70)
        logger.info("\nUsage:")
        logger.info("  python pipeline.py --mock                              # Generate 150 mock records")
        logger.info("  python pipeline.py --mock --mock-count 50              # Generate 50 mock records")
        logger.info("  python pipeline.py --input data/records.json           # Process JSON file")
        logger.info("  python pipeline.py --input data/records.csv --output results.json  # Process CSV")
        logger.info("\nInput file format (JSON):")
        logger.info("""
  {
    "records": [
      {
        "record_id": "REC-001",
        "business_name": "TECH SOLUTIONS PVT LTD",
        "pan": "AAAAA1000A",
        "gstin": "29AAAAA1000A1Z5",
        "department": "commercial_taxes",
        "district": "Bangalore Urban",
        "pincode": "560001"
      }
    ]
  }
        """)
        logger.info("\nInput file format (CSV):")
        logger.info("""
  record_id,business_name,pan,gstin,department,district,pincode
  REC-001,TECH SOLUTIONS PVT LTD,AAAAA1000A,29AAAAA1000A1Z5,commercial_taxes,Bangalore Urban,560001
        """)
        logger.info("\nCreate a sample input file to get started!")
        return
    
    # Process records
    results = pipeline.process(records)
    
    # Save results if output path specified
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info(f"\n✓ Results saved to {args.output}")


if __name__ == "__main__":
    main()
