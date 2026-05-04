# UdyamGraph Pipeline Guide

## 🚀 Pipeline Usage

The pipeline supports three modes:
1. **Mock Data Mode** - Generate and process synthetic data for testing
2. **JSON File Mode** - Process real data from JSON files
3. **CSV File Mode** - Process real data from CSV files

---

## 📊 Quick Start

### 1. Run with Mock Data (No Input File Needed)

```bash
# Generate and process 150 synthetic records
python pipeline.py --mock

# Generate custom number of records
python pipeline.py --mock --mock-count 50

# Save mock results to file
python pipeline.py --mock --output data/results.json
```

This is perfect for:
- Testing the pipeline
- Demonstrations
- Development
- Understanding the system

### 2. Process Real Data from Files

### 2. Process Real Data from Files

Prepare a JSON or CSV file with your business records.

**JSON Format** (`data/records.json`):
```json
{
  "records": [
    {
      "record_id": "CT-BLR-2024-001",
      "business_name": "TECH INNOVATIONS PVT LTD",
      "pan": "AAAAA1000A",
      "gstin": "29AAAAA1000A1Z5",
      "department": "commercial_taxes",
      "district": "Bangalore Urban",
      "city": "Bangalore",
      "pincode": "560001",
      "legal_status": "Private Limited Company",
      "business_type": "IT Services",
      "status": "Active",
      "reg_date": "2024-01-15"
    }
  ]
}
```

**CSV Format** (`data/records.csv`):
```csv
record_id,business_name,pan,gstin,department,district,city,pincode,legal_status,business_type,status,reg_date
CT-BLR-2024-001,TECH INNOVATIONS PVT LTD,AAAAA1000A,29AAAAA1000A1Z5,commercial_taxes,Bangalore Urban,Bangalore,560001,Private Limited Company,IT Services,Active,2024-01-15
```

### 3. Run the Pipeline

```bash
# Process JSON file
python pipeline.py --input data/records.json --output data/results.json

# Process CSV file
python pipeline.py --input data/records.csv --output data/results.json

# Run with mock data (no input file needed)
python pipeline.py --mock

# View usage help
python pipeline.py
```

### 4. View Results

Results are saved to the output file in JSON format:

```json
{
  "status": "success",
  "records_processed": 8,
  "candidate_pairs": 9,
  "safe_matches": [
    {
      "record_a_id": "CT-BLR-2024-001",
      "record_b_id": "SE-BLR-2024-002",
      "business_name_a": "TECH INNOVATIONS PVT LTD",
      "business_name_b": "TECH INNOVATIONS PVT LTD",
      "department_a": "commercial_taxes",
      "department_b": "shops_establishments",
      "pan_match": true,
      "is_safe": true,
      "confidence": 0.85
    }
  ],
  "blocked_matches": [...],
  "processing_time": 0.002
}
```

---

## 📋 Required Fields

### Minimum Required
- `record_id` - Unique identifier
- `business_name` - Company name
- `department` - Source department

### Recommended for Better Matching
- `pan` - PAN number (for accurate matching)
- `gstin` - GSTIN (for tax records)
- `district` - Location
- `pincode` - Postal code
- `legal_status` - Company type
- `business_type` - Industry

---

## 🎯 Sample Data

### Mock Data (Built-in)
No preparation needed! Just run:
```bash
python pipeline.py --mock
```

The pipeline generates synthetic Karnataka business records with:
- Realistic business names
- Valid PAN/GSTIN numbers
- Multiple departments per business
- Geographic distribution across Karnataka

### Real Data Sample
A sample file is included: `data/sample_records.json`

Test it:
```bash
python pipeline.py --input data/sample_records.json --output data/results.json
```

Expected output:
- 8 records processed
- 9 candidate pairs generated
- 4 safe matches found
- 5 blocked (different PANs)

---

## 🔧 Pipeline Steps

### Step 1: PII Tokenization
- Converts PAN, GSTIN to irreversible tokens
- Uses HMAC-SHA256 for security
- Removes raw PII from records

### Step 2: Candidate Pair Generation
- Uses blocking to reduce comparisons
- Strategies: PAN token, phonetic, pincode
- Typical reduction: 60-90%

### Step 3: Safety Checks
- Validates each potential match
- Checks for conflicting PII
- Blocks unsafe merges

---

## 📤 Output Format

### Safe Matches
Records that can be safely linked:
- Same PAN token
- Similar business names
- No conflicting information

### Blocked Matches
Records that should NOT be linked:
- Different PAN numbers
- Conflicting legal status
- Address mismatches

---

## 🔒 Security Features

✅ **PII Protection**
- Raw PAN/GSTIN never stored
- Irreversible tokenization
- Deterministic (same input → same token)

✅ **Safety Checks**
- Prevents false merges
- Validates PAN consistency
- Checks for conflicts

---

## 📊 Performance

| Records | Pairs | Time |
|---------|-------|------|
| 10 | ~15 | <1s |
| 100 | ~150 | ~2s |
| 1,000 | ~1,500 | ~20s |
| 10,000 | ~15,000 | ~3min |

*Times are approximate and depend on data quality*

---

## 🆘 Troubleshooting

### "Input file not found"
✅ Check file path is correct
✅ Use forward slashes: `data/records.json`

### "Unsupported file format"
✅ Use .json or .csv extension
✅ Check file format matches extension

### "No candidate pairs found"
✅ Normal if all records are distinct
✅ Check if records have matching PANs

### "Module not found"
✅ Install dependencies: `pip install -r requirements.txt`

---

## 🎓 Advanced Usage

### Custom Processing

```python
from pipeline import CleanPipeline

# Initialize
pipeline = CleanPipeline()

# Load your data
records = [
    {
        "record_id": "001",
        "business_name": "ACME CORP",
        "pan": "AAAAA1000A",
        "department": "commercial_taxes"
    }
]

# Process
results = pipeline.process(records)

# Access results
print(f"Matches: {len(results['safe_matches'])}")
```

### Integration with APIs

```python
import requests
from pipeline import CleanPipeline

# Fetch from API
response = requests.get("https://api.example.com/records")
records = response.json()

# Process
pipeline = CleanPipeline()
results = pipeline.process(records)
```

---

## 📚 Related Files

- `pipeline.py` - Main pipeline (clean, no mock data)
- `pipeline_old_with_mock.py` - Old version with synthetic data
- `data/sample_records.json` - Sample input file
- `data/results.json` - Sample output file

---

## ✅ What Changed

### Removed
- ❌ Mock data generation functions
- ❌ Synthetic Karnataka business names
- ❌ Random PAN/GSTIN generators
- ❌ Verbose demo output

### Added
- ✅ Clean pipeline with real data support
- ✅ JSON and CSV file input
- ✅ Structured output format
- ✅ Command-line interface
- ✅ Sample data files

---

**Ready to process real business records!** 🚀
