# UdyamGraph - Zero-Intrusion Entity Resolution Engine

A production-grade entity resolution system for Karnataka's business registrations across multiple government departments.

---

## ⚡ Quick Review (Standalone Demo)

The fastest way for reviewers to test the platform without setting up Kafka or Neo4j:

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Start the Web Console**:
   ```bash
   python server.py
   ```

3. **Access the UI**:
   Open **[http://localhost:8000](http://localhost:8000)** in your browser.

4. **Test the Pipeline**:
   In the dashboard, click **⚡ Run Pipeline**. Watch the live event ticker process 11 records from 5 departments and resolve them into unified identities (UBIDs).

---

## 🚀 Full Installation (Production Stack)

### Prerequisites

1. **Docker Desktop** - Must be running
   - Download: https://www.docker.com/products/docker-desktop
   - Start Docker Desktop before proceeding

2. **Python 3.9+**
   ```bash
   python --version
   ```

3. **Git** (if cloning from repository)

### Step 1: Start Infrastructure Services

**On Windows:**
```bash
START.bat
```

**On Linux/Mac:**
```bash
chmod +x START.sh
./START.sh
```

This will start:
- ✅ Kafka (message streaming)
- ✅ Neo4j (graph database)
- ✅ Redis (caching)
- ✅ Zookeeper (Kafka coordination)
- ✅ Schema Registry (Kafka schemas)

### Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Set Environment Variables (Production Only)

For development, the startup script uses safe defaults. For production:

```bash
# Windows (PowerShell)
$env:PII_HMAC_SECRET_KEY="your-secure-random-key-here"
$env:NEO4J_PASSWORD="your-secure-password"

# Linux/Mac
export PII_HMAC_SECRET_KEY="your-secure-random-key-here"
export NEO4J_PASSWORD="your-secure-password"
```

### Step 4: Run the Application

**Option A: Demo Pipeline (Test with synthetic data)**
```bash
python pipeline.py
```

**Option B: Web Server (Interactive UI)**
```bash
python server.py
```
Then open: http://localhost:8000

## 📊 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Neo4j Browser | http://localhost:7474 | neo4j / udyamgraph_dev |
| Neo4j Bolt | bolt://localhost:7687 | neo4j / udyamgraph_dev |
| Kafka | localhost:9092 | - |
| Redis | localhost:6379 | - |
| Schema Registry | http://localhost:8081 | - |

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    UdyamGraph Pipeline                        │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  Ingest → Tokenize → Block → Features → Classify → Store    │
│  (Kafka)  (HMAC)     (LSH)   (SBERT)    (XGBoost)  (Neo4j)  │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

### Key Components

1. **PII Tokenization** (`pii/tokenizer.py`)
   - HMAC-SHA256 deterministic tokenization
   - Irreversible, joinable across departments
   - Zero raw PII storage

2. **Entity Resolution** (`resolution/`)
   - Blocking engine (LSH + phonetic)
   - Feature engineering (16 features + SBERT embeddings)
   - XGBoost classifier with confidence calibration
   - SHAP explainability

3. **Safety Safeguards** (`safeguards/`)
   - Anti-false-merge engine
   - Human review queue
   - Negative evidence detection

4. **Activity Classification** (`activity/`)
   - Business status: Active, Dormant, Closed, Uncertain
   - Multi-signal analysis across departments

## 🔒 Security Features

- ✅ Deterministic PII tokenization (HMAC-SHA256)
- ✅ No raw PII storage
- ✅ Environment-based secrets management
- ✅ RBAC support (configured, needs enforcement)
- ⚠️ CORS restricted (configure for production)
- ⚠️ Authentication required (implement before production)

## 📁 Project Structure

```
udyamgraph/
├── pii/                    # PII tokenization
│   └── tokenizer.py
├── resolution/             # Entity resolution
│   ├── blocking.py
│   ├── classifier.py
│   ├── feature_engineering.py
│   └── explainer.py
├── safeguards/             # Safety checks
│   ├── anti_merge.py
│   └── reviewer.py
├── activity/               # Business activity classification
│   └── classifier.py
├── graph/                  # Neo4j schema
│   └── schema.py
├── ingestion/              # Kafka consumers
│   └── kafka_consumers.py
├── config.py               # Configuration
├── pipeline.py             # Main pipeline orchestrator
├── server.py               # FastAPI web server
└── docker-compose.yml      # Infrastructure services
```

## 🧪 Testing

### Run Demo Pipeline
```bash
python pipeline.py
```

This will:
1. Generate synthetic Karnataka business records
2. Tokenize PII (PAN, GSTIN)
3. Generate candidate pairs via blocking
4. Extract features
5. Classify matches
6. Run safety checks
7. Display results

### Test PII Tokenization
```bash
python pii/tokenizer.py
```

Runs proofs for:
- Determinism (same input → same token)
- Irreversibility (cannot recover PII from token)
- Joinability (same PAN across departments → same token)

## 🐛 Troubleshooting

### Docker not running
```
Error: Docker is not running
```
**Solution**: Start Docker Desktop

### Port already in use
```
Error: Port 7474 is already allocated
```
**Solution**: Stop conflicting services or change ports in docker-compose.yml

### Module not found
```
ModuleNotFoundError: No module named 'xgboost'
```
**Solution**: Install dependencies
```bash
pip install -r requirements.txt
```

### Neo4j connection refused
```
ServiceUnavailable: Unable to connect to bolt://localhost:7687
```
**Solution**: Wait 30 seconds for Neo4j to fully start, then retry

## 📝 Recent Fixes (See FIXES_APPLIED.md)

- ✅ Fixed syntax errors in pipeline.py
- ✅ Fixed malformed regex patterns in tokenizer
- ✅ Secured HMAC secret key configuration
- ✅ Added missing sse-starlette dependency
- ✅ Fixed Docker service hostnames
- ✅ Reduced PII token logging

## 🚦 Production Checklist

Before deploying to production:

- [ ] Set PII_HMAC_SECRET_KEY to cryptographically random value
- [ ] Set NEO4J_PASSWORD to strong password
- [ ] Configure CORS to specific trusted domains
- [ ] Implement authentication middleware (JWT/OAuth2)
- [ ] Implement RBAC enforcement on endpoints
- [ ] Enable HTTPS/TLS for all connections
- [ ] Set up secrets management (AWS Secrets Manager, etc.)
- [ ] Implement rate limiting
- [ ] Add comprehensive input validation
- [ ] Set up monitoring and alerting
- [ ] Configure log aggregation
- [ ] Perform security audit
- [ ] Load test the system

## 📚 Documentation

- **Architecture**: See docstrings in each module
- **API**: Run server and visit http://localhost:8000/docs
- **Configuration**: See config.py for all settings
- **Security**: See FIXES_APPLIED.md for security improvements

## 🤝 Contributing

1. Follow PEP 8 style guide
2. Add type hints to all functions
3. Write docstrings for public APIs
4. Test with synthetic data before real data
5. Never commit secrets or credentials

## 📄 License

[Add your license here]

## 🆘 Support

For issues or questions:
1. Check FIXES_APPLIED.md for known issues
2. Review logs: `docker-compose logs -f`
3. Check service health: `docker-compose ps`

---

**Built for Karnataka Government** - Zero-intrusion entity resolution across department systems.
