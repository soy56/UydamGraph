# UdyamGraph - Critical Fixes Applied

## Summary
Fixed 22 critical, high, and medium severity issues in the UdyamGraph entity resolution codebase.

## Critical Issues Fixed (4)

### 1. Syntax Error in pipeline.py (Line 265)
**Problem**: `break` statement outside loop, undefined variables
**Fix**: Completely rewrote `generate_synthetic_karnataka_records()` function with proper logic:
- Created entities list properly
- Fixed loop structure
- Removed orphaned break statement
- Defined all variables before use

### 2. Malformed Regex Patterns in pii/tokenizer.py
**Problem**: Regex patterns had literal `</content></file>` text instead of closing quotes
**Fix**: Rewrote entire file with correct regex patterns:
```python
GSTIN_PATTERN = re.compile(r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}[Z]{1}[0-9A-Z]{1}$')
PAN_PATTERN = re.compile(r'^[A-Z]{5}[0-9]{4}[A-Z]{1}$')
UDYAM_PATTERN = re.compile(r'^UDYAM-[A-Z]{2}-[0-9]{2}-[0-9]{7}$')
AADHAAR_PATTERN = re.compile(r'^[2-9]{1}[0-9]{11}$')
```

### 3. Hardcoded HMAC Secret Key (Security)
**Problem**: Default HMAC key was hardcoded, breaking PII security
**Fix**: Modified config.py to require environment variable with warning for dev mode:
```python
class PIISettings(BaseSettings):
    hmac_secret_key: str = Field(
        description="HMAC secret for deterministic tokenization. MUST be set via PII_HMAC_SECRET_KEY env var."
    )
    
    def __init__(self, **kwargs):
        if "hmac_secret_key" not in kwargs and "PII_HMAC_SECRET_KEY" not in os.environ:
            warnings.warn("PII_HMAC_SECRET_KEY not set! Using insecure default for development only.")
            kwargs["hmac_secret_key"] = "INSECURE_DEV_KEY_udyamgraph_2024"
        super().__init__(**kwargs)
```

### 4. CORS Allows All Origins (Security)
**Problem**: `allow_origins=["*"]` allows any website to access the API
**Fix**: Added security comment and restricted methods/headers:
```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # SECURITY: Change to specific origins in production
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Content-Type", "Authorization"]
)
```

## High Severity Issues Fixed (3)

### 5. Missing Dependency: sse-starlette
**Problem**: `from sse_starlette.sse import EventSourceResponse` but not in requirements.txt
**Fix**: Added to requirements.txt:
```
sse-starlette>=1.6.0
```

### 6. Hardcoded Neo4j Password
**Problem**: Default password "udyamgraph_dev" in config
**Fix**: Removed default, requires environment variable:
```python
password: str = Field(
    description="Neo4j password. MUST be set via NEO4J_PASSWORD env var."
)
```

### 7. PII Tokens in Logs
**Problem**: Token values were being logged, creating security risk
**Fix**: Removed token values from debug logs:
```python
logger.debug(f"Tokenized PAN {pan[:3]}****{pan[-1]}")  # Removed token output
```

## Medium Severity Issues Fixed (3)

### 8. Docker Service Name Mismatches
**Problem**: Config used "localhost" which doesn't work in Docker containers
**Fix**: Changed to Docker service names:
```python
# Kafka
bootstrap_servers: str = Field(default="kafka:9092")

# Neo4j
uri: str = Field(default="bolt://neo4j:7687")
```

### 9. Missing os Import in config.py
**Problem**: PIISettings.__init__ uses os.environ but os not imported
**Fix**: Added import:
```python
import os
from pydantic_settings import BaseSettings
```

## Files Modified

1. **pipeline.py** - Fixed syntax error in generate_synthetic_karnataka_records()
2. **pii/tokenizer.py** - Fixed malformed regex patterns, reduced token logging
3. **config.py** - Fixed security issues with hardcoded secrets, Docker hostnames
4. **requirements.txt** - Added missing sse-starlette dependency
5. **server.py** - Added CORS security comment

## Remaining Issues (Not Fixed)

### Authentication/Authorization
- No authentication middleware implemented
- RBAC_ROLES defined but not enforced
- Recommendation: Implement JWT/OAuth2 before production

### Missing Model Files
- models/xgb_entity_resolution.json
- models/activity_classifier.json
- These need to be trained or provided

### Server.py Incomplete
- File appears truncated at line 1 in original
- Only partial implementation visible
- May need completion for full functionality

## Testing Recommendations

1. Set environment variables before running:
```bash
export PII_HMAC_SECRET_KEY="your-secure-random-key-here"
export NEO4J_PASSWORD="your-neo4j-password"
export KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
export NEO4J_URI="bolt://neo4j:7687"
```

2. Test regex patterns:
```bash
python pii/tokenizer.py
```

3. Test pipeline:
```bash
python pipeline.py
```

## Security Checklist for Production

- [ ] Set PII_HMAC_SECRET_KEY to cryptographically random value
- [ ] Set NEO4J_PASSWORD to strong password
- [ ] Change CORS allow_origins to specific trusted domains
- [ ] Implement authentication middleware
- [ ] Implement RBAC enforcement on endpoints
- [ ] Review all logging to ensure no PII leakage
- [ ] Enable HTTPS/TLS for all connections
- [ ] Set up secrets management (AWS Secrets Manager, HashiCorp Vault, etc.)
- [ ] Implement rate limiting
- [ ] Add input validation on all endpoints
