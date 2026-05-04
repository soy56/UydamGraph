# 🚀 UdyamGraph - Quick Start Guide

## ✅ Status: RUNNING!

The UdyamGraph server is now running successfully!

## 🌐 Access the Application

**Web Interface**: http://localhost:8000

**API Documentation**: http://localhost:8000/docs

## 📊 What's Running

✅ **FastAPI Server** - Port 8000  
⚠️ **Docker Services** - Not started (optional for demo)

## 🎯 Quick Actions

### 1. Quick Demo (Recommended First!)
```bash
python demo_simple.py
```
Fast demo (5 seconds) showing:
- PII tokenization
- Candidate pair generation
- Safety checks

### 2. Full Pipeline Demo
```bash
python pipeline.py
```
Complete demo with 150 synthetic records (takes 30-60 seconds)

### 3. View the Web Interface
Open your browser and go to:
```
http://localhost:8000
```

### 4. Test the API
Visit the interactive API docs:
```
http://localhost:8000/docs
```

## 📝 Example API Endpoints

### Get Dashboard Stats
```bash
curl http://localhost:8000/api/dashboard
```

### Get UBID Clusters
```bash
curl http://localhost:8000/api/clusters
```

### Get Review Queue
```bash
curl http://localhost:8000/api/reviews
```

## 🛑 Stop the Server

Press `CTRL+C` in the terminal where the server is running

Or use:
```bash
# Find the process
tasklist | findstr python

# Kill it
taskkill /F /PID <process_id>
```

## 🐳 Optional: Start Docker Services

If you want to use Neo4j, Kafka, and Redis:

1. **Start Docker Desktop**
2. Run:
   ```bash
   docker-compose up -d
   ```
3. Wait 30 seconds for services to start
4. Restart the server

## 🔧 Configuration

The application is running with development defaults:

- **PII Secret**: INSECURE_DEV_KEY (⚠️ Development only!)
- **Neo4j**: bolt://neo4j:7687 (not connected - Docker not running)
- **Kafka**: kafka:9092 (not connected - Docker not running)

## 📚 Next Steps

1. **Explore the UI** - http://localhost:8000
2. **Read the docs** - See README.md
3. **Run tests** - `python pii/tokenizer.py`
4. **Check fixes** - See FIXES_APPLIED.md

## ⚠️ Important Notes

- This is running in **development mode**
- Docker services are **optional** for basic demo
- The server works **without Docker** for testing
- For production, follow the security checklist in README.md

## 🆘 Troubleshooting

### Port 8000 already in use
```bash
# Find what's using port 8000
netstat -ano | findstr :8000

# Kill the process
taskkill /F /PID <process_id>
```

### Module not found errors
```bash
pip install -r requirements.txt
```

### Server not responding
Check the terminal output for errors, or restart:
```bash
# Stop (CTRL+C)
# Start again
python server.py
```

---

**🎉 You're all set! The application is running successfully.**
