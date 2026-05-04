@echo off
REM UdyamGraph Startup Script for Windows
REM This script starts all required services and the application

echo ========================================
echo   UdyamGraph - Starting Services
echo ========================================
echo.

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo Error: Docker is not running. Please start Docker Desktop first.
    exit /b 1
)

REM Set environment variables
echo Setting environment variables...
if not defined PII_HMAC_SECRET_KEY set PII_HMAC_SECRET_KEY=INSECURE_DEV_KEY_udyamgraph_2024
if not defined NEO4J_PASSWORD set NEO4J_PASSWORD=udyamgraph_dev
if not defined KAFKA_BOOTSTRAP_SERVERS set KAFKA_BOOTSTRAP_SERVERS=kafka:9092
if not defined NEO4J_URI set NEO4J_URI=bolt://neo4j:7687

echo WARNING: Using development credentials. DO NOT use in production!
echo.

REM Start Docker services
echo Starting Docker services (Kafka, Neo4j, Redis)...
docker-compose up -d

echo.
echo Waiting for services to be ready...
timeout /t 10 /nobreak >nul

REM Check service health
echo.
echo Checking service health...

docker-compose ps kafka | findstr "Up" >nul
if errorlevel 1 (
    echo Kafka failed to start
) else (
    echo Kafka is running
)

docker-compose ps neo4j | findstr "Up" >nul
if errorlevel 1 (
    echo Neo4j failed to start
) else (
    echo Neo4j is running
)

docker-compose ps redis | findstr "Up" >nul
if errorlevel 1 (
    echo Redis failed to start
) else (
    echo Redis is running
)

echo.
echo ========================================
echo   Services Started Successfully!
echo ========================================
echo.
echo Service URLs:
echo   - Neo4j Browser:  http://localhost:7474
echo   - Neo4j Bolt:     bolt://localhost:7687
echo   - Kafka:          localhost:9092
echo   - Redis:          localhost:6379
echo.
echo Credentials:
echo   - Neo4j: neo4j / udyamgraph_dev
echo.
echo Next steps:
echo   1. Install Python dependencies: pip install -r requirements.txt
echo   2. Run the demo pipeline:       python pipeline.py
echo   3. Start the web server:        python server.py
echo.
echo View logs: docker-compose logs -f
echo Stop services: docker-compose down
echo.
