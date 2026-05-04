#!/bin/bash
# UdyamGraph Startup Script
# This script starts all required services and the application

set -e

echo "========================================"
echo "  UdyamGraph - Starting Services"
echo "========================================"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Set environment variables
echo "📝 Setting environment variables..."
export PII_HMAC_SECRET_KEY="${PII_HMAC_SECRET_KEY:-INSECURE_DEV_KEY_udyamgraph_2024}"
export NEO4J_PASSWORD="${NEO4J_PASSWORD:-udyamgraph_dev}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
export NEO4J_URI="${NEO4J_URI:-bolt://neo4j:7687}"

echo "⚠️  WARNING: Using development credentials. DO NOT use in production!"
echo ""

# Start Docker services
echo "🐳 Starting Docker services (Kafka, Neo4j, Redis)..."
docker-compose up -d

echo ""
echo "⏳ Waiting for services to be ready..."
sleep 10

# Check service health
echo ""
echo "🔍 Checking service health..."

# Check Kafka
if docker-compose ps kafka | grep -q "Up"; then
    echo "✅ Kafka is running"
else
    echo "❌ Kafka failed to start"
fi

# Check Neo4j
if docker-compose ps neo4j | grep -q "Up"; then
    echo "✅ Neo4j is running"
else
    echo "❌ Neo4j failed to start"
fi

# Check Redis
if docker-compose ps redis | grep -q "Up"; then
    echo "✅ Redis is running"
else
    echo "❌ Redis failed to start"
fi

echo ""
echo "========================================"
echo "  Services Started Successfully!"
echo "========================================"
echo ""
echo "📊 Service URLs:"
echo "  • Neo4j Browser:  http://localhost:7474"
echo "  • Neo4j Bolt:     bolt://localhost:7687"
echo "  • Kafka:          localhost:9092"
echo "  • Redis:          localhost:6379"
echo ""
echo "🔐 Credentials:"
echo "  • Neo4j: neo4j / udyamgraph_dev"
echo ""
echo "🚀 Next steps:"
echo "  1. Install Python dependencies: pip install -r requirements.txt"
echo "  2. Run the demo pipeline:       python pipeline.py"
echo "  3. Start the web server:        python server.py"
echo ""
echo "📝 View logs: docker-compose logs -f"
echo "🛑 Stop services: docker-compose down"
echo ""
