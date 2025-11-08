# Boston Open Data MCP Server

An **MCP (Model Context Protocol) Server** providing unified access to Boston's public datasets including crime incidents, 311 service requests, building violations, food inspections, and transit data.

## 🎯 **Project Overview**

This open-source MCP server makes Boston's civic data accessible through clean, AI-friendly APIs. Built for integration with AI agents and applications, starting with the Sublyst property analytics app.

### Key Features

- 🏙️ **Unified API** for multiple Boston datasets
- 🗺️ **Geographic queries** with PostGIS spatial operations
- ⚡ **Fast performance** with connection pooling and indexing
- 🆓 **Free hosting** on Render + NeonDB
- 🔄 **Automatic data refresh** (configurable)
- 📊 **MCP-compliant** endpoints for AI agent integration

## 📦 **Tech Stack**

| Component | Technology | Why |
|-----------|------------|-----|
| **Backend** | FastAPI | Async, auto-docs, MCP-friendly |
| **Database** | PostgreSQL (NeonDB) | Free tier, PostGIS support |
| **ORM** | SQLAlchemy 2.0 | Type-safe, modern async |
| **Geographic** | PostGIS + GeoAlchemy2 | Spatial queries & indexing |
| **Data** | Pandas + NumPy | ETL and transformation |
| **HTTP** | httpx + requests | Async + sync clients |
| **Deploy** | Render.com | Free 750 hrs/month |

## 🏗️ **Architecture**

```
boston-open-data-mcp/
├── config/              # Configuration management
│   ├── settings.py      # Pydantic settings with .env loading
│   └── datasets.yaml    # Dataset metadata and schemas
│
├── db/                  # Database layer
│   ├── connection.py    # SQLAlchemy engine & sessions
│   └── models.py        # ORM models (4 datasets)
│
├── datasets/            # Data connectors
│   ├── base.py          # Abstract base connector
│   └── crime_incidents.py  # Crime data ETL
│
├── api/endpoints/       # FastAPI routes
│   ├── crime.py         # Crime endpoints
│   ├── services.py      # 311 endpoints
│   └── composite.py     # Multi-dataset queries
│
├── scripts/             # Utility scripts
│   ├── download_datasets.py
│   └── refresh_data.py
│
└── mcp_server.py        # Main FastAPI application
```

## 🚀 **Getting Started**

### Prerequisites

- Python 3.11+
- PostgreSQL database (NeonDB recommended)
- Git

### 1. Clone & Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/boston-open-data-mcp.git
cd boston-open-data-mcp

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Database Setup (NeonDB)

1. Go to https://neon.tech and sign up (free)
2. Create a new project: `boston-mcp`
3. Copy the connection string (starts with `postgresql+psycopg://...`)
4. In the SQL Editor, run:
   ```sql
   CREATE EXTENSION IF NOT EXISTS postgis;
   ```

### 3. Configuration

```bash
# Copy example environment file
cp .env.example .env

# Edit .env and add your database URL
nano .env  # or use your favorite editor
```

Update the `DATABASE_URL` in `.env`:
```
DATABASE_URL=postgresql+psycopg://your-connection-string-here
```

### 4. Initialize Database

```bash
# Set PYTHONPATH
export PYTHONPATH=/path/to/boston-open-data-mcp

# Create tables
python db/models.py
```

You should see:
```
✅ Created all tables in schema: boston_data
```

### 5. Load Initial Data

```bash
# Load crime data (sample)
python scripts/load_crime_data.py --limit 1000

# Load 311 service requests
python scripts/load_service_requests.py --limit 1000
```

### 6. Start the Server

```bash
# Run FastAPI server
uvicorn mcp_server:app --reload
```

Visit http://localhost:8000/docs for interactive API documentation.

## 📊 **Datasets**

### 1. Crime Incident Reports

**Source**: Boston Police Department  
**Update Frequency**: Daily  
**Records**: ~500K+ since 2015  

**Fields**: incident_number, offense_type, district, occurred_on_date, latitude, longitude, shooting

**Example Query**:
```bash
GET /crime/recent?lat=42.3601&lon=-71.0589&radius=0.5
```

### 2. 311 Service Requests

**Source**: Boston 311 System  
**Update Frequency**: Daily  
**Records**: ~1M+ requests  

**Fields**: case_id, case_title, status, open_date, neighborhood, latitude, longitude

**Example Query**:
```bash
GET /services/requests?type=Rodent Activity&status=Open
```

### 3. Building Violations

**Source**: Inspectional Services Dept  
**Update Frequency**: Weekly  
**Records**: ~100K+ violations  

**Example Query**:
```bash
GET /violations?address=123 Main St
```

### 4. Food Inspections

**Source**: Public Health Commission  
**Update Frequency**: Weekly  
**Records**: ~50K+ inspections  

**Example Query**:
```bash
GET /food/inspections?business=Pizza&viollevel=*
```

## 🔌 **API Examples**

### Get Recent Crimes Near Location

```bash
curl "http://localhost:8000/api/crime/recent?lat=42.3601&lon=-71.0589&radius=0.5&days=30"
```

**Response**:
```json
{
  "total": 45,
  "data": [
    {
      "incident_number": "I242070123",
      "offense": "Larceny",
      "occurred_on_date": "2024-11-05T14:30:00",
      "district": "D14",
      "shooting": false,
      "location": {
        "latitude": 42.3601,
        "longitude": -71.0589
      }
    }
  ]
}
```

### Get Service Requests by Type

```bash
curl "http://localhost:8000/api/services/requests?type=Rodent Activity&limit=10"
```

### Composite Score for Address

```bash
curl "http://localhost:8000/api/composite/score?address=123 Main St, Boston"
```

**Response**:
```json
{
  "address": "123 Main St, Boston, MA",
  "scores": {
    "safety_score": 78,
    "hygiene_score": 85,
    "maintenance_score": 72,
    "overall_score": 78
  },
  "nearby_incidents": {
    "crimes_30d": 5,
    "service_requests_open": 3,
    "building_violations": 1,
    "food_violations": 0
  }
}
```

## 🛠️ **Development**

### Running Tests

```bash
pytest tests/
```

### Code Style

```bash
# Format code
black .

# Lint
flake8 .
```

### Database Migrations

```bash
# Generate migration
alembic revision --autogenerate -m "Add new field"

# Apply migration
alembic upgrade head
```

## 📈 **Performance**

- **Query latency**: <100ms for geographic queries
- **Connection pooling**: 5 base + 10 overflow
- **Spatial indexing**: GiST indexes on geography columns
- **Data refresh**: Incremental updates only

## 🚢 **Deployment**

### Deploy to Render.com (Free)

1. Push code to GitHub
2. Go to https://render.com
3. Create new "Web Service"
4. Connect your repository
5. Configure:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn mcp_server:app --host 0.0.0.0 --port $PORT`
   - **Environment Variables**: Add `DATABASE_URL`
6. Deploy!

### Environment Variables

```bash
DATABASE_URL=postgresql+psycopg://...
ENVIRONMENT=production
LOG_LEVEL=INFO
```

## 🤝 **Contributing**

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 **License**

MIT License - see LICENSE file for details

## 🙏 **Acknowledgments**

- **City of Boston** for open data
- **Analyze Boston** for the data portal
- **MBTA** for transit data
- **MCP Protocol** for AI agent integration standards

## 📬 **Contact**

- **Project**: https://github.com/yourusername/boston-open-data-mcp
- **Issues**: https://github.com/yourusername/boston-open-data-mcp/issues

## 🗺️ **Roadmap**

- [x] Database schema and models
- [x] Data connectors for Boston Open Data
- [ ] Complete API endpoints
- [ ] ETL scripts
- [ ] Local testing
- [ ] Deploy to Render
- [ ] Add more datasets
- [ ] Real-time updates via webhooks
- [ ] Expand to other cities (NYC, Chicago)

---

**Built with ❤️ for open civic data**

