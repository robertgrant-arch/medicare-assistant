# Medicare Assistant

AI-powered Medicare plan assistant with vector database, real CMS data ingestion, and live chat interface. Built for quote engine integration.

## Architecture

```
+------------------+     +------------------+     +------------------+
|   Chat UI        | --> |  Express API     | --> | PostgreSQL +     |
|   (index.html)   |     |  (server.js)     |     | pgvector         |
|   SSE Streaming  |     |  Hybrid Search   |     | Vector + Struct  |
+------------------+     +------------------+     +------------------+
                               |
                          OpenAI API
                    (Embeddings + Chat)
```

## Features

- **Vector Search**: Semantic search over plan data using pgvector + OpenAI embeddings
- **Structured Search**: SQL-based filtering by state, county, FIPS, plan type, premium, drug name
- **Hybrid RAG**: Combines vector context with structured data for accurate chat responses
- **Quote Engine Ready**: Schema designed for plan comparison, drug lookup, and premium quoting
- **Real CMS Data**: Downloads and ingests actual 2026 CMS public data files
- **Streaming Chat**: Server-Sent Events for real-time chat responses
- **Docker Deploy**: One-command setup with docker-compose

## Quick Start

### Docker (Recommended)

```bash
git clone https://github.com/robertgrant-arch/medicare-assistant.git
cd medicare-assistant
cp .env.example .env
# Edit .env with your OpenAI API key

export OPENAI_API_KEY=sk-your-key-here
docker-compose up -d

# Setup database tables
docker-compose exec app node scripts/setup-db.js

# Download CMS data (~2-5GB)
docker-compose exec app node scripts/etl/download.js

# Ingest into database
docker-compose exec app npm run ingest:all

# Generate vector embeddings
docker-compose exec app node scripts/etl/vectorize.js

# Open http://localhost:3000
```

### Local Development

```bash
# Prerequisites: Node 20+, PostgreSQL 16+ with pgvector
npm install
cp .env.example .env
# Configure DATABASE_URL and OPENAI_API_KEY in .env

# Setup database
node scripts/setup-db.js

# Download and ingest data
npm run download
npm run ingest:all
npm run vectorize

# Start server
npm start
```

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/health` | GET | Health check with plan/vector counts |
| `/api/plans` | GET | Structured plan search with filters |
| `/api/plans/:contractId/:planId` | GET | Plan details with benefits/formulary |
| `/api/drugs` | GET | Drug name search across formularies |
| `/api/chat` | POST | RAG chat with streaming (SSE) |
| `/api/stats` | GET | Database table counts |

### Plan Search Parameters

- `state` - 2-letter state code (e.g., CA, TX, FL)
- `county` - County name (partial match)
- `fips` - FIPS county code
- `plan_type` - HMO, PPO, PFFS, PDP, SNP
- `max_premium` - Maximum monthly premium
- `drug_name` - Filter by covered drug
- `limit` - Results limit (default 50)

## Data Sources

All data sourced from CMS.gov public files:

- **Landscape files**: MA/PDP plan directory with premiums, counties
- **PBP Benefits**: Plan Benefit Package cost-sharing details
- **Star Ratings**: Overall, health, and drug plan quality ratings
- **Enrollment CPSC**: Monthly enrollment by county/plan
- **Formulary PUF**: Drug formulary with tier, PA, ST, QL
- **Service Areas**: County-level plan availability
- **SNP Data**: Special Needs Plan comprehensive reports
- **Plan Directory**: MA plan directory with org details

## Database Schema

- `plans` - Core plan data (contract, premiums, county, type)
- `benefits` - Benefit categories and cost-sharing
- `enrollment` - Monthly enrollment counts by county
- `formulary` - Drug coverage with tier/restrictions
- `ratings` - Star ratings by contract
- `service_areas` - County-level plan availability
- `crosswalks` - Year-over-year plan mapping
- `vector_chunks` - Embedded text chunks for semantic search

## Quote Engine Integration

The `/api/plans` endpoint supports all filters needed for a quote engine:

```javascript
// Example: Find $0 premium HMO plans in Miami covering Eliquis
fetch('/api/plans?state=FL&county=Miami-Dade&plan_type=HMO&max_premium=0&drug_name=Eliquis')
```

## Environment Variables

| Variable | Description |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string |
| `OPENAI_API_KEY` | OpenAI API key for embeddings + chat |
| `EMBED_MODEL` | Embedding model (default: text-embedding-3-small) |
| `CHAT_MODEL` | Chat model (default: gpt-4o) |
| `PORT` | Server port (default: 3000) |

## License

MIT
