# Windows-Native Data Transformation Platform

A fully local Windows-native data transformation platform where users can write Python scripts to safely transform PostgreSQL data.

## Features

- **Table Explorer**: View and preview PostgreSQL tables
- **Script Editor**: Write Python transformation scripts
- **Sandboxed Execution**: Execute scripts safely with 5-layer security
- **Job Management**: Queue, monitor, and review transformation jobs
- **User Authentication**: JWT-based local authentication

## Security Architecture

This platform implements a **5-layer defense-in-depth** sandbox strategy:

1. **RestrictedPython**: AST-level code analysis and transformation
2. **Subprocess Isolation**: Each job runs in an isolated subprocess
3. **Import Allowlist**: Only pandas, numpy, datetime, and math allowed
4. **Resource Monitoring**: psutil-based CPU, memory, and timeout limits
5. **File System Controls**: Isolated temporary directory per job

> **Note**: This sandbox provides strong application-level isolation but is NOT equivalent to VM or container isolation. It is suitable for trusted internal enterprise environments.

## Requirements

- Windows 10/11 or Windows Server
- Python 3.10+
- PostgreSQL 14+

## Quick Start

### 1. Clone and Setup Python Environment

```cmd
cd sandboxed-python-execution

# Create virtual environment
python -m venv .venv

# Activate
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Setup PostgreSQL

Follow the guide in `scripts/setup_postgres.md` or:

```cmd
# Using psql
psql -U postgres
CREATE DATABASE transform_db;
\q
```

### 3. Configure Environment

```cmd
# Copy example config
copy .env.example .env

# Edit .env with your database credentials
notepad .env
```

Update these values in `.env`:
```
DATABASE_URL=postgresql+asyncpg://postgres:your_password@localhost:5432/transform_db
SYNC_DATABASE_URL=postgresql://postgres:your_password@localhost:5432/transform_db
JWT_SECRET_KEY=your-secure-random-key-here
```

### 4. Initialize Database

```cmd
# Create tables and seed test data
python scripts/seed_database.py
```

### 5. Start the Application

**Terminal 1 - API Server:**
```cmd
scripts\start_api.bat
# Or: python -m uvicorn app.main:app --port 8000
```

**Terminal 2 - Background Worker:**
```cmd
scripts\start_worker.bat
# Or: python -m worker.main
```

### 6. Access the API

- API Documentation: http://localhost:8000/docs
- Health Check: http://localhost:8000/health

**Test Credentials:**
- Email: `admin@example.com`
- Password: `admin123`

## API Usage

### Authentication

```bash
# Login
curl -X POST http://localhost:8000/api/v1/auth/login \
  -d "username=admin@example.com&password=admin123"

# Response: {"access_token": "eyJ...", "token_type": "bearer"}
```

### List Tables

```bash
curl http://localhost:8000/api/v1/tables \
  -H "Authorization: Bearer YOUR_TOKEN"
```

### Create Script

```bash
curl -X POST http://localhost:8000/api/v1/scripts \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Add Total",
    "code_text": "def transform(df):\n    df[\"total\"] = df[\"price\"] * df[\"qty\"]\n    return df"
  }'
```

### Submit Job

```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "script_id": 1,
    "source_table": "sales",
    "destination_table": "sales_with_totals"
  }'
```

### Check Job Status

```bash
curl http://localhost:8000/api/v1/jobs/1 \
  -H "Authorization: Bearer YOUR_TOKEN"
```

## Writing Transformation Scripts

Scripts must define a `transform(df)` function that:
- Takes a pandas DataFrame as input
- Returns a pandas DataFrame

### Allowed Imports

- `pandas` / `pd`
- `numpy` / `np`
- `datetime`
- `math`

### Example Script

```python
def transform(df):
    # Calculate total
    df["total"] = df["price"] * df["qty"]

    # Filter rows
    df = df[df["total"] > 100]

    # Add category
    df["category"] = df["total"].apply(
        lambda x: "high" if x > 500 else "low"
    )

    return df
```

### Blocked Operations

- File I/O (`open`, `to_csv`, `to_pickle`, etc.)
- Network access (`socket`, `urllib`, etc.)
- System commands (`os`, `subprocess`, etc.)
- Code execution (`exec`, `eval`, `compile`)
- Dangerous attribute access (`__class__`, `__globals__`, etc.)

## Project Structure

```
sandboxed-python-execution/
├── app/                    # FastAPI application
│   ├── api/v1/            # API endpoints
│   ├── core/              # Security, logging
│   ├── models/            # SQLAlchemy models
│   ├── schemas/           # Pydantic schemas
│   └── services/          # Business logic
├── worker/                 # Background job processor
├── sandbox/               # Sandboxed execution engine
├── scripts/               # Setup and utility scripts
├── examples/              # Example transformation scripts
├── alembic/               # Database migrations
└── tests/                 # Test suite
```

## Configuration

Key settings in `.env`:

| Setting | Default | Description |
|---------|---------|-------------|
| `SANDBOX_TIMEOUT_SECONDS` | 60 | Max execution time per job |
| `SANDBOX_MAX_MEMORY_MB` | 512 | Max memory per job |
| `SANDBOX_MAX_OUTPUT_ROWS` | 1,000,000 | Max output rows |
| `CHUNK_SIZE` | 50,000 | Rows per chunk for large tables |
| `MAX_CONCURRENT_JOBS` | 4 | Parallel job limit |

## Database Migrations

```cmd
# Create a new migration
alembic revision --autogenerate -m "description"

# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1
```

## Logs

Log files are stored in `logs/`:
- `app.log` - API server logs
- `worker.log` - Background worker logs
- `sandbox.log` - Sandbox execution logs

## Troubleshooting

### "Connection refused" errors
- Verify PostgreSQL is running: `net start postgresql-x64-14`
- Check DATABASE_URL in `.env`

### Jobs stuck in "pending"
- Ensure worker is running: `python -m worker.main`
- Check `logs/worker.log` for errors

### Sandbox execution failures
- Check `logs/sandbox.log` for details
- Verify script doesn't use blocked operations
- Check memory/timeout limits

## License

MIT License - See LICENSE file for details.

## Security Disclaimer

This sandbox provides strong application-level isolation but is **NOT** equivalent to VM or container isolation. It is suitable for:

- Trusted internal enterprise environments
- Authenticated users who are accountable
- Transformation scripts from known sources

It is **NOT** suitable for:
- Public-facing code execution
- Untrusted or anonymous users
- Processing highly sensitive data without additional network isolation
