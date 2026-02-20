# PostgreSQL Setup Guide for Windows

## Installation

1. **Download PostgreSQL for Windows**
   - Go to https://www.postgresql.org/download/windows/
   - Download the installer (version 14 or later recommended)
   - Run the installer and follow the prompts

2. **During Installation**
   - Set a password for the `postgres` superuser (remember this!)
   - Default port: 5432
   - Select your locale
   - Optionally install Stack Builder for additional tools

## Create Database

1. **Open pgAdmin** (installed with PostgreSQL) or use command line

2. **Using Command Line (recommended)**
   ```cmd
   # Open Command Prompt as Administrator
   # Navigate to PostgreSQL bin directory
   cd "C:\Program Files\PostgreSQL\14\bin"

   # Connect as postgres user
   psql -U postgres

   # Create the database
   CREATE DATABASE transform_db;

   # Create a dedicated user (optional but recommended)
   CREATE USER transform_user WITH PASSWORD 'your_secure_password';
   GRANT ALL PRIVILEGES ON DATABASE transform_db TO transform_user;

   # Exit psql
   \q
   ```

3. **Using pgAdmin**
   - Right-click on "Databases"
   - Select "Create" > "Database"
   - Name: `transform_db`
   - Owner: `postgres` (or your dedicated user)

## Configuration

1. **Update .env file**
   Copy `.env.example` to `.env` and update:
   ```
   DATABASE_URL=postgresql+asyncpg://postgres:your_password@localhost:5432/transform_db
   SYNC_DATABASE_URL=postgresql://postgres:your_password@localhost:5432/transform_db
   ```

2. **Test Connection**
   ```cmd
   cd your_project_directory
   .venv\Scripts\activate
   python -c "from app.database import engine; print('Connection OK')"
   ```

## Verify PostgreSQL Service

1. **Check if PostgreSQL is running**
   - Press Win+R, type `services.msc`
   - Find "postgresql-x64-14" (or your version)
   - Status should be "Running"

2. **Start PostgreSQL if stopped**
   ```cmd
   net start postgresql-x64-14
   ```

## Troubleshooting

### Connection Refused
- Verify PostgreSQL service is running
- Check firewall settings (port 5432)
- Verify pg_hba.conf allows local connections

### Authentication Failed
- Verify password in .env matches PostgreSQL user password
- Check pg_hba.conf authentication method (should be `md5` or `scram-sha-256`)

### pg_hba.conf Location
Usually at: `C:\Program Files\PostgreSQL\14\data\pg_hba.conf`

Ensure this line exists for local connections:
```
host    all    all    127.0.0.1/32    scram-sha-256
```

After editing, restart PostgreSQL:
```cmd
net stop postgresql-x64-14
net start postgresql-x64-14
```
