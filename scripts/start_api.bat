@REM @echo off
@REM REM Start the FastAPI server
@REM cd /d "%~dp0\.."
@REM if exist .venv\Scripts\activate.bat (
@REM     call .venv\Scripts\activate.bat
@REM ) else (
@REM     echo Virtual environment not found. Please run: python -m venv .venv
@REM     exit /b 1
@REM )
@REM echo Starting FastAPI server...
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
