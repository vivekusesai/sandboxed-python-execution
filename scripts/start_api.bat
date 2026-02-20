@echo off
REM Start the FastAPI server
cd /d "%~dp0\.."
if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
) else (
    echo Virtual environment not found. Please run: python -m venv .venv
    exit /b 1
)
echo Starting FastAPI server...
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
