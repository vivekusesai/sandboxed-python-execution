@echo off
REM Start the background worker
cd /d "%~dp0\.."
if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
) else (
    echo Virtual environment not found. Please run: python -m venv .venv
    exit /b 1
)
echo Starting background worker...
python -m worker.main
