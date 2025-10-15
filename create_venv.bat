@echo off
echo Creating virtual environment...

:: Remove existing venv if exists
if exist "venv" (
    echo Removing existing virtual environment...
    rmdir /s /q venv
)

:: Try different Python commands
echo Trying 'python' command...
python -m venv venv
if exist "venv\Scripts\activate.bat" goto success

echo Trying 'py' command...
py -m venv venv
if exist "venv\Scripts\activate.bat" goto success

echo Trying 'python3' command...
python3 -m venv venv
if exist "venv\Scripts\activate.bat" goto success

echo All methods failed. Please install Python or check your PATH.
pause
exit /b 1

:success
echo Virtual environment created successfully!

:: Activate and install dependencies
echo Activating virtual environment...
call venv\Scripts\activate.bat

echo Upgrading pip...
pip install --upgrade pip

echo Installing dependencies from requirements.txt...
if exist requirements.txt (
    pip install -r requirements.txt
) else (
    echo requirements.txt not found, installing dependencies manually...
    pip install requests aiohttp ping3 asyncio
)

echo Installing additional dependencies for IPTV...
pip install requests aiohttp ping3

echo Checking installation...
pip list

echo.
echo ========================================
echo Virtual environment setup complete!
echo To activate manually, run: venv\Scripts\activate.bat
echo ========================================
pause