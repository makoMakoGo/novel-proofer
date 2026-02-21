@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem One-click starter for Novel Proofer (Windows)
rem - Prefers uv (pyproject.toml + uv.lock)
rem - Fallback: creates/uses .venv and installs deps from requirements.lock.txt
rem - Picks a free port (default 18080)
rem - Starts server and prints URL
rem - Optional: run tests in venv

cd /d "%~dp0"

echo [novel-proofer] Working dir: %cd%

set "VENV_DIR=.venv"
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"

rem Parse args
set "MODE=serve"
if /i "%~1"=="--smoke" set "MODE=smoke"


rem Prefer uv when available
where uv >nul 2>&1
if not errorlevel 1 (
  for /f "delims=" %%v in ('uv --version 2^>^&1') do set "UVVER=%%v"
  echo [novel-proofer] Using: !UVVER!

  if /i "%MODE%"=="smoke" (
    uv sync --frozen --no-install-project --group dev
    if errorlevel 1 exit /b 1

    for /f "delims=" %%v in ('uv run --frozen --no-sync python --version 2^>^&1') do set "PYVER=%%v"
    echo [novel-proofer] Using: !PYVER!

    echo [novel-proofer] Running tests...
    uv run --frozen --no-sync -m pytest -q
    if errorlevel 1 exit /b 1
    echo [novel-proofer] Tests OK.
    exit /b 0
  )

  uv sync --frozen --no-install-project --no-dev
  if errorlevel 1 exit /b 1

  for /f "delims=" %%v in ('uv run --frozen --no-sync python --version 2^>^&1') do set "PYVER=%%v"
  echo [novel-proofer] Using: !PYVER!

  set "HOST=127.0.0.1"
  if defined NP_HOST set "HOST=%NP_HOST%"

  set "PORT=18080"
  if defined NP_PORT set "PORT=%NP_PORT%"

  call :pick_port
  if errorlevel 1 exit /b 1

  echo [novel-proofer] Starting server...
  echo [novel-proofer] URL: http://!HOST!:!PORT!/
  uv run --frozen --no-sync -m novel_proofer.server --host "!HOST!" --port !PORT!
  exit /b !errorlevel!
)

if not exist "%PYTHON_EXE%" (
  echo [novel-proofer] .venv not found, creating...
  python -m venv "%VENV_DIR%"
  if errorlevel 1 (
    echo [novel-proofer] Failed to create virtual environment.
    echo [novel-proofer] Please ensure Python 3.12+ is installed and on PATH.
    exit /b 1
  )
) else (
  echo [novel-proofer] Virtual environment already configured.
)

call "%VENV_DIR%\Scripts\activate.bat"
if errorlevel 1 (
  echo [novel-proofer] Failed to activate virtual environment.
  exit /b 1
)

for /f "delims=" %%v in ('"%PYTHON_EXE%" --version 2^>^&1') do set "PYVER=%%v"
echo [novel-proofer] Using: %PYVER%

if exist "requirements.lock.txt" (
  rem Check if all requirements are already satisfied (silent)
  "%PYTHON_EXE%" -c "import sys,re;from pathlib import Path;from importlib.metadata import version;from packaging.requirements import Requirement;sys.exit(0 if all(True if not (line:=re.split(r'\s+#',raw.lstrip('\ufeff').strip(),1)[0].strip()) or line.startswith('#') else (False if line.startswith('-') else ((req:=Requirement(line)) and ((req.marker is not None and not req.marker.evaluate()) or (not getattr(req,'url',None) and (not req.specifier or req.specifier.contains(version(req.name),prereleases=True)))))) for raw in Path('requirements.lock.txt').read_text(encoding='utf-8',errors='replace').splitlines()) else 1)" >nul 2>&1
  if errorlevel 1 (
    echo [novel-proofer] Installing dependencies from requirements.lock.txt...
    "%PYTHON_EXE%" -m pip --disable-pip-version-check install -r requirements.lock.txt
    if errorlevel 1 (
      echo [novel-proofer] Dependency install failed.
      echo [novel-proofer] If you are offline or behind a proxy, configure pip accordingly.
      exit /b 1
    )
  ) else (
    echo [novel-proofer] Dependencies already installed.
  )
) else (
  echo [novel-proofer] No requirements.lock.txt, skipping dependency install.
)

if /i "%MODE%"=="smoke" (
  echo [novel-proofer] Running tests...
  "%PYTHON_EXE%" -m pytest -q
  if errorlevel 1 exit /b 1
  echo [novel-proofer] Tests OK.
  exit /b 0
)

set "HOST=127.0.0.1"
if defined NP_HOST set "HOST=%NP_HOST%"

set "PORT=18080"
if defined NP_PORT set "PORT=%NP_PORT%"

call :pick_port
if errorlevel 1 exit /b 1

echo [novel-proofer] Starting server...
echo [novel-proofer] URL: http://%HOST%:%PORT%/
"%PYTHON_EXE%" -m novel_proofer.server --host "%HOST%" --port %PORT%
exit /b %errorlevel%

:pick_port
set /a END_PORT=%PORT%+30 >nul 2>&1
for /L %%p in (%PORT%,1,%END_PORT%) do (
  call :is_port_free %%p
  if "!PORT_FREE!"=="1" (
    set "PORT=%%p"
    exit /b 0
  )
)

echo [novel-proofer] No free port found in range %PORT%..%END_PORT%.
exit /b 1

:is_port_free
set "CANDIDATE=%~1"
set "PORT_FREE=0"
netstat -ano | findstr /R /C:":%CANDIDATE% .*LISTENING" >nul 2>&1
if errorlevel 1 set "PORT_FREE=1"
exit /b 0
