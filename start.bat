@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem One-click starter for Novel Proofer (Windows)
rem - Creates/uses .venv
rem - Installs deps from requirements.txt if present
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

if not exist "%PYTHON_EXE%" (
  echo [novel-proofer] .venv not found, creating...
  python -m venv "%VENV_DIR%"
  if errorlevel 1 (
    echo [novel-proofer] Failed to create virtual environment.
    echo [novel-proofer] Please ensure Python is installed and on PATH.
    exit /b 1
  )
)

call "%VENV_DIR%\Scripts\activate.bat"
if errorlevel 1 (
  echo [novel-proofer] Failed to activate virtual environment.
  exit /b 1
)

for /f "delims=" %%v in ('"%PYTHON_EXE%" --version 2^>^&1') do set "PYVER=%%v"
echo [novel-proofer] Using: %PYVER%

if exist "requirements.txt" (
  set "HAS_REQ=0"
  for /f "usebackq delims=" %%l in ("requirements.txt") do (
    set "LINE=%%l"
    if not "!LINE!"=="" (
      if not "!LINE:~0,1!"=="#" (
        set "HAS_REQ=1"
      )
    )
  )

  if "!HAS_REQ!"=="1" (
    echo [novel-proofer] Installing dependencies from requirements.txt...
    "%PYTHON_EXE%" -m pip --disable-pip-version-check install -r requirements.txt
    if errorlevel 1 (
      echo [novel-proofer] Dependency install failed.
      echo [novel-proofer] If you are offline or behind a proxy, configure pip accordingly.
      exit /b 1
    )
  ) else (
    echo [novel-proofer] requirements.txt has no dependencies, skipping install.
  )
) else (
  echo [novel-proofer] No requirements.txt, skipping dependency install.
)

if /i "%MODE%"=="smoke" (
  echo [novel-proofer] Running tests...
  if exist "requirements-dev.txt" (
    echo [novel-proofer] Installing dev dependencies from requirements-dev.txt...
    "%PYTHON_EXE%" -m pip --disable-pip-version-check install -r requirements-dev.txt
    if errorlevel 1 (
      echo [novel-proofer] Dev dependency install failed.
      exit /b 1
    )
  )
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
