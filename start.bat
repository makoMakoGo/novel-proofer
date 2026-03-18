@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

echo [novel-proofer] Working dir: %cd%

set "MODE=serve"
if /i "%~1"=="--smoke" set "MODE=smoke"

where uv >nul 2>&1
if errorlevel 1 (
  echo [novel-proofer] uv is required but was not found.
  echo [novel-proofer] Install uv first: https://docs.astral.sh/uv/getting-started/installation/
  exit /b 1
)

for /f "delims=" %%v in ('uv --version 2^>^&1') do set "UVVER=%%v"
echo [novel-proofer] Using: !UVVER!

if /i "%MODE%"=="smoke" (
  uv sync --frozen --no-install-project --group dev
  if errorlevel 1 exit /b 1

  for /f "delims=" %%v in ('uv run --frozen --no-sync python --version 2^>^&1') do set "PYVER=%%v"
  echo [novel-proofer] Using: !PYVER!

  echo [novel-proofer] Running tests...
  uv run --frozen --no-sync pytest -q
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
