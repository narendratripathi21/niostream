@echo off
setlocal

if "%~1"=="" (
    echo Usage:
    echo   copy.bat "\\NAS01\Media" "\\NAS02\Backup\Media"
    exit /b 1
)

set SOURCE=%~1
set DEST=%~2

java -jar "%~dp0target\niostream-2.0.0.jar" copy ^
  --source "%SOURCE%" ^
  --destination "%DEST%" ^
  --streams 4 ^
  --chunk 16M ^
  --rate 20M ^
  --min-rate 5M ^
  --max-rate 100M ^
  --resume true ^
  --verify false ^
  --retries 0 ^
  --retry-delay 5000 ^
  --preserve-time true

endlocal
