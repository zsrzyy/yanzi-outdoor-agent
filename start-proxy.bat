@echo off
chcp 65001 >nul
cd /d "%~dp0"
set PY="C:\Users\34430\.workbuddy\binaries\python\versions\3.13.12\python.exe"

echo ============================================
echo   燕子户外 · 本地数据代理（守护模式）
echo   提供：和风天气 / 12306 车次余票 / 最便宜方案
echo ============================================
echo.

rem 守护循环：代理意外退出后 3 秒自动重启，保证车票/天气随时可用
:loop
rem 依次尝试端口：8899 优先，被占用则换 8901 / 8902 / 8903
set "YANZI_PORT="
for %%P in (8899 8901 8902 8903) do (
  netstat -ano | findstr /r /c:"127.0.0.1:%%P .*LISTENING" >nul
  if errorlevel 1 (
    if not defined YANZI_PORT set "YANZI_PORT=%%P"
  ) else (
    echo [跳过] 端口 %%P 已被占用
  )
)

if not defined YANZI_PORT (
  echo 8899/8901/8902/8903 全部被占用，请先关闭旧的代理窗口再重启本脚本。
  echo 5 秒后自动重试...
  timeout /t 5 /nobreak >nul
  goto :loop
)

echo [启动] 使用端口 %YANZI_PORT%
echo   页面会自动发现这个端口，无需手动修改。
echo   保持本窗口打开；代理若意外退出会自动重启。
echo.
%PY% proxy.py

echo.
echo 代理已退出（异常或手动 Ctrl+C）。3 秒后自动重启...
timeout /t 3 /nobreak >nul
goto :loop
