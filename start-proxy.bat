@echo off
chcp 65001 >nul
cd /d "%~dp0"
set PY="C:\Users\34430\.workbuddy\binaries\python\versions\3.13.12\python.exe"

echo ============================================
echo   燕子户外 · 本地数据代理
echo   提供：和风天气 / 12306 车次余票 / 最便宜方案
echo ============================================
echo.

rem 依次尝试端口：8899 优先，被占用则换 8901 / 8902 / 8903
for %%P in (8899 8901 8902 8903) do (
  netstat -ano | findstr /r /c:"127.0.0.1:%%P .*LISTENING" >nul
  if errorlevel 1 (
    set "YANZI_PORT=%%P"
    goto :start
  ) else (
    echo [跳过] 端口 %%P 已被占用
  )
)

echo 8899/8901/8902/8903 全部被占用，请先关闭旧的代理窗口再试。
pause
exit /b 1

:start
echo [启动] 使用端口 %YANZI_PORT%
echo.
echo   页面会自动发现这个端口，无需手动修改。
echo   保持本窗口打开；关闭窗口即停止代理。
echo.
%PY% proxy.py

echo.
echo 代理已退出（可能是端口冲突或按了 Ctrl+C）。
pause
