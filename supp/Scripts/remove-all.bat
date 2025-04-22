@echo off
REM Stop all
for /F "tokens=*" %%i in ('docker ps -q') do docker stop %%i

REM Remove all
for /F "tokens=*" %%i in ('docker ps -aq') do docker rm %%i