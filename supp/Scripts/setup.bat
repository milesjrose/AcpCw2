@echo off

cd C:\Users\miles\Documents\GitHub\AcpCw2
echo starting redis...
docker run -d --name redis -p 6379:6379 -p 8001:8001 redis/redis-stack:latest
timeout /t 1 /nobreak >nul
echo starting rabbitmq...
docker run -it --rm -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.0-management
timeout /t 1 /nobreak >nul
echo creating acp_network...
docker network create acp_network
timeout /t 1 /nobreak >nul
echo starting acp_network...
docker compose -p acp up -d 
timeout /t 1 /nobreak >nul
echo loading topics...
cd C:\kafka_2.13-4.0.0\bin\windows
call kafka-topics.bat --bootstrap-server localhost:9092 --topic stockvalues  --create --partitions 2 --replication-factor 1
echo returning to scripts...
cd C:\Users\miles\scripts