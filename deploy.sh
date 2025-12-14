#!/bin/bash

# Скрипт для развертывания проекта на сервере
# Использование: ./deploy.sh [SSH_KEY_PATH] [SERVER_USER] [SERVER_IP]

SSH_KEY=${1:-"~/.ssh/id_rsa"}
SERVER_USER=${2:-"yc-user"}
SERVER_IP=${3:-"158.160.209.51"}

echo "Развертывание проекта на сервер $SERVER_USER@$SERVER_IP..."

# Создание директории для скриптов (если не существует)
echo "Создание директории /opt/airflow/scripts..."
ssh -i "$SSH_KEY" "$SERVER_USER@$SERVER_IP" "sudo mkdir -p /opt/airflow/scripts && sudo chown $SERVER_USER:$SERVER_USER /opt/airflow/scripts"

# Копирование DAG файла
echo "Копирование DAG файла..."
scp -i "$SSH_KEY" src/dags/data_marts_dag.py "$SERVER_USER@$SERVER_IP:/opt/airflow/dags/"

# Копирование скриптов
echo "Копирование скриптов..."
scp -i "$SSH_KEY" src/scripts/*.py "$SERVER_USER@$SERVER_IP:/opt/airflow/scripts/"

# Проверка наличия файлов
echo "Проверка наличия файлов на сервере..."
ssh -i "$SSH_KEY" "$SERVER_USER@$SERVER_IP" "ls -la /opt/airflow/scripts/"

echo ""
echo "Развертывание завершено!"
echo "Теперь откройте Airflow UI: http://$SERVER_IP:3000/airflow/"
echo "И включите DAG 'datalake_dag'"

