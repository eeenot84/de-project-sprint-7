# Инструкции по развертыванию проекта

## Проблема
Ошибка: `/opt/airflow/scripts/user_mart.py: [Errno 2] No such file or directory`

Это означает, что файлы не скопированы на сервер или директория не существует.

## Решение 1: Через SSH (рекомендуется)

### Шаг 1: Подключиться к серверу
```bash
ssh -i /path/to/your/ssh/key yc-user@158.160.209.51
```

### Шаг 2: Создать директорию для скриптов
```bash
sudo mkdir -p /opt/airflow/scripts
sudo chown yc-user:yc-user /opt/airflow/scripts
```

### Шаг 3: Скопировать файлы с локальной машины

**В новом терминале на локальной машине:**

```bash
# Копирование DAG файла
scp -i /path/to/your/ssh/key src/dags/data_marts_dag.py yc-user@158.160.209.51:/opt/airflow/dags/

# Копирование скриптов
scp -i /path/to/your/ssh/key src/scripts/*.py yc-user@158.160.209.51:/opt/airflow/scripts/
```

### Шаг 4: Проверить наличие файлов
```bash
ssh -i /path/to/your/ssh/key yc-user@158.160.209.51 "ls -la /opt/airflow/scripts/"
```

Должны быть файлы:
- `spark_app.py`
- `build_user_mart.py`
- `build_zone_mart.py`
- `build_friends_recommendation_mart.py`

## Решение 2: Через Jupyter Notebook

### Шаг 1: Открыть Jupyter
Откройте http://158.160.209.51:8888/

### Шаг 2: Загрузить файлы
1. Создайте новый Python notebook
2. Загрузите файлы через веб-интерфейс Jupyter
3. Выполните команды:

```python
import os
import shutil

# Создать директорию
os.makedirs('/opt/airflow/scripts', exist_ok=True)

# Скопировать файлы (предполагая, что они загружены в текущую директорию)
files = ['spark_app.py', 'build_user_mart.py', 'build_zone_mart.py', 'build_friends_recommendation_mart.py']
for file in files:
    if os.path.exists(file):
        shutil.copy(file, f'/opt/airflow/scripts/{file}')
        print(f'Скопирован {file}')

# Проверить
print(os.listdir('/opt/airflow/scripts/'))
```

### Шаг 3: Скопировать DAG файл
```python
# Скопировать DAG файл
shutil.copy('data_marts_dag.py', '/opt/airflow/dags/data_marts_dag.py')
```

## Решение 3: Использовать скрипт deploy.sh

```bash
# Сделать скрипт исполняемым
chmod +x deploy.sh

# Запустить развертывание
./deploy.sh /path/to/your/ssh/key yc-user 158.160.209.51
```

## После развертывания

1. Откройте Airflow UI: http://158.160.209.51:3000/airflow/
2. Найдите DAG `datalake_dag`
3. Включите DAG (переключите тумблер)
4. Запустите вручную через "Trigger DAG"

## Проверка

После копирования файлов проверьте:

```bash
# На сервере
ls -la /opt/airflow/scripts/
ls -la /opt/airflow/dags/data_marts_dag.py
```

## Устранение проблем

Если файлы не копируются из-за прав доступа:

```bash
# На сервере
sudo chown -R yc-user:yc-user /opt/airflow/scripts
sudo chown -R yc-user:yc-user /opt/airflow/dags
```



