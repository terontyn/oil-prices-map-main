FROM python:3.10-slim

WORKDIR /app

# Устанавливаем системные зависимости, если нужны для curl_cffi или гео-библиотек
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Запускаем через наш цикличный скрипт
CMD ["python", "-u", "run.py"]
