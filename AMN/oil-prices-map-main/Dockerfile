FROM python:3.9-slim

WORKDIR /app

# Сначала копируем зависимости (для кэширования слоев Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY src/ ./src/
COPY templates/ ./templates/
COPY main.py .

# Создаем директории для данных, если их нет
RUN mkdir -p data output

# Переменные окружения (дефолтные)
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
