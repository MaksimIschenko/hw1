# Образ для запуска приложения (main.py)
FROM python:3.13-slim

WORKDIR /app

# Установка uv и зависимостей проекта
RUN pip install --no-cache-dir uv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Код приложения
COPY main.py ./
COPY src/ ./src/
COPY schemas/ ./schemas/

# Запуск от того же пользователя, что и venv (uv создаёт .venv в /app)
ENV PATH="/app/.venv/bin:$PATH"
CMD ["python", "main.py"]
