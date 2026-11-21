FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

# 单实例：配额与队列是进程内状态，勿多 worker / 多副本
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
