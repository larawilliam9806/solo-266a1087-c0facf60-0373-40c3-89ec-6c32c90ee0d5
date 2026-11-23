FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    ARCH_DB_PATH=/data/archaeology.db \
    TMPDIR=/tmp

WORKDIR /app

# 大响应的 SpooledTemporaryFile 超过内存阈值后落到 TMPDIR，
# 容器内 /tmp 默认可写；显式声明便于运维改用 tmpfs 挂载
RUN mkdir -p /data /tmp

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

VOLUME ["/data"]

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --retries=5 \
    CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health').status==200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
