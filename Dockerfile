FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_PORT=8080 \
    DB_PATH=/data/events.db \
    RETENTION_LIMIT=100

WORKDIR /app

COPY app/ /app/app/

RUN mkdir -p /data && useradd -r -u 10001 appuser \
    && chown -R appuser:appuser /data /app
USER appuser

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=2s --retries=3 \
    CMD python -c "import os,urllib.request,sys; \
port=os.environ.get('APP_PORT','8080'); \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:'+port+'/health', timeout=2).status==200 else 1)"

CMD ["python", "-m", "app"]
