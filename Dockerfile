FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HEALTH_SERVER_HOST=0.0.0.0 \
    HEALTH_CHECK_PORT=8080

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    useradd --create-home --shell /usr/sbin/nologin appuser

COPY --chown=appuser:appuser . /app
USER appuser

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,os;urllib.request.urlopen('http://127.0.0.1:'+os.getenv('HEALTH_CHECK_PORT','8080')+'/live', timeout=3)" || exit 1

CMD ["python", "program.py"]
