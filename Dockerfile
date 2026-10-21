FROM python:3.11.17-slim-bookworm@sha256:0a310eeecf4e1f5a0743f9a6520c90c88d089c903ca5fd283f501e3a805f5f89

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HEALTH_SERVER_HOST=0.0.0.0 \
    HEALTH_CHECK_PORT=8080

WORKDIR /app
COPY requirements.lock .
RUN pip install --no-cache-dir -r requirements.lock && \
    useradd --create-home --shell /usr/sbin/nologin appuser

COPY --chown=appuser:appuser . /app
USER appuser

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,os;urllib.request.urlopen('http://127.0.0.1:'+os.getenv('HEALTH_CHECK_PORT','8080')+'/live', timeout=3)" || exit 1

CMD ["python", "program.py"]
