# The serving layer: a tiny python image, no JVM.  It reads Redis/Postgres and
# the sklearn artifact the training job published - it never runs Spark.
FROM docker.io/python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt /srv/requirements.txt
RUN pip install -r /srv/requirements.txt

COPY serve.py /srv/serve.py
RUN useradd --create-home --uid 10001 apiuser && chown -R apiuser /srv
USER apiuser

EXPOSE 8000
# `|| exit 1` makes the healthcheck fail loudly instead of on a 200-only check
CMD ["python3", "-c", "import uvicorn, os; uvicorn.run('serve:app', host='0.0.0.0', port=int(os.environ.get('PORT','8000')))"]
