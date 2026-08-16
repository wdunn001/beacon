FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*
# rns + umsgpack for the mesh; psycopg2 for Postgres; meshdata for page
# categorization (schema.org-for-micron). psycopg2-binary ships a wheel.
RUN pip install --no-cache-dir rns umsgpack psycopg2-binary \
    "meshdata @ git+https://github.com/wdunn001/meshdata@main"
COPY beacon /app/beacon
WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python3", "-m", "beacon"]
CMD ["--config", "/config/rns"]
