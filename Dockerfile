FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# rns + umsgpack for the mesh; psycopg2 for Postgres. psycopg2-binary ships a
# wheel so no compiler is needed.
RUN pip install --no-cache-dir rns umsgpack psycopg2-binary
COPY beacon /app/beacon
WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python3", "-m", "beacon"]
CMD ["--config", "/config/rns"]
