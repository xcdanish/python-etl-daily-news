FROM python:3.9-slim-buster

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source code and service configurations
COPY ./dagster_tutorial /app/dagster_tutorial
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Set up directories for data and run logs
RUN mkdir -p /app/dagster_tutorial/logs /app/dagster_tutorial/data

WORKDIR /app/dagster_tutorial

# Install the orchestration package in editable mode
RUN pip install -e .
ENV DAGSTER_HOME=/app/dagster_tutorial

# Expose ports: Dagster UI (3001) and Streamlit Dashboard (8502)
EXPOSE 3001
EXPOSE 8502

# Seed default database schemas at build time
RUN python docker_setup/init_sqlite.py
RUN python docker_setup/duckdb_setup.py

# Entrypoint initializes databases if mounted from an empty volume
ENTRYPOINT ["/app/entrypoint.sh"]

# Start Supervisord to manage Dagster webserver, daemon, and Streamlit
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
