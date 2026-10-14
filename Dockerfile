FROM python:3.9-slim-buster

WORKDIR /app

# Dependencies install karein
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Project code copy karein
COPY ./dagster_tutorial /app/dagster_tutorial
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Log directories setup
RUN mkdir -p /app/dagster_tutorial/logs /app/dagster_tutorial/data

WORKDIR /app/dagster_tutorial

# Project package install karein
RUN pip install -e .
ENV DAGSTER_HOME=/app/dagster_tutorial

# Ports expose karein
EXPOSE 3001
EXPOSE 8502

# Build time par DB initialize karein
RUN python docker_setup/init_sqlite.py
RUN python docker_setup/duckdb_setup.py

# Supervisor se dono services start karein
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
