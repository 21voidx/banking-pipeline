# ─────────────────────────────────────────────────────────────────────────────
# banking-dbt Dockerfile
# Runs dbt-bigquery in an isolated container for Airflow DockerOperator
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim

# Metadata
LABEL maintainer="data-engineering@bank.co.id"
LABEL description="dbt-bigquery image for Banking Data Platform transformation layer"

# Set working directory
WORKDIR /dbt

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install dbt + adapters
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy dbt project files
COPY dbt_project.yml  .
COPY profiles.yml     .
COPY packages.yml     .
COPY macros/          ./macros/
COPY models/          ./models/
COPY snapshots/       ./snapshots/
COPY tests/           ./tests/

# Install dbt packages at build time (dbt deps)
RUN dbt deps --project-dir /dbt --profiles-dir /dbt || true
# Note: dbt deps requires GCP auth; if it fails here it will run at task start

# Non-root user for security
RUN useradd -m -u 1000 dbt && chown -R dbt:dbt /dbt
USER dbt

# Default entrypoint — overridden by DockerOperator command
ENTRYPOINT ["dbt"]
CMD ["--help"]
