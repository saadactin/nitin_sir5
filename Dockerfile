# Multi-stage build for SQL Server to PostgreSQL Sync Application
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    HYBRID_SYNC_BATCH_SIZE=10000

# Create app directory
WORKDIR /app

# Install system dependencies and Microsoft SQL Server ODBC driver
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    gnupg2 \
    apt-transport-https \
    ca-certificates \
    lsb-release \
    wget \
    unixodbc \
    unixodbc-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Add Microsoft repository and install ODBC Driver 18 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/microsoft.gpg \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y \
        msodbcsql18 \
        mssql-tools18 \
    && echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Verify ODBC driver installation
RUN odbcinst -q -d -n "ODBC Driver 18 for SQL Server"

# Copy requirements first for better layer caching
COPY requirements.txt .

# Upgrade pip and install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir xlsxwriter openpyxl

# Create necessary directories
RUN mkdir -p /app/data/sqlserver_exports \
    && mkdir -p /app/config \
    && mkdir -p /app/templates \
    && mkdir -p /app/logs

# Copy application code
COPY . .

# Create a non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin -c "Docker image user" appuser

# Set proper permissions
RUN chown -R appuser:appuser /app \
    && chmod +x /app/*.py

# Create environment-specific configuration template
RUN echo 'postgresql:' > /app/config/db_connections.template.yaml \
    && echo '  database: ${POSTGRES_DB:-sync_db}' >> /app/config/db_connections.template.yaml \
    && echo '  host: ${POSTGRES_HOST:-postgres}' >> /app/config/db_connections.template.yaml \
    && echo '  password: ${POSTGRES_PASSWORD:-password}' >> /app/config/db_connections.template.yaml \
    && echo '  port: ${POSTGRES_PORT:-5432}' >> /app/config/db_connections.template.yaml \
    && echo '  schema: ${POSTGRES_SCHEMA:-CompanyDB}' >> /app/config/db_connections.template.yaml \
    && echo '  username: ${POSTGRES_USER:-migration_user}' >> /app/config/db_connections.template.yaml \
    && echo 'sqlservers: {}' >> /app/config/db_connections.template.yaml

# Create startup script
RUN echo '#!/bin/bash' > /app/entrypoint.sh \
    && echo 'set -e' >> /app/entrypoint.sh \
    && echo '' >> /app/entrypoint.sh \
    && echo '# Generate config from environment variables' >> /app/entrypoint.sh \
    && echo 'envsubst < /app/config/db_connections.template.yaml > /app/config/db_connections.yaml' >> /app/entrypoint.sh \
    && echo '' >> /app/entrypoint.sh \
    && echo '# Wait for PostgreSQL to be ready' >> /app/entrypoint.sh \
    && echo 'echo "Waiting for PostgreSQL to be ready..."' >> /app/entrypoint.sh \
    && echo 'while ! nc -z ${POSTGRES_HOST:-postgres} ${POSTGRES_PORT:-5432}; do' >> /app/entrypoint.sh \
    && echo '  sleep 1' >> /app/entrypoint.sh \
    && echo 'done' >> /app/entrypoint.sh \
    && echo 'echo "PostgreSQL is ready!"' >> /app/entrypoint.sh \
    && echo '' >> /app/entrypoint.sh \
    && echo '# Initialize database schema' >> /app/entrypoint.sh \
    && echo 'echo "Initializing database schema..."' >> /app/entrypoint.sh \
    && echo 'python -c "from db_utils import init_pg_schema; init_pg_schema()"' >> /app/entrypoint.sh \
    && echo '' >> /app/entrypoint.sh \
    && echo '# Start the application' >> /app/entrypoint.sh \
    && echo 'echo "Starting SQL Server to PostgreSQL Sync Application..."' >> /app/entrypoint.sh \
    && echo 'exec python app.py' >> /app/entrypoint.sh \
    && chmod +x /app/entrypoint.sh

# Install netcat for connection checking
RUN apt-get update && apt-get install -y netcat-openbsd gettext-base && rm -rf /var/lib/apt/lists/*

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:5000/login || exit 1

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Labels for metadata
LABEL maintainer="SQL Server to PostgreSQL Sync Team" \
      description="SQL Server to PostgreSQL hybrid sync application with web interface" \
      version="1.0.0" \
      org.opencontainers.image.source="https://github.com/your-repo/sql-pg-sync"