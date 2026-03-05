FROM python:3.13-slim

WORKDIR /app

# System dependencies for thefuzz[speedup]
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir ".[compute]"

# Copy application code
COPY db/ db/
COPY entity/ entity/
COPY space/ space/
COPY tools/ tools/
COPY sources/ sources/
COPY normalize/ normalize/
COPY server.py .

# Default data directory (mount your DB here)
RUN mkdir -p /app/data
VOLUME /app/data

ENV PATENT_DB_PATH=/app/data/patents.db

# Expose HTTP transport port
EXPOSE 8001

# Default: stdio transport (for Claude Desktop integration)
# Override with --transport http for HTTP mode
ENTRYPOINT ["python", "server.py"]
CMD ["--transport", "stdio"]
