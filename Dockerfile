# --- Stage 1: Build & Dependency Installation ---
FROM python:3.11-slim AS builder

WORKDIR /app

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install build dependencies if needed, and isolate wheel installation
RUN pip install --no-cache-dir --upgrade pip

COPY scripts/requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt


# --- Stage 2: Final Secure Runtime Environment ---
FROM python:3.11-slim AS runner

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/home/worker/.local/bin:$PATH

# Create a dedicated non-privileged user for OKD compliance
RUN groupadd -g 10001 worker && \
    useradd -u 10001 -g worker -m worker

# Copy installed packages from the builder stage
COPY --from=builder --chown=worker:worker /root/.local /home/worker/.local
COPY --chown=worker:worker scripts/sync.py .

# Switch context to the non-root user
USER 10001

# Run the sync process
CMD ["python", "sync.py"]
