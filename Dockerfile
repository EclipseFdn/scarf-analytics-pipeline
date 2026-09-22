# --- Stage 1: Build & Dependency Installation ---
FROM python:3.11-slim AS builder

WORKDIR /app

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install build dependencies if needed, and isolate wheel installation
RUN pip install --no-cache-dir --upgrade pip

COPY scripts/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# --- Stage 2: Final Secure Runtime Environment ---
FROM python:3.11-slim AS runner

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Create a dedicated non-privileged user for OKD compliance
RUN groupadd -g 10001 worker && \
    useradd -u 10001 -g worker -m worker

# Copy installed packages into system site-packages (not a --user/$HOME path):
# OKD's restricted SCC assigns an arbitrary UID at admission time regardless of
# this image's USER, and that UID has no /etc/passwd entry, so $HOME-relative
# site-packages are unresolvable and silently invisible to Python.
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --chown=worker:worker scripts/sync.py .

# Switch context to the non-root user
USER 10001

# Run the sync process
CMD ["python", "sync.py"]
