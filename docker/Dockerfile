# Use lightweight Python image
FROM python:3.10-slim

# Ensure logs appear immediately
ENV PYTHONUNBUFFERED=1

# Set working directory inside container
WORKDIR /app

# Install system dependencies for PostgreSQL
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy rest of application code
COPY . .

# Default command (placeholder for now)
CMD ["python", "-c", "print('Pipeline container ready')"]
