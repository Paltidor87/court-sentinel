FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy everything from current directory
COPY . .

# Default environment variables for Cloud Run
ENV PORT=8080
ENV BOT_TIER=cost
ENV ENABLE_WEB_UI=1

EXPOSE 8080
ENV PORT 8080

CMD ["python", "main.py"]
