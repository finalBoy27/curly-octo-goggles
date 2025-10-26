# Use the smallest Python image
FROM python:3.10-alpine

# Set working directory
WORKDIR /app

# Install only necessary system libs (if needed)
RUN apk update && apk add --no-cache \
    curl \
    libxml2 \
    libxslt \
    && rm -rf /var/cache/apk/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Expose Flask port (if you have a web server)
EXPOSE 10000

# Clean extra cache to reduce RAM spikes
RUN rm -rf /root/.cache

# Start bot
CMD ["python", "bot.py"]
