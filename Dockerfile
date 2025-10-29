# Use a compatible Python image for psutil
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Expose port (Render uses PORT env var)
EXPOSE 8081

# Clean extra cache to reduce RAM spikes
RUN rm -rf /root/.cache

# Start bot
CMD ["python", "bot2.py"]
