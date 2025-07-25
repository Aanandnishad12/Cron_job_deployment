# Use a lightweight Python base image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy rest of your code
COPY update.py .env ./

# Set default command (can be overridden in ECS task)
CMD ["python", "update.py"]
