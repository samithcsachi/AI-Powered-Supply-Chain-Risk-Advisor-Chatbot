FROM python:3.11-bullseye

WORKDIR /app

# Copy requirements first for efficient layer caching
COPY requirements.txt .

# Upgrade pip & install dependencies
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Expose the same port you use locally
EXPOSE 8000

# Run Chainlit app (updated syntax)
CMD ["chainlit", "run", "src/app/app.py", "--host", "0.0.0.0", "--port", "8000"]
