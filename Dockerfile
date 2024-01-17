FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .
RUN pip install --no-cache-dir -e .

# Default: show help
ENTRYPOINT ["python", "-m", "sql_to_dag.generator"]
CMD ["--help"]
