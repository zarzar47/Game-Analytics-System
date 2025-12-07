FROM python:3.11-slim

WORKDIR /app

# Copy and install Shared Library
COPY packages/game_library /tmp/game_library
RUN pip install /tmp/game_library

# Install App Dependencies
COPY apps/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy App Code
COPY apps/api/ .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]