FROM python:3.11-slim

WORKDIR /app

# Copy and install Shared Library
COPY packages/game_library /tmp/game_library
RUN pip install /tmp/game_library

# Install App Dependencies
COPY apps/web/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy App Code
COPY apps/web/ .

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]