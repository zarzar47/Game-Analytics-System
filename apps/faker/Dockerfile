FROM python:3.11-slim

WORKDIR /app

# Copy and install Shared Library
COPY packages/game_library /tmp/game_library
RUN pip install /tmp/game_library

# Install App Dependencies
RUN pip install requests kafka-python Faker

# Copy App Code
COPY apps/faker/ .

CMD ["python", "-u", "main.py"]