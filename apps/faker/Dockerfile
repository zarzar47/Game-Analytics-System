FROM python:3.11-slim
WORKDIR /app
RUN pip install requests
COPY . .
CMD ["python", "-u", "main.py"] 
# -u is crucial to see python print statements in docker logs immediately
