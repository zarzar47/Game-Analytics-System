import time
import random
import requests
import os

# "api" is the docker-compose service name
TARGET_URL = os.getenv("API_URL", "http://api:8000/internal/ingest")
GAMES = [101, 102, 103, 104, 2077]

def generate_fake_data():
    while True:
        payload = {
            "game_id": random.choice(GAMES),
            "player_count": random.randint(50, 50000),
            "sentiment_score": round(random.uniform(0.1, 1.0), 2)
        }
        
        try:
            resp = requests.post(TARGET_URL, json=payload)
            print(f"✅ Sent: {payload} | Status: {resp.status_code}")
        except Exception as e:
            print(f"❌ Error connecting to API: {e}")
            
        time.sleep(3) # Simulate 3 second interval

if __name__ == "__main__":
    print("🚀 Faker Service Started...")
    time.sleep(5) # Give API time to boot
    generate_fake_data()
