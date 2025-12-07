import time
import random
import requests
import os
from game_library import get_games

# "api" is the docker-compose service name
TARGET_URL = os.getenv("API_URL", "http://api:8000/internal/ingest")

def generate_fake_data():
    games = get_games()
    
    while True:
        # Pick a random game
        game = random.choice(games)
        game_id = game["id"]
        
        # Decide event type
        event_type = random.choices(
            ["status", "review", "purchase"], 
            weights=[0.7, 0.2, 0.1],
            k=1
        )[0]
        
        payload = {
            "game_id": game_id,
            "event_type": event_type,
            "timestamp": time.time()
        }
        
        if event_type == "status":
            payload["player_count"] = random.randint(500, 50000)
            payload["sentiment_score"] = round(random.uniform(0.1, 1.0), 2)
            
        elif event_type == "review":
            payload["sentiment_score"] = round(random.uniform(0.1, 1.0), 2)
            payload["review_text"] = random.choice([
                "Great game!", "Too buggy.", "Loved the graphics.", "Needs balance updates.", 
                "Best RPG ever.", "Trash.", "Addictive!"
            ])
            payload["playtime_session"] = round(random.uniform(0.5, 4.0), 1)
            payload["playtime_total"] = round(random.uniform(10.0, 500.0), 1)
            
        elif event_type == "purchase":
            payload["purchase_amount"] = random.choice([9.99, 19.99, 59.99, 4.99])

        try:
            resp = requests.post(TARGET_URL, json=payload)
            print(f"✅ Sent [{event_type}]: {game['name']} | Status: {resp.status_code}")
        except Exception as e:
            print(f"❌ Error connecting to API: {e}")
            
        time.sleep(1) # Faster generation

if __name__ == "__main__":
    print("🚀 Faker Service Started...")
    time.sleep(5) # Give API time to boot
    generate_fake_data()