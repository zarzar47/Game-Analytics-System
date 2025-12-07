import time
import random
import json
import os
from kafka import KafkaProducer
from game_library import get_games

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "GameAnalytics"

def get_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Connected to Kafka")
            return producer
        except Exception as e:
            print(f"⏳ Waiting for Kafka: {e}")
            time.sleep(5)

def generate_fake_data():
    producer = get_producer()
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
            "game_name": game["name"], # Denormalize for easier UI
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
            # Produce to Kafka
            producer.send(TOPIC_NAME, payload)
            print(f"✅ Sent to Kafka [{event_type}]: {game['name']}")
        except Exception as e:
            print(f"❌ Error sending to Kafka: {e}")
            
        time.sleep(1) # Interval

if __name__ == "__main__":
    print("🚀 Faker Service Started (Kafka Mode)...")
    generate_fake_data()
