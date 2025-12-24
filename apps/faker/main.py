import time
import json
import os
import random
import uuid
import math
from datetime import datetime
from kafka import KafkaProducer
from game_library import get_games
from faker import Faker

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "GameAnalytics"

# Initialize Faker
fake = Faker()

# --- MARKOV CHAIN CONFIGURATION ---

# States
STATE_OFFLINE = "OFFLINE"
STATE_LOBBY = "LOBBY"
STATE_MATCHMAKING = "MATCHMAKING"
STATE_IN_GAME = "IN_GAME"
STATE_SHOP = "SHOP"

# Player Archetypes
ARCHETYPES = {
    'CASUAL': {
        'churn_prob': 0.05,  # High chance to quit
        'skill': 0.3,
        'spend_propensity': 0.01,
        'transitions': {
            STATE_LOBBY:       {STATE_MATCHMAKING: 0.6, STATE_SHOP: 0.05, STATE_OFFLINE: 0.35},
            STATE_MATCHMAKING: {STATE_IN_GAME: 0.9, STATE_LOBBY: 0.1},
            STATE_IN_GAME:     {STATE_LOBBY: 0.1, STATE_IN_GAME: 0.9}, # Self-loop = duration
            STATE_SHOP:        {STATE_LOBBY: 0.9, STATE_SHOP: 0.1}
        }
    },
    'HARDCORE': {
        'churn_prob': 0.01,
        'skill': 0.8,
        'spend_propensity': 0.05,
        'transitions': {
            STATE_LOBBY:       {STATE_MATCHMAKING: 0.85, STATE_SHOP: 0.05, STATE_OFFLINE: 0.1},
            STATE_MATCHMAKING: {STATE_IN_GAME: 0.95, STATE_LOBBY: 0.05},
            STATE_IN_GAME:     {STATE_LOBBY: 0.02, STATE_IN_GAME: 0.98}, # Grinds for hours
            STATE_SHOP:        {STATE_LOBBY: 0.9, STATE_SHOP: 0.1}
        }
    },
    
    'WHALE': {
        'churn_prob': 0.02,
        'skill': 0.5,
        'spend_propensity': 0.8, # Loves buying
        'transitions': {
            STATE_LOBBY:       {STATE_MATCHMAKING: 0.5, STATE_SHOP: 0.4, STATE_OFFLINE: 0.1},
            STATE_MATCHMAKING: {STATE_IN_GAME: 0.9, STATE_LOBBY: 0.1},
            STATE_IN_GAME:     {STATE_LOBBY: 0.05, STATE_IN_GAME: 0.95},
            STATE_SHOP:        {STATE_LOBBY: 0.5, STATE_SHOP: 0.5} # Stays in shop
        }
    }
}

class MarkovPlayer:
    def __init__(self, game_pool):
        self.player_id = str(uuid.uuid4())
        self.username = fake.user_name()
        self.country = fake.country_code()
        self.region = random.choice(['NA', 'EU', 'ASIA', 'SA', 'OCE'])
        self.platform = random.choice(['PC', 'PS5', 'Xbox', 'Switch', 'Mobile'])
        self.archetype_name = random.choices(
            ['CASUAL', 'HARDCORE', 'WHALE'], 
            weights=[0.6, 0.3, 0.1], k=1
        )[0]
        self.profile = ARCHETYPES[self.archetype_name]
        
        self.state = STATE_OFFLINE
        self.game = random.choice(game_pool) # Affinity for one game mostly
        self.session_id = None
        self.last_state_change = time.time()
        
        # State Metadata
        self.match_id = None
        self.current_level = random.randint(1, 100)

    def transition(self):
        """Executes a Markov transition based on current state and archetype probabilities."""
        current_trans = self.profile['transitions'].get(self.state)
        
        if not current_trans:
            # If we are OFFLINE, external logic handles logging us in (Arrival Process)
            # If we are in a state with no map, default to Lobby
            return

        # Weighted random choice for next state
        next_state = random.choices(
            list(current_trans.keys()),
            weights=list(current_trans.values()),
            k=1
        )[0]

        # Handle State Entry/Exit Logic
        if self.state == STATE_LOBBY and next_state == STATE_OFFLINE:
            self._end_session()
        
        if self.state == STATE_MATCHMAKING and next_state == STATE_IN_GAME:
            self.match_id = str(uuid.uuid4())

        self.state = next_state
        self.last_state_change = time.time()

    def login(self):
        self.state = STATE_LOBBY
        self.session_id = str(uuid.uuid4())
        self.last_state_change = time.time()
        return self._generate_base_event("session_start")

    def _end_session(self):
        self.state = STATE_OFFLINE
        self.session_id = None
        self.match_id = None

    def tick(self, producer, topic):
        """Called every simulation tick. Generates events based on current state."""
        if self.state == STATE_OFFLINE:
            return

        # 1. Always possible to transition (State Change)
        # We limit transitions to happen somewhat naturally, not every second
        # e.g., staying in LOBBY for at least 5 seconds
        time_in_state = time.time() - self.last_state_change
        if time_in_state > random.randint(5, 30):
            self.transition()

        # 2. State-Specific Actions (Emissions)
        if self.state == STATE_IN_GAME:
            # Heartbeats (FPS/Latency)
            self._emit_heartbeat(producer, topic)
            
            # Level Up?
            if random.random() < 0.01:
                self.current_level += 1
                producer.send(topic, self._generate_base_event("level_up", level=self.current_level))

        elif self.state == STATE_SHOP:
            # Buy something?
            if random.random() < self.profile['spend_propensity']:
                self._emit_purchase(producer, topic)

        elif self.state == STATE_LOBBY:
            # Maybe leave a review?
            if random.random() < 0.005:
                self._emit_review(producer, topic)

    def _emit_heartbeat(self, producer, topic):
        # Latency Logic based on Region
        base_lat = {'NA': 30, 'EU': 100, 'ASIA': 150}.get(self.region, 200)
        jitter = random.randint(-10, 50)
        
        # FPS Logic based on Platform
        base_fps = 30 if self.platform in ['Mobile', 'Switch'] else 60
        if self.platform == 'PC': base_fps = 120
        
        event = self._generate_base_event(
            "heartbeat",
            fps=round(random.gauss(base_fps, 5), 1),
            latency_ms=max(10, base_lat + jitter),
            match_id=self.match_id
        )
        producer.send(topic, event)

    def _emit_purchase(self, producer, topic):
        # Pareto distribution for realistic spending
        amount = (random.paretovariate(2) - 1) * 10 + 0.99
        if self.archetype_name != 'WHALE' and amount > 50:
            amount = 9.99 # Cap normal players
            
        event = self._generate_base_event(
            "purchase",
            purchase_amount=round(amount, 2),
            currency="USD",
            item_id=f"item_{random.randint(100,999)}"
        )
        producer.send(topic, event)
        print(f"💰 {self.username} ({self.archetype_name}) spent ${round(amount, 2)}")

    def _emit_review(self, producer, topic):
        event = self._generate_base_event(
            "review",
            sentiment_score=round(random.uniform(0.1, 1.0), 2),
            review_text=fake.sentence()
        )
        producer.send(topic, event)

    def _generate_base_event(self, event_type, **kwargs):
        base = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": event_type,
            "game_id": self.game['id'],
            "game_name": self.game['name'],
            "player_id": self.player_id,
            "session_id": self.session_id,
            "region": self.region,
            "platform": self.platform,
            "player_type": self.archetype_name,
            "country": self.country
        }
        base.update(kwargs)
        return base

# --- MAIN LOOP ---

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
    
    # 1. Initialize Population
    population_size = 500
    players = [MarkovPlayer(games) for _ in range(population_size)]
    print(f"🚀 Initialized Markov Population of {population_size} players")

    # 2. Simulation Loop
    while True:
        # A. Concurrency Management (Arrival Process)
        # We randomly pick OFFLINE players and log them in based on a sine wave (Day/Night)
        cycle = (time.time() % 60) / 60.0
        target_active = int(population_size * (0.5 + 0.4 * math.sin(cycle * 2 * math.pi)))
        
        active_players = [p for p in players if p.state != STATE_OFFLINE]
        offline_players = [p for p in players if p.state == STATE_OFFLINE]
        
        # Login Logic
        if len(active_players) < target_active and offline_players:
            num_to_login = min(5, target_active - len(active_players))
            for _ in range(num_to_login):
                p = random.choice(offline_players)
                evt = p.login()
                producer.send(TOPIC_NAME, evt)
                offline_players.remove(p)

        # B. Tick all active players
        for p in active_players:
            p.tick(producer, TOPIC_NAME)
        
        # C. System Stats (Aggregated heartbeat for Dashboard Scaling)
        if random.random() < 0.1: # Every ~10 ticks
            for g in games:
                count = sum(1 for p in active_players if p.game['id'] == g['id'])
                producer.send(TOPIC_NAME, {
                    "event_type": "status",
                    "game_id": g['id'],
                    "player_count": count,
                    "timestamp": datetime.utcnow().isoformat()
                })

        time.sleep(1)

if __name__ == "__main__":
    print("Starting AI Markov Chain Generator...")
    generate_fake_data()
