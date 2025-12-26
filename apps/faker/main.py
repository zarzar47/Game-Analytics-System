import time
import json
import os
import random
import uuid
import math
from datetime import datetime, timedelta
from kafka import KafkaProducer
from game_library import get_games
from faker import Faker

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "GameAnalytics"

# Initialize Faker
fake = Faker()

# --- ENHANCED MARKOV CHAIN CONFIGURATION ---

# States
STATE_OFFLINE = "OFFLINE"
STATE_LOBBY = "LOBBY"
STATE_MATCHMAKING = "MATCHMAKING"
STATE_IN_GAME = "IN_GAME"
STATE_SHOP = "SHOP"

# Expanded Item Catalog (for dim_item)
ITEM_CATALOG = {
    'weapon_001': {'name': 'Legendary Sword', 'category': 'Weapon', 'base_price': 19.99},
    'weapon_002': {'name': 'Epic Rifle', 'category': 'Weapon', 'base_price': 14.99},
    'cosmetic_001': {'name': 'Dragon Skin', 'category': 'Cosmetic', 'base_price': 9.99},
    'cosmetic_002': {'name': 'Neon Outfit', 'category': 'Cosmetic', 'base_price': 7.99},
    'booster_001': {'name': 'XP Booster 24h', 'category': 'Booster', 'base_price': 4.99},
    'booster_002': {'name': 'Coin Doubler', 'category': 'Booster', 'base_price': 2.99},
    'currency_001': {'name': 'Gold Pack 1000', 'category': 'Currency', 'base_price': 9.99},
    'currency_002': {'name': 'Gems Pack 500', 'category': 'Currency', 'base_price': 4.99},
}

# Geography Mappings (for dim_geography)
GEOGRAPHY_MAP = {
    'US': {'country_code': 'US', 'region': 'NA'},
    'CA': {'country_code': 'CA', 'region': 'NA'},
    'MX': {'country_code': 'MX', 'region': 'NA'},
    'GB': {'country_code': 'GB', 'region': 'EU'},
    'DE': {'country_code': 'DE', 'region': 'EU'},
    'FR': {'country_code': 'FR', 'region': 'EU'},
    'JP': {'country_code': 'JP', 'region': 'ASIA'},
    'KR': {'country_code': 'KR', 'region': 'ASIA'},
    'CN': {'country_code': 'CN', 'region': 'ASIA'},
    'BR': {'country_code': 'BR', 'region': 'SA'},
    'AR': {'country_code': 'AR', 'region': 'SA'},
    'AU': {'country_code': 'AU', 'region': 'OCE'},
}

# Player Archetypes (Enhanced)
ARCHETYPES = {
    'CASUAL': {
        'churn_prob': 0.05,
        'skill': 0.3,
        'spend_propensity': 0.01,
        'session_duration_mean': 1200,  # 20 minutes
        'session_duration_std': 600,
        'transitions': {
            STATE_LOBBY:       {STATE_MATCHMAKING: 0.6, STATE_SHOP: 0.05, STATE_OFFLINE: 0.35},
            STATE_MATCHMAKING: {STATE_IN_GAME: 0.9, STATE_LOBBY: 0.1},
            STATE_IN_GAME:     {STATE_LOBBY: 0.1, STATE_IN_GAME: 0.9},
            STATE_SHOP:        {STATE_LOBBY: 0.9, STATE_SHOP: 0.1}
        }
    },
    'HARDCORE': {
        'churn_prob': 0.01,
        'skill': 0.8,
        'spend_propensity': 0.05,
        'session_duration_mean': 10800,  # 3 hours
        'session_duration_std': 3600,
        'transitions': {
            STATE_LOBBY:       {STATE_MATCHMAKING: 0.85, STATE_SHOP: 0.05, STATE_OFFLINE: 0.1},
            STATE_MATCHMAKING: {STATE_IN_GAME: 0.95, STATE_LOBBY: 0.05},
            STATE_IN_GAME:     {STATE_LOBBY: 0.02, STATE_IN_GAME: 0.98},
            STATE_SHOP:        {STATE_LOBBY: 0.9, STATE_SHOP: 0.1}
        }
    },
    'WHALE': {
        'churn_prob': 0.02,
        'skill': 0.5,
        'spend_propensity': 0.8,
        'session_duration_mean': 5400,  # 90 minutes
        'session_duration_std': 1800,
        'transitions': {
            STATE_LOBBY:       {STATE_MATCHMAKING: 0.5, STATE_SHOP: 0.4, STATE_OFFLINE: 0.1},
            STATE_MATCHMAKING: {STATE_IN_GAME: 0.9, STATE_LOBBY: 0.1},
            STATE_IN_GAME:     {STATE_LOBBY: 0.05, STATE_IN_GAME: 0.95},
            STATE_SHOP:        {STATE_LOBBY: 0.5, STATE_SHOP: 0.5}
        }
    }
}

class EnhancedMarkovPlayer:
    def __init__(self, game_pool):
        self.player_id = str(uuid.uuid4())
        self.username = fake.user_name()
        
        # Geography (properly mapped)
        country_code = random.choice(list(GEOGRAPHY_MAP.keys()))
        self.country = country_code
        geo_info = GEOGRAPHY_MAP[country_code]
        self.region = geo_info['region']
        
        self.platform = random.choice(['PC', 'PS5', 'Xbox', 'Switch', 'Mobile'])
        self.archetype_name = random.choices(
            ['CASUAL', 'HARDCORE', 'WHALE'], 
            weights=[0.6, 0.3, 0.1], k=1
        )[0]
        self.profile = ARCHETYPES[self.archetype_name]
        
        # Registration date (for dim_player)
        self.registration_date = fake.date_time_between(start_date='-2y', end_date='now')
        
        self.state = STATE_OFFLINE
        self.game = random.choice(game_pool)
        self.session_id = None
        self.session_start_time = None
        self.last_state_change = time.time()
        
        # Enhanced state metadata
        self.match_id = None
        self.current_level = random.randint(1, 100)
        self.total_playtime_hours = random.randint(10, 5000)  # Total career playtime
        self.lifetime_spend = 0.0
        
    def transition(self):
        """Executes a Markov transition based on current state and archetype probabilities."""
        current_trans = self.profile['transitions'].get(self.state)
        
        if not current_trans:
            return

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
        """Player logs in - creates session_start event"""
        self.state = STATE_LOBBY
        self.session_id = str(uuid.uuid4())
        self.session_start_time = datetime.utcnow()
        self.last_state_change = time.time()
        
        # Calculate expected session duration from archetype profile
        expected_duration = max(60, random.gauss(
            self.profile['session_duration_mean'],
            self.profile['session_duration_std']
        ))
        
        return self._generate_base_event("session_start", 
            expected_duration_sec=round(expected_duration, 2)
        )

    def _end_session(self):
        """Player logs out - session end event"""
        if self.session_id and self.session_start_time:
            actual_duration = (datetime.utcnow() - self.session_start_time).total_seconds()
            self.total_playtime_hours += actual_duration / 3600
        
        self.state = STATE_OFFLINE
        self.session_id = None
        self.session_start_time = None
        self.match_id = None

    def tick(self, producer, topic):
        """Called every simulation tick. Generates events based on current state."""
        if self.state == STATE_OFFLINE:
            return

        # State transition logic
        time_in_state = time.time() - self.last_state_change
        if time_in_state > random.randint(5, 30):
            self.transition()

        # State-Specific Actions
        if self.state == STATE_IN_GAME:
            # Heartbeats (enriched with all telemetry fields)
            if random.random() < 0.3:  # 30% chance per tick
                self._emit_heartbeat(producer, topic)
            
            # Level Up (progression event)
            if random.random() < 0.01:
                self.current_level += 1
                xp_gained = random.randint(100, 500)
                producer.send(topic, self._generate_base_event(
                    "level_up", 
                    level_index=self.current_level,
                    xp_gained=xp_gained
                ))

        elif self.state == STATE_SHOP:
            # Purchase event (enriched with all transaction fields)
            if random.random() < self.profile['spend_propensity']:
                self._emit_purchase(producer, topic)

        elif self.state == STATE_LOBBY:
            # Review/Feedback event
            if random.random() < 0.005:
                self._emit_review(producer, topic)

    def _emit_heartbeat(self, producer, topic):
        """Emit telemetry event with full fact_telemetry schema"""
        # Latency based on region
        base_lat = {'NA': 30, 'EU': 100, 'ASIA': 150, 'SA': 120, 'OCE': 180}.get(self.region, 200)
        jitter = random.randint(-10, 50)
        
        # FPS based on platform
        platform_fps_map = {
            'PC': (120, 10),
            'PS5': (60, 5),
            'Xbox': (60, 5),
            'Switch': (30, 3),
            'Mobile': (30, 5)
        }
        base_fps, fps_std = platform_fps_map.get(self.platform, (60, 5))
        
        event = self._generate_base_event(
            "heartbeat",
            fps=round(random.gauss(base_fps, fps_std), 1),
            latency_ms=max(10, base_lat + jitter),
            match_id=self.match_id,
            cpu_usage=round(random.uniform(20, 90), 1),  # For future telemetry expansion
            packet_loss_percent=round(random.uniform(0, 2), 2)
        )
        producer.send(topic, event)

    def _emit_purchase(self, producer, topic):
        """Emit purchase event with full fact_transaction schema"""
        # Select random item from catalog
        item_id = random.choice(list(ITEM_CATALOG.keys()))
        item_info = ITEM_CATALOG[item_id]
        
        # Calculate amount (with possible discount)
        base_amount = item_info['base_price']
        discount = random.choice([0, 0.1, 0.2, 0.3]) if random.random() < 0.3 else 0
        final_amount = base_amount * (1 - discount)
        
        # Whale adjustment (they spend more on premium items)
        if self.archetype_name == 'WHALE' and random.random() < 0.3:
            final_amount *= random.uniform(1.5, 3.0)
        
        self.lifetime_spend += final_amount
        
        # Determine payment method
        payment_method = random.choice(['credit_card', 'paypal', 'cryptocurrency', 'mobile_wallet'])
        
        # Is this their first purchase?
        is_first_purchase = self.lifetime_spend <= final_amount * 1.1
        
        event = self._generate_base_event(
            "purchase",
            transaction_id=str(uuid.uuid4()),
            purchase_amount=round(final_amount, 2),
            currency="USD",
            item_id=item_id,
            item_name=item_info['name'],
            item_category=item_info['category'],
            discount_applied=discount,
            payment_method=payment_method,
            is_first_purchase=is_first_purchase
        )
        producer.send(topic, event)
        print(f"💰 {self.username} ({self.archetype_name}) spent ${round(final_amount, 2)} on {item_info['name']}")

    def _emit_review(self, producer, topic):
        """Emit review/feedback event with full fact_feedback schema"""
        # Sentiment based on player experience
        # Better players (higher level, more playtime) tend to be more positive
        base_sentiment = 0.5
        level_bonus = min(0.2, self.current_level / 500)
        archetype_bonus = {'CASUAL': -0.1, 'HARDCORE': 0.2, 'WHALE': 0.1}.get(self.archetype_name, 0)
        
        sentiment = max(0.1, min(1.0, base_sentiment + level_bonus + archetype_bonus + random.gauss(0, 0.2)))
        
        review_text = fake.sentence(nb_words=random.randint(10, 30))
        
        event = self._generate_base_event(
            "review",
            review_id=str(uuid.uuid4()),
            sentiment_score=round(sentiment, 2),
            review_text=review_text,
            playtime_at_review_hours=round(self.total_playtime_hours, 1)
        )
        producer.send(topic, event)

    def _generate_base_event(self, event_type, **kwargs):
        """Generate base event structure with all common fields"""
        base = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": event_type,
            
            # Game dimension
            "game_id": str(self.game['id']),
            "game_name": self.game['name'],
            
            # Player dimension
            "player_id": self.player_id,
            "username": self.username,
            "country_code": self.country,
            "region": self.region,
            "platform": self.platform,
            "player_segment": self.archetype_name,
            "registration_date": self.registration_date.isoformat(),
            "is_active": True,
            
            # Session dimension
            "session_id": self.session_id,
            
            # Time dimension (will be parsed in Airflow)
            # No need to duplicate - timestamp is enough
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
            print("Connected to Kafka")
            return producer
        except Exception as e:
            print(f"Waiting for Kafka: {e}")
            time.sleep(5)

def generate_fake_data():
    producer = get_producer()
    games = get_games()
    
    # Initialize Population
    population_size = 2000
    players = [EnhancedMarkovPlayer(games) for _ in range(population_size)]
    print(f"Initialized Enhanced Markov Population of {population_size} players")
    print(f"Generating data for Star Schema: dim_player, dim_game, dim_time, dim_geography, dim_item")
    print(f"Populating Facts: fact_session, fact_transaction, fact_telemetry, fact_progression, fact_feedback")

    # Simulation Loop
    tick_count = 0
    while True:
        tick_count += 1
        
        # Arrival Process (Day/Night Cycle)
        cycle = (time.time() % 60) / 60.0
        target_active = int(population_size * (0.5 + 0.4 * math.sin(cycle * 2 * math.pi)))
        
        active_players = [p for p in players if p.state != STATE_OFFLINE]
        offline_players = [p for p in players if p.state == STATE_OFFLINE]
        
        # Login Logic
        if len(active_players) < target_active and offline_players:
            num_to_login = min(20, target_active - len(active_players))
            for _ in range(num_to_login):
                p = random.choice(offline_players)
                evt = p.login()
                producer.send(TOPIC_NAME, evt)
                offline_players.remove(p)

        # Tick all active players
        for p in active_players:
            p.tick(producer, TOPIC_NAME)
        
        # Status update (for monitoring)
        if tick_count % 10 == 0:
            print(f"🎮 Active Players: {len(active_players)}/{population_size} | Tick: {tick_count}")
        
        time.sleep(0.2)

if __name__ == "__main__":
    print("=" * 80)
    print("ENHANCED MARKOV CHAIN GENERATOR FOR STAR SCHEMA")
    print("=" * 80)
    generate_fake_data()