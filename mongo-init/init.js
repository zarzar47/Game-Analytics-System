// Initialize game_analytics database
db = db.getSiblingDB('game_analytics');

// Create collections
db.createCollection('game_events');
db.createCollection('sessions');
db.createCollection('transactions');
db.createCollection('archive_metadata');

// Create indexes for performance
db.game_events.createIndex({ "timestamp": 1 });
db.game_events.createIndex({ "game_id": 1 });
db.game_events.createIndex({ "event_type": 1 });
db.game_events.createIndex({ "player_id": 1 });

db.sessions.createIndex({ "player_id": 1 });
db.sessions.createIndex({ "session_id": 1 }, { unique: true });
db.sessions.createIndex({ "timestamp": 1 });

db.transactions.createIndex({ "player_id": 1, "timestamp": -1 });
db.transactions.createIndex({ "transaction_id": 1 }, { unique: true });

db.archive_metadata.createIndex({ "archive_time": -1 });
db.archive_metadata.createIndex({ "collection": 1 });

print("✅ MongoDB initialized successfully for game analytics");
