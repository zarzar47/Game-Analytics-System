import json
import os

# Define the absolute path to the data file relative to this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "games.json")

def get_games():
    """Load games from the JSON file."""
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def get_game_by_id(game_id):
    """Retrieve a specific game by its ID."""
    games = get_games()
    for game in games:
        if game["id"] == game_id:
            return game
    return None
