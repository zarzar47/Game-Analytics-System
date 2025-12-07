# Game-Analytics-System
A web app for analyzing video game data and doing advanced data analytics on it.

## Architecture

*   **Frontend**: Streamlit (Python) - Interactive Data Dashboard
*   **Backend**: FastAPI (Python) - Core Business Logic
*   **Database**: Local PostgreSQL (Dockerized)
*   **Worker**: Python Faker Service (Data Generator)

## Quick Start

1.  **Run with Docker Compose**
    This will spin up the entire stack, including a local PostgreSQL database.
    ```bash
    docker-compose up --build
    ```

2.  **Access the App**
    *   **Dashboard**: [http://localhost:8501](http://localhost:8501)
    *   **API Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
    *   **Database**: `localhost:5432` (User: `rafay`, Pass: `rafay`, DB: `game_analytics`)
