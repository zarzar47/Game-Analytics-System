# Game-Analytics-System
A web app for analyzing video game data and doing advanced data analytics on it.

## Architecture

*   **Frontend**: Streamlit (Python) - Interactive Data Dashboard
*   **Backend**: FastAPI (Python) - Core Business Logic
*   **Database**: Supabase (PostgreSQL)
*   **Worker**: Python Faker Service (Data Generator)

## Quick Start

1.  **Setup Environment**
    Create a `.env` file with your Supabase credentials:
    ```ini
    SUPABASE_DB_URL="postgresql://..."
    ```

2.  **Run with Docker Compose**
    ```bash
    docker-compose up --build
    ```

3.  **Access the App**
    *   **Dashboard**: [http://localhost:8501](http://localhost:8501)
    *   **API Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)