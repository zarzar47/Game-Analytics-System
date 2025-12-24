# Game Analytics System

A comprehensive, microservice-based web application for real-time video game analytics. It simulates, processes, and visualizes game telemetry data using a modern event-driven architecture.

## Architecture

The system is built as a **Monorepo** (Turborepo-style) using **Docker** for orchestration.

*   **Frontend (`apps/web`)**: 
    *   **Streamlit** dashboard for real-time visualization.
    *   Consumes data directly from **Kafka** for live updates.
    *   Provides a "Global Overview" grid and detailed "Analytics Views" per game.
*   **Backend (`apps/api`)**: 
    *   **FastAPI** service handling business logic.
    *   Runs background consumers to persist Kafka data to **PostgreSQL**.
*   **Data Streaming (`kafka` & `zookeeper`)**:
    *   **Apache Kafka** acts as the central event bus, decoupling data producers from consumers.
    *   Ensures low-latency delivery of game events.
*   **Data Generator (`apps/faker`)**: 
    *   **AI-Driven Simulation**: Uses **Markov Chains** to model realistic player lifecycles (Login -> Lobby -> Match -> Shop).
    *   **Behavioral Personas**: Simulates 'Whales' (High Spend), 'Casuals' (High Churn), and 'Hardcore' (Long Sessions) users with distinct statistical properties.
    *   Produces correlated event streams (Heartbeats, Purchases, Progression) to Kafka.
*   **Shared Library (`packages/game_library`)**:
    *   A common Python package containing shared game definitions (IDs, Names, Cover URLs) to ensure consistency across all services.
*   **Database**: 
    *   **PostgreSQL** (Dockerized) for persistent storage.

## Quick Start

### Prerequisites
*   Docker & Docker Compose

### Running the Stack

1.  **Clone & Start**
    Build and start all services (API, Frontend, Database, Kafka, Faker) with a single command:
    ```bash
    docker-compose up --build
    ```

2.  **Access the Dashboard**
    *   Open your browser to **[http://localhost:8501](http://localhost:8501)**.
    *   You will see the **Global Overview** updating in real-time as the `faker` service generates data.
    *   Click "Analytics" on any game card to view detailed metrics.

3.  **Other Endpoints**
    *   **API Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
    *   **PostgreSQL**: `localhost:5432` (User: `rafay`, Pass: `rafay`, DB: `game_analytics`)
    *   **Kafka Broker**: `localhost:9092`

## Tech Stack

*   **Language**: Python 3.11 (All services)
*   **Web Frameworks**: Streamlit (UI), FastAPI (API)
*   **Streaming**: Apache Kafka, Zookeeper, `kafka-python`, `aiokafka`
*   **Database**: PostgreSQL, SQLAlchemy (Async)
*   **Containerization**: Docker, Docker Compose

## Future Roadmap

Check [ROADMAP.md](./ROADMAP.md) for planned features and improvements.